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
        TransactionType::try_from(id_byte).expect(
            "Transaction ID contains invalid type byte; this is a bug in Transaction construction",
        )
    }

    pub fn timed_out(&self) -> bool {
        self.elapsed() >= crate::config::OPERATION_TTL
    }

    /// Milliseconds-since-Unix-epoch encoded in this transaction's ULID at
    /// creation time.
    ///
    /// In production this is the wall-clock creation time. In simulation mode
    /// (`GlobalSimulationTime` set) it is a fixed epoch plus a counter that
    /// increments by 1 per ULID GENERATED — NOT advanced by the simulation's
    /// virtual clock. So across transactions it is a deterministic, monotonic
    /// GENERATION-ORDER value, usable by tests as an *early-run ordering proxy*
    /// against the simulation epoch — but it is NOT a literal virtual-time
    /// reading, and its calibration to virtual seconds depends on ULID volume.
    /// Cheap and feature-independent (the `trace-ot`-gated `started()` exposes
    /// the same value as a `SystemTime`).
    pub fn created_at_ms(&self) -> u64 {
        self.id.timestamp_ms()
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
        Self::ttl_transaction_with_multiplier(1)
    }

    /// Like [`ttl_transaction`](Self::ttl_transaction) but with a custom TTL multiplier.
    ///
    /// Used for absolute timeout enforcement on operations that would otherwise
    /// be exempt from garbage collection (e.g., `under_progress` operations).
    pub fn ttl_transaction_with_multiplier(multiplier: u64) -> Self {
        let id = crate::config::GlobalSimulationTime::new_ulid();
        let ts = id.timestamp_ms();
        let ttl_ms = crate::config::OPERATION_TTL.as_millis() as u64 * multiplier;
        let ttl_epoch: u64 = ts.saturating_sub(ttl_ms);

        // Clear the timestamp bits and replace with the cutoff timestamp.
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
        let bytes: u128 = Ulid::generate().0;
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

    impl TryFrom<u8> for TransactionType {
        type Error = u8;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(TransactionType::Connect),
                1 => Ok(TransactionType::Put),
                2 => Ok(TransactionType::Get),
                3 => Ok(TransactionType::Subscribe),
                4 => Ok(TransactionType::Update),
                other => Err(other),
            }
        }
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
    Update(UpdateMsg),
    Aborted(Transaction),
    /// Neighbor hosting protocol message for tracking which neighbors host which contracts.
    NeighborHosting {
        message: NeighborHostingMessage,
    },
    /// Interest synchronization protocol for delta-based updates.
    InterestSync {
        message: InterestMessage,
    },
    /// Peer readiness advertisement: indicates whether the sender is ready
    /// to accept non-CONNECT operations. Peers behind symmetric NAT with
    /// only a gateway connection broadcast `ready: false` (implicitly, by
    /// not yet sending this message) and `ready: true` once they have
    /// enough ring connections (`min_ready_connections`).
    ReadyState {
        ready: bool,
    },
    /// Fire-and-forget hint nudging the recipient to host a contract.
    ///
    /// A host sends this to a connected neighbor that is closer to the
    /// contract's key but isn't hosting it. The recipient should subscribe to
    /// `key` directed through `holder` (the sender), thereby fetching and
    /// hosting it. There is no reply: the recipient may act on it or ignore it.
    SubscribeHint(SubscribeHintMsg),
}

/// Payload for [`NetMessageV1::SubscribeHint`]: a directed nudge to host a contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeHintMsg {
    /// The contract the recipient is being nudged to host.
    pub key: ContractKey,
    /// The peer that currently holds (hosts) the contract — the sender of this
    /// hint. The recipient subscribes to `key` directed through `holder` (a
    /// plain greedy subscribe would route away from it), thereby hosting it.
    pub holder: PeerKeyLocation,
}

/// Messages for the neighbor hosting protocol.
///
/// This protocol allows neighbors to inform each other which contracts they are hosting,
/// enabling UPDATE forwarding to hosts who may not be explicitly subscribed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum NeighborHostingMessage {
    /// Announce changes to our hosted contracts.
    HostingAnnounce {
        /// Contracts we've started hosting.
        added: Vec<ContractInstanceId>,
        /// Contracts we've stopped hosting.
        removed: Vec<ContractInstanceId>,
        /// True if this is a response to a received announcement.
        /// Recipients should not respond to responses (prevents ping-pong).
        #[serde(default)]
        is_response: bool,
    },
    /// Request a neighbor's full hosting state (used on new connections).
    HostingStateRequest,
    /// Response with the neighbor's full hosting state.
    HostingStateResponse { contracts: Vec<ContractInstanceId> },
}

/// Messages for the delta-based interest synchronization protocol.
///
/// This protocol enables peers to:
/// 1. Discover shared contract interests at connection time
/// 2. Exchange state summaries for delta computation
/// 3. Track interest changes during the connection
/// 4. Request full state resync when delta application fails
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InterestMessage {
    /// Connection-time interest exchange.
    ///
    /// Sent by both peers immediately after connection establishment.
    /// Contains fast hashes (FNV-1a) of contract IDs for efficient matching.
    Interests {
        /// Fast u32 hashes of contract IDs we're interested in.
        hashes: Vec<u32>,
    },

    /// State summaries for contracts both peers share interest in.
    ///
    /// Sent after comparing `Interests` hashes. Only includes summaries
    /// for contracts where we have state (summary is None if we have no state).
    Summaries {
        /// (contract_hash, summary_bytes) pairs for shared contracts.
        /// Summary bytes is None if we're interested but don't have state yet.
        /// Use `SummaryEntry::from_summary()` to create entries.
        entries: Vec<SummaryEntry>,
        /// Which code path built this message. NOT part of the wire format
        /// (`#[serde(skip)]`), so it neither costs bytes nor changes what an
        /// older peer decodes — see [`SummariesEmitter`] for why it exists and
        /// what an inbound message's value means.
        #[serde(skip)]
        emitter: SummariesEmitter,
    },

    /// Incremental changes to our contract interests.
    ///
    /// Sent when we gain or lose interest in contracts after connection.
    /// The receiver responds with Summaries for newly shared contracts.
    ChangeInterests {
        /// Contract hashes we've newly become interested in.
        added: Vec<u32>,
        /// Contract hashes we're no longer interested in.
        removed: Vec<u32>,
    },

    /// Request full state resync when delta application fails.
    ///
    /// Sent when a received delta cannot be applied (corruption, version mismatch).
    /// The upstream peer responds with ResyncResponse containing full state.
    ResyncRequest {
        /// The contract that needs resync.
        key: ContractKey,
    },

    /// Response to ResyncRequest with full state.
    ///
    /// Sent when a peer requests resync after delta application failure.
    /// Contains the full contract state and sender's summary.
    ResyncResponse {
        /// The contract being resynced.
        key: ContractKey,
        /// Full contract state bytes.
        state_bytes: Vec<u8>,
        /// Sender's current state summary bytes.
        summary_bytes: Vec<u8>,
    },

    // ---------------------------------------------------------------------
    // Hash-first summary exchange (#4965).
    //
    // APPENDED, and any future variant must be appended too: bincode encodes
    // the variant as its positional index, so inserting anywhere above would
    // renumber `Summaries`/`ResyncRequest`/… and silently mis-decode every
    // message from an older peer. `wire_variant_indices_are_frozen` pins this.
    // ---------------------------------------------------------------------
    /// Hash-first replacement for [`InterestMessage::Summaries`]: advertises a
    /// *digest* of each summary instead of the summary itself.
    ///
    /// Sent in place of `Summaries` when — and only when — the recipient's
    /// reported version is at or above
    /// `crate::node::HASH_FIRST_SUMMARIES_MIN_VERSION` (an older peer cannot
    /// deserialize this variant index and would drop the connection).
    ///
    /// The receiver compares each digest against a digest of **its own actual
    /// summary**, and asks for the bytes of only the ones that differ (via
    /// [`InterestMessage::SummaryRequest`]). Measured on the fleet, 98.1% of
    /// summary comparisons find both sides already byte-identical (#4965), so
    /// the overwhelmingly common case stops shipping ~33 KB per contract to
    /// say "nothing changed".
    SummaryDigests {
        /// (contract_hash, summary_digest) pairs for shared contracts.
        /// The digest is `None` if we're interested but hold no state yet —
        /// exactly the case `SummaryEntry::summary_bytes == None` covers.
        entries: Vec<SummaryDigestEntry>,
        /// Which send path built this — the same non-wire (`#[serde(skip)]`)
        /// tag [`InterestMessage::Summaries`] carries (#5052).
        ///
        /// The digest form REPLACES a `Summaries` at every send site, so it
        /// must carry the same tag or a path would lose its per-emitter
        /// attribution the moment its peer upgrades — the rollup would show
        /// the named arm shrinking and the residual arm growing, which reads
        /// as "hash-first saved bytes" when it actually means "we stopped
        /// being able to see them".
        #[serde(skip)]
        emitter: SummariesEmitter,
    },

    /// Ask a peer for the FULL summary bytes of specific contracts.
    ///
    /// Sent only in reply to [`InterestMessage::SummaryDigests`], for the
    /// entries whose advertised digest did not match our own summary's digest.
    /// The peer answers with a plain [`InterestMessage::Summaries`], so the
    /// mismatch path funnels back into the unchanged `Summaries` handler —
    /// including its semantic staleness check and targeted `SyncStateToPeer`
    /// heal. The exchange terminates there (`Summaries` never replies).
    SummaryRequest {
        /// Contract hashes (same `contract_hash` space as
        /// [`InterestMessage::Interests`]) whose summary bytes we need.
        hashes: Vec<u32>,
    },
}

/// Which code path built an [`InterestMessage::Summaries`] — a NON-WIRE
/// provenance tag carried alongside the message so the outbound byte census
/// can attribute it (#5052).
///
/// ## Why the tag rides on the message
///
/// `interest_sync_summaries` is 49.8% of all outbound bytes on the fleet, and
/// the arm counts four unrelated emitters together. They have opposite fixes:
/// if the per-state-change notification dominates, #5003 (skip co-hosts the
/// broadcast already covered, no wire change) is most of the answer; if the
/// heartbeat reply dominates, the answer is hash-first (#4965), a wire-format
/// change. A total that both drive cannot decide between them.
///
/// The census is taken at ONE choke point — the single place a `NetMessage` is
/// handed to a connection ([`OutboundClass::classify`][c]) — where all that
/// survives of the emitter is the message itself. So the emitter has to travel
/// with it. Tagging at construction rather than calling a `record_*` at each
/// site is deliberate: `.claude/rules/bug-prevention-patterns.md` has a whole
/// row on manually-mirrored telemetry counters silently rotting when an op
/// path is migrated, and a mandatory field cannot rot — a fifth emitter fails
/// to COMPILE until it names its own arm, instead of quietly landing in the
/// residual.
///
/// ## What it costs on the wire: nothing
///
/// The field is `#[serde(skip)]`, so the encoding of `Summaries` is byte-for-byte
/// what it was before this tag existed (pinned by
/// `summaries_emitter_tag_is_not_on_the_wire`). Two consequences worth stating
/// because the first is easy to forget:
///
/// * a peer on any version decodes our messages exactly as before, and
/// * an INBOUND `Summaries` always arrives as [`SummariesEmitter::Other`],
///   because that is what `Default` supplies where the wire carries nothing.
///   That is harmless today (the census only measures what this node SENDS,
///   and every outbound `Summaries` is built locally), but a future path that
///   re-sends a decoded message would report it as unattributed rather than
///   mislabelling it.
///
/// [c]: crate::node::network_bridge::outbound_message_mix::OutboundClass::classify
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SummariesEmitter {
    /// `operations::update::send_proactive_summary_notification` — one entry,
    /// fanned to every interested peer on every state change.
    Notification,
    /// `node::handle_interest_sync_message`, replying to an `Interests`
    /// advertisement — MULTI-entry, one per shared advertised contract, on
    /// every ~5-min heartbeat received.
    InterestsReply,
    /// `node::handle_interest_sync_message`, replying to a `ChangeInterests`
    /// delta — driven by interest churn rather than by the heartbeat clock. Kept
    /// apart from [`Self::InterestsReply`] so the residual arm below stays a pure
    /// residual; folding the two would repeat, one level down, exactly the
    /// conflation this tag exists to undo.
    ///
    /// **SINGLE-entry, essentially always — but by CALLER convention, not by
    /// construction.** `broadcast_change_interests` takes `added: Vec<ContractKey>`
    /// and every caller today passes at most one, yet nothing pins that; and the
    /// reply loop's hash-collision path can yield 2+ entries on a u32 FNV-1a
    /// collision. So this is an empirical property of the current call sites, and
    /// it is deliberately left unpinned: the R4b instrument is robust either way
    /// (a multi-entry reply simply classifies as `MultiEntry` and leaves the
    /// single-entry population). Contrast the NOTIFICATION leg, whose identical
    /// structural property IS pinned, by
    /// `notification_leg_is_always_full_bytes_and_single_entry` — because `p` is
    /// read off that leg, so drift there corrupts the measurement rather than
    /// merely shrinking its denominator.
    ///
    /// Corrected 2026-08-12 (#5153 review
    /// F1); this said "also multi-entry" and that was measurably false.
    /// `operations::broadcast_change_interests` is called with one contract per
    /// gossip, so the reply built for it carries one entry: mean **1.000**
    /// entries/msg with `max_entries` **1** across 418,476 messages on 1,284
    /// peers in one window. Load-bearing, not trivia — it is why message LENGTH
    /// is not a clean proxy for "this is a notification", which the R4b
    /// agreement-rate instrument depends on.
    ChangeInterestsReply,
    /// `operations::update::send_summary_back_on_rejection` — one entry, only
    /// when a rejected broadcast's summary already matched ours.
    Rejection,
    /// `node::handle_interest_sync_message`, replying to a `SummaryRequest`
    /// with the bytes a digest could not settle (#4965).
    ///
    /// The one full-bytes send that hash-first ADDS rather than replaces, so
    /// it gets its own arm: folded into `InterestsReply` it would look like
    /// the heartbeat failing to shrink, when it is the mismatch tail doing
    /// exactly what it is supposed to do.
    SummaryRequestReply,
    /// The `SummaryRequest` leg itself — a bare list of contract hashes, no
    /// summaries (#4965).
    ///
    /// Carries no payload worth attributing, but is tagged anyway because the
    /// per-emitter arms must SUM to `interest_sync_summaries`; an untagged
    /// message would open a gap between the split and the arm it splits.
    SummaryRequest,
    /// Residual: no emitter claimed this message. The `Default`, so it is also
    /// what a decoded inbound message carries.
    ///
    /// A non-zero `interest_sync_summaries_other_bytes` in the rollup means a
    /// send path exists that this enum does not describe. That is the point of
    /// having it — an unattributed emitter shows up as a number to chase
    /// instead of silently inflating one of the named arms.
    #[default]
    Other,
}

/// A summary entry for the Summaries message.
/// Uses owned bytes for wire serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryEntry {
    /// Fast hash of the contract ID.
    pub hash: u32,
    /// Summary bytes, or None if we're interested but don't have state yet.
    pub summary_bytes: Option<Vec<u8>>,
}

impl SummaryEntry {
    /// Create a summary entry from a contract hash and optional summary.
    pub fn from_summary(
        hash: u32,
        summary: Option<&freenet_stdlib::prelude::StateSummary<'_>>,
    ) -> Self {
        Self {
            hash,
            summary_bytes: summary.map(|s| s.as_ref().to_vec()),
        }
    }

    /// Convert the summary bytes back to a StateSummary.
    pub fn to_summary(&self) -> Option<freenet_stdlib::prelude::StateSummary<'static>> {
        self.summary_bytes
            .as_ref()
            .map(|bytes| freenet_stdlib::prelude::StateSummary::from(bytes.clone()))
    }
}

/// The hash-first counterpart of [`SummaryEntry`]: identifies a contract and
/// describes our summary of it, without carrying the summary.
///
/// The two fields are DIFFERENT hashes of different things and must not be
/// conflated — see [`crate::ring::interest::summary_digest`] for the full
/// contrast table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryDigestEntry {
    /// Fast FNV-1a hash of the contract INSTANCE ID — identical in meaning and
    /// value to [`SummaryEntry::hash`]. Says *which* contract; says nothing
    /// about its state.
    pub hash: u32,
    /// Truncated-BLAKE3 digest of our summary BYTES for that contract, or
    /// `None` if we're interested but hold no state yet. Says *what state* we
    /// hold.
    ///
    /// Fixed 16 bytes, so for a summary SMALLER than ~8 bytes the digest form
    /// is LARGER than the summary it replaces. Real summaries are orders of
    /// magnitude bigger (~33 KB for a River room), so this is a curiosity
    /// rather than a cost — but it means the saving is not monotonic in
    /// summary size, and a contract with a trivially small summary should not
    /// be expected to benefit.
    pub summary_digest: Option<crate::ring::interest::SummaryDigest>,
}

impl SummaryDigestEntry {
    /// Build a digest entry from the full-bytes entry we would otherwise have
    /// sent.
    ///
    /// This is deliberately the only constructor PROVIDED, so every digest is
    /// a pure function of the exact `SummaryEntry` the fallback `Summaries`
    /// form would have carried and the two wire forms cannot describe
    /// different state.
    ///
    /// It is a convention, not an invariant: the fields are `pub`, so a caller
    /// could build the struct literally and pair a digest with an unrelated
    /// summary. Nothing pins that today. Making the fields private would cost
    /// the wire-format tests their literal construction, which is why the
    /// weaker guarantee is stated rather than an enforcement implied. Those summaries in turn always come from
    /// the node's ACTUAL state (`summary_if_hosted_or_in_use` /
    /// `get_contract_summary`), never from a cached belief about a peer.
    pub fn from_entry(entry: &SummaryEntry) -> Self {
        Self {
            hash: entry.hash,
            summary_digest: entry
                .summary_bytes
                .as_deref()
                .map(crate::ring::interest::summary_digest),
        }
    }
}

/// Payload for delta-based updates.
///
/// Used in update messages to send either a delta (when we know the peer's summary)
/// or full state (when we don't know their state or delta would be inefficient).
///
/// NOTE: This type provides foundation infrastructure for delta-based updates.
/// Methods are marked `#[allow(dead_code)]` because they will be used in
/// follow-up PRs that integrate the full delta sync workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum DeltaOrFullState {
    /// A delta computed from the peer's cached summary.
    /// More efficient when both peers have state and the delta is small.
    /// Uses owned bytes for wire serialization.
    Delta(Vec<u8>),

    /// Full contract state. Used when:
    ///
    /// - Peer has no cached summary (first sync)
    /// - Delta would be larger than 50% of state size
    /// - After a ResyncRequest
    ///
    /// Uses owned bytes for wire serialization.
    FullState(Vec<u8>),
}

#[allow(dead_code)]
impl DeltaOrFullState {
    /// Create a Delta variant from a StateDelta.
    pub fn from_delta(delta: &freenet_stdlib::prelude::StateDelta<'_>) -> Self {
        Self::Delta(delta.as_ref().to_vec())
    }

    /// Create a FullState variant from a State.
    pub fn from_state(state: &freenet_stdlib::prelude::State<'_>) -> Self {
        Self::FullState(state.as_ref().to_vec())
    }

    /// Convert to a StateDelta (if this is a Delta variant).
    pub fn to_delta(&self) -> Option<freenet_stdlib::prelude::StateDelta<'static>> {
        match self {
            Self::Delta(bytes) => Some(freenet_stdlib::prelude::StateDelta::from(bytes.clone())),
            Self::FullState(_) => None,
        }
    }

    /// Convert to a State (if this is a FullState variant).
    pub fn to_state(&self) -> Option<freenet_stdlib::prelude::State<'static>> {
        match self {
            Self::Delta(_) => None,
            Self::FullState(bytes) => Some(freenet_stdlib::prelude::State::from(bytes.clone())),
        }
    }

    /// Check if this is a delta (not full state).
    pub fn is_delta(&self) -> bool {
        matches!(self, Self::Delta(_))
    }

    /// Get the raw bytes of the payload.
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Delta(bytes) | Self::FullState(bytes) => bytes,
        }
    }

    /// Get the size in bytes of the payload.
    pub fn size(&self) -> usize {
        self.bytes().len()
    }
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
            NetMessageV1::Get(_) => semver::Version::new(1, 1, 0),
            NetMessageV1::Subscribe(_) => semver::Version::new(1, 1, 0),
            // Version 2.0.0 for delta-based BroadcastTo format
            NetMessageV1::Update(_) => semver::Version::new(2, 0, 0),
            NetMessageV1::Aborted(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::NeighborHosting { .. } => semver::Version::new(1, 0, 0),
            // Version 1.1.0 for delta-based interest sync
            NetMessageV1::InterestSync { .. } => semver::Version::new(1, 1, 0),
            NetMessageV1::ReadyState { .. } => semver::Version::new(1, 2, 0),
            NetMessageV1::SubscribeHint(_) => semver::Version::new(1, 3, 0),
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
    /// Drop all connections (ring + transient). Used after suspend/resume
    /// to force fresh transport reconnection to gateways.
    DropAllConnections,
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
    /// A parked op whose awaited peer was just pruned (#4313). The event-loop
    /// handler delivers `WaiterReply::PeerDisconnected` into the waiter channel
    /// before dropping the sender, then cleans up like `TransactionCompleted`.
    TransactionOrphaned {
        tx: Transaction,
        peer: SocketAddr,
    },
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
    /// - Network peer subscriptions use the `hosting_manager.subscribers` for UPDATE propagation
    LocalSubscribeComplete {
        tx: Transaction,
        key: ContractKey,
        subscribed: bool,
        /// Whether this was a node-internal subscription renewal (no client waiting).
        is_renewal: bool,
    },
    /// Register expectation for an inbound connection from the given peer.
    ExpectPeerConnection {
        addr: SocketAddr,
    },
    /// Broadcast a proximity cache message to all connected peers.
    BroadcastHostingUpdate {
        message: NeighborHostingMessage,
    },
    /// Broadcast a ChangeInterests message to all connected peers for delta sync.
    BroadcastChangeInterests {
        added: Vec<u32>,
        removed: Vec<u32>,
    },
    /// Send an interest message to a specific peer.
    /// Used for ResyncRequest when delta application fails.
    SendInterestMessage {
        target: SocketAddr,
        message: InterestMessage,
    },
    /// Send an arbitrary `NetMessage` to a specific peer without registering
    /// a `pending_op_results` callback.
    ///
    /// Use case: the CONNECT originator driver holds an active
    /// multi-reply receiver for its transaction. When the
    /// joiner's hole-punch to an acceptor fails, it must emit
    /// `ConnectMsg::ConnectFailed` upstream so the relay chain can re-route.
    /// Routing that emission through `op_execution_sender`
    /// (`OpCtx::send_fire_and_forget` or similar) would overwrite the
    /// existing `pending_op_results` slot for the same tx, tearing down
    /// the multi-reply receiver. This event delivers the message via
    /// `ConnEvent::OutboundMessageWithTarget` without touching
    /// `pending_op_results`.
    SendNetMessage {
        target: SocketAddr,
        msg: Box<NetMessage>,
    },
    /// Broadcast state change to interested network peers.
    /// Emitted by executor when local state changes.
    /// Handled by p2p_protoc which has access to OpManager and network.
    BroadcastStateChange {
        key: ContractKey,
        new_state: WrappedState,
        /// `false` for a fresh broadcast emitted by the executor on a local
        /// state change; `true` for a no-target retry re-emission scheduled by
        /// `handle_broadcast_state_change`. Lets the handler count each fresh
        /// logical broadcast once for the #4281 propagation summary without
        /// re-counting (or being confused by) retries that share the
        /// per-contract `broadcast_retries` state.
        is_retry: bool,
        /// `true` when this broadcast is a deferred re-emission of a
        /// fresh-contract state that earlier found no targets and was stashed
        /// in `PendingBroadcastStore`, now re-driven because an interested
        /// peer appeared (issue #4359). The handler treats it like a fresh
        /// broadcast for fan-out, but must NOT re-record a `no_targets`
        /// propagation-summary event for it: the originating PUT already
        /// counted one `no_targets` when it first gave up, and counting again
        /// per flush would inflate the #4281 stats. `false` for executor-fresh
        /// and retry re-emissions.
        is_reemit: bool,
    },
    /// A V2 delegate wrote contract state; fan the change out to the network.
    ///
    /// DISPATCH: this ends in the same all-subscriber fan-out as
    /// [`Self::BroadcastStateChange`] — the handler resolves the state and
    /// hands it to `handle_broadcast_state_change`, which sends to EVERY
    /// advertised co-host of the contract. It is not targeted at one peer.
    ///
    /// Carries the contract id and NO state. The handler re-reads the current
    /// stored state when it drains, which gives two properties the
    /// state-carrying variant cannot:
    ///
    /// * **Queue cost is independent of state size.** A queued event is one
    ///   contract id, not a `WrappedState` of up to `MAX_STATE_SIZE`, so the
    ///   channel's message-count bound is also a bound on the bytes these
    ///   events retain.
    /// * **Pending broadcasts for one contract coalesce.** While a broadcast
    ///   is queued for a contract, further writes to it enqueue nothing (see
    ///   `OpManager::queue_v2_delegate_broadcast`), and the single drain
    ///   broadcasts whatever is stored by the time it runs — so a burst of
    ///   writes costs one fan-out, carrying the newest state rather than the
    ///   oldest.
    ///
    /// The re-read costs one contract-handler `GetQuery` per drained
    /// broadcast, which is why the coalescing matters: the cost is per
    /// drain, not per write. This mirrors the #4359 deferred-broadcast flush,
    /// which re-reads for the same stale-state reason.
    V2DelegateStateChanged {
        key: ContractKey,
    },
    /// Send state to a specific peer that reported a stale summary.
    /// Unlike BroadcastStateChange (which fans out to ALL subscribers),
    /// this targets only the peer that needs catching up.
    SyncStateToPeer {
        key: ContractKey,
        new_state: WrappedState,
        target: SocketAddr,
    },
    /// Nudge the node to consider migrating a contract we host toward a
    /// closer, non-hosting neighbor (directed-subscribe placement). Emitted
    /// best-effort when we begin hosting a contract or gain a new neighbor;
    /// handled in `p2p_protoc` where the connection table and version gate live.
    ConsiderContractMigration {
        key: ContractKey,
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
            NodeEvent::DropAllConnections => {
                write!(f, "DropAllConnections")
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
            NodeEvent::TransactionOrphaned { tx, peer } => {
                write!(f, "Transaction orphaned (tx: {tx}, peer: {peer})")
            }
            NodeEvent::LocalSubscribeComplete {
                tx,
                key,
                subscribed,
                ..
            } => {
                write!(
                    f,
                    "Local subscribe complete (tx: {tx}, key: {key}, subscribed: {subscribed})"
                )
            }
            NodeEvent::ExpectPeerConnection { addr } => {
                write!(f, "ExpectPeerConnection (from {addr})")
            }
            NodeEvent::BroadcastHostingUpdate { message } => {
                write!(f, "BroadcastHostingUpdate ({message:?})")
            }
            NodeEvent::BroadcastChangeInterests { added, removed } => {
                write!(
                    f,
                    "BroadcastChangeInterests (added: {}, removed: {})",
                    added.len(),
                    removed.len()
                )
            }
            NodeEvent::SendInterestMessage { target, message } => {
                let msg_summary = match message {
                    InterestMessage::Interests { hashes } => {
                        format!("Interests({} hashes)", hashes.len())
                    }
                    InterestMessage::Summaries { entries, emitter } => {
                        format!("Summaries({} entries, {emitter:?})", entries.len())
                    }
                    InterestMessage::ChangeInterests { added, removed } => {
                        format!(
                            "ChangeInterests(+{} -{} hashes)",
                            added.len(),
                            removed.len()
                        )
                    }
                    InterestMessage::ResyncRequest { key } => {
                        format!("ResyncRequest({key})")
                    }
                    InterestMessage::ResyncResponse {
                        key, state_bytes, ..
                    } => {
                        format!("ResyncResponse({key}, {} bytes)", state_bytes.len())
                    }
                    InterestMessage::SummaryDigests { entries, emitter } => {
                        format!("SummaryDigests({} entries, {emitter:?})", entries.len())
                    }
                    InterestMessage::SummaryRequest { hashes } => {
                        format!("SummaryRequest({} hashes)", hashes.len())
                    }
                };
                write!(f, "SendInterestMessage (to: {target}, {msg_summary})")
            }
            NodeEvent::SendNetMessage { target, msg } => {
                write!(f, "SendNetMessage (to: {target}, tx: {})", msg.id())
            }
            NodeEvent::BroadcastStateChange { key, .. } => {
                write!(f, "BroadcastStateChange (contract: {key})")
            }
            NodeEvent::V2DelegateStateChanged { key } => {
                write!(f, "V2DelegateStateChanged (contract: {key})")
            }
            NodeEvent::SyncStateToPeer { key, target, .. } => {
                write!(f, "SyncStateToPeer (contract: {key}, target: {target})")
            }
            NodeEvent::ConsiderContractMigration { key } => {
                write!(f, "ConsiderContractMigration (contract: {key})")
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
            NetMessageV1::NeighborHosting { .. } => Transaction::NULL,
            NetMessageV1::InterestSync { .. } => Transaction::NULL,
            NetMessageV1::ReadyState { .. } => Transaction::NULL,
            NetMessageV1::SubscribeHint(_) => Transaction::NULL,
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
            NetMessageV1::NeighborHosting { .. } => None,
            NetMessageV1::InterestSync { .. } => None,
            NetMessageV1::ReadyState { .. } => None,
            NetMessageV1::SubscribeHint(_) => None,
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
                NeighborHosting { message } => {
                    write!(f, "NeighborHosting {{ {message:?} }}")?;
                }
                InterestSync { message } => {
                    write!(f, "InterestSync {{ {message:?} }}")?;
                }
                ReadyState { ready } => {
                    write!(f, "ReadyState {{ ready: {ready} }}")?;
                }
                SubscribeHint(msg) => {
                    write!(f, "SubscribeHint(key: {}, holder: {})", msg.key, msg.holder)?;
                }
            },
        };
        write!(f, "}}")
    }
}

// ── Compile-time invariant checks ──────────────────────────────────────
//
// These const assertions catch layout and enum-variant assumptions at
// compile time, preventing a whole class of bugs that previously could
// only surface at runtime (or worse, as UB via unreachable_unchecked).

/// Transaction layout: Ulid (16 bytes) + Option<Ulid> (32 bytes) = 48 bytes.
/// `u128` has no niche, so `Option<Ulid>` cannot pack the discriminant and is
/// 32 bytes, not 24. Any change to this layout would break serialization
/// compatibility and network protocol.
const _: () = {
    // Ulid is a newtype over u128 (16 bytes).
    assert!(std::mem::size_of::<ulid::Ulid>() == 16, "Ulid size changed");
    // Transaction = { id: Ulid, parent: Option<Ulid> }.
    // Assert it stays within a reasonable bound (≤48 bytes).
    assert!(
        std::mem::size_of::<Transaction>() <= 48,
        "Transaction size grew beyond expected bounds — check serialization compatibility"
    );
};

/// TransactionType must have exactly 5 variants (0..=4).
/// If a new variant is added, `Transaction::transaction_type()` and the
/// `TryFrom<u8>` impl must be updated, and this assertion bumped.
const _: () = {
    // The highest valid discriminant must be 4 (Update).
    assert!(
        sealed_msg_type::TransactionType::Update as u8 == 4,
        "TransactionType variants changed — update TryFrom<u8> and this assertion"
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    /// The bincode variant index of every pre-existing `InterestMessage`
    /// variant, frozen (#4965).
    ///
    /// bincode encodes an enum variant as its POSITIONAL index, so inserting a
    /// variant anywhere but the end renumbers everything after it. That is not
    /// a compile error and not a deserialization error either — a peer on the
    /// old build would decode a `Summaries` as a `ChangeInterests` and act on
    /// garbage. This is the v0.2.11 incident class, and the whole reason the
    /// hash-first variants are APPENDED.
    ///
    /// bincode's default config writes the index as a little-endian u32, so
    /// the first four bytes of a serialized `InterestMessage` are its index.
    #[test]
    fn interest_message_wire_variant_indices_are_frozen() {
        use freenet_stdlib::prelude::CodeHash;

        fn variant_index(msg: &InterestMessage) -> u32 {
            let bytes = bincode::serialize(msg).expect("serialize InterestMessage");
            u32::from_le_bytes(bytes[..4].try_into().expect("variant index prefix"))
        }

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );

        // These five indices are on the wire of every released peer. Changing
        // any of them is a protocol break; a new variant goes at the END.
        assert_eq!(
            variant_index(&InterestMessage::Interests { hashes: vec![1] }),
            0,
            "Interests must stay variant 0"
        );
        assert_eq!(
            variant_index(&InterestMessage::Summaries {
                entries: vec![SummaryEntry {
                    hash: 1,
                    summary_bytes: Some(vec![9]),
                }],
                emitter: SummariesEmitter::Other,
            }),
            1,
            "Summaries must stay variant 1"
        );
        assert_eq!(
            variant_index(&InterestMessage::ChangeInterests {
                added: vec![1],
                removed: vec![],
            }),
            2,
            "ChangeInterests must stay variant 2"
        );
        assert_eq!(
            variant_index(&InterestMessage::ResyncRequest { key }),
            3,
            "ResyncRequest must stay variant 3"
        );
        assert_eq!(
            variant_index(&InterestMessage::ResyncResponse {
                key,
                state_bytes: vec![1],
                summary_bytes: vec![2],
            }),
            4,
            "ResyncResponse must stay variant 4"
        );

        // The hash-first additions occupy the next two slots. Pinned so a
        // future insertion above them is caught here rather than in
        // production: their indices are what
        // `HASH_FIRST_SUMMARIES_MIN_VERSION` gates on being decodable.
        assert_eq!(
            variant_index(&InterestMessage::SummaryDigests {
                entries: vec![],
                emitter: SummariesEmitter::Other,
            }),
            5,
            "SummaryDigests must stay variant 5 (appended, #4965)"
        );
        assert_eq!(
            variant_index(&InterestMessage::SummaryRequest { hashes: vec![] }),
            6,
            "SummaryRequest must stay variant 6 (appended, #4965)"
        );
    }

    /// The hash-first variants must survive a bincode round trip intact —
    /// including the fixed-size digest array, which is the one field whose
    /// encoding differs in kind from anything `SummaryEntry` carries.
    #[test]
    fn hash_first_variants_wire_roundtrip() {
        let digest = crate::ring::interest::summary_digest(b"a summary");
        let msg = InterestMessage::SummaryDigests {
            emitter: SummariesEmitter::Other,
            entries: vec![
                SummaryDigestEntry {
                    hash: 0xDEAD_BEEF,
                    summary_digest: Some(digest),
                },
                SummaryDigestEntry {
                    hash: 7,
                    summary_digest: None,
                },
            ],
        };
        let bytes = bincode::serialize(&msg).expect("serialize SummaryDigests");
        let decoded: InterestMessage =
            bincode::deserialize(&bytes).expect("deserialize SummaryDigests");
        #[allow(
            clippy::wildcard_enum_match_arm,
            reason = "a round-trip test asserts ONE variant; any other variant is a loud panic, not a silent fallthrough"
        )]
        match decoded {
            InterestMessage::SummaryDigests { entries, .. } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].hash, 0xDEAD_BEEF);
                assert_eq!(entries[0].summary_digest, Some(digest));
                assert_eq!(entries[1].hash, 7);
                assert_eq!(
                    entries[1].summary_digest, None,
                    "a peer with no state must round-trip as None, not as a \
                     zero digest — a zero digest would read as a real summary \
                     and could accidentally 'agree'"
                );
            }
            other => panic!("expected SummaryDigests, got {other:?}"),
        }

        let req = InterestMessage::SummaryRequest {
            hashes: vec![1, 2, 3],
        };
        let bytes = bincode::serialize(&req).expect("serialize SummaryRequest");
        let decoded: InterestMessage =
            bincode::deserialize(&bytes).expect("deserialize SummaryRequest");
        let InterestMessage::SummaryRequest { hashes } = &decoded else {
            panic!("expected SummaryRequest, got {decoded:?}");
        };
        assert_eq!(hashes, &vec![1, 2, 3]);
    }

    /// A `SummaryDigests` message must be dramatically smaller than the
    /// `Summaries` it replaces — that is the entire point (#4965), and a
    /// refactor that accidentally re-attached the bytes would otherwise pass
    /// every behavioural test in this PR while saving nothing.
    ///
    /// Sized against a River-scale summary (~33 KB measured in production).
    #[test]
    fn digest_form_is_orders_of_magnitude_smaller_than_full_bytes() {
        let summary = vec![0xABu8; 33 * 1024];
        let entry = SummaryEntry {
            hash: 42,
            summary_bytes: Some(summary),
        };
        let full = bincode::serialize(&InterestMessage::Summaries {
            entries: vec![entry.clone()],
            emitter: SummariesEmitter::Other,
        })
        .expect("serialize Summaries");
        let digests = bincode::serialize(&InterestMessage::SummaryDigests {
            entries: vec![SummaryDigestEntry::from_entry(&entry)],
            emitter: SummariesEmitter::Other,
        })
        .expect("serialize SummaryDigests");

        assert!(
            digests.len() < 64,
            "one digest entry should be a few dozen bytes, got {}",
            digests.len()
        );
        assert!(
            full.len() > 100 * digests.len(),
            "the digest form must be >100x smaller than the bytes form \
             ({} vs {} bytes) — if this fails, the digest is carrying the \
             summary and the wire change saves nothing",
            digests.len(),
            full.len()
        );
    }

    /// `SummaryDigestEntry::from_entry` must be a pure function of the entry it
    /// is derived from: same contract hash, and a digest that is exactly the
    /// digest of the bytes the fallback `Summaries` would have shipped.
    ///
    /// This is what makes the two wire forms interchangeable. If they could
    /// describe different state, a digest "match" would no longer prove the
    /// peer holds our summary, and the heal-suppression on agreement would be
    /// unsound.
    #[test]
    fn digest_entry_is_derived_from_the_bytes_it_replaces() {
        let bytes = vec![1u8, 2, 3, 4, 5];
        let entry = SummaryEntry {
            hash: 99,
            summary_bytes: Some(bytes.clone()),
        };
        let digest_entry = SummaryDigestEntry::from_entry(&entry);
        assert_eq!(digest_entry.hash, entry.hash);
        assert_eq!(
            digest_entry.summary_digest,
            Some(crate::ring::interest::summary_digest(&bytes))
        );

        let none_entry = SummaryEntry {
            hash: 99,
            summary_bytes: None,
        };
        assert_eq!(
            SummaryDigestEntry::from_entry(&none_entry).summary_digest,
            None,
            "'we hold no state' must stay distinguishable from any digest value"
        );
    }

    #[test]
    fn subscribe_hint_wire_roundtrip_and_version() {
        use freenet_stdlib::prelude::CodeHash;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([8u8; 32]),
        );
        let holder = PeerKeyLocation::random();
        // `ContractKey` is `Copy`; `holder` is not, so it is cloned.
        let msg = NetMessageV1::SubscribeHint(SubscribeHintMsg {
            key,
            holder: holder.clone(),
        });
        let bytes = bincode::serialize(&msg).expect("serialize SubscribeHint");
        let decoded: NetMessageV1 =
            bincode::deserialize(&bytes).expect("deserialize SubscribeHint");
        // `matches!` with a guard avoids a wildcard match arm over NetMessageV1.
        assert!(
            matches!(&decoded, NetMessageV1::SubscribeHint(m) if m.key == key && m.holder == holder),
            "SubscribeHint did not round-trip: {decoded:?}"
        );
        // Pin the per-variant entry in the NetMessageV1 version map so an
        // accidental reorder/bump is caught. (This map has no production
        // consumer today; the live wire-compat gate is the negotiated build
        // version vs SUBSCRIBE_HINT_MIN_VERSION.)
        assert_eq!(msg.version(), semver::Version::new(1, 3, 0));
    }

    #[test]
    fn pack_transaction_type() {
        let ts_0 = Ulid::generate();
        std::thread::sleep(Duration::from_millis(1));
        let tx = Transaction::update(TransactionType::Connect, Ulid::generate(), None);
        assert_eq!(tx.transaction_type(), TransactionType::Connect);
        let tx = Transaction::update(TransactionType::Subscribe, Ulid::generate(), None);
        assert_eq!(tx.transaction_type(), TransactionType::Subscribe);
        std::thread::sleep(Duration::from_millis(1));
        let ts_1 = Ulid::generate();
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

    #[test]
    fn ttl_transaction_with_multiplier_produces_older_cutoff() {
        let ttl_1x = Transaction::ttl_transaction();
        let ttl_5x = Transaction::ttl_transaction_with_multiplier(5);

        // 5x multiplier should produce an older (smaller timestamp) cutoff
        assert!(ttl_5x < ttl_1x, "5x multiplier should be older than 1x");

        // Verify the timestamp delta is approximately 4x OPERATION_TTL more
        let diff = ttl_1x.id.timestamp_ms() - ttl_5x.id.timestamp_ms();
        let expected = crate::config::OPERATION_TTL.as_millis() as u64 * 4;
        assert!(
            diff >= expected.saturating_sub(10) && diff <= expected + 10,
            "Timestamp delta should be ~4x OPERATION_TTL, got {diff}ms vs expected {expected}ms"
        );

        // multiplier(1) should be equivalent to ttl_transaction()
        let ttl_1x_via_multiplier = Transaction::ttl_transaction_with_multiplier(1);
        let diff_1x = ttl_1x
            .id
            .timestamp_ms()
            .abs_diff(ttl_1x_via_multiplier.id.timestamp_ms());
        assert!(
            diff_1x < 5,
            "multiplier(1) should be ~equivalent to ttl_transaction(), diff={diff_1x}ms"
        );
    }

    #[test]
    fn delta_or_full_state_delta_serialization_roundtrip() {
        use freenet_stdlib::prelude::StateDelta;

        let delta = StateDelta::from(vec![1, 2, 3, 4, 5]);
        let dofs = DeltaOrFullState::from_delta(&delta);

        // Serialize to bincode
        let serialized = bincode::serialize(&dofs).expect("serialize failed");

        // Deserialize back
        let deserialized: DeltaOrFullState =
            bincode::deserialize(&serialized).expect("deserialize failed");

        // Verify contents
        match &deserialized {
            DeltaOrFullState::Delta(bytes) => {
                assert_eq!(bytes, &vec![1, 2, 3, 4, 5]);
            }
            DeltaOrFullState::FullState(_) => panic!("expected Delta variant"),
        }

        // Verify to_delta works
        let recovered_delta = deserialized.to_delta().expect("should be delta");
        assert_eq!(recovered_delta.as_ref(), delta.as_ref());
    }

    #[test]
    fn delta_or_full_state_full_state_serialization_roundtrip() {
        use freenet_stdlib::prelude::State;

        let state = State::from(vec![10, 20, 30, 40, 50]);
        let dofs = DeltaOrFullState::from_state(&state);

        // Serialize to bincode
        let serialized = bincode::serialize(&dofs).expect("serialize failed");

        // Deserialize back
        let deserialized: DeltaOrFullState =
            bincode::deserialize(&serialized).expect("deserialize failed");

        // Verify contents
        match &deserialized {
            DeltaOrFullState::Delta(_) => panic!("expected FullState variant"),
            DeltaOrFullState::FullState(bytes) => {
                assert_eq!(bytes, &vec![10, 20, 30, 40, 50]);
            }
        }

        // Verify to_state works
        let recovered_state = deserialized.to_state().expect("should be full state");
        assert_eq!(recovered_state.as_ref(), state.as_ref());

        // Verify to_delta returns None for FullState
        assert!(deserialized.to_delta().is_none());
    }

    #[test]
    fn delta_or_full_state_conversion_methods() {
        use freenet_stdlib::prelude::{State, StateDelta};

        // Test from_delta
        let delta = StateDelta::from(vec![1, 2, 3]);
        let dofs = DeltaOrFullState::from_delta(&delta);
        assert!(matches!(dofs, DeltaOrFullState::Delta(_)));
        assert!(dofs.to_delta().is_some());
        assert!(dofs.to_state().is_none());

        // Test from_state
        let state = State::from(vec![4, 5, 6]);
        let dofs = DeltaOrFullState::from_state(&state);
        assert!(matches!(dofs, DeltaOrFullState::FullState(_)));
        assert!(dofs.to_delta().is_none());
        assert!(dofs.to_state().is_some());
    }

    #[test]
    fn delta_or_full_state_empty_data() {
        use freenet_stdlib::prelude::{State, StateDelta};

        // Empty delta
        let delta = StateDelta::from(Vec::<u8>::new());
        let dofs = DeltaOrFullState::from_delta(&delta);
        let serialized = bincode::serialize(&dofs).expect("serialize failed");
        let deserialized: DeltaOrFullState =
            bincode::deserialize(&serialized).expect("deserialize failed");
        assert!(matches!(deserialized, DeltaOrFullState::Delta(ref bytes) if bytes.is_empty()));

        // Empty state
        let state = State::from(Vec::<u8>::new());
        let dofs = DeltaOrFullState::from_state(&state);
        let serialized = bincode::serialize(&dofs).expect("serialize failed");
        let deserialized: DeltaOrFullState =
            bincode::deserialize(&serialized).expect("deserialize failed");
        assert!(matches!(deserialized, DeltaOrFullState::FullState(ref bytes) if bytes.is_empty()));
    }

    /// Verify SendInterestMessage Display produces compact output instead of
    /// dumping the full payload. This prevents regression to the 565KB-per-line
    /// log spam that caused 346MB/hr of gateway logs.
    #[test]
    fn test_send_interest_message_display_is_compact() {
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Summaries with large payload should show count, not bytes
        let summaries = NodeEvent::SendInterestMessage {
            target: addr,
            message: InterestMessage::Summaries {
                entries: vec![
                    SummaryEntry {
                        hash: 123,
                        summary_bytes: Some(vec![0u8; 10_000]),
                    },
                    SummaryEntry {
                        hash: 456,
                        summary_bytes: Some(vec![0u8; 10_000]),
                    },
                ],
                emitter: SummariesEmitter::InterestsReply,
            },
        };
        let display = format!("{summaries}");
        assert!(
            display.len() < 200,
            "Display should be compact, got {} bytes: {display}",
            display.len()
        );
        assert!(
            display.contains("Summaries(2 entries, InterestsReply)"),
            "Should show entry count and emitter: {display}"
        );

        // Interests should show hash count
        let interests = NodeEvent::SendInterestMessage {
            target: addr,
            message: InterestMessage::Interests {
                hashes: vec![1, 2, 3, 4, 5],
            },
        };
        let display = format!("{interests}");
        assert!(display.contains("Interests(5 hashes)"), "{display}");

        // ChangeInterests should show added/removed counts
        let changes = NodeEvent::SendInterestMessage {
            target: addr,
            message: InterestMessage::ChangeInterests {
                added: vec![1, 2],
                removed: vec![3],
            },
        };
        let display = format!("{changes}");
        assert!(
            display.contains("ChangeInterests(+2 -1 hashes)"),
            "{display}"
        );
    }

    /// The #5052 emitter tag must cost NOTHING on the wire and must not change
    /// what any peer decodes.
    ///
    /// This is the property that makes the whole attribution safe to ship into
    /// a mixed-version fleet, and `#[serde(skip)]` is the only thing enforcing
    /// it — delete the attribute and everything still compiles, every other
    /// test still passes, and `Summaries` silently grows a field that older
    /// peers cannot decode. Freenet has shipped exactly that bug before
    /// (v0.2.11, a protocol-enum change that broke pinned consumers), so the
    /// encoding is asserted directly rather than assumed:
    ///
    /// 1. two messages with identical entries but DIFFERENT tags encode to
    ///    identical bytes, and
    /// 2. those bytes are exactly what a tagless `Summaries` encodes to, so
    ///    the tag adds no discriminant byte either, and
    /// 3. decoding yields the `Default` tag, which is the residual arm —
    ///    an inbound message is unattributed, never mislabelled.
    #[test]
    fn summaries_emitter_tag_is_not_on_the_wire() {
        let entries = || {
            vec![SummaryEntry {
                hash: 0xDEAD_BEEF,
                summary_bytes: Some(vec![1, 2, 3, 4, 5]),
            }]
        };
        let tagged = |emitter| InterestMessage::Summaries {
            entries: entries(),
            emitter,
        };

        let notification =
            bincode::serialize(&tagged(SummariesEmitter::Notification)).expect("serialize");
        let interests_reply =
            bincode::serialize(&tagged(SummariesEmitter::InterestsReply)).expect("serialize");
        let residual = bincode::serialize(&tagged(SummariesEmitter::Other)).expect("serialize");

        assert_eq!(
            notification, interests_reply,
            "the emitter tag must not appear on the wire — two messages that \
             differ only by emitter must encode identically"
        );
        assert_eq!(
            notification, residual,
            "not even the Default tag may reach the wire"
        );

        // A sibling variant with the same payload shape is the control: it
        // shows the byte total above is a real `Summaries` encoding and not
        // some degenerate empty one, so assertion (1) has something to prove.
        let interests = bincode::serialize(&InterestMessage::Interests {
            hashes: vec![0xDEAD_BEEF],
        })
        .expect("serialize");
        assert_ne!(
            notification, interests,
            "sanity: Summaries and Interests must not encode identically"
        );

        let decoded: InterestMessage = bincode::deserialize(&notification).expect("deserialize");
        let InterestMessage::Summaries { entries, emitter } = &decoded else {
            panic!("expected Summaries, got {decoded:?}");
        };
        assert_eq!(entries.len(), 1, "payload must survive the round trip");
        assert_eq!(entries[0].hash, 0xDEAD_BEEF);
        assert_eq!(
            *emitter,
            SummariesEmitter::Other,
            "a decoded message carries no provenance, so it must land \
             in the residual arm rather than claim an emitter"
        );
    }

    #[test]
    fn test_send_net_message_display_includes_target_and_tx() {
        use std::net::SocketAddr;

        use crate::message::{NetMessageV1, Transaction};
        use crate::operations::connect::ConnectMsg;

        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let tx = Transaction::new::<ConnectMsg>();
        let net_msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::ConnectFailed {
            id: tx,
            failed_acceptor_addr: "10.0.0.1:1000".parse().unwrap(),
        }));

        let event = NodeEvent::SendNetMessage {
            target: addr,
            msg: Box::new(net_msg),
        };

        let display = format!("{event}");
        assert!(
            display.contains("SendNetMessage"),
            "should name event: {display}"
        );
        assert!(
            display.contains("127.0.0.1:9000"),
            "should include target: {display}"
        );
        assert!(
            display.contains(&tx.to_string()),
            "should include tx id: {display}"
        );
    }

    /// Wire and at-rest format pin for `Transaction` (#4882).
    ///
    /// `Transaction.id` is a `ulid::Ulid`, and `ulid`'s `Serialize` emits the
    /// 26-character Crockford base32 STRING, not the underlying `u128`. That
    /// encoding is inherited from a third-party crate, so a dependency bump can
    /// change it with no diff in this repo at all. If it ever flips to a raw
    /// `u128` — a plausible upstream change — every peer's `NetMessage` decode
    /// breaks and every existing `NetLogMessage` AOF segment
    /// (`tracing/aof.rs::encode_log`) becomes undecodable, while CI stays green:
    /// `Transaction`'s serde is derived structurally, and `NetMessageV1`
    /// versioning does not cover a nested field's representation.
    ///
    /// Verified byte-identical under ulid 1.2.1 and 3.0.0 during the 3.0 bump.
    ///
    /// This compares against FIXED bytes on purpose. A round-trip test would
    /// re-encode with the same version it decodes with, so it is self-consistent
    /// by construction and cannot detect this class of change.
    #[test]
    fn transaction_bincode_encoding_is_pinned() {
        const RAW: u128 = 0x0123_4567_89AB_CDEF_0123_4567_89AB_CDEF;
        const ENCODED: &str = "014D2PF2DBSQQG28T5CY4TQKFF";

        // The base32 alphabet and length the pin below is built on.
        assert_eq!(
            Ulid(RAW).to_string(),
            ENCODED,
            "ULID string encoding changed; the wire format changed with it"
        );

        fn expect_ulid_bytes(encoded: &str) -> Vec<u8> {
            let mut v = Vec::new();
            // bincode writes a str as a little-endian u64 length, then the bytes.
            v.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
            v.extend_from_slice(encoded.as_bytes());
            v
        }

        let mut expected = expect_ulid_bytes(ENCODED);
        expected.push(0); // Option::None

        let tx = Transaction {
            id: Ulid(RAW),
            parent: None,
        };
        let actual = bincode::serialize(&tx).expect("serialize Transaction");
        assert_eq!(
            actual, expected,
            "Transaction bincode encoding changed: this is a network protocol \
             break AND makes existing event-log segments undecodable"
        );
        assert_eq!(actual.len(), 35, "Transaction encodes to 8 + 26 + 1 bytes");

        // The parent arm too, since `Option<Ulid>` is the other half of the layout.
        let mut expected_parent = expect_ulid_bytes(ENCODED);
        expected_parent.push(1); // Option::Some
        expected_parent.extend_from_slice(&expect_ulid_bytes("00000000000000000000000001"));
        let tx = Transaction {
            id: Ulid(RAW),
            parent: Some(Ulid(1)),
        };
        assert_eq!(
            bincode::serialize(&tx).expect("serialize Transaction"),
            expected_parent,
            "Transaction parent encoding changed"
        );
    }
}
