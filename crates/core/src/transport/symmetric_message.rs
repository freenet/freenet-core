use std::{
    borrow::Cow,
    net::SocketAddr,
    sync::{LazyLock, OnceLock},
};

use bytes::Bytes;

use crate::transport::packet_data::SymmetricAES;
use aes_gcm::Aes128Gcm;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::{
    MessagePayload, PacketId, packet_data::MAX_DATA_SIZE, packet_data::PacketData,
    peer_connection::StreamId,
};

#[serde_as]
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(crate) struct SymmetricMessage {
    pub(super) packet_id: PacketId,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) confirm_receipt: Vec<PacketId>,
    pub(crate) payload: SymmetricMessagePayload,
}

impl SymmetricMessage {
    pub const FIRST_PACKET_ID: u32 = 0u32;

    pub fn deser(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    const ACK_ERROR_MSG: &str = concat!(
        "remote is using a different protocol version, expected version ",
        env!("CARGO_PKG_VERSION")
    );

    const ACK_ERROR: SymmetricMessage = SymmetricMessage {
        packet_id: Self::FIRST_PACKET_ID,
        confirm_receipt: Vec::new(),
        payload: SymmetricMessagePayload::AckConnection {
            // TODO: change to return UnsupportedProtocolVersion
            result: Err(Cow::Borrowed(Self::ACK_ERROR_MSG)),
        },
    };

    pub(crate) fn short_message_overhead() -> usize {
        thread_local! {
            static OVERHEAD: usize = {
                let blank = SymmetricMessage {
                    packet_id: u32::MAX,
                    confirm_receipt: vec![],
                    payload: SymmetricMessagePayload::ShortMessage { payload: Bytes::new() },
                };
                bincode::serialized_size(&blank).unwrap() as usize
            };
        }

        OVERHEAD.with(|o| *o)
    }

    pub(crate) fn noop_message_overhead() -> usize {
        thread_local! {
            static OVERHEAD: usize = {
                let blank = SymmetricMessage {
                    packet_id: u32::MAX,
                    confirm_receipt: vec![],
                    payload: SymmetricMessagePayload::NoOp,
                };
                bincode::serialized_size(&blank).unwrap() as usize
            };
        }

        OVERHEAD.with(|o| *o)
    }

    #[allow(dead_code)]
    pub(crate) fn stream_fragment_overhead() -> usize {
        static OVERHEAD: LazyLock<usize> = LazyLock::new(|| {
            let blank = SymmetricMessage {
                packet_id: u32::MAX,
                confirm_receipt: vec![],
                payload: SymmetricMessagePayload::StreamFragment {
                    stream_id: StreamId::next(),
                    total_length_bytes: u64::MAX,
                    fragment_number: u32::MAX,
                    payload: Bytes::new(),
                    metadata_bytes: None,
                },
            };
            bincode::serialized_size(&blank).unwrap() as usize
        });

        *OVERHEAD
    }

    pub(crate) fn max_num_of_confirm_receipts_of_noop_message() -> usize {
        static MAX_NUM_CONFIRM_RECEIPTS: LazyLock<usize> = LazyLock::new(|| {
            let overhead = SymmetricMessage::noop_message_overhead() as u64;
            let max_elems = (MAX_DATA_SIZE as u64 - overhead) / core::mem::size_of::<u32>() as u64;
            max_elems as usize
        });

        *MAX_NUM_CONFIRM_RECEIPTS
    }

    pub fn ack_error(
        outbound_sym_key: &Aes128Gcm,
    ) -> Result<PacketData<SymmetricAES>, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(|| {
            let mut packet = [0u8; MAX_DATA_SIZE];
            let size = bincode::serialized_size(&Self::ACK_ERROR).unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        let packet = PacketData::from_buf_plain(bytes);
        Ok(packet.encrypt_symmetric(outbound_sym_key))
    }

    pub fn ack_ok(
        outbound_sym_key: &Aes128Gcm,
        our_inbound_key: [u8; 16],
        remote_addr: SocketAddr,
    ) -> Result<PacketData<SymmetricAES>, bincode::Error> {
        let message = Self {
            packet_id: Self::FIRST_PACKET_ID,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::AckConnection {
                result: Ok(OutboundConnection {
                    key: our_inbound_key,
                    remote_addr,
                }),
            },
        };
        Self::serialize_ack(&message, outbound_sym_key)
    }

    /// Version-carrying form of [`Self::ack_ok`] (#5161).
    ///
    /// Emits [`SymmetricMessagePayload::AckConnectionV2`], which is
    /// byte-incompatible with a peer that does not carry that variant index —
    /// the caller MUST have established that the remote is at or above
    /// `GATEWAY_ACK_VERSION_MIN_VERSION` first. See
    /// `connection_handler::version_cmp::version_supports_ack_version`.
    ///
    /// `ack_ok` is deliberately left untouched rather than generalised to take
    /// an `Option<[u8; 8]>`: the pre-floor path must keep producing the exact
    /// bytes it produces today, and calling the same unmodified function is a
    /// stronger guarantee of that than any amount of branch review.
    /// `ack_ok_produces_identical_bytes_to_pre_5161_encoding` pins it anyway.
    pub fn ack_ok_with_version(
        outbound_sym_key: &Aes128Gcm,
        our_inbound_key: [u8; 16],
        remote_addr: SocketAddr,
        protoc_version: [u8; 8],
    ) -> Result<PacketData<SymmetricAES>, bincode::Error> {
        let message = Self {
            packet_id: Self::FIRST_PACKET_ID,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::AckConnectionV2 {
                connection: OutboundConnectionV2 {
                    key: our_inbound_key,
                    remote_addr,
                    protoc_version,
                },
            },
        };
        Self::serialize_ack(&message, outbound_sym_key)
    }

    fn serialize_ack(
        message: &Self,
        outbound_sym_key: &Aes128Gcm,
    ) -> Result<PacketData<SymmetricAES>, bincode::Error> {
        let mut packet = [0u8; MAX_DATA_SIZE];
        let size = bincode::serialized_size(message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), message)?;
        let bytes = &packet[..size as usize];

        let packet = PacketData::from_buf_plain(bytes);
        Ok(packet.encrypt_symmetric(outbound_sym_key))
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn try_serialize_msg_to_packet_data(
        packet_id: PacketId,
        payload: impl Into<SymmetricMessagePayload>,
        outbound_sym_key: &Aes128Gcm,
        confirm_receipt: Vec<u32>,
    ) -> Result<
        either::Either<PacketData<SymmetricAES>, (SymmetricMessagePayload, Vec<u32>)>,
        bincode::Error,
    > {
        let msg = Self {
            packet_id,
            confirm_receipt,
            payload: payload.into(),
        };

        let size = bincode::serialized_size(&msg)?;
        if size <= MAX_DATA_SIZE as u64 {
            let mut packet = [0u8; MAX_DATA_SIZE];
            bincode::serialize_into(packet.as_mut_slice(), &msg)?;
            let bytes = &packet[..size as usize];
            let packet = PacketData::from_buf_plain(bytes);
            Ok(either::Left(packet.encrypt_symmetric(outbound_sym_key)))
        } else {
            Ok(either::Right((msg.payload, msg.confirm_receipt)))
        }
    }

    pub fn serialize_msg_to_packet_data(
        packet_id: PacketId,
        payload: impl Into<SymmetricMessagePayload>,
        outbound_sym_key: &Aes128Gcm,
        confirm_receipt: Vec<u32>,
    ) -> Result<PacketData<SymmetricAES>, bincode::Error> {
        let message = Self {
            packet_id,
            confirm_receipt,
            payload: payload.into(),
        };

        message.to_packet_data(outbound_sym_key)
    }

    pub(crate) fn to_packet_data(
        &self,
        outbound_sym_key: &Aes128Gcm,
    ) -> Result<PacketData<SymmetricAES>, bincode::Error> {
        let mut packet = [0u8; MAX_DATA_SIZE];
        let size = bincode::serialized_size(self)?;
        if size > MAX_DATA_SIZE as u64 {
            return Err(Box::new(bincode::ErrorKind::Custom(format!(
                "Message size {} exceeds MAX_DATA_SIZE {}",
                size, MAX_DATA_SIZE
            ))));
        }
        bincode::serialize_into(packet.as_mut_slice(), self)?;
        let bytes = &packet[..size as usize];
        let packet = PacketData::from_buf_plain(bytes);
        Ok(packet.encrypt_symmetric(outbound_sym_key))
    }
}

impl From<()> for SymmetricMessagePayload {
    fn from(_: ()) -> Self {
        Self::NoOp {}
    }
}

pub(super) struct ShortMessage(pub MessagePayload);

#[cfg(test)]
impl From<Vec<u8>> for SymmetricMessagePayload {
    fn from(payload: Vec<u8>) -> Self {
        Self::ShortMessage {
            payload: Bytes::from(payload),
        }
    }
}

impl From<ShortMessage> for SymmetricMessagePayload {
    fn from(short_message: ShortMessage) -> Self {
        Self::ShortMessage {
            payload: short_message.0,
        }
    }
}

pub(super) struct StreamFragment {
    pub stream_id: StreamId,
    pub total_length_bytes: u64,
    pub fragment_number: u32,
    pub payload: MessagePayload,
    /// Serialized metadata NetMessage embedded in fragment #1 for reliability.
    /// If the separate metadata message (ResponseStreaming, RequestStreaming, etc.)
    /// is lost over UDP, the receiver can reconstruct it from this field.
    pub metadata_bytes: Option<MessagePayload>,
}

impl From<StreamFragment> for SymmetricMessagePayload {
    fn from(stream_fragment: StreamFragment) -> Self {
        Self::StreamFragment {
            stream_id: stream_fragment.stream_id,
            total_length_bytes: stream_fragment.total_length_bytes,
            fragment_number: stream_fragment.fragment_number,
            payload: stream_fragment.payload,
            metadata_bytes: stream_fragment.metadata_bytes,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(crate) struct OutboundConnection {
    pub(super) key: [u8; 16],
    pub(super) remote_addr: SocketAddr,
}

/// [`OutboundConnection`] plus the ACCEPTOR's protocol version (#5161).
///
/// The version is the same 8-byte `PROTOC_VERSION` wire encoding the intro
/// packet carries, so the receiver parses it with the identical
/// `version_cmp::parse_version_bytes` and gets `min_compatible` for free.
///
/// Why a parallel struct rather than a field on `OutboundConnection`: the
/// payload is bincode, whose field layout is not forward-tolerant, so the
/// version can only be sent to a peer already known to decode it. A plain new
/// field would have to be omitted conditionally, which bincode cannot express
/// without a hand-written `Serialize`; an `Option` would still emit its
/// discriminant byte and change the bytes a pre-floor peer sees. Leaving the
/// legacy type untouched makes the compatibility guarantee structural.
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(crate) struct OutboundConnectionV2 {
    pub(super) key: [u8; 16],
    pub(super) remote_addr: SocketAddr,
    /// Acceptor's `PROTOC_VERSION` bytes, in the intro packet's encoding.
    pub(super) protoc_version: [u8; 8],
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(crate) enum SymmetricMessagePayload {
    AckConnection {
        // a remote acknowledges a connection and returns the private key to use
        // for communication and the remote address
        result: Result<OutboundConnection, Cow<'static, str>>,
    },
    ShortMessage {
        payload: MessagePayload,
    },
    StreamFragment {
        stream_id: StreamId,
        total_length_bytes: u64, // we shouldn't allow messages larger than u32, that's already crazy big
        fragment_number: u32,
        payload: MessagePayload,
        /// Serialized metadata NetMessage embedded in fragment #1 for reliability.
        /// None for fragments other than #1. When present, the transport layer
        /// dispatches this as if a ShortMessage were received, so the operations
        /// layer processes it through the normal path.
        metadata_bytes: Option<MessagePayload>,
    },
    NoOp,
    /// Bidirectional liveness probe - sender expects a Pong response.
    /// Used to detect asymmetric connection failures where packets flow
    /// in only one direction.
    Ping {
        /// Sequence number to correlate with Pong response
        sequence: u64,
    },
    /// Response to a Ping, confirms bidirectional connectivity.
    Pong {
        /// Sequence number from the corresponding Ping
        sequence: u64,
    },
    /// Successful connection ack that ALSO carries the acceptor's protocol
    /// version (#5161).
    ///
    /// The legacy [`Self::AckConnection`] tells the joiner nothing about who it
    /// just connected to, so a node permanently treated every gateway — and
    /// every peer whose hole-punch raced ahead to the ack — as unknown-version,
    /// and every version-gated wire feature failed closed on exactly those
    /// links. The acceptor already parsed the joiner's intro packet before it
    /// builds the ack, so it knows whether the joiner can decode this variant;
    /// a joiner below `GATEWAY_ACK_VERSION_MIN_VERSION` still receives the
    /// byte-identical legacy `AckConnection`.
    ///
    /// MUST stay last: appending keeps every existing variant's bincode index
    /// unchanged, which is what makes the legacy encoding provably untouched.
    /// The failure case has no V2 form — an incompatible peer is rejected with
    /// the legacy `AckConnection { result: Err(..) }` (`ack_error`), which is
    /// correct precisely because such a peer may not carry this index.
    AckConnectionV2 {
        connection: OutboundConnectionV2,
    },
}

#[cfg(test)]
impl std::fmt::Display for SymmetricMessagePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SymmetricMessagePayload::AckConnection { result } => {
                write!(
                    f,
                    "AckConnection: {}",
                    result.as_ref().map(|_| "Ok").unwrap_or("Err")
                )
            }
            SymmetricMessagePayload::ShortMessage { .. } => {
                write!(f, "ShortMessage")
            }
            SymmetricMessagePayload::StreamFragment {
                stream_id,
                fragment_number,
                ..
            } => write!(
                f,
                "StreamFragment: (stream id: {stream_id:?}, fragment no: {fragment_number:?}) "
            ),
            SymmetricMessagePayload::NoOp => write!(f, "NoOp"),
            SymmetricMessagePayload::Ping { sequence } => write!(f, "Ping({sequence})"),
            SymmetricMessagePayload::Pong { sequence } => write!(f, "Pong({sequence})"),
            SymmetricMessagePayload::AckConnectionV2 { .. } => {
                write!(f, "AckConnectionV2")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use aes_gcm::KeyInit;

    use super::*;

    fn gen_key() -> Aes128Gcm {
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        Aes128Gcm::new(&key.into())
    }

    fn serialization_round_trip(
        payload: impl Into<SymmetricMessagePayload>,
        key: &Aes128Gcm,
    ) -> SymmetricMessagePayload {
        let enc_sym_packet =
            SymmetricMessage::serialize_msg_to_packet_data(1, payload, key, vec![]).unwrap();
        let dec_sym_packet = enc_sym_packet.decrypt(key).unwrap();
        SymmetricMessage::deser(dec_sym_packet.data())
            .unwrap()
            .payload
    }

    #[test]
    fn check_symmetric_message_serialization() {
        let test_cases = [
            SymmetricMessagePayload::AckConnection {
                result: Ok(OutboundConnection {
                    key: [0; 16],
                    remote_addr: (Ipv4Addr::LOCALHOST, 1234).into(),
                }),
            },
            SymmetricMessagePayload::AckConnection {
                result: Err(Cow::Borrowed("error")),
            },
            SymmetricMessagePayload::ShortMessage {
                payload: Bytes::from({
                    let mut buf = vec![0u8; 100];
                    crate::config::GlobalRng::fill_bytes(&mut buf);
                    buf
                }),
            },
            SymmetricMessagePayload::StreamFragment {
                stream_id: StreamId::next(),
                total_length_bytes: 100,
                fragment_number: 1,
                payload: Bytes::from({
                    let mut buf = vec![0u8; 100];
                    crate::config::GlobalRng::fill_bytes(&mut buf);
                    buf
                }),
                metadata_bytes: None,
            },
            SymmetricMessagePayload::StreamFragment {
                stream_id: StreamId::next(),
                total_length_bytes: 200,
                fragment_number: 1,
                payload: Bytes::from({
                    let mut buf = vec![0u8; 50];
                    crate::config::GlobalRng::fill_bytes(&mut buf);
                    buf
                }),
                metadata_bytes: Some(Bytes::from({
                    let mut buf = vec![0u8; 80];
                    crate::config::GlobalRng::fill_bytes(&mut buf);
                    buf
                })),
            },
            SymmetricMessagePayload::NoOp,
            SymmetricMessagePayload::Ping { sequence: 12345 },
            SymmetricMessagePayload::Pong { sequence: 12345 },
        ];
        let key = gen_key();

        for case in test_cases {
            let deserialized = serialization_round_trip(case.clone(), &key);
            assert_eq!(deserialized, case);
        }
    }

    #[test]
    fn ack_error_msg() -> Result<(), Box<dyn std::error::Error>> {
        let key = gen_key();
        let packet = SymmetricMessage::ack_error(&key)?;
        let data = packet.decrypt(&key).unwrap();
        let deser = SymmetricMessage::deser(data.data())?;
        assert!(matches!(
            deser.payload,
            SymmetricMessagePayload::AckConnection { result: Err(_) }
        ));
        Ok(())
    }

    #[test]
    fn ack_ok_msg() -> Result<(), Box<dyn std::error::Error>> {
        let enc = bincode::serialize(&SymmetricMessage {
            packet_id: SymmetricMessage::FIRST_PACKET_ID,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::AckConnection {
                result: Ok(OutboundConnection {
                    key: [0; 16],
                    remote_addr: (Ipv4Addr::LOCALHOST, 1234).into(),
                }),
            },
        })?;
        let _dec: SymmetricMessage = bincode::deserialize(&enc)?;

        let key = gen_key();
        let packet = SymmetricMessage::ack_ok(&key, [0; 16], (Ipv4Addr::LOCALHOST, 1234).into())?;
        let data = packet.decrypt(&key).unwrap();
        let deser = SymmetricMessage::deser(data.data())?;
        assert!(matches!(
            deser.payload,
            SymmetricMessagePayload::AckConnection { result: Ok(_) }
        ));
        Ok(())
    }

    /// **The #5161 backward-compatibility guarantee, asserted on bytes.**
    ///
    /// A joiner below `GATEWAY_ACK_VERSION_MIN_VERSION` must receive the exact
    /// ack it receives today. `ack_ok` is what it receives, so this pins
    /// `ack_ok`'s plaintext for a fixed input against a golden literal captured
    /// from the pre-#5161 encoding.
    ///
    /// A golden literal rather than a re-serialization of the same struct: the
    /// latter would compare the encoder against itself and pass even if the
    /// wire layout moved wholesale. This fails if ANYTHING shifts the legacy
    /// bytes — a field added to `OutboundConnection`, a reordering of
    /// `SymmetricMessage`, a bincode config change, or a variant inserted
    /// before `AckConnection` in `SymmetricMessagePayload`.
    #[test]
    fn ack_ok_produces_identical_bytes_to_pre_5161_encoding() {
        // packet_id=0 (u32 LE) | confirm_receipt len=0 (u64 LE)
        // | payload variant 0 = AckConnection (u32 LE)
        // | Result variant 0 = Ok (u32 LE)
        // | key: 16 raw bytes
        // | remote_addr: SocketAddr variant 0 = V4 (u32 LE), 4 octets, port (u16 LE)
        const PRE_5161_ACK_OK_PLAINTEXT: &[u8] = &[
            0, 0, 0, 0, // packet_id
            0, 0, 0, 0, 0, 0, 0, 0, // confirm_receipt: empty vec
            0, 0, 0, 0, // SymmetricMessagePayload::AckConnection
            0, 0, 0, 0, // Result::Ok
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // key
            0, 0, 0, 0, // SocketAddr::V4
            127, 0, 0, 1, // 127.0.0.1
            210, 4, // port 1234
        ];

        let key = gen_key();
        let packet = SymmetricMessage::ack_ok(
            &key,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            (Ipv4Addr::LOCALHOST, 1234).into(),
        )
        .unwrap();
        let plaintext = packet.decrypt(&key).unwrap();

        assert_eq!(
            plaintext.data(),
            PRE_5161_ACK_OK_PLAINTEXT,
            "the legacy ack encoding MOVED. A pre-floor joiner would no longer receive \
             byte-identical traffic, which is the whole compatibility guarantee of #5161."
        );
    }

    /// Appending `AckConnectionV2` must not renumber any existing variant.
    ///
    /// If it did, the compatibility guarantee above would hold for `ack_ok`
    /// alone while every OTHER message silently changed meaning on the wire —
    /// a peer would read a `ShortMessage` as a `StreamFragment`. Reads the
    /// variant index straight out of the serialized bytes, at the fixed offset
    /// after `packet_id` (u32) and the empty `confirm_receipt` length (u64).
    #[test]
    fn appending_ack_connection_v2_did_not_renumber_existing_variants() {
        const VARIANT_INDEX_OFFSET: usize = 4 + 8;

        fn variant_index(payload: SymmetricMessagePayload) -> u32 {
            let bytes = bincode::serialize(&SymmetricMessage {
                packet_id: 0,
                confirm_receipt: vec![],
                payload,
            })
            .unwrap();
            u32::from_le_bytes(
                bytes[VARIANT_INDEX_OFFSET..VARIANT_INDEX_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            )
        }

        let expected: [(u32, SymmetricMessagePayload); 7] = [
            (
                0,
                SymmetricMessagePayload::AckConnection {
                    result: Err(Cow::Borrowed("e")),
                },
            ),
            (
                1,
                SymmetricMessagePayload::ShortMessage {
                    payload: Bytes::new(),
                },
            ),
            (
                2,
                SymmetricMessagePayload::StreamFragment {
                    stream_id: StreamId::next(),
                    total_length_bytes: 1,
                    fragment_number: 0,
                    payload: Bytes::new(),
                    metadata_bytes: None,
                },
            ),
            (3, SymmetricMessagePayload::NoOp),
            (4, SymmetricMessagePayload::Ping { sequence: 0 }),
            (5, SymmetricMessagePayload::Pong { sequence: 0 }),
            (
                6,
                SymmetricMessagePayload::AckConnectionV2 {
                    connection: OutboundConnectionV2 {
                        key: [0; 16],
                        remote_addr: (Ipv4Addr::LOCALHOST, 1).into(),
                        protoc_version: [0; 8],
                    },
                },
            ),
        ];

        for (index, payload) in expected {
            assert_eq!(
                variant_index(payload),
                index,
                "SymmetricMessagePayload variant indices are wire-visible and MUST NOT move; \
                 AckConnectionV2 has to stay appended at the end"
            );
        }
    }

    /// The version-carrying ack round-trips, and is a DIFFERENT encoding from
    /// the legacy one for the same connection parameters — i.e. the gate that
    /// chooses between them is choosing between genuinely different bytes, not
    /// decorating an unchanged message.
    #[test]
    fn ack_ok_with_version_round_trips_and_differs_from_legacy() {
        let key = gen_key();
        let sym_key = [9u8; 16];
        let addr = (Ipv4Addr::LOCALHOST, 4321).into();
        let version = [0xFF, 0, 2, 0, 120, 0, 80, 1];

        let legacy = SymmetricMessage::ack_ok(&key, sym_key, addr).unwrap();
        let versioned =
            SymmetricMessage::ack_ok_with_version(&key, sym_key, addr, version).unwrap();

        let legacy_plain = legacy.decrypt(&key).unwrap();
        let versioned_plain = versioned.decrypt(&key).unwrap();
        assert_ne!(legacy_plain.data(), versioned_plain.data());

        let deser = SymmetricMessage::deser(versioned_plain.data()).unwrap();
        match deser.payload {
            SymmetricMessagePayload::AckConnectionV2 { connection } => {
                assert_eq!(connection.key, sym_key);
                assert_eq!(connection.remote_addr, addr);
                assert_eq!(connection.protoc_version, version);
            }
            other => panic!("expected AckConnectionV2, got {other}"),
        }
        assert_eq!(deser.packet_id, SymmetricMessage::FIRST_PACKET_ID);
    }

    #[test]
    fn max_confirm_receipts_of_noop_message() {
        let num = SymmetricMessage::max_num_of_confirm_receipts_of_noop_message();

        let msg = SymmetricMessage {
            packet_id: u32::MAX,
            confirm_receipt: vec![u32::MAX; num],
            payload: SymmetricMessagePayload::NoOp,
        };
        let size = bincode::serialized_size(&msg).unwrap();
        // MAX_DATA_SIZE was reduced by 1 byte for packet type discrimination.
        // Due to alignment/rounding, the message may not perfectly fill MAX_DATA_SIZE,
        // but it should be close and definitely not exceed it.
        assert!(
            size <= MAX_DATA_SIZE as u64,
            "Message size {} exceeds MAX_DATA_SIZE {}",
            size,
            MAX_DATA_SIZE
        );
        assert!(
            size >= (MAX_DATA_SIZE - 4) as u64,
            "Message size {} is too far from MAX_DATA_SIZE {} (off by more than 4 bytes)",
            size,
            MAX_DATA_SIZE
        );
    }

    #[test]
    fn max_short_message() {
        let overhead = SymmetricMessage::short_message_overhead();

        let msg = SymmetricMessage {
            packet_id: u32::MAX,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::ShortMessage {
                payload: Bytes::from(vec![0u8; MAX_DATA_SIZE - overhead]),
            },
        };
        let size = bincode::serialized_size(&msg).unwrap();
        assert_eq!(size, MAX_DATA_SIZE as u64);
    }

    #[test]
    fn measure_stream_fragment_overhead() {
        let overhead = SymmetricMessage::stream_fragment_overhead();

        // The perf docs assumed 100 bytes overhead, but let's measure actual
        println!("StreamFragment overhead: {} bytes", overhead);

        // Verify it fits in MAX_DATA_SIZE
        let msg = SymmetricMessage {
            packet_id: u32::MAX,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::StreamFragment {
                stream_id: StreamId::next(),
                total_length_bytes: u64::MAX,
                fragment_number: u32::MAX,
                payload: Bytes::from(vec![0u8; MAX_DATA_SIZE - overhead]),
                metadata_bytes: None,
            },
        };
        let size = bincode::serialized_size(&msg).unwrap();
        assert_eq!(size, MAX_DATA_SIZE as u64);

        // Verify our assumption: overhead should be much less than 100
        assert!(
            overhead < 100,
            "Overhead is {} bytes, expected < 100",
            overhead
        );
    }
}
