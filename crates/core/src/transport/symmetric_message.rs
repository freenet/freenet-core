use std::{borrow::Cow, net::SocketAddr, sync::OnceLock};

use crate::transport::packet_data::SymmetricAES;
use aes_gcm::Aes128Gcm;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::{
    packet_data::MAX_DATA_SIZE, peer_connection::StreamId, MessagePayload, PacketData, PacketId,
};

#[serde_as]
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(super) struct SymmetricMessage {
    pub packet_id: PacketId,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    pub confirm_receipt: Vec<PacketId>,
    pub payload: SymmetricMessagePayload,
}

impl SymmetricMessage {
    pub const FIRST_PACKET_ID: u32 = 0u32;

    pub fn deser(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    const ACK_ERROR: SymmetricMessage = SymmetricMessage {
        packet_id: Self::FIRST_PACKET_ID,
        confirm_receipt: Vec::new(),
        payload: SymmetricMessagePayload::AckConnection {
            // TODO: change to return UnsupportedProtocolVersion
            result: Err(Cow::Borrowed(
                "remote is using a different protocol version",
            )),
        },
    };

    pub(crate) fn short_message_overhead() -> usize {
        static OVERHEAD: Lazy<usize> = Lazy::new(|| {
            let blank = SymmetricMessage {
                packet_id: u32::MAX,
                confirm_receipt: vec![],
                payload: SymmetricMessagePayload::ShortMessage { payload: vec![] },
            };
            bincode::serialized_size(&blank).unwrap() as usize
        });

        *OVERHEAD
    }

    pub(crate) fn noop_message_overhead() -> usize {
        static OVERHEAD: Lazy<usize> = Lazy::new(|| {
            let blank = SymmetricMessage {
                packet_id: u32::MAX,
                confirm_receipt: vec![],
                payload: SymmetricMessagePayload::NoOp,
            };
            bincode::serialized_size(&blank).unwrap() as usize
        });

        *OVERHEAD
    }

    pub(crate) fn max_num_of_confirm_receipts_of_noop_message() -> usize {
        static MAX_NUM_CONFIRM_RECEIPTS: Lazy<usize> = Lazy::new(|| {
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
        let mut packet = [0u8; MAX_DATA_SIZE];
        let size = bincode::serialized_size(&message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), &message)?;
        let bytes = &packet[..size as usize];

        let packet = PacketData::from_buf_plain(bytes);
        Ok(packet.encrypt_symmetric(outbound_sym_key))
    }

    #[allow(clippy::type_complexity)]
    pub fn try_serialize_msg_to_packet_data(
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
        debug_assert!(size <= MAX_DATA_SIZE as u64);
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
}

impl From<StreamFragment> for SymmetricMessagePayload {
    fn from(stream_fragment: StreamFragment) -> Self {
        Self::StreamFragment {
            stream_id: stream_fragment.stream_id,
            total_length_bytes: stream_fragment.total_length_bytes,
            fragment_number: stream_fragment.fragment_number,
            payload: stream_fragment.payload,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(super) struct OutboundConnection {
    pub key: [u8; 16],
    pub remote_addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Debug, Clone))]
pub(super) enum SymmetricMessagePayload {
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
    },
    NoOp,
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
                "StreamFragment: (stream id: {:?}, fragment no: {:?}) ",
                stream_id, fragment_number
            ),
            SymmetricMessagePayload::NoOp => write!(f, "NoOp"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use aes_gcm::KeyInit;

    use super::*;

    fn gen_key() -> Aes128Gcm {
        let key = rand::random::<[u8; 16]>();
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
                payload: std::iter::repeat(())
                    .take(100)
                    .map(|_| rand::random::<u8>())
                    .collect(),
            },
            SymmetricMessagePayload::StreamFragment {
                stream_id: StreamId::next(),
                total_length_bytes: 100,
                fragment_number: 1,
                payload: std::iter::repeat(())
                    .take(100)
                    .map(|_| rand::random::<u8>())
                    .collect(),
            },
            SymmetricMessagePayload::NoOp,
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

    #[test]
    fn max_confirm_receipts_of_noop_message() {
        let num = SymmetricMessage::max_num_of_confirm_receipts_of_noop_message();

        let msg = SymmetricMessage {
            packet_id: u32::MAX,
            confirm_receipt: vec![u32::MAX; num],
            payload: SymmetricMessagePayload::NoOp,
        };
        let size = bincode::serialized_size(&msg).unwrap();
        assert_eq!(size, MAX_DATA_SIZE as u64);
    }

    #[test]
    fn max_short_message() {
        let overhead = SymmetricMessage::short_message_overhead();

        let msg = SymmetricMessage {
            packet_id: u32::MAX,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::ShortMessage {
                payload: vec![0; MAX_DATA_SIZE - overhead],
            },
        };
        let size = bincode::serialized_size(&msg).unwrap();
        assert_eq!(size, MAX_DATA_SIZE as u64);
    }
}
