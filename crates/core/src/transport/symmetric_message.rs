use std::{borrow::Cow, sync::OnceLock};

use aes_gcm::Aes128Gcm;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::transport::packet_data::MAX_DATA_SIZE;

use super::{
    packet_data::MAX_PACKET_SIZE, peer_connection::StreamId, MessagePayload, PacketData, PacketId,
};

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(super) struct SymmetricMessage {
    // TODO: make sure we handle wrapping around the u32 properly
    pub packet_id: PacketId,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub confirm_receipt: Vec<PacketId>,
    pub payload: SymmetricMessagePayload,
}

impl SymmetricMessage {
    pub const FIRST_PACKET_ID: u32 = 0u32;

    pub fn deser(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    pub fn ack_error(outbound_sym_key: &Aes128Gcm) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(|| {
            let mut packet = [0u8; MAX_PACKET_SIZE];
            let size = bincode::serialized_size(&Self::ACK_ERROR).unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        // TODO: we need exact size of this packet so we can return an optimized PacketData
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
    }

    pub fn ack_ok(outbound_sym_key: &Aes128Gcm) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(move || {
            let mut packet = [0u8; MAX_PACKET_SIZE];
            let size = bincode::serialized_size(&SymmetricMessage {
                packet_id: Self::FIRST_PACKET_ID,
                confirm_receipt: vec![],
                payload: SymmetricMessagePayload::AckConnection { result: Ok(()) },
            })
            .unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        // TODO: we need exact size of this packet so we can return an optimized PacketData
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
    }

    pub fn ack_gateway_connection(
        outbound_sym_key: &Aes128Gcm,
        key: [u8; 16],
    ) -> Result<PacketData, bincode::Error> {
        let message = Self {
            packet_id: Self::FIRST_PACKET_ID,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::GatewayConnection { key },
        };
        let mut packet = [0u8; MAX_PACKET_SIZE];
        let size = bincode::serialized_size(&message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), &message)?;
        let bytes = &packet[..size as usize];
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
    }

    pub fn serialize_msg_to_packet_data(
        message_id: PacketId,
        payload: impl Into<SymmetricMessagePayload>,
        outbound_sym_key: &Aes128Gcm,
        confirm_receipt: Vec<u32>,
    ) -> Result<PacketData, bincode::Error> {
        let message = Self {
            packet_id: message_id,
            confirm_receipt,
            payload: payload.into(),
        };
        let mut packet = [0u8; MAX_PACKET_SIZE];
        let size = bincode::serialized_size(&message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), &message)?;
        let bytes = &packet[..size as usize];
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
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

pub(super) struct LongMessageFragment {
    pub stream_id: StreamId,
    pub total_length_bytes: u64,
    pub fragment_number: u32,
    pub payload: MessagePayload,
}

impl From<LongMessageFragment> for SymmetricMessagePayload {
    fn from(long_message_fragment: LongMessageFragment) -> Self {
        Self::LongMessageFragment {
            stream_id: long_message_fragment.stream_id,
            total_length_bytes: long_message_fragment.total_length_bytes,
            fragment_number: long_message_fragment.fragment_number,
            payload: long_message_fragment.payload,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) enum SymmetricMessagePayload {
    AckConnection {
        // if we successfully connected to a remote we attempt to connect to initially
        // then we return our TransportPublicKey so they can enroute other peers to us
        result: Result<(), Cow<'static, str>>,
    },
    GatewayConnection {
        // a gateway acknowledges a connection and returns the private key to use
        // for communication
        key: [u8; 16],
    },
    ShortMessage {
        payload: MessagePayload,
    },
    LongMessageFragment {
        stream_id: StreamId,
        total_length_bytes: u64, // we shouldn't allow messages larger than u32, that's already crazy big
        fragment_number: u32,
        payload: MessagePayload,
    },
    NoOp,
}

#[test]
fn ack_error_msg() -> Result<(), Box<dyn std::error::Error>> {
    use aes_gcm::KeyInit;
    let key = Aes128Gcm::new(&[0; 16].into());
    let packet = SymmetricMessage::ack_error(&key)?;

    let _packet = PacketData::<1000>::encrypt_symmetric(packet.data(), &key);

    let data = packet.decrypt(&key).unwrap();
    let deser = SymmetricMessage::deser(data.data())?;
    assert!(matches!(
        deser.payload,
        SymmetricMessagePayload::AckConnection { result: Err(_) }
    ));
    Ok(())
}
