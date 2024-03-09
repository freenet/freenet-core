use std::{borrow::Cow, net::SocketAddr, sync::OnceLock};

use aes_gcm::Aes128Gcm;
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

    pub fn ack_error(outbound_sym_key: &Aes128Gcm) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(|| {
            let mut packet = [0u8; MAX_DATA_SIZE];
            let size = bincode::serialized_size(&Self::ACK_ERROR).unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
    }

    const ACK_OK: SymmetricMessage = SymmetricMessage {
        packet_id: Self::FIRST_PACKET_ID,
        confirm_receipt: Vec::new(),
        payload: SymmetricMessagePayload::AckConnection { result: Ok(()) },
    };

    pub fn ack_ok(outbound_sym_key: &Aes128Gcm) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(move || {
            let mut packet = [0u8; MAX_DATA_SIZE];
            let size = bincode::serialized_size(&Self::ACK_OK).unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_OK).unwrap();
            (&packet[..size as usize]).into()
        });
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
    }

    pub fn ack_gateway_connection(
        outbound_sym_key: &Aes128Gcm,
        key: [u8; 16],
        remote_addr: SocketAddr,
    ) -> Result<PacketData, bincode::Error> {
        let message = Self {
            packet_id: Self::FIRST_PACKET_ID,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::GatewayConnection { key, remote_addr },
        };
        let mut packet = [0u8; MAX_DATA_SIZE];
        let size = bincode::serialized_size(&message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), &message)?;
        let bytes = &packet[..size as usize];
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
    }

    pub fn serialize_msg_to_packet_data(
        packet_id: PacketId,
        payload: impl Into<SymmetricMessagePayload>,
        outbound_sym_key: &Aes128Gcm,
        confirm_receipt: Vec<u32>,
    ) -> Result<PacketData, bincode::Error> {
        let message = Self {
            packet_id,
            confirm_receipt,
            payload: payload.into(),
        };
        let mut packet = [0u8; MAX_DATA_SIZE];
        let size = bincode::serialized_size(&message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), &message)?;
        let bytes = &packet[..size as usize];
        Ok(PacketData::encrypt_symmetric(bytes, outbound_sym_key))
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
        remote_addr: SocketAddr,
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
mod test {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::KeyInit;
    use rand::RngCore;

    use super::*;

    fn gen_key() -> Aes128Gcm {
        let mut key = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut key);
        let key = GenericArray::from_slice(&key);
        Aes128Gcm::new(key)
    }

    fn serialization_round_trip(
        payload: impl Into<SymmetricMessagePayload>,
        key: &Aes128Gcm,
    ) -> SymmetricMessagePayload {
        let enc_sym_packet =
            SymmetricMessage::serialize_msg_to_packet_data(1, payload, key, vec![]).unwrap();
        let dec_sym_packet = enc_sym_packet.decrypt(key).unwrap();
        assert_eq!(dec_sym_packet.data(), enc_sym_packet.data());
        SymmetricMessage::deser(dec_sym_packet.data())
            .unwrap()
            .payload
    }

    #[test]
    fn check_symmetric_message_serialization() {
        let key = gen_key();
        let payload = SymmetricMessagePayload::AckConnection { result: Ok(()) };
        let deserialized = serialization_round_trip(payload.clone(), &key);
        assert_eq!(deserialized, payload);

        let payload = SymmetricMessagePayload::AckConnection {
            result: Err(Cow::Borrowed("error")),
        };
        let deserialized = serialization_round_trip(payload.clone(), &key);
        assert_eq!(deserialized, payload);
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
        let enc = bincode::serialize(&SymmetricMessage::ACK_OK)?;
        let dec: SymmetricMessage = bincode::deserialize(&enc)?;
        assert_eq!(SymmetricMessage::ACK_OK, dec);

        let key = gen_key();
        let packet = SymmetricMessage::ack_ok(&key)?;
        let data = packet.decrypt(&key).unwrap();
        let deser = SymmetricMessage::deser(data.data())?;
        assert!(matches!(
            deser.payload,
            SymmetricMessagePayload::AckConnection { result: Ok(_) }
        ));
        Ok(())
    }
}
