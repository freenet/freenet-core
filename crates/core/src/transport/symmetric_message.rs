use std::{borrow::Cow, sync::OnceLock};

use aes_gcm::Aes128Gcm;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::transport::packet_data::MAX_DATA_SIZE;

use super::{crypto::TransportPublicKey, packet_data::MAX_PACKET_SIZE, MessagePayload, PacketData};

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(super) struct SymmetricMessage {
    // todo: make sure we handle wrapping around the u16 properly
    pub message_id: u32,
    // todo: profile what is better here on average in the future
    // (vec, fixed array size of what given length etc.
    // #[serde_as(as = "Option<[_; 50]>")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub confirm_receipt: Vec<u32>,
    pub payload: SymmetricMessagePayload,
}

const FIRST_MESSAGE_ID: u32 = 0u32;

impl SymmetricMessage {
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
        // todo: we need exact size of this packet so we can return an optimized PacketData
        Ok(PacketData::encrypted_with_cipher(bytes, outbound_sym_key))
    }

    pub fn ack_ok(
        outbound_sym_key: &Aes128Gcm,
        pub_key: &TransportPublicKey,
    ) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(move || {
            let mut packet = [0u8; MAX_PACKET_SIZE];
            let size = bincode::serialized_size(&SymmetricMessage {
                message_id: FIRST_MESSAGE_ID,
                confirm_receipt: vec![],
                payload: SymmetricMessagePayload::AckConnection {
                    result: Ok(pub_key.clone()),
                },
            })
            .unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        // todo: we need exact size of this packet so we can return an optimized PacketData
        Ok(PacketData::encrypted_with_cipher(bytes, outbound_sym_key))
    }

    pub fn short_message(
        message_id: u32,
        payload: MessagePayload,
        outbound_sym_key: &Aes128Gcm,
        confirm_receipt: Vec<u32>,
    ) -> Result<PacketData, bincode::Error> {
        let message = Self {
            message_id,
            confirm_receipt,
            payload: SymmetricMessagePayload::ShortMessage { payload },
        };
        let mut packet = [0u8; MAX_PACKET_SIZE]; // todo: optimize this
        let size = bincode::serialized_size(&message)?;
        debug_assert!(size <= MAX_DATA_SIZE as u64);
        bincode::serialize_into(packet.as_mut_slice(), &message)?;
        let bytes = &packet[..size as usize];
        Ok(PacketData::encrypted_with_cipher(bytes, outbound_sym_key))
    }

    const ACK_ERROR: SymmetricMessage = SymmetricMessage {
        message_id: FIRST_MESSAGE_ID,
        confirm_receipt: Vec::new(),
        payload: SymmetricMessagePayload::AckConnection {
            // todo: change to return UnsupportedProtocolVersion
            result: Err(Cow::Borrowed(
                "remote is using a different protocol version",
            )),
        },
    };
}

#[derive(Serialize, Deserialize)]
pub(super) enum SymmetricMessagePayload {
    AckConnection {
        // if we successfully connected to a remote we attempt to connect to initially
        // then we return our TransportPublicKey so they can enroute other peers to us
        result: Result<TransportPublicKey, Cow<'static, str>>,
    },
    ShortMessage {
        payload: MessagePayload,
    },
    LongMessageFragment {
        total_length: u64,
        index: u64,
        payload: MessagePayload,
    },
}

#[test]
fn ack_error_msg() -> Result<(), Box<dyn std::error::Error>> {
    use aes_gcm::KeyInit;
    let key = Aes128Gcm::new(&[0; 16].into());
    let packet = SymmetricMessage::ack_error(&key)?;

    let _packet = PacketData::<1500>::encrypted_with_cipher(packet.data(), &key);

    let data = packet.decrypt(&key).unwrap();
    let deser = SymmetricMessage::deser(data.data())?;
    assert!(matches!(
        deser.payload,
        SymmetricMessagePayload::AckConnection { result: Err(_) }
    ));
    Ok(())
}
