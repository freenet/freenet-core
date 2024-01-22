use std::{borrow::Cow, sync::OnceLock};

use aes_gcm::Aes128Gcm;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::{packet_data::MAX_PACKET_SIZE, PacketData};

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(super) struct SymmetricMessage {
    // todo: make sure we handle wrapping around the u16 properly
    pub message_id: u16,
    #[serde_as(as = "Option<[_; 50]>")]
    pub confirm_receipt: Option<[u16; 50]>,
    pub payload: SymmetricMessagePayload,
}

impl SymmetricMessage {
    pub fn deser(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    pub fn ack_error(outbound_sym_key: &mut Aes128Gcm) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(|| {
            let mut packet = [0u8; MAX_PACKET_SIZE];
            let size = bincode::serialized_size(&Self::ACK_ERROR).unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        Ok(PacketData::encrypted_with_cipher(bytes, outbound_sym_key))
    }

    pub fn ack_ok(outbound_sym_key: &mut Aes128Gcm) -> Result<PacketData, bincode::Error> {
        static SERIALIZED: OnceLock<Box<[u8]>> = OnceLock::new();
        let bytes = SERIALIZED.get_or_init(|| {
            let mut packet = [0u8; MAX_PACKET_SIZE];
            let size = bincode::serialized_size(&Self::ACK_OK).unwrap();
            bincode::serialize_into(packet.as_mut_slice(), &Self::ACK_ERROR).unwrap();
            (&packet[..size as usize]).into()
        });
        Ok(PacketData::encrypted_with_cipher(bytes, outbound_sym_key))
    }

    const ACK_ERROR: SymmetricMessage = SymmetricMessage {
        message_id: 0,
        confirm_receipt: None,
        payload: SymmetricMessagePayload::AckConnection {
            result: Err(Cow::Borrowed(
                "remote is using a different protocol version",
            )),
        },
    };

    const ACK_OK: SymmetricMessage = SymmetricMessage {
        message_id: 0,
        confirm_receipt: None,
        payload: SymmetricMessagePayload::AckConnection { result: Ok(()) },
    };
}

#[derive(Serialize, Deserialize)]
pub(super) enum SymmetricMessagePayload {
    AckConnection {
        result: Result<(), Cow<'static, str>>,
    },
    ShortMessage {
        payload: Vec<u8>,
    },
}

#[test]
fn ack_error_msg() -> Result<(), Box<dyn std::error::Error>> {
    use aes_gcm::KeyInit;
    let mut key = Aes128Gcm::new(&[0; 16].into());
    let packet = SymmetricMessage::ack_error(&mut key)?;

    let _packet = PacketData::<{ 1501 }>::encrypted_with_cipher(packet.send_data(), &mut key);

    let data = packet.decrypt(&mut key).unwrap();
    let deser = SymmetricMessage::deser(data.send_data())?;
    assert!(matches!(
        deser.payload,
        SymmetricMessagePayload::AckConnection { result: Err(_) }
    ));
    Ok(())
}
