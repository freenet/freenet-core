use std::borrow::Cow;

use aes_gcm::Aes128Gcm;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::{connection_handler::MAX_PACKET_SIZE, PacketData};

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

    pub fn ack_error(outbound_sym_key: &Aes128Gcm) -> Result<PacketData, bincode::Error> {
        let msg = SymmetricMessage {
            message_id: 0,
            confirm_receipt: None,
            payload: SymmetricMessagePayload::AckConnection {
                result: Err("remote is using a different protocol version".into()),
            },
        };
        let mut packet = [0u8; MAX_PACKET_SIZE];
        bincode::serialize_into(packet.as_mut_slice(), &msg)?;
        Ok(PacketData::encrypted_with_cipher(
            packet,
            packet.len(),
            outbound_sym_key,
        ))
    }
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
    let key = Aes128Gcm::new(&[0; 16].into());
    let packet = SymmetricMessage::ack_error(&key)?;
    let data = PacketData::decrypt(packet.send_data(), &key).unwrap();
    let deser = SymmetricMessage::deser(&data)?;
    assert!(matches!(
        deser.payload,
        SymmetricMessagePayload::AckConnection { result: Err(_) }
    ));
    Ok(())
}
