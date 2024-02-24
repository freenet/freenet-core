use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use aes_gcm::Aes128Gcm;
use tokio::sync::mpsc;

use crate::{
    transport::{
        connection_handler::TransportError,
        packet_data,
        sent_packet_tracker::SentPacketTracker,
        symmetric_message::{self},
    },
    util::time_source::InstantTimeSrc,
};

pub(crate) type StreamBytes = Vec<u8>;

// TODO: measure the space overhead of SymmetricMessage::LongMessage since is likely less than 100
/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::LongMessage metadata
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 100;

// TODO: unit test
/// Handles sending a long message which is not being streamed,
/// streaming messages will be tackled differently, in the interim time before
/// the necessary changes are done to the codebase we will use this function
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_long_message(
    stream_id: u32,
    last_message_id: Arc<AtomicU32>,
    sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    remote_addr: SocketAddr,
    mut message: StreamBytes,
    outbound_symmetric_key: Aes128Gcm,
    mut sent_confirmed_recv: mpsc::Receiver<u32>,
    sent_tracker: Arc<parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>>,
) -> Result<(), TransportError> {
    let total_length_bytes = message.len() as u32;
    let mut total_messages = message.len() / MAX_DATA_SIZE;
    total_messages += if message.len() % MAX_DATA_SIZE == 0 {
        0
    } else {
        1
    };
    let mut sent_not_confirmed = HashSet::new();
    let mut sent_confirmed = 0;
    let mut confirm_receipts = Vec::new();
    let mut next_fragment_number = 1; // 1-indexed

    let mut msg_id = stream_id;
    loop {
        loop {
            match sent_confirmed_recv.try_recv() {
                Ok(idx) => {
                    if sent_not_confirmed.remove(&idx) {
                        sent_confirmed += 1;
                        confirm_receipts.push(idx);
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) if !sent_not_confirmed.is_empty() => {
                    // the receiver has been dropped, we should stop sending
                    return Err(TransportError::ConnectionClosed);
                }
                _ => break,
            }
        }

        let sent_so_far = sent_confirmed + sent_not_confirmed.len();
        if sent_so_far < total_messages {
            let mut rest = {
                if message.len() > MAX_DATA_SIZE {
                    message.split_off(MAX_DATA_SIZE)
                } else {
                    std::mem::take(&mut message)
                }
            };
            std::mem::swap(&mut message, &mut rest);
            next_fragment_number += 1;
            let idx = super::packet_sending(
                remote_addr,
                &sender,
                msg_id,
                &outbound_symmetric_key,
                std::mem::take(&mut confirm_receipts),
                symmetric_message::LongMessageFragment {
                    stream_id,
                    total_length_bytes: total_length_bytes as u64,
                    fragment_number: next_fragment_number,
                    payload: rest,
                },
                &sent_tracker,
            )
            .await?;
            if sent_so_far + 1 < total_messages {
                // there will be more packets send, so we need to increment the message id
                msg_id = last_message_id.fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            sent_not_confirmed.insert(idx);
            continue;
        }

        if message.is_empty() && sent_not_confirmed.is_empty() {
            break;
        }

        // we sent all messages (self.message is empty) but we still need to confirm all were received
        debug_assert!(message.is_empty());
        debug_assert!(!sent_not_confirmed.is_empty());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::KeyInit;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_send_long_message_success() {
        let (sender, _receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let message = vec![1, 2, 3, 4, 5];
        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());
        let (sent_confirmed_send, sent_confirmed_recv) = mpsc::channel(100);
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        let result = send_long_message(
            0,
            Arc::new(AtomicU32::new(0)),
            sender,
            remote_addr,
            message,
            cipher,
            sent_confirmed_recv,
            sent_tracker,
        )
        .await;

        assert!(result.is_ok());
        /*
        for (addr, bytes) in packets_sent.lock().iter() {
            let packet = packet_data::PacketData::decrypt(bytsamees.as_ref(), &cipher).unwrap();
            let message = symmetric_message::SymmetricMessage::deser(packet.data()).unwrap();
            assert_eq!(
                message.payload,
                symmetric_message::SymmetricMessagePayload::LongMessageFragment(
                    symmetric_message::LongMessageFragment {
                        stream_id: 0,
                        total_length_bytes: 20 * 1024,
                        fragment_number: 1,
                        payload: vec![0u8; MAX_DATA_SIZE],
                    }
                )
            );
        } */
    }

    // Add more tests here for other scenarios
}
