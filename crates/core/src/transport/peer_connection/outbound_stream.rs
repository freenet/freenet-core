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
            let idx = super::send_packet_with_receipt_tracking(
                remote_addr,
                &sender,
                &last_message_id,
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
        let (sender, mut receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let message = vec![0u8; 20 * 1024];
        let message = message.into_iter().map(|_| rand::random::<u8>()).collect();
        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());
        let (sent_confirmed_send, sent_confirmed_recv) = mpsc::channel(100);

        let packets_sent = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let packets_sent_clone = packets_sent.clone();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        tokio::spawn(async move {
            loop {
                let packet = { receiver.recv().await.unwrap() };
                packets_sent_clone.lock().push(packet);
            }
        });

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
    }

    // Add more tests here for other scenarios
}
