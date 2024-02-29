use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use aes_gcm::Aes128Gcm;
use tokio::sync::mpsc;

use crate::transport::PacketId;
use crate::{
    transport::{
        packet_data,
        sent_packet_tracker::SentPacketTracker,
        symmetric_message::{self},
        TransportError,
    },
    util::time_source::InstantTimeSrc,
};

use super::StreamId;

pub(crate) type SerializedStream = Vec<u8>;

// TODO: measure the space overhead of SymmetricMessage::LongMessage since is likely less than 100
/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::LongMessage metadata
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 100;

// TODO: unit test
/// Handles sending a stream that is *not piped*. In the future this will be replaced by
/// piped streams which start forwarding before the stream has been received.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_stream(
    stream_id: StreamId,
    last_packet_id: Arc<AtomicU32>,
    sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    destination_addr: SocketAddr,
    mut stream_to_send: SerializedStream,
    outbound_symmetric_key: Aes128Gcm,
    mut confirmed_sent_packet_receiver: mpsc::Receiver<PacketId>,
    sent_packet_tracker: Arc<parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>>,
) -> Result<(), TransportError> {
    let total_length_bytes = stream_to_send.len() as u32;
    let mut total_packets = stream_to_send.len() / MAX_DATA_SIZE;
    total_packets += if stream_to_send.len() % MAX_DATA_SIZE == 0 {
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
            match confirmed_sent_packet_receiver.try_recv() {
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
        if sent_so_far < total_packets {
            let mut rest = {
                if stream_to_send.len() > MAX_DATA_SIZE {
                    stream_to_send.split_off(MAX_DATA_SIZE)
                } else {
                    std::mem::take(&mut stream_to_send)
                }
            };
            std::mem::swap(&mut stream_to_send, &mut rest);
            next_fragment_number += 1;
            let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::Release);
            super::packet_sending(
                destination_addr,
                &sender,
                packet_id,
                &outbound_symmetric_key,
                std::mem::take(&mut confirm_receipts),
                symmetric_message::StreamFragment {
                    stream_id,
                    total_length_bytes: total_length_bytes as u64,
                    fragment_number: next_fragment_number,
                    payload: rest,
                },
                &sent_packet_tracker,
            )
            .await?;
            sent_not_confirmed.insert(packet_id);
            continue;
        }

        if stream_to_send.is_empty() && sent_not_confirmed.is_empty() {
            break;
        }

        // we sent all packets (self.package is empty) but we still need to confirm all were received
        debug_assert!(stream_to_send.is_empty());
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
    async fn test_send_stream_success() {
        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let message = vec![1, 2, 3, 4, 5];
        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());
        let (sent_confirmed_send, sent_confirmed_recv) = mpsc::channel(100);
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        // todo: in a background task (tokio::spawn) listen to the send_stream

        // and in the main thread we collect from outbound_receiver
        // and when the backgound task completes call `.await`, we check that after decrypting all the inbound packets
        // the message is complete
        /*
        e.g.
        let background_task = tokio::task(async move {
            let result = send_stream(...).await;
            Ok(result)
        });

        let mut inbound_bytes = Vec::new();
        while let Some(packet) = outbound_receiver.recv().await {
            let descrypted_packet = PacketData:decrypt(packet);
            inbound_bytes.extend_from_slice(descrypted_packet);
        }

        let result = background_task.await.unwrap();

        assert_eq!(result, inbound_bytes);
        */

        // todo: in order to test this, we need to use the `sent_confirmed_send` to send a confirmation
        // that the packet was received
        let result = send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            outbound_sender,
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
