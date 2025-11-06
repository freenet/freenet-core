use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec;

use aes_gcm::Aes128Gcm;
use tokio::sync::mpsc;

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
    sent_packet_tracker: Arc<parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>>,
    bandwidth_limit: Option<usize>,
) -> Result<(), TransportError> {
    tracing::debug!(stream_id = %stream_id.0, length = stream_to_send.len(), ?bandwidth_limit, "sending stream");
    let total_length_bytes = stream_to_send.len() as u32;
    let total_packets = stream_to_send.len().div_ceil(MAX_DATA_SIZE);
    let mut sent_so_far = 0;
    let mut next_fragment_number = 1; // Fragment numbers are 1-indexed

    // Calculate packets per batch based on bandwidth limit
    // NOTE: This is a temporary, simple rate limiting implementation for large data transfers.
    // It uses a fixed 10ms batch window to avoid issues with very short sleep times
    // (sub-millisecond sleeps can be unreliable). This approach sends bursts of packets
    // followed by a sleep to maintain the average bandwidth limit.
    // TODO: Replace with a more sophisticated rate limiting mechanism that:
    //   - Implements proper flow control and congestion avoidance
    //   - Provides fairness between different streams
    //   - Adapts to network conditions
    //   - Uses token bucket or leaky bucket algorithm
    let packets_per_batch = if let Some(limit) = bandwidth_limit {
        // 10ms batch window - chosen as a balance between responsiveness and reliability
        const BATCH_WINDOW_MS: f64 = 10.0;
        // Calculate bytes allowed in batch window
        let bytes_per_batch = (limit as f64 * BATCH_WINDOW_MS / 1000.0) as usize;
        // Calculate packets per batch (with some margin for overhead)
        let packets = bytes_per_batch / MAX_DATA_SIZE;
        // At least 1 packet per batch to avoid stalling
        packets.max(1)
    } else {
        // No limit, send all at once
        usize::MAX
    };

    let mut packets_in_current_batch = 0;
    let mut batch_start = tokio::time::Instant::now();

    loop {
        if sent_so_far == total_packets {
            break;
        }

        // Check if we need to rate limit
        if let Some(_limit) = bandwidth_limit {
            if packets_in_current_batch >= packets_per_batch {
                // We've sent enough packets in this batch, wait for the remainder of the 10ms window
                let elapsed = batch_start.elapsed();
                if elapsed < tokio::time::Duration::from_millis(10) {
                    let sleep_time = tokio::time::Duration::from_millis(10) - elapsed;
                    tracing::trace!(stream_id = %stream_id.0, ?sleep_time, packets_sent = packets_in_current_batch, "rate limiting, sleeping");
                    tokio::time::sleep(sleep_time).await;
                }
                // Reset for next batch
                packets_in_current_batch = 0;
                batch_start = tokio::time::Instant::now();
            }
        }

        let rest = {
            if stream_to_send.len() > MAX_DATA_SIZE {
                let mut rest = stream_to_send.split_off(MAX_DATA_SIZE);
                std::mem::swap(&mut stream_to_send, &mut rest);
                rest
            } else {
                std::mem::take(&mut stream_to_send)
            }
        };
        let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::Release);
        super::packet_sending(
            destination_addr,
            &sender,
            packet_id,
            &outbound_symmetric_key,
            vec![],
            symmetric_message::StreamFragment {
                stream_id,
                total_length_bytes: total_length_bytes as u64,
                fragment_number: next_fragment_number,
                payload: rest,
            },
            &sent_packet_tracker,
        )
        .await?;
        next_fragment_number += 1;
        sent_so_far += 1;
        packets_in_current_batch += 1;
    }

    // tracing::trace!(stream_id = %stream_id.0, total_packets = %sent_so_far, "stream sent");

    Ok(())
}

#[cfg(test)]
mod tests {
    use aes_gcm::KeyInit;
    use std::net::Ipv4Addr;
    use std::time::Instant;
    use tests::packet_data::MAX_PACKET_SIZE;
    use tracing::debug;

    use super::{
        symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
        *,
    };
    use crate::transport::packet_data::PacketData;

    #[tokio::test]
    async fn test_send_stream_success() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message: Vec<_> = std::iter::repeat(())
            .take(100_000)
            .map(|_| rand::random::<u8>())
            .collect();
        let cipher = {
            let key = rand::random::<[u8; 16]>();
            Aes128Gcm::new(&key.into())
        };
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        let background_task = tokio::spawn(send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            outbound_sender,
            remote_addr,
            message.clone(),
            cipher.clone(),
            sent_tracker,
            None, // No bandwidth limit for existing test
        ));

        let mut inbound_bytes = Vec::new();
        while let Some((_, packet)) = outbound_receiver.recv().await {
            let decrypted_packet = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet.as_ref())
                .try_decrypt_sym(&cipher)
                .map_err(|e| e.to_string())?;
            let deserialized = SymmetricMessage::deser(decrypted_packet.data())?;
            let SymmetricMessagePayload::StreamFragment { payload, .. } = deserialized.payload
            else {
                panic!("Expected a StreamFragment, got {:?}", deserialized.payload);
            };
            inbound_bytes.extend_from_slice(payload.as_ref());
        }

        let result = background_task.await?;
        assert!(result.is_ok());
        assert_eq!(&message[..10], &inbound_bytes[..10]);
        assert_eq!(inbound_bytes.len(), 100_000);
        assert_eq!(&message[99_990..], &inbound_bytes[99_990..]);
        Ok(())
    }

    #[tokio::test]
    async fn test_send_stream_with_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Set bandwidth limit to 100KB/s (100,000 bytes/second)
        let bandwidth_limit = Some(100_000);

        // Expected: 100KB/s = 1KB per 10ms window
        // With ~1400 byte packets, that's ~0.7 packets per batch (rounds to 1)
        // So we should see rate limiting happening

        let start_time = Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: mpsc::Sender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();
        let key_clone = key.clone();

        // Spawn receiver task to collect packets
        let receiver_task = tokio::spawn(async move {
            let mut packet_count = 0;
            let mut total_bytes = 0;
            while let Some((addr, packet)) = outbound_receiver.recv().await {
                assert_eq!(addr, destination_addr);
                packet_count += 1;
                total_bytes += packet.len();

                // Decrypt and verify it's a stream fragment
                let packet_data = PacketData::<_, MAX_PACKET_SIZE>::from_buf(&packet);
                let decrypted = packet_data.try_decrypt_sym(&key_clone).unwrap();
                let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                match msg.payload {
                    SymmetricMessagePayload::StreamFragment { .. } => {
                        // Expected
                    }
                    _ => panic!("Expected stream fragment"),
                }
            }
            (packet_count, total_bytes)
        });

        // Spawn the send_stream task
        let send_task = tokio::spawn(send_stream(
            stream_id,
            last_packet_id.clone(),
            outbound_sender,
            destination_addr,
            message.clone(),
            key.clone(),
            sent_tracker.clone(),
            bandwidth_limit,
        ));

        // Wait for send task to complete
        send_task.await??;
        let elapsed = start_time.elapsed();

        // Drop the cloned sender to close the channel
        drop(sender_clone);

        // Get receiver results
        let (packet_count, _total_bytes) = receiver_task.await?;

        // Verify we sent the expected number of packets
        let expected_packets = message.len().div_ceil(MAX_DATA_SIZE);
        assert_eq!(packet_count, expected_packets);

        // Verify that rate limiting occurred
        // For 10KB at 100KB/s, should take at least 100ms theoretically
        // But with 8 packets and 1 packet per 10ms batch, actual time is ~70-80ms
        // Allow margin for processing overhead and timing precision
        debug!(
            "Transfer took: {elapsed:?}, packets sent: {packet_count}, expected: {expected_packets}"
        );
        debug!("Bytes per packet: ~{MAX_DATA_SIZE}");
        assert!(
            elapsed.as_millis() >= 60,
            "Transfer completed too quickly: {elapsed:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_send_stream_without_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // No bandwidth limit
        let bandwidth_limit = None;

        let start_time = Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: mpsc::Sender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();

        // Spawn receiver task to collect packets
        let receiver_task = tokio::spawn(async move {
            let mut packet_count = 0;
            while let Some((addr, _packet)) = outbound_receiver.recv().await {
                assert_eq!(addr, destination_addr);
                packet_count += 1;
            }
            packet_count
        });

        // Spawn the send_stream task
        let send_task = tokio::spawn(send_stream(
            stream_id,
            last_packet_id.clone(),
            outbound_sender,
            destination_addr,
            message.clone(),
            key.clone(),
            sent_tracker.clone(),
            bandwidth_limit,
        ));

        // Wait for send task to complete
        send_task.await??;
        let elapsed = start_time.elapsed();

        // Drop the cloned sender to close the channel
        drop(sender_clone);

        // Get receiver results
        let packet_count = receiver_task.await?;

        // Verify we sent the expected number of packets
        let expected_packets = message.len().div_ceil(MAX_DATA_SIZE);
        assert_eq!(packet_count, expected_packets);

        // Without rate limiting, should complete very quickly (< 50ms)
        assert!(
            elapsed.as_millis() < 50,
            "Transfer took too long without rate limit: {elapsed:?}"
        );

        Ok(())
    }
}
