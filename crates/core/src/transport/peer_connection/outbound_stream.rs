use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec;

use aes_gcm::Aes128Gcm;

use crate::{
    transport::{
        fast_channel::FastSender,
        packet_data,
        sent_packet_tracker::SentPacketTracker,
        symmetric_message::{self},
        TransportError,
    },
    util::time_source::InstantTimeSrc,
};

use super::StreamId;

pub(crate) type SerializedStream = Vec<u8>;

/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::StreamFragment metadata.
/// Measured overhead: 40 bytes (see symmetric_message::stream_fragment_overhead())
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 40;

// TODO: unit test
/// Handles sending a stream that is *not piped*. In the future this will be replaced by
/// piped streams which start forwarding before the stream has been received.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_stream(
    stream_id: StreamId,
    last_packet_id: Arc<AtomicU32>,
    sender: FastSender<(SocketAddr, Arc<[u8]>)>,
    destination_addr: SocketAddr,
    mut stream_to_send: SerializedStream,
    outbound_symmetric_key: Aes128Gcm,
    sent_packet_tracker: Arc<parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>>,
    token_bucket: Arc<super::super::token_bucket::TokenBucket>,
    ledbat: Arc<super::super::ledbat::LedbatController>,
) -> Result<(), TransportError> {
    tracing::debug!(
        stream_id = %stream_id.0,
        length_bytes = stream_to_send.len(),
        initial_rate_bytes_per_sec = token_bucket.rate(),
        ledbat_cwnd = ledbat.current_cwnd(),
        "Sending stream"
    );
    let total_length_bytes = stream_to_send.len() as u32;
    let total_packets = stream_to_send.len().div_ceil(MAX_DATA_SIZE);
    let mut sent_so_far = 0;
    let mut next_fragment_number = 1; // Fragment numbers are 1-indexed

    loop {
        if sent_so_far == total_packets {
            break;
        }

        // Token bucket rate limiting - reserve tokens and wait if needed
        let packet_size = stream_to_send.len().min(MAX_DATA_SIZE);
        let wait_time = token_bucket.reserve(packet_size);
        if !wait_time.is_zero() {
            tracing::trace!(
                stream_id = %stream_id.0,
                wait_time_ms = wait_time.as_millis(),
                packet_size,
                "Rate limiting stream transmission"
            );
            tokio::time::sleep(wait_time).await;
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

        // Track packet send for LEDBAT congestion control
        ledbat.on_send(packet_size);

        next_fragment_number += 1;
        sent_so_far += 1;
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
    use crate::transport::fast_channel;
    use crate::transport::ledbat::LedbatController;
    use crate::transport::packet_data::PacketData;
    use crate::transport::token_bucket::TokenBucket;

    #[tokio::test]
    async fn test_send_stream_success() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, outbound_receiver) = fast_channel::bounded(1);
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

        // Initialize LEDBAT and TokenBucket for test
        let ledbat = Arc::new(LedbatController::new(
            2928,
            2928,
            1_000_000_000,
        ));
        let token_bucket = Arc::new(TokenBucket::new(
            10_000,
            10_000_000,
        ));

        let background_task = tokio::spawn(send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            outbound_sender,
            remote_addr,
            message.clone(),
            cipher.clone(),
            sent_tracker,
            token_bucket,
            ledbat,
        ));

        let mut inbound_bytes = Vec::new();
        while let Ok((_, packet)) = outbound_receiver.recv_async().await {
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
        let (outbound_sender, outbound_receiver) = fast_channel::bounded(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Set bandwidth limit to 100KB/s (100,000 bytes/second)
        let bandwidth_limit = 100_000;

        // Initialize LEDBAT and TokenBucket for test
        // Use small burst capacity (1KB) to ensure rate limiting is observable
        let ledbat = Arc::new(LedbatController::new(
            2928,
            2928,
            1_000_000_000,
        ));
        let token_bucket = Arc::new(TokenBucket::new(
            1_000,  // 1KB burst - ensures rate limiting kicks in
            bandwidth_limit,
        ));

        // Expected: 100KB/s with token bucket rate limiting
        // Should throttle appropriately

        let start_time = Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: FastSender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();
        let key_clone = key.clone();

        // Spawn receiver task to collect packets
        let receiver_task = tokio::spawn(async move {
            let mut packet_count = 0;
            let mut total_bytes = 0;
            while let Ok((addr, packet)) = outbound_receiver.recv_async().await {
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
            token_bucket,
            ledbat,
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
        // For 10KB at 100KB/s with 1KB burst:
        // - First 1KB sent immediately (burst)
        // - Remaining 9KB at 100KB/s = ~90ms
        // - Actual timing: ~50-60ms due to concurrent token refill
        debug!(
            "Transfer took: {elapsed:?}, packets sent: {packet_count}, expected: {expected_packets}"
        );
        debug!("Bytes per packet: ~{MAX_DATA_SIZE}");
        assert!(
            elapsed.as_millis() >= 50,
            "Transfer completed too quickly: {elapsed:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_send_stream_without_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, outbound_receiver) = fast_channel::bounded(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Initialize LEDBAT and TokenBucket with very high rate (effectively unlimited)
        let ledbat = Arc::new(LedbatController::new(
            2928,
            2928,
            1_000_000_000,
        ));
        let token_bucket = Arc::new(TokenBucket::new(
            100_000,      // 100 KB burst capacity
            1_000_000_000, // 1 GB/s rate (effectively unlimited)
        ));

        let start_time = Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: FastSender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();

        // Spawn receiver task to collect packets
        let receiver_task = tokio::spawn(async move {
            let mut packet_count = 0;
            while let Ok((addr, _packet)) = outbound_receiver.recv_async().await {
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
            token_bucket,
            ledbat,
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
