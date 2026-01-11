use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::Aes128Gcm;
use bytes::Bytes;

use crate::{
    simulation::TimeSource,
    tracing::TransferDirection,
    transport::{
        congestion_control::{CongestionControl, CongestionController},
        metrics::{emit_transfer_completed, emit_transfer_failed, emit_transfer_started},
        packet_data,
        sent_packet_tracker::SentPacketTracker,
        symmetric_message::{self},
        TransferStats, TransportError,
    },
};

use super::StreamId;

/// Stream payload type using zero-copy Bytes for efficient fragmentation.
/// Using Bytes::slice() instead of Vec::split_off() eliminates per-fragment allocations.
pub(crate) type SerializedStream = Bytes;

/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::StreamFragment metadata.
/// Measured overhead: 40 bytes (see symmetric_message::stream_fragment_overhead())
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 40;

// TODO: unit test
/// Handles sending a stream that is *not piped*. In the future this will be replaced by
/// piped streams which start forwarding before the stream has been received.
///
/// Returns `TransferStats` on success with LEDBAT congestion control metrics
/// for telemetry purposes.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_stream<S: super::super::Socket, T: TimeSource>(
    stream_id: StreamId,
    last_packet_id: Arc<AtomicU32>,
    socket: Arc<S>,
    destination_addr: SocketAddr,
    mut stream_to_send: SerializedStream,
    outbound_symmetric_key: Aes128Gcm,
    sent_packet_tracker: Arc<parking_lot::Mutex<SentPacketTracker<T>>>,
    token_bucket: Arc<super::super::token_bucket::TokenBucket<T>>,
    congestion_controller: Arc<CongestionController<T>>,
    time_source: T,
) -> Result<TransferStats, TransportError> {
    let start_time = time_source.now();
    let bytes_to_send = stream_to_send.len() as u64;

    // Emit transfer started telemetry event
    emit_transfer_started(
        stream_id.0 as u64,
        destination_addr,
        bytes_to_send,
        TransferDirection::Send,
    );

    tracing::debug!(
        stream_id = %stream_id.0,
        length_bytes = stream_to_send.len(),
        initial_rate_bytes_per_sec = token_bucket.rate(),
        cwnd = congestion_controller.current_cwnd(),
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

        let packet_size = stream_to_send.len().min(MAX_DATA_SIZE);

        // LEDBAT congestion control - wait until cwnd has space for this packet.
        // This enforces the congestion window calculated by LEDBAT's slow start
        // and congestion avoidance algorithms.
        //
        // IMPORTANT: This loop requires that recv() is being called on this connection
        // to process incoming ACKs. ACKs reduce flightsize via on_ack(), which opens
        // cwnd space. If recv() is never called, flightsize never decreases and this
        // loop will block forever.
        //
        // In production, PeerConnection is always used in a bidirectional select! loop
        // (see peer_connection_listener in p2p_protoc.rs) which ensures recv() is
        // always being polled. Tests must follow the same pattern.
        let mut cwnd_wait_iterations = 0;
        loop {
            let flightsize = congestion_controller.flightsize();
            let cwnd = congestion_controller.current_cwnd();

            // Check if we have space in the congestion window
            if flightsize + packet_size <= cwnd {
                break; // Space available, proceed to send
            }

            cwnd_wait_iterations += 1;
            if cwnd_wait_iterations == 1 {
                tracing::trace!(
                    stream_id = %stream_id.0,
                    flightsize_kb = flightsize / 1024,
                    cwnd_kb = cwnd / 1024,
                    packet_size,
                    "Waiting for cwnd space (ensure recv() is being called to process ACKs)"
                );
            }

            // Exponential backoff to balance responsiveness and CPU usage
            // First 10 attempts: immediate yield (context switch only)
            // Next 90 attempts: 100μs sleep
            // Beyond 100: 1ms sleep (graceful degradation)
            if cwnd_wait_iterations <= 10 {
                tokio::task::yield_now().await;
            } else if cwnd_wait_iterations <= 100 {
                time_source.sleep(Duration::from_micros(100)).await;
            } else {
                time_source.sleep(Duration::from_millis(1)).await;
            }
        }

        if cwnd_wait_iterations > 0 {
            tracing::trace!(
                stream_id = %stream_id.0,
                wait_iterations = cwnd_wait_iterations,
                "Acquired cwnd space"
            );
        }

        // Token bucket rate limiting - reserve tokens and wait if needed
        let wait_time = token_bucket.reserve(packet_size);
        if !wait_time.is_zero() {
            tracing::trace!(
                stream_id = %stream_id.0,
                wait_time_ms = wait_time.as_millis(),
                packet_size,
                "Rate limiting stream transmission"
            );
            time_source.sleep(wait_time).await;
        }

        // Zero-copy fragmentation using Bytes::slice()
        // This avoids allocating a new Vec for each fragment
        let fragment = {
            if stream_to_send.len() > MAX_DATA_SIZE {
                let fragment = stream_to_send.slice(..MAX_DATA_SIZE);
                stream_to_send = stream_to_send.slice(MAX_DATA_SIZE..);
                fragment
            } else {
                std::mem::take(&mut stream_to_send)
            }
        };
        let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::Release);
        if let Err(e) = super::packet_sending(
            destination_addr,
            &socket,
            packet_id,
            &outbound_symmetric_key,
            vec![],
            symmetric_message::StreamFragment {
                stream_id,
                total_length_bytes: total_length_bytes as u64,
                fragment_number: next_fragment_number,
                payload: fragment,
            },
            sent_packet_tracker.as_ref(),
        )
        .await
        {
            // Emit transfer failed telemetry event
            let bytes_sent = (sent_so_far * MAX_DATA_SIZE) as u64;
            let elapsed = time_source.now().saturating_sub(start_time);
            emit_transfer_failed(
                stream_id.0 as u64,
                destination_addr,
                bytes_sent.min(bytes_to_send),
                e.to_string(),
                elapsed.as_millis() as u64,
                TransferDirection::Send,
            );
            return Err(e);
        }

        // Track packet send for congestion control
        congestion_controller.on_send(packet_size);

        next_fragment_number += 1;
        sent_so_far += 1;
    }

    // Gather congestion control stats for telemetry
    // Use LEDBAT-specific stats when available for detailed metrics
    let generic_stats = congestion_controller.stats();
    let ledbat_stats = congestion_controller.ledbat_stats();
    let elapsed = time_source.now().saturating_sub(start_time);

    tracing::debug!(
        stream_id = %stream_id.0,
        total_packets = %sent_so_far,
        bytes = bytes_to_send,
        elapsed_ms = elapsed.as_millis(),
        peak_cwnd_kb = generic_stats.peak_cwnd / 1024,
        final_cwnd_kb = generic_stats.cwnd / 1024,
        slowdowns = ledbat_stats.as_ref().map(|s| s.periodic_slowdowns).unwrap_or(0),
        "Stream sent"
    );

    // Emit transfer completed telemetry event
    emit_transfer_completed(
        stream_id.0 as u64,
        destination_addr,
        bytes_to_send,
        elapsed.as_millis() as u64,
        if elapsed.as_secs() > 0 {
            bytes_to_send / elapsed.as_secs()
        } else {
            bytes_to_send * 1000 / elapsed.as_millis().max(1) as u64
        },
        Some(generic_stats.peak_cwnd as u32),
        Some(generic_stats.cwnd as u32),
        ledbat_stats.as_ref().map(|s| s.periodic_slowdowns as u32),
        Some(generic_stats.base_delay.as_millis() as u32),
        Some(generic_stats.ssthresh as u32),
        ledbat_stats.as_ref().map(|s| s.min_ssthresh_floor as u32),
        Some(generic_stats.total_timeouts as u32),
        TransferDirection::Send,
    );

    Ok(TransferStats {
        stream_id: stream_id.0 as u64,
        remote_addr: destination_addr,
        bytes_transferred: bytes_to_send,
        elapsed,
        peak_cwnd_bytes: generic_stats.peak_cwnd as u32,
        final_cwnd_bytes: generic_stats.cwnd as u32,
        slowdowns_triggered: ledbat_stats
            .as_ref()
            .map(|s| s.periodic_slowdowns as u32)
            .unwrap_or(0),
        base_delay: generic_stats.base_delay,
        final_ssthresh_bytes: generic_stats.ssthresh as u32,
        min_ssthresh_floor_bytes: ledbat_stats
            .as_ref()
            .map(|s| s.min_ssthresh_floor as u32)
            .unwrap_or(0),
        total_timeouts: generic_stats.total_timeouts as u32,
    })
}

#[cfg(test)]
mod tests {
    use aes_gcm::KeyInit;
    use std::net::Ipv4Addr;
    use tests::packet_data::MAX_PACKET_SIZE;
    use tracing::debug;

    use super::{
        symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
        *,
    };
    use crate::config::GlobalExecutor;
    use crate::simulation::{RealTime, VirtualTime};
    use crate::transport::congestion_control::CongestionControlConfig;
    use crate::transport::fast_channel::{self, FastSender};
    use crate::transport::ledbat::LedbatConfig;
    use crate::transport::packet_data::PacketData;
    use crate::transport::token_bucket::TokenBucket;

    /// Simple test socket that writes to a channel
    struct TestSocket {
        sender: fast_channel::FastSender<(SocketAddr, Arc<[u8]>)>,
    }

    impl TestSocket {
        fn new(sender: fast_channel::FastSender<(SocketAddr, Arc<[u8]>)>) -> Self {
            Self { sender }
        }
    }

    impl crate::transport::Socket for TestSocket {
        async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            unimplemented!()
        }

        async fn recv_from(&self, _buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            unimplemented!()
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.sender
                .send_async((target, buf.into()))
                .await
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.sender
                .send((target, buf.into()))
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }
    }

    #[tokio::test]
    async fn test_send_stream_success() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, outbound_receiver) = fast_channel::bounded(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let mut message = vec![0u8; 100_000];
        crate::config::GlobalRng::fill_bytes(&mut message);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        // Use VirtualTime for deterministic testing
        // Token bucket has enough capacity that no sleeping is needed
        let time_source = VirtualTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Initialize congestion controller and TokenBucket for test with VirtualTime
        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            1_000_000,
            10_000_000,
            time_source.clone(),
        ));

        let background_task = GlobalExecutor::spawn(send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(message.clone()),
            cipher.clone(),
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
        ));

        let mut inbound_bytes = Vec::with_capacity(message.len());
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
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Set bandwidth limit to 100KB/s (100,000 bytes/second)
        let bandwidth_limit = 100_000;

        // Use real time for integration testing with bandwidth limiting
        let time_source = RealTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Initialize congestion controller and TokenBucket for test
        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        // Use small burst capacity (1KB) to ensure rate limiting is observable
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc();
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            1_000, // 1KB burst - ensures rate limiting kicks in
            bandwidth_limit,
            time_source.clone(),
        ));

        // Expected: 100KB/s with token bucket rate limiting
        // Should throttle appropriately

        let start_time = tokio::time::Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: FastSender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();
        let key_clone = key.clone();

        // Spawn receiver task to collect packets
        let receiver_task = GlobalExecutor::spawn(async move {
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
        let send_task = GlobalExecutor::spawn(send_stream(
            stream_id,
            last_packet_id.clone(),
            Arc::new(TestSocket::new(outbound_sender)),
            destination_addr,
            Bytes::from(message.clone()),
            key.clone(),
            sent_tracker.clone(),
            token_bucket,
            congestion_controller,
            time_source,
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
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Use real time for integration testing without bandwidth limiting
        let time_source = RealTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Initialize congestion controller and TokenBucket with very high rate (effectively unlimited)
        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc();
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            100_000,       // 100 KB burst capacity
            1_000_000_000, // 1 GB/s rate (effectively unlimited)
            time_source.clone(),
        ));

        let start_time = tokio::time::Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: FastSender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();

        // Spawn receiver task to collect packets
        let receiver_task = GlobalExecutor::spawn(async move {
            let mut packet_count = 0;
            while let Ok((addr, _packet)) = outbound_receiver.recv_async().await {
                assert_eq!(addr, destination_addr);
                packet_count += 1;
            }
            packet_count
        });

        // Spawn the send_stream task
        let send_task = GlobalExecutor::spawn(send_stream(
            stream_id,
            last_packet_id.clone(),
            Arc::new(TestSocket::new(outbound_sender)),
            destination_addr,
            Bytes::from(message.clone()),
            key.clone(),
            sent_tracker.clone(),
            token_bucket,
            congestion_controller,
            time_source,
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
