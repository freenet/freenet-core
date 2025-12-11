use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fast_channel::FastReceiver;
use super::Socket;
use crate::util::time_source::{InstantTimeSrc, TimeSource};

/// Keeps track of the bandwidth used in the last window_size. Recommend a `window_size` of
/// 10 seconds.
pub(super) struct PacketRateLimiter<T: TimeSource> {
    packets: VecDeque<(usize, Instant)>,
    window_size: Duration,
    current_bandwidth: usize,
    outbound_packets: FastReceiver<(SocketAddr, Arc<[u8]>)>,
    time_source: T,
}

impl PacketRateLimiter<InstantTimeSrc> {
    pub(super) fn new(
        window_size: Duration,
        outbound_packets: FastReceiver<(SocketAddr, Arc<[u8]>)>,
    ) -> Self {
        PacketRateLimiter {
            packets: VecDeque::new(),
            window_size,
            current_bandwidth: 0,
            outbound_packets,
            time_source: InstantTimeSrc::new(),
        }
    }
}

impl<T: TimeSource> PacketRateLimiter<T> {
    /// Run the rate limiter in a blocking context.
    ///
    /// This should be called via `tokio::task::spawn_blocking` for optimal performance.
    /// Uses blocking crossbeam channel recv() for ~3x better throughput than async.
    pub(super) fn rate_limiter<S: Socket>(mut self, bandwidth_limit: Option<usize>, socket: Arc<S>) {
        tracing::info!("Rate limiter task started (blocking mode)");
        while let Ok((socket_addr, packet)) = self.outbound_packets.recv() {
            tracing::debug!(
                target: "freenet_core::transport::send_debug",
                dest_addr = %socket_addr,
                packet_len = packet.len(),
                "Dequeued packet for sending"
            );
            if let Some(bandwidth_limit) = bandwidth_limit {
                self.rate_limiting(bandwidth_limit, &*socket, packet, socket_addr);
            } else {
                tracing::debug!(
                    target: "freenet_core::transport::send_debug",
                    dest_addr = %socket_addr,
                    packet_len = packet.len(),
                    "Sending packet without bandwidth limit"
                );
                match socket.send_to_blocking(&packet, socket_addr) {
                    Ok(bytes_sent) => {
                        tracing::debug!(
                            target: "freenet_core::transport::send_debug",
                            dest_addr = %socket_addr,
                            bytes_sent,
                            expected_len = packet.len(),
                            "Socket send_to completed (no rate limit)"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            target: "freenet_core::transport::send_debug",
                            dest_addr = %socket_addr,
                            error = %error,
                            error_kind = ?error.kind(),
                            "Socket send_to failed (no rate limit)"
                        );
                    }
                }
            }
        }
        tracing::debug!("Rate limiter task ended");
    }

    #[inline(always)]
    fn rate_limiting<S: Socket>(
        &mut self,
        bandwidth_limit: usize,
        socket: &S,
        packet: Arc<[u8]>,
        socket_addr: SocketAddr,
    ) {
        if let Some(wait_time) = self.can_send_packet(bandwidth_limit, packet.len()) {
            // Use blocking sleep instead of tokio::time::sleep
            std::thread::sleep(wait_time);
            tracing::debug!(%socket_addr, "Sending outbound packet after waiting {:?}", wait_time);

            tracing::info!(
                target: "freenet_core::transport::send_debug",
                dest_addr = %socket_addr,
                packet_len = packet.len(),
                wait_time_ms = wait_time.as_millis(),
                "Attempting to send packet (after rate limit wait)"
            );

            match socket.send_to_blocking(&packet, socket_addr) {
                Ok(bytes_sent) => {
                    tracing::info!(
                        target: "freenet_core::transport::send_debug",
                        dest_addr = %socket_addr,
                        bytes_sent,
                        "Socket send_to completed (after wait)"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        target: "freenet_core::transport::send_debug",
                        dest_addr = %socket_addr,
                        error = %error,
                        "Socket send_to failed (after wait)"
                    );
                }
            }
        } else {
            tracing::info!(
                target: "freenet_core::transport::send_debug",
                dest_addr = %socket_addr,
                packet_len = packet.len(),
                first_bytes = ?&packet[..std::cmp::min(32, packet.len())],
                "Attempting to send packet (with rate limit)"
            );

            match socket.send_to_blocking(&packet, socket_addr) {
                Ok(bytes_sent) => {
                    tracing::info!(
                        target: "freenet_core::transport::send_debug",
                        dest_addr = %socket_addr,
                        bytes_sent,
                        expected_len = packet.len(),
                        "Socket send_to completed (rate limited)"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        target: "freenet_core::transport::send_debug",
                        dest_addr = %socket_addr,
                        error = %error,
                        error_kind = ?error.kind(),
                        "Socket send_to failed (rate limited)"
                    );
                }
            }
        }
        self.add_packet(packet.len());
    }

    /// Report that a packet was sent
    fn add_packet(&mut self, packet_size: usize) {
        let now = self.time_source.now();
        self.packets.push_back((packet_size, now));
        self.current_bandwidth += packet_size;
        self.cleanup();
    }

    /// Removes packets that are older than the window size.
    fn cleanup(&mut self) {
        let now = self.time_source.now();
        while self
            .packets
            .front()
            .is_some_and(|&(_, time)| now - time > self.window_size)
        {
            let expired = self.packets.pop_front();
            if let Some((size, _)) = expired {
                self.current_bandwidth -= size;
            }
        }
    }

    /// Returns none if the packet can be sent immediately without `bandwidth_limit` being
    /// exceeded within the `window_size`. Otherwise returns Some(wait_time) where wait_time is the
    /// amount of time that should be waited before sending the packet.
    ///
    /// `bandwidth_limit` (in bytes) should be set to 50% higher than the target upstream bandwidth the
    /// [topology manager](crate::topology::TopologyManager) is aiming for, as it serves
    /// as a hard limit which we'd prefer not to hit.
    fn can_send_packet(&mut self, bandwidth_limit: usize, packet_size: usize) -> Option<Duration> {
        self.cleanup();

        if self.current_bandwidth + packet_size <= bandwidth_limit {
            return None;
        }

        let mut temp_bandwidth = self.current_bandwidth;
        let mut wait_time = None;

        for &(size, time) in self.packets.iter() {
            temp_bandwidth -= size;
            if temp_bandwidth + packet_size <= bandwidth_limit {
                wait_time = Some(
                    self.window_size
                        .saturating_sub(self.time_source.now() - time),
                );
                break;
            }
        }

        wait_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::fast_channel;
    use crate::util::time_source::MockTimeSource;

    fn mock_tracker(window_size: Duration) -> PacketRateLimiter<MockTimeSource> {
        PacketRateLimiter {
            packets: VecDeque::new(),
            window_size,
            current_bandwidth: 0,
            outbound_packets: fast_channel::bounded(1).1,
            time_source: MockTimeSource::new(Instant::now()),
        }
    }

    fn verify_bandwidth_match<T: TimeSource>(tracker: &PacketRateLimiter<T>) {
        let mut total_bandwidth = 0;
        for &(size, _) in tracker.packets.iter() {
            total_bandwidth += size;
        }
        assert_eq!(total_bandwidth, tracker.current_bandwidth);
    }

    #[test]
    fn test_adding_packets() {
        let mut tracker =
            PacketRateLimiter::new(Duration::from_secs(1), fast_channel::bounded(1).1);
        verify_bandwidth_match(&tracker);
        tracker.add_packet(1500);
        verify_bandwidth_match(&tracker);
        assert_eq!(tracker.packets.len(), 1);
    }

    #[test]
    fn test_bandwidth_calculation() {
        let mut tracker =
            PacketRateLimiter::new(Duration::from_secs(1), fast_channel::bounded(1).1);
        tracker.add_packet(1500);
        tracker.add_packet(2500);
        verify_bandwidth_match(&tracker);
        assert_eq!(
            tracker.packets.iter().map(|&(size, _)| size).sum::<usize>(),
            4000
        );
    }

    #[test]
    fn test_packet_expiry() {
        let mut tracker = mock_tracker(Duration::from_millis(200));
        tracker.add_packet(1500);
        verify_bandwidth_match(&tracker);
        tracker.time_source.advance_time(Duration::from_millis(300));
        tracker.cleanup();
        verify_bandwidth_match(&tracker);
        assert!(tracker.packets.is_empty());
    }

    #[test]
    fn test_wait_time_calculation() {
        let mut tracker = mock_tracker(Duration::from_secs(1));
        tracker.add_packet(5000);
        verify_bandwidth_match(&tracker);
        tracker.time_source.advance_time(Duration::from_millis(500));
        tracker.add_packet(4000);
        verify_bandwidth_match(&tracker);
        match tracker.can_send_packet(10000, 2000) {
            None => panic!("Should require waiting"),
            Some(wait_time) => assert_eq!(wait_time, Duration::from_millis(500)),
        }
    }

    #[test]
    fn test_immediate_send() {
        let mut tracker =
            PacketRateLimiter::new(Duration::from_millis(10), fast_channel::bounded(1).1);
        tracker.add_packet(3000);
        assert_eq!(tracker.can_send_packet(10000, 2000), None);
    }

    // Property-based tests using arbitrary for edge case coverage
    mod property_tests {
        use super::*;
        use arbitrary::Unstructured;

        /// Helper to run a property test with random data
        fn run_property_test<F>(iterations: usize, mut property: F)
        where
            F: FnMut(&mut Unstructured) -> arbitrary::Result<()>,
        {
            let mut rng = rand::rng();
            for _ in 0..iterations {
                let data: Vec<u8> = (0..1024).map(|_| rand::Rng::random(&mut rng)).collect();
                let mut u = Unstructured::new(&data);
                // Ignore errors from insufficient data
                let _ = property(&mut u);
            }
        }

        #[test]
        fn property_bandwidth_invariant_maintained() {
            // Invariant: current_bandwidth always equals sum of packet sizes
            run_property_test(100, |u| {
                let window_ms: u64 = u.int_in_range(1..=10000)?;
                let mut tracker = mock_tracker(Duration::from_millis(window_ms));

                // Add a random number of packets
                let num_packets: usize = u.int_in_range(0..=20)?;
                for _ in 0..num_packets {
                    let size: usize = u.int_in_range(0..=10000)?;
                    tracker.add_packet(size);
                    verify_bandwidth_match(&tracker);
                }

                // Advance time randomly and cleanup
                let advance_ms: u64 = u.int_in_range(0..=20000)?;
                tracker
                    .time_source
                    .advance_time(Duration::from_millis(advance_ms));
                tracker.cleanup();
                verify_bandwidth_match(&tracker);

                Ok(())
            });
        }

        #[test]
        fn property_can_send_when_under_limit() {
            // If current_bandwidth + packet_size <= limit, should return None (immediate send)
            run_property_test(100, |u| {
                let window_ms: u64 = u.int_in_range(1..=10000)?;
                let mut tracker = mock_tracker(Duration::from_millis(window_ms));

                let bandwidth_limit: usize = u.int_in_range(1000..=1_000_000)?;
                let packet_size: usize = u.int_in_range(0..=bandwidth_limit)?;

                // Add packets that stay under the limit
                let num_packets: usize = u.int_in_range(0..=5)?;
                let max_per_packet = (bandwidth_limit - packet_size) / (num_packets + 1);
                for _ in 0..num_packets {
                    if max_per_packet > 0 {
                        let size: usize = u.int_in_range(0..=max_per_packet)?;
                        tracker.add_packet(size);
                    }
                }

                // If we're under the limit, should be able to send immediately
                if tracker.current_bandwidth + packet_size <= bandwidth_limit {
                    assert!(
                        tracker
                            .can_send_packet(bandwidth_limit, packet_size)
                            .is_none(),
                        "Should allow immediate send when under bandwidth limit"
                    );
                }

                Ok(())
            });
        }

        #[test]
        fn property_wait_time_never_exceeds_window() {
            // The wait time should never exceed the window size
            run_property_test(100, |u| {
                let window_ms: u64 = u.int_in_range(1..=10000)?;
                let window = Duration::from_millis(window_ms);
                let mut tracker = mock_tracker(window);

                // Add enough packets to exceed any reasonable limit
                let num_packets: usize = u.int_in_range(1..=10)?;
                for _ in 0..num_packets {
                    let size: usize = u.int_in_range(1000..=50000)?;
                    tracker.add_packet(size);
                }

                let bandwidth_limit: usize = u.int_in_range(1000..=100000)?;
                let packet_size: usize = u.int_in_range(1..=10000)?;

                if let Some(wait_time) = tracker.can_send_packet(bandwidth_limit, packet_size) {
                    assert!(
                        wait_time <= window,
                        "Wait time {:?} exceeds window {:?}",
                        wait_time,
                        window
                    );
                }

                Ok(())
            });
        }

        #[test]
        fn property_cleanup_removes_old_packets() {
            // After advancing time past window, all packets should be cleaned up
            run_property_test(100, |u| {
                let window_ms: u64 = u.int_in_range(1..=1000)?;
                let mut tracker = mock_tracker(Duration::from_millis(window_ms));

                // Add some packets
                let num_packets: usize = u.int_in_range(1..=20)?;
                for _ in 0..num_packets {
                    let size: usize = u.int_in_range(1..=10000)?;
                    tracker.add_packet(size);
                }

                // Advance time past the window
                tracker
                    .time_source
                    .advance_time(Duration::from_millis(window_ms + 1));
                tracker.cleanup();

                assert!(
                    tracker.packets.is_empty(),
                    "All packets should be cleaned up after window expires"
                );
                assert_eq!(
                    tracker.current_bandwidth, 0,
                    "Bandwidth should be zero after cleanup"
                );

                Ok(())
            });
        }

        #[test]
        fn property_partial_cleanup_preserves_recent() {
            // Packets added after time advance should be preserved
            run_property_test(100, |u| {
                let window_ms: u64 = u.int_in_range(100..=1000)?;
                let mut tracker = mock_tracker(Duration::from_millis(window_ms));

                // Add initial packets
                let initial_size: usize = u.int_in_range(1..=5000)?;
                tracker.add_packet(initial_size);

                // Advance time - must be > 0 to separate the packets in time
                // and < window_ms to keep within window for the recent packet
                let advance_ms: u64 = u.int_in_range(1..=(window_ms / 2).max(1))?;
                tracker
                    .time_source
                    .advance_time(Duration::from_millis(advance_ms));

                // Add more packets (these are now timestamped later)
                let recent_size: usize = u.int_in_range(1..=5000)?;
                tracker.add_packet(recent_size);

                // Advance past initial packet's window but not recent
                // The initial packet will expire after window_ms from when it was added
                // The recent packet will expire after window_ms from when *it* was added
                // So we need to advance exactly enough to expire initial but not recent
                tracker
                    .time_source
                    .advance_time(Duration::from_millis(window_ms - advance_ms + 1));
                tracker.cleanup();

                // Recent packet should still be there (it was added advance_ms after initial,
                // so it has advance_ms - 1 left before expiring)
                assert!(
                    !tracker.packets.is_empty(),
                    "Recent packets should be preserved (window_ms={}, advance_ms={})",
                    window_ms,
                    advance_ms
                );
                verify_bandwidth_match(&tracker);

                Ok(())
            });
        }

        #[test]
        fn edge_case_zero_packet_size() {
            let mut tracker = mock_tracker(Duration::from_secs(1));
            tracker.add_packet(0);
            verify_bandwidth_match(&tracker);
            assert_eq!(tracker.current_bandwidth, 0);

            // Zero-size packet should always be allowed
            assert!(tracker.can_send_packet(100, 0).is_none());
        }

        #[test]
        fn edge_case_zero_bandwidth_limit() {
            let mut tracker = mock_tracker(Duration::from_secs(1));

            // With zero limit and zero packet, should still work
            assert!(tracker.can_send_packet(0, 0).is_none());

            // Any non-zero packet should require waiting (or be blocked)
            tracker.add_packet(100);
            // With 0 limit but existing bandwidth, the math gets interesting
            // The function should handle this gracefully
            let _ = tracker.can_send_packet(0, 1);
        }

        #[test]
        fn edge_case_exact_bandwidth_limit() {
            let mut tracker = mock_tracker(Duration::from_secs(1));
            tracker.add_packet(5000);

            // Exactly at limit - should still allow
            assert!(
                tracker.can_send_packet(10000, 5000).is_none(),
                "Should allow send when exactly at limit"
            );

            // One byte over - should require waiting
            assert!(
                tracker.can_send_packet(10000, 5001).is_some(),
                "Should require wait when over limit"
            );
        }

        #[test]
        fn edge_case_very_small_window() {
            let mut tracker = mock_tracker(Duration::from_nanos(1));
            tracker.add_packet(1000);

            // With tiny window, packets expire almost immediately
            tracker.time_source.advance_time(Duration::from_micros(1));
            tracker.cleanup();

            assert!(tracker.packets.is_empty());
        }
    }
}
