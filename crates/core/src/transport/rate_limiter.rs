use tokio::sync::mpsc;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::Socket;
use crate::util::time_source::{InstantTimeSrc, TimeSource};

/// Keeps track of the bandwidth used in the last window_size. Recommend a `window_size` of
/// 10 seconds.
pub(super) struct PacketRateLimiter<T: TimeSource> {
    packets: VecDeque<(usize, Instant)>,
    window_size: Duration,
    current_bandwidth: usize,
    outbound_packets: mpsc::Receiver<(SocketAddr, Arc<[u8]>)>,
    time_source: T,
}

impl PacketRateLimiter<InstantTimeSrc> {
    pub(super) fn new(
        window_size: Duration,
        outbound_packets: mpsc::Receiver<(SocketAddr, Arc<[u8]>)>,
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
    pub(super) async fn rate_limiter<S: Socket>(mut self, bandwidth_limit: usize, socket: Arc<S>) {
        while let Some((socket_addr, packet)) = self.outbound_packets.recv().await {
            if let Some(wait_time) = self.can_send_packet(bandwidth_limit, packet.len()) {
                tokio::time::sleep(wait_time).await;
                if let Err(error) = socket.send_to(&packet, socket_addr).await {
                    tracing::debug!("Error sending packet: {:?}", error);
                } else {
                    self.add_packet(packet.len());
                }
            } else if let Err(error) = socket.send_to(&packet, socket_addr).await {
                tracing::debug!(%socket_addr, "Error sending packet: {:?}", error);
            } else {
                self.add_packet(packet.len());
            }
        }
        tracing::debug!("Rate limiter task ended unexpectedly");
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
            .map_or(false, |&(_, time)| now - time > self.window_size)
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
    /// `bandwidth_limit` should be set to 50% higher than the target upstream bandwidth the
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
    use crate::util::time_source::MockTimeSource;

    fn mock_tracker(window_size: Duration) -> PacketRateLimiter<MockTimeSource> {
        PacketRateLimiter {
            packets: VecDeque::new(),
            window_size,
            current_bandwidth: 0,
            outbound_packets: mpsc::channel(1).1,
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
        let mut tracker = PacketRateLimiter::new(Duration::from_secs(1), mpsc::channel(1).1);
        verify_bandwidth_match(&tracker);
        tracker.add_packet(1500);
        verify_bandwidth_match(&tracker);
        assert_eq!(tracker.packets.len(), 1);
    }

    #[test]
    fn test_bandwidth_calculation() {
        let mut tracker = PacketRateLimiter::new(Duration::from_secs(1), mpsc::channel(1).1);
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
        let mut tracker = PacketRateLimiter::new(Duration::from_millis(10), mpsc::channel(1).1);
        tracker.add_packet(3000);
        assert_eq!(tracker.can_send_packet(10000, 2000), None);
    }
}
