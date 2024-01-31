use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Keeps track of the bandwidth used in the last window_size. Recommend a `window_size` of
/// 10 seconds.
pub(super) struct PacketBWTracker<T: TimeSource> {
    packets: VecDeque<(usize, Instant)>,
    window_size: Duration,
    current_bandwidth: usize,
    time_source: T,
}

impl PacketBWTracker<SystemTime> {
    pub(super) fn new(window_size: Duration) -> Self {
        PacketBWTracker {
            packets: VecDeque::new(),
            window_size,
            current_bandwidth: 0,
            time_source: SystemTime,
        }
    }
}

impl<T: TimeSource> PacketBWTracker<T> {
    /// Report that a packet was sent
    pub(super) fn add_packet(&mut self, packet_size: usize) {
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
    pub(super) fn can_send_packet(
        &mut self,
        bandwidth_limit: usize,
        packet_size: usize,
    ) -> Option<Duration> {
        self.cleanup();

        if self.current_bandwidth + packet_size <= bandwidth_limit {
            return None;
        }

        let mut temp_bandwidth = self.current_bandwidth;
        let mut wait_time = None;

        for &(size, time) in self.packets.iter() {
            temp_bandwidth -= size;
            if temp_bandwidth + packet_size <= bandwidth_limit {
                wait_time = Some(self.window_size - (self.time_source.now() - time));
                break;
            }
        }

        wait_time
    }
}

pub(super) trait TimeSource {
    fn now(&self) -> Instant;
}

pub(super) struct SystemTime;

impl TimeSource for SystemTime {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct MockTimeSource {
        current_instant: Instant,
    }

    impl MockTimeSource {
        fn new(start_instant: Instant) -> Self {
            MockTimeSource {
                current_instant: start_instant,
            }
        }

        fn advance_time(&mut self, duration: Duration) {
            self.current_instant += duration;
        }
    }

    impl TimeSource for MockTimeSource {
        fn now(&self) -> Instant {
            self.current_instant
        }
    }

    fn mock_tracker(window_size: Duration) -> (PacketBWTracker<MockTimeSource>, MockTimeSource) {
        let time_source = MockTimeSource::new(Instant::now());
        let tracker = PacketBWTracker {
            packets: VecDeque::new(),
            window_size,
            current_bandwidth: 0,
            time_source: time_source.clone(),
        };
        (tracker, time_source)
    }

    fn verify_bandwidth_match<T: TimeSource>(tracker: &PacketBWTracker<T>) {
        let mut total_bandwidth = 0;
        for &(size, _) in tracker.packets.iter() {
            total_bandwidth += size;
        }
        assert_eq!(total_bandwidth, tracker.current_bandwidth);
    }

    #[test]
    fn test_adding_packets() {
        let mut tracker = PacketBWTracker::new(Duration::from_secs(1));
        verify_bandwidth_match(&tracker);
        tracker.add_packet(1500);
        verify_bandwidth_match(&tracker);
        assert_eq!(tracker.packets.len(), 1);
    }

    #[test]
    fn test_bandwidth_calculation() {
        let mut tracker = PacketBWTracker::new(Duration::from_secs(1));
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
        let (mut tracker, mut ts) = mock_tracker(Duration::from_millis(200));
        tracker.add_packet(1500);
        verify_bandwidth_match(&tracker);
        ts.advance_time(Duration::from_millis(300));
        tracker.cleanup();
        verify_bandwidth_match(&tracker);
        assert!(tracker.packets.is_empty());
    }

    #[test]
    fn test_wait_time_calculation() {
        let (mut tracker, mut ts) = mock_tracker(Duration::from_secs(1));
        tracker.add_packet(5000);
        verify_bandwidth_match(&tracker);
        ts.advance_time(Duration::from_millis(500));
        tracker.add_packet(4000);
        verify_bandwidth_match(&tracker);
        match tracker.can_send_packet(10000, 2000) {
            None => panic!("Should require waiting"),
            Some(wait_time) => assert_eq!(wait_time, Duration::from_millis(500)),
        }
    }

    #[test]
    fn test_immediate_send() {
        let mut tracker = PacketBWTracker::new(Duration::from_secs(10));
        tracker.add_packet(3000);
        assert_eq!(tracker.can_send_packet(10000, 2000), None);
    }
}
