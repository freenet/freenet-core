//! Deterministic event scheduler for simulation.
//!
//! The scheduler processes events in a deterministic order based on:
//! 1. Event timestamp (earlier first)
//! 2. Peer ID (for same timestamp)
//! 3. Event type (for same timestamp and peer)
//! 4. Event ID (for complete tie-breaking)

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    net::SocketAddr,
    time::Duration,
};

use super::{
    rng::SimulationRng,
    time::{TimeSource, VirtualTime},
};

/// Unique identifier for an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(u64);

impl EventId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Types of events that can be scheduled.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventType {
    /// A network message delivery
    MessageDelivery {
        /// Source peer address
        from: SocketAddr,
        /// Target peer address
        to: SocketAddr,
        /// Message payload (serialized)
        payload: Vec<u8>,
    },
    /// A timer/wakeup event
    Timer {
        /// Peer that scheduled the timer
        peer: SocketAddr,
        /// Timer identifier
        timer_id: u64,
    },
    /// A fault injection event (partition start/end, node crash)
    Fault {
        /// Affected peers
        peers: Vec<SocketAddr>,
        /// Description of the fault
        description: String,
    },
    /// Custom event for extensibility
    Custom {
        /// Peer associated with this event
        peer: SocketAddr,
        /// Event kind identifier
        kind: String,
        /// Event data
        data: Vec<u8>,
    },
}

impl EventType {
    /// Returns the primary peer associated with this event (for ordering).
    pub fn primary_peer(&self) -> Option<SocketAddr> {
        match self {
            EventType::MessageDelivery { to, .. } => Some(*to),
            EventType::Timer { peer, .. } => Some(*peer),
            EventType::Fault { peers, .. } => peers.first().copied(),
            EventType::Custom { peer, .. } => Some(*peer),
        }
    }
}

/// A scheduled event in the simulation.
#[derive(Debug, Clone)]
pub struct Event {
    /// When this event should be processed (virtual nanos)
    pub timestamp: u64,
    /// Unique identifier for ordering ties
    pub id: EventId,
    /// The event type and payload
    pub event_type: EventType,
}

impl Event {
    fn new(timestamp: u64, id: EventId, event_type: EventType) -> Self {
        Self {
            timestamp,
            id,
            event_type,
        }
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse ordering so smallest timestamp comes first
        // Tie-breaking order:
        // 1. Timestamp (earlier first)
        // 2. Primary peer address (for determinism)
        // 3. Event type (via derived Ord)
        // 4. Event ID (registration order)
        match other.timestamp.cmp(&self.timestamp) {
            Ordering::Equal => {
                let self_peer = self.event_type.primary_peer();
                let other_peer = other.event_type.primary_peer();
                match other_peer.cmp(&self_peer) {
                    Ordering::Equal => match other.event_type.cmp(&self.event_type) {
                        Ordering::Equal => other.id.0.cmp(&self.id.0),
                        ord => ord,
                    },
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

/// Configuration for the scheduler.
#[derive(Debug, Clone, Default)]
pub struct SchedulerConfig {
    /// Maximum events to process per step (0 = unlimited)
    pub max_events_per_step: usize,
    /// Whether to log events as they're processed
    pub trace_events: bool,
}

/// Deterministic event scheduler for simulation.
///
/// Processes events in timestamp order with deterministic tie-breaking.
/// All randomness goes through the seeded RNG.
pub struct Scheduler {
    /// Virtual time for the simulation
    time: VirtualTime,
    /// Seeded RNG for deterministic decisions
    rng: SimulationRng,
    /// Priority queue of pending events
    pending_events: BinaryHeap<Event>,
    /// Counter for generating unique event IDs
    next_event_id: u64,
    /// Log of processed events (for replay verification)
    event_log: Vec<Event>,
    /// Configuration
    config: SchedulerConfig,
}

impl Scheduler {
    /// Creates a new scheduler with the given seed.
    pub fn new(seed: u64) -> Self {
        Self::with_config(seed, SchedulerConfig::default())
    }

    /// Creates a new scheduler with the given seed and configuration.
    pub fn with_config(seed: u64, config: SchedulerConfig) -> Self {
        Self {
            time: VirtualTime::new(),
            rng: SimulationRng::new(seed),
            pending_events: BinaryHeap::new(),
            next_event_id: 0,
            event_log: Vec::new(),
            config,
        }
    }

    /// Returns a reference to the virtual time.
    pub fn time(&self) -> &VirtualTime {
        &self.time
    }

    /// Returns a mutable reference to the virtual time.
    pub fn time_mut(&mut self) -> &mut VirtualTime {
        &mut self.time
    }

    /// Returns the current virtual time in nanoseconds.
    pub fn now(&self) -> u64 {
        self.time.now_nanos()
    }

    /// Returns a reference to the RNG.
    pub fn rng(&self) -> &SimulationRng {
        &self.rng
    }

    /// Returns the seed used for this scheduler.
    pub fn seed(&self) -> u64 {
        self.rng.seed()
    }

    /// Returns the number of pending events.
    pub fn pending_count(&self) -> usize {
        self.pending_events.len()
    }

    /// Returns the event log for replay verification.
    pub fn event_log(&self) -> &[Event] {
        &self.event_log
    }

    /// Clears the event log.
    pub fn clear_event_log(&mut self) {
        self.event_log.clear();
    }

    /// Schedules an event at the given absolute timestamp.
    pub fn schedule_at(&mut self, timestamp: u64, event_type: EventType) -> EventId {
        let id = EventId(self.next_event_id);
        self.next_event_id += 1;

        let event = Event::new(timestamp, id, event_type);
        self.pending_events.push(event);
        id
    }

    /// Schedules an event after the given delay from now.
    pub fn schedule_after(&mut self, delay: Duration, event_type: EventType) -> EventId {
        let timestamp = self.now().saturating_add(delay.as_nanos() as u64);
        self.schedule_at(timestamp, event_type)
    }

    /// Schedules an event at the current time.
    pub fn schedule_now(&mut self, event_type: EventType) -> EventId {
        self.schedule_at(self.now(), event_type)
    }

    /// Cancels a pending event by ID.
    ///
    /// Returns true if the event was found and cancelled.
    pub fn cancel(&mut self, id: EventId) -> bool {
        let mut events: Vec<_> = std::mem::take(&mut self.pending_events).into_vec();
        let original_len = events.len();
        events.retain(|e| e.id != id);
        let cancelled = events.len() < original_len;
        self.pending_events = BinaryHeap::from(events);
        cancelled
    }

    /// Returns the timestamp of the next pending event, if any.
    pub fn next_event_time(&self) -> Option<u64> {
        self.pending_events.peek().map(|e| e.timestamp)
    }

    /// Processes the next pending event, advancing time if necessary.
    ///
    /// Returns the processed event, or None if no events are pending.
    pub fn step(&mut self) -> Option<Event> {
        let event = self.pending_events.pop()?;

        // Advance time to the event's timestamp if needed
        if event.timestamp > self.now() {
            self.time.advance_to(event.timestamp);
        }

        if self.config.trace_events {
            tracing::trace!(
                timestamp = event.timestamp,
                id = event.id.0,
                ?event.event_type,
                "Processing event"
            );
        }

        self.event_log.push(event.clone());
        Some(event)
    }

    /// Processes events until the given condition is true or no events remain.
    ///
    /// Returns the number of events processed.
    pub fn run_until<F>(&mut self, mut condition: F) -> usize
    where
        F: FnMut(&Scheduler) -> bool,
    {
        let mut processed = 0;
        while !condition(self) {
            if self.step().is_none() {
                break;
            }
            processed += 1;

            if self.config.max_events_per_step > 0 && processed >= self.config.max_events_per_step {
                break;
            }
        }
        processed
    }

    /// Processes events until the given time is reached.
    ///
    /// Returns the number of events processed.
    pub fn run_until_time(&mut self, target_time: u64) -> usize {
        let mut processed = 0;
        while let Some(next_time) = self.next_event_time() {
            if next_time > target_time {
                break;
            }
            if self.step().is_none() {
                break;
            }
            processed += 1;
        }

        // Advance time to target even if no events
        if self.now() < target_time {
            self.time.advance_to(target_time);
        }

        processed
    }

    /// Processes all pending events.
    ///
    /// Returns the number of events processed.
    pub fn run_all(&mut self) -> usize {
        let mut processed = 0;
        while self.step().is_some() {
            processed += 1;
        }
        processed
    }

    /// Drains all pending events without processing them.
    pub fn drain_pending(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.pending_events).into_vec()
    }
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("now", &self.now())
            .field("seed", &self.rng.seed())
            .field("pending_count", &self.pending_count())
            .field("event_log_len", &self.event_log.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn test_scheduler_basic() {
        let mut scheduler = Scheduler::new(42);

        scheduler.schedule_at(
            100,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 1,
            },
        );
        scheduler.schedule_at(
            50,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 2,
            },
        );
        scheduler.schedule_at(
            200,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 3,
            },
        );

        // Should process in timestamp order
        let e1 = scheduler.step().unwrap();
        assert_eq!(e1.timestamp, 50);

        let e2 = scheduler.step().unwrap();
        assert_eq!(e2.timestamp, 100);

        let e3 = scheduler.step().unwrap();
        assert_eq!(e3.timestamp, 200);

        assert!(scheduler.step().is_none());
    }

    #[test]
    fn test_scheduler_time_advancement() {
        let mut scheduler = Scheduler::new(42);

        scheduler.schedule_at(
            1000,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 1,
            },
        );

        assert_eq!(scheduler.now(), 0);
        scheduler.step();
        assert_eq!(scheduler.now(), 1000);
    }

    #[test]
    fn test_scheduler_same_time_ordering() {
        let mut scheduler = Scheduler::new(42);

        // Schedule events at same time but different peers
        scheduler.schedule_at(
            100,
            EventType::Timer {
                peer: addr(3000),
                timer_id: 1,
            },
        );
        scheduler.schedule_at(
            100,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 2,
            },
        );
        scheduler.schedule_at(
            100,
            EventType::Timer {
                peer: addr(2000),
                timer_id: 3,
            },
        );

        // Should be ordered by peer address for determinism
        let e1 = scheduler.step().unwrap();
        let e2 = scheduler.step().unwrap();
        let e3 = scheduler.step().unwrap();

        // All at same timestamp
        assert_eq!(e1.timestamp, 100);
        assert_eq!(e2.timestamp, 100);
        assert_eq!(e3.timestamp, 100);

        // Verify deterministic order (by peer address)
        let get_peer = |e: &Event| match &e.event_type {
            EventType::Timer { peer, .. } => *peer,
            _ => panic!("unexpected event type"),
        };

        let peers: Vec<_> = [&e1, &e2, &e3].iter().map(|e| get_peer(e)).collect();
        // Should be sorted by peer address
        let mut sorted_peers = peers.clone();
        sorted_peers.sort();
        assert_eq!(peers, sorted_peers);
    }

    #[test]
    fn test_scheduler_cancel() {
        let mut scheduler = Scheduler::new(42);

        let id1 = scheduler.schedule_at(
            100,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 1,
            },
        );
        scheduler.schedule_at(
            200,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 2,
            },
        );

        assert!(scheduler.cancel(id1));
        assert!(!scheduler.cancel(id1)); // Already cancelled

        let e = scheduler.step().unwrap();
        assert_eq!(e.timestamp, 200); // Should skip to second event
    }

    #[test]
    fn test_scheduler_run_until_time() {
        let mut scheduler = Scheduler::new(42);

        for i in 1..=5 {
            scheduler.schedule_at(
                i * 100,
                EventType::Timer {
                    peer: addr(1000),
                    timer_id: i,
                },
            );
        }

        let processed = scheduler.run_until_time(250);
        assert_eq!(processed, 2); // Events at 100 and 200
        assert_eq!(scheduler.now(), 250);
        assert_eq!(scheduler.pending_count(), 3); // Events at 300, 400, 500
    }

    #[test]
    fn test_scheduler_run_until() {
        let mut scheduler = Scheduler::new(42);

        for i in 1..=10 {
            scheduler.schedule_at(
                i * 100,
                EventType::Timer {
                    peer: addr(1000),
                    timer_id: i,
                },
            );
        }

        let processed = scheduler.run_until(|s| s.now() >= 500);
        assert_eq!(processed, 5);
        assert_eq!(scheduler.now(), 500);
    }

    #[test]
    fn test_scheduler_event_log() {
        let mut scheduler = Scheduler::new(42);

        scheduler.schedule_at(
            100,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 1,
            },
        );
        scheduler.schedule_at(
            200,
            EventType::Timer {
                peer: addr(1000),
                timer_id: 2,
            },
        );

        scheduler.run_all();

        let log = scheduler.event_log();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].timestamp, 100);
        assert_eq!(log[1].timestamp, 200);
    }

    #[test]
    fn test_scheduler_determinism() {
        fn run_simulation(seed: u64) -> Vec<(u64, EventId)> {
            let mut scheduler = Scheduler::new(seed);

            // Schedule events using RNG for timestamps
            for i in 0..20 {
                let delay = scheduler.rng().gen_range(0..1000) as u64;
                scheduler.schedule_at(
                    delay,
                    EventType::Timer {
                        peer: addr((1000 + i) as u16),
                        timer_id: i as u64,
                    },
                );
            }

            scheduler.run_all();
            scheduler
                .event_log()
                .iter()
                .map(|e| (e.timestamp, e.id))
                .collect()
        }

        // Same seed should produce identical results
        let result1 = run_simulation(42);
        let result2 = run_simulation(42);
        assert_eq!(result1, result2);

        // Different seed should produce different results
        let result3 = run_simulation(43);
        assert_ne!(result1, result3);
    }

    #[test]
    fn test_message_delivery_event() {
        let mut scheduler = Scheduler::new(42);

        scheduler.schedule_at(
            100,
            EventType::MessageDelivery {
                from: addr(1000),
                to: addr(2000),
                payload: vec![1, 2, 3],
            },
        );

        let event = scheduler.step().unwrap();
        match event.event_type {
            EventType::MessageDelivery { from, to, payload } => {
                assert_eq!(from, addr(1000));
                assert_eq!(to, addr(2000));
                assert_eq!(payload, vec![1, 2, 3]);
            }
            _ => panic!("unexpected event type"),
        }
    }
}
