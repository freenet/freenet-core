//! In-memory socket implementation for testing.
//!
//! This module provides an in-memory implementation of the `Socket` trait
//! that allows the production event loop (`P2pConnManager`) to be used in
//! tests without real UDP sockets.
//!
//! The implementation uses a global registry to route packets between sockets,
//! and integrates with the existing fault injection infrastructure for
//! deterministic testing.

use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use tokio::sync::Mutex;

use super::Socket;
use crate::simulation::{FaultConfig, SimulationRng, TimeSource, VirtualTime};

/// Maximum packet size for in-memory transport (matches typical UDP MTU)
const MAX_PACKET_SIZE: usize = 65535;

/// A packet pending delivery at a virtual time deadline.
#[derive(Debug, Clone)]
pub struct PendingPacket {
    /// Virtual time deadline (nanoseconds) when packet should be delivered
    pub deadline: u64,
    /// The packet data
    pub data: Vec<u8>,
    /// Source address
    pub from: SocketAddr,
    /// Target address
    pub target: SocketAddr,
}

/// Statistics for socket-level fault injection.
#[derive(Debug, Clone, Default)]
pub struct SocketNetworkStats {
    /// Total packets sent
    pub packets_sent: u64,
    /// Packets delivered immediately
    pub packets_delivered: u64,
    /// Packets dropped due to message loss rate
    pub packets_dropped_loss: u64,
    /// Packets dropped due to network partitions
    pub packets_dropped_partition: u64,
    /// Packets dropped due to crashed nodes
    pub packets_dropped_crash: u64,
    /// Packets queued for delayed delivery (VirtualTime mode)
    pub packets_queued: u64,
    /// Packets delivered after delay
    pub packets_delayed_delivered: u64,
    /// Total latency added (in nanoseconds)
    pub total_latency_nanos: u64,
}

impl SocketNetworkStats {
    /// Returns total dropped packets.
    pub fn total_dropped(&self) -> u64 {
        self.packets_dropped_loss + self.packets_dropped_partition + self.packets_dropped_crash
    }

    /// Returns the packet loss ratio (0.0 to 1.0).
    pub fn loss_ratio(&self) -> f64 {
        if self.packets_sent == 0 {
            0.0
        } else {
            self.total_dropped() as f64 / self.packets_sent as f64
        }
    }

    /// Returns average latency per delayed packet.
    pub fn average_latency(&self) -> Duration {
        if self.packets_delayed_delivered == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.total_latency_nanos / self.packets_delayed_delivered)
        }
    }
}

/// State for socket-level fault injection.
pub struct SocketFaultInjector {
    /// Fault configuration
    pub config: FaultConfig,
    /// Seeded RNG for deterministic decisions
    pub rng: SimulationRng,
    /// Optional VirtualTime for deterministic latency
    pub virtual_time: Option<VirtualTime>,
    /// Packets pending delivery in VirtualTime mode
    pub pending_packets: Vec<PendingPacket>,
    /// Network statistics
    pub stats: SocketNetworkStats,
}

impl SocketFaultInjector {
    /// Creates a new fault injector with the given config and seed.
    pub fn new(config: FaultConfig, seed: u64) -> Self {
        Self {
            config,
            rng: SimulationRng::new(seed),
            virtual_time: None,
            pending_packets: Vec::new(),
            stats: SocketNetworkStats::default(),
        }
    }

    /// Enables VirtualTime mode.
    pub fn with_virtual_time(mut self, vt: VirtualTime) -> Self {
        self.virtual_time = Some(vt);
        self
    }

    /// Returns reference to VirtualTime if enabled.
    pub fn virtual_time(&self) -> Option<&VirtualTime> {
        self.virtual_time.as_ref()
    }

    /// Advances virtual time and delivers pending packets.
    /// Returns number of packets delivered.
    pub fn advance_time(&mut self) -> usize {
        let Some(ref vt) = self.virtual_time else {
            return 0;
        };

        let now = vt.now_nanos();
        let mut delivered = 0;

        // Find packets ready for delivery
        let ready: Vec<usize> = self
            .pending_packets
            .iter()
            .enumerate()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(i, _)| i)
            .collect();

        // Deliver in reverse order to maintain indices
        for idx in ready.into_iter().rev() {
            let packet = self.pending_packets.remove(idx);
            let registry = SOCKET_REGISTRY.read().unwrap();
            if registry.deliver_packet(packet.target, packet.data, packet.from) {
                delivered += 1;
                self.stats.packets_delayed_delivered += 1;
            }
        }

        delivered
    }

    /// Returns reference to stats.
    pub fn stats(&self) -> &SocketNetworkStats {
        &self.stats
    }

    /// Resets statistics.
    pub fn reset_stats(&mut self) {
        self.stats = SocketNetworkStats::default();
    }
}

/// Reason for dropping a packet.
#[derive(Debug, Clone, Copy)]
pub enum PacketDropReason {
    RandomLoss,
    Partition,
    NodeCrashed,
}

/// Result of checking packet delivery.
#[derive(Debug)]
pub enum PacketDeliveryDecision {
    /// Deliver immediately
    Deliver,
    /// Delay delivery (real-time mode)
    DelayedDelivery(Duration),
    /// Queue for virtual time delivery
    QueuedDelivery { deadline: u64, latency: Duration },
    /// Drop the packet
    Drop(PacketDropReason),
}

/// Global fault injector for socket-level testing.
static SOCKET_FAULT_INJECTOR: LazyLock<RwLock<Option<Arc<std::sync::Mutex<SocketFaultInjector>>>>> =
    LazyLock::new(|| RwLock::new(None));

/// Sets the global socket fault injector.
pub fn set_socket_fault_injector(injector: Option<Arc<std::sync::Mutex<SocketFaultInjector>>>) {
    *SOCKET_FAULT_INJECTOR.write().unwrap() = injector;
}

/// Gets the global socket fault injector.
pub fn get_socket_fault_injector() -> Option<Arc<std::sync::Mutex<SocketFaultInjector>>> {
    SOCKET_FAULT_INJECTOR.read().unwrap().clone()
}

/// Checks if a packet should be delivered.
fn check_packet_delivery(from: SocketAddr, to: SocketAddr) -> PacketDeliveryDecision {
    let Some(injector) = get_socket_fault_injector() else {
        return PacketDeliveryDecision::Deliver;
    };

    let mut state = injector.lock().unwrap();
    state.stats.packets_sent += 1;

    // Check if sender or receiver is crashed
    if state.config.is_crashed(&from) || state.config.is_crashed(&to) {
        state.stats.packets_dropped_crash += 1;
        return PacketDeliveryDecision::Drop(PacketDropReason::NodeCrashed);
    }

    // Get current virtual time
    let current_time = state
        .virtual_time
        .as_ref()
        .map(|vt| vt.now_nanos())
        .unwrap_or(0);

    // Check for partition
    if state.config.is_partitioned(&from, &to, current_time) {
        state.stats.packets_dropped_partition += 1;
        return PacketDeliveryDecision::Drop(PacketDropReason::Partition);
    }

    // Check message loss
    if state.config.should_drop_message(&state.rng) {
        state.stats.packets_dropped_loss += 1;
        return PacketDeliveryDecision::Drop(PacketDropReason::RandomLoss);
    }

    // Check latency injection
    if let Some(latency) = state.config.generate_latency(&state.rng) {
        if let Some(ref vt) = state.virtual_time {
            let deadline = vt.now_nanos() + latency.as_nanos() as u64;
            state.stats.packets_queued += 1;
            state.stats.total_latency_nanos += latency.as_nanos() as u64;
            return PacketDeliveryDecision::QueuedDelivery { deadline, latency };
        } else {
            state.stats.total_latency_nanos += latency.as_nanos() as u64;
            return PacketDeliveryDecision::DelayedDelivery(latency);
        }
    }

    state.stats.packets_delivered += 1;
    PacketDeliveryDecision::Deliver
}

/// A received packet with source address.
#[derive(Debug, Clone)]
struct ReceivedPacket {
    data: Vec<u8>,
    from: SocketAddr,
}

/// Per-socket inbox for received packets.
struct SocketInbox {
    /// Queue of received packets
    packets: VecDeque<ReceivedPacket>,
    /// Notifier for when packets arrive
    notify: Arc<tokio::sync::Notify>,
}

impl SocketInbox {
    fn new() -> Self {
        Self {
            packets: VecDeque::new(),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn push(&mut self, data: Vec<u8>, from: SocketAddr) {
        self.packets.push_back(ReceivedPacket { data, from });
        self.notify.notify_one();
    }

    fn pop(&mut self) -> Option<ReceivedPacket> {
        self.packets.pop_front()
    }

    fn notifier(&self) -> Arc<tokio::sync::Notify> {
        self.notify.clone()
    }
}

/// Global registry for in-memory sockets.
struct SocketRegistry {
    /// Maps socket address to inbox
    sockets: HashMap<SocketAddr, Arc<Mutex<SocketInbox>>>,
}

impl Default for SocketRegistry {
    fn default() -> Self {
        Self {
            sockets: HashMap::new(),
        }
    }
}

impl SocketRegistry {
    fn register(&mut self, addr: SocketAddr) -> Arc<Mutex<SocketInbox>> {
        let inbox = Arc::new(Mutex::new(SocketInbox::new()));
        self.sockets.insert(addr, inbox.clone());
        inbox
    }

    fn unregister(&mut self, addr: &SocketAddr) {
        self.sockets.remove(addr);
    }

    fn deliver_packet(&self, target: SocketAddr, data: Vec<u8>, from: SocketAddr) -> bool {
        if let Some(inbox) = self.sockets.get(&target) {
            // Use try_lock to avoid blocking - if we can't get the lock, spawn a task
            if let Ok(mut guard) = inbox.try_lock() {
                guard.push(data, from);
                true
            } else {
                // Inbox is locked, spawn delivery task
                let inbox = inbox.clone();
                tokio::spawn(async move {
                    inbox.lock().await.push(data, from);
                });
                true
            }
        } else {
            tracing::trace!(target = %target, "No socket registered at target address");
            false
        }
    }

    fn is_registered(&self, addr: &SocketAddr) -> bool {
        self.sockets.contains_key(addr)
    }
}

/// Global socket registry.
static SOCKET_REGISTRY: LazyLock<RwLock<SocketRegistry>> =
    LazyLock::new(|| RwLock::new(SocketRegistry::default()));

/// Checks if a socket is registered at the given address.
pub fn is_socket_registered(addr: &SocketAddr) -> bool {
    SOCKET_REGISTRY.read().unwrap().is_registered(addr)
}

/// Unregisters a socket from the global registry.
pub fn unregister_socket(addr: &SocketAddr) {
    SOCKET_REGISTRY.write().unwrap().unregister(addr);
}

/// Clears all registered sockets (useful between tests).
pub fn clear_socket_registry() {
    SOCKET_REGISTRY.write().unwrap().sockets.clear();
}

/// An in-memory socket implementing the `Socket` trait.
///
/// This allows the production `P2pConnManager` and event loop to be used
/// in tests with simulated network conditions.
pub struct InMemorySocket {
    /// This socket's bound address
    addr: SocketAddr,
    /// Inbox for received packets
    inbox: Arc<Mutex<SocketInbox>>,
    /// Notifier for packet arrival
    notify: Arc<tokio::sync::Notify>,
}

impl Socket for InMemorySocket {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let inbox = SOCKET_REGISTRY.write().unwrap().register(addr);
        let notify = inbox.lock().await.notifier();

        tracing::debug!(addr = %addr, "InMemorySocket bound");

        Ok(Self { addr, inbox, notify })
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            // Try to get a packet from inbox
            {
                let mut inbox = self.inbox.lock().await;
                if let Some(packet) = inbox.pop() {
                    let len = packet.data.len().min(buf.len());
                    buf[..len].copy_from_slice(&packet.data[..len]);
                    return Ok((len, packet.from));
                }
            }

            // Wait for notification of new packet
            self.notify.notified().await;
        }
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        if buf.len() > MAX_PACKET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too large",
            ));
        }

        let data = buf.to_vec();

        match check_packet_delivery(self.addr, target) {
            PacketDeliveryDecision::Drop(_) => {
                // Silently drop - from sender's perspective, packet was sent
                Ok(buf.len())
            }
            PacketDeliveryDecision::DelayedDelivery(delay) => {
                // Spawn task to deliver after delay
                let from = self.addr;
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let registry = SOCKET_REGISTRY.read().unwrap();
                    registry.deliver_packet(target, data, from);
                });
                Ok(buf.len())
            }
            PacketDeliveryDecision::QueuedDelivery { deadline, .. } => {
                // Queue for virtual time delivery
                if let Some(injector) = get_socket_fault_injector() {
                    let mut state = injector.lock().unwrap();
                    state.pending_packets.push(PendingPacket {
                        deadline,
                        data,
                        from: self.addr,
                        target,
                    });
                }
                Ok(buf.len())
            }
            PacketDeliveryDecision::Deliver => {
                // Immediate delivery
                let registry = SOCKET_REGISTRY.read().unwrap();
                registry.deliver_packet(target, data, self.addr);
                Ok(buf.len())
            }
        }
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        if buf.len() > MAX_PACKET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too large",
            ));
        }

        let data = buf.to_vec();

        match check_packet_delivery(self.addr, target) {
            PacketDeliveryDecision::Drop(_) => Ok(buf.len()),
            PacketDeliveryDecision::DelayedDelivery(delay) => {
                // For blocking context, just sleep and deliver
                std::thread::sleep(delay);
                let registry = SOCKET_REGISTRY.read().unwrap();
                registry.deliver_packet(target, data, self.addr);
                Ok(buf.len())
            }
            PacketDeliveryDecision::QueuedDelivery { deadline, .. } => {
                // Queue for virtual time
                if let Some(injector) = get_socket_fault_injector() {
                    let mut state = injector.lock().unwrap();
                    state.pending_packets.push(PendingPacket {
                        deadline,
                        data,
                        from: self.addr,
                        target,
                    });
                }
                Ok(buf.len())
            }
            PacketDeliveryDecision::Deliver => {
                let registry = SOCKET_REGISTRY.read().unwrap();
                registry.deliver_packet(target, data, self.addr);
                Ok(buf.len())
            }
        }
    }
}

impl Drop for InMemorySocket {
    fn drop(&mut self) {
        SOCKET_REGISTRY.write().unwrap().unregister(&self.addr);
        tracing::debug!(addr = %self.addr, "InMemorySocket dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::FaultConfigBuilder;

    #[tokio::test]
    async fn test_socket_bind_and_send() {
        clear_socket_registry();

        let addr1: SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10002".parse().unwrap();

        let socket1 = InMemorySocket::bind(addr1).await.unwrap();
        let socket2 = InMemorySocket::bind(addr2).await.unwrap();

        // Send from socket1 to socket2
        let msg = b"hello";
        socket1.send_to(msg, addr2).await.unwrap();

        // Receive on socket2
        let mut buf = [0u8; 100];
        let (len, from) = socket2.recv_from(&mut buf).await.unwrap();

        assert_eq!(&buf[..len], msg);
        assert_eq!(from, addr1);
    }

    #[tokio::test]
    async fn test_socket_with_fault_injection() {
        clear_socket_registry();
        set_socket_fault_injector(None);

        let addr1: SocketAddr = "127.0.0.1:10003".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10004".parse().unwrap();

        // Configure 100% message loss
        let config = FaultConfigBuilder::default()
            .message_loss_rate(1.0)
            .build();
        let injector = Arc::new(std::sync::Mutex::new(SocketFaultInjector::new(config, 42)));
        set_socket_fault_injector(Some(injector.clone()));

        let socket1 = InMemorySocket::bind(addr1).await.unwrap();
        let _socket2 = InMemorySocket::bind(addr2).await.unwrap();

        // Send should succeed (from sender's perspective)
        let msg = b"hello";
        let result = socket1.send_to(msg, addr2).await;
        assert!(result.is_ok());

        // Check stats
        let stats = injector.lock().unwrap().stats.clone();
        assert_eq!(stats.packets_sent, 1);
        assert_eq!(stats.packets_dropped_loss, 1);
        assert_eq!(stats.packets_delivered, 0);

        // Cleanup
        set_socket_fault_injector(None);
    }

    #[tokio::test]
    async fn test_socket_stats() {
        let stats = SocketNetworkStats {
            packets_sent: 100,
            packets_delivered: 70,
            packets_dropped_loss: 15,
            packets_dropped_partition: 10,
            packets_dropped_crash: 5,
            packets_queued: 0,
            packets_delayed_delivered: 20,
            total_latency_nanos: 100_000_000,
        };

        assert_eq!(stats.total_dropped(), 30);
        assert!((stats.loss_ratio() - 0.3).abs() < 0.001);
        assert_eq!(stats.average_latency(), Duration::from_millis(5));
    }
}
