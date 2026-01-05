//! In-memory socket implementation for testing.
//!
//! This module provides an in-memory implementation of the `Socket` trait
//! that allows the production event loop (`P2pConnManager`) to be used in
//! tests without real UDP sockets.
//!
//! The implementation uses network-scoped registries to route packets between
//! sockets, enabling test isolation when multiple SimNetwork instances run
//! concurrently. Each network (identified by name) has its own socket registry.
//!
//! # Usage
//!
//! Before binding an `InMemorySocket`, register the address with its network:
//!
//! ```ignore
//! use freenet::transport::in_memory_socket::{register_address_network, InMemorySocket};
//!
//! register_address_network(addr, "my-test-network");
//! let socket = InMemorySocket::bind(addr).await?;
//! ```

use crate::config::GlobalExecutor;

use dashmap::DashMap;
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use super::Socket;
use crate::simulation::{RealTime, TimeSource, VirtualTime};

/// Maximum packet size for in-memory transport (matches typical UDP MTU)
const MAX_PACKET_SIZE: usize = 65535;

/// Global registry mapping socket addresses to their network names.
/// This is used during `InMemorySocket::bind()` to determine which network
/// a socket belongs to. The mapping is thread-safe and works across async tasks.
///
/// Uses DashMap instead of RwLock<HashMap> for lock-free concurrent access.
/// This eliminates coarse-grained lock contention when multiple sockets
/// are being registered/looked up simultaneously during test startup.
static ADDRESS_NETWORKS: LazyLock<DashMap<SocketAddr, String>> = LazyLock::new(DashMap::new);

/// Global registry mapping network names to their VirtualTime instances.
/// This enables SimulationSocket to use network-scoped VirtualTime for
/// deterministic time control in simulation tests.
///
/// Each SimNetwork registers its VirtualTime here on creation and
/// unregisters it on drop, providing test isolation.
static NETWORK_TIME_SOURCES: LazyLock<DashMap<String, VirtualTime>> = LazyLock::new(DashMap::new);

/// Registers a socket address with a network name.
///
/// This must be called before `InMemorySocket::bind()` to specify which
/// network the socket belongs to. Sockets in different networks are isolated
/// and cannot communicate with each other.
///
/// This function is thread-safe and can be called from any thread.
///
/// # Example
/// ```ignore
/// register_address_network(addr, "test-network-1");
/// let socket = InMemorySocket::bind(addr).await?;
/// ```
pub fn register_address_network(addr: SocketAddr, network_name: &str) {
    ADDRESS_NETWORKS.insert(addr, network_name.to_string());
}

/// Unregisters a socket address from the network mapping.
fn unregister_address_network(addr: &SocketAddr) {
    ADDRESS_NETWORKS.remove(addr);
}

/// Gets the network name for an address, if registered.
fn get_address_network(addr: &SocketAddr) -> Option<String> {
    ADDRESS_NETWORKS.get(addr).map(|r| r.value().clone())
}

/// Clears all address-network mappings. Useful for test cleanup.
pub fn clear_all_address_networks() {
    ADDRESS_NETWORKS.clear();
}

/// Registers a VirtualTime instance for a network.
///
/// This should be called when creating a SimNetwork to enable
/// SimulationSocket to use the network's VirtualTime for deterministic
/// time control.
///
/// # Arguments
/// * `network_name` - The unique name of the simulation network
/// * `virtual_time` - The VirtualTime instance to use for this network
pub fn register_network_time_source(network_name: &str, virtual_time: VirtualTime) {
    NETWORK_TIME_SOURCES.insert(network_name.to_string(), virtual_time);
}

/// Unregisters a VirtualTime instance for a network.
///
/// This should be called when dropping a SimNetwork to clean up.
pub fn unregister_network_time_source(network_name: &str) {
    NETWORK_TIME_SOURCES.remove(network_name);
}

/// Gets the VirtualTime for a network, if registered.
fn get_network_time_source(network_name: &str) -> Option<VirtualTime> {
    NETWORK_TIME_SOURCES.get(network_name).map(|r| r.clone())
}

/// Result of checking packet delivery through fault injection.
#[derive(Debug)]
pub enum PacketDeliveryDecision {
    /// Deliver immediately
    Deliver,
    /// Delay delivery (real-time mode)
    DelayedDelivery(Duration),
    /// Queue for virtual time delivery
    QueuedDelivery {
        /// Virtual time deadline in nanoseconds
        deadline: u64,
    },
    /// Drop the packet
    Drop,
}

/// Callback type for packet delivery decisions.
///
/// This allows the fault injection logic to be injected from outside
/// without creating circular dependencies.
pub type PacketDeliveryCallback =
    Arc<dyn Fn(&str, SocketAddr, SocketAddr) -> PacketDeliveryDecision + Send + Sync>;

/// Callback for queuing a packet for delayed delivery.
pub type QueuePacketCallback =
    Arc<dyn Fn(&str, u64, Vec<u8>, SocketAddr, SocketAddr) + Send + Sync>;

/// Global callbacks for fault injection integration.
static DELIVERY_CALLBACK: LazyLock<RwLock<Option<PacketDeliveryCallback>>> =
    LazyLock::new(|| RwLock::new(None));

static QUEUE_PACKET_CALLBACK: LazyLock<RwLock<Option<QueuePacketCallback>>> =
    LazyLock::new(|| RwLock::new(None));

/// Registers the packet delivery callback for fault injection.
///
/// This is called by the testing infrastructure to wire up fault injection.
pub fn set_packet_delivery_callback(callback: Option<PacketDeliveryCallback>) {
    *DELIVERY_CALLBACK.write().unwrap() = callback;
}

/// Registers the queue packet callback for virtual time delivery.
pub fn set_queue_packet_callback(callback: Option<QueuePacketCallback>) {
    *QUEUE_PACKET_CALLBACK.write().unwrap() = callback;
}

/// Checks if a packet should be delivered based on fault injection config.
fn check_packet_delivery(
    network_name: &str,
    from: SocketAddr,
    to: SocketAddr,
) -> PacketDeliveryDecision {
    let callback = DELIVERY_CALLBACK.read().unwrap();
    match callback.as_ref() {
        Some(cb) => cb(network_name, from, to),
        None => PacketDeliveryDecision::Deliver,
    }
}

/// Queues a packet for delayed delivery in virtual time mode.
fn queue_packet_for_delivery(
    network_name: &str,
    deadline: u64,
    data: Vec<u8>,
    from: SocketAddr,
    target: SocketAddr,
) {
    let callback = QUEUE_PACKET_CALLBACK.read().unwrap();
    if let Some(cb) = callback.as_ref() {
        cb(network_name, deadline, data, from, target);
    }
}

/// A received packet with source address.
#[derive(Debug, Clone)]
struct ReceivedPacket {
    data: Vec<u8>,
    from: SocketAddr,
}

/// Per-socket inbox for received packets.
///
/// Uses std::sync::Mutex instead of tokio::sync::Mutex for deterministic behavior.
/// The push/pop operations are very fast (just VecDeque operations), so blocking
/// is acceptable and eliminates the non-deterministic try_lock + spawn pattern.
struct SocketInbox {
    /// Queue of received packets (protected by std Mutex for deterministic locking)
    packets: std::sync::Mutex<VecDeque<ReceivedPacket>>,
    /// Notifier for when packets arrive
    notify: Arc<tokio::sync::Notify>,
}

impl SocketInbox {
    fn new() -> Self {
        Self {
            packets: std::sync::Mutex::new(VecDeque::new()),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Push a packet to the inbox. Uses blocking lock for deterministic ordering.
    fn push(&self, data: Vec<u8>, from: SocketAddr) {
        self.packets
            .lock()
            .unwrap()
            .push_back(ReceivedPacket { data, from });
        self.notify.notify_one();
    }

    /// Pop a packet from the inbox. Uses blocking lock for deterministic ordering.
    fn pop(&self) -> Option<ReceivedPacket> {
        self.packets.lock().unwrap().pop_front()
    }

    fn notifier(&self) -> Arc<tokio::sync::Notify> {
        self.notify.clone()
    }
}

/// Registry for sockets within a single network.
///
/// Uses BTreeMap instead of HashMap to ensure deterministic iteration order
/// for reproducible simulation tests. SocketAddr implements Ord, making it
/// suitable as a BTreeMap key.
#[derive(Default)]
struct SocketRegistry {
    /// Maps socket address to inbox (BTreeMap for deterministic ordering)
    sockets: BTreeMap<SocketAddr, Arc<SocketInbox>>,
}

impl SocketRegistry {
    fn register(&mut self, addr: SocketAddr) -> Arc<SocketInbox> {
        let inbox = Arc::new(SocketInbox::new());
        self.sockets.insert(addr, inbox.clone());
        inbox
    }

    fn unregister(&mut self, addr: &SocketAddr) {
        self.sockets.remove(addr);
    }

    /// Deliver a packet to a target socket.
    ///
    /// Uses blocking lock internally for deterministic ordering - no async spawning
    /// that could introduce non-determinism.
    fn deliver_packet(&self, target: SocketAddr, data: Vec<u8>, from: SocketAddr) -> bool {
        if let Some(inbox) = self.sockets.get(&target) {
            // Deterministic: uses std::sync::Mutex internally, no spawning
            inbox.push(data, from);
            true
        } else {
            tracing::trace!(target = %target, "No socket registered at target address");
            false
        }
    }

    fn is_registered(&self, addr: &SocketAddr) -> bool {
        self.sockets.contains_key(addr)
    }
}

/// Global map of network name -> socket registry.
///
/// Each SimNetwork gets its own registry, providing test isolation.
///
/// Uses DashMap instead of RwLock<HashMap> for lock-free concurrent access.
/// This is particularly beneficial for the get-or-create pattern in
/// `get_or_create_registry()`, which previously required dropping a read
/// lock and reacquiring a write lock. DashMap's `entry()` API handles
/// this atomically without the lock upgrade overhead.
static SOCKET_REGISTRIES: LazyLock<DashMap<String, Arc<RwLock<SocketRegistry>>>> =
    LazyLock::new(DashMap::new);

/// Gets or creates the socket registry for a network.
fn get_or_create_registry(network_name: &str) -> Arc<RwLock<SocketRegistry>> {
    // DashMap's entry API handles the get-or-insert atomically,
    // eliminating the previous read-lock-check-then-write-lock pattern.
    SOCKET_REGISTRIES
        .entry(network_name.to_string())
        .or_insert_with(|| Arc::new(RwLock::new(SocketRegistry::default())))
        .value()
        .clone()
}

/// Gets the socket registry for a network, if it exists.
fn get_registry(network_name: &str) -> Option<Arc<RwLock<SocketRegistry>>> {
    SOCKET_REGISTRIES
        .get(network_name)
        .map(|r| r.value().clone())
}

/// Delivers a packet to a target socket within a network.
///
/// This is called by `FaultInjectorState::advance_time()` to deliver
/// delayed packets when their deadline is reached.
pub fn deliver_packet_to_network(
    network_name: &str,
    target: SocketAddr,
    data: Vec<u8>,
    from: SocketAddr,
) -> bool {
    if let Some(registry) = get_registry(network_name) {
        registry.read().unwrap().deliver_packet(target, data, from)
    } else {
        tracing::warn!(
            network = %network_name,
            "Attempted to deliver packet to non-existent network"
        );
        false
    }
}

/// Checks if a socket is registered at the given address in a network.
pub fn is_socket_registered(network_name: &str, addr: &SocketAddr) -> bool {
    get_registry(network_name)
        .map(|r| r.read().unwrap().is_registered(addr))
        .unwrap_or(false)
}

/// Unregisters a socket from a network's registry.
pub fn unregister_socket(network_name: &str, addr: &SocketAddr) {
    if let Some(registry) = get_registry(network_name) {
        registry.write().unwrap().unregister(addr);
    }
}

/// Clears all registered sockets for a network (useful for test cleanup).
pub fn clear_network_sockets(network_name: &str) {
    if let Some(registry) = get_registry(network_name) {
        registry.write().unwrap().sockets.clear();
    }
}

/// Clears all socket registries (useful between test runs).
pub fn clear_all_socket_registries() {
    SOCKET_REGISTRIES.clear();
}

/// An in-memory socket implementing the `Socket` trait.
///
/// This allows the production `P2pConnManager` and event loop to be used
/// in tests with simulated network conditions.
///
/// Sockets are scoped to a network (identified by name). Sockets in different
/// networks cannot communicate with each other, providing test isolation.
///
/// Supports virtual time for deterministic testing via the `TimeSource` trait.
pub struct InMemorySocket<T: TimeSource = RealTime> {
    /// Network this socket belongs to
    network_name: String,
    /// This socket's bound address
    addr: SocketAddr,
    /// Inbox for received packets (uses internal std::sync::Mutex for determinism)
    inbox: Arc<SocketInbox>,
    /// Notifier for packet arrival
    notify: Arc<tokio::sync::Notify>,
    /// Time source for sleep operations
    time_source: T,
}

impl<T: TimeSource> std::fmt::Debug for InMemorySocket<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemorySocket")
            .field("network_name", &self.network_name)
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl<T: TimeSource> InMemorySocket<T> {
    /// Create a new in-memory socket with a custom time source.
    ///
    /// This is the generic constructor for testing with virtual time.
    pub async fn bind_with_time_source(addr: SocketAddr, time_source: T) -> io::Result<Self> {
        let network_name = get_address_network(&addr).ok_or_else(|| {
            io::Error::other(format!(
                "No network registered for address {}. Call register_address_network() before binding InMemorySocket.",
                addr
            ))
        })?;

        let registry = get_or_create_registry(&network_name);
        let inbox = registry.write().unwrap().register(addr);
        let notify = inbox.notifier();

        tracing::debug!(network = %network_name, addr = %addr, "InMemorySocket bound");

        Ok(Self {
            network_name,
            addr,
            inbox,
            notify,
            time_source,
        })
    }

    /// Receive data from the socket.
    ///
    /// Uses blocking std::sync::Mutex internally for deterministic behavior.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            // Try to get a packet from inbox (blocking lock for determinism)
            if let Some(packet) = self.inbox.pop() {
                let len = packet.data.len().min(buf.len());
                buf[..len].copy_from_slice(&packet.data[..len]);
                return Ok((len, packet.from));
            }

            // Wait for notification of new packet
            self.notify.notified().await;
        }
    }

    /// Generic async send_to implementation using the time source.
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        if buf.len() > MAX_PACKET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too large",
            ));
        }

        let data = buf.to_vec();

        match check_packet_delivery(&self.network_name, self.addr, target) {
            PacketDeliveryDecision::Drop => {
                // Silently drop - from sender's perspective, packet was sent
                Ok(buf.len())
            }
            PacketDeliveryDecision::DelayedDelivery(delay) => {
                // Spawn task to deliver after delay using time source
                let network_name = self.network_name.clone();
                let from = self.addr;
                let time_source = self.time_source.clone();
                GlobalExecutor::spawn(async move {
                    time_source.sleep(delay).await;
                    deliver_packet_to_network(&network_name, target, data, from);
                });
                Ok(buf.len())
            }
            PacketDeliveryDecision::QueuedDelivery { deadline } => {
                // Queue for virtual time delivery
                queue_packet_for_delivery(&self.network_name, deadline, data, self.addr, target);
                Ok(buf.len())
            }
            PacketDeliveryDecision::Deliver => {
                // Immediate delivery
                deliver_packet_to_network(&self.network_name, target, data, self.addr);
                Ok(buf.len())
            }
        }
    }

    /// Synchronous send for use in blocking contexts (e.g., spawn_blocking).
    pub fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        if buf.len() > MAX_PACKET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too large",
            ));
        }

        let data = buf.to_vec();

        match check_packet_delivery(&self.network_name, self.addr, target) {
            PacketDeliveryDecision::Drop => Ok(buf.len()),
            PacketDeliveryDecision::DelayedDelivery(delay) => {
                // For blocking context, just sleep and deliver
                // Note: For VirtualTime, this uses wall-clock sleep; consider using
                // blocking simulation methods if deterministic tests require it.
                std::thread::sleep(delay);
                deliver_packet_to_network(&self.network_name, target, data, self.addr);
                Ok(buf.len())
            }
            PacketDeliveryDecision::QueuedDelivery { deadline } => {
                // Queue for virtual time
                queue_packet_for_delivery(&self.network_name, deadline, data, self.addr, target);
                Ok(buf.len())
            }
            PacketDeliveryDecision::Deliver => {
                deliver_packet_to_network(&self.network_name, target, data, self.addr);
                Ok(buf.len())
            }
        }
    }
}

impl Socket for InMemorySocket<RealTime> {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        Self::bind_with_time_source(addr, RealTime::new()).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        InMemorySocket::<RealTime>::recv_from(self, buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        InMemorySocket::<RealTime>::send_to(self, buf, target).await
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        InMemorySocket::<RealTime>::send_to_blocking(self, buf, target)
    }
}

impl<T: TimeSource> Drop for InMemorySocket<T> {
    fn drop(&mut self) {
        unregister_socket(&self.network_name, &self.addr);
        unregister_address_network(&self.addr);
        tracing::debug!(
            network = %self.network_name,
            addr = %self.addr,
            "InMemorySocket dropped"
        );
    }
}

// =============================================================================
// SimulationSocket - VirtualTime-backed socket for deterministic simulation
// =============================================================================

/// A simulation socket that uses VirtualTime for deterministic simulation.
///
/// This socket uses the network's VirtualTime instance (registered by SimNetwork)
/// for all time operations. VirtualTime only advances when explicitly called via
/// `VirtualTime::advance()`, providing fully deterministic test behavior independent
/// of wall-clock time.
///
/// # Test Isolation
///
/// Each SimNetwork has a unique network name. Sockets are isolated by network
/// name through the address registry, ensuring parallel tests don't interfere.
///
/// # Usage
///
/// SimulationSocket is used automatically by SimNetwork's in-memory node builder.
/// You don't need to create it directly.
pub struct SimulationSocket(InMemorySocket<VirtualTime>);

impl SimulationSocket {
    /// Bind to the given address (public wrapper for tests)
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        <Self as Socket>::bind(addr).await
    }

    /// Receive a packet (public wrapper for tests)
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buf).await
    }

    /// Send a packet (public wrapper for tests)
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.0.send_to(buf, target).await
    }
}

impl std::fmt::Debug for SimulationSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulationSocket")
            .field("addr", &self.0.addr)
            .field("network", &self.0.network_name)
            .finish()
    }
}

impl Socket for SimulationSocket {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        // Look up the network name for this address to verify it's registered
        let network_name = get_address_network(&addr).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "No network registered for address {}. \
                     Call register_address_network() before binding SimulationSocket.",
                    addr
                ),
            )
        })?;

        // Get the VirtualTime for this network (required for simulation)
        let virtual_time = get_network_time_source(&network_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "No VirtualTime registered for network '{}'. \
                     SimNetwork should register VirtualTime before nodes bind sockets.",
                    network_name
                ),
            )
        })?;

        tracing::debug!(
            addr = %addr,
            network = %network_name,
            "SimulationSocket binding with VirtualTime"
        );

        // Create the socket with VirtualTime for deterministic simulation
        let inner = InMemorySocket::bind_with_time_source(addr, virtual_time).await?;
        Ok(Self(inner))
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.0.send_to(buf, target).await
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.0.send_to_blocking(buf, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    #[tokio::test]
    async fn test_socket_bind_and_send() {
        let network = "test-bind-send";
        clear_network_sockets(network);

        let addr1: SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10002".parse().unwrap();

        // Register addresses with network before binding
        register_address_network(addr1, network);
        register_address_network(addr2, network);

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
    async fn test_network_isolation() {
        let network1 = "test-isolation-1";
        let network2 = "test-isolation-2";
        clear_network_sockets(network1);
        clear_network_sockets(network2);

        let addr1: SocketAddr = "127.0.0.1:20001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:20002".parse().unwrap();

        // Create socket in network1
        register_address_network(addr1, network1);
        let socket1 = InMemorySocket::bind(addr1).await.unwrap();

        // Create socket in network2 with a different address
        // (same address would fail since addr1 is already registered)
        register_address_network(addr2, network2);
        let socket2 = InMemorySocket::bind(addr2).await.unwrap();

        // Verify they're in different registries
        assert!(is_socket_registered(network1, &addr1));
        assert!(is_socket_registered(network2, &addr2));
        assert!(!is_socket_registered(network1, &addr2));
        assert!(!is_socket_registered(network2, &addr1));

        // Clean up
        drop(socket1);
        drop(socket2);
    }

    #[tokio::test]
    async fn test_bind_without_registration_fails() {
        // Use a fresh address that's not registered
        let addr: SocketAddr = "127.0.0.1:30001".parse().unwrap();
        unregister_address_network(&addr); // Ensure it's not registered

        let result = InMemorySocket::bind(addr).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("No network registered"));
    }

    #[tokio::test]
    async fn test_socket_with_virtual_time() {
        let network = "test-virtual-time";
        clear_network_sockets(network);

        let addr1: SocketAddr = "127.0.0.1:40001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:40002".parse().unwrap();

        // Create virtual time instance
        let time_source = VirtualTime::new();

        // Register addresses with network before binding
        register_address_network(addr1, network);
        register_address_network(addr2, network);

        // Bind sockets with virtual time
        let socket1 = InMemorySocket::bind_with_time_source(addr1, time_source.clone())
            .await
            .unwrap();
        let socket2 = InMemorySocket::bind_with_time_source(addr2, time_source.clone())
            .await
            .unwrap();

        // Send from socket1 to socket2
        let msg = b"hello from virtual time";
        socket1.send_to(msg, addr2).await.unwrap();

        // Receive on socket2
        let mut buf = [0u8; 100];
        let (len, from) = socket2.recv_from(&mut buf).await.unwrap();

        assert_eq!(&buf[..len], msg);
        assert_eq!(from, addr1);
    }
}
