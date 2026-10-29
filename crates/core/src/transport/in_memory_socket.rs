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
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use super::Socket;
use crate::simulation::{RealTime, TimeSource, VirtualTime};

/// Maximum packet size for in-memory transport (matches typical UDP MTU)
const MAX_PACKET_SIZE: usize = 65535;

/// Global registry mapping each socket address to the networks that registered it.
/// This is used during `InMemorySocket::bind()` to determine which network
/// a socket belongs to.
///
/// An address can have several owners. Simulations derive node addresses from
/// their seed and node locations, so two simulations running concurrently in
/// one process (plain `cargo test`) often register the same address. When this
/// held one network per address, the last registration won, so a socket could
/// bind into the OTHER simulation's network and exchange packets with its
/// nodes, and one network's socket teardown removed the other's registration
/// (#5676). [`resolve_address_network`] picks the owner.
///
/// Uses DashMap instead of RwLock<HashMap> for lock-free concurrent access.
/// This eliminates coarse-grained lock contention when multiple sockets
/// are being registered/looked up simultaneously during test startup.
static ADDRESS_NETWORKS: LazyLock<DashMap<SocketAddr, Vec<String>>> = LazyLock::new(DashMap::new);

thread_local! {
    /// The network each address was last registered for by THIS thread.
    ///
    /// A simulation registers its node addresses and binds their sockets on
    /// the thread that runs it (the direct and Turmoil runners both drive
    /// every node from one thread), so when an address has several owners in
    /// [`ADDRESS_NETWORKS`], the one this thread registered is the binding
    /// node's network.
    ///
    /// This separates simulations on DIFFERENT threads only. One slot per
    /// address per thread means two simulations driven by the same thread
    /// that register the same address resolve to whichever registered last,
    /// as before #5676. A node therefore resolves correctly only if no other
    /// network on its thread re-registers its address between its own
    /// registration and its bind (or a bind retried after `AddrInUse`).
    static THREAD_ADDRESS_NETWORKS: RefCell<HashMap<SocketAddr, String>> =
        RefCell::new(HashMap::new());
}

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
    {
        let mut owners = ADDRESS_NETWORKS.entry(addr).or_default();
        if !owners.iter().any(|n| n == network_name) {
            owners.push(network_name.to_string());
        }
    }
    THREAD_ADDRESS_NETWORKS.with(|m| {
        m.borrow_mut().insert(addr, network_name.to_string());
    });
}

/// Removes `network_name`'s registration of `addr`, leaving any other
/// network's registration of the same address in place.
pub(crate) fn unregister_address_network(addr: &SocketAddr, network_name: &str) {
    if let dashmap::Entry::Occupied(mut entry) = ADDRESS_NETWORKS.entry(*addr) {
        entry.get_mut().retain(|n| n != network_name);
        if entry.get().is_empty() {
            entry.remove();
        }
    }
    // `try_with`: this runs from socket `Drop`, which can happen while the
    // thread's locals are being destroyed. An `Err` means the record is
    // already gone with the thread, so there is nothing to remove.
    if let Err(_destroyed) = THREAD_ADDRESS_NETWORKS.try_with(|m| {
        let mut m = m.borrow_mut();
        if m.get(addr).is_some_and(|n| n == network_name) {
            m.remove(addr);
        }
    }) {}
}

/// Why an address could not be resolved to one network.
#[derive(Debug, PartialEq, Eq)]
enum ResolveError {
    /// No network has registered the address.
    Unregistered,
    /// Several networks registered it, and none of them from this thread.
    Ambiguous(Vec<String>),
}

/// Resolves the network a socket binding `addr` on this thread belongs to.
///
/// With one owner, that owner. With several (concurrent simulations that
/// picked the same address, #5676), the one this thread registered. Several
/// owners and no registration from this thread is an error rather than a
/// guess, since a wrong guess silently connects the node to another
/// simulation.
///
/// A registration made on one thread and bound on another (a multi-thread
/// runtime) is resolved correctly only while no other network has
/// registered the same address.
fn resolve_address_network(addr: &SocketAddr) -> Result<String, ResolveError> {
    let owners = match ADDRESS_NETWORKS.get(addr) {
        Some(owners) if !owners.is_empty() => owners.value().clone(),
        _ => return Err(ResolveError::Unregistered),
    };
    let registered_here = THREAD_ADDRESS_NETWORKS.with(|m| m.borrow().get(addr).cloned());
    if let Some(name) = registered_here.filter(|n| owners.contains(n)) {
        return Ok(name);
    }
    match <[String; 1]>::try_from(owners) {
        Ok([only]) => Ok(only),
        Err(owners) => Err(ResolveError::Ambiguous(owners)),
    }
}

/// The `io::Error` a bind returns when `addr` cannot be resolved.
fn resolve_error(addr: &SocketAddr, kind: io::ErrorKind, err: ResolveError) -> io::Error {
    match err {
        ResolveError::Unregistered => io::Error::new(
            kind,
            format!(
                "No network registered for address {addr}. \
                 Call register_address_network() before binding."
            ),
        ),
        ResolveError::Ambiguous(owners) => io::Error::new(
            kind,
            format!(
                "Address {addr} is registered by several networks ({owners:?}) and by none \
                 of them from this thread, so the socket's network is ambiguous. \
                 Call register_address_network() on the thread that binds."
            ),
        ),
    }
}

/// Clears all address-network mappings, for EVERY network in the process.
#[deprecated(
    note = "also wipes simulations running concurrently in this process (#5673); \
            use clear_network_address_mappings"
)]
pub fn clear_all_address_networks() {
    ADDRESS_NETWORKS.clear();
    THREAD_ADDRESS_NETWORKS.with(|m| m.borrow_mut().clear());
}

/// Clears address-network mappings for a specific network.
/// Retains mappings for other networks, including other networks'
/// registrations of the same addresses.
pub fn clear_network_address_mappings(network_name: &str) {
    ADDRESS_NETWORKS.retain(|_, owners| {
        owners.retain(|n| n != network_name);
        !owners.is_empty()
    });
    // `try_with`: this runs from `SimNetwork::drop`, see
    // `unregister_address_network`.
    if let Err(_destroyed) =
        THREAD_ADDRESS_NETWORKS.try_with(|m| m.borrow_mut().retain(|_, n| n != network_name))
    {}
}

/// Every in-memory socket bind in this test process: the binding thread, the
/// network the socket was bound into, and its address. Lets a test check
/// which network each of its threads' sockets joined without trusting the
/// resolution it is testing.
#[cfg(test)]
pub(crate) static BIND_LOG: std::sync::Mutex<Vec<(std::thread::ThreadId, String, SocketAddr)>> =
    std::sync::Mutex::new(Vec::new());

/// The clock type each simulated node's QUIC endpoint was built on, keyed by
/// `(network, address)`. Written by `bind_udp_listener` for an address that
/// belongs to a simulation network; read by tests to check the endpoint
/// really runs on the network's `VirtualTime`.
#[cfg(any(test, feature = "testing"))]
static QUIC_ENDPOINT_CLOCKS: LazyLock<DashMap<(String, SocketAddr), &'static str>> =
    LazyLock::new(DashMap::new);

/// Records that the QUIC endpoint at `addr` runs on a clock of type `clock`,
/// if `addr` belongs to a simulation network.
///
/// The network is resolved as the socket bind resolves it
/// ([`resolve_address_network`]), on the same thread, so when several
/// simulations share `addr` the record is keyed by the network this node's
/// socket joined, not by whichever registered `addr` last (#5676).
#[cfg(any(test, feature = "testing"))]
pub(crate) fn record_quic_endpoint_clock(addr: SocketAddr, clock: &'static str) {
    if let Ok(network) = resolve_address_network(&addr) {
        QUIC_ENDPOINT_CLOCKS.insert((network, addr), clock);
    }
}

/// The clock type the QUIC endpoint at `addr` in `network` was built on, or
/// `None` if no endpoint was built there.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn quic_endpoint_clock(network: &str, addr: SocketAddr) -> Option<&'static str> {
    QUIC_ENDPOINT_CLOCKS
        .get(&(network.to_string(), addr))
        .map(|clock| *clock)
}

/// Forgets every recorded QUIC endpoint clock of `network`.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn clear_quic_endpoint_clocks(network: &str) {
    QUIC_ENDPOINT_CLOCKS.retain(|(owner, _), _| owner != network);
}

/// Clears all network time sources, for EVERY network in the process.
#[deprecated(
    note = "also wipes simulations running concurrently in this process (#5673); \
            use unregister_network_time_source"
)]
pub fn clear_all_network_time_sources() {
    NETWORK_TIME_SOURCES.clear();
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

/// Returns the simulation `VirtualTime` for the network that owns `addr`, if any.
///
/// Used by the transport's gateway admission rate limiter so its ramp-up advances
/// with simulation time instead of real wall-clock (the connection handler is
/// otherwise built with a `RealTime` source). Returns `None` outside simulation
/// (no address/network registered), where real time is the correct clock.
pub(crate) fn get_socket_virtual_time(addr: &SocketAddr) -> Option<VirtualTime> {
    let network_name = resolve_address_network(addr).ok()?;
    get_network_time_source(&network_name)
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
/// without creating circular dependencies. Arguments: network name, source,
/// destination, and the datagram itself (so a fault can act on one class of
/// datagram, e.g. QUIC by its first byte).
pub type PacketDeliveryCallback =
    Arc<dyn Fn(&str, SocketAddr, SocketAddr, &[u8]) -> PacketDeliveryDecision + Send + Sync>;

/// Callback for queuing a packet for delayed delivery.
pub type QueuePacketCallback =
    Arc<dyn Fn(&str, u64, Vec<u8>, SocketAddr, SocketAddr) + Send + Sync>;

/// Fault-injection callbacks, keyed by the network that installed them.
///
/// Scoped per network (like every other registry in this module) because
/// several `SimNetwork`s run concurrently in one process under plain
/// `cargo test`. When this was a single process-global slot, the first
/// network to drop cleared it for every network still running, so their
/// `CrashNode` crashes silently stopped dropping packets (#5673).
static DELIVERY_CALLBACKS: LazyLock<DashMap<String, PacketDeliveryCallback>> =
    LazyLock::new(DashMap::new);

static QUEUE_PACKET_CALLBACKS: LazyLock<DashMap<String, QueuePacketCallback>> =
    LazyLock::new(DashMap::new);

/// Registers (or with `None`, removes) the packet delivery callback for one
/// network. Other networks' callbacks are unaffected.
///
/// This is called by the testing infrastructure to wire up fault injection.
pub fn set_packet_delivery_callback(network_name: &str, callback: Option<PacketDeliveryCallback>) {
    match callback {
        Some(cb) => {
            DELIVERY_CALLBACKS.insert(network_name.to_string(), cb);
        }
        None => {
            DELIVERY_CALLBACKS.remove(network_name);
        }
    }
}

/// Registers (or with `None`, removes) the queue packet callback for virtual
/// time delivery on one network. Other networks' callbacks are unaffected.
pub fn set_queue_packet_callback(network_name: &str, callback: Option<QueuePacketCallback>) {
    match callback {
        Some(cb) => {
            QUEUE_PACKET_CALLBACKS.insert(network_name.to_string(), cb);
        }
        None => {
            QUEUE_PACKET_CALLBACKS.remove(network_name);
        }
    }
}

/// Returns whether `network_name` currently has a delivery callback installed.
#[cfg(test)]
pub(crate) fn has_packet_delivery_callback(network_name: &str) -> bool {
    DELIVERY_CALLBACKS.contains_key(network_name)
}

/// Checks if a packet should be delivered based on this network's fault
/// injection callback. Networks without one deliver everything.
pub(crate) fn check_packet_delivery(
    network_name: &str,
    from: SocketAddr,
    to: SocketAddr,
    datagram: &[u8],
) -> PacketDeliveryDecision {
    // Clone the Arc out so no map shard guard is held while the callback runs
    // (it locks the network's fault injector).
    let callback = DELIVERY_CALLBACKS
        .get(network_name)
        .map(|r| r.value().clone());
    match callback {
        Some(cb) => cb(network_name, from, to, datagram),
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
    let callback = QUEUE_PACKET_CALLBACKS
        .get(network_name)
        .map(|r| r.value().clone());
    if let Some(cb) = callback {
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

/// Removes the socket registry for a specific network entirely.
///
/// Unlike `clear_network_sockets` which clears socket contents but leaves
/// the DashMap entry, this removes the entry itself. Called during
/// `SimNetwork::Drop` to keep global state clean between sequential
/// simulation runs (e.g., in determinism tests).
pub fn remove_network_socket_registry(network_name: &str) {
    SOCKET_REGISTRIES.remove(network_name);
}

/// Clears all socket registries, for EVERY network in the process.
#[deprecated(
    note = "also wipes simulations running concurrently in this process (#5673); \
            use remove_network_socket_registry"
)]
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
        let network_name = resolve_address_network(&addr)
            .map_err(|e| resolve_error(&addr, io::ErrorKind::Other, e))?;

        #[cfg(test)]
        BIND_LOG.lock().unwrap_or_else(|e| e.into_inner()).push((
            std::thread::current().id(),
            network_name.clone(),
            addr,
        ));

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
        self.send_now(buf, target)
    }

    /// Non-blocking send: the same fault-injection decision and delivery as
    /// [`Self::send_to`], which never waits either. An in-memory socket has
    /// no send buffer, so this never returns `WouldBlock`.
    pub fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.send_now(buf, target)
    }

    /// The body of [`Self::send_to`] and [`Self::try_send_to`]. A delayed
    /// delivery is a spawned task on the time source, so nothing here waits.
    fn send_now(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        if buf.len() > MAX_PACKET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too large",
            ));
        }

        let data = buf.to_vec();

        match check_packet_delivery(&self.network_name, self.addr, target, buf) {
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

        match check_packet_delivery(&self.network_name, self.addr, target, buf) {
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

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        InMemorySocket::<RealTime>::try_send_to(self, buf, target)
    }
}

impl<T: TimeSource> Drop for InMemorySocket<T> {
    fn drop(&mut self) {
        unregister_socket(&self.network_name, &self.addr);
        unregister_address_network(&self.addr, &self.network_name);
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
        let network_name = resolve_address_network(&addr)
            .map_err(|e| resolve_error(&addr, io::ErrorKind::NotFound, e))?;

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

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.0.try_send_to(buf, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    /// Delivery callbacks are scoped per network (#5673): installing one
    /// network's callback does not replace another's, and removing one (as
    /// `SimNetwork::Drop` does) leaves every other network's in force.
    #[test]
    fn delivery_callbacks_are_scoped_per_network() {
        let net_a = "callback-scope-a";
        let net_b = "callback-scope-b";
        let from: SocketAddr = "127.0.0.1:10101".parse().unwrap();
        let to: SocketAddr = "127.0.0.1:10102".parse().unwrap();
        let is_drop = |d: PacketDeliveryDecision| matches!(d, PacketDeliveryDecision::Drop);

        let drop_all: PacketDeliveryCallback =
            Arc::new(|_: &str, _: SocketAddr, _: SocketAddr, _: &[u8]| {
                PacketDeliveryDecision::Drop
            });
        let deliver_all: PacketDeliveryCallback =
            Arc::new(|_: &str, _: SocketAddr, _: SocketAddr, _: &[u8]| {
                PacketDeliveryDecision::Deliver
            });

        set_packet_delivery_callback(net_a, Some(drop_all));
        set_packet_delivery_callback(net_b, Some(deliver_all));
        assert!(
            is_drop(check_packet_delivery(net_a, from, to, &[])),
            "installing network B's callback must not replace network A's"
        );
        assert!(!is_drop(check_packet_delivery(net_b, from, to, &[])));

        set_packet_delivery_callback(net_b, None);
        assert!(
            is_drop(check_packet_delivery(net_a, from, to, &[])),
            "removing network B's callback must leave network A's in force"
        );

        set_packet_delivery_callback(net_a, None);
        assert!(
            !is_drop(check_packet_delivery(net_a, from, to, &[])),
            "a network with no callback delivers everything"
        );

        // The queue callback has the same per-network scoping.
        let queued_on = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let record = |log: Arc<std::sync::Mutex<Vec<String>>>| -> QueuePacketCallback {
            Arc::new(move |net: &str, _, _, _, _| log.lock().unwrap().push(net.to_string()))
        };
        set_queue_packet_callback(net_a, Some(record(queued_on.clone())));
        set_queue_packet_callback(net_b, Some(record(queued_on.clone())));
        set_queue_packet_callback(net_b, None);
        queue_packet_for_delivery(net_a, 0, vec![1], from, to);
        queue_packet_for_delivery(net_b, 0, vec![2], from, to);
        assert_eq!(
            *queued_on.lock().unwrap(),
            vec![net_a.to_string()],
            "removing network B's queue callback must leave network A's in force"
        );
        set_queue_packet_callback(net_a, None);
    }

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
        // No test registers this address.
        let addr: SocketAddr = "127.0.0.1:30001".parse().unwrap();

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

    /// `try_send_to` takes the fault-injection decision `send_to` takes, for
    /// every kind of decision: one target per kind, each datagram sent once
    /// through each method, and the two methods' outcomes compared at each
    /// target. Delayed delivery runs on the network's `VirtualTime`, so the
    /// test moves that clock rather than waiting.
    #[tokio::test]
    async fn simulation_socket_try_send_to_takes_the_same_fault_decision_as_send_to() {
        use futures::FutureExt;

        let network = "test-try-send-parity";
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        let addr = |port: u16| -> SocketAddr { SocketAddr::from(([127, 0, 0, 1], port)) };
        let (sender, drop_to, deliver_to, queue_to, delay_to) = (
            addr(41001),
            addr(41002),
            addr(41003),
            addr(41004),
            addr(41005),
        );
        for a in [sender, drop_to, deliver_to, queue_to, delay_to] {
            register_address_network(a, network);
        }
        const DEADLINE: u64 = 7_000;
        const DELAY: Duration = Duration::from_millis(30);
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, _, to, _| match to {
                t if t == drop_to => PacketDeliveryDecision::Drop,
                t if t == queue_to => PacketDeliveryDecision::QueuedDelivery { deadline: DEADLINE },
                t if t == delay_to => PacketDeliveryDecision::DelayedDelivery(DELAY),
                _ => PacketDeliveryDecision::Deliver,
            })),
        );
        let queued = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = queued.clone();
        set_queue_packet_callback(
            network,
            Some(Arc::new(move |_, deadline, data, from, to| {
                log.lock().unwrap().push((deadline, data, from, to));
            })),
        );

        let send = SimulationSocket::bind(sender).await.unwrap();
        let mut receivers = Vec::new();
        for a in [drop_to, deliver_to, queue_to, delay_to] {
            receivers.push((a, SimulationSocket::bind(a).await.unwrap()));
        }
        let inbox = |socket: &SimulationSocket| -> Vec<Vec<u8>> {
            let mut got = Vec::new();
            let mut buf = [0u8; 64];
            while let Some(Ok((len, _))) = socket.recv_from(&mut buf).now_or_never() {
                got.push(buf[..len].to_vec());
            }
            got
        };

        for (target, _) in &receivers {
            assert_eq!(Socket::send_to(&send, b"async", *target).await.unwrap(), 5);
            assert_eq!(Socket::try_send_to(&send, b"try", *target).unwrap(), 3);
        }
        // Let the delayed deliveries' tasks register their sleeps, then move
        // the clock past the delay and let them deliver.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let at_once: Vec<Vec<Vec<u8>>> = receivers.iter().map(|(_, s)| inbox(s)).collect();
        time.advance(DELAY);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let after_delay: Vec<Vec<Vec<u8>>> = receivers.iter().map(|(_, s)| inbox(s)).collect();

        let both = vec![b"async".to_vec(), b"try".to_vec()];
        // Drop: neither arrives. Deliver: both at once. Queue: neither
        // arrives here, both go to the queue callback. Delay: both, after
        // the delay only.
        assert_eq!(at_once, vec![vec![], both.clone(), vec![], vec![]]);
        assert_eq!(after_delay, vec![vec![], vec![], vec![], both.clone()]);
        assert_eq!(
            *queued.lock().unwrap(),
            vec![
                (DEADLINE, b"async".to_vec(), sender, queue_to),
                (DEADLINE, b"try".to_vec(), sender, queue_to),
            ]
        );

        // The size limit is the same too.
        let too_big = vec![0u8; MAX_PACKET_SIZE + 1];
        let async_err = Socket::send_to(&send, &too_big, deliver_to)
            .await
            .unwrap_err();
        let try_err = Socket::try_send_to(&send, &too_big, deliver_to).unwrap_err();
        assert_eq!(async_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(try_err.kind(), io::ErrorKind::InvalidInput);

        set_packet_delivery_callback(network, None);
        set_queue_packet_callback(network, None);
        unregister_network_time_source(network);
    }

    /// Two simulations that register the SAME address, each on its own
    /// thread, each bind a socket in their own network (#5676).
    ///
    /// Simulations derive node addresses from their seed and node locations,
    /// so concurrent simulations in one `cargo test` process collide on
    /// addresses. With one network per address, the last registration won:
    /// network A's node bound into network B, exchanged packets with B's
    /// nodes, read B's clock for its gateway rate limiter, and B's socket
    /// teardown removed A's registration.
    ///
    /// The interleaving that loses is forced with barriers rather than left
    /// to the scheduler: A registers, then B registers, then both bind. Both
    /// simulations are built by this test, so it fails the same way under
    /// nextest and plain `cargo test`.
    #[test]
    fn concurrent_networks_sharing_an_address_each_bind_their_own() {
        use futures::FutureExt;
        use std::sync::Barrier;

        const NET_A: &str = "addr-collision-5676-a";
        const NET_B: &str = "addr-collision-5676-b";
        // The address both networks register, and a second node in A only.
        let shared: SocketAddr = "[fd00::5676]:1".parse().unwrap();
        let a_sender: SocketAddr = "[fd00::5676]:2".parse().unwrap();
        // A's clock is set ahead of B's, so a lookup of the wrong network's
        // clock is visible.
        const A_CLOCK_AHEAD: Duration = Duration::from_secs(5);

        #[derive(Debug, PartialEq)]
        struct Seen {
            /// Network the shared address's socket was bound into.
            bound_into: Result<String, String>,
            /// Clock reading of the VirtualTime resolved for the shared address.
            clock_nanos: Option<u64>,
            /// Packets the shared socket received from A's other node.
            received: Vec<Vec<u8>>,
            /// Whether the shared address still resolves for this thread after
            /// the OTHER network's socket at the same address was dropped.
            resolves_after_other_dropped: Option<u64>,
        }

        let after_register_a = Arc::new(Barrier::new(2));
        let after_register_b = Arc::new(Barrier::new(2));
        let after_send = Arc::new(Barrier::new(2));
        let after_b_dropped = Arc::new(Barrier::new(2));

        let run = |name: &'static str,
                   is_a: bool,
                   barriers: [Arc<Barrier>; 4]|
         -> std::thread::JoinHandle<Seen> {
            std::thread::spawn(move || {
                let [
                    after_register_a,
                    after_register_b,
                    after_send,
                    after_b_dropped,
                ] = barriers;
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let time = VirtualTime::new();
                    if is_a {
                        time.advance(A_CLOCK_AHEAD);
                    }
                    register_network_time_source(name, time);

                    if is_a {
                        register_address_network(shared, name);
                        register_address_network(a_sender, name);
                    }
                    after_register_a.wait();
                    if !is_a {
                        register_address_network(shared, name);
                    }
                    after_register_b.wait();

                    let socket = SimulationSocket::bind(shared).await;
                    let bound_into = socket
                        .as_ref()
                        .map(|s| s.0.network_name.clone())
                        .map_err(|e| e.to_string());
                    let clock_nanos = get_socket_virtual_time(&shared).map(|t| t.now_nanos());

                    if is_a {
                        let sender = SimulationSocket::bind(a_sender).await.unwrap();
                        sender.send_to(b"to-a", shared).await.unwrap();
                    }
                    after_send.wait();

                    let mut received = Vec::new();
                    if let Ok(socket) = &socket {
                        let mut buf = [0u8; 64];
                        while let Some(Ok((len, _))) = socket.recv_from(&mut buf).now_or_never() {
                            received.push(buf[..len].to_vec());
                        }
                    }

                    // B tears its socket down first; A then checks its own
                    // registration survived.
                    if !is_a {
                        drop(socket);
                        after_b_dropped.wait();
                        clear_network_address_mappings(name);
                        unregister_network_time_source(name);
                        return Seen {
                            bound_into,
                            clock_nanos,
                            received,
                            resolves_after_other_dropped: None,
                        };
                    }
                    after_b_dropped.wait();
                    let resolves_after_other_dropped =
                        get_socket_virtual_time(&shared).map(|t| t.now_nanos());
                    drop(socket);
                    clear_network_address_mappings(name);
                    unregister_network_time_source(name);
                    Seen {
                        bound_into,
                        clock_nanos,
                        received,
                        resolves_after_other_dropped,
                    }
                })
            })
        };
        let barriers = || {
            [
                after_register_a.clone(),
                after_register_b.clone(),
                after_send.clone(),
                after_b_dropped.clone(),
            ]
        };
        let a = run(NET_A, true, barriers());
        let b = run(NET_B, false, barriers());
        let a = a.join().expect("network A's thread panicked");
        let b = b.join().expect("network B's thread panicked");

        let a_clock = A_CLOCK_AHEAD.as_nanos() as u64;
        assert_eq!(
            a,
            Seen {
                bound_into: Ok(NET_A.to_string()),
                clock_nanos: Some(a_clock),
                received: vec![b"to-a".to_vec()],
                resolves_after_other_dropped: Some(a_clock),
            },
            "network A's node must bind into A, read A's clock, receive A's \
             packet, and keep its registration when B's socket is dropped"
        );
        assert_eq!(
            b,
            Seen {
                bound_into: Ok(NET_B.to_string()),
                clock_nanos: Some(0),
                received: vec![],
                resolves_after_other_dropped: None,
            },
            "network B's node must bind into B, read B's clock, and see none \
             of A's packets"
        );
    }

    /// An address registered by two networks, neither from the binding
    /// thread, is refused rather than bound into either (#5676).
    #[tokio::test]
    async fn bind_refuses_an_address_ambiguous_to_this_thread() {
        let addr: SocketAddr = "[fd00::5676]:3".parse().unwrap();
        let (net_a, net_b) = ("ambiguous-5676-a", "ambiguous-5676-b");
        std::thread::spawn(move || {
            register_address_network(addr, net_a);
            register_address_network(addr, net_b);
        })
        .join()
        .unwrap();

        let err = InMemorySocket::bind(addr).await.unwrap_err();
        assert!(
            err.to_string().contains("ambiguous"),
            "unexpected error: {err}"
        );

        // With one owner left, a bind from any thread resolves to it.
        clear_network_address_mappings(net_a);
        let socket = InMemorySocket::bind(addr).await.unwrap();
        assert_eq!(socket.network_name, net_b);
        drop(socket);
        clear_network_address_mappings(net_b);
    }
}
