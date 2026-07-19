//! The main node data type which encapsulates all the behaviour for maintaining a connection
//! and performing operations within the network.
//!
//! This module contains the primary event loop (`NodeP2P::run_node`) that orchestrates
//! interactions between different components like the network, operations, contracts, and clients.
//! It receives events and dispatches actions via channels.
//!
//! # Implementations
//! Node comes with different underlying implementations that can be used upon construction.
//! Those implementations are:
//! - libp2p: all the connection is handled by libp2p.
//! - in-memory: a simplifying node used for emulation purposes mainly.
//! - inter-process: similar to in-memory, but can be rana cross multiple processes, closer to the real p2p impl
//!
//! The main node data structure and execution loop.
//! See [`../../architecture.md`](../../architecture.md) for a high-level overview of the node's role and the event loop interactions.

use anyhow::Context;
use freenet_stdlib::{
    client_api::{ClientRequest, ErrorKind},
    prelude::ContractInstanceId,
};
use std::{
    borrow::Cow,
    fs::File,
    io::Read,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use std::{collections::HashSet, convert::Infallible};

use self::p2p_impl::NodeP2P;
use crate::{
    client_events::{BoxedClient, ClientEventsProxy, ClientId, OpenRequest},
    config::{Address, GatewayConfig, WebsocketApiConfig},
    contract::{ExecutorError, NetworkContractHandler},
    local_node::Executor,
    message::{InnerMessage, NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{OpError, connect, get, put, subscribe, update},
    ring::{Location, PeerKeyLocation},
    tracing::{EventRegister, NetEventLog, NetEventRegister},
};
use crate::{
    config::Config,
    message::{MessageStats, NetMessageV1},
};
use freenet_stdlib::client_api::DelegateRequest;
use serde::{Deserialize, Serialize};

pub(crate) use network_bridge::{
    ConnectionError, EventLoopNotificationsSender, NetworkBridge, OpExecutionPayload, WaiterReply,
};
// Re-export the UPDATE-broadcast stream-assembly telemetry global (#4440) so the
// `Ring` snapshot task can read it (the broadcast queue lives behind the private
// `network_bridge` module). Mirrors `crate::transport::metrics::TRANSPORT_METRICS`.
// (The module-cache metrics were a sibling process-global until #4488 made them
// a per-node `Arc`.)
pub(crate) use network_bridge::broadcast_queue::BROADCAST_STREAM_METRICS;
// Re-export the summary-first PUT version gate (#4642 step 3-bis) so
// `ring::connection_manager::ConnectionManager::supports_summary_first_put`
// can consult it — the gate lives behind the private `network_bridge`
// module, so a sibling top-level module (`ring`) needs a targeted
// re-export rather than a path through `network_bridge` directly. Mirrors
// `BROADCAST_STREAM_METRICS` above.
pub(crate) use network_bridge::p2p_protoc::{
    SUMMARY_FIRST_PUT_MIN_VERSION, version_supports_summary_first_put,
};
#[cfg(test)]
pub(crate) use network_bridge::{EventLoopNotificationsReceiver, event_loop_notification_channel};
// Re-export types for dev_tool and testing
pub use network_bridge::{EventLoopExitReason, NetworkStats, reset_channel_id_counter};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::OpManager;

mod network_bridge;

// Re-export fault injection types for test infrastructure.
// No cfg gate: underlying items are unconditionally compiled and integration
// tests compile the lib without cfg(test).
pub use network_bridge::in_memory::{FaultInjectorState, get_fault_injector, set_fault_injector};
pub(crate) mod background_task_monitor;
pub(crate) mod neighbor_hosting;
pub(crate) mod network_status;
mod op_state_manager;
mod p2p_impl;
mod request_router;
pub(crate) mod resource_metrics;
pub(crate) mod testing_impl;

pub(crate) use p2p_impl::abort_process_on_redb_poison;
pub use p2p_impl::{
    enable_abort_on_fatal_listener_exit, enable_abort_on_redb_poison, enable_fast_crash_exit_code,
};
pub use request_router::{DeduplicatedRequest, RequestRouter};

/// Handle to trigger graceful shutdown of the node.
#[derive(Clone)]
pub struct ShutdownHandle {
    tx: tokio::sync::mpsc::Sender<NodeEvent>,
    /// Counter of currently-running client-originated driver tasks
    /// (`run_client_put` / `_get` / `_update` / `_subscribe`). Read by
    /// `shutdown` to wait for those tasks to finish before triggering
    /// the Disconnect.
    inflight_client_ops: Arc<std::sync::atomic::AtomicUsize>,
    /// Admission gate flipped by `shutdown` *before* the drain begins,
    /// so `start_client_*` can fail fast with `OpError::NodeShuttingDown`
    /// instead of slipping a new op into the post-drain race window.
    /// Same `Arc` is held by `OpManager::shutting_down` so the gate is
    /// visible to the spawn sites without a separate channel.
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    /// Maximum time to wait for `inflight_client_ops` to reach zero
    /// before forcing the disconnect anyway. `Duration::ZERO` disables
    /// the drain (legacy immediate-disconnect behaviour).
    drain_timeout: std::time::Duration,
}

impl ShutdownHandle {
    /// Trigger a graceful shutdown of the node.
    ///
    /// Three-phase shutdown — order matters:
    ///
    /// 1. **Close admission**: flip `OpManager::shutting_down` so
    ///    `start_client_{put,get,update,subscribe}` immediately
    ///    refuse new work with `OpError::NodeShuttingDown`. Without
    ///    this, a new client op could spawn between the drain
    ///    observing `counter == 0` and Disconnect being sent — that
    ///    op would bump the counter (now unobserved) and then get
    ///    cut off by the Disconnect. (Codex reviewer call-out
    ///    2026-05.)
    /// 2. **Drain**: wait up to `drain_timeout` for the in-flight
    ///    client-op counter to reach zero. Without this wait, a
    ///    SIGTERM arriving mid-PUT (e.g. release-driven auto-update
    ///    on the nova gateway) drops the client's WebSocket
    ///    mid-operation. See the rationale on
    ///    `Config::shutdown_drain_secs`.
    /// 3. **Disconnect**: send `NodeEvent::Disconnect`, which closes
    ///    peer connections and exits the event loop.
    ///
    /// Scope limitation: the drain covers **client-originated**
    /// drivers only. In-flight *relay* operations (peer-to-peer
    /// PUT/GET this node is forwarding) are NOT drained — those are
    /// short-lived per-message work and the peer can re-attempt. The
    /// targeted failure mode is user-facing WS client requests (the
    /// `freenet-git` mirror), not relay traffic.
    pub async fn shutdown(&self) {
        use std::sync::atomic::Ordering;

        // Phase 1: close admission BEFORE the drain. Subsequent
        // start_client_* calls fail fast. SeqCst is required for the
        // Dekker-style handshake with `admit_client_op` — see
        // `OpManager::admit_client_op` rustdoc for the full memory-
        // ordering analysis (Codex r3 + skeptical r3).
        self.shutting_down.store(true, Ordering::SeqCst);

        // Phase 2: drain.
        self.wait_for_drain().await;

        // Phase 3: trigger event-loop teardown.
        if let Err(err) = self
            .tx
            .send(NodeEvent::Disconnect {
                cause: Some("graceful shutdown".into()),
            })
            .await
        {
            tracing::debug!(
                error = %err,
                "failed to send graceful shutdown signal; shutdown channel may already be closed"
            );
        }
    }

    /// Poll-loop the in-flight client-op counter until it hits zero or
    /// `drain_timeout` expires. Cap each individual sleep at 200ms so
    /// the drain can react promptly when the counter clears.
    ///
    /// Counter loads use `SeqCst` so they synchronize with
    /// `ClientOpGuard::new`'s `fetch_add(SeqCst)` — without this, the
    /// Dekker-style handshake described in
    /// `OpManager::admit_client_op` would let a racing client bump
    /// go unobserved (Codex r3 + skeptical r3 finding).
    async fn wait_for_drain(&self) {
        use std::sync::atomic::Ordering;

        if self.drain_timeout.is_zero() {
            return;
        }
        let initial = self.inflight_client_ops.load(Ordering::SeqCst);
        if initial == 0 {
            return;
        }
        tracing::info!(
            initial,
            drain_timeout_secs = self.drain_timeout.as_secs(),
            "Shutdown drain: waiting for in-flight client ops to finish"
        );

        // `tokio::time` is appropriate here even under the
        // `TimeSource`-or-bust rule for crates/core: shutdown drain is
        // a process-exit code path that wall-clock blocks on real
        // tokio sleeps, has no analogue in simulation tests, and is
        // explicitly bounded by `drain_timeout`.
        let drained = tokio::time::timeout(self.drain_timeout, async {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First `tick` fires immediately; advance past it so the
            // loop body actually sleeps between checks.
            tick.tick().await;
            loop {
                // SeqCst: participates in the admission handshake
                // (see admit_client_op rustdoc). Relaxed here could
                // let the poll see a stale 0 even after a racing
                // bump, missing a late-arrived op.
                if self.inflight_client_ops.load(Ordering::SeqCst) == 0 {
                    return;
                }
                tick.tick().await;
            }
        })
        .await;

        // Final log-only read after the drain decision — Relaxed is
        // fine, this doesn't gate any further action.
        let remaining = self.inflight_client_ops.load(Ordering::Relaxed);
        match drained {
            Ok(()) => tracing::info!(initial, "Shutdown drain complete (all client ops finished)"),
            Err(_) => tracing::warn!(
                initial,
                remaining,
                drain_timeout_secs = self.drain_timeout.as_secs(),
                "Shutdown drain timed out; proceeding with disconnect"
            ),
        }
    }
}

pub struct Node {
    inner: NodeP2P,
    shutdown_handle: ShutdownHandle,
}

impl Node {
    pub fn update_location(&mut self, location: Location) {
        self.inner
            .op_manager
            .ring
            .connection_manager
            .update_location(Some(location));
    }

    /// Get a handle that can be used to trigger graceful shutdown.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown_handle.clone()
    }

    pub async fn run(self) -> anyhow::Result<Infallible> {
        self.inner.run_node().await
    }
}

/// When instancing a node you can either join an existing network or bootstrap a new network with a listener
/// which will act as the initial provider. This initial peer will be listening at the provided port and assigned IP.
/// If those are not free the instancing process will return an error.
///
/// In order to bootstrap a new network the following arguments are required to be provided to the builder:
/// - ip: IP associated to the initial node.
/// - port: listening port of the initial node.
///
/// If both are provided but also additional peers are added via the [`Self::add_gateway()`] method, this node will
/// be listening but also try to connect to an existing peer.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[non_exhaustive] // avoid directly instantiating this struct
pub struct NodeConfig {
    /// Determines if an initial connection should be attempted.
    /// Only true for an initial gateway/node. If false, the gateway will be disconnected unless other peers connect through it.
    pub should_connect: bool,
    pub is_gateway: bool,
    /// If not specified, a key is generated and used when creating the node.
    pub key_pair: TransportKeypair,
    // optional local info, in case this is an initial bootstrap node
    /// IP to bind to the network listener.
    pub network_listener_ip: IpAddr,
    /// socket port to bind to the network listener.
    pub network_listener_port: u16,
    /// Our own external socket address, if known (set for gateways, learned for peers).
    pub(crate) own_addr: Option<SocketAddr>,
    pub(crate) config: Arc<Config>,
    /// At least one gateway is required for joining the network.
    /// Not necessary if this is an initial node.
    pub(crate) gateways: Vec<InitPeerNode>,
    /// the location of this node, used for gateways.
    pub(crate) location: Option<Location>,
    pub(crate) max_hops_to_live: Option<usize>,
    pub(crate) rnd_if_htl_above: Option<usize>,
    pub(crate) max_number_conn: Option<usize>,
    pub(crate) min_number_conn: Option<usize>,
    pub(crate) max_upstream_bandwidth: Option<Rate>,
    pub(crate) max_downstream_bandwidth: Option<Rate>,
    pub(crate) blocked_addresses: Option<HashSet<SocketAddr>>,
    pub(crate) transient_budget: usize,
    pub(crate) transient_ttl: Duration,
    /// Minimum ring connections before this peer advertises readiness
    /// to accept non-CONNECT operations. `None` or `Some(0)` disables the gate.
    /// Default: `Some(3)` in production.
    #[serde(default)]
    pub(crate) relay_ready_connections: Option<usize>,
    /// Test-only override for the governance manager's configuration.
    /// Lets simulation tests compress the production minute-to-hour
    /// timescales and lower `min_samples` so the rate-limit → MAD →
    /// evict → ban chain can be exercised within a paused-time sim.
    /// `None` in production (and never serialized — `#[serde(skip)]`),
    /// where `GovernanceConfig::default()` is used. See issue #4301.
    ///
    /// Not cfg-gated: `node::testing_impl` (which sets this) is compiled
    /// unconditionally, so the field must exist in every build. The
    /// `Option` is simply always `None` outside tests.
    #[serde(skip)]
    pub(crate) governance_config_override: Option<crate::contract::governance::GovernanceConfig>,
    /// Test-only override for the placement-migration version floor
    /// (`SUBSCRIBE_HINT_MIN_VERSION`). The floor is consulted as
    /// `subscribe_hint_floor_override().unwrap_or(SUBSCRIBE_HINT_MIN_VERSION)`
    /// on both the send gate (`p2p_protoc::peer_supports_subscribe_hint`) and the
    /// receive gate (`node::handle_pure_network_message_v1`).
    ///
    /// In production this is `None` → the real `(0,2,80)` floor (untouched).
    ///
    /// In simulations the crate version is now AT/ABOVE the real floor, so a
    /// `None` override would make the `SubscribeHint` gate fire in EVERY sim and
    /// pile migration load onto unrelated simulations (the #4601 regression that
    /// reddened the 500-node nightly). `SimNetwork` therefore defaults this to an
    /// unreachable floor (`SimNetwork::SIM_MIGRATION_DISABLED_FLOOR`) — FAIL-CLOSED,
    /// so migration is genuinely OPT-IN. A test exercising the cascade calls
    /// `SimNetwork::enable_placement_migration`, which sets it to `Some((0,0,0))`
    /// for its own nodes; every other sim stays off and cannot be perturbed.
    ///
    /// Not cfg-gated for the same reason as `governance_config_override`:
    /// `node::testing_impl` sets it and is compiled unconditionally.
    /// `#[serde(skip)]`; never serialized.
    #[serde(skip)]
    pub(crate) subscribe_hint_floor_override: Option<(u8, u8, u16)>,
    /// Test-only override for the summary-first PUT probe version floor
    /// (`SUMMARY_FIRST_PUT_MIN_VERSION`). Threaded exactly like
    /// `subscribe_hint_floor_override` above: consulted as
    /// `summary_first_put_floor_override().unwrap_or(SUMMARY_FIRST_PUT_MIN_VERSION)`
    /// by `ConnectionManager::supports_summary_first_put`, the send gate the
    /// originator's PUT driver checks before probing a target.
    ///
    /// In production this is `None` → the real `(0, 2, 95)` floor (untouched).
    ///
    /// In simulations every `SimNetwork` defaults this to an unreachable
    /// floor (`SimNetwork::SIM_MIGRATION_DISABLED_FLOOR`), mirroring
    /// `subscribe_hint_floor_override`'s fail-closed default: summary-first
    /// PUT is genuinely opt-in per sim via
    /// `SimNetwork::enable_summary_first_put`, rather than firing in every
    /// sim once the crate version passes the real floor.
    ///
    /// Not cfg-gated for the same reason as `subscribe_hint_floor_override`:
    /// `node::testing_impl` sets it and is compiled unconditionally.
    /// `#[serde(skip)]`; never serialized.
    #[serde(skip)]
    pub(crate) summary_first_put_floor_override: Option<(u8, u8, u16)>,
    /// Test-only harness flag: when set, a startup-hosted contract
    /// (`SeedHostedContract`, i.e. `append_contracts` with `subscription =
    /// true`) is registered in the neighbor-hosting advertised set so the
    /// connection-established `HostingStateResponse` exchange advertises it to
    /// neighbors. Defaults to `false` — the harness's historical behavior, so
    /// a seeded host does NOT advertise and a key-routed GET to a
    /// non-hosting region still dead-ends (the migration dead-end controls
    /// depend on this). Opted into per-network via
    /// `SimNetwork::enable_seeded_host_advertisements` for tests that exercise
    /// the terminal advertisement consult. Not cfg-gated for the same reason
    /// as `subscribe_hint_floor_override`. `#[serde(skip)]`.
    #[serde(skip)]
    pub(crate) advertise_seeded_hosts: bool,
    /// Test-only override for the hosting manager's time source.
    ///
    /// Lets simulation tests inject a controllable clock (e.g.
    /// `SharedMockTimeSource`) so hosting-cache TTL and subscription-lease
    /// eviction advance deterministically under test control instead of wall
    /// time. `None` in production (and never serialized — `#[serde(skip)]`),
    /// where the `Ring`'s default `Arc<InstantTimeSrc>` is used. Consumed at
    /// `Ring::new` → `HostingManager::with_time_source`. See #4642 (piece A).
    ///
    /// Not cfg-gated for the same reason as `governance_config_override`:
    /// `node::testing_impl` sets it and is compiled unconditionally, so the
    /// field must exist in every build. The `Option` is simply always `None`
    /// outside tests.
    #[serde(skip)]
    pub(crate) hosting_time_source_override: Option<crate::util::time_source::DynTimeSource>,
}

impl NodeConfig {
    /// This node's own peer id as a telemetry attribution string
    /// (public key + best-effort address). The address portion falls
    /// back to the listener address for non-gateway nodes until
    /// external-address discovery — a refresh path is tracked in
    /// #4294. Shared by the telemetry reporter and the shadow-RTT /
    /// reference-ping emitters so the two constructions can't drift.
    pub(crate) fn local_peer_id_string(&self) -> String {
        let addr = self.own_addr.unwrap_or_else(|| {
            std::net::SocketAddr::new(self.network_listener_ip, self.network_listener_port)
        });
        PeerId::new(self.key_pair.public().clone(), addr).to_string()
    }

    pub async fn new(config: Config) -> anyhow::Result<NodeConfig> {
        tracing::info!("Loading node configuration for mode {}", config.mode);

        // Get our own public key to filter out self-connections
        let own_pub_key = config.transport_keypair().public();

        let mut gateways = Vec::with_capacity(config.gateways.len());
        for gw in &config.gateways {
            let GatewayConfig {
                address,
                public_key_path,
                location,
            } = gw;

            // Wait for the public key file to be in X25519 hex format.
            // The gateway may still be initializing and converting from RSA PEM.
            let mut key_bytes = None;
            for attempt in 0..10 {
                let mut key_file = File::open(public_key_path).with_context(|| {
                    format!("failed loading gateway pubkey from {public_key_path:?}")
                })?;
                let mut buf = String::new();
                key_file.read_to_string(&mut buf)?;
                let buf = buf.trim();

                // Check for legacy RSA PEM format - gateway may still be initializing
                if buf.starts_with("-----BEGIN") {
                    if attempt < 9 {
                        tracing::debug!(
                            public_key_path = ?public_key_path,
                            attempt = attempt + 1,
                            "Gateway public key is still RSA PEM format, waiting for X25519 conversion..."
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    } else {
                        tracing::warn!(
                            public_key_path = ?public_key_path,
                            "Gateway public key still in RSA PEM format after 5s. Skipping this gateway."
                        );
                        break;
                    }
                }

                match hex::decode(buf) {
                    Ok(bytes) if bytes.len() == 32 => {
                        key_bytes = Some(bytes);
                        break;
                    }
                    Ok(bytes) => {
                        anyhow::bail!(
                            "invalid gateway pubkey length {} (expected 32) from {public_key_path:?}",
                            bytes.len()
                        );
                    }
                    Err(e) => {
                        anyhow::bail!(
                            "failed to decode gateway pubkey hex from {public_key_path:?}: {e}"
                        );
                    }
                }
            }

            let key_bytes = match key_bytes {
                Some(bytes) => bytes,
                None => continue, // Skip this gateway
            };
            let mut key_arr = [0u8; 32];
            key_arr.copy_from_slice(&key_bytes);
            let transport_pub_key = TransportPublicKey::from_bytes(key_arr);

            // Skip if this gateway's public key matches our own
            if &transport_pub_key == own_pub_key {
                tracing::warn!(
                    "Skipping gateway with same public key as self: {:?}",
                    public_key_path
                );
                continue;
            }

            let address = Self::parse_socket_addr(address).await?;
            let peer_key_location = PeerKeyLocation::new(transport_pub_key, address);
            let location = location
                .map(Location::new)
                .unwrap_or_else(|| Location::from_address(&address));
            gateways.push(InitPeerNode::new(peer_key_location, location));
        }
        tracing::info!(
            "Node will be listening at {}:{} internal address",
            config.network_api.address,
            config.network_api.port
        );
        if let Some(own_addr) = &config.peer_id {
            tracing::info!("Node external address: {}", own_addr.socket_addr());
        }
        Ok(NodeConfig {
            should_connect: true,
            is_gateway: config.is_gateway,
            key_pair: config.transport_keypair().clone(),
            gateways,
            own_addr: config.peer_id.clone().map(|p| p.socket_addr()),
            network_listener_ip: config.network_api.address,
            network_listener_port: config.network_api.port,
            location: config.location.map(Location::new),
            config: Arc::new(config.clone()),
            max_hops_to_live: None,
            rnd_if_htl_above: None,
            max_number_conn: Some(config.network_api.max_connections),
            min_number_conn: Some(config.network_api.min_connections),
            max_upstream_bandwidth: None,
            max_downstream_bandwidth: None,
            blocked_addresses: config.network_api.blocked_addresses.clone(),
            transient_budget: config.network_api.transient_budget,
            transient_ttl: Duration::from_secs(config.network_api.transient_ttl_secs),
            relay_ready_connections: if config.network_api.skip_load_from_network {
                Some(0) // Local/test networks: disable relay gate
            } else {
                Some(3) // Production: require 3 relay-ready upstream peers
            },
            governance_config_override: None,
            subscribe_hint_floor_override: None,
            summary_first_put_floor_override: None,
            advertise_seeded_hosts: false,
            hosting_time_source_override: None,
        })
    }

    pub(crate) async fn parse_socket_addr(address: &Address) -> anyhow::Result<SocketAddr> {
        let (hostname, port) = match address {
            // New form: host and port already separated. `port` is always
            // populated (defaulted to DEFAULT_GATEWAY_PORT at deserialize time).
            crate::config::Address::Host { host, port } => {
                let host_with_port = format!("{host}:{port}");
                if let Ok(mut addrs) = host_with_port.to_socket_addrs() {
                    if let Some(addr) = addrs.next() {
                        return Ok(addr);
                    }
                }
                (Cow::Borrowed(host.as_str()), Some(*port))
            }
            crate::config::Address::Hostname(hostname) => {
                match hostname.rsplit_once(':') {
                    None => {
                        // No port found. Default to the gateway port (31337), NOT
                        // a random local port — we are addressing a gateway we need
                        // to reach (issue #1388).
                        let hostname_with_port =
                            format!("{}:{}", hostname, crate::config::DEFAULT_GATEWAY_PORT);

                        if let Ok(mut addrs) = hostname_with_port.to_socket_addrs() {
                            if let Some(addr) = addrs.next() {
                                return Ok(addr);
                            }
                        }

                        (Cow::Borrowed(hostname.as_str()), None)
                    }
                    Some((host, port)) => match port.parse::<u16>() {
                        Ok(port) => {
                            if let Ok(mut addrs) = hostname.to_socket_addrs() {
                                if let Some(addr) = addrs.next() {
                                    return Ok(addr);
                                }
                            }

                            (Cow::Borrowed(host), Some(port))
                        }
                        Err(_) => return Err(anyhow::anyhow!("Invalid port number: {port}")),
                    },
                }
            }
            Address::HostAddress(addr) => return Ok(*addr),
        };

        let resolver = hickory_resolver::TokioResolver::builder_tokio()?.build()?;

        // only issue one query with .
        let hostname = if hostname.ends_with('.') {
            hostname
        } else {
            Cow::Owned(format!("{hostname}."))
        };

        let ips = resolver.lookup_ip(hostname.as_ref()).await?;
        match ips.iter().next() {
            Some(ip) => Ok(SocketAddr::new(
                ip,
                // No explicit port → default to the gateway port (31337), not a
                // random local port (issue #1388).
                port.unwrap_or(crate::config::DEFAULT_GATEWAY_PORT),
            )),
            None => Err(anyhow::anyhow!("Fail to resolve IP address of {hostname}")),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn is_gateway(&mut self) -> &mut Self {
        self.is_gateway = true;
        self
    }

    pub fn first_gateway(&mut self) {
        self.should_connect = false;
    }

    pub fn with_should_connect(&mut self, should_connect: bool) -> &mut Self {
        self.should_connect = should_connect;
        self
    }

    pub fn max_hops_to_live(&mut self, num_hops: usize) -> &mut Self {
        self.max_hops_to_live = Some(num_hops);
        self
    }

    pub fn rnd_if_htl_above(&mut self, num_hops: usize) -> &mut Self {
        self.rnd_if_htl_above = Some(num_hops);
        self
    }

    pub fn max_number_of_connections(&mut self, num: usize) -> &mut Self {
        self.max_number_conn = Some(num);
        self
    }

    pub fn min_number_of_connections(&mut self, num: usize) -> &mut Self {
        self.min_number_conn = Some(num);
        self
    }

    pub fn relay_ready_connections(&mut self, num: Option<usize>) -> &mut Self {
        self.relay_ready_connections = num;
        self
    }

    pub fn with_own_addr(&mut self, addr: SocketAddr) -> &mut Self {
        self.own_addr = Some(addr);
        self
    }

    pub fn with_location(&mut self, loc: Location) -> &mut Self {
        self.location = Some(loc);
        self
    }

    /// Connection info for an already existing peer. Required in case this is not a gateway node.
    pub fn add_gateway(&mut self, peer: InitPeerNode) -> &mut Self {
        self.gateways.push(peer);
        self
    }

    /// Builds a node using the default backend connection manager.
    pub async fn build<const CLIENTS: usize>(
        self,
        clients: [BoxedClient; CLIENTS],
    ) -> anyhow::Result<Node> {
        let (node, _flush_handle) = self.build_with_flush_handle(clients).await?;
        Ok(node)
    }

    /// Builds a node and returns flush handle for event aggregation (for testing).
    pub async fn build_with_flush_handle<const CLIENTS: usize>(
        self,
        clients: [BoxedClient; CLIENTS],
    ) -> anyhow::Result<(Node, crate::tracing::EventFlushHandle)> {
        let (event_register, flush_handle) = {
            use super::tracing::{DynamicRegister, TelemetryReporter};

            let event_reg = EventRegister::new(self.config.event_log());
            let flush_handle = event_reg.flush_handle();

            let mut registers: Vec<Box<dyn NetEventRegister>> = vec![Box::new(event_reg)];

            // Add OpenTelemetry register if feature enabled
            #[cfg(feature = "trace-ot")]
            {
                use super::tracing::OTEventRegister;
                registers.push(Box::new(OTEventRegister::new()));
            }

            // Add telemetry reporter if enabled in config. The local
            // peer id (public key + best-effort address, same
            // construction as the shadow-RTT events in `p2p_impl.rs`)
            // attributes transport-level events — transfer_failed,
            // transport_snapshot, timeout — which otherwise carry an
            // empty peer_id and cannot be correlated to a sender in
            // the collector (#4345 observability gap).
            if let Some(telemetry) =
                TelemetryReporter::new(&self.config.telemetry, self.local_peer_id_string())
            {
                registers.push(Box::new(telemetry));
            }

            (DynamicRegister::new(registers), flush_handle)
        };
        let cfg = self.config.clone();
        let drain_timeout = std::time::Duration::from_secs(cfg.shutdown_drain_secs);
        let (node_inner, shutdown_tx) = NodeP2P::build::<NetworkContractHandler, CLIENTS, _>(
            self,
            clients,
            event_register,
            cfg,
        )
        .await?;
        let shutdown_handle = ShutdownHandle {
            tx: shutdown_tx,
            inflight_client_ops: node_inner.op_manager.inflight_client_ops_handle(),
            shutting_down: node_inner.op_manager.shutting_down_handle(),
            drain_timeout,
        };
        Ok((
            Node {
                inner: node_inner,
                shutdown_handle,
            },
            flush_handle,
        ))
    }

    pub fn get_own_addr(&self) -> Option<SocketAddr> {
        self.own_addr
    }

    /// Returns all specified gateways for this peer. Returns an error if the peer is not a gateway
    /// and no gateways are specified.
    fn get_gateways(&self) -> anyhow::Result<Vec<PeerKeyLocation>> {
        let gateways: Vec<PeerKeyLocation> = self
            .gateways
            .iter()
            .map(|node| node.peer_key_location.clone())
            .collect();

        if !self.is_gateway && gateways.is_empty() {
            anyhow::bail!(
                "At least one remote gateway is required to join an existing network for non-gateway nodes."
            )
        } else {
            Ok(gateways)
        }
    }
}

/// Gateway node to use for joining the network.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InitPeerNode {
    peer_key_location: PeerKeyLocation,
    location: Location,
}

impl InitPeerNode {
    pub fn new(peer_key_location: PeerKeyLocation, location: Location) -> Self {
        Self {
            peer_key_location,
            location,
        }
    }
}

async fn report_result(
    tx: Option<Transaction>,
    op_result: Result<(), OpError>,
    op_manager: &OpManager,
    _event_listener: &mut dyn NetEventRegister,
) {
    // Add UPDATE-specific debug logging at the start
    if let Some(tx_id) = tx {
        if matches!(tx_id.transaction_type(), TransactionType::Update) {
            tracing::debug!("report_result called for UPDATE transaction {}", tx_id);
        }
    }

    match op_result {
        Ok(()) => {
            // No legacy `OpEnum` to report. Task-per-tx drivers publish
            // their own `HostResult` via `result_router_tx`, record
            // route events through `record_relay_route_event` /
            // `record_acceptor_outcome`, and handle dashboard
            // classification inline. Nothing remains for this branch
            // to do beyond the dispatch site's own logging.
            tracing::debug!(?tx, "Network message dispatch finished");
        }
        Err(err) => {
            // Mark operation as completed and notify waiting clients of the error
            if let Some(tx) = tx {
                // Sub-operations (e.g., Subscribe spawned by PUT) have no client
                // registered — sending errors for them would pollute the
                // SessionActor's pending_results cache.
                if !tx.is_sub_operation() {
                    let client_error = freenet_stdlib::client_api::ClientError::from(
                        freenet_stdlib::client_api::ErrorKind::OperationError {
                            cause: err.to_string().into(),
                        },
                    );
                    op_manager.send_client_result(tx, Err(client_error));
                }

                op_manager.completed(tx);
            }
            #[cfg(any(debug_assertions, test))]
            {
                use std::io::Write;
                #[cfg(debug_assertions)]
                let OpError::InvalidStateTransition { tx, state, trace } = err else {
                    tracing::error!("Finished transaction with error: {err}");
                    return;
                };
                #[cfg(not(debug_assertions))]
                let OpError::InvalidStateTransition { tx } = err else {
                    tracing::error!("Finished transaction with error: {err}");
                    return;
                };
                // todo: this can be improved once std::backtrace::Backtrace::frames is stabilized
                #[cfg(debug_assertions)]
                let trace = format!("{trace}");
                #[cfg(debug_assertions)]
                {
                    let mut tr_lines = trace.lines();
                    let trace = tr_lines
                        .nth(2)
                        .map(|second_trace| {
                            let second_trace_lines =
                                [second_trace, tr_lines.next().unwrap_or_default()];
                            second_trace_lines.join("\n")
                        })
                        .unwrap_or_default();
                    let peer = op_manager.ring.connection_manager.own_location();
                    let log = format!(
                        "Transaction ({tx} @ {peer}) error trace:\n {trace} \nstate:\n {state:?}\n"
                    );
                    std::io::stderr().write_all(log.as_bytes()).unwrap();
                }
                #[cfg(not(debug_assertions))]
                {
                    let peer = op_manager.ring.connection_manager.own_location();
                    let log = format!("Transaction ({tx} @ {peer}) error\n");
                    std::io::stderr().write_all(log.as_bytes()).unwrap();
                }
            }
            #[cfg(not(any(debug_assertions, test)))]
            {
                tracing::debug!("Finished transaction with error: {err}");
            }
        }
    }
}

/// Process a network message and deliver results to clients via the canonical
/// path: report_result → send_client_result → ResultRouter → SessionActor.
pub(crate) async fn process_message_decoupled<CB>(
    msg: NetMessage,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    mut event_listener: Box<dyn NetEventRegister>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<crate::node::WaiterReply>>,
) where
    CB: NetworkBridge + Clone + 'static,
{
    let tx = *msg.id();

    let op_result = handle_pure_network_message(
        msg,
        source_addr,
        op_manager.clone(),
        conn_manager,
        event_listener.as_mut(),
        pending_op_result,
    )
    .await;

    // Report result and deliver to clients via the single canonical path:
    // report_result → send_client_result → ResultRouter → SessionActor
    report_result(Some(tx), op_result, &op_manager, &mut *event_listener).await;
}

/// Pure network message handling (no client concerns)
#[allow(clippy::too_many_arguments)]
async fn handle_pure_network_message<CB>(
    msg: NetMessage,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: &mut dyn NetEventRegister,
    pending_op_result: Option<tokio::sync::mpsc::Sender<crate::node::WaiterReply>>,
) -> Result<(), crate::node::OpError>
where
    CB: NetworkBridge + Clone + 'static,
{
    match msg {
        NetMessage::V1(msg_v1) => {
            handle_pure_network_message_v1(
                msg_v1,
                source_addr,
                op_manager,
                conn_manager,
                event_listener,
                pending_op_result,
            )
            .await
        }
    }
}

/// Forward an inbound reply directly to the awaiting
/// [`OpCtx::send_and_await`][ocxawait] caller.
///
/// Returns `true` if a callback was registered (message forwarded or
/// dropped on a closed receiver — either way the caller must not fall
/// through to other handling). Returns `false` if no callback is
/// registered.
///
/// # Safety argument
///
/// `p2p_protoc::pending_op_results` is only populated via
/// `p2p_protoc::handle_op_execution`, driven by `op_execution_sender`.
/// The only way to obtain a clone of that sender is through
/// [`crate::node::OpManager::op_ctx`], whose round-trip method is
/// [`OpCtx::send_and_await`][ocxawait]. This is a **structural
/// invariant**: the sender field is `pub(crate)` and there is no other
/// `pub` accessor on `EventLoopNotificationsSender`.
///
/// [ocxawait]: crate::operations::OpCtx::send_and_await
///
/// # Channel safety
///
/// Uses `try_send` on the bounded reply channel created by the
/// `OpCtx::send_*` family. A `try_send` failure means the reply could not
/// be handed to the OpCtx driver and is dropped — which is benign and
/// expected, not an error. The two failure modes (surfaced in the logged
/// `err` field) are:
///
/// - `TrySendError::Closed`: the driver's receiver is gone because the
///   caller already finished, timed out, or was cancelled. The dominant
///   source is SUBSCRIBE renewals, whose ~25s outer cancel deadline fires
///   before the ~60s per-attempt peer wait (see issue #4350), so a peer's
///   reply routinely lands after the renewal task was dropped.
///   `send_fire_and_forget` / `send_local_loopback` (UPDATE,
///   originator-loopback PUT) also drop the receiver by design, so they
///   produce `Closed` here as normal operation.
/// - `TrySendError::Full`: the reply channel is at capacity. For a
///   capacity-1 caller (GET/PUT/SUBSCRIBE via `send_and_await`) that means a
///   duplicate reply arrived before the driver drained the first; for the
///   capacity-N CONNECT fan-in (`send_to_and_collect_replies`) it means a
///   burst of distinct replies exceeded the buffer — an expected overflow,
///   see `compute_reply_capacity` in `connect/op_ctx_task.rs`.
///
/// In every case the channel is intentionally lossy
/// (`.claude/rules/channel-safety.md`: drop when full rather than block) and
/// the operation makes progress without this reply, so the drop is logged at
/// `debug`, matching `.claude/rules/operations.md` ("WHEN a reply arrives
/// with no waiter → Benign → debug log"). Logging it at `error` produced a
/// steady stream of false-alarm errors on busy gateways (~30/hr on nova
/// after the v0.2.69 rollout, when ~745 hosted contracts re-subscribe at
/// once after a restart); `warn` is likewise wrong because the CONNECT
/// fan-in legitimately hits the full-channel case under load.
///
/// Either way the handler still makes progress and returns `true`.
fn try_forward_driver_reply(
    pending_op_result: Option<&tokio::sync::mpsc::Sender<crate::node::WaiterReply>>,
    reply: NetMessage,
    op_label: &'static str,
) -> bool {
    let Some(callback) = pending_op_result else {
        return false;
    };
    let tx_id = *reply.id();
    if let Err(err) = callback.try_send(crate::node::WaiterReply::Reply(reply)) {
        // Benign, expected, and intentionally lossy (see `# Channel safety`):
        // the reply could not be delivered (receiver closed, or the channel
        // full for a CONNECT-style capacity-N fan-in) and the operation
        // proceeds without it. `err` distinguishes Closed vs Full.
        tracing::debug!(
            %err,
            %tx_id,
            op = op_label,
            "Driver reply dropped (OpCtx receiver closed or reply channel full); operation proceeds without it"
        );
    }
    true
}

/// Fill in an acceptor's external address from `source_addr` when the
/// `ConnectMsg::Response` arrives with `acceptor.peer_addr = Unknown`.
///
/// An acceptor behind NAT does not know its own external address; the
/// inbound transport's `source_addr` is used to backstop the missing
/// value before the driver reads it. The driver itself does not see
/// `source_addr`, so the rewrite must happen at the dispatch site.
///
/// Non-`Response` variants and `Response` with an already-`Known`
/// acceptor address pass through unchanged.
fn fill_connect_response_acceptor_addr(
    op: connect::ConnectMsg,
    source_addr: Option<std::net::SocketAddr>,
) -> connect::ConnectMsg {
    #[allow(clippy::wildcard_enum_match_arm)]
    match op {
        connect::ConnectMsg::Response { id, mut payload } => {
            if payload.acceptor.peer_addr.is_unknown() {
                if let Some(addr) = source_addr {
                    payload.acceptor.peer_addr = crate::ring::PeerAddr::Known(addr);
                    tracing::debug!(
                        acceptor_pub_key = %payload.acceptor.pub_key(),
                        acceptor_addr = %addr,
                        "connect bypass: filled acceptor address from source_addr"
                    );
                } else {
                    tracing::warn!(
                        acceptor_pub_key = %payload.acceptor.pub_key(),
                        "connect bypass: response received without source_addr, cannot fill acceptor address"
                    );
                }
            }
            connect::ConnectMsg::Response { id, payload }
        }
        other => other,
    }
}

/// Pure network message processing for V1 messages (no client concerns)
#[allow(clippy::too_many_arguments, clippy::needless_return)]
async fn handle_pure_network_message_v1<CB>(
    msg: NetMessageV1,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: &mut dyn NetEventRegister,
    pending_op_result: Option<tokio::sync::mpsc::Sender<crate::node::WaiterReply>>,
) -> Result<(), crate::node::OpError>
where
    CB: NetworkBridge + Clone + 'static,
{
    // Register network events (pure network concern)
    event_listener
        .register_events(NetEventLog::from_inbound_msg_v1(
            &msg,
            &op_manager,
            source_addr,
        ))
        .await;

    let tx = Some(*msg.id());
    tracing::debug!(?tx, "Processing pure network operation");

    match msg {
        NetMessageV1::Connect(ref op) => {
            // CONNECT reply forwarding: the joiner expects fan-in
            // (up to `target_connections` `Response`s over time).
            // The waiter (`OpCtx::send_and_collect_replies`) has a
            // multi-reply receiver, so this bypass forwards every
            // non-`Request` variant without short-circuiting after
            // the first hit.
            //
            // `Request` is NEVER forwarded here: it spawns a relay
            // driver via the dispatch gate below.
            if matches!(
                op,
                connect::ConnectMsg::Response { .. }
                    | connect::ConnectMsg::Rejected { .. }
                    | connect::ConnectMsg::ObservedAddress { .. }
                    | connect::ConnectMsg::ConnectFailed { .. }
            ) {
                let forwarded_op = fill_connect_response_acceptor_addr(op.clone(), source_addr);
                if try_forward_driver_reply(
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Connect(forwarded_op)),
                    "connect",
                ) {
                    return Ok(());
                }
            }

            // Relay-CONNECT dispatch: fresh inbound `Request` with
            // a real upstream address and no running relay driver
            // spawns `start_relay_connect`. The driver owns the
            // entire transaction lifetime in task locals. The
            // `active_relay_connect_txs` check dedups against
            // GC-spawned retries and duplicate Requests.
            //
            // `source_addr.is_none()` (originator loopback) cannot
            // reach this branch — the reply bypass above handles
            // joiner-side replies first. Originator state lives in
            // `start_client_connect` task locals; there is no
            // joiner-side legacy state machine.
            if let connect::ConnectMsg::Request { id, payload } = op {
                if let Some(upstream_addr) = source_addr {
                    if !op_manager.active_relay_connect_txs.contains(id) {
                        if let Err(err) = connect::op_ctx_task::start_relay_connect(
                            op_manager.clone(),
                            *id,
                            payload.clone(),
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %upstream_addr,
                                error = %err,
                                "CONNECT relay dispatch: start_relay_connect failed"
                            );
                        }
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %upstream_addr,
                            "CONNECT: duplicate Request, relay driver already running"
                        );
                    }
                } else {
                    tracing::debug!(
                        tx = %id,
                        "CONNECT: Request without source_addr ignored (no legacy joiner path)"
                    );
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "CONNECT: non-Request variant ignored \
                     (Response/Rejected/ObservedAddress/ConnectFailed already handled by bypass)"
                );
            }
            return Ok(());
        }
        NetMessageV1::Put(ref op) => {
            // Forward only **terminal** Response/ResponseStreaming/Error
            // messages to the originator's awaiting task via the
            // bypass. Non-terminal messages (Request,
            // RequestStreaming, ForwardingAck) must NOT be
            // forwarded: they would fill the capacity-1 reply
            // channel and cause `classify_reply` to fail.
            //
            // `Error` is terminal-by-construction (issue #4111): the
            // originator-loopback failure path emits it via
            // `send_local_loopback` so the originator's
            // `start_client_put` retry loop classifies the local
            // contract-side rejection as `Terminal(Err(cause))` once,
            // rather than burning the retry budget against a closed
            // per-attempt reply channel.
            if matches!(
                op,
                put::PutMsg::Response { .. }
                    | put::PutMsg::ResponseStreaming { .. }
                    | put::PutMsg::Error { .. }
                    | put::PutMsg::ProbeResponse { .. }
            ) && try_forward_driver_reply(
                pending_op_result.as_ref(),
                NetMessage::V1(NetMessageV1::Put((*op).clone())),
                "put",
            ) {
                return Ok(());
            }

            // Phase 7 ban-list gate (inbound REQUEST variants only).
            // Responses to our OWN outbound requests pass through
            // above; here we drop new PUTs for banned contracts so
            // the contract can't get re-hosted while banned.
            #[allow(clippy::wildcard_enum_match_arm)]
            let banned_key = match op {
                put::PutMsg::Request { contract, .. } => Some(contract.key()),
                put::PutMsg::RequestStreaming { contract_key, .. } => Some(*contract_key),
                put::PutMsg::ProbeRequest { contract_key, .. } => Some(*contract_key),
                put::PutMsg::ProbeReconcile { key, .. } => Some(*key),
                _ => None,
            };
            if let Some(key) = banned_key {
                if op_manager.ring.contract_ban_list.is_banned(key.id()) {
                    tracing::debug!(
                        tx = %op.id(),
                        %key,
                        phase = "put_banned_drop",
                        "PUT dispatch: dropping request for banned contract"
                    );
                    return Ok(());
                }
            }

            // Relay PUT dispatch. `start_relay_put` handles
            // non-streaming Request (with upgrade-on-forward to
            // streaming when payload > threshold);
            // `start_relay_put_streaming` handles direct
            // `RequestStreaming` inbound. `ForwardingAck` is a
            // no-op kept for backward compatibility.
            //
            // Originator loopback: `start_client_put`'s
            // `send_and_await(target=None)` arrives with
            // `source_addr=None`. Map to `upstream_addr=own_addr`
            // so the same driver handles both relay hops and
            // originator loopback.
            let effective_upstream =
                source_addr.or_else(|| op_manager.ring.connection_manager.get_own_addr());
            if let Some(upstream_addr) = effective_upstream {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    put::PutMsg::Request {
                        id,
                        contract,
                        related_contracts,
                        value,
                        htl,
                        skip_list,
                    } => {
                        if let Err(err) = put::op_ctx_task::start_relay_put(
                            op_manager.clone(),
                            conn_manager.clone(),
                            *id,
                            contract.clone(),
                            related_contracts.clone(),
                            value.clone(),
                            *htl,
                            skip_list.clone(),
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                contract = %contract.key(),
                                error = %err,
                                "PUT relay dispatch: start_relay_put failed"
                            );
                        }
                    }
                    put::PutMsg::RequestStreaming {
                        id,
                        stream_id,
                        contract_key,
                        total_size,
                        htl,
                        skip_list,
                        subscribe,
                    } => {
                        if let Err(err) = put::op_ctx_task::start_relay_put_streaming(
                            op_manager.clone(),
                            conn_manager.clone(),
                            *id,
                            *stream_id,
                            *contract_key,
                            *total_size,
                            *htl,
                            skip_list.clone(),
                            *subscribe,
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                contract = %contract_key,
                                error = %err,
                                "PUT relay dispatch: start_relay_put_streaming failed"
                            );
                        }
                    }
                    put::PutMsg::ProbeRequest {
                        id,
                        contract_key,
                        summary,
                        htl,
                        skip_list,
                    } => {
                        if let Err(err) = put::op_ctx_task::start_relay_probe(
                            op_manager.clone(),
                            *id,
                            *contract_key,
                            summary.clone(),
                            *htl,
                            skip_list.clone(),
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                contract = %contract_key,
                                error = %err,
                                "PUT relay dispatch: start_relay_probe failed"
                            );
                        }
                    }
                    put::PutMsg::ProbeReconcile {
                        id,
                        key,
                        delta,
                        htl,
                        skip_list,
                    } => {
                        if let Err(err) = put::op_ctx_task::start_relay_probe_reconcile(
                            op_manager.clone(),
                            *id,
                            *key,
                            delta.clone(),
                            *htl,
                            skip_list.clone(),
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                contract = %key,
                                error = %err,
                                "PUT relay dispatch: start_relay_probe_reconcile failed"
                            );
                        }
                    }
                    _ => {
                        tracing::debug!(
                            tx = %op.id(),
                            ?op,
                            "PUT: non-dispatch variant ignored \
                             (Response/ResponseStreaming/Error/ProbeResponse \
                             already handled by bypass; ForwardingAck is no-op)"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "PUT: no own_addr available — pre-handshake \
                     message ignored"
                );
            }
            return Ok(());
        }
        NetMessageV1::Get(ref op) => {
            // Forward only **terminal** Response/ResponseStreaming
            // messages to the originator's awaiting task. Other
            // variants must NOT be forwarded — they would fill the
            // capacity-1 reply channel.
            if matches!(
                op,
                get::GetMsg::Response { .. } | get::GetMsg::ResponseStreaming { .. }
            ) && try_forward_driver_reply(
                pending_op_result.as_ref(),
                NetMessage::V1(NetMessageV1::Get((*op).clone())),
                "get",
            ) {
                return Ok(());
            }

            // Phase 7 ban-list gate (inbound REQUEST only). Responses
            // pass through above. We refuse to serve state for a
            // banned contract.
            if let get::GetMsg::Request { instance_id, .. } = op {
                if op_manager.ring.contract_ban_list.is_banned(instance_id) {
                    tracing::debug!(
                        tx = %op.id(),
                        %instance_id,
                        phase = "get_banned_drop",
                        "GET dispatch: dropping request for banned contract"
                    );
                    return Ok(());
                }
            }

            // Relay GET dispatch. Originator loopback
            // (`source_addr=None`) is mapped to
            // `upstream_addr=own_addr` so the same `start_relay_get`
            // driver handles both relay hops and loopback.
            let effective_upstream =
                source_addr.or_else(|| op_manager.ring.connection_manager.get_own_addr());
            if let Some(upstream_addr) = effective_upstream {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    get::GetMsg::Request {
                        id,
                        instance_id,
                        fetch_contract,
                        htl,
                        visited,
                        subscribe,
                    } => {
                        if let Err(err) = get::op_ctx_task::start_relay_get(
                            op_manager.clone(),
                            conn_manager.clone(),
                            *id,
                            *instance_id,
                            *htl,
                            upstream_addr,
                            visited.clone(),
                            *fetch_contract,
                            *subscribe,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %instance_id,
                                error = %err,
                                "GET relay dispatch: start_relay_get failed"
                            );
                        }
                    }
                    _ => {
                        tracing::debug!(
                            tx = %op.id(),
                            ?op,
                            "GET: non-dispatch variant ignored \
                             (Response/ResponseStreaming already handled \
                             by bypass; ForwardingAck is no-op; \
                             ResponseStreamingAck handled by stream layer)"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "GET: no own_addr available — pre-handshake \
                     message ignored"
                );
            }
            return Ok(());
        }
        NetMessageV1::Update(ref op) => {
            // UPDATE is fire-and-forget end-to-end — no upstream
            // reply to await. For relay hops
            // (`source_addr.is_some()`) dispatch the matching
            // driver and return. `source_addr.is_none()` would
            // mean an internal caller; there are none, so the else
            // branch logs and drops.
            if let Some(sender_addr) = source_addr {
                // Phase 2 front-line rate limit. Apply UNIFORMLY across
                // all four UPDATE wire variants so a flooder can't
                // bypass by switching opcode (RequestUpdate /
                // BroadcastTo / RequestUpdateStreaming /
                // BroadcastToStreaming). The check happens BEFORE the
                // dedup gate inside the relay drivers — that ordering
                // is what made the previous PR-MVP iteration race-
                // free per Codex review: rejected attempts never enter
                // the dedup set, so a legitimate retry of the same
                // tx is not silently dropped as a duplicate. See
                // `crate::ring::update_rate_limit` for design.
                let key = match op {
                    update::UpdateMsg::RequestUpdate { key, .. }
                    | update::UpdateMsg::BroadcastTo { key, .. }
                    | update::UpdateMsg::RequestUpdateStreaming { key, .. }
                    | update::UpdateMsg::BroadcastToStreaming { key, .. } => *key,
                };

                // Phase 7 ban-list gate. Runs BEFORE the rate limiter
                // so a banned contract's traffic doesn't even count
                // against the per-(sender, contract) window — keeps
                // the rate limiter's signal-to-noise high.
                if op_manager.ring.contract_ban_list.is_banned(key.id()) {
                    tracing::debug!(
                        tx = %op.id(),
                        %key,
                        %sender_addr,
                        phase = "update_dispatch_banned_drop",
                        "UPDATE dispatch: dropping request for banned contract"
                    );
                    return Ok(());
                }

                let rate_decision = op_manager
                    .ring
                    .update_rate_limiter
                    .check_and_record(sender_addr, *key.id());
                if !rate_decision.is_allowed() {
                    tracing::debug!(
                        tx = %op.id(),
                        %key,
                        %sender_addr,
                        ?rate_decision,
                        phase = "update_dispatch_rate_limited",
                        "UPDATE dispatch: rejected by per-(sender, contract) rate limit"
                    );
                    return Ok(());
                }

                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    update::UpdateMsg::RequestUpdate {
                        id,
                        key,
                        related_contracts,
                        value,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_request_update(
                            op_manager.clone(),
                            *id,
                            *key,
                            related_contracts.clone(),
                            value.clone(),
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_request_update failed"
                            );
                        }
                        return Ok(());
                    }
                    update::UpdateMsg::BroadcastTo {
                        id,
                        key,
                        payload,
                        sender_summary_bytes,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_broadcast_to(
                            op_manager.clone(),
                            *id,
                            *key,
                            payload.clone(),
                            sender_summary_bytes.clone(),
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_broadcast_to failed"
                            );
                        }
                        return Ok(());
                    }
                    // Streaming relay UPDATE: claim stream → assemble
                    // → apply → BroadcastStateChange fans out.
                    update::UpdateMsg::RequestUpdateStreaming {
                        id,
                        key,
                        stream_id,
                        total_size,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_request_update_streaming(
                            op_manager.clone(),
                            *id,
                            *key,
                            *stream_id,
                            *total_size,
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_request_update_streaming failed"
                            );
                        }
                        return Ok(());
                    }
                    update::UpdateMsg::BroadcastToStreaming {
                        id,
                        key,
                        stream_id,
                        total_size,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_broadcast_to_streaming(
                            op_manager.clone(),
                            *id,
                            *key,
                            *stream_id,
                            *total_size,
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_broadcast_to_streaming failed"
                            );
                        }
                        return Ok(());
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "UPDATE: internal-source variant ignored"
                );
            }
            return Ok(());
        }
        NetMessageV1::Subscribe(ref op) => {
            // Forward only **terminal** Response messages to the
            // originator's awaiting task. Other variants must NOT
            // be forwarded — they would fill the capacity-1 reply
            // channel.
            if matches!(op, subscribe::SubscribeMsg::Response { .. })
                && try_forward_driver_reply(
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Subscribe((*op).clone())),
                    "subscribe",
                )
            {
                return Ok(());
            }

            // Relay SUBSCRIBE dispatch. Originator loopback
            // (`source_addr=None`) is mapped to
            // `upstream_addr=own_addr` so the same
            // `start_relay_subscribe` driver handles both.
            let effective_upstream =
                source_addr.or_else(|| op_manager.ring.connection_manager.get_own_addr());
            if let Some(upstream_addr) = effective_upstream {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    subscribe::SubscribeMsg::Request {
                        id,
                        instance_id,
                        htl,
                        visited,
                        is_renewal,
                    } => {
                        // Phase 7 ban-list gate. Drop SUBSCRIBE for
                        // banned contracts before reaching the driver
                        // so we don't register interest in something
                        // we have already decided to reject.
                        if op_manager.ring.contract_ban_list.is_banned(instance_id) {
                            tracing::debug!(
                                tx = %id,
                                %instance_id,
                                %upstream_addr,
                                phase = "subscribe_dispatch_banned_drop",
                                "SUBSCRIBE dispatch: dropping request for banned contract"
                            );
                            return Ok(());
                        }

                        if let Err(err) = subscribe::op_ctx_task::start_relay_subscribe(
                            op_manager.clone(),
                            *id,
                            *instance_id,
                            *htl,
                            visited.clone(),
                            *is_renewal,
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %instance_id,
                                error = %err,
                                "SUBSCRIBE relay dispatch: start_relay_subscribe failed"
                            );
                        }
                    }
                    subscribe::SubscribeMsg::Unsubscribe { id, instance_id } => {
                        subscribe::handle_unsubscribe_inbound(
                            &op_manager,
                            *id,
                            *instance_id,
                            source_addr,
                        )
                        .await;
                    }
                    _ => {
                        // Response handled by bypass above;
                        // ForwardingAck is a wire-only telemetry
                        // hook (#3570) with no state mutation.
                        tracing::debug!(
                            tx = %op.id(),
                            ?op,
                            "SUBSCRIBE: non-dispatch variant ignored \
                             (Response already handled by bypass; \
                             ForwardingAck is no-op)"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "SUBSCRIBE: no own_addr available — pre-handshake \
                     message ignored"
                );
            }
            return Ok(());
        }
        // Non-transactional message types: process once and return immediately.
        // These must NOT fall through to the post-loop "Dropping message" warning,
        // which is only meant for operation retry exhaustion.
        NetMessageV1::NeighborHosting { ref message } => {
            let Some(source) = source_addr else {
                tracing::warn!(
                    "Received NeighborHosting message without source address (pure network)"
                );
                return Ok(());
            };
            tracing::debug!(
                from = %source,
                "Processing NeighborHosting message (pure network)"
            );

            // Note: In the simplified architecture (2026-01 refactor), we no longer
            // attempt to establish subscriptions based on HostingAnnounce messages.
            // Update propagation uses the neighbor hosting manager directly, and subscriptions
            // are lease-based with automatic expiry.

            // Resolve source SocketAddr to TransportPublicKey for neighbor hosting
            let source_pub_key = op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(source)
                .map(|pkl| pkl.pub_key().clone());
            let Some(source_pub_key) = source_pub_key else {
                tracing::debug!(
                    %source,
                    "NeighborHosting: could not resolve source addr to pub_key, skipping"
                );
                return Ok(());
            };
            let result = op_manager
                .neighbor_hosting
                .handle_message(&source_pub_key, message.clone());
            if let Some(response) = result.response {
                // Send response back to sender
                let response_msg =
                    NetMessage::V1(NetMessageV1::NeighborHosting { message: response });
                if let Err(err) = conn_manager.send(source, response_msg).await {
                    tracing::error!(%err, %source, "Failed to send NeighborHosting response");
                }
            }
            // Proactive state sync: broadcast our state for shared contracts
            // so the neighbor gets current state if they're stale after restart.
            // Only sync contracts we're actively interested in (receiving updates
            // or have downstream subscribers) — skip cached-only contracts.
            for instance_id in result.overlapping_contracts {
                // Phase 7 egress gate. If we've banned the contract,
                // don't proactively push its state to a sibling peer
                // via the overlap-sync path — that would undermine
                // the wire-boundary drop the ban list is supposed to
                // provide.
                if op_manager.ring.contract_ban_list.is_banned(&instance_id) {
                    tracing::debug!(
                        %instance_id,
                        peer = %source_pub_key,
                        phase = "neighbor_hosting_banned_skip",
                        "skipping proximity sync for banned contract"
                    );
                    continue;
                }
                // Skip the per-contract state fetch — a `GetQuery` that opens
                // the `fetch_contract` span on the single-threaded
                // contract-handling loop — for contracts we neither actively
                // serve nor owe a deferred broadcast. A node carrying phantom
                // interest (e.g. the #4404 placement migration left hundreds
                // of not-held contracts) otherwise fetched state for EVERY
                // overlapping contract on EVERY inbound NeighborHosting
                // announce, only to discard it at the
                // `is_receiving_updates() || has_downstream_subscribers()`
                // gate below. That fetch-then-discard was the residual #4473
                // `fetch_contract` churn on technic (the fetch-path sibling of
                // the #4475 / #4482 summarize gates).
                //
                // The gate reuses the existing discard predicate
                // (`is_receiving_updates || has_downstream_subscribers`), so it
                // changes nothing for served contracts, and adds a
                // `pending_broadcasts` clause so the #4359 fresh-PUT flush at
                // the matching arm still runs for any contract that owes one.
                // Skipping is safe for the flush because a deferred broadcast
                // is only ever stashed for a contract THIS node originated
                // (broadcast give-up), so the flush is a guaranteed no-op for
                // every contract this gate skips. The predicates take a
                // synthetic key with a zero code hash: `ContractKey` equality
                // and hashing are instance-only (freenet-stdlib `key.rs`), so
                // the hosting / subscription maps resolve correctly from the
                // instance id alone — `get_contract_state_by_id` is the only
                // path that recovers the full key here, and that is exactly the
                // round-trip we are avoiding.
                let probe_key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
                    instance_id,
                    freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
                );
                if !op_manager.ring.is_receiving_updates(&probe_key)
                    && !op_manager.ring.has_downstream_subscribers(&probe_key)
                    && !op_manager.pending_broadcasts.contains(&instance_id)
                {
                    continue;
                }
                if let Some((key, state)) =
                    get_contract_state_by_id(&op_manager, &instance_id).await
                {
                    // #4359 (MUST-FIX 1, Source 1 / proximity): this neighbor
                    // just announced it hosts a contract we also host, so it is
                    // now a `neighbors_with_contract` broadcast target. If a
                    // fresh-contract PUT gave up with no targets and is stashed,
                    // flush it here — this is the proximity first-viable-target
                    // signal, distinct from the interest-registration flush sites
                    // (`register_peer_interest`; the Source-2 live fan-out arm
                    // itself was removed in #4642 step 9). Must run BEFORE the
                    // receiving-updates/downstream
                    // gate below, which `continue`s for exactly the
                    // locally-hosted-only fresh-PUT case this fix targets.
                    op_manager.flush_pending_broadcast_on_interest(&key).await;

                    if !op_manager.ring.is_receiving_updates(&key)
                        && !op_manager.ring.has_downstream_subscribers(&key)
                    {
                        continue;
                    }
                    tracing::debug!(
                        contract = %key,
                        peer = %source_pub_key,
                        "Proximity cache overlap — syncing state to neighbor"
                    );
                    // Non-blocking emit: SyncStateToPeer is best-effort
                    // gossip — if dropped, the next interest-sync round
                    // or a subsequent summary mismatch will catch it. A
                    // blocking 30 s `.await` here would itself stack on
                    // the same notification channel that the executor's
                    // try_notify path is trying to keep responsive
                    // (#4145 / #4234).
                    //
                    // Targeted heal: `stale_peer_sync_event` builds a
                    // `SyncStateToPeer` aimed at exactly `source` (the one
                    // neighbor whose proximity announcement overlaps our
                    // hosting), NEVER a `BroadcastStateChange` that would fan
                    // the state out to ALL subscribers. Routing through the
                    // shared builder keeps this second emit site pinned by
                    // `proximity_overlap_emit_site_uses_targeted_builder`
                    // (#3791/#3796) — the exact fan-out regression class.
                    if let Err(e) =
                        op_manager.try_notify_node_event(stale_peer_sync_event(key, state, source))
                    {
                        // Best-effort by design (see comment above);
                        // log at debug to keep the caller layer in
                        // step with the helper-internal downgrade
                        // (#4238).
                        tracing::debug!(
                            contract = %instance_id,
                            error = %e,
                            "Failed to emit SyncStateToPeer for proximity sync (best-effort)"
                        );
                    }
                }
            }
            return Ok(());
        }
        NetMessageV1::InterestSync { ref message } => {
            let Some(source) = source_addr else {
                tracing::warn!("Received InterestSync message without source address");
                return Ok(());
            };
            tracing::debug!(
                from = %source,
                "Processing InterestSync message"
            );

            // Handle interest synchronization for delta-based updates
            if let Some(response) =
                handle_interest_sync_message(&op_manager, source, message.clone()).await
            {
                let response_msg = NetMessage::V1(NetMessageV1::InterestSync { message: response });
                if let Err(err) = conn_manager.send(source, response_msg).await {
                    tracing::error!(%err, %source, "Failed to send InterestSync response");
                }
            }
            return Ok(());
        }
        NetMessageV1::ReadyState { ready } => {
            let Some(source) = source_addr else {
                tracing::warn!("Received ReadyState message without source address");
                return Ok(());
            };
            if ready {
                op_manager.ring.connection_manager.mark_peer_ready(source);
            } else {
                op_manager
                    .ring
                    .connection_manager
                    .mark_peer_not_ready(source);
            }
            tracing::debug!(
                from = %source,
                ready,
                "Processed ReadyState from peer"
            );
            return Ok(());
        }
        NetMessageV1::SubscribeHint(hint) => {
            // Disabled 0.2.88: over-aggressive migration storm (#4630-adjacent).
            // do NOT re-enable aggressive placement migration; see
            // .claude/rules/hosting-invariants.md (anti-pattern: holding-driven
            // placement push). A PRODUCTION node (no floor override) ignores
            // inbound hints entirely so a fresh 0.2.88 node is never pulled into
            // the storm by a not-yet-upgraded 0.2.87 neighbor that still emits
            // hints. A SIMULATION node (override present) defers to the
            // version-floor check below, preserving the migration-mechanism
            // tests. Early-return BEFORE any telemetry so a disabled production
            // node reports no placement-migration activity at all, matching the
            // pre-0.2.80 "migration off" baseline; the debug log still surfaces
            // residual inbound hint volume for operators.
            if !crate::node::network_bridge::p2p_protoc::placement_migration_enabled(
                op_manager
                    .ring
                    .connection_manager
                    .subscribe_hint_floor_override(),
            ) {
                tracing::debug!(
                    key = %hint.key,
                    ?source_addr,
                    "Ignoring inbound SubscribeHint: placement migration disabled \
                     (0.2.88 kill switch)"
                );
                return Ok(());
            }
            // Placement-migration telemetry (#4404 follow-up): count every inbound
            // hint, before any admission gate, so `received` is the true arrival
            // rate and `received - acted` is the gated/dropped fraction.
            op_manager
                .ring
                .placement_migration_metrics()
                .record_received();
            // Placement-migration version gate. The migration is re-enabled at
            // floor `(0, 2, 80)` (#4499 made it load-safe). The SEND side
            // (`p2p_protoc::peer_supports_subscribe_hint`) gates emission on the
            // remote peer's version; the RECEIVE path must gate too, so a node on
            // an older release does not ACT on a hint from an upgraded peer and
            // keep migration load alive on a peer that predates the load-safe fix.
            //
            // The symmetric (sender-version) gate is not cleanly reachable here:
            // the per-connection remote version lives in `P2pConnManager.connections`
            // and is not exposed through the `NetworkBridge` trait, so use this
            // node's OWN version against the SAME floor the send side uses. A node
            // on `>= 0.2.80` acts on inbound hints; a pre-floor node ignores them.
            // Lowering the floor (sim override) re-activates both sides together.
            //
            // Read the floor via `subscribe_hint_floor_override().unwrap_or(...)`,
            // identical to the send side, so a simulation that opts into the
            // cascade (`SimNetwork::enable_placement_migration`, which lowers the
            // per-node floor to `(0,0,0)`) still has its receivers act on hints.
            let floor = op_manager
                .ring
                .connection_manager
                .subscribe_hint_floor_override()
                .unwrap_or(crate::node::network_bridge::p2p_protoc::SUBSCRIBE_HINT_MIN_VERSION);
            let own_version = crate::node::network_bridge::p2p_protoc::own_crate_version();
            if !crate::node::network_bridge::p2p_protoc::version_supports_subscribe_hint(
                Some(own_version),
                floor,
            ) {
                tracing::debug!(
                    key = %hint.key,
                    ?own_version,
                    ?floor,
                    ?source_addr,
                    "Ignoring inbound SubscribeHint: own version is below the \
                     SubscribeHint re-enable floor (pre-floor peer, wire-compat)"
                );
                op_manager
                    .ring
                    .placement_migration_metrics()
                    .record_refused_version_floor();
                return Ok(());
            }
            // Directed-subscribe placement (#4404): a holder is nudging us to
            // host `hint.key` because we are closer to it in the ring. If we
            // already host it there is nothing to do. Otherwise start a
            // fire-and-forget directed subscribe routed THROUGH the holder
            // (`hint.holder`), which fetches and thereby hosts the contract.
            if op_manager.ring.is_hosting_contract(&hint.key) {
                tracing::debug!(
                    key = %hint.key,
                    ?source_addr,
                    "Received SubscribeHint for an already-hosted contract — ignoring"
                );
                op_manager
                    .ring
                    .placement_migration_metrics()
                    .record_refused_already_hosting();
                return Ok(());
            }
            // `hint.holder` is network-sourced. A legitimate sender always sets
            // `holder = its own location`, so the holder's address must equal the
            // address this hint actually arrived from. Requiring that:
            //   - drops an address-less holder (the directed-subscribe driver
            //     routes through the holder's socket address and would otherwise
            //     panic), and
            //   - prevents a peer from redirecting us to directed-subscribe
            //     through an arbitrary THIRD party (a cheap 1-packet → 1-spawned-
            //     -op amplification / SSRF-style vector). A peer can still nudge
            //     us toward ITSELF, which is exactly a legitimate hint.
            // Fail-safe: a dropped legitimate hint is re-sent on the next
            // migration trigger, so being strict here costs nothing.
            if hint.holder.socket_addr() != source_addr {
                tracing::debug!(
                    key = %hint.key,
                    holder = ?hint.holder.socket_addr(),
                    ?source_addr,
                    "Received SubscribeHint whose holder is not the sender — ignoring"
                );
                op_manager
                    .ring
                    .placement_migration_metrics()
                    .record_refused_holder_mismatch();
                return Ok(());
            }
            // Backpressure-aware migration admission (#4534): refuse to take on
            // a NEW migrated contract when the contract module cache lacks the
            // headroom to host it without recompile thrash (which fills the fair
            // queue / OOMs memory-bound gateways). The signal the gate keys on
            // depends on the ACTIVE eviction policy, because the gate is only
            // sound if it predicts what eviction will actually do:
            //
            //  * Interest-tiered eviction ACTIVE (the DEFAULT since the canary
            //    validation on 2026-06-28; FREENET_MODULE_CACHE_INTEREST_TIERED
            //    unset or truthy): eviction reclaims COLD (no-interest) entries
            //    first, so admitting while the cache is full of cold modules
            //    merely evicts cold ones — no thrash. Here we gate on INTERESTED
            //    (hot) occupancy. This is the fix: the cache is an LRU that fills
            //    to ~100% with cold modules even on an idle node, so the old raw
            //    gate refused migration ~permanently on the small-cache majority
            //    for no real reason (live 0.2.86: 340/635 small nodes refused, all
            //    at interested-occ ~0% vs raw ~98%).
            //
            //  * Plain byte-LRU (operator opt-out, FREENET_MODULE_CACHE_INTEREST_TIERED=0):
            //    eviction reclaims the absolute LRU entry regardless of interest,
            //    so admitting into a full cache can evict a HOT module and
            //    recompile it — exactly the #4534 thrash. Cold-evictable headroom
            //    is NOT guaranteed to be reclaimed, so the interested-occupancy
            //    assumption does not hold (Codex review). Here we keep gating on
            //    RAW occupancy: identical to #4534's shipped behavior, so thrash
            //    protection is preserved unchanged.
            //
            // Preserving #4534 thrash protection is the load-bearing invariant, so
            // the gate matches the active policy rather than always trusting the
            // interested signal. With interest-tiered eviction now the default the
            // over-refusal fix reaches the small-cache majority; an operator who
            // forces plain byte-LRU keeps the raw gate. The recovered/recoverable
            // counter below tallies admissions the interested gate recovers
            // (default) or would recover (opt-out).
            //
            // This gates ONLY the directed-subscribe placement nudge; the node's
            // own local client subscribes/GETs are never gated here. The hint is
            // dropped silently and the holder re-proposes it on its next migration
            // trigger once headroom returns.
            // Read the per-node module-cache metrics off the same `Ring` the
            // caches publish into (a process-global until #4488).
            let module_cache_metrics = op_manager.ring.module_cache_metrics();
            let interest_tiered = crate::wasm_runtime::interest_tiered_enabled();
            // Force a fresh interested/cold recompute BEFORE reading the gauge —
            // but ONLY when the gate actually reads the interested gauge (tiered
            // eviction active). The interested-bytes split is otherwise throttled
            // to ≤ once per INTEREST_SHADOW_REFRESH_INTERVAL (10 s) on cache
            // touches, and on an idle node only refreshes at the 5-min
            // router-snapshot cadence; a burst of SubscribeHints would otherwise
            // be gated against a stale-low hot-occupancy reading and over-admit,
            // re-opening the #4534 thrash window (Codex review). The refresh is an
            // O(entries) scan under the cache mutex, so under plain LRU — where the
            // gate decision uses RAW occupancy (already fresh, O(1) per
            // insert/remove) and never reads the interested gauge — we skip it to
            // avoid paying that per-hint cost for nothing (Codex review). The
            // refresh makes the gauge reflect the cache's CURRENT resident hot set
            // at decision time (no-op before the runtime pool is built). It cannot
            // see migrations still in-flight (admitted but not yet
            // hosted/compiled), so a tight burst can still overshoot by ~one
            // migration-completion latency before completed migrations push the hot
            // set to the ceiling — a bounded, self-correcting residual, not the
            // unbounded 10-s-stale window. Uses the GAUGES-ONLY refresher: it must
            // NOT bump the throttle-sampled would-reclassify counter (a burst would
            // inflate it by hint volume) nor reset that throttle (Codex review).
            if interest_tiered {
                module_cache_metrics.refresh_interest_gauges_now();
            }
            // Recovered/recoverable measurement (#4534): bump BEFORE and
            // independent of the gate decision below. Counts inbound hints the raw
            // gate refuses but the interested gate would admit — actually RECOVERED
            // when interest-tiered eviction is active, or RECOVERABLE (the benefit
            // a tiered-eviction flip would unlock) when it is not. Kept
            // UNCONDITIONAL so plain-LRU nodes still record the recoverable benefit
            // — exactly the data that justifies flipping the eviction policy. This
            // read is cheap (O(1) atomics, no cache mutex / no scan), so unlike the
            // refresh above it costs nothing per hint; under plain LRU it reads the
            // throttled (≤10s-stale) interested gauge, which is fine for a coarse
            // benefit metric.
            if crate::wasm_runtime::migration_admission_recovered_now(
                &module_cache_metrics,
                MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT,
            ) {
                module_cache_metrics.record_migration_admission_recovered();
            }
            let admission = migration_admission_decision(&module_cache_metrics, interest_tiered);
            if !admission.admit {
                tracing::debug!(
                    key = %hint.key,
                    holder = %hint.holder,
                    ?source_addr,
                    gate_signal = if admission.interest_tiered {
                        "interested"
                    } else {
                        "raw"
                    },
                    occupancy_pct = ?admission.occupancy_pct,
                    interested_occupancy_pct = ?crate::wasm_runtime::contract_cache_interested_occupancy_pct(
                        &module_cache_metrics
                    ),
                    raw_occupancy_pct = ?crate::wasm_runtime::contract_cache_occupancy_pct(
                        &module_cache_metrics
                    ),
                    ceiling_pct = MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT,
                    "Refusing inbound SubscribeHint: contract module cache at/above \
                     the migration-admission occupancy ceiling — deferring placement \
                     migration to bound the hosted working set and avoid recompile \
                     thrash (#4534)"
                );
                op_manager
                    .ring
                    .placement_migration_metrics()
                    .record_refused_cache_admission();
                return Ok(());
            }
            tracing::debug!(
                key = %hint.key,
                holder = %hint.holder,
                ?source_addr,
                "Received SubscribeHint — starting directed subscribe to holder"
            );
            // Placement-migration telemetry (#4404 follow-up): count only the
            // hints we actually act on (all gates passed), i.e. the migrations
            // that actually start a directed subscribe and thereby host the
            // contract closer to its key.
            op_manager.ring.placement_migration_metrics().record_acted();
            subscribe::start_directed_subscribe(op_manager.clone(), hint.key, hint.holder);
            return Ok(());
        }
        NetMessageV1::Aborted(tx) => {
            // Drivers own their own cancellation; `Aborted` senders are
            // drivers themselves and the bypass handles in-driver delivery.
            tracing::debug!(
                %tx,
                tx_type = ?tx.transaction_type(),
                "Received Aborted message — driver owns cancellation, ignoring"
            );
            Ok(())
        }
    }
}

/// Maximum number of stale-contract `SyncStateToPeer` events emitted per
/// `Summaries` message handled (#3798 Gap 1, anti-amplification hardening).
///
/// A single peer whose summary diverges on N contracts would otherwise trigger
/// N `SyncStateToPeer` emissions in one `handle_interest_sync_message` call —
/// still targeted at one peer (O(1) peer, unlike `BroadcastStateChange`), but
/// an unbounded burst per message. Capping the per-message burst keeps the
/// notification channel responsive under a divergent (or crafted) summary.
///
/// Eventual consistency is preserved without a backlog queue: staleness is
/// re-derived every heartbeat cycle from the durable summary comparison in the
/// `Summaries` arm below (driven by the 5-minute interest heartbeat in
/// `ring::Ring::interest_heartbeat`, expiry-swept every 60 s by
/// `InterestManager::sweep_expired_interests`). Any contract over the cap this
/// cycle is re-detected and synced on a later cycle.
///
/// Starvation avoidance: when the stale set exceeds the cap, the emission loop
/// starts at a random offset and wraps (see the `rotate_left` in the `Summaries`
/// arm), so the cap window slides across the whole stale set over successive
/// cycles instead of always re-processing the same prefix. Without this, a
/// contract stuck in the leading `cap` positions — e.g. one whose
/// `SyncStateToPeer` is dropped on a full channel, lost in transit, or not
/// applied by the peer — would re-consume the budget every cycle and
/// permanently starve every contract past the cap. Random rotation makes each
/// over-cap contract eligible with independent probability each cycle, so its
/// expected wait is bounded regardless of whether the rest of the set
/// converges.
///
/// Value chosen to match the per-message burst-control family of existing
/// caps (`MAX_BROADCAST_RETRIES = 3`, `MAX_BROADCAST_STREAK_ENTRIES = 256`,
/// `MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT = 512`): 32 bounds the burst well
/// below those while staying comfortably above the typical handful of stale
/// contracts a healthy peer reports in one summary exchange.
const MAX_STALE_SYNCS_PER_SUMMARIES: usize = 32;

/// The per-message budget of stale-contract `SyncStateToPeer` emissions
/// (#3798 Gap 1): `min(stale_contracts_len, MAX_STALE_SYNCS_PER_SUMMARIES)`.
///
/// Returns the maximum number of events the `Summaries` handler may emit this
/// call. The caller increments an `emitted` counter only for contracts it
/// actually emits for (banned / no-local-state contracts are skipped without
/// consuming the budget) and stops once `emitted` reaches this value, so the
/// number of `SyncStateToPeer` events is hard-bounded by
/// [`MAX_STALE_SYNCS_PER_SUMMARIES`] regardless of `stale_contracts_len`.
fn stale_sync_emit_budget(stale_contracts_len: usize) -> usize {
    stale_contracts_len.min(MAX_STALE_SYNCS_PER_SUMMARIES)
}

/// Maximum contract-module-cache occupancy (percent of budget) at which this
/// node still accepts an inbound placement-migration `SubscribeHint` (#4534).
///
/// The ceiling is applied to whichever occupancy signal
/// [`migration_admission_decision`] selects for the active eviction policy
/// (INTERESTED occupancy under interest-tiered eviction, RAW occupancy under
/// plain byte-LRU). The ~10% headroom below 100% is reserved for the node's own
/// locally-requested contracts, whose directed subscribes are NOT gated here.
/// Refusal is silent and best-effort: the holder re-proposes the migration on
/// its next trigger, so placement migration resumes automatically once occupancy
/// falls back below the ceiling.
///
/// Under interest-tiered eviction the signal is the HOT (interested) working set,
/// which is self-limiting: as admitted migrations complete and compile in, the
/// hot set grows until it reaches the ceiling and the node sheds further load —
/// the #4534 thrash boundary, applied to the set that actually causes thrash. The
/// gate forces a fresh hot-occupancy read at decision time
/// (`refresh_interest_gauges_now`), so the only slack is migrations admitted but
/// not yet hosted/compiled: a tight burst can overshoot by ~one migration-
/// completion latency before completed migrations pull the gauge to the ceiling.
/// That residual is bounded and self-correcting (and the producer side of the
/// migration storm is bounded separately, #4440/#4145); accounting for in-flight,
/// not-yet-compiled migrations would need a reserved-bytes counter with its own
/// leak/TTL risk and is deliberately left as a follow-up.
const MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT: u64 = 90;

/// Whether to accept an inbound placement-migration hint given the selected
/// contract-module-cache occupancy signal. `None` (budget gauge not yet published
/// — no runtime pool built) admits, since there is no pressure signal to act on.
///
/// Split out as a pure function so the threshold logic is unit-testable without
/// constructing an `OpManager` or touching the global cache gauges (#4534).
fn migration_admission_allowed(occupancy_pct: Option<u64>) -> bool {
    match occupancy_pct {
        Some(pct) => pct < MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT,
        None => true,
    }
}

/// Outcome of the placement-migration admission gate for the current contract-
/// cache state. Carries the occupancy value the decision was based on and which
/// signal was used, both for the refuse-path log.
struct MigrationAdmission {
    /// Whether to admit the migration hint.
    admit: bool,
    /// Occupancy percent the decision was based on (the selected signal), or
    /// `None` when the budget gauge is unpublished.
    occupancy_pct: Option<u64>,
    /// `true` when interest-tiered eviction is active and the gate used INTERESTED
    /// occupancy; `false` when it fell back to RAW occupancy under plain LRU.
    interest_tiered: bool,
}

/// The placement-migration admission decision for the current contract-cache
/// state, keyed on the signal that matches the ACTIVE eviction policy.
///
/// Soundness depends on predicting what eviction will actually do (see the gate
/// comment in `handle_pure_network_message_v1`):
/// - interest-tiered eviction active → cold entries are reclaimed first, so the
///   INTERESTED (hot) occupancy is the right thrash signal (the #4534 fix);
/// - plain byte-LRU (operator opt-out) → eviction is interest-blind, so
///   cold-evictable headroom is not guaranteed to be reclaimed; gating on RAW
///   occupancy keeps #4534's shipped thrash protection unchanged.
///
/// `interest_tiered` is the live eviction policy (the gate passes
/// [`crate::wasm_runtime::interest_tiered_enabled`]); injected as a parameter so
/// the unit tests can pin BOTH policies deterministically without touching the
/// process environment (`gate_*` tests).
fn migration_admission_decision(
    metrics: &crate::wasm_runtime::ModuleCacheMetrics,
    interest_tiered: bool,
) -> MigrationAdmission {
    let occupancy_pct = if interest_tiered {
        crate::wasm_runtime::contract_cache_interested_occupancy_pct(metrics)
    } else {
        crate::wasm_runtime::contract_cache_occupancy_pct(metrics)
    };
    MigrationAdmission {
        admit: migration_admission_allowed(occupancy_pct),
        occupancy_pct,
        interest_tiered,
    }
}

#[cfg(test)]
mod migration_admission_tests {
    use super::{
        MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT,
        migration_admission_allowed, migration_admission_decision,
    };
    use crate::wasm_runtime::{
        ModuleCacheMetrics, contract_cache_interested_occupancy_pct, contract_cache_occupancy_pct,
    };

    /// No runtime pool / budget gauge yet → no pressure signal → admit.
    #[test]
    fn admits_when_cache_budget_uninitialized() {
        assert!(migration_admission_allowed(None));
    }

    /// Below the ceiling the node keeps accepting migration (the #4404 feature
    /// is not crippled on healthy nodes).
    #[test]
    fn admits_below_occupancy_ceiling() {
        assert!(migration_admission_allowed(Some(0)));
        assert!(migration_admission_allowed(Some(
            MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT - 1
        )));
    }

    /// At or above the ceiling the node sheds inbound migration — this is the
    /// #4534 fix. The over-budget transient (> 100%) is refused too.
    #[test]
    fn refuses_at_or_above_occupancy_ceiling() {
        assert!(!migration_admission_allowed(Some(
            MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT
        )));
        assert!(!migration_admission_allowed(Some(
            MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT + 5
        )));
        assert!(!migration_admission_allowed(Some(150)));
    }

    /// THE FIX (#4534), and its eviction-policy dependency: a cache that is
    /// LRU-full of COLD modules (raw occupancy 98%) but whose HOT/interested
    /// working set is ~empty (0%).
    ///
    /// - Under interest-tiered eviction (`interest_tiered = true`) the gate keys
    ///   on interested occupancy and ADMITS — the over-budget bytes are
    ///   cold-evictable, so admitting only evicts a cold module (no thrash). This
    ///   is the recovered admission.
    /// - Under plain byte-LRU (`interest_tiered = false`, the operator opt-out)
    ///   eviction is interest-blind, so cold-evictable headroom is NOT guaranteed
    ///   to be reclaimed; the gate keys on raw occupancy and REFUSES, exactly as
    ///   the shipped #4534 gate did — preserving thrash protection on opted-out
    ///   nodes (Codex review).
    ///
    /// Drives the exact composition the live gate uses, so a regression that
    /// keyed the tiered branch on raw occupancy (the bug this fixes) would flip
    /// the admit assertion and fail.
    #[test]
    fn gate_admits_cold_filled_only_under_tiered_eviction() {
        // raw 980/1000 = 98%, interested 0/1000 = 0%.
        let cold_filled = ModuleCacheMetrics::with_contract_gauges_for_test(980, 1000, 0);

        // Tiered eviction active → interested signal → ADMIT (the fix).
        let tiered = migration_admission_decision(&cold_filled, true);
        assert!(tiered.interest_tiered);
        assert_eq!(tiered.occupancy_pct, Some(0), "hot set is empty");
        assert!(
            tiered.admit,
            "under tiered eviction a cold-filled cache must ADMIT — admitting \
             evicts only a cold module (no thrash)"
        );

        // Plain LRU (operator opt-out) → raw signal → REFUSE (preserve #4534 on
        // opted-out nodes; cold-evictable headroom is not guaranteed to be
        // reclaimed).
        let plain = migration_admission_decision(&cold_filled, false);
        assert!(!plain.interest_tiered);
        assert_eq!(
            plain.occupancy_pct,
            Some(98),
            "raw occupancy under plain LRU"
        );
        assert!(
            !plain.admit,
            "under plain byte-LRU the gate must keep refusing at raw 98% — \
             admitting could evict a hot module and re-open the #4534 thrash"
        );

        // Witness the divergence directly via the occupancy helpers.
        assert_eq!(
            contract_cache_interested_occupancy_pct(&cold_filled),
            Some(0)
        );
        assert_eq!(contract_cache_occupancy_pct(&cold_filled), Some(98));
    }

    /// Thrash protection preserved under tiered eviction (#4534): raw occupancy
    /// 98% AND the HOT/interested set is also near budget (95%) → REFUSE.
    /// Admitting here would force recompile thrash even with cold-first eviction.
    #[test]
    fn gate_refuses_when_hot_set_near_budget_under_tiered() {
        // raw 980/1000 = 98%, interested 950/1000 = 95%.
        let hot = ModuleCacheMetrics::with_contract_gauges_for_test(980, 1000, 950);
        let d = migration_admission_decision(&hot, true);
        assert_eq!(d.occupancy_pct, Some(95));
        assert!(
            !d.admit,
            "a genuinely hot working set near budget must REFUSE (thrash risk)"
        );
    }

    /// Boundary (tiered eviction): interested occupancy exactly at the ceiling
    /// refuses; one below admits. `None` (unpublished budget) admits under both
    /// policies.
    #[test]
    fn gate_boundary_and_unpublished_budget() {
        // interested exactly at the ceiling → refuse.
        let at_ceiling = ModuleCacheMetrics::with_contract_gauges_for_test(1000, 1000, 900);
        assert_eq!(
            contract_cache_interested_occupancy_pct(&at_ceiling),
            Some(MIGRATION_ADMISSION_MAX_CONTRACT_CACHE_INTERESTED_OCCUPANCY_PCT)
        );
        assert!(!migration_admission_decision(&at_ceiling, true).admit);

        // interested one below the ceiling → admit (even with raw at 100%).
        let below = ModuleCacheMetrics::with_contract_gauges_for_test(1000, 1000, 890);
        assert_eq!(contract_cache_interested_occupancy_pct(&below), Some(89));
        assert!(migration_admission_decision(&below, true).admit);

        // Unpublished budget gauge → no pressure signal → admit under either
        // eviction policy.
        let fresh = ModuleCacheMetrics::new();
        for tiered in [true, false] {
            let d = migration_admission_decision(&fresh, tiered);
            assert_eq!(d.occupancy_pct, None);
            assert!(d.admit);
        }
    }
}

/// Per-contract disposition in the stale-sync emission loop, used to model the
/// loop's cap accounting in a unit test without constructing an `OpManager`.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StaleSyncDisposition {
    /// Contract is not banned and has local state — emits a `SyncStateToPeer`
    /// (and so consumes one unit of the emit budget).
    Emit,
    /// Contract is banned — skipped without emitting or consuming the budget.
    Banned,
    /// No local state available — skipped without emitting or consuming the
    /// budget.
    NoState,
}

/// Pure model of the stale-sync emission loop's cap accounting (#3798 Gap 1).
///
/// Mirrors the `for contract in stale_contracts` loop in the `Summaries` arm of
/// [`handle_interest_sync_message`]: break once `emitted` reaches the budget;
/// `Banned` / `NoState` contracts are skipped without consuming the budget;
/// every `Emit` before the budget is exhausted counts. Returns the number of
/// `SyncStateToPeer` events the real loop would emit for the given sequence.
///
/// Kept in lockstep with the production loop by the
/// `stale_sync_loop_uses_emit_budget_pin` source-scrape test, which asserts the
/// loop still applies this budget-and-break structure.
#[cfg(test)]
fn count_stale_syncs_emitted(dispositions: &[StaleSyncDisposition]) -> usize {
    let budget = stale_sync_emit_budget(dispositions.len());
    let mut emitted = 0usize;
    for d in dispositions {
        if emitted >= budget {
            break;
        }
        if *d == StaleSyncDisposition::Emit {
            emitted += 1;
        }
    }
    emitted
}

/// Pure model of which original stale-contract indices the capped loop emits
/// for, given the random rotation start used in the `Summaries` arm (#3798
/// Gap 1 starvation avoidance).
///
/// Mirrors `stale_contracts.rotate_left(start)` followed by the capped
/// emit loop where every contract is emittable (the all-`Emit` case, which is
/// the worst case for starvation): after rotation, loop position `p` holds
/// original index `(start + p) % total`, and the loop emits the first `budget`
/// positions. Returns the set of original indices that would be emitted this
/// cycle. Used to prove that, across rotation starts, the cap window covers the
/// whole stale set (no index is permanently unreachable).
#[cfg(test)]
fn emitted_indices_for_rotation(total: usize, start: usize) -> Vec<usize> {
    let budget = stale_sync_emit_budget(total);
    (0..budget).map(|p| (start + p) % total).collect()
}

/// Build the `NodeEvent` that heals a single peer by sending it our local
/// state for one contract.
///
/// Two production emit sites route through this builder:
///
/// 1. The summary-mismatch heal in `handle_interest_sync_message`'s
///    `Summaries` arm — `target` is the peer that reported the stale summary.
/// 2. The proximity-cache overlap path in `handle_connect_msg` — `target` is
///    the neighbor whose interest announcement just overlapped a contract we
///    also host.
///
/// This MUST be a `SyncStateToPeer` — a **targeted** send to exactly the one
/// peer (`target`) — and MUST NOT be a `BroadcastStateChange`, which fans the
/// state out to *all* subscribers of the contract. Both sites are the same
/// fan-out regression class (#3791/#3796): a single overlap/mismatch must cost
/// O(1) peer transmissions, not O(subscribers).
///
/// # Why this is its own function (regression guard for #3791 / #3796)
///
/// A six-week-old regression (#3791) had the summary-mismatch handler emit
/// `BroadcastStateChange` here. A misleading comment claimed it "sends state
/// only to peers with stale summaries", but the production dispatch
/// (`handle_broadcast_state_change` → `get_broadcast_targets_update`) fanned out
/// to every subscriber (~28 peers), turning one mismatch into O(N × fan-out)
/// transmissions and producing 19:1–163:1 upload/download ratios in production.
///
/// The original regression test only covered `InterestManager` data logic and
/// would have stayed green if the `node.rs` dispatch were reverted. Isolating
/// the dispatch *decision* (which `NodeEvent` variant, aimed at which peer) in
/// this pure function lets `stale_peer_sync_event_is_targeted_not_broadcast`
/// pin it directly: reverting to a `BroadcastStateChange` here fails that test.
///
/// See `.claude/rules/operations.md` (Event emission review) and
/// `docs/architecture/event-dispatch.md` for the targeted-vs-fan-out
/// distinction.
fn stale_peer_sync_event(
    key: freenet_stdlib::prelude::ContractKey,
    new_state: freenet_stdlib::prelude::WrappedState,
    target: std::net::SocketAddr,
) -> crate::message::NodeEvent {
    crate::message::NodeEvent::SyncStateToPeer {
        key,
        new_state,
        target,
    }
}

/// Handle incoming InterestSync messages for delta-based state synchronization.
///
/// This function processes the interest exchange protocol:
/// - `Interests`: Connection-time discovery of shared contract interests
/// - `Summaries`: State summaries for shared contracts
/// - `ChangeInterests`: Incremental interest changes
/// - `ResyncRequest`: Request full state when delta application fails
async fn handle_interest_sync_message(
    op_manager: &Arc<OpManager>,
    source: std::net::SocketAddr,
    message: crate::message::InterestMessage,
) -> Option<crate::message::InterestMessage> {
    use crate::message::{InterestMessage, SummaryEntry};
    use crate::ring::interest::contract_hash;

    match message {
        InterestMessage::Interests { hashes } => {
            tracing::debug!(
                from = %source,
                hash_count = hashes.len(),
                "Received Interests message"
            );

            let peer_key = get_peer_key_from_addr(op_manager, source);

            // Full-replace semantics: the incoming hashes represent the peer's
            // complete interest set. Remove entries for contracts whose hash is
            // NOT in the incoming set, then register/refresh the rest.
            if let Some(ref pk) = peer_key {
                let incoming_hashes: std::collections::HashSet<u32> =
                    hashes.iter().copied().collect();
                let current_contracts = op_manager.interest_manager.get_contracts_for_peer(pk);

                // Hash collisions (FNV-1a u32) can cause a stale entry to
                // survive if its hash collides with a live one. This is the
                // safe direction — false negatives on removal, not false
                // positives — and extremely rare in practice.
                let mut removed = 0usize;
                for contract in &current_contracts {
                    let h = contract_hash(contract);
                    if !incoming_hashes.contains(&h) {
                        op_manager
                            .interest_manager
                            .remove_peer_interest(contract, pk);
                        removed += 1;
                    }
                }
                if removed > 0 {
                    tracing::debug!(
                        from = %source,
                        removed,
                        "Full-replace: removed stale interest entries"
                    );
                }
            }

            // Find contracts we share interest in
            let matching = op_manager.interest_manager.get_matching_contracts(&hashes);

            // Build summaries for shared contracts and register/refresh peer interest
            let mut entries = Vec::with_capacity(matching.len());
            for contract in matching {
                let hash = contract_hash(&contract);
                // Only summarize contracts we host or actively serve; phantom
                // peer-interest contracts (no state, no live subscriber) have
                // nothing to advertise and their pointless GetSummaryQuery
                // round-trips were the dominant #4473 storm. See
                // `summary_if_hosted_or_in_use`.
                let summary = summary_if_hosted_or_in_use(op_manager, &contract).await;
                entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));

                if let Some(ref pk) = peer_key {
                    // Refresh TTL for existing entries (preserves cached summary).
                    // Only register new interest if this is a genuinely new entry;
                    // otherwise register_peer_interest would overwrite the cached
                    // summary with None, defeating delta optimization.
                    if op_manager
                        .interest_manager
                        .get_peer_interest(&contract, pk)
                        .is_some()
                    {
                        op_manager
                            .interest_manager
                            .refresh_peer_interest(&contract, pk);
                    } else {
                        let is_new = op_manager.interest_manager.register_peer_interest(
                            &contract,
                            pk.clone(),
                            None, // New entry; summary arrives in their Summaries response
                            false,
                        );
                        if is_new {
                            // #4359 (MUST-FIX 1): an Interests-sync registration
                            // makes this peer a viable broadcast target. Flush
                            // any deferred fresh-contract broadcast so a cold-id
                            // PUT that gave up with no targets reaches it.
                            op_manager
                                .flush_pending_broadcast_on_interest(&contract)
                                .await;
                        }
                    }
                }
            }

            if entries.is_empty() {
                None
            } else {
                Some(InterestMessage::Summaries { entries })
            }
        }

        InterestMessage::Summaries { entries } => {
            tracing::debug!(
                from = %source,
                entry_count = entries.len(),
                "Received Summaries message"
            );

            // Update peer summaries and detect stale peers (#3221).
            //
            // Compare each peer summary with our own before storing it. If they
            // differ, the peer missed an earlier broadcast. We send state only
            // to the specific peer that reported the stale summary via
            // SyncStateToPeer (not BroadcastStateChange which fans out to ALL
            // subscribers). This avoids O(peers^2) broadcast storms where N
            // peers each trigger a full fan-out broadcast. See #3791.
            //
            // Both sides may detect the same mismatch (A sees B is stale, B sees
            // A is stale). This is safe: the contract's merge semantics (CRDTs
            // etc.) ensure the newer/correct state wins regardless of push order.
            //
            // When either summary is None, we skip the comparison. A peer with
            // no summary has no state yet and should receive it via the normal
            // subscription/GET flow, not via broadcast.
            let peer_key = get_peer_key_from_addr(op_manager, source);
            let mut stale_contracts = Vec::new();
            // Collect (contract, state_hash) for deferred StateConfirmed telemetry.
            // Only emitted in direct-runner mode to avoid .await points that change
            // turmoil task scheduling.
            let emit_confirmed = crate::config::SimulationIdleTimeout::is_enabled();
            let mut confirmed_states: Vec<(freenet_stdlib::prelude::ContractKey, String)> =
                Vec::new();

            if let Some(pk) = peer_key {
                for entry in entries {
                    for contract in op_manager.interest_manager.lookup_by_hash(entry.hash) {
                        if !op_manager.interest_manager.has_local_interest(&contract) {
                            continue;
                        }

                        let their_summary = entry.to_summary();
                        // Only summarize contracts we host or actively serve (see
                        // `summary_if_hosted_or_in_use`, #4473). A contract skipped
                        // here is neither hosted nor has a live subscriber, so it
                        // has nothing to advertise and no subscriber whose stale
                        // copy we'd heal: `our_summary` is None → not stale → no
                        // SyncStateToPeer, while the loop round-trip is avoided.
                        let our_summary = summary_if_hosted_or_in_use(op_manager, &contract).await;

                        if emit_confirmed {
                            if let Some(ref summary) = our_summary {
                                confirmed_states.push((contract, hex::encode(summary.as_ref())));
                            }
                        }

                        let is_stale = our_summary
                            .as_ref()
                            .zip(their_summary.as_ref())
                            .is_some_and(|(ours, theirs)| ours.as_ref() != theirs.as_ref());

                        op_manager.interest_manager.update_peer_summary(
                            &contract,
                            &pk,
                            their_summary,
                        );

                        if is_stale && !stale_contracts.contains(&contract) {
                            stale_contracts.push(contract);
                        }
                    }
                }
            }

            // Send current state only to the specific peer that reported a stale
            // summary. Previously this emitted BroadcastStateChange which fanned
            // out to ALL subscribers (~28 peers), causing O(peers^2) traffic when
            // many peers reported mismatches within the same heartbeat cycle.
            //
            // #3798 Gap 1: cap the number of SyncStateToPeer events emitted per
            // Summaries message so a peer diverging on many contracts cannot
            // trigger an unbounded burst in one handler call. `emit_budget`
            // bounds *emitted* events (not loop iterations) — banned and
            // no-local-state contracts are skipped without consuming the
            // budget. Overflow is not dropped permanently: each later
            // heartbeat re-derives the still-stale set from the durable summary
            // comparison above and syncs the next batch (see
            // MAX_STALE_SYNCS_PER_SUMMARIES rustdoc for the eventual-consistency
            // argument).
            let total_stale = stale_contracts.len();
            let emit_budget = stale_sync_emit_budget(total_stale);
            // Starvation avoidance: when the stale set exceeds the cap, rotate
            // the start of the iteration by a random offset so the cap window
            // slides across the whole set over successive cycles. Without this,
            // a contract stuck in the leading `cap` positions (dropped emit,
            // lost packet, peer fails to apply) would re-consume the budget
            // every cycle and permanently starve everything past the cap. No
            // rotation when total_stale <= cap — every contract is emitted
            // anyway, so the order does not matter. GlobalRng keeps this
            // deterministic under simulation/test.
            if total_stale > emit_budget {
                let start = crate::config::GlobalRng::random_range(0..total_stale);
                stale_contracts.rotate_left(start);
            }
            let mut emitted = 0usize;
            for contract in stale_contracts {
                if emitted >= emit_budget {
                    // Cap reached; the still-stale remainder (any that are not
                    // banned / have local state) is re-detected and synced on a
                    // subsequent interest-sync cycle rather than emitted now.
                    tracing::warn!(
                        stale_peer = %source,
                        total_stale,
                        emitted,
                        cap = MAX_STALE_SYNCS_PER_SUMMARIES,
                        "Stale-contract sync cap hit for Summaries message; \
                         deferring the remainder to a later interest-sync cycle"
                    );
                    break;
                }
                // Phase 7 egress gate. Don't repair a stale peer's
                // summary mismatch by pushing state for a contract
                // we have banned — same rationale as the inbound
                // wire-boundary drop, applied to the proactive heal
                // path.
                if op_manager.ring.contract_ban_list.is_banned(contract.id()) {
                    tracing::debug!(
                        %contract,
                        stale_peer = %source,
                        phase = "interest_sync_banned_skip",
                        "skipping summary-mismatch sync for banned contract"
                    );
                    continue;
                }
                let Some(state) = get_contract_state(op_manager, &contract).await else {
                    tracing::trace!(
                        contract = %contract,
                        "Skipping stale-peer sync — no local state available"
                    );
                    continue;
                };
                // Count this contract against the emit budget: it has local
                // state and is not banned, so we are about to emit a
                // SyncStateToPeer event for it. Increment before the emit so a
                // channel-full drop still consumes the budget — the dropped
                // event is retried next cycle exactly like an over-cap one.
                emitted += 1;
                // Fires per stale-peer detection during interest sync, which
                // is dominant on hot contracts. Diagnostic-grade rather than
                // user-actionable; keep accessible via RUST_LOG=…=debug.
                tracing::debug!(
                    contract = %contract,
                    stale_peer = %source,
                    "Summary mismatch in interest sync — syncing state to stale peer"
                );
                // Non-blocking emit: SyncStateToPeer is best-effort
                // gossip — if dropped, the next interest-sync round
                // will retry. Blocking here would stack the heal
                // path on the same notification channel the executor
                // is trying to keep responsive (#4145 / #4234).
                // Targeted heal: `stale_peer_sync_event` builds a
                // `SyncStateToPeer` aimed at exactly `source`, NEVER an
                // all-subscriber fan-out. Pinned by
                // `stale_peer_sync_event_is_targeted_not_broadcast` (#3791/#3796).
                if let Err(e) =
                    op_manager.try_notify_node_event(stale_peer_sync_event(contract, state, source))
                {
                    // Best-effort by design (see comment above); log
                    // at debug to keep the caller layer in step with
                    // the helper-internal downgrade (#4238).
                    tracing::debug!(
                        contract = %contract,
                        error = %e,
                        "Failed to emit SyncStateToPeer for stale peer correction (best-effort)"
                    );
                }
            }

            // Emit deferred StateConfirmed telemetry so the convergence
            // checker has up-to-date state hashes for CRDT-merged state.
            for (key, state_hash) in confirmed_states {
                if let Some(event) =
                    crate::tracing::NetEventLog::state_confirmed(&op_manager.ring, key, state_hash)
                {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(event))
                        .await;
                }
            }

            // No response needed for Summaries
            None
        }

        InterestMessage::ChangeInterests { added, removed } => {
            tracing::debug!(
                from = %source,
                added_count = added.len(),
                removed_count = removed.len(),
                "Received ChangeInterests message"
            );

            let peer_key = get_peer_key_from_addr(op_manager, source);

            // Handle removals
            if let Some(ref pk) = peer_key {
                for hash in removed {
                    // Handle hash collisions - remove interest from all matching contracts
                    for contract in op_manager.interest_manager.lookup_by_hash(hash) {
                        op_manager
                            .interest_manager
                            .remove_peer_interest(&contract, pk);
                    }
                }
            }

            // Handle additions - respond with summaries for newly shared contracts
            let mut entries = Vec::new();
            if let Some(ref pk) = peer_key {
                for hash in added {
                    // Handle hash collisions - process all matching contracts
                    for contract in op_manager.interest_manager.lookup_by_hash(hash) {
                        // Only process if we have local interest in this contract
                        if !op_manager.interest_manager.has_local_interest(&contract) {
                            continue;
                        }

                        // Register their interest — but preserve an existing
                        // entry's `is_upstream` flag (and cached summary). A
                        // ChangeInterests "added" gossip from a peer that is
                        // ALREADY our upstream host (is_upstream = true, set when
                        // we subscribed through it — subscribe.rs / operations.rs)
                        // must NOT be downgraded to a plain downstream interest.
                        // A bare re-registration with is_upstream=false overwrites
                        // the whole PeerInterest, flipping is_upstream true -> false
                        // and wiping the delta-sync summary to None. The is_upstream
                        // clobber defeats event-driven collapse:
                        // `send_unsubscribe_upstream` locates the upstream via
                        // `is_upstream`, so after such a gossip the Unsubscribe is
                        // never sent upstream and the chain only lapses on lease
                        // expiry (~6 min stale window). This path is EVENT-DRIVEN,
                        // not periodic: `ChangeInterests` is emitted only on a 0->1
                        // interest transition (`broadcast_change_interests`); the
                        // ~5-min interest-sync heartbeat sends `Interests` (the
                        // guarded full-replace arm above), NOT `ChangeInterests`.
                        // Hitting a real upstream edge needs the upstream's own
                        // interest to lapse-and-revive so it re-emits an "added" for
                        // a contract we already hold it as upstream for — uncommon on
                        // current main (hosts renew leases unconditionally, so
                        // upstream interest does not lapse) but load-bearing under
                        // piece-D interest-gated renewal (#4642). Mirror the
                        // refresh-guard the `Interests` full-replace arm above uses.
                        // (Guarded wiring pinned by
                        // change_interests_arm_guards_register_with_refresh_pin.)
                        let is_new = if op_manager
                            .interest_manager
                            .get_peer_interest(&contract, pk)
                            .is_some()
                        {
                            op_manager
                                .interest_manager
                                .refresh_peer_interest(&contract, pk);
                            false
                        } else {
                            op_manager.interest_manager.register_peer_interest(
                                &contract,
                                pk.clone(),
                                None,
                                false,
                            )
                        };
                        if is_new {
                            // #4359 (MUST-FIX 1): a ChangeInterests addition
                            // makes this peer a viable broadcast target. Flush
                            // any deferred fresh-contract broadcast so a cold-id
                            // PUT that gave up with no targets reaches it.
                            op_manager
                                .flush_pending_broadcast_on_interest(&contract)
                                .await;
                        }

                        // Get our summary to send back — only for contracts we
                        // host or actively serve (see `summary_if_hosted_or_in_use`,
                        // #4473).
                        let summary = summary_if_hosted_or_in_use(op_manager, &contract).await;
                        entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));
                    }
                }
            }

            if entries.is_empty() {
                None
            } else {
                Some(InterestMessage::Summaries { entries })
            }
        }

        InterestMessage::ResyncRequest { key } => {
            tracing::info!(
                from = %source,
                contract = %key,
                event = "resync_request_received",
                "Received ResyncRequest - peer needs full state"
            );

            // Track this for testing - high counts indicate incorrect summary caching (PR #2763)
            op_manager.interest_manager.record_resync_request_received();
            crate::config::GlobalTestMetrics::record_resync_request();

            // CHEAP existence check BEFORE the rate limiters (#4864 round-4 P1).
            // Both limiter buckets allocate a slot (vacant-at-capacity) BEFORE any
            // existence check, so a peer spraying bogus contract keys could
            // exhaust the strictly-capped limiter maps and then DENY new legit
            // (peer, contract) keys (fail-closed → no response). A cheap
            // state-presence probe rejects unknown contracts before they can touch
            // a limiter slot. The ASYNC variant (#4864 round-5) keeps the redb
            // synchronous point-lookup fast-path AND adds a real SQLite EXISTS
            // probe, so the gate is backend-agnostic (the sync probe was a no-op
            // on sqlite builds, reopening the hole there). (A known contract whose
            // state we can no longer fetch a moment later still bails at
            // get_contract_state below, but only after passing this gate plus the
            // rate limits.)
            if !op_manager.ring.contract_state_present_async(&key).await {
                tracing::debug!(
                    from = %source,
                    contract = %key,
                    event = "resync_response_no_state",
                    "ResyncRequest for a contract we have no state for — not responding (pre-limiter existence check)"
                );
                return None;
            }

            // Rate-limit the full-state response per (peer, contract) (#4861).
            // This is the mixed-version-rollout guard: a not-yet-upgraded peer
            // that still emits unlimited ResyncRequests must not be able to make
            // this (upgraded) node full-state-reply in a loop. When suppressed,
            // simply don't respond — the requester will retry later.
            if !op_manager
                .ring
                .resync_response_limiter
                .check_and_record((source, *key.id()))
            {
                crate::config::GlobalTestMetrics::record_resync_response_suppressed_per_peer();
                tracing::debug!(
                    from = %source,
                    contract = %key,
                    event = "resync_response_suppressed",
                    "ResyncResponse suppressed by per-(peer, contract) rate limit"
                );
                return None;
            }

            // GLOBAL per-contract cap, checked AFTER the per-peer limit (#4861).
            // Per-(peer, contract) alone is insufficient: production saw ~45
            // distinct requester IPs drive ~9,733 full-state responses/day for
            // one forked contract. This bounds a single contract's total
            // resync-response cost (~12/min) regardless of requester count.
            if !op_manager
                .ring
                .resync_response_global_limiter
                .check_and_record(*key.id())
            {
                crate::config::GlobalTestMetrics::record_resync_response_suppressed_global();
                tracing::debug!(
                    from = %source,
                    contract = %key,
                    event = "resync_response_suppressed_global",
                    "ResyncResponse suppressed by global per-contract rate limit"
                );
                return None;
            }

            // Clear cached summary for this peer
            let peer_key = get_peer_key_from_addr(op_manager, source);
            if let Some(ref pk) = peer_key {
                op_manager
                    .interest_manager
                    .update_peer_summary(&key, pk, None);
            }

            // Get PeerKeyLocation for telemetry
            let from_peer = op_manager.ring.connection_manager.get_peer_by_addr(source);

            // Emit telemetry for ResyncRequest received
            if let Some(ref from_pkl) = from_peer {
                if let Some(event) = crate::tracing::NetEventLog::resync_request_received(
                    &op_manager.ring,
                    key,
                    from_pkl.clone(),
                ) {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(event))
                        .await;
                }
            } else {
                tracing::debug!(
                    contract = %key,
                    source = %source,
                    "ResyncRequest telemetry skipped: peer lookup failed"
                );
            }

            // Fetch current state from store
            let state = get_contract_state(op_manager, &key).await;
            let Some(state) = state else {
                tracing::warn!(
                    contract = %key,
                    "ResyncRequest for contract we don't have state for"
                );
                return None;
            };

            // Fetch our summary (serving a peer's ResyncRequest — relay-tier).
            let summary =
                get_contract_summary(op_manager, &key, crate::contract::Priority::NetworkRelay)
                    .await;
            let Some(summary) = summary else {
                tracing::warn!(
                    contract = %key,
                    "ResyncRequest for contract we can't compute summary for"
                );
                return None;
            };

            tracing::info!(
                to = %source,
                contract = %key,
                state_size = state.as_ref().len(),
                summary_size = summary.as_ref().len(),
                event = "resync_response_sent",
                "Sending ResyncResponse with full state"
            );

            // Emit telemetry for ResyncResponse sent
            if let Some(ref to_pkl) = from_peer {
                if let Some(event) = crate::tracing::NetEventLog::resync_response_sent(
                    &op_manager.ring,
                    key,
                    to_pkl.clone(),
                    state.as_ref().len(),
                ) {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(event))
                        .await;
                }
            }

            Some(InterestMessage::ResyncResponse {
                key,
                state_bytes: state.as_ref().to_vec(),
                summary_bytes: summary.as_ref().to_vec(),
            })
        }

        InterestMessage::ResyncResponse {
            key,
            state_bytes,
            summary_bytes,
        } => {
            tracing::info!(
                from = %source,
                contract = %key,
                state_size = state_bytes.len(),
                event = "resync_response_received",
                "Received ResyncResponse with full state"
            );

            // Apply the full state using an update.
            //
            // This full-state WASM merge is deliberately NOT gated by the
            // merge-failure backoff (#4861): unlike the broadcast drivers, the
            // resync path is already double-bounded — the emitter-side per- and
            // per-(peer,contract) + global rate limits throttle how often
            // ResyncRequests (and thus these responses) are produced at all, and
            // epoch preemption caps the cost of each individual merge. Gating it
            // here would also risk suppressing a genuine heal.
            let state = freenet_stdlib::prelude::State::from(state_bytes.clone());
            let update_data = freenet_stdlib::prelude::UpdateData::State(state);

            // Send to contract handler
            use crate::contract::ContractHandlerEvent;
            match op_manager
                .notify_contract_handler(ContractHandlerEvent::UpdateQuery {
                    key,
                    data: update_data,
                    related_contracts: Default::default(),
                })
                .await
            {
                Ok(ContractHandlerEvent::UpdateResponse {
                    new_value: Ok(_),
                    state_changed: true,
                    ..
                }) => {
                    // NOTE (#4861): deliberately do NOT reset the merge-failure
                    // backoff here. A full-state resync apply "succeeds" (even
                    // with changed=true) merely by REPLACING the local state — it
                    // proves nothing about convergence. In the observed semantic
                    // fork-oscillation poison class, two stable divergent states
                    // reject each other's deltas and every resync apply just
                    // flips the node to the other fork (~1 cycle/min forever); if
                    // that reset the backoff, the backoff would never trip and the
                    // storm would continue. The backoff is reset ONLY by a
                    // genuine successful DELTA merge in the broadcast driver —
                    // full-state merges (streaming broadcast included) never
                    // reset, since they carry the same fork-flip ambiguity. See
                    // the source-scrape pin
                    // `resync_apply_does_not_reset_merge_backoff`.
                    //
                    // BUT this CHANGED apply advances the state, which
                    // invalidates the failed-payload MEMO's premise (a delta that
                    // failed against the old state may be valid against the new
                    // one) — clear ONLY the memo (#4864 round-4 P2). Gated on
                    // `state_changed: true` (#4864 round-5 item 8): the executor
                    // returns `UpdateResponse { state_changed: false }` for a
                    // redundant no-op apply (CurrentWon / NoChange-with-fetch),
                    // and invalidating on THOSE would needlessly re-admit
                    // known-bad payloads without a real state advance. This is NOT
                    // a backoff reset: no cooldown channel is touched, so the
                    // strictly-delta-only no-reset doctrine and the
                    // `resync_apply_does_not_reset_merge_backoff` pin (which
                    // forbids a backoff reset from any node.rs site) both still
                    // hold — `invalidate_payload_memo` is a distinct method that
                    // clears only the memo, never a cooldown.
                    op_manager
                        .ring
                        .merge_backoff
                        .invalidate_payload_memo(key.id());
                    tracing::info!(
                        from = %source,
                        contract = %key,
                        event = "resync_applied",
                        changed = true,
                        "ResyncResponse state applied successfully"
                    );
                }
                Ok(ContractHandlerEvent::UpdateResponse {
                    new_value: Ok(_),
                    state_changed: false,
                    ..
                })
                | Ok(ContractHandlerEvent::UpdateNoChange { .. }) => {
                    // A no-op resync apply (CurrentWon / NoChange — the executor
                    // returns either `UpdateResponse { state_changed: false }` or
                    // `UpdateNoChange`) did NOT advance the state. As above
                    // (#4861) it is not a convergence signal, so it must not reset
                    // the backoff — AND it is not a memo-invalidation either
                    // (#4864 round-5 item 8): the failed-payload memo's premise
                    // still holds when the state did not change, so
                    // `invalidate_payload_memo` is deliberately NOT called here,
                    // and the log field is honestly `changed = false`.
                    tracing::info!(
                        from = %source,
                        contract = %key,
                        event = "resync_applied",
                        changed = false,
                        "ResyncResponse state unchanged (already had this state)"
                    );
                }
                Ok(other) => {
                    // Display, not Debug, for `other` — `?other` Debug-prints
                    // the full UpdateResponse, expands the inner
                    // anyhow::Error, and emits a ~15-line backtrace per call
                    // under queue saturation (issue #4251).
                    // ContractHandlerEvent's hand-written Display
                    // (`contract/handler.rs:706`) gives a single-line variant
                    // summary without expanding nested anyhow chains.
                    tracing::debug!(
                        from = %source,
                        contract = %key,
                        event = "resync_failed",
                        response = %other,
                        "Unexpected response to resync update"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        from = %source,
                        contract = %key,
                        event = "resync_failed",
                        error = %e,
                        "Failed to apply resync state"
                    );
                }
            }

            // Update the peer's summary in our interest tracker
            let peer_key = get_peer_key_from_addr(op_manager, source);
            if let Some(pk) = peer_key {
                let summary = freenet_stdlib::prelude::StateSummary::from(summary_bytes);
                op_manager
                    .interest_manager
                    .update_peer_summary(&key, &pk, Some(summary));
            }

            // No response needed
            None
        }
    }
}

/// Get the contract state from the state store.
async fn get_contract_state(
    op_manager: &Arc<OpManager>,
    key: &freenet_stdlib::prelude::ContractKey,
) -> Option<freenet_stdlib::prelude::WrappedState> {
    get_contract_state_by_id(op_manager, key.id())
        .await
        .map(|(_, state)| state)
}

/// Get the contract state by instance ID, returning both the full `ContractKey` and state.
///
/// Used for proactive state sync when proximity cache discovers overlapping contracts,
/// where we only have a `ContractInstanceId` (not a full `ContractKey`).
async fn get_contract_state_by_id(
    op_manager: &Arc<OpManager>,
    instance_id: &freenet_stdlib::prelude::ContractInstanceId,
) -> Option<(
    freenet_stdlib::prelude::ContractKey,
    freenet_stdlib::prelude::WrappedState,
)> {
    use crate::contract::ContractHandlerEvent;

    match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *instance_id,
            return_contract_code: false,
        })
        .await
    {
        Ok(ContractHandlerEvent::GetResponse {
            key: Some(key),
            response: Ok(store_response),
        }) => store_response.state.map(|state| (key, state)),
        Ok(ContractHandlerEvent::GetResponse {
            response: Err(e), ..
        }) => {
            tracing::warn!(
                contract = %instance_id,
                error = %e,
                "Failed to get contract state by instance id"
            );
            None
        }
        _ => None,
    }
}

/// Get the contract state summary using the contract's summarize_state method.
///
/// `priority` lets the periodic interest-sync path issue the summarize at
/// [`Priority::Background`](crate::contract::Priority::Background) so the
/// post-#4473 residual summarize load never starves client work (#4534), while
/// relay/resync callers keep the default `NetworkRelay` precedence.
async fn get_contract_summary(
    op_manager: &Arc<OpManager>,
    key: &freenet_stdlib::prelude::ContractKey,
    priority: crate::contract::Priority,
) -> Option<freenet_stdlib::prelude::StateSummary<'static>> {
    use crate::contract::ContractHandlerEvent;

    match op_manager
        .notify_contract_handler_prioritized(
            ContractHandlerEvent::GetSummaryQuery { key: *key },
            priority,
        )
        .await
    {
        Ok(ContractHandlerEvent::GetSummaryResponse {
            summary: Ok(summary),
            ..
        }) => Some(summary),
        Ok(ContractHandlerEvent::GetSummaryResponse {
            summary: Err(e), ..
        }) => {
            // Fires repeatedly when the executor queue is saturated for a hot
            // contract (issue #4251). Demoted to debug because the actionable
            // signal is the queue saturation itself, not the per-summary
            // failure.
            tracing::debug!(
                contract = %key,
                error = %e,
                "Failed to get contract summary"
            );
            None
        }
        _ => None,
    }
}

/// Compute our summary for `key` for interest-sync, but ONLY if we host it or
/// are actively serving it (a live local-client or downstream subscriber);
/// otherwise return `None` without touching the contract-handling loop.
///
/// A node can carry *interest* in a contract it neither hosts nor serves —
/// phantom interest advertised by peers in the InterestSync heartbeat, e.g. the
/// after-effect of the placement migration (#4404). It has no local state to
/// advertise for such a contract, yet the old code issued a `GetSummaryQuery`
/// for it on every heartbeat from every connected peer. Each query is a
/// round-trip on the single-threaded `contract_handling` loop that returns
/// "state not found" (uncached) every time. Measured on `technic`: ~40
/// summarize/sec across ~69 such phantom contracts while taking <10 real
/// UPDATEs/hour — a ~4,000× amplification that saturated the loop (starving
/// real GET/PUT/UPDATE on relays; feeding the #4145 notification-channel
/// saturation on gateways). #4473.
///
/// Gating on `(is_hosting_contract || contract_in_use)` alone proved
/// insufficient (#4610): the inbound relay-SUBSCRIBE / placement-migration path
/// marks a contract `is_hosting`/`contract_in_use` (a downstream subscriber
/// renewal) WITHOUT its state ever being fetched and stored, so ~655 "phantom"
/// (interested-but-stateless) contracts still passed the gate and drove the
/// `summarize_contract_state` storm back to ~70-80/sec (#4440 root cause). The
/// gate therefore ALSO requires `contract_state_present` — actual state in the
/// on-disk store — which is the only signal that distinguishes a phantom from a
/// contract we can really summarize:
/// - Phantom contracts (the storm) have no stored state, so they are skipped.
/// - A subscribed contract we hold keeps its state ON DISK: under normal
///   `AtCapacity` pressure `evict_over_budget` orders it LAST (shed only as a
///   last resort when nothing with fewer subscribers is eligible), so while any
///   fewer-subscriber contract exists it is retained and `contract_state_present`
///   stays true, and it KEEPS summarizing — which is why the gate reads the
///   state store, not the in-memory hosting cache. In the all-subscribed
///   last-resort extreme where it IS shed, `teardown_evicted_in_use_contract`
///   clears its subscription state so `contract_in_use` is false and
///   `reclaim_evicted_contract` deletes the disk state — at which point it is no
///   longer held and correctly stops summarizing.
/// - The moment a contract's state is fetched/stored it summarizes again — no
///   loss of proactive repair for any contract we genuinely hold.
async fn summary_if_hosted_or_in_use(
    op_manager: &Arc<OpManager>,
    key: &freenet_stdlib::prelude::ContractKey,
) -> Option<freenet_stdlib::prelude::StateSummary<'static>> {
    if op_manager.ring.should_summarize_or_broadcast(key) {
        // Periodic interest-sync summarize: best-effort background work, so it
        // yields the contract loop to client/relay traffic (#4534 / #4473).
        get_contract_summary(op_manager, key, crate::contract::Priority::Background).await
    } else {
        None
    }
}

/// Get the PeerKey for a socket address.
fn get_peer_key_from_addr(
    op_manager: &Arc<OpManager>,
    addr: std::net::SocketAddr,
) -> Option<crate::ring::interest::PeerKey> {
    op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(addr)
        .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key.clone()))
}

/// Attempts to subscribe to a contract. Thin wrapper around
/// [`subscribe_with_id`] that allocates a fresh transaction.
#[allow(dead_code)]
pub async fn subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_id: Option<ClientId>,
) -> Result<Transaction, OpError> {
    subscribe_with_id(op_manager, instance_id, client_id, None).await
}

/// Subscribe to a contract with a specific transaction ID (for
/// deduplication).
///
/// Entry point for **client-initiated** SUBSCRIBE only. Other callers
/// (executor auto-subscribe, ring renewals, PUT/GET sub-op fallback)
/// invoke their own drivers directly — `run_executor_subscribe`,
/// `run_renewal_subscribe`, `run_client_subscribe`. `is_renewal` is
/// accepted only by `run_renewal_subscribe`, so renewal misrouting is
/// a compile error.
///
/// # Parameters
///
/// - `client_id`: If set, registers a subscription-result waiter via
///   `ch_outbound.waiting_for_subscription_result`. Both WS call sites
///   in `client_events.rs` leave this `None` because they pre-register
///   a transaction-result waiter via `waiting_for_transaction_result`.
/// - `transaction_id`: Client-visible tx id. If `None`, a fresh one is
///   allocated — currently only the dead-code wrapper `subscribe()`
///   does this.
pub async fn subscribe_with_id(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_id: Option<ClientId>,
    transaction_id: Option<Transaction>,
) -> Result<Transaction, OpError> {
    let client_tx = match transaction_id {
        Some(id) => id,
        None => Transaction::new::<subscribe::SubscribeMsg>(),
    };

    if let Some(client_id) = client_id {
        use crate::client_events::RequestId;
        // Generate a default RequestId for internal subscription operations.
        // Legacy behaviour preserved: callers that pass a `client_id` expect
        // the subscription-result waiter to be registered here. The WS path
        // does not hit this branch (it pre-registers its own waiter).
        let request_id = RequestId::new();
        if let Err(e) = op_manager
            .ch_outbound
            .waiting_for_subscription_result(client_tx, instance_id, client_id, request_id)
            .await
        {
            tracing::warn!(tx = %client_tx, error = %e, "failed to register subscription result waiter");
        }
    }

    // Spawn the driver and return the client-visible tx immediately.
    // The driver owns retries, peer selection, local completion, and
    // result delivery via `result_router_tx`.
    subscribe::start_client_subscribe(op_manager, instance_id, client_tx).await
}

/// The identifier of a peer in the network: a known public key and socket address.
///
/// This is a type alias for [`ring::KnownPeerKeyLocation`], which bundles a peer's
/// cryptographic identity (public key) with its guaranteed-known network address.
///
/// Use `KnownPeerKeyLocation` directly when you need the full type name for clarity.
/// Use `PeerKeyLocation` when the address may be unknown (e.g., during NAT traversal).
pub type PeerId = crate::ring::KnownPeerKeyLocation;

pub async fn run_local_node(
    mut executor: Executor,
    socket: WebsocketApiConfig,
) -> anyhow::Result<()> {
    if !crate::server::is_private_ip(&socket.address) {
        anyhow::bail!(
            "invalid ip: {}, only loopback and private network addresses are allowed",
            socket.address
        )
    }

    // Seed the dashboard so it renders immediately (not "Starting up…"
    // forever). Local mode never joins the ring, so there are no peers,
    // no contracts, and no transport stats.
    crate::node::network_status::init(
        socket.port,
        std::collections::HashSet::new(),
        crate::config::PCK_VERSION.to_string(),
    );

    let (mut gw, mut ws_proxy) = crate::server::serve_client_api_in(socket).await?;

    // TODO: use combinator instead
    // let mut all_clients =
    //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
    enum Receiver {
        Ws,
        Gw,
    }
    let mut receiver;
    loop {
        let req = crate::deterministic_select! {
            req = ws_proxy.recv() => {
                receiver = Receiver::Ws;
                req?
            },
            req = gw.recv() => {
                receiver = Receiver::Gw;
                req?
            },
        };
        let OpenRequest {
            client_id: id,
            request,
            notification_channel,
            token,
            origin_contract,
            user_context,
            ..
        } = req;
        tracing::debug!(client_id = %id, ?token, "Received OpenRequest -> {request}");

        let res = match *request {
            ClientRequest::ContractOp(op) => {
                executor
                    .contract_requests(op, id, notification_channel)
                    .await
            }
            ClientRequest::DelegateOp(op) => {
                // Use the origin_contract already resolved by the WebSocket/HTTP client API
                // instead of re-looking up from gw.origin_contracts (which could fail
                // if the token expired between WebSocket connect and this request)
                let op_name = match op {
                    DelegateRequest::RegisterDelegate { .. } => "RegisterDelegate",
                    DelegateRequest::ApplicationMessages { .. } => "ApplicationMessages",
                    DelegateRequest::UnregisterDelegate(_) => "UnregisterDelegate",
                    _ => "Unknown",
                };
                tracing::debug!(
                    op_name = ?op_name,
                    ?origin_contract,
                    "Handling ClientRequest::DelegateOp"
                );
                // `user_context` is `Some` only in hosted mode with a user token;
                // `None` keeps secrets on the single-user `SecretScope::Local`.
                executor.delegate_request(op, origin_contract.as_ref(), None, user_context.as_ref())
            }
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                continue;
            }
            ClientRequest::Authenticate { .. }
            | ClientRequest::NodeQueries(_)
            | ClientRequest::Close
            | _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        };

        match res {
            Ok(res) => {
                match receiver {
                    Receiver::Ws => ws_proxy.send(id, Ok(res)).await?,
                    Receiver::Gw => gw.send(id, Ok(res)).await?,
                };
            }
            Err(err) if err.is_request() => {
                let err = ErrorKind::RequestError(err.unwrap_request());
                match receiver {
                    Receiver::Ws => {
                        ws_proxy.send(id, Err(err.into())).await?;
                    }
                    Receiver::Gw => {
                        gw.send(id, Err(err.into())).await?;
                    }
                };
            }
            Err(err) => {
                tracing::error!("{err}");
                let err = Err(ErrorKind::Unhandled {
                    cause: format!("{err}").into(),
                }
                .into());
                match receiver {
                    Receiver::Ws => {
                        ws_proxy.send(id, err).await?;
                    }
                    Receiver::Gw => {
                        gw.send(id, err).await?;
                    }
                };
            }
        }
    }
}

pub async fn run_network_node(mut node: Node) -> anyhow::Result<()> {
    tracing::info!("Starting node");

    let is_gateway = node.inner.is_gateway;
    let location = if let Some(loc) = node.inner.location {
        Some(loc)
    } else {
        is_gateway
            .then(|| {
                node.inner
                    .peer_id
                    .as_ref()
                    .map(|id| Location::from_address(&id.socket_addr()))
            })
            .flatten()
    };

    if let Some(location) = location {
        tracing::info!("Setting initial location: {location}");
        node.update_location(location);
    }

    match node.run().await {
        Ok(_) => {
            if is_gateway {
                tracing::info!("Gateway finished");
            } else {
                tracing::info!("Node finished");
            }

            Ok(())
        }
        Err(e) => {
            tracing::error!("{e}");
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;
    use rstest::rstest;

    /// Source-level pins for the three log sites in this file that were
    /// demoted / format-fixed in PR #4252 for issue #4251.
    ///
    /// Anchors on the closest preceding `tracing::` macro (via `rfind`)
    /// and parses the macro name out of the source, rather than scanning
    /// a fixed byte window. Adopted from the #4272 pin tests (see
    /// `operations/update.rs::no_targets_propagation_logs_at_debug_pin_test`):
    /// the old byte-window scan false-broke when added structured fields
    /// shifted bytes, and could false-pass off a neighboring macro. A
    /// line-prefix guard rejects a `tracing::` match that lands inside a
    /// string literal or comment instead of a real macro invocation.
    ///
    /// `expected_macro` pins the macro family (e.g. "debug"); the equality
    /// check rejects every other level implicitly. The optional
    /// `must_contain` / `must_not_contain` substrings are matched within
    /// the macro invocation body (between the macro and the anchor
    /// message) and guard format-specifier regressions such as Display
    /// (`%field`) vs Debug (`?field`) expansion of a structured field.
    fn assert_log_site_pin(
        needle: &str,
        expected_macro: &str,
        must_contain: &[&str],
        must_not_contain: &[&str],
    ) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/node.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        let idx = source
            .find(needle)
            .unwrap_or_else(|| panic!("log message `{needle}` must still exist in source"));
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .unwrap_or_else(|| panic!("a tracing macro must precede the `{needle}` log site"));
        let line_start = preceding[..macro_idx].rfind('\n').map_or(0, |n| n + 1);
        let line_prefix = &preceding[line_start..macro_idx];
        assert!(
            line_prefix.chars().all(char::is_whitespace),
            "rfind matched `tracing::` inside a string literal or comment, \
             not a macro invocation. Prefix on its line: {line_prefix:?}"
        );
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        // Char-boundary-safe last-200-bytes window: a raw byte slice could
        // start mid-UTF-8-char and panic while building the failure message.
        let tail_start = preceding
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| preceding.len() - i <= 200)
            .unwrap_or(0);
        let context = &preceding[tail_start..];
        assert_eq!(
            macro_name, expected_macro,
            "log site for `{needle}` must be at `tracing::{expected_macro}!` \
             (closest preceding macro is `tracing::{macro_name}!`). \
             A level change here restores an issue #4251 regression.\n\
             Preceding source (last 200 bytes):\n{context}"
        );
        // Scan only the macro invocation body (macro start -> anchor
        // message) so the format-specifier checks can't match a
        // neighboring macro or an explanatory comment above the call.
        let macro_body = &source[macro_idx..idx];
        for substr in must_contain {
            assert!(
                macro_body.contains(substr),
                "log site for `{needle}` must contain `{substr}` in its macro invocation:\n{macro_body}"
            );
        }
        for forbidden in must_not_contain {
            assert!(
                !macro_body.contains(forbidden),
                "log site for `{needle}` must NOT contain `{forbidden}` \
                 (would restore an issue #4251 regression):\n{macro_body}"
            );
        }
    }

    #[test]
    fn summary_mismatch_in_interest_sync_logs_at_debug_pin_test() {
        // Demoted from INFO to DEBUG to stop dominating peer logs on
        // hot contracts. Per #4251 review (testing reviewer #1).
        assert_log_site_pin(
            "Summary mismatch in interest sync \u{2014} syncing state to stale peer",
            "debug",
            &[],
            &[],
        );
    }

    #[test]
    fn unexpected_resync_response_uses_display_not_debug_pin_test() {
        // Switched from `response = ?other` (Debug-expanded UpdateResponse
        // → anyhow chain → ~15-line backtrace per call) to `response =
        // %other` (single-line Display via ContractHandlerEvent's
        // hand-written impl). Per #4251 review (code-first + Codex).
        assert_log_site_pin(
            "Unexpected response to resync update",
            "debug",
            &["response = %other"],
            &["response = ?other"],
        );
    }

    #[test]
    fn failed_to_get_contract_summary_logs_at_debug_pin_test() {
        // Demoted from WARN to DEBUG: this site fires repeatedly when
        // the executor queue is saturated for a hot contract (#4251).
        // The actionable signal is the queue saturation itself, not
        // the per-summary failure. Caught by rule-review on PR #4252.
        assert_log_site_pin("Failed to get contract summary", "debug", &[], &[]);
    }

    /// Regression pin for the #4473 / #4145 interest-sync summarize storm.
    ///
    /// The three PERIODIC interest-sync arms (`Interests`, `Summaries`,
    /// `ChangeInterests`) each handle a message from every connected peer on its
    /// 5-min heartbeat and summarize every shared-interest contract. Before #4473
    /// they called `get_contract_summary` directly even for contracts we neither
    /// host nor serve, flooding the serial `contract_handling` loop with pointless
    /// "state not found" round-trips (~40/sec measured on a relay; decoupled from
    /// the <10/hour real update rate). They MUST route through
    /// `summary_if_hosted_or_in_use`, which skips the round-trip for contracts we
    /// neither host nor actively serve.
    ///
    /// Fails (finding a bare `get_contract_summary`) on the pre-fix code. The one
    /// legitimate bare call left in `handle_interest_sync_message` is the
    /// `ResyncRequest` arm, which is already state-gated (returns early when
    /// `get_contract_state` is `None`) and is not heartbeat-driven, so it is
    /// excluded by slicing up to that arm.
    #[test]
    fn interest_sync_periodic_arms_summarize_only_hosted_or_in_use_pin() {
        let src = include_str!("node.rs");

        // The helper must gate the expensive call on the SINGLE composed
        // predicate `Ring::should_summarize_or_broadcast`, which is
        // `(is_hosting_contract || contract_in_use) && contract_state_present`.
        // The composition (incl. the load-bearing `&&` vs `||` — an `||` would
        // let a phantom pass and re-open the #4610 storm) is behaviourally
        // verified by `summarize_gate_skips_stateless_phantom_keeps_stateful_4610`
        // in ring/hosting.rs. Here we only pin that the helper DELEGATES to it
        // rather than re-inlining a partial gate.
        let helper_start = src
            .find("async fn summary_if_hosted_or_in_use(")
            .expect("summary_if_hosted_or_in_use helper not found");
        // Bound the slice to the helper body (its closing `}` at column 0) so the
        // gate-condition assertions below can't false-pass on a neighboring fn.
        let helper_end = helper_start
            + src[helper_start..]
                .find("\n}\n")
                .expect("summary_if_hosted_or_in_use body end not found");
        let helper_src = &src[helper_start..helper_end];
        assert!(
            helper_src.contains("should_summarize_or_broadcast"),
            "summary_if_hosted_or_in_use must gate on the composed \
             should_summarize_or_broadcast predicate (single source of truth, \
             #4610), not re-inline a partial (is_hosting || in_use) gate that \
             would re-admit phantom stateless contracts"
        );

        // Slice the periodic arms = handler start .. the ResyncRequest arm.
        let handler_start = src
            .find("async fn handle_interest_sync_message(")
            .expect("handle_interest_sync_message not found");
        let resync_off = src[handler_start..]
            .find("InterestMessage::ResyncRequest")
            .expect("ResyncRequest arm not found");
        let periodic_arms = &src[handler_start..handler_start + resync_off];

        assert!(
            !periodic_arms.contains("get_contract_summary("),
            "the periodic interest-sync arms (Interests/Summaries/ChangeInterests) \
             must call summary_if_hosted_or_in_use, not get_contract_summary \
             directly (#4473) — a bare call here reintroduces the summarize storm"
        );
        let gated_calls = periodic_arms
            .matches("summary_if_hosted_or_in_use(")
            .count();
        assert!(
            gated_calls >= 3,
            "expected the 3 periodic interest-sync arms to call \
             summary_if_hosted_or_in_use, found {gated_calls}"
        );
    }

    /// Regression pin for the D2 `is_upstream` clobber in the `ChangeInterests`
    /// interest-sync arm.
    ///
    /// A peer that is already our UPSTREAM host (`is_upstream = true`, set when
    /// we subscribed through it) can re-advertise interest via a
    /// `ChangeInterests { added }` gossip. That path is EVENT-DRIVEN, not
    /// periodic: `ChangeInterests` is emitted only on a 0->1 interest transition
    /// (`broadcast_change_interests`), whereas the ~5-min interest-sync
    /// *heartbeat* sends `InterestMessage::Interests` (the already-guarded
    /// full-replace arm), NOT `ChangeInterests`. A bare
    /// `register_peer_interest(.., is_upstream = false)` overwrites the whole
    /// `PeerInterest`, flipping `is_upstream` true -> false and wiping the cached
    /// delta-sync summary. The is_upstream clobber defeats event-driven chain
    /// collapse: `send_unsubscribe_upstream` finds the upstream via `is_upstream`,
    /// so once clobbered the Unsubscribe is never sent upstream and the chain
    /// only lapses on lease expiry (~6-min stale window). Hitting a real upstream
    /// edge needs the upstream's own interest to lapse-and-revive (uncommon on
    /// current main, where leases renew unconditionally; load-bearing under
    /// piece-D interest-gated renewal, #4642). The arm therefore MUST guard
    /// re-registration of an existing entry with
    /// `get_peer_interest().is_some() -> refresh_peer_interest()`, exactly like
    /// the `Interests` full-replace arm.
    ///
    /// This is the regression signal for the handler WIRING: it FAILS on the
    /// pre-fix code (whose arm had a single unguarded bare
    /// `register_peer_interest(.., false)` — NEITHER guard call) and passes only
    /// on the guarded shape. Driving the async handler end-to-end needs a full
    /// OpManager + connection fixture (none exists in this test module), so —
    /// following the codebase convention for interest-sync wiring (see
    /// `op_state_manager.rs` #4359 pins) — the wiring is pinned by structural
    /// source-scrape here. The PRIMITIVE behaviour the guard relies on (refresh
    /// preserves, bare register clobbers) is separately pinned in
    /// `ring/interest.rs` by
    /// `upstream_interest_survives_refresh_but_bare_register_clobbers_it`.
    #[test]
    fn change_interests_arm_guards_register_with_refresh_pin() {
        // Strip `//` line comments so the structural assertions below match only
        // real code, not a comment that mentions a guard token. Without this a
        // future comment naming `refresh_peer_interest(` could let a reverted,
        // bare `register_peer_interest(.., false)` arm false-PASS this pin (L1).
        // Crude but sufficient: the arm has no `//` inside a string literal.
        fn strip_line_comments(src: &str) -> String {
            src.lines()
                .map(|line| line.split_once("//").map(|(code, _)| code).unwrap_or(line))
                .collect::<Vec<_>>()
                .join("\n")
        }

        let src = include_str!("node.rs");

        let handler_start = src
            .find("async fn handle_interest_sync_message(")
            .expect("handle_interest_sync_message not found");
        let handler_src = &src[handler_start..];

        // Bound the slice to the ChangeInterests arm: from its match pattern up
        // to the next arm (ResyncRequest), so the assertions below can't
        // false-pass on a neighbouring arm's guard.
        let arm_start = handler_src
            .find("InterestMessage::ChangeInterests { added, removed } =>")
            .expect("ChangeInterests arm not found");
        let arm_len = handler_src[arm_start..]
            .find("InterestMessage::ResyncRequest")
            .expect("ResyncRequest arm (ChangeInterests terminator) not found");
        let arm = strip_line_comments(&handler_src[arm_start..arm_start + arm_len]);

        // Structural pin (NOT mere token presence): the arm must GUARD the
        // re-registration — look up the existing entry, and on a hit REFRESH it
        // (preserving is_upstream + summary) rather than clobber it with a bare
        // register. We assert the SHAPE and ORDER
        //   get_peer_interest( .. ).is_some() -> refresh_peer_interest( .. )
        //     -> register_peer_interest( .. )
        // so the register can only be the else-branch fallback for a genuinely
        // new peer, never the primary path. The pre-fix arm had a single
        // unguarded `register_peer_interest(.., false)` and NEITHER the
        // presence check nor the refresh call, so the first two `expect`s below
        // trip on it.
        let get_at = arm.find("get_peer_interest(").expect(
            "ChangeInterests arm MUST look up the existing entry with \
             get_peer_interest() before (re-)registering, so an existing upstream \
             peer is not clobbered to is_upstream=false (D2)",
        );
        let is_some_at = arm[get_at..]
            .find(".is_some()")
            .map(|off| get_at + off)
            .expect(
                "ChangeInterests arm MUST use get_peer_interest(..).is_some() as the \
                 presence check that gates the refresh (D2)",
            );
        let refresh_at = arm[is_some_at..]
            .find("refresh_peer_interest(")
            .map(|off| is_some_at + off)
            .expect(
                "ChangeInterests arm MUST refresh (not re-register) an existing entry \
                 to preserve is_upstream + the cached summary (D2)",
            );
        let register_at = arm[refresh_at..]
            .find("register_peer_interest(")
            .map(|off| refresh_at + off)
            .expect(
                "ChangeInterests arm MUST still register a genuinely new peer in the \
                 else branch (and flush the #4359 deferred broadcast)",
            );

        // The sequential slice searches already enforce
        // get_at < is_some_at < refresh_at < register_at; assert it explicitly so
        // a reshape that reorders (e.g. an unguarded register before the check)
        // gets a clear message.
        assert!(
            get_at < is_some_at && is_some_at < refresh_at && refresh_at < register_at,
            "ChangeInterests guard must be shaped get_peer_interest().is_some() -> \
             refresh_peer_interest(), with register_peer_interest() only as the \
             else-branch fallback (D2)"
        );

        // Exactly ONE register in the arm — the guarded fallback. A second would
        // mean an unguarded bare register slipped back in alongside the guard.
        assert_eq!(
            arm.matches("register_peer_interest(").count(),
            1,
            "ChangeInterests arm must contain exactly one (guarded, else-branch) \
             register_peer_interest call; a second would be an unguarded clobber (D2)"
        );
    }

    /// Regression pin for the #4473 residual `fetch_contract` churn (the
    /// fetch-path sibling of the summarize gate pinned above).
    ///
    /// The NeighborHosting overlap-sync loop fetched `get_contract_state_by_id`
    /// for EVERY overlapping contract on EVERY inbound announce, only to discard
    /// the result at the `is_receiving_updates || has_downstream_subscribers`
    /// gate for contracts we don't actively serve — a `fetch_contract` span
    /// burst on the serial `contract_handling` loop driven by phantom interest.
    /// The activity gate (plus a `pending_broadcasts` clause that preserves the
    /// #4359 fresh-PUT flush) MUST precede the `get_contract_state_by_id` fetch
    /// so the span is never opened for a skipped contract. If a refactor moves
    /// the gate after the fetch, the churn regresses silently, so pin the
    /// ordering at the source level.
    #[test]
    fn neighbor_hosting_overlap_sync_gates_before_state_fetch_pin() {
        let src = include_str!("node.rs");

        // Bound the slice to the NeighborHosting overlap-sync loop so the
        // ordering check can't false-pass on a neighbouring handler. The loop
        // is the only site that iterates `result.overlapping_contracts`.
        let loop_start = src
            .find("for instance_id in result.overlapping_contracts {")
            .expect("NeighborHosting overlap-sync loop not found");
        let loop_src = &src[loop_start..];

        let gate_pos = loop_src
            .find("op_manager.pending_broadcasts.contains(&instance_id)")
            .expect(
                "overlap-sync loop MUST gate on the activity predicate + \
                 pending_broadcasts.contains before fetching state (#4473)",
            );
        let recv_pos = loop_src
            .find("is_receiving_updates(&probe_key)")
            .expect("overlap-sync gate MUST check is_receiving_updates on the probe key");
        let downstream_pos = loop_src
            .find("has_downstream_subscribers(&probe_key)")
            .expect("overlap-sync gate MUST check has_downstream_subscribers on the probe key");
        let fetch_pos = loop_src
            .find("get_contract_state_by_id(&op_manager, &instance_id)")
            .expect("overlap-sync loop must still fetch state on the served path");

        assert!(
            recv_pos < fetch_pos && downstream_pos < fetch_pos && gate_pos < fetch_pos,
            "the activity gate (is_receiving_updates || has_downstream_subscribers \
             || pending_broadcasts.contains) MUST precede get_contract_state_by_id, \
             or the #4473 fetch_contract churn regresses for phantom contracts"
        );
    }

    // Hostname resolution tests
    #[tokio::test]
    async fn test_hostname_resolution_localhost() {
        // A port-less host must resolve to the fixed gateway port (31337), NOT a
        // random local port. Regression for issue #1388: the old code fell back
        // to `default_network_api_port()` (a random free port), which made the
        // gateway unreachable.
        let addr = Address::Hostname("localhost".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert!(
            socket_addr.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
                || socket_addr.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST)
        );
        assert_eq!(
            socket_addr.port(),
            crate::config::DEFAULT_GATEWAY_PORT,
            "port-less gateway host must default to 31337, not a random port"
        );
    }

    #[tokio::test]
    async fn test_hostname_resolution_with_port() {
        let addr = Address::Hostname("google.com:8080".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert_eq!(socket_addr.port(), 8080);
    }

    #[tokio::test]
    async fn test_host_variant_defaults_to_gateway_port() {
        // New `{ host, port }` form with the default port resolves to 31337.
        let addr = Address::Host {
            host: "localhost".to_string(),
            port: crate::config::DEFAULT_GATEWAY_PORT,
        };
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert!(
            socket_addr.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
                || socket_addr.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST)
        );
        assert_eq!(socket_addr.port(), crate::config::DEFAULT_GATEWAY_PORT);
    }

    #[tokio::test]
    async fn test_host_variant_explicit_port() {
        // New `{ host, port }` form honors an explicit non-default port.
        let addr = Address::Host {
            host: "localhost".to_string(),
            port: 12345,
        };
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert_eq!(socket_addr.port(), 12345);
    }

    #[tokio::test]
    async fn test_hostname_resolution_with_trailing_dot() {
        // DNS names with trailing dot should be handled
        let addr = Address::Hostname("localhost.".to_string());
        let result = NodeConfig::parse_socket_addr(&addr).await;
        // This should either succeed or fail gracefully
        if let Ok(socket_addr) = result {
            assert!(
                socket_addr.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
                    || socket_addr.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST)
            );
        }
    }

    #[tokio::test]
    async fn test_hostname_resolution_direct_socket_addr() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let addr = Address::HostAddress(socket);
        let resolved = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert_eq!(resolved, socket);
    }

    #[tokio::test]
    async fn test_hostname_resolution_invalid_port() {
        let addr = Address::Hostname("localhost:not_a_port".to_string());
        let result = NodeConfig::parse_socket_addr(&addr).await;
        assert!(result.is_err());
    }

    // TODO(#4869): sqlite bogus-key flood test — blocked by the crate
    // having NO compilable SQLite-only feature combination. The round-5 fix
    // added an async SQLite EXISTS probe (`contract_state_present_async` →
    // `get_state_size`) so bogus ResyncRequests are rejected before consuming a
    // limiter slot on a SQLite build too, but that path cannot be exercised in a
    // test: `cargo build/test -p freenet --no-default-features --features
    // sqlite,...` fails to compile (~52 lib errors / ~99 lib-test errors). The
    // SQLite `Pool` backend (crates/core/src/contract/storages/sqlite.rs) has
    // drifted behind `ReDb` and is missing ~15 methods the rest of the crate
    // calls (get_state_sync / store_state_sync / update_state_sync,
    // contract_blob_lock, {load_all,store,remove}_{contract,delegate,secrets,
    // user_secrets}_index). A SQLite mirror of the redb test below can only be
    // added once the SQLite backend is repaired (out of scope here: this change
    // was restricted to node.rs and may not touch sqlite.rs). The redb test
    // below covers the redb backend's synchronous fast-path.

    /// #4864 round-4 P1: a peer spraying `ResyncRequest`s for contracts we do
    /// NOT hold must not be able to exhaust the strictly-capped resync-response
    /// limiter maps. Both limiters allocate a bucket slot (vacant-at-capacity)
    /// the moment `check_and_record` runs, so if the existence check did not
    /// precede them, N distinct bogus keys would occupy N slots and then start
    /// DENYING new legit `(peer, contract)` keys (fail-closed → no response for
    /// contracts we actually host).
    ///
    /// The `ResyncRequest` arm now does a cheap synchronous redb state-presence
    /// point lookup BEFORE either limiter and bails on absence, so bogus keys
    /// never touch a limiter slot. This test fires 50 distinct bogus keys and
    /// asserts (a) every response is `None`, and (b) both limiter maps stay
    /// EMPTY — proving the pre-limiter existence gate holds.
    ///
    /// Requires a real (empty) redb hosting store: with NO storage handle,
    /// `contract_state_present` conservatively returns `true` for every key
    /// (see `hosting.rs::contract_state_present`), which would let the requests
    /// through and populate the limiters, making the test vacuous. Gated on the
    /// `redb` feature (a default feature; runs under `--features testing`) for
    /// the same reason the #4612 store test is: only the redb backend has the
    /// cheap synchronous existence check.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn resync_request_for_bogus_keys_does_not_consume_limiter_slots() {
        // Build a real OpManager (mirrors `build_broadcast_test_node` in
        // operations/update/op_ctx_task.rs; no contract-handler spawn needed
        // because the existence check returns before any handler interaction).
        let config_args = crate::config::ConfigArgs {
            id: Some("resync-bogus-4864".to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let node_config = NodeConfig::new(config_args.build().await.expect("build Config"))
            .await
            .expect("build NodeConfig");
        let (_notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, _ch_channel, _wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = std::sync::Arc::new(
            crate::node::OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        op_manager
            .ring
            .connection_manager
            .set_own_addr("127.0.0.1:14000".parse().unwrap());

        // Attach an EMPTY redb hosting store so `contract_state_present`
        // returns false for unknown keys (a fresh store creates the STATE
        // table but holds no state). Keep `dir` alive for the whole test so
        // the temp directory is not removed while the store is open.
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = crate::contract::storages::Storage::new(dir.path())
            .await
            .expect("storage");
        op_manager.ring.set_hosting_storage(storage);

        let source: SocketAddr = "127.0.0.1:15000".parse().unwrap();
        for i in 0u8..50 {
            // Distinct bogus key per iteration; none of these are stored.
            let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
                freenet_stdlib::prelude::ContractInstanceId::new([i; 32]),
                freenet_stdlib::prelude::CodeHash::new([i; 32]),
            );
            let response = handle_interest_sync_message(
                &op_manager,
                source,
                crate::message::InterestMessage::ResyncRequest { key },
            )
            .await;
            assert!(
                response.is_none(),
                "ResyncRequest for a contract with no stored state (bogus key {i}) \
                 must produce no response"
            );
        }

        assert_eq!(
            op_manager.ring.resync_response_limiter.len(),
            0,
            "#4864: bogus keys must NOT occupy per-(peer, contract) limiter slots \
             (the pre-limiter existence check must reject them first)"
        );
        assert_eq!(
            op_manager.ring.resync_response_global_limiter.len(),
            0,
            "#4864: bogus keys must NOT occupy global per-contract limiter slots \
             (the pre-limiter existence check must reject them first)"
        );
    }

    /// #4864 round-5 item 5 — POSITIVE CONTROL twin of
    /// `resync_request_for_bogus_keys_does_not_consume_limiter_slots`.
    ///
    /// The bogus-keys test proves the existence gate REJECTS absent contracts
    /// (both limiter maps stay EMPTY). On its own that stays green even if
    /// `contract_state_present_async` regressed to ALWAYS-false — a gate that
    /// rejects *everything* would also leave the maps empty. This twin removes
    /// that blind spot: a `ResyncRequest` for a contract we DO hold must PASS
    /// the gate, so a limiter slot IS consumed (`len() >= 1`), and — because
    /// the stand-in handler answers the post-gate state/summary fetches — a
    /// full `ResyncResponse` comes back.
    ///
    /// Together the pair proves the gate DISCRIMINATES (bogus → `len() == 0`,
    /// held → `len() >= 1`), not merely that it always-rejects or always-accepts.
    ///
    /// redb-gated for the same reason as the twin: only the redb backend has
    /// the cheap synchronous existence check, and an empty/unset store would
    /// make the precondition unrepresentable.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn resync_request_for_held_contract_passes_gate_and_responds() {
        use crate::contract::{ContractHandlerEvent, StoreResponse};
        use freenet_stdlib::prelude::{StateSummary, WrappedState};

        // Build a real OpManager (identical harness to the bogus-keys twin),
        // but this time WITH a stand-in contract handler: the held path reaches
        // `get_contract_state` / `get_contract_summary` after the gate, which
        // round-trip through the contract-handling channel, so an unanswered
        // channel would hang the test.
        let config_args = crate::config::ConfigArgs {
            id: Some("resync-held-4864".to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let node_config = NodeConfig::new(config_args.build().await.expect("build Config"))
            .await
            .expect("build NodeConfig");
        let (_notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, mut ch_channel, _wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = std::sync::Arc::new(
            crate::node::OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        op_manager
            .ring
            .connection_manager
            .set_own_addr("127.0.0.1:14100".parse().unwrap());

        // The one contract we DO hold.
        let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
            freenet_stdlib::prelude::ContractInstanceId::new([200u8; 32]),
            freenet_stdlib::prelude::CodeHash::new([201u8; 32]),
        );

        // Store its state into the hosting store BEFORE handing the store to
        // the ring, so `contract_state_present_async(&key)` (the redb sync
        // point lookup on the STATE table — the same table `store_state_sync`
        // writes and `get_state_size` reads) returns true.
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = crate::contract::storages::Storage::new(dir.path())
            .await
            .expect("storage");
        storage
            .store_state_sync(&key, WrappedState::new(vec![9u8, 9, 9]))
            .expect("store held state");
        op_manager.ring.set_hosting_storage(storage);

        // Precondition: the sync redb probe (what the async gate delegates to
        // under redb) sees the state we just stored.
        assert!(
            op_manager.ring.contract_state_present(&key),
            "precondition: the held contract's state must be present in the store"
        );

        // Stand-in contract handler: answers the post-gate GET (full state) and
        // GET-summary so the responder can build a ResyncResponse. Owns the
        // receiver for the whole test.
        let handler_key = key;
        let _handler = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                let response = match ev {
                    ContractHandlerEvent::GetQuery { .. } => ContractHandlerEvent::GetResponse {
                        key: Some(handler_key),
                        response: Ok(StoreResponse {
                            state: Some(WrappedState::new(vec![9u8, 9, 9])),
                            contract: None,
                        }),
                    },
                    ContractHandlerEvent::GetSummaryQuery { key } => {
                        ContractHandlerEvent::GetSummaryResponse {
                            key,
                            summary: Ok(StateSummary::from(vec![7u8, 7, 7])),
                        }
                    }
                    other => {
                        panic!("unexpected handler event in held-contract stand-in: {other:?}")
                    }
                };
                if ch_channel.send_to_sender(id, response).await.is_err() {
                    break;
                }
            }
        });

        let source: SocketAddr = "127.0.0.1:15100".parse().unwrap();
        let response = handle_interest_sync_message(
            &op_manager,
            source,
            crate::message::InterestMessage::ResyncRequest { key },
        )
        .await;

        // PRIMARY assertion (the exact regression this guards): the gate PASSED,
        // so a limiter slot WAS consumed — impossible if the existence check had
        // regressed to always-reject.
        assert!(
            op_manager.ring.resync_response_limiter.len() >= 1,
            "#4864: a ResyncRequest for a HELD contract must PASS the existence \
             gate and consume a per-(peer, contract) limiter slot (contrast the \
             bogus-keys twin, which asserts len() == 0)"
        );
        assert!(
            op_manager.ring.resync_response_global_limiter.len() >= 1,
            "#4864: a ResyncRequest for a HELD contract must PASS the existence \
             gate and consume a global per-contract limiter slot"
        );

        // Stronger check: with the state + summary answered, a full ResyncResponse
        // for the held key is produced (the whole happy path, not just the gate).
        match response {
            Some(crate::message::InterestMessage::ResyncResponse { key: got, .. }) => {
                assert_eq!(got, key, "ResyncResponse must be for the held contract");
            }
            other => panic!("expected Some(ResyncResponse) for a held contract, got {other:?}"),
        }
    }

    // Superseded: Old addr-only equality (same_addr_different_keys → equal) was replaced
    // with full-field equality (addr + pub_key) in #3616. Kept as historical documentation
    // of the old behavior.
    #[ignore]
    #[rstest]
    #[case::same_addr_different_keys(8080, 8080, true)]
    #[case::different_addr_same_key(8080, 8081, false)]
    fn test_peer_id_equality(#[case] port1: u16, #[case] port2: u16, #[case] expected_equal: bool) {
        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port1);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port2);
        // Old behavior: PeerId equality was addr-only, so same_addr_different_keys was true.
        // New behavior: equality uses full fields, so same_addr_different_keys is false.
        let peer1 = PeerId::new(keypair1.public().clone(), addr1);
        let peer2 = PeerId::new(keypair2.public().clone(), addr2);
        assert_eq!(peer1 == peer2, expected_equal);
    }

    // PeerId (KnownPeerKeyLocation) equality tests
    // PeerId now uses full-field equality (both addr and pub_key), matching identity semantics.
    #[test]
    fn test_peer_id_equality_same_key_same_addr() {
        let keypair = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let peer1 = PeerId::new(keypair.public().clone(), addr);
        let peer2 = PeerId::new(keypair.public().clone(), addr);
        assert_eq!(peer1, peer2);
    }

    #[test]
    fn test_peer_id_equality_different_key_same_addr() {
        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        // Different keys at same addr are different peers (key is identity)
        let peer1 = PeerId::new(keypair1.public().clone(), addr);
        let peer2 = PeerId::new(keypair2.public().clone(), addr);
        assert_ne!(peer1, peer2);
    }

    #[test]
    fn test_peer_id_equality_different_addr() {
        let keypair = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);
        let peer1 = PeerId::new(keypair.public().clone(), addr1);
        let peer2 = PeerId::new(keypair.public().clone(), addr2);
        assert_ne!(peer1, peer2);
    }

    #[rstest]
    #[case::lower_port_first(8080, 8081)]
    #[case::high_port_diff(1024, 65535)]
    fn test_peer_id_ordering(#[case] lower_port: u16, #[case] higher_port: u16) {
        let keypair = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), lower_port);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), higher_port);

        let peer1 = PeerId::new(keypair.public().clone(), addr1);
        let peer2 = PeerId::new(keypair.public().clone(), addr2);

        assert!(peer1 < peer2);
        assert!(peer2 > peer1);
    }

    #[test]
    fn test_peer_id_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let keypair = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let peer1 = PeerId::new(keypair.public().clone(), addr);
        let peer2 = PeerId::new(keypair.public().clone(), addr);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        peer1.hash(&mut hasher1);
        peer2.hash(&mut hasher2);

        // Same key + same address should produce same hash
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_peer_id_random_produces_unique() {
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        // Random peers should have different addresses (with high probability)
        assert_ne!(peer1.socket_addr(), peer2.socket_addr());
    }

    #[test]
    fn test_peer_id_serialization() {
        let peer = PeerId::random();
        let bytes = peer.to_bytes();
        assert!(!bytes.is_empty());

        // Should be deserializable
        let deserialized: PeerId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(peer.socket_addr(), deserialized.socket_addr());
    }

    #[test]
    fn test_peer_id_display() {
        let peer = PeerId::random();
        let display = format!("{}", peer);
        let debug = format!("{:?}", peer);

        // Display and Debug should produce the same output
        assert_eq!(display, debug);
        // Should not be empty
        assert!(!display.is_empty());
    }

    // InitPeerNode tests
    #[test]
    fn test_init_peer_node_construction() {
        let keypair = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let peer_key_location = PeerKeyLocation::new(keypair.public().clone(), addr);
        let location = Location::new(0.5);

        let init_peer = InitPeerNode::new(peer_key_location.clone(), location);

        assert_eq!(init_peer.peer_key_location, peer_key_location);
        assert_eq!(init_peer.location, location);
    }

    // Tests for the INBOUND `SubscribeHint` receive gate.
    //
    // The placement migration is RE-ENABLED at floor `(0, 2, 80)` (#4499 made it
    // load-safe). The receive handler shares this floor with the send side, so a
    // node acts on an inbound hint only when both it and the producing peer are at
    // or above the floor; it still ignores hints from pre-floor peers, which
    // preserves wire-compat during the staggered rollout.
    mod inbound_subscribe_hint_gate {
        use crate::node::network_bridge::p2p_protoc::{
            SUBSCRIBE_HINT_MIN_VERSION, own_crate_version, version_supports_subscribe_hint,
        };

        // Superseded: the placement migration was RE-ENABLED at `(0, 2, 80)` by
        // PR #4511 (#4145 fixed in #4499). This test pinned the v0.2.74
        // deactivation (own version below the parked floor, so all inbound hints
        // ignored) and now documents that prior behavior; its `own < floor`
        // assert no longer holds once the crate reaches the floor. Replaced by
        // `receive_gate_active_at_reenable_floor` below.
        #[ignore]
        #[test]
        fn receive_gate_ignores_hint_while_deactivated() {
            let own = own_crate_version();
            assert!(
                own < SUBSCRIBE_HINT_MIN_VERSION,
                "own version {own:?} must be below the parked floor \
                 {SUBSCRIBE_HINT_MIN_VERSION:?} for the migration to stay off"
            );
            assert!(
                !version_supports_subscribe_hint(Some(own), SUBSCRIBE_HINT_MIN_VERSION),
                "while deactivated, the receive gate must IGNORE inbound hints \
                 (own version below the floor)"
            );
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 73)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
        }

        /// At the re-enable floor the receive gate ACTS on hints from peers at or
        /// above the floor and IGNORES hints from pre-floor peers (wire-compat).
        /// Uses explicit versions rather than `own_crate_version` so the assertion
        /// is stable across the 0.2.79 -> 0.2.80 boundary (the crate is still
        /// 0.2.79 until the re-enable release bumps it to the floor version).
        #[test]
        fn receive_gate_active_at_reenable_floor() {
            // The `supported(0,2,80)` + `!supported(0,2,79)` pair pins the floor
            // to exactly `(0, 2, 80)`; an accidental change trips these asserts.
            // Peers at or above the floor are acted on.
            assert!(version_supports_subscribe_hint(
                Some((0, 2, 80)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            assert!(version_supports_subscribe_hint(
                Some((0, 3, 0)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            // Pre-floor peers are still ignored: older 0.2.x peers, and the
            // original 0.2.73 sender from the staggered rollout.
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 79)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 73)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            // Unknown remote version fails closed (the migration's send/receive
            // gate must never act on a peer whose version we could not determine).
            assert!(!version_supports_subscribe_hint(
                None,
                SUBSCRIBE_HINT_MIN_VERSION
            ));
        }

        /// Lowering the floor (as `SimNetwork::enable_placement_migration` does
        /// to `(0, 0, 0)`) re-activates the receive side: the gate now ACTS on
        /// the hint. This is the symmetry the cascade simulation test relies on.
        #[test]
        fn receive_gate_acts_on_hint_when_floor_lowered() {
            let own = own_crate_version();
            assert!(
                version_supports_subscribe_hint(Some(own), (0, 0, 0)),
                "with the floor lowered to (0,0,0) the receive side must act on hints"
            );
        }

        /// Source-pin: the `SubscribeHint` receive arm must compute the floor
        /// the SAME way as the send side (`subscribe_hint_floor_override()`
        /// `unwrap_or` the production constant) and bail via the gate predicate
        /// BEFORE invoking `start_directed_subscribe`. Without this pin a future
        /// refactor could delete the gate and the predicate unit tests above
        /// would still pass.
        #[test]
        fn receive_gate_is_wired_before_directed_subscribe() {
            const SOURCE: &str = include_str!("node.rs");
            let arm_anchor: String = ["NetMessageV1::", "SubscribeHint(hint)", " => {"].concat();
            let arm_start = SOURCE
                .find(&arm_anchor)
                .expect("SubscribeHint receive arm not found — update this guard");
            // Bound at the start of the next match arm.
            let next_anchor: String = ["NetMessageV1::", "Aborted(tx)", " => {"].concat();
            let arm_end = SOURCE[arm_start..]
                .find(&next_anchor)
                .map(|i| arm_start + i)
                .expect("end of SubscribeHint arm not found — update guard");
            let arm = &SOURCE[arm_start..arm_end];

            let gate_idx = arm
                .find("version_supports_subscribe_hint(")
                .expect("receive arm must call version_supports_subscribe_hint as a gate");
            let directed_idx = arm
                .find("start_directed_subscribe(")
                .expect("receive arm must still call start_directed_subscribe");
            assert!(
                gate_idx < directed_idx,
                "the version gate must run BEFORE start_directed_subscribe"
            );
            assert!(
                arm.contains("subscribe_hint_floor_override()"),
                "receive gate must read the same per-node floor override as the send side"
            );
        }
    }

    // Tests for `try_forward_driver_reply`.
    //
    // The bypass routes a reply directly to an awaiting
    // `OpCtx::send_and_await` caller. These tests cover the
    // helper's contract; end-to-end branch coverage lives in the
    // per-driver tests.
    mod callback_forward_tests {
        use super::super::try_forward_driver_reply;
        use crate::message::{MessageStats, NetMessage, NetMessageV1, Transaction};
        use crate::operations::connect::ConnectMsg;

        fn dummy_reply() -> NetMessage {
            NetMessage::V1(NetMessageV1::Aborted(Transaction::new::<ConnectMsg>()))
        }

        // ───────────────────────────────────────────────────────────
        // Tests for `try_forward_driver_reply`.
        //
        // The bypass routes a reply directly to an awaiting
        // `OpCtx::send_and_await` caller. These tests cover the
        // helper's contract; end-to-end branch coverage lives in the
        // per-driver tests.
        // ───────────────────────────────────────────────────────────

        #[tokio::test]
        async fn bypass_forwards_when_callback_registered() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let reply = dummy_reply();
            let expected_id = *reply.id();

            let taken = try_forward_driver_reply(Some(&tx), reply, "subscribe");
            assert!(taken, "callback present → bypass must be taken");

            let received = rx
                .try_recv()
                .expect("helper should forward the reply to the callback");
            match received {
                crate::node::WaiterReply::Reply(msg) => assert_eq!(*msg.id(), expected_id),
                other => panic!("expected WaiterReply::Reply, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn bypass_returns_false_when_no_callback() {
            // No callback registered → caller must fall through to legacy
            // `handle_op_request`. The helper must not panic and must
            // return `false`.
            let taken = try_forward_driver_reply(None, dummy_reply(), "subscribe");
            assert!(!taken, "no callback → bypass must not be taken");
        }

        #[tokio::test]
        async fn bypass_returns_true_even_when_receiver_dropped() {
            // Structural rule: once a callback is registered, the bypass
            // is taken — the legacy path must NOT run regardless of
            // whether the task-side receiver is still alive. If the task
            // was cancelled and dropped its receiver, `try_send` fails
            // with `Closed` and we log, but we still return `true` so
            // the caller returns `Ok(None)` from the pipeline.
            //
            // Running `handle_op_request` in this case would call
            // `load_or_init` on an empty DashMap and return
            // `OpNotPresent`, which is meaningless for a tx owned by a
            // (now-dead) task and pointlessly wastes a pipeline
            // iteration.
            let (tx, rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            drop(rx);

            let taken = try_forward_driver_reply(Some(&tx), dummy_reply(), "subscribe");
            assert!(
                taken,
                "callback present but receiver dropped → bypass still taken"
            );
        }

        // Note: the behavioral contract of the dropped-reply path (drop the
        // reply, never block, still return `true`) is already pinned for both
        // the closed-receiver and full-channel cases by
        // `bypass_returns_true_even_when_receiver_dropped` and
        // `bypass_does_not_block_when_channel_already_full`. The pin test
        // below guards that the drop is logged at `debug`, never at the alarm
        // levels (`error` / `warn`).

        /// Pin the log level of the dropped-reply path in
        /// `try_forward_driver_reply`. A `try_send` failure here is always a
        /// benign, intentionally-lossy drop — either a closed receiver (caller
        /// finished / cancelled / timed out, dominated by SUBSCRIBE renewals,
        /// see issue #4350) or a full reply channel (CONNECT's capacity-N
        /// fan-in overflow, or a capacity-1 duplicate). Per
        /// `.claude/rules/operations.md` ("WHEN a reply arrives with no waiter
        /// → Benign → debug log") and `channel-safety.md` (drop-when-full is
        /// intended), it MUST be logged at `debug` — never `error` (which
        /// produced ~30/hr false-alarm errors on nova after the v0.2.69
        /// rollout) and never `warn` (CONNECT legitimately reaches the
        /// full-channel case under load, so warning on it is also a false
        /// alarm).
        ///
        /// Reads `node.rs` at compile time and asserts this function's body
        /// logs at `debug` and contains no `error!` / `warn!`. A refactor that
        /// re-escalates the benign drop fails here at the unit-test level.
        /// Needles are assembled at runtime so this test cannot match its own
        /// source; the window is bounded to the function body.
        #[test]
        fn forward_driver_reply_logs_benign_drop_at_debug_only() {
            const SOURCE: &str = include_str!("node.rs");

            let fn_anchor: String = ["fn try_forward_driver_reply", "("].concat();
            let start = SOURCE.find(&fn_anchor).expect(
                "try_forward_driver_reply definition not found — \
                 it was renamed or moved; update this guard",
            );
            // Bound the window at this function's own closing brace (a `}` in
            // column 0), so only its body is inspected — not any neighbouring
            // function's doc comment or body.
            let fn_end: String = ["\n", "}", "\n"].concat();
            let after = start + fn_anchor.len();
            let window_end = SOURCE[after..]
                .find(&fn_end)
                .map(|i| after + i + fn_end.len())
                .expect("closing brace of try_forward_driver_reply not found");
            let body = &SOURCE[start..window_end];

            let debug_macro: String = ["tracing", "::debug!"].concat();
            let warn_macro: String = ["tracing", "::warn!"].concat();
            let error_macro: String = ["tracing", "::error!"].concat();

            assert!(
                body.contains(&debug_macro),
                "the benign dropped-reply path must be logged at debug"
            );
            assert!(
                !body.contains(&error_macro),
                "try_forward_driver_reply must NOT log at error: the dropped \
                 reply (closed receiver from a cancelled SUBSCRIBE renewal, or \
                 a full CONNECT fan-in channel) is benign and intentionally \
                 lossy. Re-escalating to error! reintroduces the false-alarm \
                 spam this guard prevents (see issue #4350)."
            );
            assert!(
                !body.contains(&warn_macro),
                "try_forward_driver_reply must NOT log at warn: CONNECT's \
                 capacity-N fan-in legitimately reaches the full-channel case \
                 under load, so warning on the benign drop is also a false \
                 alarm."
            );
        }

        /// Pin the bypass call site. Without this regression guard a
        /// future refactor could delete the
        /// `try_forward_driver_reply` invocation in the SUBSCRIBE
        /// branch of `handle_pure_network_message_v1` and the unit tests
        /// on the helper itself would still pass — because unit coverage
        /// on the helper only proves the helper works, not that it's
        /// wired in. Integration (simulation) failures would catch it
        /// eventually but as end-to-end hangs, which is a noisy signal.
        ///
        /// This test reads the `node.rs` source at compile time via
        /// `include_str!` and asserts that the SUBSCRIBE branch of
        /// `handle_pure_network_message_v1` invokes
        /// `try_forward_driver_reply` before running
        /// `handle_op_request`. A refactor that deletes the bypass call
        /// will fail this test at the unit-test level (review finding
        /// Testing #1).
        ///
        /// If the match arm structure changes (e.g. SUBSCRIBE branch
        /// moves or is renamed), the string patterns below need to be
        /// updated to match. That's a load-bearing but intentional
        /// coupling — the whole point is to fail loudly when the wiring
        /// changes so the change is noticed.
        #[test]
        fn bypass_is_wired_into_subscribe_branch_regression_guard() {
            // Full file text, read at compile time.
            const SOURCE: &str = include_str!("node.rs");

            // Locate the SUBSCRIBE branch of handle_pure_network_message_v1.
            // Use a runtime-built needle so this test cannot self-match
            // its own anchor string in the test source below.
            let subscribe_branch_anchor: String =
                ["NetMessageV1::", "Subscribe(ref op)", " => {"].concat();
            let branch_start = SOURCE.find(&subscribe_branch_anchor).expect(
                "SUBSCRIBE branch of handle_pure_network_message_v1 not found; \
                         the match arm has been renamed or moved — update this regression guard",
            );

            // Bound the window at the end-of-SUBSCRIBE-arm sentinel
            // ("Non-transactional message types:" header that precedes
            // the next match arm).
            let next_variant_anchor: String = ["// Non-transactional", " message types:"].concat();
            let window_end = SOURCE[branch_start..]
                .find(&next_variant_anchor)
                .expect("end of SUBSCRIBE branch not found — update guard")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // The bypass helper MUST be invoked in the SUBSCRIBE branch.
            // If this assertion fails, either:
            //   (a) the bypass was removed (regression — re-add it), or
            //   (b) the branch was restructured (update this guard).
            assert!(
                window.contains("try_forward_driver_reply("),
                "SUBSCRIBE branch no longer calls \
                 try_forward_driver_reply before relay dispatch. \
                 Either restore the bypass or update this regression \
                 guard if the branch was legitimately refactored."
            );

            // The bypass MUST be gated on Response-only. Without this
            // filter, non-terminal messages like ForwardingAck fill the
            // capacity-1 reply channel and cause UnexpectedOpState
            // (commit 5cb6f37c).
            let response_gate: String = [
                "matches!(op, ",
                "subscribe::SubscribeMsg::Response { .. }",
                ")",
            ]
            .concat();
            assert!(
                window.contains(&response_gate),
                "SUBSCRIBE branch bypass is not gated on Response-only. \
                 Non-terminal messages (ForwardingAck, Unsubscribe) must NOT \
                 be forwarded to the driver channel — they would fill \
                 the capacity-1 reply slot and block the real Response."
            );
        }

        /// Issue #4111 regression guard. The PUT branch of
        /// `handle_pure_network_message_v1` must forward `PutMsg::Error`
        /// through `try_forward_driver_reply` exactly like
        /// `PutMsg::Response` / `PutMsg::ResponseStreaming`. Without
        /// this, the originator-loopback failure path's
        /// `send_local_loopback(PutMsg::Error)` would arrive at the
        /// dispatch site, find no bypass match for `Error`, and the
        /// catch-all wildcard would drop it as
        /// "non-dispatch variant ignored" — re-introducing the bug
        /// the fix addresses (retry-storm + `"failed notifying,
        /// channel closed"` synthesised for a deterministic local
        /// failure).
        ///
        /// Same pattern as
        /// `bypass_is_wired_into_subscribe_branch_regression_guard`:
        /// a structural source-scrape so a future refactor that
        /// breaks the wiring fails at the unit-test level instead of
        /// as an end-to-end hang.
        #[test]
        fn put_branch_bypass_includes_error_variant_regression_guard() {
            const SOURCE: &str = include_str!("node.rs");

            let put_branch_anchor: String = ["NetMessageV1::", "Put(ref op)", " => {"].concat();
            let branch_start = SOURCE.find(&put_branch_anchor).expect(
                "PUT branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );

            // The PUT branch ends at the GET branch start.
            let next_anchor: String = ["NetMessageV1::", "Get(ref op)", " => {"].concat();
            let window_end = SOURCE[branch_start..]
                .find(&next_anchor)
                .expect("end of PUT branch not found — update guard")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            assert!(
                window.contains("try_forward_driver_reply("),
                "PUT branch no longer calls try_forward_driver_reply \
                 — either restore the bypass or update this guard."
            );

            // The terminal-reply gate MUST list `Error` alongside
            // `Response` and `ResponseStreaming`. We check on the
            // substring rather than the full `matches!` pattern so a
            // line-wrap or arm-reorder doesn't trip the guard
            // spuriously — the load-bearing claim is "Error appears
            // inside the matches! that gates the bypass forward".
            let gate_start = window
                .find("matches!(\n                op,")
                .or_else(|| window.find("matches!(op,"))
                .expect("terminal-gate matches! not found in PUT branch");
            let gate_end = window[gate_start..]
                .find(") && try_forward_driver_reply(")
                .expect("end of terminal-gate matches! not found")
                + gate_start;
            let gate = &window[gate_start..gate_end];

            for expected in [
                "put::PutMsg::Response { .. }",
                "put::PutMsg::ResponseStreaming { .. }",
                "put::PutMsg::Error { .. }",
            ] {
                assert!(
                    gate.contains(expected),
                    "PUT bypass terminal-gate missing `{expected}` — \
                     issue #4111: without Error in the gate, the \
                     originator-loopback failure path's \
                     send_local_loopback(PutMsg::Error) lands in the \
                     dispatch wildcard and the originator's retry-loop \
                     re-runs the same deterministic local failure."
                );
            }
        }

        #[tokio::test]
        async fn bypass_does_not_block_when_channel_already_full() {
            // Pin the non-blocking contract: `try_send` on a full
            // channel must fail without blocking the handler. Future
            // refactors must not switch to `.send().await` (see
            // `.claude/rules/channel-safety.md`).
            let (tx, _rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            // Pre-fill the capacity-1 channel.
            tx.try_send(crate::node::WaiterReply::Reply(dummy_reply()))
                .expect("capacity-1 channel should accept first message");

            let taken = try_forward_driver_reply(Some(&tx), dummy_reply(), "subscribe");
            assert!(
                taken,
                "callback present but channel full → bypass still taken"
            );
            // The test would hang on regression: blocking `send().await`
            // on a full channel whose receiver is still alive would
            // stall the `#[tokio::test]` runtime indefinitely.
        }

        // Note on per-variant coverage: Phase 1's point is that every op
        // variant of `handle_pure_network_message_v1` can terminate an
        // `OpCtx::send_and_await` round-trip. The helper tested above is
        // variant-agnostic once the `is_operation_completed` guard passes,
        // and each op's own `is_completed` impl is covered by unit tests in
        // `crates/core/src/operations/{connect,put,get,subscribe,update}.rs`.
        // The remaining "do the five branches of `handle_pure_network_message_v1`
        // actually invoke the helper with the matching reply variant?"
        // question is enforced by the compiler — each branch binds `ref op`
        // for the concrete op type and reconstructs the same variant before
        // handing it to `forward_pending_op_result_if_completed`. An
        // end-to-end integration test that spins up a node and exercises
        // `OpCtx::send_and_await` for each op kind belongs alongside
        // the per-op driver suites.

        // ───────────────────────────────────────────────────────────
        // Regression tests for the subscribe-branch message-type
        // filter added in the ForwardingAck fix (5cb6f37c).
        //
        // The bug: `try_forward_driver_reply` was called for ALL
        // subscribe message types (including ForwardingAck). A relay
        // peer's ForwardingAck would fill the capacity-1 reply
        // channel, causing the task to receive it instead of the
        // real Response and fail with UnexpectedOpState.
        //
        // These tests verify the filtering logic that
        // `handle_pure_network_message_v1` applies BEFORE calling the
        // bypass helper: only `SubscribeMsg::Response` is forwarded.
        // ───────────────────────────────────────────────────────────

        use crate::operations::VisitedPeers;
        use crate::operations::subscribe::{SubscribeMsg, SubscribeMsgResult};

        /// Helper: simulate the filtering logic from the SUBSCRIBE
        /// branch of `handle_pure_network_message_v1`. Returns
        /// `true` if the message would be forwarded to the
        /// driver channel (and the branch would return early).
        fn subscribe_branch_would_forward(
            op: &SubscribeMsg,
            callback: Option<&tokio::sync::mpsc::Sender<crate::node::WaiterReply>>,
        ) -> bool {
            matches!(op, SubscribeMsg::Response { .. })
                && try_forward_driver_reply(
                    callback,
                    NetMessage::V1(NetMessageV1::Subscribe(op.clone())),
                    "subscribe",
                )
        }

        #[tokio::test]
        async fn subscribe_response_is_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([1u8; 32]);
            let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
                instance_id,
                freenet_stdlib::prelude::CodeHash::new([2u8; 32]),
            );
            let op = SubscribeMsg::Response {
                id: sub_tx,
                instance_id,
                result: SubscribeMsgResult::Subscribed { key },
                hop_count: 0,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(taken, "Response with callback → must be forwarded");

            match rx.try_recv().expect("Response should be in channel") {
                crate::node::WaiterReply::Reply(msg) => assert_eq!(*msg.id(), sub_tx),
                other => panic!("expected WaiterReply::Reply, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn forwarding_ack_is_not_forwarded_to_task() {
            // ForwardingAck is non-terminal: relay peers send it to
            // signal "I'm working on it". Forwarding it would fill
            // the capacity-1 channel and block the real Response.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([3u8; 32]);
            let op = SubscribeMsg::ForwardingAck {
                id: sub_tx,
                instance_id,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(
                !taken,
                "ForwardingAck must NOT be forwarded to task channel"
            );
            assert!(
                rx.try_recv().is_err(),
                "channel must remain empty after ForwardingAck"
            );
        }

        #[tokio::test]
        async fn unsubscribe_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([4u8; 32]);
            let op = SubscribeMsg::Unsubscribe {
                id: sub_tx,
                instance_id,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(!taken, "Unsubscribe must NOT be forwarded to task channel");
            assert!(rx.try_recv().is_err(), "channel must remain empty");
        }

        #[tokio::test]
        async fn request_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([5u8; 32]);
            let op = SubscribeMsg::Request {
                id: sub_tx,
                instance_id,
                htl: 5,
                visited: VisitedPeers::new(&sub_tx),
                is_renewal: false,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(!taken, "Request must NOT be forwarded to task channel");
            assert!(rx.try_recv().is_err(), "channel must remain empty");
        }

        #[tokio::test]
        async fn response_without_callback_falls_through() {
            // No callback registered (legacy path) — filter must
            // return false so handle_op_request runs.
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([6u8; 32]);
            let op = SubscribeMsg::Response {
                id: sub_tx,
                instance_id,
                result: SubscribeMsgResult::NotFound,
                hop_count: 0,
            };

            let taken = subscribe_branch_would_forward(&op, None);
            assert!(
                !taken,
                "Response without callback → must fall through to legacy path"
            );
        }

        // ───────────────────────────────────────────────────────────
        // Regression guard: PUT branch of handle_pure_network_message_v1
        // must call try_forward_driver_reply before relay dispatch,
        // gated on Response|ResponseStreaming only.
        // ───────────────────────────────────────────────────────────

        #[test]
        fn bypass_is_wired_into_put_branch_regression_guard() {
            const SOURCE: &str = include_str!("node.rs");

            let put_branch_anchor = "NetMessageV1::Put(ref op) => {";
            let branch_start = SOURCE.find(put_branch_anchor).expect(
                "PUT branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this regression guard",
            );

            // End the window at the next NetMessageV1 variant to bound
            // the search to the PUT arm only.
            let next_variant = "NetMessageV1::Get(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of PUT arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            assert!(
                window.contains("try_forward_driver_reply("),
                "PUT branch no longer calls try_forward_driver_reply. \
                 Restore the bypass or update this regression guard."
            );

            assert!(
                window.contains("put::PutMsg::Response { .. }"),
                "PUT branch bypass is not gated on Response. \
                 Non-terminal messages must NOT be forwarded to the driver channel."
            );

            assert!(
                window.contains("put::PutMsg::ResponseStreaming { .. }"),
                "PUT branch bypass is not gated on ResponseStreaming. \
                 Both terminal variants must be forwarded."
            );

            // The legacy fallthrough must NOT return. Compose needles
            // at runtime so the assert source itself does not contain
            // them.
            let legacy_dispatch_needle = format!("handle{}::<put::PutOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "PUT branch must not call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_put");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "PUT branch must not gate dispatch on per-op DashMap existence"
            );
        }

        // ───────────────────────────────────────────────────────────
        // Per-variant filter tests for the PUT branch bypass.
        // Only Response and ResponseStreaming may be forwarded; all
        // other variants must fall through to relay dispatch.
        // ───────────────────────────────────────────────────────────

        use crate::operations::put::PutMsg;
        use freenet_stdlib::prelude::*;

        fn dummy_put_key(a: u8, b: u8) -> ContractKey {
            ContractKey::from_id_and_code(ContractInstanceId::new([a; 32]), CodeHash::new([b; 32]))
        }

        fn put_branch_would_forward(
            op: &PutMsg,
            callback: Option<&tokio::sync::mpsc::Sender<crate::node::WaiterReply>>,
        ) -> bool {
            matches!(
                op,
                PutMsg::Response { .. } | PutMsg::ResponseStreaming { .. }
            ) && try_forward_driver_reply(
                callback,
                NetMessage::V1(NetMessageV1::Put(op.clone())),
                "put",
            )
        }

        #[tokio::test]
        async fn put_response_is_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(10, 11);
            let op = PutMsg::Response {
                id: put_tx,
                key,
                hop_count: 0,
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(taken, "Response with callback → must be forwarded");

            match rx.try_recv().expect("Response should be in channel") {
                crate::node::WaiterReply::Reply(msg) => assert_eq!(*msg.id(), put_tx),
                other => panic!("expected WaiterReply::Reply, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn put_response_streaming_is_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(12, 13);
            let op = PutMsg::ResponseStreaming {
                id: put_tx,
                key,
                continue_forwarding: false,
                hop_count: 0,
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(taken, "ResponseStreaming with callback → must be forwarded");

            match rx
                .try_recv()
                .expect("ResponseStreaming should be in channel")
            {
                crate::node::WaiterReply::Reply(msg) => assert_eq!(*msg.id(), put_tx),
                other => panic!("expected WaiterReply::Reply, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn put_forwarding_ack_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(14, 15);
            let op = PutMsg::ForwardingAck {
                id: put_tx,
                contract_key: key,
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(
                !taken,
                "ForwardingAck must NOT be forwarded to task channel"
            );
            assert!(
                rx.try_recv().is_err(),
                "channel must remain empty after ForwardingAck"
            );
        }

        #[tokio::test]
        async fn put_request_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let op = PutMsg::Request {
                id: put_tx,
                contract: ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                    WrappedContract::new(
                        std::sync::Arc::new(ContractCode::from(vec![0u8])),
                        Parameters::from(vec![]),
                    ),
                )),
                related_contracts: RelatedContracts::default(),
                value: WrappedState::new(vec![1u8]),
                htl: 5,
                skip_list: std::collections::HashSet::new(),
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(!taken, "Request must NOT be forwarded to task channel");
            assert!(rx.try_recv().is_err(), "channel must remain empty");
        }

        #[tokio::test]
        async fn put_response_without_callback_falls_through() {
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(16, 17);
            let op = PutMsg::Response {
                id: put_tx,
                key,
                hop_count: 0,
            };

            let taken = put_branch_would_forward(&op, None);
            assert!(
                !taken,
                "Response without callback → must fall through to legacy path"
            );
        }

        // ───────────────────────────────────────────────────────────
        // Regression guards for the GET branch.
        //
        // Two dispatch layers:
        //   1. Reply bypass: terminal Response/ResponseStreaming for
        //      an active client driver → `try_forward_driver_reply`.
        //   2. Relay dispatch: `GetMsg::Request` →  `start_relay_get`,
        //      with originator loopback mapped to `upstream=own_addr`.
        // ───────────────────────────────────────────────────────────

        #[test]
        fn get_branch_dispatches_relay_driver() {
            const SOURCE: &str = include_str!("node.rs");
            let anchor = "NetMessageV1::Get(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "GET branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );
            // End the window at the next NetMessageV1 variant to bound
            // the search to the GET arm only.
            let next_variant = "NetMessageV1::Update(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of GET arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // Reply bypass must precede relay dispatch.
            assert!(
                window.contains("try_forward_driver_reply("),
                "GET branch no longer calls try_forward_driver_reply \
                 before relay dispatch. Restore the bypass."
            );
            assert!(
                window.contains("get::GetMsg::Response { .. }"),
                "GET branch bypass is not gated on Response. \
                 Non-terminal messages must NOT be forwarded to the driver channel."
            );
            assert!(
                window.contains("get::GetMsg::ResponseStreaming { .. }"),
                "GET branch bypass is not gated on ResponseStreaming. \
                 Both terminal variants must be forwarded."
            );

            // Relay dispatch must call start_relay_get.
            assert!(
                window.contains("start_relay_get("),
                "GET branch no longer calls start_relay_get for relay dispatch."
            );

            // Originator loopback (source_addr=None) is mapped to
            // upstream=own_addr, so dispatch is conditional on an
            // effective upstream rather than `source_addr.is_some()`.
            assert!(
                window.contains("effective_upstream") || window.contains("upstream_addr"),
                "GET relay dispatch must thread an effective upstream address \
                 (source_addr or own_addr loopback) into the relay driver."
            );

            // Legacy fallthrough and gate must stay deleted. Compose
            // needles at runtime so the assert source itself does not
            // contain them.
            let legacy_dispatch_needle = format!("handle{}::<get::GetOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "GET branch must NOT call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_get");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "GET branch must NOT gate on per-op DashMap existence"
            );

            // Bypass must precede relay dispatch in source order
            // (terminal-reply fast path has priority).
            let bypass_pos = window
                .find("try_forward_driver_reply(")
                .expect("try_forward_driver_reply not found in GET branch");
            let relay_pos = window
                .find("start_relay_get(")
                .expect("start_relay_get not found in GET branch");
            assert!(
                bypass_pos < relay_pos,
                "Reply bypass (try_forward_driver_reply) must \
                 appear BEFORE relay dispatch (start_relay_get) — \
                 swapping order would break the terminal-reply fast \
                 path."
            );
        }

        // ───────────────────────────────────────────────────────────
        // Regression guards for the UPDATE branch.
        //
        // UPDATE is fire-and-forget end-to-end — no upstream reply
        // to await, so no reply bypass exists. Only relay dispatch
        // is wired here.
        // ───────────────────────────────────────────────────────────

        /// Pin: every UPDATE wire variant dispatches to a relay
        /// driver. No legacy fallthrough remains — every reachable
        /// arm spawns a driver and returns `Ok(None)`.
        #[test]
        fn update_branch_dispatches_all_relay_drivers() {
            const SOURCE: &str = include_str!("node.rs");

            let anchor = "NetMessageV1::Update(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "UPDATE branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this regression guard",
            );

            // End the window at the next NetMessageV1 variant to bound
            // the search to the UPDATE arm only.
            let next_variant = "NetMessageV1::Subscribe(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of UPDATE arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            for driver in [
                "start_relay_request_update(",
                "start_relay_broadcast_to(",
                "start_relay_request_update_streaming(",
                "start_relay_broadcast_to_streaming(",
            ] {
                assert!(
                    window.contains(driver),
                    "UPDATE branch must call {driver} for relay dispatch."
                );
            }

            // Negative pins for the fallthrough: composing
            // needles at runtime so this test's source doesn't trip its
            // own assertion.
            let legacy_call = ["handle_op_request::<update::", "UpdateOp", ", _>"].concat();
            assert!(
                !window.contains(&legacy_call),
                "UPDATE branch must NOT call handle_op_request"
            );
            let dispatch_gate = ["has_", "update_op"].concat();
            assert!(
                !window.contains(&dispatch_gate),
                "UPDATE relay dispatch must NOT consult has_update_op"
            );
        }

        /// Pin: relay UPDATE dispatch is gated on
        /// `source_addr.is_some()`; internal callers must not spawn
        /// drivers.
        #[test]
        fn update_branch_dispatch_gates_on_source_addr() {
            const SOURCE: &str = include_str!("node.rs");

            let anchor = "NetMessageV1::Update(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect("UPDATE branch not found");
            let next_variant = "NetMessageV1::Subscribe(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of UPDATE arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            assert!(
                window.contains("if let Some(sender_addr) = source_addr"),
                "UPDATE relay dispatch must be gated on source_addr.is_some() — \
                 internal callers must NOT spawn relay drivers."
            );
        }

        // ── Relay PUT dispatch structural pin tests.

        #[test]
        fn put_branch_dispatches_relay_drivers() {
            const SOURCE: &str = include_str!("node.rs");
            let anchor = "NetMessageV1::Put(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "PUT branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );
            // End the window at the next NetMessageV1 variant to bound
            // the search to the PUT arm only.
            let next_variant = "NetMessageV1::Get(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of PUT arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];
            assert!(
                window.contains("start_relay_put("),
                "PUT branch no longer calls start_relay_put for relay dispatch."
            );
            assert!(
                window.contains("start_relay_put_streaming("),
                "PUT branch must call start_relay_put_streaming for streaming relay hops."
            );
            // Originator loopback (source_addr=None) is mapped to
            // upstream=own_addr, so dispatch is conditional on an
            // effective upstream rather than `source_addr.is_some()`.
            assert!(
                window.contains("effective_upstream") || window.contains("upstream_addr"),
                "PUT relay dispatch must thread an effective upstream address \
                 (source_addr or own_addr loopback) into the relay drivers."
            );
            // Legacy fallthrough and gate must stay deleted. Compose
            // needles at runtime so the assert source itself does not
            // contain them.
            let legacy_dispatch_needle = format!("handle{}::<put::PutOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "PUT branch must NOT call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_put");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "PUT branch must NOT gate on per-op DashMap existence"
            );
        }

        /// Pin: `start_relay_put` (slice A driver) MUST itself perform
        /// the upgrade-on-forward decision. The dispatch gate in
        /// node.rs no longer pre-checks `should_use_streaming` —
        /// the driver re-serializes the merged payload after
        /// `relay_put_store_locally` and conditionally builds either
        /// `PutMsg::Request` or `PutMsg::RequestStreaming` +
        /// `send_stream`.
        #[test]
        fn start_relay_put_handles_upgrade_on_forward() {
            const SOURCE: &str = include_str!("operations/put/op_ctx_task.rs");
            let anchor = "async fn drive_relay_put<CB>(";
            let driver_start = SOURCE
                .find(anchor)
                .expect("drive_relay_put fn not found — has the signature changed?");
            // Bound the search to the function body. End at the next
            // top-level `async fn` declaration in the module.
            let driver_end = SOURCE[driver_start + anchor.len()..]
                .find("\nasync fn ")
                .map(|idx| idx + driver_start + anchor.len())
                .unwrap_or(SOURCE.len());
            let body = &SOURCE[driver_start..driver_end];
            assert!(
                body.contains("should_use_streaming("),
                "drive_relay_put must call should_use_streaming on the merged \
                 payload to decide between non-streaming Request and streaming \
                 upgrade on forward."
            );
            assert!(
                body.contains("PutMsg::RequestStreaming {"),
                "drive_relay_put must build PutMsg::RequestStreaming when the \
                 forwarded payload would exceed streaming_threshold."
            );
            assert!(
                body.contains("send_stream("),
                "drive_relay_put must call NetworkBridge::send_stream for the \
                 raw fragments after the RequestStreaming metadata send."
            );
        }

        // ── Relay SUBSCRIBE dispatch structural pin tests.
        //
        // Every SUBSCRIBE wire variant routes either to the relay
        // driver (Request) or to a dedicated inbound handler
        // (Unsubscribe). Response is forwarded via the reply bypass.

        #[test]
        fn subscribe_branch_dispatches_relay_driver() {
            const SOURCE: &str = include_str!("node.rs");
            let anchor = "NetMessageV1::Subscribe(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "SUBSCRIBE branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );
            // End the window at the next NetMessageV1 variant to bound
            // the search to the SUBSCRIBE arm only.
            let next_variant = "// Non-transactional message types:";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of SUBSCRIBE arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // Terminal-reply bypass must still be present (gated on
            // SubscribeMsg::Response).
            assert!(
                window.contains("try_forward_driver_reply("),
                "SUBSCRIBE branch no longer calls try_forward_driver_reply \
                 before relay dispatch — restore it."
            );
            assert!(
                window.contains("subscribe::SubscribeMsg::Response { .. }"),
                "SUBSCRIBE branch bypass is not gated on Response. \
                 Non-terminal messages must NOT be forwarded to the driver channel."
            );

            // Relay dispatch must call start_relay_subscribe and route
            // the Unsubscribe variant through the inbound handler.
            assert!(
                window.contains("start_relay_subscribe("),
                "SUBSCRIBE branch no longer calls start_relay_subscribe for relay \
                 dispatch — restore it."
            );
            assert!(
                window.contains("handle_unsubscribe_inbound("),
                "SUBSCRIBE branch must call handle_unsubscribe_inbound \
                 for Unsubscribe wire messages."
            );

            // Originator loopback (source_addr=None) is mapped to
            // upstream=own_addr.
            assert!(
                window.contains("effective_upstream") || window.contains("upstream_addr"),
                "SUBSCRIBE relay dispatch must thread an effective upstream address \
                 (source_addr or own_addr loopback) into the relay driver."
            );

            // Legacy fallthrough and gate must stay deleted. Compose
            // needles at runtime so the assert source itself does not
            // contain them.
            let legacy_dispatch_needle =
                format!("handle{}::<subscribe::SubscribeOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "SUBSCRIBE branch must NOT call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_subscribe");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "SUBSCRIBE branch must NOT gate on per-op DashMap existence"
            );

            // Bypass must precede relay dispatch in source order
            // (terminal-reply fast path has priority).
            let bypass_pos = window
                .find("try_forward_driver_reply(")
                .expect("try_forward_driver_reply not found in SUBSCRIBE branch");
            let relay_pos = window
                .find("start_relay_subscribe(")
                .expect("start_relay_subscribe not found in SUBSCRIBE branch");
            assert!(
                bypass_pos < relay_pos,
                "SUBSCRIBE bypass (try_forward_driver_reply) must appear \
                 BEFORE relay dispatch (start_relay_subscribe). Swapping order \
                 would break the client-driver terminal-reply fast path."
            );
        }
    }

    /// Tests for `fill_connect_response_acceptor_addr`. The driver
    /// does not see `source_addr`, so the dispatch site must rewrite
    /// the payload before forwarding.
    mod fill_connect_response_acceptor_addr_tests {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        use super::super::fill_connect_response_acceptor_addr;
        use crate::message::Transaction;
        use crate::operations::connect::{ConnectMsg, ConnectResponse};
        use crate::ring::{PeerAddr, PeerKeyLocation};

        fn dummy_unknown_pkl() -> PeerKeyLocation {
            let pkl = PeerKeyLocation::random();
            PeerKeyLocation {
                pub_key: pkl.pub_key,
                peer_addr: PeerAddr::Unknown,
            }
        }

        fn known_addr() -> SocketAddr {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)), 50051)
        }

        #[test]
        fn fills_unknown_acceptor_addr_from_source_addr() {
            let id = Transaction::new::<ConnectMsg>();
            let payload = ConnectResponse {
                acceptor: dummy_unknown_pkl(),
            };
            let msg = ConnectMsg::Response { id, payload };

            let source = known_addr();
            let filled = fill_connect_response_acceptor_addr(msg, Some(source));

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Response { payload, .. } => {
                    assert_eq!(payload.acceptor.socket_addr(), Some(source));
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }

        #[test]
        fn leaves_known_acceptor_addr_unchanged() {
            let id = Transaction::new::<ConnectMsg>();
            let original_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)), 12345);
            let pkl = PeerKeyLocation::random();
            let payload = ConnectResponse {
                acceptor: PeerKeyLocation {
                    pub_key: pkl.pub_key,
                    peer_addr: PeerAddr::Known(original_addr),
                },
            };
            let msg = ConnectMsg::Response { id, payload };

            let filled = fill_connect_response_acceptor_addr(msg, Some(known_addr()));

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Response { payload, .. } => {
                    assert_eq!(
                        payload.acceptor.socket_addr(),
                        Some(original_addr),
                        "fill must NOT overwrite a known acceptor address"
                    );
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }

        #[test]
        fn unknown_acceptor_without_source_addr_passes_through() {
            // No source_addr available (e.g. inbound delivery dropped it).
            // The helper must not panic; the unknown address survives so
            // the driver's downstream `socket_addr()` check logs+drops.
            let id = Transaction::new::<ConnectMsg>();
            let payload = ConnectResponse {
                acceptor: dummy_unknown_pkl(),
            };
            let msg = ConnectMsg::Response { id, payload };

            let filled = fill_connect_response_acceptor_addr(msg, None);

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Response { payload, .. } => {
                    assert!(
                        payload.acceptor.peer_addr.is_unknown(),
                        "fill must remain Unknown when source_addr is None"
                    );
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }

        #[test]
        fn rejected_variant_passes_through_untouched() {
            // The bypass forwards both Response and Rejected; only Response
            // carries an acceptor. The helper must leave Rejected alone.
            use crate::ring::Location;
            let id = Transaction::new::<ConnectMsg>();
            let dl = Location::new(0.42);
            let msg = ConnectMsg::Rejected {
                id,
                desired_location: dl,
            };

            let filled = fill_connect_response_acceptor_addr(msg, Some(known_addr()));

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Rejected {
                    id: rid,
                    desired_location,
                } => {
                    assert_eq!(rid, id);
                    assert_eq!(desired_location, dl);
                }
                other => panic!("expected Rejected, got {other:?}"),
            }
        }
    }

    /// Regression guards for the CONNECT bypass `matches!` predicate.
    ///
    /// The relay-CONNECT driver owns the entire tx lifetime in task
    /// locals, so all four non-`Request` `ConnectMsg` variants
    /// (Response, Rejected, ObservedAddress, ConnectFailed) must
    /// reach the per-tx waiter receiver. `Request` is the spawn
    /// signal and is handled by the dispatch gate.
    mod connect_bypass_coverage_guards {
        const SOURCE: &str = include_str!("node.rs");

        fn connect_branch_window() -> &'static str {
            let branch_anchor = "NetMessageV1::Connect(ref op) => {";
            let branch_start = SOURCE.find(branch_anchor).expect(
                "Connect branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );

            let next_variant_anchor = "NetMessageV1::Put(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant_anchor)
                .expect("end of Connect branch not found — update guard")
                + branch_start;

            &SOURCE[branch_start..window_end]
        }

        #[test]
        fn connect_branch_bypass_forwards_response() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::Response { .. }"),
                "Connect bypass `matches!` no longer forwards Response. \
                 Response is the joiner-fan-in terminal variant and MUST \
                 reach the per-tx multi-reply receiver."
            );
        }

        #[test]
        fn connect_branch_bypass_forwards_rejected() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::Rejected { .. }"),
                "Connect bypass `matches!` no longer forwards Rejected. \
                 Relay drivers and the joiner driver both observe Rejected \
                 to record connection failure / record_connection_failure."
            );
        }

        #[test]
        fn connect_branch_bypass_forwards_observed_address() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::ObservedAddress { .. }"),
                "Connect bypass `matches!` no longer forwards \
                 ObservedAddress. The joiner driver inbox owns the \
                 set_own_addr / update_location side effect; dropping \
                 ObservedAddress here breaks NAT discovery."
            );
        }

        #[test]
        fn connect_branch_bypass_forwards_connect_failed() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::ConnectFailed { .. }"),
                "Connect bypass `matches!` no longer forwards \
                 ConnectFailed. The relay driver inbox owns hole-punch \
                 failure re-route; dropping ConnectFailed here strands \
                 the re-route on legacy `process_message`."
            );
        }

        #[test]
        fn connect_branch_bypass_does_not_forward_request() {
            // `Request` is the spawn signal handled by the dispatch
            // gate (commit 3); forwarding it via the bypass would
            // route fresh Requests into a multi-reply receiver that
            // doesn't exist yet, dropping them silently.
            let window = connect_branch_window();
            // Locate the bypass `matches!` block specifically — the
            // dispatch gate further down does destructure
            // `ConnectMsg::Request { id, payload }`, which is fine.
            let bypass_anchor = "if matches!(\n                op,";
            let bypass_start = window
                .find(bypass_anchor)
                .expect("bypass `matches!` block not found in Connect branch — guard outdated");
            let bypass_end = window[bypass_start..]
                .find(") {")
                .expect("bypass `matches!` block has no closing `) {`")
                + bypass_start;
            let bypass_block = &window[bypass_start..bypass_end];

            assert!(
                !bypass_block.contains("connect::ConnectMsg::Request"),
                "Connect bypass `matches!` MUST NOT forward Request. \
                 Request is the spawn signal for start_relay_connect; \
                 forwarding it would route fresh Requests into a multi-reply \
                 receiver that doesn't exist yet."
            );
        }

        /// The Connect branch MUST dispatch to `start_relay_connect`
        /// for fresh inbound Requests.
        #[test]
        fn connect_branch_dispatches_start_relay_connect_for_fresh_request() {
            let window = connect_branch_window();
            assert!(
                window.contains("start_relay_connect("),
                "Connect branch no longer calls start_relay_connect — \
                 removing it strands relay CONNECT on legacy."
            );
        }

        /// Relay dispatch must be gated on `source_addr.is_some()` so
        /// originator loop-back from `start_client_connect` (which
        /// cannot happen for CONNECT, but the guard documents the
        /// invariant) is dropped rather than spawning a self-loop.
        #[test]
        fn connect_relay_dispatch_gated_on_source_addr() {
            let window = connect_branch_window();
            let dispatch_anchor = "start_relay_connect(";
            let dispatch_pos = window
                .find(dispatch_anchor)
                .expect("start_relay_connect not found in Connect branch");
            let gate_start = dispatch_pos.saturating_sub(500);
            let gate_window = &window[gate_start..dispatch_pos];
            assert!(
                gate_window.contains("source_addr"),
                "CONNECT relay dispatch is not gated on source_addr — \
                 originator loop-back must NOT spawn a relay driver."
            );
        }

        /// Relay dispatch must also check `!active_relay_connect_txs.contains(id)`
        /// to avoid re-spawning a driver while a previous one is still running
        /// (e.g. duplicate Request retransmission while the driver is mid-
        /// handle_request).
        #[test]
        fn connect_relay_dispatch_guarded_by_active_relay_set() {
            let window = connect_branch_window();
            let dispatch_pos = window
                .find("start_relay_connect(")
                .expect("start_relay_connect not found in Connect branch");
            let gate_start = dispatch_pos.saturating_sub(500);
            let gate_window = &window[gate_start..dispatch_pos];
            assert!(
                gate_window.contains("active_relay_connect_txs"),
                "CONNECT relay dispatch is not guarded by \
                 active_relay_connect_txs.contains(id). Without it, a \
                 duplicate Request retransmission could spawn a second \
                 driver before the first inserts into the dedup set."
            );
        }
    }

    /// Source-level pin for the #4145 non-streaming caching safety net.
    ///
    /// The summary-cache fix (#4145) caches a peer's summary on any
    /// *delivered* broadcast. That is only safe because the
    /// `ResyncRequest` handler clears the SENDER's cached summary for the
    /// peer when a downstream delta fails to apply — otherwise a wrongly
    /// cached summary would trap the pair sending unappliable deltas.
    /// If a refactor drops the `update_peer_summary(.., None)` clear from
    /// this handler, the #4145 caching loses its corrective backstop and
    /// the behavioural sim test would still pass. Pin it at the source
    /// level so the omission fails CI.
    mod resync_request_clears_sender_summary {
        const SOURCE: &str = include_str!("node.rs");

        /// The body of the `InterestMessage::ResyncRequest` match arm,
        /// bounded by the start of the following `ResyncResponse` arm.
        fn resync_request_arm() -> &'static str {
            let arm_anchor = "InterestMessage::ResyncRequest { key } => {";
            let arm_start = SOURCE.find(arm_anchor).expect(
                "ResyncRequest arm of handle_interest_sync_message not found — \
                 the match arm has been renamed or moved; update this guard",
            );
            let next_anchor = "InterestMessage::ResyncResponse {";
            let arm_end = SOURCE[arm_start..]
                .find(next_anchor)
                .map(|i| arm_start + i)
                .expect("end of ResyncRequest arm not found — update guard");
            &SOURCE[arm_start..arm_end]
        }

        #[test]
        fn resync_request_handler_clears_cached_peer_summary() {
            let arm = resync_request_arm();
            assert!(
                arm.contains("update_peer_summary"),
                "ResyncRequest handler no longer calls update_peer_summary. \
                 #4145 caching relies on this handler clearing the sender's \
                 cached summary so a delta-apply failure forces a fresh \
                 full-state resend instead of looping on unappliable deltas."
            );
            // The clear MUST pass `None` (clear), not a `Some(summary)` cache.
            // Strip whitespace so the multi-line call (`op_manager\n
            // .interest_manager\n .update_peer_summary(&key, pk, None);`)
            // matches regardless of formatting.
            let collapsed: String = arm.chars().filter(|c| !c.is_whitespace()).collect();
            assert!(
                collapsed.contains("update_peer_summary(&key,pk,None)"),
                "ResyncRequest handler must clear the cached summary with \
                 `update_peer_summary(&key, pk, None)` (the `None` clears it). \
                 Caching a summary here instead would defeat the #4145 backstop."
            );
        }

        /// Pin (#4861 + #4864 round-4): the responder MUST, in order, (1) do a
        /// cheap state-presence check, (2) rate-limit per (peer, contract), (3)
        /// apply the global per-contract cap, and only then (4) do the expensive
        /// state/summary fetch. The existence check gating FIRST is the #4864
        /// round-4 fix — otherwise a peer spraying bogus keys exhausts the capped
        /// limiter maps and denies legit (peer, contract) keys.
        #[test]
        fn resync_request_handler_rate_limits_response() {
            let arm = resync_request_arm();
            // (1) cheap existence check BEFORE either limiter (async so the probe
            // is backend-agnostic: redb sync fast-path + real SQLite EXISTS,
            // #4864 round-5).
            let exists_pos = arm.find("contract_state_present_async(&key)").expect(
                "ResyncRequest handler must do a cheap state-presence check BEFORE \
                 the rate limiters (#4864 round-4/round-5)",
            );
            let gate_pos = arm.find("resync_response_limiter").expect(
                "ResyncRequest handler must rate-limit the response via \
                 resync_response_limiter (#4861)",
            );
            let state_fetch_pos = arm
                .find("get_contract_state(op_manager, &key)")
                .expect("state fetch not found in ResyncRequest arm");
            assert!(
                gate_pos < state_fetch_pos,
                "the responder rate-limit gate ({gate_pos}) must run BEFORE the \
                 state fetch ({state_fetch_pos}) so a suppressed request pays no \
                 fetch/summary cost"
            );
            // The GLOBAL per-contract cap must also gate, after the per-peer
            // limit and before the state fetch (#4861).
            let global_pos = arm.find("resync_response_global_limiter").expect(
                "ResyncRequest handler must also apply the GLOBAL per-contract \
                 response cap (#4861)",
            );
            assert!(
                exists_pos < gate_pos,
                "the cheap existence check ({exists_pos}) must run BEFORE the \
                 per-peer limiter ({gate_pos}) so a bogus contract never consumes \
                 a limiter slot (#4864 round-4)"
            );
            assert!(
                gate_pos < global_pos && global_pos < state_fetch_pos,
                "global cap ({global_pos}) must be checked after the per-peer \
                 limit ({gate_pos}) and before the state fetch ({state_fetch_pos})"
            );
            assert!(
                arm.contains("record_resync_response_suppressed_per_peer()")
                    && arm.contains("record_resync_response_suppressed_global()"),
                "each suppressed branch must record its own (per-peer vs global) \
                 resync-response-suppressed metric (#4864 review)"
            );
        }

        /// Pin (#4861): NO code path in node.rs may reset the merge-failure
        /// backoff. In particular the ResyncResponse APPLY arm must not — a
        /// full-state resync apply "succeeds" by replacing local state even in
        /// the semantic fork-oscillation poison class (it just flips the node to
        /// the other fork), so resetting here would make the backoff never trip
        /// and let the ~1-cycle/min storm continue. The backoff is reset ONLY by
        /// a genuine successful DELTA merge in the UPDATE broadcast driver
        /// (`operations/update/op_ctx_task.rs`) — full-state merges, streaming
        /// broadcast included, never reset because they carry the same
        /// fork-flip ambiguity as a resync apply.
        #[test]
        fn resync_apply_does_not_reset_merge_backoff() {
            // Assemble the needle from parts so the contiguous call string never
            // appears verbatim in this file — otherwise `.contains(<literal>)`
            // would match its OWN argument via `include_str!` (self-reference).
            let needle = concat!("merge_backoff", ".record_success");
            assert!(
                !SOURCE.contains(needle),
                "node.rs must NOT reset the merge backoff anywhere — a \
                 resync-apply (or any node.rs) reset would defeat the #4861 \
                 fork-oscillation containment. Reset belongs only in the broadcast \
                 drivers on a genuine merge success."
            );
        }

        /// Pin (#4864 round-5 item 6): the CHANGED ResyncResponse apply arm MUST
        /// call `invalidate_payload_memo` (presence — the no-reset pin above only
        /// asserts ABSENCE of a reset), and it must live in the `state_changed:
        /// true` sub-arm so a no-op apply does NOT invalidate the memo (item 8).
        /// Dropping the call silently widens the memo staleness corner to the full
        /// 10-min TTL.
        #[test]
        fn resync_changed_apply_invalidates_payload_memo() {
            // Window-bound to the ResyncResponse apply arm so the far-away test
            // literals below cannot satisfy the needles (self-reference guard).
            let start = SOURCE
                .find(concat!("InterestMessage::Resync", "Response {"))
                .expect("ResyncResponse arm not found");
            let arm = &SOURCE[start..(start + 6000).min(SOURCE.len())];
            let invalidate = concat!("invalidate_", "payload_memo(");
            // Match the PATTERN form (trailing comma) so a prose mention of
            // `state_changed: false` in the changed:true arm's comment is not
            // mistaken for the false sub-arm (self-reference guard).
            let changed_true = arm
                .find("state_changed: true,")
                .expect("state_changed:true sub-arm not found");
            let invalidate_pos = arm.find(invalidate).expect(
                "the CHANGED ResyncResponse apply arm must call invalidate_payload_memo \
                 (#4864 round-5 item 6)",
            );
            let changed_false = arm
                .find("state_changed: false,")
                .expect("state_changed:false sub-arm not found");
            assert!(
                changed_true < invalidate_pos && invalidate_pos < changed_false,
                "invalidate_payload_memo must live in the state_changed:true sub-arm \
                 so a no-op apply (state_changed:false) does NOT invalidate the memo"
            );
        }
    }

    /// Tests for `ShutdownHandle::shutdown`'s drain behaviour. The
    /// drain stops in-flight client PUT/GET/UPDATE/SUBSCRIBE drivers
    /// from being torn down mid-operation when the gateway is stopped
    /// for an auto-update (motivating incident: `freenet-git` mirror
    /// failures on the nova gateway).
    mod shutdown_drain {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        /// Construct a ShutdownHandle wired to a fresh channel,
        /// counter, and admission gate, mirroring the production
        /// wire-up in `NodeBuilder::build`. The receiver is returned
        /// so tests can assert what (if anything) was sent; the gate
        /// is returned so tests can observe Phase 1 flipping it.
        fn make_handle(
            initial_count: usize,
            drain_timeout: Duration,
        ) -> (
            ShutdownHandle,
            Arc<AtomicUsize>,
            Arc<std::sync::atomic::AtomicBool>,
            tokio::sync::mpsc::Receiver<NodeEvent>,
        ) {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let counter = Arc::new(AtomicUsize::new(initial_count));
            let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let handle = ShutdownHandle {
                tx,
                inflight_client_ops: counter.clone(),
                shutting_down: gate.clone(),
                drain_timeout,
            };
            (handle, counter, gate, rx)
        }

        #[tokio::test]
        async fn shutdown_with_zero_ops_returns_immediately() {
            let (handle, _counter, _gate, mut rx) = make_handle(0, Duration::from_secs(60));
            let start = std::time::Instant::now();
            handle.shutdown().await;
            assert!(
                start.elapsed() < Duration::from_millis(100),
                "shutdown with zero in-flight ops should not sleep"
            );
            // Disconnect must still be sent.
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }

        #[tokio::test]
        async fn shutdown_waits_then_proceeds_on_timeout() {
            // 1 op in flight that never decrements; drain capped at 200ms.
            let (handle, _counter, _gate, mut rx) = make_handle(1, Duration::from_millis(200));
            let start = std::time::Instant::now();
            handle.shutdown().await;
            let elapsed = start.elapsed();
            assert!(
                elapsed >= Duration::from_millis(180),
                "shutdown should wait the full drain timeout when ops \
                 never finish (elapsed: {elapsed:?})"
            );
            // Disconnect must still be sent so the node can exit.
            assert!(matches!(
                rx.recv()
                    .await
                    .expect("Disconnect must be sent even on drain timeout"),
                NodeEvent::Disconnect { .. }
            ));
        }

        #[tokio::test]
        async fn shutdown_proceeds_as_soon_as_counter_clears() {
            // 1 op in flight; another task decrements after 100ms.
            let (handle, counter, _gate, mut rx) = make_handle(1, Duration::from_secs(5));
            let counter_clone = counter.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                counter_clone.fetch_sub(1, Ordering::Relaxed);
            });
            let start = std::time::Instant::now();
            handle.shutdown().await;
            let elapsed = start.elapsed();
            assert!(
                elapsed >= Duration::from_millis(80) && elapsed < Duration::from_secs(2),
                "shutdown should return shortly after the counter clears, \
                 not wait the full drain timeout (elapsed: {elapsed:?})"
            );
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }

        #[tokio::test]
        async fn drain_disabled_skips_wait_even_with_ops_in_flight() {
            // Tests opt out of the drain via Duration::ZERO so a
            // SimNetwork teardown doesn't block on the 30s production
            // default. Verify the zero-timeout path bypasses the wait
            // entirely even when ops are "in flight".
            let (handle, _counter, _gate, mut rx) = make_handle(5, Duration::ZERO);
            let start = std::time::Instant::now();
            handle.shutdown().await;
            assert!(
                start.elapsed() < Duration::from_millis(50),
                "drain_timeout=0 must skip the wait"
            );
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }

        /// Phase 1 of the three-phase shutdown: admission gate MUST be
        /// flipped before the drain begins, so `start_client_*` calls
        /// arriving during the drain wait fail fast and don't slip
        /// through the post-drain race window. Codex reviewer call-out
        /// 2026-05 — re-opening this race re-opens the
        /// gateway-restart-kills-mirror-PUT failure for any op spawned
        /// in the window between drain-complete and Disconnect-send.
        #[tokio::test]
        async fn shutdown_closes_admission_gate_before_drain() {
            // 1 op in flight; drain caps at 500ms so we have time to
            // observe the gate during the wait.
            let (handle, counter, gate, mut rx) = make_handle(1, Duration::from_millis(500));
            assert!(
                !gate.load(Ordering::Relaxed),
                "admission gate must start closed"
            );

            let counter_clone = counter.clone();
            let gate_clone = gate.clone();
            let observed_during_drain = tokio::spawn(async move {
                // Wait briefly so shutdown's Phase 1 fires first.
                tokio::time::sleep(Duration::from_millis(50)).await;
                let g = gate_clone.load(Ordering::Relaxed);
                // Release the op so drain can complete.
                counter_clone.fetch_sub(1, Ordering::Relaxed);
                g
            });

            handle.shutdown().await;

            let gate_was_set_during_drain = observed_during_drain
                .await
                .expect("observer task must not panic");
            assert!(
                gate_was_set_during_drain,
                "shutdown() must flip the admission gate BEFORE the \
                 drain wait, not after. Otherwise a new client op \
                 spawned during the drain bypasses the gate, bumps \
                 the counter (now unobserved), and gets cut off."
            );
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }
    }

    // ───────────────────────────────────────────────────────────
    // #3798 Gap 1: cap stale-contract SyncStateToPeer emission per
    // Summaries message. Anti-amplification hardening — a peer whose
    // summary diverges on many contracts must not trigger an unbounded
    // burst of SyncStateToPeer events in one handler invocation.
    // ───────────────────────────────────────────────────────────
    mod stale_sync_cap {
        use super::super::{
            MAX_STALE_SYNCS_PER_SUMMARIES, StaleSyncDisposition, count_stale_syncs_emitted,
            emitted_indices_for_rotation, stale_sync_emit_budget,
        };
        use std::collections::HashSet;

        const EMIT: StaleSyncDisposition = StaleSyncDisposition::Emit;
        const BANNED: StaleSyncDisposition = StaleSyncDisposition::Banned;
        const NO_STATE: StaleSyncDisposition = StaleSyncDisposition::NoState;

        #[test]
        fn emit_budget_caps_at_max() {
            // Below cap: budget is the full count.
            assert_eq!(stale_sync_emit_budget(0), 0);
            assert_eq!(stale_sync_emit_budget(1), 1);
            assert_eq!(
                stale_sync_emit_budget(MAX_STALE_SYNCS_PER_SUMMARIES - 1),
                MAX_STALE_SYNCS_PER_SUMMARIES - 1
            );
            // At and above cap: budget saturates at the cap.
            assert_eq!(
                stale_sync_emit_budget(MAX_STALE_SYNCS_PER_SUMMARIES),
                MAX_STALE_SYNCS_PER_SUMMARIES
            );
            assert_eq!(
                stale_sync_emit_budget(MAX_STALE_SYNCS_PER_SUMMARIES + 1),
                MAX_STALE_SYNCS_PER_SUMMARIES
            );
            assert_eq!(
                stale_sync_emit_budget(MAX_STALE_SYNCS_PER_SUMMARIES * 100),
                MAX_STALE_SYNCS_PER_SUMMARIES
            );
        }

        /// The core regression: with far more stale contracts than the cap, the
        /// loop emits at most MAX_STALE_SYNCS_PER_SUMMARIES events. Without the
        /// cap this would emit one per contract (here, 200).
        #[test]
        fn many_stale_contracts_emit_at_most_cap() {
            let n = MAX_STALE_SYNCS_PER_SUMMARIES * 100; // 3200 divergent contracts
            let dispositions = vec![EMIT; n];
            let emitted = count_stale_syncs_emitted(&dispositions);
            assert_eq!(
                emitted, MAX_STALE_SYNCS_PER_SUMMARIES,
                "a peer diverging on {n} contracts must emit at most the cap \
                 ({MAX_STALE_SYNCS_PER_SUMMARIES}) SyncStateToPeer events per \
                 Summaries message, not one per contract"
            );
        }

        /// Boundary: exactly cap-many emittable contracts emit all of them.
        #[test]
        fn exactly_cap_emits_all() {
            let dispositions = vec![EMIT; MAX_STALE_SYNCS_PER_SUMMARIES];
            assert_eq!(
                count_stale_syncs_emitted(&dispositions),
                MAX_STALE_SYNCS_PER_SUMMARIES
            );
        }

        /// Below cap: emits exactly the emittable count, no spurious cap.
        #[test]
        fn below_cap_emits_all_emittable() {
            let dispositions = vec![EMIT; 5];
            assert_eq!(count_stale_syncs_emitted(&dispositions), 5);
            assert_eq!(count_stale_syncs_emitted(&[]), 0);
        }

        /// Banned / no-state contracts are skipped WITHOUT consuming the budget,
        /// so the emit count is the number of emittable contracts (capped), and
        /// a leading run of skips does not starve later emittable contracts when
        /// the total emittable count is under the cap.
        #[test]
        fn skips_do_not_consume_budget() {
            // 10 banned/no-state skips followed by 3 emittable contracts.
            // budget = min(13, cap) = 13 (cap is 32), so all 3 emit despite the
            // skips appearing first.
            let mut dispositions = vec![BANNED, NO_STATE, BANNED, NO_STATE, BANNED];
            dispositions.extend([NO_STATE, BANNED, NO_STATE, BANNED, NO_STATE]);
            dispositions.extend([EMIT, EMIT, EMIT]);
            assert_eq!(
                count_stale_syncs_emitted(&dispositions),
                3,
                "banned/no-state contracts must be skipped without consuming \
                 the emit budget"
            );
        }

        /// Even with skips interleaved, the number of EMIT events never exceeds
        /// the cap when there are more than `cap` emittable contracts. Here the
        /// budget = min(len, cap) where len > cap, so the loop stops at the cap
        /// (the trailing emittable contracts beyond the cap are deferred).
        #[test]
        fn interleaved_skips_still_capped() {
            // cap*3 emittable contracts, each preceded by one skip → len = cap*6,
            // budget = cap. The loop counts only EMITs toward the budget and
            // breaks at the cap.
            let mut dispositions = Vec::new();
            for _ in 0..(MAX_STALE_SYNCS_PER_SUMMARIES * 3) {
                dispositions.push(BANNED);
                dispositions.push(EMIT);
            }
            let emitted = count_stale_syncs_emitted(&dispositions);
            assert_eq!(
                emitted, MAX_STALE_SYNCS_PER_SUMMARIES,
                "emitted SyncStateToPeer events must be capped at \
                 {MAX_STALE_SYNCS_PER_SUMMARIES} even with skips interleaved"
            );
        }

        /// Starvation regression (codex P2 on PR #4468): with more stale
        /// contracts than the cap, the random rotation must make EVERY contract
        /// eligible for some rotation start. Otherwise a contract stuck in the
        /// fixed leading `cap` positions would re-consume the budget every cycle
        /// and permanently starve the tail. Asserts the union of emitted indices
        /// over all rotation starts covers the whole stale set.
        #[test]
        fn rotation_covers_every_contract_over_cap() {
            let total = MAX_STALE_SYNCS_PER_SUMMARIES * 3; // 96 > cap
            let mut covered = HashSet::new();
            for start in 0..total {
                for idx in emitted_indices_for_rotation(total, start) {
                    covered.insert(idx);
                }
            }
            assert_eq!(
                covered.len(),
                total,
                "every one of the {total} stale contracts must be reachable for \
                 some rotation start — otherwise contracts past the cap are \
                 permanently starved when the leading prefix stays stale"
            );
            // And each cycle still emits exactly the cap (no over/under-emit).
            for start in 0..total {
                assert_eq!(
                    emitted_indices_for_rotation(total, start).len(),
                    MAX_STALE_SYNCS_PER_SUMMARIES
                );
            }
        }

        /// A contract stuck at original index 0 (its emit keeps failing) must
        /// NOT prevent an over-cap contract from being attempted. With
        /// `total = cap + 1`, each rotation window of `cap` consecutive indices
        /// (mod total) covers all but exactly one index, so there is a rotation
        /// start whose window includes the tail index while excluding the
        /// assumed-stuck head index 0 — proving the anti-starvation property the
        /// deterministic prefix-only loop lacked.
        #[test]
        fn stuck_prefix_does_not_block_tail_under_rotation() {
            let total = MAX_STALE_SYNCS_PER_SUMMARIES + 1; // cap + 1
            let last = total - 1;
            let mut found = false;
            for start in 0..total {
                let window: HashSet<usize> = emitted_indices_for_rotation(total, start)
                    .into_iter()
                    .collect();
                if window.contains(&last) && !window.contains(&0) {
                    found = true;
                    break;
                }
            }
            assert!(
                found,
                "there must be a rotation start that attempts the tail contract \
                 ({last}) without attempting the (assumed-stuck) head contract \
                 (0); otherwise a stuck head starves the tail"
            );
        }

        /// Source-scrape pin: the `Summaries` arm of
        /// `handle_interest_sync_message` must still wire the emit budget into
        /// its `for contract in stale_contracts` loop (compute the budget,
        /// rotate by a random offset when over the cap to avoid starvation,
        /// break when `emitted >= emit_budget`, and increment `emitted` per
        /// emission). Guards against a future refactor silently dropping the
        /// cap or the rotation and re-opening the #3798 Gap 1 amplification
        /// burst / starvation — the behavioral tests above run against the
        /// model helpers, not the live loop, so this pin keeps the two in
        /// lockstep.
        #[test]
        fn stale_sync_loop_uses_emit_budget_pin() {
            const SOURCE: &str = include_str!("node.rs");

            // Bound the search window to the stale-contract emission loop.
            let loop_anchor = "for contract in stale_contracts {";
            let start = SOURCE.find(loop_anchor).expect(
                "stale-contract emission loop not found; the `for contract in \
                 stale_contracts` loop has been renamed or moved — update this \
                 pin and re-verify the #3798 Gap 1 cap is still applied",
            );
            // End the window at the next sibling loop in the Summaries arm.
            let window_end = SOURCE[start..]
                .find("for (key, state_hash) in confirmed_states {")
                .map(|off| start + off)
                .unwrap_or(SOURCE.len());
            // Include the budget computation that immediately precedes the loop.
            let budget_decl = "let emit_budget = stale_sync_emit_budget(";
            let budget_pos = SOURCE[..start]
                .rfind(budget_decl)
                .expect("emit budget is not computed before the stale-sync loop");
            let window = &SOURCE[budget_pos..window_end];

            assert!(
                window.contains("stale_sync_emit_budget("),
                "stale-sync loop no longer computes the emit budget — the \
                 #3798 Gap 1 cap has been dropped"
            );
            assert!(
                window.contains("if emitted >= emit_budget {"),
                "stale-sync loop no longer breaks when the emit budget is \
                 reached — the #3798 Gap 1 cap is not enforced"
            );
            assert!(
                window.contains("emitted += 1;"),
                "stale-sync loop no longer counts emissions against the budget \
                 — the #3798 Gap 1 cap cannot be enforced without it"
            );
            assert!(
                window.contains("MAX_STALE_SYNCS_PER_SUMMARIES"),
                "stale-sync cap warning no longer references the cap constant"
            );
            // Starvation avoidance (codex P2 on #4468): the over-cap branch must
            // rotate the stale set by a random offset before the loop, else a
            // stuck leading prefix re-consumes the cap every cycle and starves
            // the tail.
            assert!(
                window.contains("if total_stale > emit_budget {")
                    && window.contains("rotate_left("),
                "stale-sync loop no longer rotates the stale set when over the \
                 cap — over-cap contracts can be permanently starved by a stuck \
                 prefix (#3798 Gap 1 / #4468 codex P2)"
            );
            assert!(
                window.contains("GlobalRng::random_range("),
                "stale-sync rotation offset is no longer drawn from GlobalRng — \
                 a fixed/non-random rotation does not avoid starvation and \
                 breaks simulation determinism"
            );
        }
    }

    // ───────────────────────────────────────────────────────────
    // Regression guard for #3791 (fixed by #3793) / #3796.
    //
    // The summary-mismatch handler in `handle_interest_sync_message`'s
    // `Summaries` arm must heal a stale peer with a TARGETED
    // `SyncStateToPeer` aimed at exactly that peer — never a
    // `BroadcastStateChange`, which fans the state out to ALL subscribers
    // (~28 peers in production) and caused the 19:1–163:1 upload/download
    // ratios reported in #3791.
    //
    // The ORIGINAL regression test only exercised `InterestManager`
    // stale-peer *data* logic and would have stayed green if the `node.rs`
    // dispatch were reverted to `BroadcastStateChange`. These tests instead
    // exercise the actual `node.rs` dispatch DECISION — which `NodeEvent`
    // variant is built, and which peer it targets — via the pure
    // `stale_peer_sync_event` builder the production loop calls. Reverting
    // that builder to a broadcast fails `is_targeted_not_broadcast`; a
    // source-scrape pin (`emit_site_uses_targeted_builder`) additionally
    // fails if the loop stops routing through the builder.
    // ───────────────────────────────────────────────────────────
    mod fanout_regression_guard {
        use super::super::stale_peer_sync_event;
        use crate::message::NodeEvent;
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey, WrappedState};

        fn test_key() -> ContractKey {
            ContractKey::from_id_and_code(
                ContractInstanceId::new([7u8; 32]),
                CodeHash::new([9u8; 32]),
            )
        }

        /// The load-bearing assertion: the summary-mismatch heal event is a
        /// TARGETED `SyncStateToPeer` at the reporting peer, NOT an
        /// all-subscriber `BroadcastStateChange`. This is exactly the bug
        /// #3791 reintroduced — the event that reverting the fix would flip
        /// back to a broadcast fan-out.
        #[test]
        fn stale_peer_sync_event_is_targeted_not_broadcast() {
            let key = test_key();
            let state = WrappedState::new(vec![1, 2, 3, 4]);
            let target: std::net::SocketAddr = "203.0.113.7:31337".parse().unwrap();

            let event = stale_peer_sync_event(key, state.clone(), target);

            // The whole point of #3791/#3796: this MUST be a targeted
            // SyncStateToPeer, NOT a BroadcastStateChange (all-subscriber
            // fan-out) or any other variant.
            let NodeEvent::SyncStateToPeer {
                key: ev_key,
                new_state,
                target: ev_target,
            } = event
            else {
                panic!(
                    "summary-mismatch heal must emit a targeted SyncStateToPeer, \
                     got fan-out/other variant: {event:?}"
                );
            };
            assert_eq!(
                ev_target, target,
                "SyncStateToPeer must target EXACTLY the peer that reported the \
                 stale summary, not any other peer"
            );
            assert_eq!(ev_key, key, "event must carry the mismatched contract key");
            assert_eq!(
                new_state.as_ref(),
                state.as_ref(),
                "event must carry the local state to heal the peer with"
            );
        }

        /// A different reporting peer must be the sole target — proves the
        /// event is genuinely per-peer targeted and not accidentally fixed to
        /// one address.
        #[test]
        fn stale_peer_sync_event_targets_the_reporting_peer() {
            let key = test_key();
            let state = WrappedState::new(vec![0xAB]);
            let a: std::net::SocketAddr = "198.51.100.1:1000".parse().unwrap();
            let b: std::net::SocketAddr = "198.51.100.2:2000".parse().unwrap();

            for target in [a, b] {
                let event = stale_peer_sync_event(key, state.clone(), target);
                let NodeEvent::SyncStateToPeer {
                    target: ev_target, ..
                } = event
                else {
                    panic!("expected targeted SyncStateToPeer, got {event:?}");
                };
                assert_eq!(ev_target, target);
            }
        }

        /// Source-scrape pin: the production emit site in the `Summaries` arm
        /// must route through `stale_peer_sync_event` rather than constructing
        /// a `NodeEvent` inline. Without this, someone could reintroduce an
        /// inline `NodeEvent::BroadcastStateChange { .. }` at the emit site and
        /// leave the (still-passing) builder tests above untouched — the exact
        /// test-gap #3796 calls out.
        #[test]
        fn emit_site_uses_targeted_builder() {
            const SOURCE: &str = include_str!("node.rs");

            let anchor = "for contract in stale_contracts {";
            let start = SOURCE
                .find(anchor)
                .expect("stale-contract emission loop not found — update this pin");
            let window_end = SOURCE[start..]
                .find("for (key, state_hash) in confirmed_states {")
                .map(|off| start + off)
                .unwrap_or(SOURCE.len());
            let window = &SOURCE[start..window_end];

            assert!(
                window.contains("stale_peer_sync_event(contract, state, source)"),
                "the stale-peer heal emit site no longer routes through \
                 `stale_peer_sync_event(contract, state, source)` — the \
                 targeted-vs-broadcast dispatch decision is no longer pinned \
                 by the builder tests (#3791/#3796)"
            );
            assert!(
                !window.contains("NodeEvent::BroadcastStateChange"),
                "the stale-peer heal emit site now constructs a \
                 NodeEvent::BroadcastStateChange — this is the #3791 \
                 regression (all-subscriber fan-out instead of a targeted \
                 SyncStateToPeer)"
            );
        }

        /// Source-scrape pin for the SECOND emit site (#3796 B2 gap): the
        /// proximity-cache overlap path in the connect handler must also route
        /// through `stale_peer_sync_event` and MUST NOT construct an inline
        /// `NodeEvent::SyncStateToPeer` (which a future edit could silently
        /// flip to `BroadcastStateChange`, uncaught by any test). This site
        /// was left unguarded by the first #3796 pass; this test closes it so
        /// BOTH targeted-send sites are pinned to the same guarded builder.
        #[test]
        fn proximity_overlap_emit_site_uses_targeted_builder() {
            const SOURCE: &str = include_str!("node.rs");

            let anchor = "Proximity cache overlap — syncing state to neighbor";
            let start = SOURCE
                .find(anchor)
                .expect("proximity-overlap emit site not found — update this pin");
            // Bound the window to the emit block: from the log line to the end
            // of the best-effort error handler that follows the try_notify call.
            let window_end = SOURCE[start..]
                .find("for proximity sync (best-effort)")
                .map(|off| start + off)
                .expect("proximity-overlap emit block end marker not found");
            let window = &SOURCE[start..window_end];

            assert!(
                window.contains("stale_peer_sync_event(key, state, source)"),
                "the proximity-overlap emit site no longer routes through \
                 `stale_peer_sync_event(key, state, source)` — its \
                 targeted-vs-broadcast dispatch decision is no longer pinned \
                 by the builder tests (#3791/#3796 B2 gap)"
            );
            assert!(
                !window.contains("NodeEvent::SyncStateToPeer"),
                "the proximity-overlap emit site constructs an inline \
                 NodeEvent::SyncStateToPeer instead of using the guarded \
                 builder — an inline construction is one edit away from an \
                 unguarded BroadcastStateChange fan-out (#3796 B2 gap)"
            );
            assert!(
                !window.contains("NodeEvent::BroadcastStateChange"),
                "the proximity-overlap emit site now constructs a \
                 NodeEvent::BroadcastStateChange — all-subscriber fan-out \
                 instead of a targeted SyncStateToPeer (#3791/#3796)"
            );
        }
    }
}
