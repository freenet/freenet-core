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
pub(crate) use network_bridge::broadcast_queue_metrics::BROADCAST_QUEUE_EFFICIENCY_METRICS;
// Re-export the summary-first PUT version gate (#4642 step 3-bis) so
// `ring::connection_manager::ConnectionManager::supports_summary_first_put`
// can consult it — the gate lives behind the private `network_bridge`
// module, so a sibling top-level module (`ring`) needs a targeted
// re-export rather than a path through `network_bridge` directly. Mirrors
// `BROADCAST_STREAM_METRICS` above.
pub(crate) use network_bridge::p2p_protoc::{
    BROADCAST_TARGET_LIST_MIN_VERSION, HASH_FIRST_SUMMARIES_MIN_VERSION,
    SUMMARY_FIRST_PUT_MIN_VERSION, version_supports_broadcast_target_list,
    version_supports_hash_first_summaries, version_supports_summary_first_put,
};
// Test-only: the release-timing marker has no runtime reader by design (see
// its rustdoc), so re-exporting it unconditionally is an unused import in the
// non-test build.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use network_bridge::p2p_protoc::BROADCAST_TARGET_LIST_SHIPPED_IN;
#[cfg(test)]
pub(crate) use network_bridge::p2p_protoc::HASH_FIRST_SHIPPED_IN;
#[cfg(test)]
pub(crate) use network_bridge::{EventLoopNotificationsReceiver, event_loop_notification_channel};
// Re-export types for dev_tool and testing
pub use network_bridge::{EventLoopExitReason, NetworkStats, reset_channel_id_counter};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::OpManager;

mod network_bridge;

/// Where an applied contract update came from (#5062). Re-exported because
/// `operations::update` has to name it to declare its provenance, and
/// `network_bridge` is private outside this module.
pub(crate) use network_bridge::broadcast_payload_mix::ApplyOrigin;

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
    listener_exit_is_graceful,
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
    /// Test-only override for the hash-first summary version floor
    /// (`HASH_FIRST_SUMMARIES_MIN_VERSION`, #4965). Threaded exactly like the
    /// two overrides above and consulted as
    /// `hash_first_summaries_floor_override().unwrap_or(HASH_FIRST_SUMMARIES_MIN_VERSION)`
    /// by `ConnectionManager::supports_hash_first_summaries`.
    ///
    /// In production this is `None` → the real `(0, 2, 116)` floor (untouched).
    ///
    /// **Simulations default this to `SIM_MIGRATION_ENABLED_FLOOR` — ON — which
    /// is the OPPOSITE of the two overrides above.** The deviation is
    /// deliberate and is the whole reason this field exists. Those two gate
    /// behavioural *cascades* (extra directed subscribes, extra probes) that
    /// pile load onto unrelated sims, so they fail closed. Hash-first changes
    /// only the ENCODING of an exchange that already runs in every sim, with
    /// identical convergence semantics — so defaulting it ON costs unrelated
    /// sims nothing and buys the one thing a version-gated wire change
    /// otherwise cannot get: the whole simulation suite exercising the new
    /// path BEFORE the release that lifts the crate version over the floor.
    /// Without it, the first integration-level run of this code would be the
    /// release PR itself, where a red sim could not be attributed between the
    /// feature and the version bump.
    ///
    /// `SimNetwork::disable_hash_first_summaries` pins it OFF for a test that
    /// needs the pre-0.2.116 fallback (mixed-version interop), and
    /// `enable_hash_first_summaries` states the default explicitly at a call
    /// site that depends on it.
    ///
    /// Not cfg-gated for the same reason as `subscribe_hint_floor_override`:
    /// `node::testing_impl` sets it and is compiled unconditionally.
    /// `#[serde(skip)]`; never serialized.
    #[serde(skip)]
    pub(crate) hash_first_summaries_floor_override: Option<(u8, u8, u16)>,
    /// Test-only override for the version-carrying-ack floor
    /// (`GATEWAY_ACK_VERSION_MIN_VERSION`, #5161). Threaded down into the
    /// TRANSPORT layer (`create_connection_handler`), unlike the three
    /// overrides above, which are read at the `ConnectionManager`: the gate it
    /// controls decides how a handshake ack is ENCODED, and the handshake runs
    /// below the node layer entirely.
    ///
    /// In production this is `None` → the real `(0, 2, 120)` floor (untouched).
    ///
    /// **Simulations default this OFF**, like the two cascade gates and unlike
    /// `hash_first_summaries_floor_override`. The gate is encoding-only where it
    /// fires, but the version it teaches is the INPUT to every other
    /// `version_supports_*` gate, so enabling it network-wide makes
    /// node->gateway links newly eligible for those features — a cascade in
    /// effect. Measured rather than assumed: defaulting it ON changes the
    /// outcome of the summary-first PUT sims.
    ///
    /// A sim that wants it calls `SimNetwork::enable_gateway_ack_version`; see
    /// that method for the full argument and for what the OFF default costs.
    ///
    /// Not cfg-gated for the same reason as `subscribe_hint_floor_override`:
    /// `node::testing_impl` sets it and is compiled unconditionally.
    /// `#[serde(skip)]`; never serialized.
    #[serde(skip)]
    pub(crate) ack_version_floor_override: Option<(u8, u8, u16)>,
    /// Test-only override for the originator-target-list version floor
    /// ([`BROADCAST_TARGET_LIST_MIN_VERSION`], #5147). Threaded exactly like
    /// the three overrides above and consulted as
    /// `broadcast_target_list_floor_override.unwrap_or(BROADCAST_TARGET_LIST_MIN_VERSION)`
    /// by `ConnectionManager::supports_broadcast_target_list`.
    ///
    /// In production this is `None` → the real `(0, 2, 120)` floor.
    ///
    /// **Simulations default this to `SIM_MIGRATION_DISABLED_FLOOR` — OFF —
    /// like `subscribe_hint_floor_override` and `summary_first_put_floor_override`,
    /// and unlike `hash_first_summaries_floor_override`.** The rule stated at
    /// that field applies: hash-first defaults ON because it changes only the
    /// ENCODING of an exchange with identical semantics, whereas this gate
    /// changes BEHAVIOUR — it removes peers from a fan-out. Every sim that
    /// asserts on delivery or convergence would silently be exercising a
    /// different delivery graph, and a failure could not be attributed between
    /// the sim's own subject and this suppression. Tests that want it opt in
    /// via `SimNetwork::enable_broadcast_target_list`.
    ///
    /// Not cfg-gated, for the same reason as the others: `node::testing_impl`
    /// sets it and is compiled unconditionally. `#[serde(skip)]`; never
    /// serialized.
    #[serde(skip)]
    pub(crate) broadcast_target_list_floor_override: Option<(u8, u8, u16)>,
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
            hash_first_summaries_floor_override: None,
            ack_version_floor_override: None,
            broadcast_target_list_floor_override: None,
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

            let mut registers: Vec<Box<dyn NetEventRegister>> = Vec::new();

            // The local append-only diagnostic log (`_EVENT_LOG`) is opt-in on
            // network nodes (#4968). Measured on a live 0.2.111 peer it cost
            // ~61 MiB/hour of appends and accounted for 95% of every fsync the
            // process issued, for a forensic record nothing currently harvests
            // (no `freenet service report` path reads it; `fdev verify-state`
            // reads the Local-mode `_EVENT_LOG_LOCAL`, which stays on by
            // default). This gate does NOT affect the `TelemetryReporter`
            // added below — that is a separate sink fed in-memory off the same
            // event stream, and it is what feeds telemetry.freenet.org.
            let flush_handle = if self.config.event_log_enabled() {
                let event_reg = EventRegister::new(self.config.event_log());
                let handle = event_reg.flush_handle();
                registers.push(Box::new(event_reg));
                handle
            } else {
                crate::tracing::EventFlushHandle::noop()
            };

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

            // Independent of the TelemetryReporter above: a separate opt-in
            // (`otel-telemetry-enabled`), a separate endpoint, and a separate
            // collector. It is not a NetEventRegister — it installs a global
            // meter provider that instrumentation reaches via
            // `opentelemetry::global::meter`.
            // The transport keypair, NOT a `PeerId`: it yields both identity
            // resource attributes and, in `freenet` auth mode, the token
            // signing key. A PeerId renders as `{pub_key}@{addr}`, which would
            // put this node's socket address on every exported batch and
            // re-identify the node on every address change.
            crate::tracing::otel::init(&self.config.otel, &self.key_pair);

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
        // Was silent at every level. Finding the clobber bug required patching
        // a node to see this branch at all, because "no waiter is registered"
        // is exactly the symptom a displaced waiter produces.
        tracing::debug!(
            tx_id = %reply.id(),
            %op_label,
            "try_forward_driver_reply: no waiter registered for this tx; dropping the reply"
        );
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
                    | update::UpdateMsg::BroadcastToStreaming { key, .. }
                    | update::UpdateMsg::BroadcastToV2 { key, .. }
                    | update::UpdateMsg::BroadcastToStreamingV2 { key, .. } => *key,
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
                    // The legacy variant carries no target list, so the
                    // relayer suppresses nothing and behaves exactly as it does
                    // today. `CoveredPeers::empty()` is that "nobody is
                    // covered" statement, not a missing value.
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
                            crate::ring::broadcast_coverage::CoveredPeers::empty(),
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
                            crate::ring::broadcast_coverage::CoveredPeers::empty(),
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
                    // #5147. These two arms are the ONLY places a target list
                    // is honored, and that is the security rule rather than an
                    // implementation detail: the list is trusted solely because
                    // it arrived on the message that delivered the payload, so
                    // its author is attesting to sends it actually made. A
                    // relayer therefore cannot fabricate an "everyone is
                    // covered" list to suppress third-party fan-out, because
                    // there is no other message type that carries one. Do NOT
                    // add a `covered` field to a non-payload-bearing variant.
                    update::UpdateMsg::BroadcastToV2 {
                        id,
                        key,
                        payload,
                        sender_summary_bytes,
                        covered,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_broadcast_to(
                            op_manager.clone(),
                            *id,
                            *key,
                            payload.clone(),
                            sender_summary_bytes.clone(),
                            sender_addr,
                            covered.clone(),
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_broadcast_to (v2) failed"
                            );
                        }
                        return Ok(());
                    }
                    update::UpdateMsg::BroadcastToStreamingV2 {
                        id,
                        key,
                        stream_id,
                        total_size,
                        covered,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_broadcast_to_streaming(
                            op_manager.clone(),
                            *id,
                            *key,
                            *stream_id,
                            *total_size,
                            sender_addr,
                            covered.clone(),
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_broadcast_to_streaming (v2) failed"
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
            // as of the last interest-set snapshot (no-op before the runtime pool
            // is built). Since #5268 that is NOT strictly decision-time fresh: the
            // cache is keyed by code hash, so its interest predicate answers "is
            // any in-use contract running this code", and that set is itself
            // memoized for 10 s (`InUseCodeHashes`). The scan this forces is fresh;
            // the demand it reads can be up to that window stale. It also cannot
            // see migrations still in-flight (admitted but not yet
            // hosted/compiled), so a tight burst can still overshoot by ~one
            // migration-completion latency before completed migrations push the hot
            // set to the ceiling — a bounded, self-correcting residual, not the
            // unbounded 10-s-stale window this refresh was added to close. Uses the
            // GAUGES-ONLY refresher: it must
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
/// starts at a random offset and wraps (see the `rotate_left` in
/// `emit_stale_peer_syncs`, the helper both the `Summaries` and
/// `SummaryDigests` arms share), so the cap window slides across the whole
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

/// Per-message cap on how many DISTINCT contract hashes a `SummaryRequest`
/// may name (#4965).
///
/// # Scope, after #5238
///
/// This constant used to govern the `SummaryDigests` window and the
/// `SummaryRequest` answer as well. It no longer does: the WORK bound on both
/// moved to [`MAX_SUMMARY_COMPARISONS_PER_MESSAGE`], which is two orders of
/// magnitude smaller, because the reasoning recorded below — that per-entry
/// work is a cheap DashMap get — was wrong about the receive side, and being
/// wrong about it in exactly this way is what produced the #5238 storm.
///
/// What is left here is the INPUT filter on `SummaryRequest`'s hash list, where
/// the constant is still doing its original job: stopping a peer naming an
/// arbitrarily long list. The two are now independently tunable, which the
/// paragraph at the end of this comment used to say they were not.
///
/// A digest entry costs the SENDER ~20 bytes and can cost the RECEIVER a
/// `summary_if_hosted_or_in_use` round trip, which is an amplification ratio
/// the full-bytes `Summaries` never had: there, an entry cost the sender a
/// whole `StateSummary`. The cap restores a bound. It is per-message and per
/// DISTINCT hash: a peer repeating one hash is deduplicated before any work,
/// so repetition buys nothing.
///
/// Over-cap hashes are not dropped permanently — the interest heartbeat
/// re-advertises them, though since #5238 that takes `ceil(n / 64)` rounds
/// rather than the next one. The starvation argument
/// [`MAX_STALE_SYNCS_PER_SUMMARIES`] makes still applies wherever a cap binds
/// on this path: `get_matching_contracts` sorts by contract id, so a fixed
/// prefix would starve the tail forever, and every capped site here rotates.
///
/// # Why 4096, measured rather than guessed
///
/// This constant was 256, chosen on the assumption that "a healthy pair shares
/// a handful of contracts". **Production telemetry says otherwise.**
/// `hosting_contract_count` over 16,578 samples from the deployed collector:
///
/// | p50 | p75 | p90 | p95 | p99 | max |
/// |-----|-----|-----|-----|-----|-----|
/// | 417 | 698 | 825 | 976 | 2463 | 2814 |
///
/// **73% of samples exceed 256.** A median peer hosts 417 contracts, so the
/// old value would have bound on the common case, not the abuse case.
///
/// The reason this was not obvious from the bandwidth data — and the reason
/// the byte metrics everyone had been staring at could not have revealed it —
/// is that **entry count is decoupled from message size**:
/// `summary_if_hosted_or_in_use` returns `None` for any contract the responder
/// does not host-or-serve, and a `None` entry costs ~5 bytes on the wire. A
/// pair sharing 400 contracts of which the responder hosts a handful produces
/// a CHEAP 400-entry message. Byte size bounds nothing here.
///
/// 4096 clears the observed maximum (2814) with headroom. The counter that
/// measures this directly per message, `summaries_entries` (#5061), ships in
/// this same release and is the ongoing field validation.
///
/// # What the cap is, and is not
///
/// It is a **backstop, not the operative bound**, and the argument for that
/// used to run: `lookup_by_hash` can only resolve contracts WE already track,
/// so a peer cannot make us summarize something we do not have, and what the
/// cap really bounds is a per-entry DashMap-get loop — cheap, so a generous
/// value costs nothing.
///
/// **The last step of that is what #5238 disproved.** "A peer cannot make us
/// summarize something we do not have" bounds WHICH contracts, not HOW MANY,
/// and on a peer hosting 933 contracts the honest heartbeat is itself hundreds
/// of entries. The per-entry work is a contract-handler round trip that
/// re-enters WASM on a cache miss, not a DashMap get. Left in place because it
/// is the reasoning error, and a future editor is better served by seeing it
/// than by seeing a corrected version that looks like nobody ever got it wrong.
///
/// Its one remaining use is the `SummaryRequest` input slice, and there the
/// original reasoning does hold: a long hash list really is only a
/// `get_matching_contracts` filter, and the summarize loop that follows it is
/// separately bounded by [`MAX_SUMMARY_COMPARISONS_PER_MESSAGE`].
const MAX_SUMMARY_HASHES_PER_MESSAGE: usize = 4096;

/// Per-`Summaries`-message cap on semantic-staleness probes — the WASM
/// `get_state_delta` calls the `Summaries` handler issues to decide, for a
/// byte-differing summary, whether a peer is genuinely stale (#4857 secondary
/// finding). Mirrors the sibling per-message WASM-fan-out caps
/// (`MAX_STALE_SYNCS_PER_SUMMARIES = 32` here, `MAX_DELTA_COMPUTATIONS_PER_FANOUT`
/// in the broadcast path) and the code-style rule that per-recipient WASM work
/// MUST be bounded: without it a peer that sends crafted/novel summary bytes for
/// every hosted contract would force unbounded `get_state_delta` execution on
/// the serial contract-handling loop. 32 matches the burst-control family and
/// sits far above the handful of genuinely-diverged contracts a healthy peer
/// reports per exchange.
const MAX_STALENESS_PROBES_PER_SUMMARIES: usize = 32;

/// What to do about one byte-differing contract when deciding staleness in the
/// `Summaries` handler, given the cheap in-memory cache lookup and how many
/// probes this message has already spent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StalenessProbeAction {
    /// The shared delta cache already answered — use this verdict, no WASM,
    /// no budget consumed. Cache hits are free, so they never count against
    /// the probe budget (a healthy peer with a warm cache is fully evaluated).
    UseCached(bool),
    /// Cache miss with budget remaining — run the WASM `get_state_delta` probe
    /// and count it against the per-message budget.
    RunProbe,
    /// Cache miss with the probe budget exhausted for this message — do NOT
    /// probe; fall back to the conservative byte comparison (differing bytes =>
    /// stale => heal, itself capped downstream by [`MAX_STALE_SYNCS_PER_SUMMARIES`]).
    /// The contract is re-evaluated on the next heartbeat once the cache warms.
    BudgetExhaustedFallBack,
}

/// Decide how to resolve staleness for one byte-differing contract, bounding the
/// number of WASM probes per `Summaries` message to
/// [`MAX_STALENESS_PROBES_PER_SUMMARIES`]. Only cache MISSES (which require WASM)
/// consume the budget; cache hits are free and always answered. Pure so the cap
/// is unit-testable (see `staleness_probe_cap`).
fn plan_staleness_probe(cached: Option<bool>, probes_used: usize) -> StalenessProbeAction {
    match cached {
        Some(has_change) => StalenessProbeAction::UseCached(has_change),
        None if probes_used < MAX_STALENESS_PROBES_PER_SUMMARIES => StalenessProbeAction::RunProbe,
        None => StalenessProbeAction::BudgetExhaustedFallBack,
    }
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

/// What a peer's advertised summary digest tells us about that contract.
///
/// Pure classification of one [`crate::message::SummaryDigestEntry`] against
/// OUR OWN summary, factored out so the one comparison the whole hash-first
/// exchange rests on is unit-testable without a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DigestVerdict {
    /// The peer's digest equals a digest of our own summary, so we know their
    /// summary bytes without them sending any: they are ours.
    ///
    /// This is the 98.1% case (#4965) and the entire point of the exchange.
    Agree,
    /// The peer reported no summary at all (`summary_digest: None`) — they are
    /// interested but hold no state. Identical in meaning to a
    /// `SummaryEntry { summary_bytes: None }`, and handled identically: clear
    /// our cached summary for them, no heal (there is no divergence to heal
    /// when one side has nothing).
    PeerHasNoState,
    /// We cannot settle this entry from the digest alone and must ask for the
    /// bytes. Either the digests differ (real divergence, or a
    /// non-deterministically-serialized summary), or the peer has a summary
    /// and we do not — in which case we still need their bytes to seed the
    /// peer-summary cache (#4952) and keep them off the full-state broadcast
    /// path.
    NeedBytes,
}

/// Classify one advertised digest against our own summary.
///
/// Both inputs come from FACT, never belief: `our_summary` is this node's
/// actual state summary (`summary_if_hosted_or_in_use`), and `their_digest` is
/// what the peer computed from its actual state. The cached
/// `PeerInterest.summary` — our *belief* about the peer — is deliberately not
/// consulted, because every failure anti-entropy exists to repair is precisely
/// that belief being wrong.
fn classify_summary_digest(
    our_summary: Option<&freenet_stdlib::prelude::StateSummary<'static>>,
    their_digest: Option<&crate::ring::interest::SummaryDigest>,
) -> DigestVerdict {
    match (our_summary, their_digest) {
        (_, None) => DigestVerdict::PeerHasNoState,
        (None, Some(_)) => DigestVerdict::NeedBytes,
        (Some(ours), Some(theirs)) => {
            if crate::ring::interest::summary_digest(ours.as_ref()) == *theirs {
                DigestVerdict::Agree
            } else {
                DigestVerdict::NeedBytes
            }
        }
    }
}

/// Choose the wire form for a `Summaries` reply aimed at `target`: the
/// hash-first [`InterestMessage::SummaryDigests`] when the peer is new enough
/// to decode it, otherwise the full-bytes [`InterestMessage::Summaries`].
///
/// The digest form is derived from the very `SummaryEntry` values we would
/// otherwise have sent, so the two forms cannot describe different state: the
/// digest is a pure function of the exact bytes the fallback carries.
///
/// Fail-closed on an unknown peer version (see
/// [`crate::node::HASH_FIRST_SUMMARIES_MIN_VERSION`]): the fallback is what
/// every peer does today, so the cost of guessing wrong is bandwidth, never
/// convergence.
///
/// # Only the REPLY legs use this (#4965 review §2)
///
/// `ChangeInterestsReply` routes through here. `InterestsReply` no longer
/// does: #5155 needs the form BEFORE it builds entries, so that it can bound
/// what the full-bytes fallback carries, and it calls [`summary_reply_form`]
/// and [`summaries_reply_in_form`] separately rather than reading the gate
/// twice. The encoding decision is identical; only the point at which it is
/// taken moved.
///
/// The heading said "MULTI-ENTRY reply legs" until 2026-08-12; corrected per
/// #5153 review F1, because **`ChangeInterestsReply` is single-entry** (one
/// contract per `broadcast_change_interests` gossip; measured mean 1.000
/// entries/msg, `max_entries` 1, over 418,476 messages on 1,284 peers). Only
/// `InterestsReply` is genuinely multi-entry. The distinction is load-bearing
/// for the R4b agreement-rate instrument, which cannot read the emitter tag and
/// so uses message length as its proxy: `ChangeInterestsReply` is that proxy's
/// largest contaminant, and calling it multi-entry is what made the proxy look
/// clean. See `network_bridge::outbound_message_mix`.
///
/// `Notification` and `Rejection` are also single-entry, but call
/// [`full_summaries_message`] directly and ship full bytes this release — which
/// is why a single-entry observation on the DIGEST leg cannot be a notification
/// today.
///
/// The reason is evidential, not technical: the 98.1% agreement rate that
/// justifies hash-first was measured on a heartbeat-dominated population, and
/// the state-change-driven legs are the population least likely to agree
/// (their receivers may not have applied the update yet). A mismatch turns 1
/// message into 3 for the same bytes, which is the wrong direction on the
/// #4861 messages/s axis. Extend to those legs once the agreement counters
/// give a field reading.
pub(crate) fn summaries_reply_for_peer(
    op_manager: &OpManager,
    target: std::net::SocketAddr,
    entries: Vec<crate::message::SummaryEntry>,
    emitter: crate::message::SummariesEmitter,
) -> crate::message::InterestMessage {
    summaries_reply_in_form(summary_reply_form(op_manager, target), entries, emitter)
}

/// Maximum number of entries in one full-bytes summary fallback reply (#5155).
///
/// The fallback carries every shared contract's summary in a single message.
/// At the measured 265.91 shared contracts and 16,675 B per summary that is a
/// multi-megabyte message every 5 minutes, and the largest single message
/// observed in the field was 1.26 MB. Bounding it is what stops one peer we
/// cannot negotiate digests with from costing more than every peer we can.
///
/// 64 comes from tying the fallback's cost to the digest message it stands in
/// for: a digest reply over the same set is 265.91 x 21 B ~= 5.6 KB, and a
/// full-bytes entry averages 143 B, so ~39 entries is break-even. 64 spends
/// about 9.2 KB for a shorter rotation.
///
/// # This is the SECONDARY bound, and on a real fallback link it is inert
///
/// 143 B/entry is the interests_reply average across the whole fleet, which is
/// dominated by 21-byte digest entries and by entries for contracts the sender
/// does not host (those carry no summary at all). It does not describe the
/// population that takes this path. A peer whose shared set is mostly hosted
/// River rooms carries ~16.7 KB per entry, so
/// [`MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`] binds after the first hosted
/// contract and this cap is never reached.
///
/// The entry cap is therefore best read as the guard for the OPPOSITE
/// extreme — a wide set of contracts we do not host, where each entry costs
/// almost nothing and the byte budget would never bind — not as the number
/// that governs how fast the rotation turns. Quote the byte budget for that.
const MAX_FALLBACK_SUMMARIES_PER_REPLY: usize = 64;

/// Byte budget for one full-bytes summary fallback reply (#5155).
///
/// [`MAX_FALLBACK_SUMMARIES_PER_REPLY`] on its own does NOT bound the message.
/// An entry carries a whole summary and summaries are not uniform: a contract
/// we do not host contributes no summary bytes at all, while a River room
/// contributes ~16.7 KB. The 143 B/entry that the entry cap is derived from is
/// a fleet AVERAGE, so it describes the typical mix rather than the worst case,
/// and 64 heavy entries is ~1.07 MB — the message class this change exists to
/// remove, reproduced under a cap that looks like it forbids it.
///
/// 9 KiB is the same break-even target expressed in the unit that actually
/// bounds the wire (64 x 143 B ~= 9.2 KB). On a typical mix it never binds and
/// the entry cap governs; it binds exactly where the entry cap would have
/// failed.
///
/// The budget is checked BEFORE adding each entry and never blocks the first
/// one, so a reply is at most `budget + one summary` and a contract whose
/// summary alone exceeds the budget still gets sent rather than stalling the
/// rotation forever. That trade is deliberate: a single oversized summary is
/// bounded by the contract, an unbounded reply is not.
///
/// # What this actually costs, in rounds
///
/// Because the budget usually binds before the entry cap, the cycle length is
/// governed by BYTES, not by entry count:
///
/// ```text
/// rounds per cycle ~= total summary bytes for the shared set / 9 KiB
/// ```
///
/// For the heaviest links this change targets — the reconstructed ~1.16 MB
/// reply behind the 1.26 MB largest message observed in the field — that is
/// ~130 rounds at a 5-minute heartbeat, so **on the order of ten hours** to
/// come back round, not the ~25 minutes a naive `ceil(266 / 64)` reading
/// suggests. Worst case, a set where every summary exceeds the whole budget,
/// it degenerates to one contract per round.
///
/// That is the real, stated cost of this bound. It is defensible because this
/// is the backstop layer and not the delivery path — the event-driven paths
/// (push-on-update, request-full-state-when-too-far-behind, resend-on-failed-
/// patch) are untouched and still bound detection for any contract anybody
/// touches — but it must not be quoted as minutes. The alternative on those
/// same links is a multi-megabyte message every 5 minutes; there is no setting
/// of this constant that is both cheap and fast, which is the argument for
/// repairing the version gate (#5156 / #5161) rather than tuning here.
///
/// # Before retuning either constant, get the field reading
///
/// Everything above is a MODEL, derived from fleet averages. That is how the
/// entry cap came to be sized off a mean of 143 B/entry when the population it
/// governs is nothing like the mean — the error survived review of the
/// arithmetic because no counter contradicted it. **#5168** adds
/// entries-per-reply and bytes-per-reply on this path. Do not move either
/// constant on the strength of another average; read the distribution.
///
/// Those counters are also the cleanest read on #5161: as gateway links learn
/// peer versions and stop taking this path at all, they should fall toward
/// zero.
const MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY: usize = 9 * 1024;

/// Maximum number of contract summaries one periodic `Interests` reply computes
/// on the hash-first DIGEST path (#5238).
///
/// #5155 bounded the full-bytes fallback and deliberately left this path alone,
/// on the reasoning that digests are cheap to SEND. They are — 21 bytes each.
/// But an entry of either form costs one `summary_if_hosted_or_in_use` call to
/// PRODUCE, and that call is a contract-handler round trip that re-enters WASM
/// whenever the memoised state-hash misses. Bounding bytes never bounded that,
/// so once digest-capability became the common case (#5066 merged 2026-07-31,
/// floor 0.2.116) the periodic heartbeat was left looping over the WHOLE shared
/// interest set, on every message, in both directions.
///
/// That produced the third `summarize_contract_state` storm in the family
/// (#4473 -> #4610 -> #5238): a real NATed peer hosting 933 contracts over 18
/// connections sustained ~54 calls/sec against the 30/sec per-callsite log
/// limiter, resuming within seconds of every restart. It is NOT a
/// phantom-hosting recurrence — every call in that loop is a correctly-gated
/// summarize of a genuinely hosted contract (`should_summarize_or_broadcast`
/// holds), and the `Contract state not found in store` WARN that was #4610's
/// signature does not appear. The defect is that nothing bounded HOW MANY.
///
/// # Where 64 comes from
///
/// The cost bounded here is CPU, so it is sized against a CPU budget rather
/// than a message size. Per heartbeat and per peer we pay two summarize-heavy
/// legs: the reply we build here, and the peer's reply that we compare against
/// our own state in the `SummaryDigests` arm (bounded by
/// [`MAX_SUMMARY_COMPARISONS_PER_MESSAGE`]). So
///
/// ```text
/// summarize calls/sec ~= 2 x cap x connected_peers / INTEREST_HEARTBEAT_INTERVAL
/// ```
///
/// At 64 and the observed 18 connections that is ~7.7/sec — about a quarter of
/// the 30/sec per-callsite limiter these storms are measured against, and an
/// ~7x reduction from the measured 54/sec. It is also the same number as
/// [`MAX_FALLBACK_SUMMARIES_PER_REPLY`], which restores the property #5155's
/// split lost: one periodic reply costs the same whichever wire form it takes.
///
/// # What it costs in convergence
///
/// A stable shared set of `n` contracts is covered in `ceil(n / 64)` heartbeats
/// (the window rotates and wraps — see `rotation_window_indices`). At the
/// ~450-contract shared sets implied by the field measurement that is 8 rounds,
/// or **~40 minutes** for the anti-entropy backstop to come back round to a
/// given contract, against 5 minutes before. Two reasons that is affordable,
/// and one caveat:
///
/// - This is the BACKSTOP layer, not the delivery path. A committed update goes
///   to every connected advertised co-host immediately via live fan-out, with
///   no INTEREST check and untouched by this change; the event-driven repairs
///   (resend-on-failed-patch, request-full-state-when-too-far-behind) are also
///   untouched. Anti-entropy only has to catch a divergence that ALL of those
///   missed.
///
///   Read "no interest check" narrowly, because live fan-out is not
///   unconditional. It has a SUMMARY gate — `plan_fanout_send` returns `Skip`
///   when our cached belief about the peer's summary is byte-equal to ours —
///   and that gate is fed by the very cache this change warms more slowly. A
///   MISSING belief is safe in the direction that matters (see the
///   second-order cost below: it makes us send more, never skip). A WRONG
///   belief is the case this bullet must not be read as covering: after a lost
///   stream tail the sender can believe a peer has state it does not have,
///   nothing is sent, so no delta fails and the ResyncRequest repair cannot
///   fire either. For a contract that then goes quiet, anti-entropy is the
///   only correction, and this change stretches that window by roughly 8x.
///   It stays bounded, and it is confined to quiescent contracts, but it is a
///   real cost of this change rather than one the delivery path absorbs.
///   `broadcast_queue.rs` carries the same note where the belief is cached.
/// - It is far tighter than the cost already accepted on the sibling path:
///   [`MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`] documents cycles on the order of
///   ten HOURS for the heaviest full-bytes links, shipped in #5155.
/// - **The layer being slowed almost never finds anything.** Measured
///   fleet-wide over ~27,500 node-minutes: 26,457,275 summary comparisons
///   agreed against 44,305 that differed — 99.83% identical. Anti-entropy is
///   paying a continuous CPU cost to discover a divergence roughly one time in
///   six hundred, which is what makes trading its latency for that CPU the
///   right side of the deal.
///
/// # The second-order cost: the peer-summary cache warms at the same rate
///
/// Not obvious, and it lands on the majority of links rather than the minority
/// #5155's equivalent did, so it is stated here rather than left to be
/// discovered. `SummaryPopulationSource::DigestAgreement` is the only thing
/// that seeds our cached belief about a peer's summary for a QUIESCENT contract
/// on a digest link, and it now fires for at most 64 contracts per heartbeat.
/// The broadcast path sends a delta only to a peer whose summary we hold, so
/// for up to `ceil(n / 64)` heartbeats after a connection forms, the first
/// update to a not-yet-covered contract ships FULL STATE instead of a delta.
///
/// Two things bound it. The direction is safe — a missing cached summary makes
/// us send more, never skip — so no update is lost. And `Delivery` seeds the
/// cache after any successful send, so only the FIRST update per
/// (contract, peer) pays, and #5153 found 57.5% of cold-start sends fire before
/// any heartbeat could have helped anyway. It is a bytes-for-bytes trade
/// against a CPU saving, not purely a latency one.
///
/// # The caveat, and the retune candidate
///
/// This is a per-REPLY cap, so the per-NODE rate still scales with connection
/// count. A 200-connection gateway sharing 64+ contracts with every peer would
/// model to ~85 calls/sec even at this cap. Capping is monotone — it can only
/// lower what a reply computes, never raise it — so no peer is made worse off,
/// but an aggregate node-wide budget is a real remaining gap. Sizing one needs
/// the entries-per-reply distribution (#5168), not another fleet average; see
/// [`MAX_FALLBACK_SUMMARIES_PER_REPLY`] for how retuning off a mean went wrong
/// last time.
///
/// **128 is the obvious retune candidate and was deliberately not taken here.**
/// It models to ~15.4 calls/sec at 18 connections, still half the limiter, and
/// it would halve both the convergence latency and the cache-warm latency
/// above. The reason to ship 64 first is that the node this was measured on is
/// pinned at its 2 GB cgroup cap right now, so the larger margin is worth more
/// than the latency; and that the equality with
/// [`MAX_FALLBACK_SUMMARIES_PER_REPLY`] is a symmetry argument, not a budget
/// one, so it should not by itself hold the value down once #5168 gives a
/// distribution to read.
const MAX_DIGEST_SUMMARIES_PER_REPLY: usize = 64;

/// Maximum number of inbound entries one `SummaryDigests` (or `Summaries`)
/// message may COMPARE against local state (#5238), where "compare" means the
/// entries that cost us a `summary_if_hosted_or_in_use` call.
///
/// The digest exchange has two summarize-heavy legs, not one, and they are
/// independent: the sender pays [`MAX_DIGEST_SUMMARIES_PER_REPLY`] calls to
/// build the message, and the receiver pays one call per COSTED entry to
/// compute its OWN summary for the comparison. Bounding only the send side
/// would move half the storm rather than remove it, and would leave the
/// receiver exposed to any peer that has not upgraded (or is not honest)
/// sending a full set.
///
/// # It bounds CALLS, not entries (#5338)
///
/// An entry advertising no summary — `summary_digest: None`, or
/// `summary_bytes: None` on the full-bytes twin — is settled without our
/// summary being consulted at all (`classify_summary_digest`'s `(_, None)` arm;
/// the `_ => false` staleness arm on the twin), so the receiver skips the round
/// trip and does not charge it. Entry COUNT is bounded separately, by
/// [`MAX_SUMMARY_ENTRIES_PER_MESSAGE`].
///
/// # One exception, and it is deliberate: `SimulationIdleTimeout`
///
/// Under that flag (`emit_confirmed`, off in production, on for every
/// direct-runner simulation) both receive arms fetch our summary for free
/// entries too, so the round trips are bounded by
/// [`MAX_SUMMARY_ENTRIES_PER_MESSAGE`] rather than by this constant — up to 2x.
/// Bounded, not unbounded, but a different bound, so **a simulation's
/// receive-leg summarize count reads high and cannot be used to measure this
/// path's CPU cost.** Do not size a future cap from a simulation number without
/// subtracting the telemetry probes.
///
/// The exception is not an oversight and removing it is not a cleanup. The
/// `StateConfirmed` events those fetches produce feed the convergence checker
/// via `EventKind::stored_state_hash()`; without them a peer's recorded state
/// hash goes stale or the peer drops out of the check's per-contract map
/// entirely, which takes the contract below the two-peer threshold and skips it
/// SILENTLY. Trading a suite that can pass vacuously for an accurate CPU
/// number in a measurement nobody is currently taking is the wrong way round.
/// #5338 made that trade once, on a consumer search for the literal variant
/// name — the consumption goes through a generic accessor, so the name does not
/// appear at the consumers and the search came back empty.
///
/// Until #5338 this cap counted entries, which was the same number because the
/// send leg charged its own budget the same way. Once the send leg stopped
/// charging free entries its replies could exceed 64 entries, and an
/// entry-counting receiver would have truncated them back — discarding a random
/// subset of the digests the sender spent its whole budget producing, and
/// turning the send window's contiguous tiling into a random sample at the
/// receiver. The two legs charge on the same basis so that cannot happen.
///
/// Equal to [`MAX_DIGEST_SUMMARIES_PER_REPLY`] on purpose: a sender running
/// this release never COSTS us more than that, so between two upgraded peers
/// this cap never truncates and the two legs cover exactly the same contracts
/// each round. It binds only on a message from a peer that predates this
/// change.
///
/// This REPLACES [`MAX_SUMMARY_HASHES_PER_MESSAGE`] (4096) as the WORK bound on
/// this arm. That constant is unchanged and still bounds the `SummaryRequest`
/// input, where it remains the right anti-abuse ceiling.
///
/// Over-ceiling messages are still processed from a random offset (the existing
/// `rotate_left`), so truncating never starves the tail of a set the sender
/// happens to emit in contract-id order.
///
/// # What truncation costs, when it binds
///
/// The random offset is a fresh uniform draw each round rather than the
/// id-keyed cursor the send side uses, so coverage of an over-cap sender's set
/// is coupon-collector rather than `ceil(n / 64)`: about `(n / 64) * H_n`
/// rounds, ~43 rounds (a few hours) for n = 450, against 8. That is worth
/// knowing and is NOT the same guarantee the send side gives.
///
/// It is nevertheless the right trade, for a reason specific to who can reach
/// it. Only a pre-#5238 sender emits an over-cap message, so this binds during
/// a rollout and then stops. In that window the degraded direction is only "we
/// notice that IT is stale"; the old peer's own view is repaired by our SEND
/// leg, which is cursor-rotated and covers in `ceil(n / 64)`, and which the old
/// peer compares in full because it has no cap. So the un-upgraded peer
/// converges on the normal schedule and only our detection of its staleness is
/// a slower random walk. Giving this leg a cursor too would need the truncated
/// set sorted and a second cursor cache keyed the same way; worth doing if a
/// long-lived mixed-version population ever becomes normal, and not worth the
/// machinery for a rollout window.
const MAX_SUMMARY_COMPARISONS_PER_MESSAGE: usize = MAX_DIGEST_SUMMARIES_PER_REPLY;

/// Absolute ceiling on the number of ENTRIES one periodic summary exchange
/// carries or processes, in either wire form (#5338).
///
/// Distinct from [`MAX_DIGEST_SUMMARIES_PER_REPLY`] /
/// [`MAX_SUMMARY_COMPARISONS_PER_MESSAGE`], which bound the expensive thing (a
/// `summary_if_hosted_or_in_use` call). Since #5338 an entry that costs no such
/// call is not charged against those, so they no longer bound the entry count
/// on their own and something else has to: entries still cost wire bytes on the
/// way out and a `lookup_by_hash` plus a `clear_peer_summary` on the way in.
///
/// **Why it is 2x rather than 1x.** At 1x this ceiling would bind before the
/// summarize budget did and the #5338 fix would be a no-op — a reply would
/// still stop at 64 entries however few of them were hosted. 2x lets the window
/// walk past a shared set that is up to half not-hosted-by-us and still fill
/// all 64 summarize slots with contracts we can actually advertise. Past that
/// point the fix degrades gracefully rather than failing: a set that is 70%
/// not-hosted still gets ~38 costed entries per round against the ~19 it gets
/// today.
///
/// **Why not 4x**, which was the first value here and buys a fully-filled
/// budget out to 75% not-hosted. Every entry past the summarize budget is a
/// free rider on two things worth rationing, and 4x quadruples both where 2x
/// doubles them:
///
/// - On the RECEIVE leg each free entry drives a `clear_peer_summary`, and per
///   the #4952 note there that turns the peer from a delta target into a
///   full-state broadcast target for that contract. Full-state broadcast volume
///   is exactly what #5153 is investigating right now, so this is a lever on a
///   number somebody is actively trying to bring down.
/// - A peer below [`HASH_FIRST_SUMMARIES_MIN_VERSION`] has no receive-side cap
///   at all and summarizes every entry it is sent (see the mixed-version note
///   below), so the ceiling is the whole of its per-message cost.
///
/// # The adversarial case, in numbers
///
/// State it explicitly, because this ceiling is the ONLY thing bounding it and
/// the honest case above is not the one that decides the value.
///
/// A hostile or broken peer's message is truncated to this ceiling, so the
/// worst FULLY-PROCESSED message is 128 entries. Of those:
///
/// - **Summarize round trips: at most 64, unchanged by #5338.**
///   [`MAX_SUMMARY_COMPARISONS_PER_MESSAGE`] still caps them, and free entries
///   cannot consume that budget because they make no call. (The full-bytes arm
///   charges per ENTRY, so a hash collision can still make one entry fetch for
///   several contracts — a pre-existing property, noted at that loop.)
/// - **`clear_peer_summary` calls: at most 128, up from 64.** A message of
///   nothing but free entries reaches the summarize budget never, so every one
///   of them is processed. That is the 2x, and it is a 2x on the lever
///   described above; at 4x it would have been 256.
///
/// **`break` instead of `continue` past the budget would not help**, which is
/// worth recording so the next reader does not reach for it as the cheap fix. A
/// message of pure free entries never reaches the budget at all, so the `break`
/// would never fire and the count would be identical. Charging free entries
/// against the budget would bound it — and would also delete the fix. Lowering
/// the ceiling is the only lever that moves this number, which is the second
/// reason it is 2x rather than 4x.
///
/// The benefit above 2x is speculative — nobody has measured the not-hosted
/// fraction of a real shared set — while the cost above 2x is concrete and
/// lands on a number #5153 is actively measuring. Raise it when there IS a
/// measurement (#5168's entries-per-reply distribution is the natural one),
/// which is a one-line change that
/// `an_all_free_window_stops_at_the_entry_ceiling` will notice.
///
/// A digest entry is 21 bytes, so 128 of them is ~2.7 KB, well inside the 9 KiB
/// the full-bytes path already spends per reply
/// ([`MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`]).
///
/// **It is deliberately the same number on both legs.** The receive leg
/// truncates an over-ceiling message (random offset, as its siblings do), so a
/// sender that respects this ceiling is never truncated by a receiver that
/// enforces it. Lower it on the receive side alone and the send window's
/// `ceil(n / 64)` tiling silently becomes a random sample at the receiver —
/// which is the class of "narrower than advertised" defect #5338 is about.
///
/// # Mixed versions
///
/// The two legs have DIFFERENT baselines in the shipped release, and an earlier
/// version of this comment got that wrong by treating them as one. In
/// v0.2.127's `Interests` arm the window is `None` for `Digests` and
/// `Some(MAX_FALLBACK_SUMMARIES_PER_REPLY)` for `FullBytes` (#5164), so:
///
/// - **A peer receiving DIGESTS** (at or above [`HASH_FIRST_SUMMARIES_MIN_VERSION`],
///   which is most of the fleet) is sent the ENTIRE shared set today — hundreds
///   of entries, the storm #5238 exists to stop. For that population this
///   ceiling is a large reduction whatever it is set to.
/// - **A peer receiving FULL BYTES** (below that floor) is already sent at most
///   `MAX_FALLBACK_SUMMARIES_PER_REPLY` = 64 entries today, and does not skip
///   the free ones on receipt. For that population this ceiling is an INCREASE,
///   up to 2x, in the number of summarize round trips one reply can cost it.
///
/// That second bullet is a real regression for that population, and it is the
/// honest reason the ceiling is 2x rather than 4x.
///
/// **It COULD be version-gated away, and is not.** An earlier revision claimed
/// gating was impossible because the entries pushing a reply past 64 are
/// precisely the free ones, so withholding them would withhold their
/// `PeerHasNoState` repairs. That reasoning is measured against the NEW
/// behaviour and is wrong against the shipped one: v0.2.127's full-bytes window
/// counts ENTRIES, not costed entries, so such a peer already receives 64
/// entries of which some are free today. Gating `FullBytes` back to
/// [`MAX_FALLBACK_SUMMARIES_PER_REPLY`] would restore exactly today's behaviour
/// for it and drop no repair it currently gets.
///
/// So this is a judgement rather than an impossibility, and the judgement is:
/// one uniform ceiling, because a version-conditional bound adds a second thing
/// to reason about in code whose invariants have already needed correcting
/// twice, and because the size of the affected population is **unmeasured** —
/// `hash_first_declined_pre_floor` and `hash_first_declined_unknown_version` in
/// `connection_manager.rs` would answer it. If that measurement ever shows the
/// pre-floor population is not negligible, gate it; do not assume it is small
/// because this comment is short.
const MAX_SUMMARY_ENTRIES_PER_MESSAGE: usize = 2 * MAX_DIGEST_SUMMARIES_PER_REPLY;

/// The send ceiling must not exceed the receive ceiling, or our own replies
/// would be truncated by an upgraded peer — see
/// [`MAX_SUMMARY_ENTRIES_PER_MESSAGE`]. Trivially true while one constant
/// serves both; the assertion is here so that splitting it into two later
/// cannot silently invert the inequality.
const _: () = assert!(
    MAX_SUMMARY_ENTRIES_PER_MESSAGE >= MAX_DIGEST_SUMMARIES_PER_REPLY
        && MAX_SUMMARY_ENTRIES_PER_MESSAGE >= MAX_FALLBACK_SUMMARIES_PER_REPLY,
    "the per-message entry ceiling must leave room for a full summarize budget, \
     or the budget can never be spent"
);

/// The `Summaries` receive-leg cap is documented (see its comment above) as
/// never binding against a peer running #5155 or later, because such a peer
/// caps its own `InterestsReply` fallback at
/// [`MAX_FALLBACK_SUMMARIES_PER_REPLY`]. That is a relationship BETWEEN two
/// independently-motivated constants, not a property of either one: the
/// fallback cap has its own byte-budget rationale, and raising it alone would
/// start silently truncating legitimate replies from upgraded peers with no
/// test failing. The alias above keeps the 64 -> 128 retune this module
/// actively invites self-consistent; this assertion is what keeps the OTHER
/// side of the inequality honest.
///
/// Since #5338 both sides of this inequality count SUMMARIZE CALLS rather than
/// entries, so what it now forbids is an upgraded sender costing an upgraded
/// receiver more comparisons than the receiver will make. The entry-count
/// analogue is asserted separately, on
/// [`MAX_SUMMARY_ENTRIES_PER_MESSAGE`].
const _: () = assert!(
    MAX_FALLBACK_SUMMARIES_PER_REPLY <= MAX_SUMMARY_COMPARISONS_PER_MESSAGE,
    "raising MAX_FALLBACK_SUMMARIES_PER_REPLY above MAX_SUMMARY_COMPARISONS_PER_MESSAGE \
     would make the Summaries receive-leg cap truncate replies from #5155+ peers"
);

/// Which wire form a multi-entry `Summaries` reply to a peer takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SummaryReplyForm {
    /// Peer is at or above the hash-first floor: 21-byte digests, bounded and
    /// rotated by entry COUNT (#5238 — #5155 left this form unbounded because
    /// it was bounding bytes, and digests are 21 bytes each; what stormed was
    /// the summarize call each entry costs to produce).
    Digests,
    /// Peer is pre-floor, or its version is unknown: full summary bytes,
    /// bounded and rotated by entry count AND by bytes (#5155).
    FullBytes,
}

/// Resolve the reply's wire form ONCE, so the size decision and the encoding
/// decision cannot disagree.
///
/// The caller needs the answer twice — to decide how many entries to build, and
/// to encode them — and reading the version gate twice would open a window in
/// which a reconnect changes it in between. The dangerous direction is known ->
/// unknown: `record_remote_version` writes `None` THROUGH on the
/// joiner->gateway path (`ring/connection_manager.rs`), clearing the entry, so
/// a re-read could build the FULL entry set believing it was headed for digests
/// and then encode it as full bytes. That is precisely the unbounded message
/// this change removes, reassembled from two correct-looking halves. One read,
/// threaded through.
pub(crate) fn summary_reply_form(
    op_manager: &OpManager,
    target: std::net::SocketAddr,
) -> SummaryReplyForm {
    if op_manager
        .ring
        .connection_manager
        .supports_hash_first_summaries(target)
    {
        SummaryReplyForm::Digests
    } else {
        SummaryReplyForm::FullBytes
    }
}

/// Encode `entries` in an already-resolved [`SummaryReplyForm`].
///
/// Split out of [`summaries_reply_for_peer`] so a caller that must know the
/// form in advance (to bound what it builds) can reuse the one decision rather
/// than taking a second, possibly different, reading of the gate.
pub(crate) fn summaries_reply_in_form(
    form: SummaryReplyForm,
    entries: Vec<crate::message::SummaryEntry>,
    emitter: crate::message::SummariesEmitter,
) -> crate::message::InterestMessage {
    use crate::message::{InterestMessage, SummaryDigestEntry};

    match form {
        SummaryReplyForm::Digests => {
            crate::config::GlobalTestMetrics::record_summary_digest_msg();
            InterestMessage::SummaryDigests {
                entries: entries.iter().map(SummaryDigestEntry::from_entry).collect(),
                emitter,
            }
        }
        SummaryReplyForm::FullBytes => full_summaries_message(entries, emitter),
    }
}

/// Build a full-bytes [`InterestMessage::Summaries`] and record the summary
/// payload it puts on the wire (#4965).
///
/// **This is the only way production code may construct a `Summaries`**, and
/// `no_uninstrumented_full_summaries_construction` pins that. The falsifier
/// for this whole change is "`summary_full_bytes() == 0` means not one summary
/// byte was sent"; a construction site that bypassed the counter would make
/// that reading silently false rather than merely untested. Taking the
/// recording inside the constructor makes the bypass unrepresentable instead
/// of discouraged — the same reasoning that moved the per-message dedup inside
/// `record_summary_comparison`.
pub(crate) fn full_summaries_message(
    entries: Vec<crate::message::SummaryEntry>,
    emitter: crate::message::SummariesEmitter,
) -> crate::message::InterestMessage {
    // Only the summaries themselves, not the enclosing bincode framing: the
    // framing is the same handful of bytes in both wire forms, and including
    // it would blur the one number the falsifier rests on.
    let payload_bytes: u64 = entries
        .iter()
        .filter_map(|e| e.summary_bytes.as_ref())
        .map(|b| b.len() as u64)
        .sum();
    crate::config::GlobalTestMetrics::record_summary_full_msg(payload_bytes);
    crate::message::InterestMessage::Summaries { entries, emitter }
}

/// Emit targeted `SyncStateToPeer` heals for the contracts on which `source`
/// was found stale, bounded by [`stale_sync_emit_budget`].
///
/// Extracted from the `Summaries` arm so the `SummaryDigests` arm shares the
/// IDENTICAL heal path rather than growing a second copy that can drift. The
/// hash-first exchange must not cost convergence, so there is exactly one
/// implementation of "we decided this peer is stale, now fix it".
///
/// `peer_key` is the stable identity of `source`, carried in solely so the
/// shadow-mode futile-repair detector can attribute the attempt to the same
/// (contract, peer) edge the outcome is later observed on
/// (`crate::ring::futile_repair`). It gates nothing: a `None` here suppresses
/// only the diagnostic recording, never a heal.
async fn emit_stale_peer_syncs(
    op_manager: &Arc<OpManager>,
    source: std::net::SocketAddr,
    peer_key: Option<&crate::ring::interest::PeerKey>,
    mut stale_contracts: Vec<freenet_stdlib::prelude::ContractKey>,
) {
    // #3798 Gap 1: cap the number of SyncStateToPeer events emitted per
    // message so a peer diverging on many contracts cannot trigger an
    // unbounded burst in one handler call. `emit_budget` bounds *emitted*
    // events (not loop iterations) — banned and no-local-state contracts are
    // skipped without consuming the budget. Overflow is not dropped
    // permanently: each later heartbeat re-derives the still-stale set from
    // the durable summary comparison and syncs the next batch (see
    // MAX_STALE_SYNCS_PER_SUMMARIES rustdoc for the eventual-consistency
    // argument).
    let total_stale = stale_contracts.len();
    let emit_budget = stale_sync_emit_budget(total_stale);
    // Starvation avoidance: when the stale set exceeds the cap, rotate the
    // start of the iteration by a random offset so the cap window slides
    // across the whole set over successive cycles. Without this, a contract
    // stuck in the leading `cap` positions (dropped emit, lost packet, peer
    // fails to apply) would re-consume the budget every cycle and permanently
    // starve everything past the cap. No rotation when total_stale <= cap —
    // every contract is emitted anyway, so the order does not matter.
    // GlobalRng keeps this deterministic under simulation/test.
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
        // Phase 7 egress gate. Don't repair a stale peer's summary mismatch by
        // pushing state for a contract we have banned — same rationale as the
        // inbound wire-boundary drop, applied to the proactive heal path.
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
        // Count this contract against the emit budget: it has local state and
        // is not banned, so we are about to emit a SyncStateToPeer event for
        // it. Increment before the emit so a channel-full drop still consumes
        // the budget — the dropped event is retried next cycle exactly like an
        // over-cap one.
        emitted += 1;
        // SHADOW MODE (futile-repair detector, `crate::ring::futile_repair`).
        // Recorded HERE — past the ban and no-local-state gates and inside the
        // budget — because those three skips mean no repair was sent. Charging
        // the edge for a heal we never emitted would make the detector measure
        // our own emit budget rather than whether repair works. Pure
        // accounting: no gate, no early return, no behaviour change.
        if let Some(pk) = peer_key {
            op_manager
                .interest_manager
                .record_repair_attempt(&contract, pk);
        }
        // Fires per stale-peer detection during interest sync, which is
        // dominant on hot contracts. Diagnostic-grade rather than
        // user-actionable; keep accessible via RUST_LOG=…=debug.
        tracing::debug!(
            contract = %contract,
            stale_peer = %source,
            "Summary mismatch in interest sync — syncing state to stale peer"
        );
        // Non-blocking emit: SyncStateToPeer is best-effort gossip — if
        // dropped, the next interest-sync round will retry. Blocking here
        // would stack the heal path on the same notification channel the
        // executor is trying to keep responsive (#4145 / #4234).
        // Targeted heal: `stale_peer_sync_event` builds a `SyncStateToPeer`
        // aimed at exactly `source`, NEVER an all-subscriber fan-out. Pinned
        // by `stale_peer_sync_event_is_targeted_not_broadcast` (#3791/#3796).
        if let Err(e) =
            op_manager.try_notify_node_event(stale_peer_sync_event(contract, state, source))
        {
            // Best-effort by design (see comment above); log at debug to keep
            // the caller layer in step with the helper-internal downgrade
            // (#4238).
            tracing::debug!(
                contract = %contract,
                error = %e,
                "Failed to emit SyncStateToPeer for stale peer correction (best-effort)"
            );
        }
    }
}

/// Handle incoming InterestSync messages for delta-based state synchronization.
///
/// This function processes the interest exchange protocol:
/// - `Interests`: Connection-time discovery of shared contract interests
/// - `Summaries`: State summaries for shared contracts
/// - `SummaryDigests`: hash-first advertisement of those same summaries (#4965)
/// - `SummaryRequest`: the bytes-on-mismatch follow-up to `SummaryDigests`
/// - `ChangeInterests`: Incremental interest changes
/// - `ResyncRequest`: Request full state when delta application fails
async fn handle_interest_sync_message(
    op_manager: &Arc<OpManager>,
    source: std::net::SocketAddr,
    message: crate::message::InterestMessage,
) -> Option<crate::message::InterestMessage> {
    use crate::message::{InterestMessage, SummariesEmitter, SummaryEntry};
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
                        op_manager.interest_manager.remove_peer_interest_for(
                            contract,
                            pk,
                            crate::ring::interest::InterestRemovalCause::InterestsReplace,
                        );
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

            // #5155: resolve the wire form BEFORE building anything, and take
            // exactly one reading of the version gate (see
            // `summary_reply_form`). The form still decides the ENCODING and
            // whether a byte budget applies; since #5238 it no longer decides
            // WHETHER the reply is bounded — both forms are.
            let form = summary_reply_form(op_manager, source);

            // Positions in `matching` whose summaries this reply carries, in
            // the order they are charged against the caps.
            //
            // A bounded window that rotates from where the last reply to this
            // peer stopped, so successive rounds cover the whole set instead of
            // re-sending the same prefix forever. The resume point is derived
            // from the last contract id SENT rather than a stored index, which
            // is what makes it survive contracts being added and removed
            // between rounds — see `first_index_after`.
            //
            // #5238: the digest path is windowed too. #5155 left it whole
            // because it was bounding BYTES and a digest is 21 of them, but
            // every entry of either form costs one `summary_if_hosted_or_in_use`
            // call to produce, and that call is what stormed. Only the LIMIT
            // differs by form, and only the full-bytes path additionally
            // charges a byte budget on top (see the loop below).
            //
            // #5338: the cap counts entries that COST a summarize call, and the
            // window is walked up to `MAX_SUMMARY_ENTRIES_PER_MESSAGE`
            // candidate positions to fill it. `matching` comes from
            // `contract_hash_index`, which peer interest registrations populate
            // as well as our own hosting, so it contains contracts we track but
            // do NOT host; those return `None` through the in-memory gate
            // without any contract-handler round trip. Charging them a slot
            // spent a CPU budget on entries that cost no CPU, so convergence
            // latency for the contracts we can actually advertise scaled with
            // `|matching|` while the cost being bounded scaled only with the
            // hosted subset.
            let summarize_cap = match form {
                SummaryReplyForm::Digests => MAX_DIGEST_SUMMARIES_PER_REPLY,
                SummaryReplyForm::FullBytes => MAX_FALLBACK_SUMMARIES_PER_REPLY,
            };
            // #5338: keyed by the peer's stable transport key, not by the
            // address it happens to be reaching us from. Without a resolvable
            // key there is no identity to rotate against, so the reply takes a
            // fresh random offset and records nothing — the same degraded mode
            // an evicted cursor gets, and it only arises for a source we have
            // no connection entry for, which is also a source whose interest we
            // do not register above.
            let window = {
                let start = peer_key.as_ref().map_or_else(
                    || {
                        if matching.is_empty() {
                            0
                        } else {
                            crate::config::GlobalRng::random_range(0..matching.len())
                        }
                    },
                    |pk| {
                        op_manager
                            .interest_manager
                            .summary_window_start(pk, &matching)
                    },
                );
                crate::ring::interest::rotation_window_indices(
                    matching.len(),
                    start,
                    MAX_SUMMARY_ENTRIES_PER_MESSAGE,
                )
            };

            // Register/refresh peer interest for EVERY shared contract, not just
            // the ones this round summarises. The rotation bounds what we
            // ADVERTISE; interest bookkeeping decides who is a viable broadcast
            // target, and rotating it would drop this peer out of the broadcast
            // set for whatever fell outside the window — turning a bandwidth
            // bound into missed updates.
            if let Some(ref pk) = peer_key {
                for contract in &matching {
                    // Refresh TTL for existing entries (preserves cached summary).
                    // Only register new interest if this is a genuinely new entry;
                    // otherwise register_peer_interest would overwrite the cached
                    // summary with None, defeating delta optimization.
                    // One acquisition via the refresh's own bool — see
                    // `InterestManager::refresh_peer_interest` for why the
                    // `get_peer_interest(..).is_some()` form it replaces was both
                    // expensive (it clones the cached summary) and racy. This runs
                    // per shared contract per heartbeat, so the clone was not free.
                    if !op_manager
                        .interest_manager
                        .refresh_peer_interest(contract, pk)
                    {
                        let is_new = op_manager.interest_manager.register_peer_interest_from(
                            contract,
                            pk.clone(),
                            None, // New entry; summary arrives in their Summaries response
                            false,
                            crate::ring::interest::InterestRegistrationSource::Interests,
                        );
                        if is_new {
                            // #4359 (MUST-FIX 1): an Interests-sync registration
                            // makes this peer a viable broadcast target. Flush
                            // any deferred fresh-contract broadcast so a cold-id
                            // PUT that gave up with no targets reaches it.
                            op_manager
                                .flush_pending_broadcast_on_interest(contract)
                                .await;
                        }
                    }
                }
            }

            // Build the summaries this reply carries, in rotation order.
            // Sized to the WINDOW, not the summarize budget: the vec holds the
            // free entries too, so `summarize_cap` would under-reserve by up to
            // the ceiling and reallocate mid-reply (#5338 review D).
            let mut entries = Vec::with_capacity(window.len());
            let mut summary_bytes_used = 0usize;
            let mut last_included: Option<ContractInstanceId> = None;
            // Entries that cost a `summary_if_hosted_or_in_use` round trip.
            // This — not `entries.len()` — is what the cap bounds (#5338).
            let mut summarized = 0usize;
            for &index in &window {
                // Checked BEFORE the entry rather than after, so the loop stops
                // on the round trip that would breach the budget instead of
                // making it first. A free entry never trips it, which is the
                // whole point; free entries trailing the last charged one are
                // simply not reached, keeping the message no larger than the
                // budget needs it to be.
                if summarized >= summarize_cap {
                    break;
                }
                let contract = matching[index];
                // #5155 byte budget. The check is RETROSPECTIVE — it asks
                // whether the budget is already spent, not whether this entry
                // would breach it — which is what guarantees the first entry
                // always goes out and a summary larger than the whole budget
                // cannot stall the rotation behind it. `entries.is_empty()` is
                // belt-and-braces: the accumulator only grows once an entry is
                // pushed, so it is implied today, and it is here so that a
                // future edit charging bytes for skipped entries cannot turn
                // this into a reply that carries nothing. See
                // `MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`.
                if form == SummaryReplyForm::FullBytes
                    && !entries.is_empty()
                    && summary_bytes_used >= MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY
                {
                    break;
                }
                let hash = contract_hash(&contract);
                // Only summarize contracts we host or actively serve; phantom
                // peer-interest contracts (no state, no live subscriber) have
                // nothing to advertise and their pointless GetSummaryQuery
                // round-trips were the dominant #4473 storm. See
                // `summary_if_hosted_or_in_use`.
                let probe = summary_if_hosted_or_in_use(op_manager, &contract).await;
                // Charged from the PROBE's own report of what it did, never
                // re-derived from whether a summary came back: a round trip
                // that ran and returned nothing cost exactly as much as one
                // that returned bytes, and must be charged.
                if probe.summarized {
                    summarized += 1;
                }
                summary_bytes_used += probe.summary.as_ref().map_or(0, |s| s.as_ref().len());
                entries.push(SummaryEntry::from_summary(hash, probe.summary.as_ref()));
                last_included = Some(*contract.id());
            }

            // Advance the rotation to what was actually SENT. A budget cut
            // short of the selected window must not advance past the entries it
            // dropped, or they wait a full cycle instead of coming next.
            //
            // Two known imprecisions here, both accepted, both costing at most
            // one cycle of delay for the affected contracts and neither able to
            // skip one permanently (the window wraps, and a cycle boundary
            // restarts at a random offset):
            //
            // - This advances on send ATTEMPT. The reply is handed to the
            //   connection afterwards and a send failure is only logged, so a
            //   dropped reply costs those contracts a cycle. Making it
            //   delivery-driven would need an ack this exchange does not have.
            // - Two `Interests` from the same peer handled concurrently both
            //   read the same start and build the same window, losing one
            //   round of progress. Reserving the window at read time instead
            //   would trade that for a worse failure: a budget-cut round would
            //   then skip everything it did not reach.
            //
            // #5238: recorded for BOTH forms now. Under #5155 only the
            // full-bytes path rotated, so only it had a cursor to advance.
            //
            // #5338: recorded against the peer's KEY. Keying this by address
            // discarded the cursor whenever a NATed peer resumed on a new
            // source port, which sent that peer's rotation back to a random
            // offset every reconnect.
            if let (Some(pk), Some(last)) = (peer_key.as_ref(), last_included) {
                op_manager.interest_manager.record_summary_cursor(pk, last);
            }

            if entries.is_empty() {
                None
            } else {
                // #5052: the heartbeat reply — MULTI-entry, one per shared
                // advertised contract, on every `Interests` received. #4965
                // is the change #5052 anticipated here: this emitter now sends
                // DIGESTS to a capable peer, and the tag rides the full-bytes
                // fallback so the per-emitter rollup keeps attributing it.
                // #5155 bounds the full-bytes side; `form` is the single gate
                // reading both that bound and this encoding are taken from.
                Some(summaries_reply_in_form(
                    form,
                    entries,
                    SummariesEmitter::InterestsReply,
                ))
            }
        }

        InterestMessage::Summaries { entries, .. } => {
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

            // Cloned rather than moved so `peer_key` survives for the
            // `emit_stale_peer_syncs` call below, which needs the peer's stable
            // identity to attribute the heal to a futile-repair edge. A
            // `PeerKey` wraps an x25519 `PublicKey` — a 32-byte `Copy` — so
            // this is a byte copy, not a key operation, and it happens once per
            // InterestSync message.
            if let Some(pk) = peer_key.clone() {
                // Per-message budget for semantic-staleness WASM probes
                // (`get_state_delta`), spent across ALL entries/contracts in
                // this Summaries message. Bounds the DoS surface of a peer
                // sending novel summary bytes for every hosted contract. See
                // `MAX_STALENESS_PROBES_PER_SUMMARIES` / `plan_staleness_probe`.
                let mut staleness_probes_used = 0usize;
                // Contracts already counted by the #4965 summary-comparison
                // measurement in THIS message. Scoped to one message, so it
                // cannot accumulate across a connection. Its size is bounded by
                // the number of DISTINCT locally-known contracts the entries
                // resolve to — `lookup_by_hash` only yields contracts this node
                // already tracks, so a peer cannot grow it with unknown ids —
                // and it holds contract ids only, no state.
                let mut compared_contracts: HashSet<ContractInstanceId> = HashSet::new();
                // Separate dedup set from `compared_contracts`: a contract is
                // either a two-sided comparison or a one-sided observation,
                // never both in one message, and sharing one set would let the
                // first kind silence the second.
                let mut one_sided_counted: HashSet<ContractInstanceId> = HashSet::new();
                // R4b instrument (#5153). THIS is the arm that reads `p`
                // BACKWARD: a proactive summary notification lands here today as
                // a single-entry FULL-BYTES `Summaries`, and the loop below
                // already performs exactly the comparison a 21-byte digest would
                // have made. So classifying by message shape measures the
                // notification leg's agreement rate on real production traffic
                // before R4b ships anything — no wire change, no new variant, no
                // version gate. See `OutboundMix::record_summary_comparison` for
                // why shape is the only available discriminator (the emitter tag
                // is `#[serde(skip)]`) and for the measured contamination.
                //
                // Computed BEFORE the loop consumes `entries`, and before the
                // #5238 truncation below, so it classifies the length of the
                // message the peer actually sent rather than the windowed
                // remainder. Same expression as the `SummaryDigests` arm's, so
                // the two legs stay comparable.
                let single_entry = entries.len() == 1;

                // #5238: bound this leg too. It is the full-bytes twin of the
                // `SummaryDigests` arm and pays the same per-entry summarize
                // cost, with neither that arm's per-message memo nor its pair
                // dedup, so it was the cheapest remaining path in the family
                // once the other three were bounded — the same argument the
                // `SummaryRequest` arm records.
                //
                // It never binds on a peer running #5155 or later: an
                // `InterestsReply` fallback is capped at
                // `MAX_FALLBACK_SUMMARIES_PER_REPLY` entries and a
                // `SummaryRequestReply` answers a request we ourselves capped
                // at `MAX_SUMMARY_COMPARISONS_PER_MESSAGE`. Random offset
                // rather than a prefix, for the tail-starvation reason the
                // sibling arms document.
                //
                // #5338: the ENTRY ceiling and the SUMMARIZE budget are two
                // different numbers now, for the reason the `SummaryDigests`
                // arm states at length — an entry carrying no summary bytes
                // reaches the `_ => false` staleness arm and a
                // `clear_peer_summary` without our summary being needed, so it
                // costs no round trip and must not be charged one.
                let mut entries = entries;
                if entries.len() > MAX_SUMMARY_ENTRIES_PER_MESSAGE {
                    let start = crate::config::GlobalRng::random_range(0..entries.len());
                    entries.rotate_left(start);
                    entries.truncate(MAX_SUMMARY_ENTRIES_PER_MESSAGE);
                }
                // Charged per ENTRY rather than per contract, as the pre-#5338
                // cap was: a hash collision makes one entry fetch for several
                // contracts, so this is the same slight under-count it always
                // had, bounded by the collision rate rather than by anything a
                // peer chooses.
                let mut charged = 0usize;
                for entry in entries {
                    let costs_a_summarize = entry.summary_bytes.is_some();
                    if costs_a_summarize {
                        if charged >= MAX_SUMMARY_COMPARISONS_PER_MESSAGE {
                            // See the sibling arm: free entries past the budget
                            // still carry their repair and still cost nothing.
                            continue;
                        }
                        charged += 1;
                    }
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
                        //
                        // #5338: skipped outright when the peer sent no summary
                        // bytes — the comparison below cannot reach a two-sided
                        // arm, so our summary could only ever have been
                        // discarded. `emit_confirmed` (simulation only) keeps
                        // paying it so the convergence checker still receives a
                        // `StateConfirmed` for these contracts — see the
                        // `SummaryDigests` twin for the consumer chain, the
                        // vacuous-pass failure mode it prevents, and why a
                        // search for the variant name wrongly reports it unused.
                        let our_summary = if costs_a_summarize || emit_confirmed {
                            summary_if_hosted_or_in_use(op_manager, &contract)
                                .await
                                .summary
                        } else {
                            None
                        };

                        if emit_confirmed {
                            if let Some(ref summary) = our_summary {
                                confirmed_states.push((contract, hex::encode(summary.as_ref())));
                            }
                        }

                        // Semantic staleness (#4857 secondary finding / the
                        // 2.56M `summarize_contract_state` storm). A raw byte
                        // comparison of summaries is WRONG: a contract whose
                        // summary serializes non-deterministically (HashMap /
                        // HashSet order) yields different summary bytes for the
                        // SAME logical state across peers, so a byte compare
                        // flags a converged peer stale and fires a full-state
                        // heal every heartbeat. Only pay the (bounded, cached)
                        // contract delta probe when the summaries actually
                        // differ byte-wise; ask the contract itself whether we
                        // hold state the peer lacks. See
                        // `summary_indicates_stale_peer`.
                        // SHADOW MODE (futile-repair detector). What the
                        // `is_stale` below actually RESTS ON. `is_stale` is the
                        // right heal decision in all three cases, but only one
                        // of them is evidence about convergence: the other two
                        // default to stale with nothing behind them, and their
                        // frequency grows with peer breadth and node load
                        // rather than with brokenness. Tracked alongside rather
                        // than re-derived afterwards, because only the branch
                        // that TOOK the default knows it took one. See
                        // `crate::ring::futile_repair::OutcomeEvidence`.
                        let mut evidence = crate::ring::futile_repair::OutcomeEvidence::Verdict;
                        let is_stale = match (our_summary.as_ref(), their_summary.as_ref()) {
                            (Some(ours), Some(theirs)) => {
                                let identical = ours.as_ref() == theirs.as_ref();
                                // #4965 falsifier: how often are the two sides
                                // byte-identical? That fraction is exactly what a
                                // hash-first `Summaries` exchange would save, since
                                // `SummaryEntry` ships full summary bytes
                                // unconditionally today. Recorded here rather than
                                // at the send site because only the receiver can
                                // compare, and only in this arm because a `None` on
                                // either side is not a comparison at all.
                                //
                                // Counted once per contract per message: `entries`
                                // is peer-supplied and may repeat a hash, and
                                // without this a peer could inflate either bucket
                                // at will — corrupting the very ratio that decides
                                // whether the wire change is worth building. The
                                // dedup guards ONLY the measurement; the staleness
                                // logic below still runs per entry exactly as
                                // before, so this changes no behavior.
                                op_manager.outbound_mix.record_summary_comparison(
                                    contract.id(),
                                    ours.as_ref(),
                                    theirs.as_ref(),
                                    crate::node::network_bridge::outbound_message_mix::
                                        SummaryObservation::full_bytes(single_entry),
                                    &mut compared_contracts,
                                );
                                let delta_verdict = if identical {
                                    // Byte-identical => converged; skip the probe.
                                    None
                                } else {
                                    // Cheap in-memory cache lookup first (no WASM,
                                    // no budget). Only a genuine cache MISS needs a
                                    // WASM `get_state_delta`, so only misses are
                                    // rationed by the per-message probe budget.
                                    let cached =
                                        op_manager.interest_manager.cached_staleness_verdict(
                                            &contract,
                                            theirs.as_ref(),
                                            ours.as_ref(),
                                        );
                                    match plan_staleness_probe(cached, staleness_probes_used) {
                                        StalenessProbeAction::UseCached(has_change) => {
                                            Some(has_change)
                                        }
                                        StalenessProbeAction::RunProbe => {
                                            staleness_probes_used += 1;
                                            let verdict = op_manager
                                                .interest_manager
                                                .peer_summary_has_pending_state(
                                                    op_manager, &contract, theirs, ours,
                                                )
                                                .await;
                                            if verdict.is_none() {
                                                // The probe ran and produced no
                                                // answer (delta error, timeout,
                                                // unexpected response), so the
                                                // byte-compare default below is
                                                // not a convergence verdict.
                                                evidence = crate::ring::futile_repair::
                                                    OutcomeEvidence::ProbeUnavailable;
                                            }
                                            verdict
                                        }
                                        StalenessProbeAction::BudgetExhaustedFallBack => {
                                            // Budget spent for this message: fall
                                            // back to the conservative byte-compare
                                            // (differing bytes => stale => heal,
                                            // capped by MAX_STALE_SYNCS_PER_SUMMARIES).
                                            // Re-evaluated next heartbeat once the
                                            // cache warms. `None` => byte-compare in
                                            // `summary_indicates_stale_peer`.
                                            //
                                            // This is the load-correlated channel:
                                            // everything past the 32-probe budget
                                            // reads as stale every round with no
                                            // divergence at all, so it must never
                                            // reach the futile-repair detector as a
                                            // verdict.
                                            evidence = crate::ring::futile_repair::
                                                OutcomeEvidence::ProbeBudgetExhausted;
                                            None
                                        }
                                    }
                                };
                                crate::ring::interest::summary_indicates_stale_peer(
                                    ours,
                                    theirs,
                                    delta_verdict,
                                )
                            }
                            // One side has no summary => no basis to heal
                            // (unchanged from the prior `.zip()` semantics).
                            //
                            // #4965 review S2: count the WE-have-nothing /
                            // THEY-have-something case. It was never in the
                            // 98.1%-identical denominator (that counted only
                            // `(Some, Some)`), and it is not neutral under
                            // hash-first — it classifies as `NeedBytes`, so it
                            // costs +2 messages for bytes this path delivers
                            // immediately. Sizing it keeps the headline honest.
                            (None, Some(_)) => {
                                op_manager.outbound_mix.record_summary_one_sided(
                                    contract.id(),
                                    crate::node::network_bridge::outbound_message_mix::
                                        SummaryObservation::full_bytes(single_entry),
                                    &mut one_sided_counted,
                                );
                                false
                            }
                            _ => false,
                        };

                        // SHADOW MODE (futile-repair detector,
                        // `crate::ring::futile_repair`). This is the OUTCOME
                        // observation: a two-sided comparison — both sides
                        // reported a real summary — that settles whatever heal
                        // we last sent on this edge. Gated on two-sidedness
                        // because a one-sided comparison is not a verdict about
                        // convergence; the `(None, Some(_))` and `_` arms above
                        // return `false` for "no basis to heal", not for
                        // "agreed", and feeding that in would score every
                        // contract we don't host as a successful repair.
                        //
                        // Passing `!is_stale` rather than a constant is the
                        // whole detector: it is what separates a repair that
                        // failed to converge from one that worked. `evidence`
                        // is what keeps the two conservative defaults out of
                        // that number. Pure accounting, no behaviour change.
                        if our_summary.is_some() && their_summary.is_some() {
                            op_manager
                                .interest_manager
                                .record_repair_outcome(&contract, &pk, !is_stale, evidence);
                        }

                        // #4952: upsert (not update) when the peer reported a
                        // real summary, so the ~5-min anti-entropy exchange can
                        // seed a summary for an advertised co-host we don't
                        // interest-track — otherwise those peers stay full-state
                        // broadcast targets forever. A `None` report keeps the
                        // old clear-only semantics (no entry is created just to
                        // hold `None`).
                        //
                        // LOAD-BEARING POSITION, not just a write: this runs
                        // OUTSIDE the `is_stale` branch, on converged and stale
                        // peers alike, and it is what refreshes the peer's
                        // interest TTL here — both `set_summary` and
                        // `clear_summary` end in `PeerInterest::refresh`. There
                        // is no explicit `refresh_peer_interest` on this path;
                        // the refresh is a CONSEQUENCE of the write.
                        //
                        // So moving this inside the staleness branch — which
                        // reads as a pure optimisation ("why rewrite a summary
                        // we just proved unchanged?") — silently stops
                        // refreshing every converged peer, and they age out at
                        // `INTEREST_TTL` while every test stays green. That is
                        // the #3046/#3093 bug class, which has already appeared
                        // at two other sites in this same fan-out logic (the
                        // production and sim-only converged skips, both fixed in
                        // #5055). Pinned by
                        // `summaries_arm_writes_summary_outside_staleness_branch_pin`.
                        match their_summary {
                            Some(theirs) => {
                                op_manager.interest_manager.upsert_peer_summary_from(
                                    &contract,
                                    &pk,
                                    theirs,
                                    crate::ring::interest::SummaryPopulationSource::InterestSummary,
                                );
                            }
                            None => op_manager.interest_manager.clear_peer_summary(
                                &contract,
                                &pk,
                                crate::ring::interest::SummaryMissingReason::ClearedByNoneReport,
                            ),
                        }

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
            // The bounded, targeted emission lives in `emit_stale_peer_syncs`,
            // shared verbatim with the `SummaryDigests` arm.
            emit_stale_peer_syncs(op_manager, source, peer_key.as_ref(), stale_contracts).await;

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

        InterestMessage::SummaryDigests { entries, .. } => {
            tracing::debug!(
                from = %source,
                entry_count = entries.len(),
                "Received SummaryDigests message"
            );

            // Hash-first half of the summary exchange (#4965).
            //
            // The peer told us what its summaries HASH to instead of shipping
            // them. For each entry we compute OUR OWN summary from actual
            // local state — exactly as the `Summaries` arm does — and compare.
            //
            // # This does not short-circuit the heal
            //
            // A digest match proves the peer's summary bytes EQUAL ours. That
            // is the same input the `Summaries` arm's staleness check gets, so
            // this arm runs that check for real rather than assuming its
            // answer: `record_summary_comparison` is called with the operands
            // it would have been called with, `summary_indicates_stale_peer`
            // is invoked, and a `true` verdict lands in the SAME
            // `emit_stale_peer_syncs` the `Summaries` arm uses. Nothing is
            // skipped on the strength of "identical summaries are never
            // stale"; that stays a property of the predicate, not an
            // assumption baked in here.
            //
            // Everything the digest CANNOT settle — differing digests, or a
            // peer holding state we don't — is answered with a
            // `SummaryRequest`, and the bytes come back as a plain
            // `Summaries` that runs the untouched original handler, including
            // its semantic (`get_state_delta`) staleness probe. Divergence
            // therefore costs one extra round trip (sub-second) against a
            // ~5-min heartbeat, and is never silently dropped. If the request
            // or its answer IS lost, the contract simply stays diverged until
            // a later heartbeat re-advertises it — the same outcome a lost
            // `Summaries` has today, not a new failure mode.
            //
            // #5238 made "a later heartbeat" materially later: the reply is
            // windowed, so re-advertisement takes up to `ceil(shared / 64)`
            // rounds rather than the very next one — on the order of an hour
            // for a peer sharing the whole of a 933-contract hosted set,
            // against 5 minutes before. Still bounded, still not a new failure
            // mode, but this sentence used to lean on a latency it no longer
            // has, so read it with that number rather than the old one.
            //
            // Telemetry note: the #4965 identical/differing counters are
            // recorded on AGREEMENT here and on ARRIVAL of the requested bytes
            // in the `Summaries` arm, so each contract is counted exactly once
            // per exchange. A request whose answer never arrives is the one
            // case that goes uncounted, biasing the ratio very slightly toward
            // "identical"; read the counters as a floor on the agreement rate,
            // not a point estimate.
            let peer_key = get_peer_key_from_addr(op_manager, source);
            let mut stale_contracts = Vec::new();
            let emit_confirmed = crate::config::SimulationIdleTimeout::is_enabled();
            let mut confirmed_states: Vec<(freenet_stdlib::prelude::ContractKey, String)> =
                Vec::new();
            let mut request_hashes: Vec<u32> = Vec::new();

            // Cloned rather than moved so `peer_key` survives for the
            // `emit_stale_peer_syncs` call below, which needs the peer's stable
            // identity to attribute the heal to a futile-repair edge. A
            // `PeerKey` wraps an x25519 `PublicKey` — a 32-byte `Copy` — so
            // this is a byte copy, not a key operation, and it happens once per
            // InterestSync message.
            if let Some(pk) = peer_key.clone() {
                // See `MAX_SUMMARY_COMPARISONS_PER_MESSAGE`: entries are
                // peer-supplied and cheap for the peer to fabricate, so
                // deduplicate by hash and process a bounded window. The window
                // starts at a random offset when the cap binds, for the same
                // starvation reason `emit_stale_peer_syncs` rotates — a
                // sender's entries arrive in contract-id order, so a fixed
                // prefix would starve the tail on every cycle forever.
                //
                // #5238: this cap was `MAX_SUMMARY_HASHES_PER_MESSAGE` (4096),
                // which bounds the MESSAGE but not the WORK — each entry costs
                // a `summary_if_hosted_or_in_use` call below, so a routine
                // heartbeat from a peer sharing hundreds of contracts ran
                // hundreds of them. This is the receiving half of the same
                // storm the send-side window closes; both legs of one
                // round-trip pay it independently.
                //
                // #5338: the ceiling that bounds the ENTRY count is now
                // `MAX_SUMMARY_ENTRIES_PER_MESSAGE`, and
                // `MAX_SUMMARY_COMPARISONS_PER_MESSAGE` bounds only the
                // entries that cost us a summarize (below). An entry whose
                // digest is `None` is settled by
                // `classify_summary_digest`'s `(_, None)` arm without
                // consulting our summary at all, so it costs no round trip
                // and charging it one was bounding the wrong thing —
                // symmetrically with the send leg, whose replies since #5338
                // may carry free entries past the summarize budget. Sized to
                // match the send ceiling exactly, so an upgraded peer's reply
                // is never truncated here.
                let mut entries = entries;
                if entries.len() > MAX_SUMMARY_ENTRIES_PER_MESSAGE {
                    let start = crate::config::GlobalRng::random_range(0..entries.len());
                    entries.rotate_left(start);
                    entries.truncate(MAX_SUMMARY_ENTRIES_PER_MESSAGE);
                }
                // #4965 agreement-rate proxy: the state-change-driven send
                // sites (proactive notification, rejection summary-back) are
                // single-entry by construction, and only `InterestsReply` (the
                // ~5-min heartbeat) is genuinely multi-entry. The emitter tag
                // is non-wire so the receiver cannot read it; this is the
                // closest available discriminator.
                //
                // It is a CONTAMINATED discriminator, and on THIS arm — the
                // digest leg — the contamination is the whole population.
                // `ChangeInterestsReply` is single-entry 100% of the time
                // (measured mean exactly 1.000, `max_entries` 1, over 418,476
                // messages on 1,284 peers; corroborated in two further
                // windows), because `broadcast_change_interests` gossips one
                // contract per message. It is ALSO version-gated to digests and
                // the fleet is past the floor, so 95-99% of its sends arrive
                // here. Meanwhile a notification ships full bytes
                // unconditionally (`send_proactive_summary_notification`), so a
                // single-entry observation on the digest leg is churn-leg BY
                // CONSTRUCTION and is not the R4b population at all — which is
                // why `SummaryObservation::digest` keeps it in separate buckets
                // rather than folding it into `*_single`. Do not read this flag
                // here as "this was a notification"; it is not, today.
                //
                // Reads `entries.len()` AFTER the ceiling above, which since
                // #5338 both reorders and truncates. For any message at or under
                // the ceiling — every message from a peer running this release —
                // that is the length the peer actually sent. An over-ceiling
                // message is classified by the truncated length instead, which
                // only affects the `single_entry` flag, and a message long
                // enough to be truncated is not single-entry either way.
                let single_entry = entries.len() == 1;
                // Dedup on the (hash, digest) PAIR, not on the hash alone.
                //
                // The sender emits one entry per CONTRACT, and two DISTINCT
                // contracts can collide on the 32-bit FNV-1a `contract_hash`
                // — `lookup_by_hash` returns a Vec precisely because that
                // happens. Deduping on hash alone dropped the second colliding
                // entry, and that is not a lost optimisation but permanent
                // silent divergence: with both local summaries byte-identical,
                // the FIRST entry's digest agrees against BOTH contracts, so
                // both record as converged while the entry carrying the
                // genuinely-diverged digest never runs. No request fires, and
                // every later heartbeat repeats it. The full-bytes `Summaries`
                // arm has no such dedup, so this was a REGRESSION for the
                // collision input class, not a pre-existing gap.
                //
                // Pinned by `colliding_contract_hashes_do_not_drop_the_second_entry`.
                let mut seen_pairs: HashSet<(u32, Option<crate::ring::interest::SummaryDigest>)> =
                    HashSet::new();
                // Separate from the pair set: the REQUEST list stays deduped by
                // hash, so N colliding entries still ask for their shared hash
                // once. That bound is what stops a collision (or a crafted
                // message) inflating the reply, and it is independent of the
                // per-entry dedup above.
                let mut requested_hashes: HashSet<u32> = HashSet::new();
                // Cache of OUR summaries for the hash CURRENTLY being
                // processed, keyed by contract. Cleared whenever the hash
                // changes (see `cached_for_hash`).
                //
                // It bounds the expensive operation. Pair-dedup (above) is
                // required for correctness but removed the incidental bound
                // hash-dedup used to provide: a peer can name ONE known hash
                // with many distinct fabricated digests, and without this each
                // pair would rerun `summary_if_hosted_or_in_use` — a
                // contract-handler round trip — for every contract matching
                // that hash, sequentially, on the loop the executor needs
                // responsive (`.claude/rules/code-style.md`, fan-out cost).
                //
                // PER-HASH, not per-message, and that scoping is the memory
                // bound. These are OWNED summary clones; a message-scoped cache
                // retains one per matched contract until the whole message
                // finishes, so a peer naming many distinct locally-hosted
                // hashes accumulates (matched contracts x summary size) —
                // hundreds of MB at the cap with River-scale summaries. That is
                // the large-value retention class `contract/executor.rs`
                // byte-bounds its own summary cache for.
                //
                // Per-hash scoping costs nothing real: distinct hashes resolve
                // to ~disjoint contract sets, so cross-hash caching never had
                // hits to give. And within one hash the skip below holds
                // processed pairs to ~2, so stopping the second pair from
                // refetching is the cache's entire job — which a per-hash cache
                // does completely. An attacker interleaving A,B,A,B to force
                // clears gains nothing either: each hash goes inert after its
                // first mismatch arms the skip.
                //
                // Retention is therefore bounded by ONE hash's contract set,
                // with no byte-budget machinery needed.
                let mut local_summaries: std::collections::HashMap<
                    ContractInstanceId,
                    Option<freenet_stdlib::prelude::StateSummary<'static>>,
                > = std::collections::HashMap::new();
                let mut cached_for_hash: Option<u32> = None;
                let mut compared_contracts: HashSet<ContractInstanceId> = HashSet::new();
                // No `one_sided_counted` set here, unlike the `Summaries` arm:
                // this arm no longer records one-sided observations at all
                // (#5153 review F2 — it double-counted against the full-bytes
                // reply that follows). Its absence is a nudge, NOT a guard: a
                // re-added recording could pass `&mut HashSet::new()` as a
                // temporary and compile fine. The actual guard is the test
                // `digest_arm_records_no_one_sided_observation`, which fails if
                // this arm regains a one-sided call.
                // WIRE-ORDER INDEPENDENCE — the invariant this grouping exists
                // to establish.
                //
                // The receiver's work must depend on the message's CONTENT, not
                // on the order the peer chose to send it in. Take the bounded,
                // pair-deduped set and GROUP IT BY HASH before doing any work,
                // so every hash is visited exactly once, contiguously.
                //
                // This is a root-cause fix, not another patch. Three separate
                // findings on this arm were all instances of peer-controlled
                // ordering: with entries walked in wire order, a peer choosing
                // an interleaving (A, B, A, B, ...) forced the per-hash cache to
                // clear and refetch on every revisit. An earlier comment here
                // argued that could not happen because "the skip arms after a
                // hash's first mismatch" — that argument was WRONG and is
                // removed: `DigestVerdict::Agree` does not set `needs_bytes`
                // (only the `NeedBytes` arm does), so a round of non-arming
                // entries leaves every hash live and revisitable.
                //
                // Grouping makes the whole class unreachable rather than
                // patching its instances, and restores both bounds at once:
                // <=1 fetch per contract per message (CPU) and retention <= one
                // hash's contract set (memory). The pair dedup, the request-list
                // hash dedup and the skip all remain — grouping does not replace
                // them, it removes the ordering freedom they were being asked to
                // absorb.
                let mut bounded: Vec<crate::message::SummaryDigestEntry> = Vec::new();
                // Distinct PAIRS that cost a summarize, matching what is
                // actually paid for — counting distinct hashes would no longer
                // bound the work now that several pairs can share a hash, and
                // counting ENTRIES would charge the free ones (#5338).
                let mut charged = 0usize;
                for entry in entries {
                    // Whether this entry can make us summarize, decided from
                    // the entry itself: an absent digest is settled by
                    // `classify_summary_digest` without our summary, so the
                    // fetch below is skipped for it. That makes this an upper
                    // bound on the round trips — never an under-count, which is
                    // the direction a budget has to err in.
                    let costs_a_summarize = entry.summary_digest.is_some();
                    if costs_a_summarize && charged >= MAX_SUMMARY_COMPARISONS_PER_MESSAGE {
                        // `continue`, not `break`: the free entries after this
                        // point still cost nothing and still carry a real
                        // repair (`clear_peer_summary`), so dropping them would
                        // discard information for no saving.
                        continue;
                    }
                    if !seen_pairs.insert((entry.hash, entry.summary_digest)) {
                        continue;
                    }
                    if costs_a_summarize {
                        charged += 1;
                    }
                    bounded.push(entry);
                }
                // Stable sort by hash: equal hashes become contiguous, and the
                // relative order WITHIN a hash is preserved, so behaviour for a
                // single hash is unchanged from before the grouping.
                bounded.sort_by_key(|e| e.hash);

                for entry in bounded {
                    // Once a hash is on the request list, further pairs naming
                    // it are inert: the full-bytes `Summaries` reply we are
                    // about to receive carries OUR summaries for ALL contracts
                    // matching the hash, so any divergence those pairs would
                    // have found is resolved there anyway.
                    //
                    // This cannot lose a heal — it defers one, to the reply that
                    // is already on its way.
                    if requested_hashes.contains(&entry.hash) {
                        continue;
                    }
                    // Drop the previous hash's summaries before moving to the
                    // next. This is the retention bound, and with the grouping
                    // above it fires exactly once per distinct hash.
                    if cached_for_hash != Some(entry.hash) {
                        local_summaries.clear();
                        cached_for_hash = Some(entry.hash);
                    }
                    crate::config::GlobalTestMetrics::note_summary_cache_size(
                        local_summaries.len(),
                    );
                    // One hash can resolve to several contracts (FNV-1a
                    // collisions), and any one of them needing bytes is enough
                    // to ask — but the hash goes on the request list at most
                    // once, so the reply cannot be inflated by collisions.
                    let mut needs_bytes = false;
                    for contract in op_manager.interest_manager.lookup_by_hash(entry.hash) {
                        if !op_manager.interest_manager.has_local_interest(&contract) {
                            continue;
                        }

                        // Our ACTUAL summary, from the same gated helper the
                        // `Summaries` arm uses (#4473). Never the cached
                        // `PeerInterest.summary`: that is our belief about the
                        // peer, and repairing a wrong belief is the entire job
                        // of this exchange.
                        //
                        // Memoized per MESSAGE (see `local_summaries`): the
                        // value cannot change while this handler runs, and
                        // re-fetching per pair is the amplification the pair
                        // dedup would otherwise open.
                        //
                        // #5338: skipped entirely when the peer advertised no
                        // digest. `classify_summary_digest` settles `(_, None)`
                        // as `PeerHasNoState` whatever our summary is, so the
                        // round trip could not change the verdict — it was pure
                        // cost, and it is what made a free entry expensive for
                        // the RECEIVER even though it was free for the sender.
                        // `emit_confirmed` (SIMULATION ONLY) keeps paying it,
                        // and that exception is load-bearing rather than
                        // decorative. `StateConfirmed` feeds the convergence
                        // checker through `EventKind::stored_state_hash()`,
                        // which `SimNetwork::check_convergence` and
                        // `check_convergence_from_logs` fold into a
                        // per-(peer, contract) latest-state map, and the direct
                        // runner enables this flag for EVERY simulation. Drop
                        // the exception and a peer's recorded hash goes stale
                        // (false divergence, flaky red) or the peer leaves
                        // `contract_states` entirely, taking the contract below
                        // the two-peer threshold the check needs — which skips
                        // it silently and passes VACUOUSLY. That second failure
                        // mode is why this stays: it makes the suite report
                        // success while checking less.
                        //
                        // #5338 removed this exception once, on a search for the
                        // literal `StateConfirmed` at the consumers. The
                        // consumption is through a generic accessor, so the
                        // string does not appear there and the search found
                        // nothing. Trace `stored_state_hash()`, not the variant
                        // name, before concluding this is unused.
                        //
                        // The cost is stated at
                        // `MAX_SUMMARY_COMPARISONS_PER_MESSAGE`: under this flag
                        // the receive leg's round trips are bounded by
                        // `MAX_SUMMARY_ENTRIES_PER_MESSAGE` instead.
                        let needs_our_summary = entry.summary_digest.is_some() || emit_confirmed;
                        let our_summary = if needs_our_summary {
                            if !local_summaries.contains_key(contract.id()) {
                                let fetched =
                                    summary_if_hosted_or_in_use(op_manager, &contract).await;
                                local_summaries.insert(*contract.id(), fetched.summary);
                                crate::config::GlobalTestMetrics::note_summary_cache_size(
                                    local_summaries.len(),
                                );
                            }
                            local_summaries
                                .get(contract.id())
                                .expect("just inserted")
                                .clone()
                        } else {
                            None
                        };

                        if emit_confirmed {
                            if let Some(ref summary) = our_summary {
                                confirmed_states.push((contract, hex::encode(summary.as_ref())));
                            }
                        }

                        match classify_summary_digest(
                            our_summary.as_ref(),
                            entry.summary_digest.as_ref(),
                        ) {
                            DigestVerdict::Agree => {
                                // The digest proves their summary bytes are
                                // ours, so hand the staleness machinery those
                                // bytes and let it decide, exactly as the
                                // `Summaries` arm would have.
                                let ours = our_summary
                                    .expect("DigestVerdict::Agree is only reachable with Some");
                                op_manager.outbound_mix.record_summary_comparison(
                                    contract.id(),
                                    ours.as_ref(),
                                    ours.as_ref(),
                                    // DIGEST leg, deliberately a separate bucket
                                    // from the full-bytes one: a notification
                                    // ships full bytes unconditionally today, so
                                    // a single-entry observation arriving here is
                                    // churn-leg by construction and is NOT part
                                    // of the population R4b's `p` is about
                                    // (#5153 review F1).
                                    crate::node::network_bridge::outbound_message_mix::
                                        SummaryObservation::digest(single_entry),
                                    &mut compared_contracts,
                                );
                                crate::config::GlobalTestMetrics::record_summary_digest_agreement(
                                    single_entry,
                                );
                                // `None` delta verdict: there is nothing to
                                // probe. `summary_indicates_stale_peer` sees
                                // byte-equal summaries and never reaches the
                                // verdict — the same path a byte-identical
                                // `SummaryEntry` takes today.
                                let is_stale = crate::ring::interest::summary_indicates_stale_peer(
                                    &ours, &ours, None,
                                );
                                // SHADOW MODE (futile-repair detector,
                                // `crate::ring::futile_repair`). Two-sided by
                                // construction: the digest PROVED the peer's
                                // summary bytes are ours, so this settles an
                                // outstanding heal on the edge exactly as the
                                // `Summaries` arm does. `NeedBytes` deliberately
                                // records nothing — it defers to the full-bytes
                                // `Summaries` reply, which observes there, so
                                // one divergence is never counted twice.
                                //
                                // Evidence is `Verdict` by construction and NOT
                                // a shortcut: `summary_indicates_stale_peer` is
                                // called on byte-identical operands here, which
                                // short-circuits before any delta probe, so no
                                // probe budget is consulted and no default can
                                // be taken. Pure accounting, no behaviour
                                // change.
                                op_manager.interest_manager.record_repair_outcome(
                                    &contract,
                                    &pk,
                                    !is_stale,
                                    crate::ring::futile_repair::OutcomeEvidence::Verdict,
                                );
                                // #4952: seed the peer-summary cache so an
                                // advertised co-host does not stay a
                                // full-state broadcast target. Fact, not
                                // belief: the digest established that these
                                // bytes are what the peer holds.
                                op_manager.interest_manager.upsert_peer_summary_from(
                                    &contract,
                                    &pk,
                                    ours,
                                    crate::ring::interest::SummaryPopulationSource::DigestAgreement,
                                );
                                if is_stale && !stale_contracts.contains(&contract) {
                                    stale_contracts.push(contract);
                                }
                            }
                            DigestVerdict::PeerHasNoState => {
                                // Same handling a `SummaryEntry` with
                                // `summary_bytes: None` gets: clear-only, no
                                // entry created just to hold `None`, no heal.
                                op_manager.interest_manager.clear_peer_summary(
                                    &contract,
                                    &pk,
                                    crate::ring::interest::SummaryMissingReason::ClearedByNoneReport,
                                );
                            }
                            // Defer to the full-bytes path: the digest cannot
                            // settle this contract, so ask for the summary and
                            // let the untouched `Summaries` handler decide.
                            DigestVerdict::NeedBytes => {
                                crate::config::GlobalTestMetrics::record_summary_digest_mismatch(
                                    single_entry,
                                );
                                // R4b instrument (#5153): a genuine digest
                                // disagreement deliberately does NOT record a
                                // `summary_entries_differing` here, on either the
                                // total or the single-entry bucket. It defers to
                                // the full-bytes `Summaries` reply that the
                                // `SummaryRequest` below provokes, which observes
                                // it in the other arm — recording here as well
                                // would count one divergence twice and inflate
                                // exactly the denominator `p` is computed
                                // against. Continuity across R4b is preserved
                                // rather than lost: the reply carries one entry
                                // per requested hash, so a single-entry digest
                                // mismatch produces a single-entry reply and
                                // classifies the same way on arrival.
                                //
                                // ACCEPTED BIAS: if the `SummaryRequest` or its
                                // reply is DROPPED, this divergence is never
                                // recorded anywhere, so it pushes `p` UP by the
                                // loss rate on one round trip. Double-counting
                                // would be plain wrong arithmetic, so the
                                // deferral stays.
                                //
                                // This is NOT the only bias and `p` is NOT a
                                // ceiling — an earlier revision of this comment
                                // said it was. The contamination term runs the
                                // other way and is larger: a single-entry
                                // `SummaryRequestReply` is differing by
                                // construction, so it inflates the denominator.
                                // See `OutboundMix::record_summary_comparison`
                                // and `notification_share_bounds` for both terms
                                // and why `p` must be quoted as an interval.
                                //
                                // Also not shape-preserving under hash collision:
                                // if two locally-known contracts share one 32-bit
                                // FNV-1a hash, the reply carries TWO entries and
                                // classifies as `MultiEntry`, so the deferred
                                // observation reaches no single-entry bucket at
                                // all. Rare, and it removes rather than
                                // fabricates, but it is a third small downward
                                // path on the single-entry counters.
                                // #5153 review F2 — the one-sided recording that
                                // used to live here is REMOVED, not moved.
                                //
                                // It fired when `our_summary.is_none()` and then
                                // set `needs_bytes`, so the bytes were requested
                                // and the full-bytes `Summaries` arm observed the
                                // SAME contract again on its `(None, Some(_))`
                                // branch. Two different messages with two
                                // different per-message dedup sets, so nothing
                                // suppressed the repeat: one divergence, counted
                                // twice. That directly contradicted the
                                // no-double-count property this arm's `differing`
                                // deferral is built on, and it inflated
                                // `summary_entries_one_sided_single`, which is
                                // presented as an R4b cost input.
                                //
                                // The full-bytes arm still records it, so nothing
                                // is lost: `our_summary` is None there too, so
                                // the reply takes `(None, Some(_))` and counts
                                // exactly once. One-sided now defers exactly as
                                // `differing` already did — consistently, rather
                                // than one of the two.
                                needs_bytes = true;
                            }
                        }
                    }
                    if needs_bytes && requested_hashes.insert(entry.hash) {
                        request_hashes.push(entry.hash);
                    }
                }
            }

            // A digest-match verdict of "stale" is not expected (the predicate
            // short-circuits on equal bytes) but is honoured rather than
            // assumed away — same bounded, targeted emission as `Summaries`.
            //
            // ACCEPTED COST, stated plainly: because agreement provably never
            // heals, every heal that DOES happen on this leg now costs one
            // extra round trip — the divergence is discovered from a digest,
            // the bytes are requested, and only then does the `Summaries` arm
            // run its staleness probe and emit. Against a ~300 s heartbeat a
            // sub-second RTT is not a convergence risk, but it is a real
            // regression in heal LATENCY on the legs that ship digests, and it
            // is the price of not shipping the summary every cycle.
            emit_stale_peer_syncs(op_manager, source, peer_key.as_ref(), stale_contracts).await;

            // A contract on the mismatch path is confirmed TWICE: once here,
            // once again when the requested bytes arrive at the `Summaries`
            // arm. That is idempotent for the convergence checker by
            // construction rather than by assumption — all three consumers
            // (`testing_impl.rs:3712`, `:6535`, `simulation_integration.rs:4682`)
            // fold into `BTreeMap<contract, BTreeMap<peer, hash>>` via
            // `.insert(peer_addr, hash)`, so a repeat for the same
            // (contract, peer) overwrites with the same value — or, if our
            // state moved in between, with the newer and more correct one.
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

            if request_hashes.is_empty() {
                // The 98.1% case: both sides already agree on every shared
                // contract and not one summary byte crossed the wire.
                None
            } else {
                // This is the ONE hash-first send with no version check, and
                // it is safe by inference rather than by gate: a
                // `SummaryRequest` is only ever emitted in reply to a
                // `SummaryDigests` we just decoded, and we only receive one
                // from a peer that chose the digest encoding — which it can
                // only do if it read OUR version as at-or-above the floor and
                // carries the variants itself. So the sender is necessarily
                // capable of decoding the reply. Gating here would be
                // redundant; NOT recording the inference would leave the next
                // reader to wonder whether it was an oversight.
                crate::config::GlobalTestMetrics::record_summary_byte_request();
                Some(InterestMessage::SummaryRequest {
                    hashes: request_hashes,
                })
            }
        }

        InterestMessage::SummaryRequest { hashes } => {
            tracing::debug!(
                from = %source,
                hash_count = hashes.len(),
                "Received SummaryRequest message"
            );

            // The peer's digest comparison could not settle these contracts,
            // so it needs the actual bytes. Answer with a plain `Summaries` —
            // ALWAYS the full-bytes form, NEVER the version-gated encoding
            // chooser the other reply sites use. (The chooser is not named
            // here on purpose: the pin test below asserts its identifier is
            // absent from this arm, and a mention in a comment would satisfy
            // that search and disarm the pin.)
            //
            // Two reasons, both load-bearing: replying with digests to a
            // request FOR bytes would loop (request → digests → request …),
            // and the requester has already established that a digest cannot
            // settle these entries. `summary_request_reply_is_always_full_bytes`
            // pins this.
            //
            // Disclosure and cost are identical to the `Interests` arm: the
            // same `get_matching_contracts` filter (so only contracts we
            // already track can be named) and the same
            // `summary_if_hosted_or_in_use` gate. Unlike the `Interests` arm
            // this registers no interest and removes none — a request is not
            // an interest advertisement. `MAX_SUMMARY_HASHES_PER_MESSAGE`
            // bounds the input so a peer cannot name an arbitrarily long hash
            // list.
            //
            // #5238: this arm now ALSO bounds the summarize loop itself, and
            // the reason is a consequence of that change rather than a new
            // policy. The previous justification for leaving it unbounded was
            // that "a peer could already force the same work by spamming
            // `Interests`, which runs the same loop" — true while the
            // `Interests` arm gave a digest-capable peer an unwindowed loop.
            // Now that BOTH forms of `Interests` are windowed, that arm is
            // strictly cheaper than this one, so leaving this one unbounded
            // would leave behind an amplification path easier than the one
            // being closed. Bounding it costs nothing in practice: a requester
            // running this release compares at most
            // `MAX_SUMMARY_COMPARISONS_PER_MESSAGE` entries, so it can never
            // ask for more hashes than this cap allows, and anything a
            // pre-upgrade peer asks for beyond it is re-requested from its next
            // heartbeat's digest comparison. Random offset, not a prefix, for
            // the tail-starvation reason the sibling arms document.
            let bounded = &hashes[..hashes.len().min(MAX_SUMMARY_HASHES_PER_MESSAGE)];
            let mut matching = op_manager.interest_manager.get_matching_contracts(bounded);
            if matching.len() > MAX_SUMMARY_COMPARISONS_PER_MESSAGE {
                let start = crate::config::GlobalRng::random_range(0..matching.len());
                matching.rotate_left(start);
                matching.truncate(MAX_SUMMARY_COMPARISONS_PER_MESSAGE);
            }
            let mut entries = Vec::with_capacity(matching.len());
            for contract in matching {
                let hash = contract_hash(&contract);
                let summary = summary_if_hosted_or_in_use(op_manager, &contract)
                    .await
                    .summary;
                entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));
            }

            if entries.is_empty() {
                None
            } else {
                // #5052/#4965: the one full-bytes send hash-first ADDS rather
                // than replaces, so it gets its own emitter arm — folded into
                // the heartbeat reply it would look like the heartbeat failing
                // to shrink, when it is the mismatch tail doing its job.
                Some(full_summaries_message(
                    entries,
                    SummariesEmitter::SummaryRequestReply,
                ))
            }
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
                        op_manager.interest_manager.remove_peer_interest_for(
                            &contract,
                            pk,
                            crate::ring::interest::InterestRemovalCause::ChangeInterests,
                        );
                    }
                }
            }

            // Handle additions - respond with summaries for newly shared contracts
            let mut entries = Vec::new();
            if let Some(ref pk) = peer_key {
                // #5238: deduplicate the peer-supplied hash list before doing
                // any work.
                //
                // This arm is deliberately NOT windowed — it is driven by
                // interest churn rather than by a clock, so there is no
                // guaranteed next round to rotate into and a window could defer
                // a newly-added interest indefinitely rather than by a bounded
                // number of heartbeats. That argument is recorded at the arm
                // below and it still stands.
                //
                // It is an argument against ROTATION, not against DEDUP, and
                // the two are not the same trade. A rotation window can drop a
                // new interest; a dedup set cannot, because the second and
                // later copies of a hash carry no information the first did not
                // already deliver. So dedup is free here in exactly the way a
                // window is not.
                //
                // It is worth doing because bounding the other four loops makes
                // THIS the cheapest amplification path in the family: `added`
                // is peer-supplied and uncapped, and every entry costs one
                // `summary_if_hosted_or_in_use` round trip that re-enters WASM
                // on a memo miss, sequentially on the handler. Leaving it would
                // repeat #5155's mistake one level up — bound the paths you
                // measured and leave the equivalent one you did not.
                //
                // Note this bounds work by DISTINCT hash, not by message size.
                // A peer naming many genuinely-distinct new interests still
                // costs one round trip each, by design: those are real
                // additions and dropping them is the failure mode the missing
                // window is avoiding.
                let mut seen_added: HashSet<u32> = HashSet::new();
                for hash in added {
                    if !seen_added.insert(hash) {
                        continue;
                    }
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
                        // One acquisition via the refresh's own bool — see
                        // `InterestManager::refresh_peer_interest` for why the
                        // `get_peer_interest(..).is_some()` form it replaces was
                        // both expensive (it clones the cached summary) and racy.
                        let is_new = if op_manager
                            .interest_manager
                            .refresh_peer_interest(&contract, pk)
                        {
                            false
                        } else {
                            op_manager.interest_manager.register_peer_interest_from(
                                &contract,
                                pk.clone(),
                                None,
                                false,
                                crate::ring::interest::InterestRegistrationSource::ChangeInterests,
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
                        //
                        // #5238 bounded every OTHER summarize loop in this
                        // handler and deliberately left this one alone, for
                        // #5155's reason: this arm is driven by interest CHURN,
                        // not by a clock, so there is no guaranteed next round
                        // to rotate into and a window here could defer a
                        // newly-added interest indefinitely rather than by a
                        // bounded number of heartbeats. Production emits
                        // `ChangeInterests` on a 0->1 transition, so `added` is
                        // normally one hash. A peer CAN name many at once, and
                        // that is the residual of this family; bounding it
                        // needs a mechanism that cannot drop a new interest,
                        // not the rotation the periodic arms use.
                        let summary = summary_if_hosted_or_in_use(op_manager, &contract)
                            .await
                            .summary;
                        entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));
                    }
                }
            }

            if entries.is_empty() {
                None
            } else {
                // #5052: also built by `summary_if_hosted_or_in_use`, but driven
                // by interest CHURN rather than the heartbeat clock — a peer
                // joining or dropping interest, not a periodic tick. A different
                // thing to fix if it is the large one.
                //
                // SINGLE-entry, unlike the `InterestsReply` above — corrected
                // 2026-08-12 (#5153 review F1), where this said "also
                // multi-entry". `broadcast_change_interests` gossips one contract
                // per message, so the `entries` built above carries exactly one:
                // measured mean 1.000, `max_entries` 1, over 418,476 messages on
                // 1,284 peers. Load-bearing rather than trivia — the R4b
                // agreement-rate instrument cannot read the emitter tag and uses
                // message LENGTH as its proxy for "this was a notification", so
                // this arm is that proxy's largest contaminant.
                Some(summaries_reply_for_peer(
                    op_manager,
                    source,
                    entries,
                    SummariesEmitter::ChangeInterestsReply,
                ))
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

            // Delta-incompatibility signal (HQk7 resync loop): a ResyncRequest
            // arriving shortly after WE delivered a delta to this peer for
            // this contract means our delta failed to apply there. Count it
            // toward the sender-side "this contract can't take deltas" memo so
            // the broadcast path falls back to full-state sends instead of
            // recomputing doomed deltas. Runs BEFORE the rate limiters AND
            // before the broken-contract egress gate below — the signal is
            // valid even when the full-state response is suppressed (a broken
            // contract that also can't take deltas still wants full-state
            // fallback recorded for the peers we DO serve). See
            // `crate::ring::delta_incompat`.
            op_manager
                .ring
                .delta_incompat
                .note_resync_request(*key.id(), source);

            // Egress gate (broken invariants): a contract flagged as
            // violating CRDT idempotency must not have its full state
            // served to peers. The executor already suppresses commit +
            // BroadcastStateChange for a flagged contract, but a
            // ResyncResponse from this node would still hand the
            // problematic state to the requester, re-seeding the
            // non-idempotent broadcast echo the flag exists to quarantine
            // (#4279 storm shape). Suppress the response; the requester's
            // retry lands on an unflagged peer or waits out the TTL. See
            // `crate::ring::broken_invariants`.
            if op_manager.ring.is_contract_broken(&key) {
                tracing::debug!(
                    from = %source,
                    contract = %key,
                    event = "resync_response_suppressed_broken_contract",
                    "ResyncRequest for contract flagged as broken — not serving full state"
                );
                return None;
            }

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
                op_manager.interest_manager.clear_peer_summary(
                    &key,
                    pk,
                    crate::ring::interest::SummaryMissingReason::ClearedByResync,
                );
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

            // #4864 round-8 (Codex P1): CORRELATE with an outstanding ResyncRequest
            // WE sent to this peer for this contract, BEFORE touching the contract
            // handler. The apply below runs a full-state WASM merge that is
            // deliberately NOT backoff-gated, so an unsolicited or replayed
            // ResyncResponse would burn up to a full WASM budget per message,
            // bypassing every emitter-side gate (those only bound OUR requests).
            // `consume` require-and-consumes a matching (contract, source) entry;
            // consume-on-first-match makes replay dead, and a stale (TTL-expired)
            // or absent entry drops the response without running WASM. Mixed-version
            // safe: an old peer only ever answers OUR request, so a legit response
            // always has an entry unless it raced the 60s TTL (anti-entropy heals
            // that corner).
            if !op_manager
                .ring
                .outstanding_resync_requests
                .consume(*key.id(), source)
            {
                crate::config::GlobalTestMetrics::record_resync_response_unsolicited();
                tracing::debug!(
                    from = %source,
                    contract = %key,
                    event = "resync_response_unsolicited",
                    "ResyncResponse with no matching outstanding request — dropping \
                     without applying (unsolicited, replayed, or TTL-expired)"
                );
                return None;
            }

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
                // #4952: upsert — a ResyncResponse sender self-reported the
                // summary of the full state it just shipped us; seed it even
                // when we don't interest-track that co-host.
                op_manager.interest_manager.upsert_peer_summary_from(
                    &key,
                    &pk,
                    summary,
                    crate::ring::interest::SummaryPopulationSource::ResyncResponse,
                );
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
) -> SummaryProbe {
    if op_manager.ring.should_summarize_or_broadcast(key) {
        // Periodic interest-sync summarize: best-effort background work, so it
        // yields the contract loop to client/relay traffic (#4534 / #4473).
        SummaryProbe {
            summary: get_contract_summary(op_manager, key, crate::contract::Priority::Background)
                .await,
            summarized: true,
        }
    } else {
        SummaryProbe {
            summary: None,
            summarized: false,
        }
    }
}

/// What one [`summary_if_hosted_or_in_use`] call produced, together with
/// whether it actually paid for it.
///
/// The two are reported together on purpose (#5338). The rotation window exists
/// to bound the number of contract-handler round trips a reply makes, so its
/// budget must be charged by the code that decides whether to make one — not
/// re-derived by the caller from `summary.is_none()`, which conflates "the gate
/// declined, nothing was spent" with "the round trip ran and came back empty".
/// See the "metric describing a filtering decision" entry in
/// `.claude/rules/bug-prevention-patterns.md` for the general shape and the
/// three wrong counts it produced last time.
///
/// For the population this matters for — a contract we track only because a
/// peer registered interest in it — the gate really is pure in-memory work:
/// `should_summarize_or_broadcast` is
/// `(is_hosting_contract || contract_in_use) && contract_state_present`, and
/// `&&` short-circuits, so a contract that is neither hosted nor in use never
/// reaches the state-store lookup at all. It costs two map reads.
///
/// The one declining class that DOES pay a state-store point lookup is the
/// hosted-or-in-use contract with no stored state — the #4610 phantom — which
/// is a small population by construction and still makes no contract-handler
/// round trip and runs no WASM, which is the cost the budget is sized against.
///
/// This paragraph is load-bearing for anyone retuning
/// [`MAX_SUMMARY_ENTRIES_PER_MESSAGE`], which is why it is stated precisely
/// rather than hedged. An earlier version of it claimed the gate was "not
/// literally free" because of that state-store lookup, without noting the
/// short-circuit — and a comment that overstates what walking past a free entry
/// costs is an invitation to tighten the ceiling in order to buy back a cost
/// that is not there. The costs that ceiling really rations are on the RECEIVE
/// side and on the wire; see the constant.
struct SummaryProbe {
    /// Our summary. Absent when the gate declined OR when the round trip ran
    /// and produced nothing; `summarized` is what tells those apart.
    summary: Option<freenet_stdlib::prelude::StateSummary<'static>>,
    /// Whether the contract-handler round trip actually ran.
    summarized: bool,
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
            connection_scope,
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
                    DelegateRequest::RegisterDelegateWithPredecessors { .. } => {
                        "RegisterDelegateWithPredecessors"
                    }
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
                // `connection_scope` decides whether this request may receive an
                // ATTESTED application identity (GHSA-824h-7x5x-wfmf). It comes
                // from the connection layer on the same forge-proof channel as
                // `origin_contract` and `user_context`.
                executor.delegate_request(
                    op,
                    origin_contract.as_ref(),
                    None,
                    connection_scope,
                    user_context.as_ref(),
                )
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

    /// Strip `//` comment lines from a source window before asserting on it.
    ///
    /// Load-bearing, not tidiness. Source-scrape pins assert that an arm CALLS
    /// something; the identifiers they search for also appear in that arm's own
    /// explanatory comments, so an unstripped window is satisfied by PROSE.
    ///
    /// Two pins in this file were vacuous for exactly this reason:
    /// `digest_arm_shares_the_single_heal_path` (its window's comments name
    /// `summary_indicates_stale_peer` and `record_summary_comparison`), and
    /// `summaries_arm_uses_semantic_staleness_probe_pin` (whose CORRECT window
    /// still carries comment mentions of `plan_staleness_probe` at node.rs:3032
    /// and `summary_indicates_stale_peer` at :3074/:3135, alongside the real
    /// calls at :3095/:3115/:3123/:3140).
    ///
    /// Note this is only ONE of the two guards a scrape pin needs. The sibling
    /// guard is on the NEEDLE — `concat!("record_summary", "_comparison")` — so
    /// the assertion's own source cannot satisfy its own scrape. Needle guard
    /// and window guard defend against different self-matches; a pin wants
    /// both. See #5076.
    fn code_only(window: &str) -> String {
        window
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

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
    /// #4952 pin: the `Summaries` handler must UPSERT a reported summary so
    /// the ~5-min anti-entropy exchange can seed one for an advertised
    /// co-host we don't interest-track (`update_peer_summary` no-ops for
    /// untracked peers — the full-state fixed point). A `None` report keeps
    /// the clear-only `update_` semantics: no entry is created just to hold
    /// `None`. Matches whitespace-stripped source (rustfmt-proof).
    #[test]
    fn summaries_arm_upserts_reported_summary_pin() {
        let src = include_str!("node.rs");
        let handler_start = src
            .find("async fn handle_interest_sync_message")
            .expect("handle_interest_sync_message not found");
        let handler_end = handler_start
            + src[handler_start..]
                .find("\nmod tests {")
                .or_else(|| src[handler_start..].find("\n#[cfg(test)]"))
                .expect("end of handler region not found");
        let body: String = src[handler_start..handler_end].split_whitespace().collect();
        assert!(
            body.contains("upsert_peer_summary_from(&contract,&pk,theirs,"),
            "Summaries arm must upsert a Some(summary) report (seeds untracked \
             co-hosts, #4952)"
        );
        assert!(
            !body.contains("update_peer_summary(&contract,&pk,Some"),
            "a Some(summary) report must go through the upsert, not the \
             untracked-no-op update path (#4952)"
        );
        assert!(
            body.contains("clear_peer_summary(&contract,&pk,")
                && body.contains("SummaryMissingReason::ClearedByNoneReport"),
            "Summaries arm must keep clear-only semantics for a None report \
             (creating an entry just to hold None would relabel untracked \
             traffic as tracked without enabling deltas), and must tag the \
             clear ClearedByNoneReport so #4961 can attribute the \
             full_no_their_summary_tracked arm to this path"
        );
    }

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
            gated_calls >= 5,
            "expected the 5 periodic interest-sync arms (Interests, Summaries, \
             SummaryDigests, SummaryRequest, ChangeInterests) to call \
             summary_if_hosted_or_in_use, found {gated_calls}. The hash-first \
             arms (#4965) summarize on the same schedule as the ones they \
             replace, so an ungated call there re-opens the #4473 storm on the \
             hottest path in the protocol."
        );
    }

    /// Pin: the `Summaries` interest-sync arm must decide staleness
    /// SEMANTICALLY — routing through `peer_summary_has_pending_state` (the
    /// contract `get_state_delta` probe) and `summary_indicates_stale_peer`
    /// (the decision policy), under the per-message probe cap via
    /// `plan_staleness_probe` — NOT a bare summary byte comparison (#4857
    /// secondary finding / the summarize storm).
    ///
    /// The data-layer unit tests in `ring/interest.rs` exercise those helpers
    /// directly, so they stay green even if this handler hunk is reverted to a
    /// bare `ours != theirs` byte compare (re-opening the storm). This
    /// source-scrape is the only guard on the handler WIRING — mirrors the
    /// sibling `interest_sync_periodic_arms_summarize_only_hosted_or_in_use_pin`
    /// and the #3791 targeted-heal source-scrape pins.
    #[test]
    fn summaries_arm_uses_semantic_staleness_probe_pin() {
        let src = include_str!("node.rs");

        let handler_start = src
            .find("async fn handle_interest_sync_message(")
            .expect("handle_interest_sync_message not found");
        // Scope to the `Summaries` arm = its match start .. the next arm
        // (`ChangeInterests`), so the assertions can't false-pass on unrelated
        // code elsewhere in the handler.
        // `{ entries, .. }` since #5052 added the non-wire emitter tag to the
        // variant — the RECEIVE arm ignores it (an inbound tag is always the
        // `Default`; see `SummariesEmitter`), so it destructures with `..`.
        let summaries_off = src[handler_start..]
            .find("InterestMessage::Summaries { entries, .. }")
            .expect("Summaries arm not found");
        // End at the arm that IMMEDIATELY follows, not at a later one. When
        // the hash-first arms (#4965) were inserted between `Summaries` and
        // `ChangeInterests`, an end marker of `ChangeInterests` silently
        // widened this window to cover them — and because the digest arm also
        // calls `record_summary_comparison` and `summary_indicates_stale_peer`,
        // the positive assertions below would have kept passing with those
        // calls deleted from `Summaries` itself.
        let next_off = src[handler_start..]
            // `{ entries, .. }` — the destructuring the REAL arm uses. An
            // earlier revision searched for the bare `{ entries }` form, which
            // appears NOWHERE in this file except the needle line itself, so
            // `include_str!` made it match ITS OWN SOURCE: the window widened
            // roughly eightfold, swallowing the digest arm's own calls and this
            // pin's rustdoc, either of which satisfies all four assertions with
            // the true `Summaries` arm's calls deleted. A needle must resolve
            // to the INTENDED place, not merely resolve somewhere (#5076).
            .find("InterestMessage::SummaryDigests { entries, .. } => {")
            .expect("SummaryDigests arm not found");
        let window_end = handler_start + next_off;

        // Hard guard against that failure recurring. If the real arm's shape
        // ever drifts again, `find` falls through to this pin's own literal
        // above and the window silently widens to span the test module — which
        // is exactly how this pin was vacuous twice on this branch. Asserting
        // the end lands in PRODUCTION code turns the recurrence into a loud
        // failure instead of a quiet pass.
        //
        // Anchored on the outer `mod tests`, not the first `#[cfg(test)]`:
        // node.rs has a `#[cfg(test)]` near the top, so that marker would put
        // the boundary BEFORE the handler and make this assertion fire on
        // correct code. (#5076: a scrape's own scope needs checking too.)
        let test_mod_start = src
            .find("\n#[cfg(test)]\nmod tests {")
            .expect("node.rs outer test module not found");
        assert!(
            window_end < test_mod_start,
            "the Summaries-arm window ends at byte {window_end}, inside the \
             test module (starts at {test_mod_start}). The end needle has \
             fallen through to this pin's own source again — the #5076 \
             self-match — and the window has silently widened. Re-anchor it on \
             the arm that immediately follows `Summaries` in production code."
        );

        let summaries_arm = &code_only(&src[handler_start + summaries_off..window_end]);

        // #4965: the measurement call must stay wired into this arm. It is
        // pure observation, so deleting it breaks no test and no behavior —
        // the rollup would simply report zero comparisons forever, which reads
        // as "nothing to save here" and would retire the hash-first redesign
        // for the wrong reason. Needle split so this assertion's own source
        // cannot satisfy the scrape.
        assert!(
            summaries_arm.contains(concat!("record_summary", "_comparison")),
            "the Summaries arm must record the #4965 summary-comparison \
             measurement; without it the identical/differing rollup is \
             silently always zero"
        );
        // The per-message dedup needs no pin: `record_summary_comparison` takes
        // the seen-set as an argument, so there is no way to call it without
        // deduping. That replaced a call-site `if` guard which no source pin
        // could protect — a structural pin for it was written and deleted after
        // mutation testing showed it green when the call was moved out from
        // behind the guard. Making the bypass unrepresentable beat testing for
        // it; the repeated-call behavior is covered directly in
        // `outbound_message_mix::tests`.

        assert!(
            summaries_arm.contains("peer_summary_has_pending_state"),
            "the Summaries arm must consult the contract delta probe \
             (peer_summary_has_pending_state) to decide staleness — a bare \
             summary byte comparison re-opens the #4857 summarize storm"
        );
        assert!(
            summaries_arm.contains("summary_indicates_stale_peer"),
            "the Summaries arm must decide staleness via \
             summary_indicates_stale_peer (semantic policy), not inline byte \
             inequality"
        );
        assert!(
            summaries_arm.contains("plan_staleness_probe"),
            "the Summaries arm must ration WASM probes through \
             plan_staleness_probe (MAX_STALENESS_PROBES_PER_SUMMARIES cap) — \
             an uncapped probe per contract is a DoS amplification surface"
        );
    }

    /// Source-scrape pin (#3046 / #3093): the `Summaries` arm's summary write
    /// must stay OUTSIDE the staleness branch, because that write is what
    /// refreshes the peer's interest TTL on this path.
    ///
    /// There is no explicit `refresh_peer_interest` here. `upsert_peer_summary`
    /// and `clear_peer_summary` both end in `PeerInterest::refresh`, so the TTL
    /// refresh is a CONSEQUENCE of writing the summary, and it currently reaches
    /// converged and stale peers alike only because the write sits after the
    /// verdict rather than inside it.
    ///
    /// That makes the obvious "optimisation" — skip the rewrite when we just
    /// proved the summary unchanged — a silent subscriber-expiry bug: every
    /// converged peer stops being refreshed and ages out at `INTEREST_TTL`,
    /// with no test failing. This is the third site of a class that already
    /// appeared twice in the fan-out logic (the production and sim-only
    /// converged skips, both fixed in #5055), which is why it is pinned before
    /// anyone tries it rather than after.
    #[test]
    fn summaries_arm_writes_summary_outside_staleness_branch_pin() {
        let src = include_str!("node.rs");
        let handler_start = src
            .find("async fn handle_interest_sync_message(")
            .expect("handle_interest_sync_message not found");
        let summaries_off = src[handler_start..]
            .find("InterestMessage::Summaries { entries, .. }")
            .expect("Summaries arm not found");
        let change_off = src[handler_start..]
            .find("InterestMessage::ChangeInterests")
            .expect("ChangeInterests arm not found");
        let summaries_arm = &src[handler_start + summaries_off..handler_start + change_off];

        // Both writes must be present — they are the only thing refreshing the
        // TTL on this path.
        let upsert_at = summaries_arm.find("upsert_peer_summary_from(").expect(
            "the Summaries arm must cache the peer's reported summary via \
             upsert_peer_summary — that write is also what refreshes the peer's \
             interest TTL here (#4952, #3046)",
        );
        assert!(
            summaries_arm.contains("clear_peer_summary("),
            "the None-report branch must clear via clear_peer_summary — it too \
             refreshes the TTL, so replacing it with a bare no-op stops \
             refreshing peers that report no summary"
        );

        // The load-bearing structural fact: the write is NOT nested inside the
        // staleness decision. `is_stale` is consumed AFTER the write; if a
        // refactor moves the write inside an `if is_stale` block, the write
        // (and with it the refresh) stops running for converged peers.
        let is_stale_use = summaries_arm.find("if is_stale").expect(
            "the Summaries arm must still branch on is_stale for the heal \
             decision — if this moved, re-check where the summary write sits",
        );
        assert!(
            upsert_at < is_stale_use,
            "the summary write MUST precede (and sit outside) the `if is_stale` \
             heal branch. Moving it inside skips the write — and therefore the \
             interest-TTL refresh, which on this path is only a side effect of \
             the write — for every CONVERGED peer, so they age out at \
             INTEREST_TTL and silently stop receiving broadcasts (#3046/#3093). \
             upsert at {upsert_at}, `if is_stale` at {is_stale_use}"
        );

        // And it must not be conditioned on staleness some other way.
        let stripped: String = summaries_arm
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            !stripped.contains("ifis_stale{op_manager.interest_manager.upsert_peer_summary_from("),
            "the summary write must not be gated on is_stale — see above"
        );
    }

    /// Source-scrape pin (HQk7 resync loop): the `ResyncRequest` arm must feed
    /// the sender-side delta-incompatibility memo (`note_resync_request`)
    /// BEFORE the response rate limiters run. A ResyncRequest arriving shortly
    /// after we delivered a delta to that peer is the wire-visible form of the
    /// peer's `delta_apply_failed`; it is a valid incompatibility signal even
    /// when the full-state response itself is suppressed by the #4861
    /// limiters, so gating it behind them would starve the memo exactly during
    /// the storm it exists to stop. See `crate::ring::delta_incompat`.
    #[test]
    fn resync_request_arm_feeds_delta_incompat_memo_before_limiters() {
        let src = include_str!("node.rs");

        let handler_start = src
            .find("async fn handle_interest_sync_message(")
            .expect("handle_interest_sync_message not found");
        // Scope to the `ResyncRequest` arm = its match start .. the
        // `ResyncResponse` arm, so the assertions can't false-pass on
        // unrelated code elsewhere in the handler.
        let req_off = src[handler_start..]
            .find("InterestMessage::ResyncRequest { key }")
            .expect("ResyncRequest arm not found");
        let resp_off = src[handler_start..]
            .find("InterestMessage::ResyncResponse {")
            .expect("ResyncResponse arm not found");
        let req_arm = &src[handler_start + req_off..handler_start + resp_off];

        let memo_pos = req_arm
            .find(".note_resync_request(")
            .expect("the ResyncRequest arm must feed the delta-incompat memo");
        let limiter_pos = req_arm
            .find("resync_response_limiter")
            .expect("per-(peer, contract) response limiter not found in ResyncRequest arm");
        assert!(
            memo_pos < limiter_pos,
            "note_resync_request must run BEFORE the response rate limiters \
             (memo {memo_pos} < limiter {limiter_pos}) — a suppressed response \
             is still a valid delta-incompatibility signal"
        );
        // Also lock memo-before-broken-gate: B's `is_contract_broken → return
        // None` egress gate sits between the memo and the limiters, and the
        // arming's own comment requires it run "before the broken-contract
        // egress gate". Without this assertion a future edit could move the
        // arming after the gate (silently dropping the signal for broken
        // contracts) and still pass the memo-before-limiter check above.
        let broken_pos = req_arm
            .find("is_contract_broken")
            .expect("the ResyncRequest arm must gate on the broken-contract egress check (B)");
        assert!(
            memo_pos < broken_pos,
            "note_resync_request must run BEFORE the broken-contract egress gate \
             (memo {memo_pos} < broken {broken_pos}) — the delta-incompatibility \
             signal is valid even when the full-state response is suppressed for a \
             broken contract"
        );
    }

    /// Source-scrape pin (HQk7 fork investigation): the `ResyncResponse` arm
    /// must apply the received full state through the contract handler's
    /// `UpdateQuery` merge path — i.e. through the contract's own
    /// `validate_state`/`update_state` — and never via a direct store. The
    /// executor-side pins (`full_state_version_gate_pins` in
    /// `executor_impl.rs`) guard the upsert body; this pin guards the
    /// likelier regression site, the resync arm itself, where a future
    /// "optimization" could bypass the merge with a blind state write and
    /// silently disable a well-behaved contract's version gate (the
    /// lower-version-overwrites-higher failure class).
    #[test]
    fn resync_response_arm_applies_via_update_query_not_blind_store() {
        let src = include_str!("node.rs");

        let handler_start = src
            .find("async fn handle_interest_sync_message(")
            .expect("handle_interest_sync_message not found");
        // Scope to the apply segment of the ResyncResponse arm: from the
        // received-log marker (unique to the arm) to the applied-log marker.
        // The correlation gate, UpdateData construction, and contract-handler
        // dispatch all live between the two.
        let recv_off = src[handler_start..]
            .find("event = \"resync_response_received\"")
            .expect("resync_response_received marker not found");
        let applied_off = src[handler_start..]
            .find("event = \"resync_applied\"")
            .expect("resync_applied marker not found");
        let apply_segment = &src[handler_start + recv_off..handler_start + applied_off];

        assert!(
            apply_segment.contains("UpdateData::State("),
            "the resync full state must be wrapped as UpdateData::State"
        );
        assert!(
            apply_segment.contains("ContractHandlerEvent::UpdateQuery"),
            "the resync apply must route through notify_contract_handler \
             (ContractHandlerEvent::UpdateQuery) so the contract's own \
             validate_state/update_state judge the incoming state"
        );
        assert!(
            !apply_segment.contains("PutQuery") && !apply_segment.contains(".store("),
            "the resync arm must NOT install state via PutQuery or a direct \
             store — that would bypass the contract's version acceptance for \
             already-held contracts"
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
    /// re-registration of an existing entry on `refresh_peer_interest()`'s own
    /// return value (`true` = an entry existed and was refreshed), exactly like
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

        // Match the guard SHAPE regardless of how rustfmt wraps the builder
        // chain across lines (see `feedback_source_pins_survive_rustfmt`).
        fn strip_whitespace(src: &str) -> String {
            src.chars().filter(|c| !c.is_whitespace()).collect()
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
        // re-registration on the refresh's OWN return value, so the register can
        // only be the else-branch fallback for a genuinely new peer, never the
        // primary path. Asserting the whole `refresh(..) { false } else {` shape
        // rather than "refresh appears somewhere before register" is what makes
        // this a wiring pin: a reverted arm that calls refresh for some unrelated
        // reason and then registers unconditionally would satisfy mere ordering.
        // The pre-fix arm had a single unguarded
        // `register_peer_interest(.., false)` and no refresh at all, so the
        // needle below is absent on it.
        let stripped = strip_whitespace(&arm);
        assert!(
            stripped.contains("refresh_peer_interest(&contract,pk){false}else{"),
            "ChangeInterests arm MUST branch on refresh_peer_interest()'s return \
             value — refreshing an existing entry (preserving is_upstream + the \
             cached summary) and registering ONLY in the else branch. Arm:\n{arm}"
        );
        assert!(
            stripped.contains("else{op_manager.interest_manager.register_peer_interest_from("),
            "the register MUST be the else-branch fallback of that guard, not a \
             separate statement that runs regardless (D2). Arm:\n{arm}"
        );

        // Exactly ONE of each in the arm. A second register would mean an
        // unguarded bare register slipped back in alongside the guard; a second
        // refresh would mean the guard needle above matched a different call
        // than the one gating the register.
        assert_eq!(
            arm.matches("register_peer_interest_from(").count(),
            1,
            "ChangeInterests arm must contain exactly one (guarded, else-branch) \
             register_peer_interest call; a second would be an unguarded clobber (D2)"
        );
        assert_eq!(
            arm.matches("refresh_peer_interest(").count(),
            1,
            "ChangeInterests arm must contain exactly one refresh_peer_interest \
             call — the one gating the register (D2)"
        );

        // The two-lookup `get_peer_interest(..).is_some()` form this replaced
        // clones the cached summary (state-sized on the contracts that matter)
        // purely to test presence, and loses the registration entirely if the
        // entry is removed between the lookups. Reverting to it is a silent
        // regression, so forbid it here.
        assert!(
            !stripped.contains("get_peer_interest(&contract,pk).is_some()"),
            "ChangeInterests arm must not re-introduce the two-lookup \
             get_peer_interest(..).is_some() guard: it deep-copies the cached \
             summary just to test presence, and is not atomic with the refresh"
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
            .set_own_addr_local_for_test("127.0.0.1:14000".parse().unwrap());

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
            .set_own_addr_local_for_test("127.0.0.1:14100".parse().unwrap());

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
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "a stand-in executor loop: it only serves the two \
                              queries this test issues, and ContractHandlerEvent \
                              has 20+ variants — any other event reaching it is \
                              an unexpected-input panic, not a silent fallthrough"
                )]
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

    /// #4864 round-8 (Codex P1): an UNSOLICITED `ResyncResponse` — one with NO
    /// matching outstanding `ResyncRequest` we recorded for that
    /// `(contract, source)` — MUST be dropped by the correlation gate BEFORE any
    /// state is applied. This is the SECURITY property: the resync apply runs a
    /// full-state WASM merge that is deliberately NOT backoff-gated, so without
    /// the gate a peer could spray unsolicited full-state responses and burn a
    /// full WASM merge budget per message, bypassing every emitter-side rate
    /// limit (those only bound OUR requests). The gate
    /// (`outstanding_resync_requests.consume(..)`) runs before
    /// `notify_contract_handler(ContractHandlerEvent::UpdateQuery { .. })`, so an
    /// unsolicited response never reaches the contract handler at all.
    ///
    /// Asserts: (a) the handler returns `None`; (b) NO `UpdateQuery` ever reached
    /// the (stand-in) contract handler — the WASM-apply path was never entered;
    /// (c) the unsolicited-drop metric incremented exactly once.
    #[tokio::test]
    async fn resync_response_unsolicited_is_dropped_without_applying() {
        use crate::contract::ContractHandlerEvent;
        use std::sync::atomic::{AtomicBool, Ordering};

        // Build a real OpManager (same harness as
        // resync_request_for_held_contract_passes_gate_and_responds).
        let config_args = crate::config::ConfigArgs {
            id: Some("resync-resp-unsolicited-4864".to_string()),
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
            .set_own_addr_local_for_test("127.0.0.1:14200".parse().unwrap());

        // Stand-in contract handler: flips `update_query_seen` to true if it EVER
        // receives an UpdateQuery (proof the WASM-apply path was entered),
        // answering it so nothing hangs. For this test it must NEVER fire — any
        // event reaching the handler is the regression this guards against.
        let update_query_seen = std::sync::Arc::new(AtomicBool::new(false));
        let handler_flag = update_query_seen.clone();
        let _handler = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "a stand-in executor loop: it only serves the one \
                              query this test expects, and ContractHandlerEvent \
                              has 20+ variants — any other event reaching it is \
                              the regression under test, so it panics rather \
                              than falling through silently"
                )]
                let response = match ev {
                    ContractHandlerEvent::UpdateQuery { .. } => {
                        handler_flag.store(true, Ordering::SeqCst);
                        ContractHandlerEvent::UpdateResponse {
                            new_value: Ok(freenet_stdlib::prelude::WrappedState::new(vec![
                                1u8, 2, 3,
                            ])),
                            state_changed: true,
                        }
                    }
                    other => panic!(
                        "an unsolicited ResyncResponse must not reach the contract \
                         handler, got: {other:?}"
                    ),
                };
                if ch_channel.send_to_sender(id, response).await.is_err() {
                    break;
                }
            }
        });

        let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
            freenet_stdlib::prelude::ContractInstanceId::new([202u8; 32]),
            freenet_stdlib::prelude::CodeHash::new([203u8; 32]),
        );
        let source: SocketAddr = "127.0.0.1:15200".parse().unwrap();

        // NO outstanding entry seeded → the response is unsolicited.
        assert_eq!(
            op_manager.ring.outstanding_resync_requests.len(),
            0,
            "precondition: no outstanding ResyncRequest recorded for this (contract, source)"
        );

        crate::config::GlobalTestMetrics::reset();

        let response = handle_interest_sync_message(
            &op_manager,
            source,
            crate::message::InterestMessage::ResyncResponse {
                key,
                state_bytes: vec![1u8, 2, 3],
                summary_bytes: vec![4u8, 5, 6],
            },
        )
        .await;

        // Give the (should-be-idle) handler task a scheduling window: if the gate
        // had wrongly forwarded an UpdateQuery, it would surface now. (The arm
        // AWAITS the handler round-trip, so a leak would already have set the flag
        // before the call above returned; this is belt-and-suspenders.)
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert!(
            response.is_none(),
            "an unsolicited ResyncResponse must produce no reply"
        );
        assert!(
            !update_query_seen.load(Ordering::SeqCst),
            "SECURITY (#4864 round-8): the correlation gate must drop the unsolicited \
             ResyncResponse BEFORE any UpdateQuery / WASM apply — the contract handler \
             must never see it"
        );
        assert_eq!(
            crate::config::GlobalTestMetrics::resync_responses_unsolicited(),
            1,
            "the unsolicited-drop must be counted exactly once (#4864 round-8)"
        );
    }

    /// #4864 round-8 (Codex P1): a SOLICITED `ResyncResponse` — one that matches
    /// an outstanding `ResyncRequest` we recorded — passes the correlation gate
    /// and IS applied (reaches the contract handler's `UpdateQuery`). Crucially
    /// the gate `consume`s the entry on first match, so an identical REPLAY of the
    /// same response finds nothing and is dropped WITHOUT a second apply — replay
    /// is dead.
    ///
    /// Asserts (first call): the `UpdateQuery` reached the handler (applied), and
    /// the outstanding entry was consumed (`len() == 0`), and the unsolicited-drop
    /// metric stayed 0. (Replay call): returns `None`, NO second `UpdateQuery`, and
    /// the unsolicited-drop metric incremented to 1.
    #[tokio::test]
    async fn resync_response_solicited_applies_then_replay_is_suppressed() {
        use crate::contract::ContractHandlerEvent;
        use std::sync::atomic::{AtomicBool, Ordering};

        // Build a real OpManager (same harness as
        // resync_request_for_held_contract_passes_gate_and_responds).
        let config_args = crate::config::ConfigArgs {
            id: Some("resync-resp-solicited-4864".to_string()),
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
            .set_own_addr_local_for_test("127.0.0.1:14300".parse().unwrap());

        // Stand-in contract handler: answers UpdateQuery with a SUCCESSFUL,
        // state-changed merge (mirrors the ResyncResponse arm's
        // `UpdateResponse { new_value: Ok(_), state_changed: true, .. }` match)
        // and records that the apply was reached.
        let update_query_seen = std::sync::Arc::new(AtomicBool::new(false));
        let handler_flag = update_query_seen.clone();
        let _handler = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "a stand-in executor loop: it only serves the one \
                              query this test expects, and ContractHandlerEvent \
                              has 20+ variants — any other event reaching it is \
                              an unexpected-input panic, not a silent fallthrough"
                )]
                let response = match ev {
                    ContractHandlerEvent::UpdateQuery { .. } => {
                        handler_flag.store(true, Ordering::SeqCst);
                        ContractHandlerEvent::UpdateResponse {
                            new_value: Ok(freenet_stdlib::prelude::WrappedState::new(vec![
                                9u8, 8, 7,
                            ])),
                            state_changed: true,
                        }
                    }
                    other => {
                        panic!("unexpected handler event in solicited-resync stand-in: {other:?}")
                    }
                };
                if ch_channel.send_to_sender(id, response).await.is_err() {
                    break;
                }
            }
        });

        let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
            freenet_stdlib::prelude::ContractInstanceId::new([204u8; 32]),
            freenet_stdlib::prelude::CodeHash::new([205u8; 32]),
        );
        let source: SocketAddr = "127.0.0.1:15300".parse().unwrap();

        // Seed the outstanding entry: WE sent a ResyncRequest to `source` for
        // `key`, so the incoming response is solicited.
        assert!(
            op_manager
                .ring
                .outstanding_resync_requests
                .record(*key.id(), source),
            "seeding the outstanding entry must correlate (map is empty here)"
        );
        assert_eq!(
            op_manager.ring.outstanding_resync_requests.len(),
            1,
            "precondition: exactly one outstanding ResyncRequest recorded"
        );

        crate::config::GlobalTestMetrics::reset();

        // FIRST delivery: solicited → passes the gate → applied.
        let resp1 = handle_interest_sync_message(
            &op_manager,
            source,
            crate::message::InterestMessage::ResyncResponse {
                key,
                state_bytes: vec![1u8, 2, 3],
                summary_bytes: vec![4u8, 5, 6],
            },
        )
        .await;
        assert!(
            resp1.is_none(),
            "a ResyncResponse never produces a reply (it applies locally)"
        );
        assert!(
            update_query_seen.load(Ordering::SeqCst),
            "a solicited ResyncResponse must be applied (reach the UpdateQuery apply)"
        );
        assert_eq!(
            op_manager.ring.outstanding_resync_requests.len(),
            0,
            "the correlation gate must CONSUME the outstanding entry on apply (#4864 round-8)"
        );
        assert_eq!(
            crate::config::GlobalTestMetrics::resync_responses_unsolicited(),
            0,
            "a solicited response must NOT be counted as unsolicited"
        );

        // REPLAY: byte-identical response. The entry was already consumed, so the
        // gate finds nothing and drops it — no second apply.
        update_query_seen.store(false, Ordering::SeqCst);
        let resp2 = handle_interest_sync_message(
            &op_manager,
            source,
            crate::message::InterestMessage::ResyncResponse {
                key,
                state_bytes: vec![1u8, 2, 3],
                summary_bytes: vec![4u8, 5, 6],
            },
        )
        .await;

        // Window for a wrongly-forwarded UpdateQuery to surface (belt-and-suspenders;
        // a real leak would have set the flag before the awaited call returned).
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert!(
            resp2.is_none(),
            "a replayed ResyncResponse must produce no reply"
        );
        assert!(
            !update_query_seen.load(Ordering::SeqCst),
            "SECURITY (#4864 round-8): a replayed ResyncResponse must NOT reach a second \
             UpdateQuery apply — consume-on-first-match makes replay dead"
        );
        assert_eq!(
            crate::config::GlobalTestMetrics::resync_responses_unsolicited(),
            1,
            "the replayed response must be counted as an unsolicited drop (#4864 round-8)"
        );
    }

    /// #4864 round-8 (Codex P1) source-scrape pin: the `ResyncResponse` receive
    /// arm MUST require-and-consume an outstanding `ResyncRequest`
    /// (`outstanding_resync_requests.consume(..)`) BEFORE it applies the state via
    /// `ContractHandlerEvent::UpdateQuery`. If the consume ever moves after (or is
    /// downgraded to a non-consuming peek), an unsolicited or replayed
    /// ResyncResponse would reach the un-backoff-gated full-state WASM merge —
    /// exactly the DoS surface the behavioral tests above guard against.
    #[test]
    fn resync_response_arm_consumes_outstanding_before_apply() {
        let src = include_str!("node.rs");
        let arm = src
            .find(r#"event = "resync_response_received""#)
            .expect("ResyncResponse receive anchor not found");
        let region = &src[arm..];
        let consume = region.find("outstanding_resync_requests").expect(
            "ResyncResponse arm must correlate via outstanding_resync_requests (#4864 round-8)",
        );
        let apply = region
            .find("ContractHandlerEvent::UpdateQuery")
            .expect("ResyncResponse arm must apply via UpdateQuery");
        assert!(
            consume < apply,
            "outstanding_resync_requests.consume MUST run BEFORE the UpdateQuery apply so an \
             unsolicited/replayed ResyncResponse never reaches WASM (#4864 round-8)"
        );
        assert!(
            region[consume..(consume + 120).min(region.len())].contains(".consume("),
            "the correlation must be a require-and-consume (.consume), not a peek"
        );
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
                arm.contains("clear_peer_summary"),
                "ResyncRequest handler no longer calls clear_peer_summary. \
                 #4145 caching relies on this handler clearing the sender's \
                 cached summary so a delta-apply failure forces a fresh \
                 full-state resend instead of looping on unappliable deltas."
            );
            // The clear MUST go through the tagged clear API, not a
            // `update_peer_summary` cache-write. Strip whitespace so the
            // multi-line call matches regardless of formatting.
            let collapsed: String = arm.chars().filter(|c| !c.is_whitespace()).collect();
            assert!(
                collapsed.contains("clear_peer_summary(&key,pk,"),
                "ResyncRequest handler must clear the cached summary via \
                 `clear_peer_summary`. Caching a summary here instead would \
                 defeat the #4145 backstop."
            );
            assert!(
                collapsed.contains("SummaryMissingReason::ClearedByResync"),
                "the clear must be tagged ClearedByResync — #4961 needs the \
                 full_no_their_summary_tracked arm attributed per clear path, \
                 and an untagged clear would silently land in another bucket"
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
            // Window-bound to the ResyncResponse RECEIVE-and-apply arm so the
            // far-away test literals below cannot satisfy the needles
            // (self-reference guard). Anchored on the receive-log event (assembled
            // via concat so this literal isn't self-matched) rather than the first
            // `InterestMessage::ResyncResponse {` — that matched the SEND-side
            // construction in the ResyncRequest arm, and the #4864 round-8 consume
            // gate inserted between the receive log and the apply pushed the
            // sub-arm markers past the old fixed window.
            let start = SOURCE
                .find(concat!("event = \"resync_response_", "received\""))
                .expect("ResyncResponse receive arm not found");
            // Window widened to 8000 (#4864 round-9 addendum): after the round-8
            // consume gate the margin to `state_changed: false,` was only ~125
            // bytes, so the next addition to the arm would push it past the bound
            // and fail with a misleading "sub-arm not found".
            let arm = &SOURCE[start..(start + 8000).min(SOURCE.len())];
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

        /// Source-scrape pin: the shadow-mode futile-repair attempt is recorded
        /// only where a heal is ACTUALLY emitted.
        ///
        /// The behavioural tests in `futile_repair_shadow` cannot see this: the
        /// harness contract is never banned, always has local state, and never
        /// exceeds the emit budget, so moving `record_repair_attempt` above
        /// those gates leaves them all green. But a heal we did not send is not
        /// a repair, and counting one would turn the detector into a measure of
        /// our own emit budget — the exact "metric re-derived away from the
        /// decision" failure in `.claude/rules/bug-prevention-patterns.md`.
        #[test]
        fn futile_repair_attempt_is_recorded_only_where_the_heal_is_emitted() {
            const SOURCE: &str = include_str!("node.rs");

            // Scoped to `emit_stale_peer_syncs`'s OWN body. An earlier revision
            // ran the window from the loop header to an anchor ~500 lines later
            // in the `Summaries` arm, so it stayed green for a
            // `record_repair_attempt` moved clean out of the function — which
            // is precisely the regression it names. The function's body ends at
            // the first `\n}` in column 0 after its signature, which is what a
            // top-level `fn` terminator looks like in this file.
            let fn_start = SOURCE
                .find("async fn emit_stale_peer_syncs(")
                .expect("emit_stale_peer_syncs not found — update this pin");
            let fn_end = fn_start
                + SOURCE[fn_start..]
                    .find("\n}\n")
                    .expect("end of emit_stale_peer_syncs not found")
                + 1;
            let body = &SOURCE[fn_start..fn_end];
            let start = fn_start
                + body
                    .find("for contract in stale_contracts {")
                    .expect("stale-contract emission loop not found");
            let window = &SOURCE[start..fn_end];

            let record = window.find("record_repair_attempt(").expect(
                "the futile-repair attempt is no longer recorded in the heal \
                 emission loop — the detector cannot pair an outcome with an \
                 attempt that was never recorded",
            );
            for (gate, why) in [
                (
                    "if emitted >= emit_budget {",
                    "an over-budget contract is deferred, not healed",
                ),
                (
                    "is_banned(contract.id())",
                    "a banned contract is skipped, not healed",
                ),
                (
                    "Skipping stale-peer sync",
                    "a contract with no local state is skipped, not healed",
                ),
                (
                    "emitted += 1;",
                    "the attempt must be counted with the emission it belongs to",
                ),
            ] {
                let gate_pos = window.find(gate).unwrap_or_else(|| {
                    panic!("heal-loop gate `{gate}` not found — update this pin")
                });
                assert!(
                    gate_pos < record,
                    "record_repair_attempt must come AFTER `{gate}`: {why}, so \
                     charging the edge for it makes the futile-repair detector \
                     measure our own emit budget instead of whether repair works"
                );
            }
        }

        /// Source-scrape pin: the outcome sites must pass the COMPARISON
        /// VERDICT, not a constant.
        ///
        /// This is the mutation the feature is defined against — a detector fed
        /// `false` unconditionally counts every repair as futile and is
        /// measuring load. `futile_repair_shadow` fails under that mutation
        /// too; this pin survives a `#[cfg(test)]` module being cut, and names
        /// the two observation sites so a third one cannot be added silently.
        #[test]
        fn futile_repair_outcome_sites_pass_the_staleness_verdict() {
            use crate::node::tests::code_only;

            const SOURCE: &str = include_str!("node.rs");
            let handler = SOURCE
                .find("async fn handle_interest_sync_message(")
                .expect("handle_interest_sync_message not found");
            // Bound the window to the handler region. Without this the pin
            // matches its OWN source (this test names every needle it looks
            // for) and passes no matter what the handler does — the
            // self-matching-needle trap.
            let handler_end = handler
                + SOURCE[handler..]
                    .find("\n#[cfg(test)]")
                    .or_else(|| SOURCE[handler..].find("\nmod tests {"))
                    .expect("end of handler region not found");
            let body = code_only(&SOURCE[handler..handler_end]);

            let sites: Vec<_> = body.match_indices("record_repair_outcome(").collect();
            assert_eq!(
                sites.len(),
                2,
                "expected exactly two futile-repair outcome sites (the \
                 `Summaries` two-sided comparison and the `SummaryDigests` \
                 agreement); found {}. A new site must be a genuinely \
                 two-sided comparison, or the detector starts scoring \
                 one-sided reports as successful repairs",
                sites.len()
            );
            let mut passes_tracked_evidence = 0usize;
            for (pos, _) in sites {
                let call = &body[pos..body[pos..]
                    .find(");")
                    .map(|o| pos + o)
                    .unwrap_or(body.len())];
                // Whitespace-stripped: rustfmt collapses or explodes an
                // argument list depending on how long the arguments are, so a
                // pin that matches raw source breaks the first time an argument
                // is renamed — and a broken pin gets weakened, not fixed. Only
                // the argument SEQUENCE is load-bearing here.
                let squashed_call: String = call.split_whitespace().collect();
                assert!(
                    squashed_call.contains("!is_stale"),
                    "a futile-repair outcome site passes something other than \
                     `!is_stale`: the verdict IS the detector — a constant \
                     there measures repair volume, not repair failure. Got: \
                     {call}"
                );
                // The verdict alone is not enough. `is_stale` also comes out
                // `true` when the per-message probe budget ran out or the delta
                // probe failed, neither of which is evidence about convergence,
                // and the first of those grows with peer breadth rather than
                // with brokenness. Every site must say which it is.
                assert!(
                    squashed_call.contains("evidence")
                        || squashed_call.contains("OutcomeEvidence::"),
                    "a futile-repair outcome site no longer passes an \
                     `OutcomeEvidence`: without it the load-correlated \
                     budget-exhausted default is counted as futility and the \
                     headline number tracks how busy this node is. Got: {call}"
                );
                if squashed_call.contains("!is_stale,evidence") {
                    passes_tracked_evidence += 1;
                }
            }
            assert_eq!(
                passes_tracked_evidence, 1,
                "exactly one outcome site (the `Summaries` arm) must pass the \
                 `evidence` TRACKED alongside the staleness decision. \
                 Hard-coding `OutcomeEvidence::Verdict` there re-derives the \
                 provenance away from the branch that took the default, which \
                 is the whole finding — only the `SummaryDigests` agreement \
                 arm may name a literal, and only because it compares \
                 byte-identical operands and can take no default"
            );
            // Whitespace-stripped so rustfmt wrapping the condition cannot
            // break the pin.
            let squashed: String = body.split_whitespace().collect();
            assert!(
                squashed.contains("our_summary.is_some()&&their_summary.is_some()"),
                "the `Summaries` outcome site no longer gates on a TWO-SIDED \
                 comparison — the one-sided arms return `false` for \"no basis \
                 to heal\", not for \"converged\", and feeding those in scores \
                 every contract we do not host as a successful repair"
            );
        }
    }

    // ───────────────────────────────────────────────────────────
    // Per-message cap on semantic-staleness WASM probes
    // (`MAX_STALENESS_PROBES_PER_SUMMARIES`). A peer sending crafted/novel
    // summary bytes for every hosted contract must not force unbounded
    // `get_state_delta` execution on the serial contract loop (#4857
    // secondary-finding DoS hardening). Cache hits are free and never
    // consume the budget; only cache MISSES (which need WASM) are rationed,
    // and the overflow falls back to the conservative byte comparison.
    // ───────────────────────────────────────────────────────────
    mod staleness_probe_cap {
        use super::super::{
            MAX_STALENESS_PROBES_PER_SUMMARIES, StalenessProbeAction, plan_staleness_probe,
        };

        #[test]
        fn cache_hit_bypasses_budget() {
            // A cache hit does no WASM, so it is answered even far past the cap.
            assert_eq!(
                plan_staleness_probe(Some(true), 0),
                StalenessProbeAction::UseCached(true)
            );
            assert_eq!(
                plan_staleness_probe(Some(false), MAX_STALENESS_PROBES_PER_SUMMARIES * 10),
                StalenessProbeAction::UseCached(false)
            );
        }

        #[test]
        fn cache_miss_probes_until_budget_then_falls_back() {
            // Under budget: run the WASM probe.
            assert_eq!(
                plan_staleness_probe(None, 0),
                StalenessProbeAction::RunProbe
            );
            assert_eq!(
                plan_staleness_probe(None, MAX_STALENESS_PROBES_PER_SUMMARIES - 1),
                StalenessProbeAction::RunProbe
            );
            // At/over budget: stop probing, fall back to the byte comparison.
            assert_eq!(
                plan_staleness_probe(None, MAX_STALENESS_PROBES_PER_SUMMARIES),
                StalenessProbeAction::BudgetExhaustedFallBack
            );
            assert_eq!(
                plan_staleness_probe(None, MAX_STALENESS_PROBES_PER_SUMMARIES + 5),
                StalenessProbeAction::BudgetExhaustedFallBack
            );
        }

        /// Simulates the handler loop's counter over a run of byte-differing
        /// cache-MISS contracts (the DoS shape: novel bytes for every contract)
        /// and asserts WASM probes are hard-capped while the overflow falls back
        /// to the conservative byte compare.
        #[test]
        fn probe_budget_caps_wasm_probes_per_message() {
            let total_contracts = MAX_STALENESS_PROBES_PER_SUMMARIES * 3; // 96 > cap
            let mut probes_used = 0usize;
            let mut probed = 0usize;
            let mut fell_back = 0usize;
            for _ in 0..total_contracts {
                match plan_staleness_probe(None, probes_used) {
                    StalenessProbeAction::RunProbe => {
                        probes_used += 1;
                        probed += 1;
                    }
                    StalenessProbeAction::BudgetExhaustedFallBack => fell_back += 1,
                    StalenessProbeAction::UseCached(_) => {
                        unreachable!("every contract is a cache miss in this scenario")
                    }
                }
            }
            assert_eq!(
                probed, MAX_STALENESS_PROBES_PER_SUMMARIES,
                "WASM probes must be hard-capped at the per-message budget"
            );
            assert_eq!(
                fell_back,
                total_contracts - MAX_STALENESS_PROBES_PER_SUMMARIES,
                "every over-budget contract must fall back to byte-compare"
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

    /// Behavioural tests for the hash-first `InterestSync` exchange (#4965).
    ///
    /// The exchange replaces "ship every summary to every co-host every cycle"
    /// with "ship a digest, ship the bytes only on mismatch". The properties
    /// that matter, and that these tests pin:
    ///
    /// 1. A digest MATCH puts no summary bytes on the wire, yet still seeds the
    ///    peer-summary cache and still runs the staleness check.
    /// 2. A digest MISMATCH asks for the bytes, and the resulting `Summaries`
    ///    runs the untouched original handler — so the targeted
    ///    `SyncStateToPeer` heal still fires.
    /// 3. A peer that sends no digest at all (an old peer, or one with no
    ///    state) still converges.
    mod hash_first_summaries {
        use super::*;
        use crate::contract::{ContractHandlerEvent, StoreResponse};
        use crate::message::{InterestMessage, NodeEvent, SummaryDigestEntry, SummaryEntry};
        use crate::ring::interest::{PeerKey, contract_hash, summary_digest};
        use either::Either;
        use freenet_stdlib::prelude::{
            CodeHash, ContractInstanceId, ContractKey, StateSummary, WrappedState,
        };
        use std::net::SocketAddr;

        /// Everything a hash-first handler test needs, kept alive together.
        struct Harness {
            op_manager: Arc<OpManager>,
            notifications: crate::node::EventLoopNotificationsReceiver,
            /// The one contract the node hosts, is locally interested in, and
            /// can summarize.
            key: ContractKey,
            /// The summary the stand-in contract handler reports for `key`.
            our_summary: Vec<u8>,
            /// A connected peer, recorded at the hash-first version floor.
            new_peer: SocketAddr,
            /// A connected peer with NO recorded version (an old peer, as far
            /// as the fail-closed gate is concerned).
            old_peer: SocketAddr,
            /// How many `GetSummaryQuery` round trips the stand-in contract
            /// handler has answered. The observable for the amplification
            /// bound: it counts the EXPENSIVE operation directly, rather than
            /// a proxy that could stay flat while the work still happened.
            summary_queries: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            _guard: Box<dyn std::any::Any>,
        }

        impl Harness {
            fn peer_key_of(&self, addr: SocketAddr) -> PeerKey {
                PeerKey::from(
                    self.op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(addr)
                        .expect("peer must be connected")
                        .pub_key
                        .clone(),
                )
            }

            /// Every `SyncStateToPeer` heal emitted so far, as
            /// (contract, target). This is the observable the "PRESERVE THE
            /// HEAL" property is asserted on.
            fn drain_heals(&mut self) -> Vec<(ContractKey, SocketAddr)> {
                let mut out = Vec::new();
                while let Ok(ev) = self.notifications.notifications_receiver.try_recv() {
                    if let Either::Right(NodeEvent::SyncStateToPeer { key, target, .. }) = ev {
                        out.push((key, target));
                    }
                }
                out
            }
        }

        /// Build a node that hosts exactly one contract with a known summary,
        /// connected to one hash-first-capable peer and one pre-floor peer.
        ///
        /// `contract_state_present` returns `true` when no hosting storage is
        /// attached, so the `should_summarize_or_broadcast` gate is satisfied
        /// by `host_contract` alone and no redb temp dir is needed.
        /// What the stand-in contract handler answers a `GetDeltaQuery` — the
        /// semantic-staleness probe (#4857) — with.
        ///
        /// These are the three things `interest::peer_summary_has_pending_state`
        /// can return, and they produce three DIFFERENT provenances for one
        /// `is_stale: bool`. The futile-repair detector has to tell them apart
        /// (see `crate::ring::futile_repair::OutcomeEvidence`), so the harness
        /// has to be able to produce them.
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum DeltaBehavior {
            /// The contract holds state the peer lacks: a byte mismatch is a
            /// real divergence and heals. The default, and what every
            /// pre-existing test here assumes.
            NonEmpty,
            /// The contract says the peer is logically converged despite
            /// differing summary bytes — the non-deterministic-serialization
            /// shape #4857 exists for. A real verdict, and it is NOT stale.
            Empty,
            /// The probe produced no verdict at all.
            /// `summary_indicates_stale_peer` then falls back to the
            /// conservative byte compare, so the peer reads STALE on no
            /// evidence whatsoever.
            Failing,
        }

        async fn build_harness(id: &str, port_base: u16, our_summary: Vec<u8>) -> Harness {
            build_harness_with(id, port_base, our_summary, DeltaBehavior::NonEmpty).await
        }

        async fn build_harness_with(
            id: &str,
            port_base: u16,
            our_summary: Vec<u8>,
            delta_behavior: DeltaBehavior,
        ) -> Harness {
            let config_args = crate::config::ConfigArgs {
                id: Some(id.to_string()),
                mode: Some(crate::contract::OperationMode::Local),
                ..Default::default()
            };
            let node_config = NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
            let (notifications, notification_tx) = crate::node::event_loop_notification_channel();
            let (ops_ch_channel, mut ch_channel, wait_for_event) =
                crate::contract::contract_handler_channel();
            let connection_manager = crate::ring::ConnectionManager::new(&node_config);
            let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
            let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
            let op_manager = Arc::new(
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
            let self_addr: SocketAddr = format!("127.0.0.1:{port_base}").parse().unwrap();
            op_manager
                .ring
                .connection_manager
                .set_own_addr_local_for_test(self_addr);

            let key = ContractKey::from_id_and_code(
                ContractInstanceId::new([42u8; 32]),
                CodeHash::new([43u8; 32]),
            );
            // Hosted + locally interested + hash-indexed, so
            // `summary_if_hosted_or_in_use` will summarize it and
            // `lookup_by_hash` will resolve the peer's advertised hash to it.
            let _ = op_manager.ring.host_contract(
                key,
                128,
                crate::ring::AccessType::Put,
                crate::ring::HostingCause::Other,
            );
            op_manager.interest_manager.register_local_hosting(&key);

            // Two connected peers, distinguished ONLY by whether a remote
            // version was recorded — the exact production discriminator.
            let new_peer: SocketAddr = format!("127.0.0.1:{}", port_base + 1).parse().unwrap();
            let old_peer: SocketAddr = format!("127.0.0.1:{}", port_base + 2).parse().unwrap();
            for (i, addr) in [new_peer, old_peer].into_iter().enumerate() {
                let pub_key = crate::transport::TransportPublicKey::from_bytes([(i as u8) + 1; 32]);
                assert!(
                    op_manager.ring.connection_manager.add_connection(
                        crate::ring::Location::new(0.1 + (i as f64) * 0.1),
                        addr,
                        pub_key,
                        false,
                    ),
                    "test peer must be admitted to the ring"
                );
            }
            op_manager.ring.connection_manager.record_remote_version(
                new_peer,
                Some(crate::node::HASH_FIRST_SUMMARIES_MIN_VERSION),
            );

            let summary_for_handler = our_summary.clone();
            let handler_key = key;
            let summary_queries = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let queries_for_handler = std::sync::Arc::clone(&summary_queries);
            let handler = tokio::spawn(async move {
                while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                    #[allow(
                        clippy::wildcard_enum_match_arm,
                        reason = "a stand-in executor loop: it only serves the \
                                  three queries this test issues, and \
                                  ContractHandlerEvent has 20+ variants — any \
                                  other event reaching it is an unexpected-input \
                                  panic, not a silent fallthrough"
                    )]
                    let response = match ev {
                        ContractHandlerEvent::GetSummaryQuery { key } => {
                            queries_for_handler.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            ContractHandlerEvent::GetSummaryResponse {
                                key,
                                summary: Ok(StateSummary::from(summary_for_handler.clone())),
                            }
                        }
                        ContractHandlerEvent::GetQuery { .. } => {
                            ContractHandlerEvent::GetResponse {
                                key: Some(handler_key),
                                response: Ok(StoreResponse {
                                    state: Some(WrappedState::new(vec![1u8, 2, 3])),
                                    contract: None,
                                }),
                            }
                        }
                        // The semantic-staleness probe (#4857). The default
                        // (`NonEmpty`) is the contract answering "yes, I hold
                        // state this peer lacks", which is what turns a byte
                        // mismatch into a real heal — returning an empty delta
                        // by default would make every divergence test vacuous.
                        // The other two variants exist so a test can produce
                        // the OTHER two provenances of `is_stale`; see
                        // `DeltaBehavior`.
                        ContractHandlerEvent::GetDeltaQuery { key, .. } => {
                            let delta = match delta_behavior {
                                DeltaBehavior::NonEmpty => {
                                    Ok(freenet_stdlib::prelude::StateDelta::from(vec![1u8, 2, 3]))
                                }
                                DeltaBehavior::Empty => {
                                    Ok(freenet_stdlib::prelude::StateDelta::from(Vec::<u8>::new()))
                                }
                                DeltaBehavior::Failing => {
                                    Err(crate::contract::ExecutorError::other(
                                        crate::contract::ContractQueueFull,
                                    ))
                                }
                            };
                            ContractHandlerEvent::GetDeltaResponse { key, delta }
                        }
                        other => panic!("unexpected handler event: {other:?}"),
                    };
                    if ch_channel.send_to_sender(id, response).await.is_err() {
                        break;
                    }
                }
            });

            let guard: Box<dyn std::any::Any> =
                Box::new((handler, result_router_rx, task_monitor, wait_for_event));
            Harness {
                op_manager,
                notifications,
                key,
                our_summary,
                new_peer,
                old_peer,
                summary_queries,
                _guard: guard,
            }
        }

        /// A hash-first-capable peer asking for our interests gets DIGESTS
        /// back; a peer whose version we don't know gets the full bytes.
        ///
        /// This is the backward-compatibility property stated as behaviour
        /// rather than as a predicate: the same node, the same contract, the
        /// same handler call — only the recorded peer version differs, and the
        /// old peer still receives a message it can decode.
        #[tokio::test]
        async fn interests_reply_is_digests_only_for_capable_peers() {
            let mut h = build_harness("hf-reply-form", 17000, vec![7u8; 64]).await;
            let hash = contract_hash(&h.key);

            let to_new = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Interests { hashes: vec![hash] },
            )
            .await;
            match to_new {
                Some(InterestMessage::SummaryDigests { entries, .. }) => {
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0].hash, hash);
                    assert_eq!(
                        entries[0].summary_digest,
                        Some(summary_digest(&h.our_summary)),
                        "the digest must be computed from our ACTUAL summary"
                    );
                }
                other => panic!("expected SummaryDigests for a capable peer, got {other:?}"),
            }

            let to_old = handle_interest_sync_message(
                &h.op_manager,
                h.old_peer,
                InterestMessage::Interests { hashes: vec![hash] },
            )
            .await;
            match to_old {
                Some(InterestMessage::Summaries { entries, .. }) => {
                    assert_eq!(entries.len(), 1);
                    assert_eq!(
                        entries[0].summary_bytes.as_deref(),
                        Some(h.our_summary.as_slice()),
                        "a peer with an unknown version must still receive the \
                         full summary bytes — it cannot decode SummaryDigests \
                         and would drop the connection"
                    );
                }
                other => panic!("expected full Summaries for a pre-floor peer, got {other:?}"),
            }

            assert!(
                h.drain_heals().is_empty(),
                "answering an Interests query must not emit any heal"
            );
        }

        /// The 98.1% case: the peer advertises a digest equal to ours.
        ///
        /// Asserts all three halves of "cheap AND correct":
        /// - no `SummaryRequest` goes back, so NO summary bytes cross the wire;
        /// - the peer-summary cache is seeded with the agreed bytes (#4952), so
        ///   the peer does not stay a full-state broadcast target;
        /// - no heal fires, because there is nothing to heal.
        #[tokio::test]
        async fn matching_digest_costs_no_bytes_and_still_seeds_the_summary_cache() {
            let mut h = build_harness("hf-agree", 17010, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);
            let pk = h.peer_key_of(h.new_peer);

            assert!(
                h.op_manager
                    .interest_manager
                    .get_peer_summary(&h.key, &pk)
                    .is_none(),
                "precondition: we hold no cached summary for this peer"
            );

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![SummaryDigestEntry {
                        hash,
                        summary_digest: Some(summary_digest(&h.our_summary)),
                    }],
                },
            )
            .await;

            assert!(
                reply.is_none(),
                "a digest that matches must produce NO follow-up message — not \
                 one summary byte on the wire. Got {reply:?}"
            );
            assert_eq!(
                h.op_manager
                    .interest_manager
                    .get_peer_summary(&h.key, &pk)
                    .as_deref()
                    .map(|s| s.to_vec()),
                Some(h.our_summary.clone()),
                "the agreed summary must still be cached for the peer (#4952): \
                 the digest PROVED these are the bytes it holds, so skipping \
                 the seeding would leave it a full-state broadcast target"
            );
            assert!(
                h.drain_heals().is_empty(),
                "two peers that agree must not trigger a state push"
            );
        }

        /// A digest we cannot match must ask for the bytes — and the answer to
        /// that request must be the FULL-BYTES `Summaries`, whose untouched
        /// handler then emits the targeted heal.
        ///
        /// This is the "PRESERVE THE HEAL" property end to end: the digest
        /// exchange defers the decision rather than making it, so divergence
        /// still reaches exactly the same `SyncStateToPeer` it reaches today,
        /// one round trip later.
        #[tokio::test]
        async fn mismatching_digest_requests_bytes_and_the_heal_still_fires() {
            let mut h = build_harness("hf-mismatch", 17020, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);

            // Leg 1: peer advertises a digest of DIFFERENT state.
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![SummaryDigestEntry {
                        hash,
                        summary_digest: Some(summary_digest(b"some other state")),
                    }],
                },
            )
            .await;
            match &reply {
                Some(InterestMessage::SummaryRequest { hashes }) => {
                    assert_eq!(hashes, &vec![hash]);
                }
                other => panic!("a mismatching digest must request bytes, got {other:?}"),
            }
            assert!(
                h.drain_heals().is_empty(),
                "the digest leg must not heal on its own — it has not yet seen \
                 the peer's summary, so any heal here would be guesswork"
            );

            // Leg 2: the peer answers our request. It must answer with real
            // bytes, never with more digests (that would loop).
            let their_summary = vec![6u8; 128];
            let answer = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryRequest { hashes: vec![hash] },
            )
            .await;
            match &answer {
                Some(InterestMessage::Summaries { entries, .. }) => {
                    assert_eq!(
                        entries[0].summary_bytes.as_deref(),
                        Some(h.our_summary.as_slice()),
                        "a SummaryRequest must be answered with the real bytes"
                    );
                }
                other => panic!(
                    "a SummaryRequest must be answered with full-bytes Summaries \
                     (answering with digests would loop), got {other:?}"
                ),
            }

            // Leg 3: the peer's bytes arrive here, through the ORIGINAL
            // handler. The heal fires exactly as it does on today's main.
            let follow_up = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Summaries {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![SummaryEntry {
                        hash,
                        summary_bytes: Some(their_summary.clone()),
                    }],
                },
            )
            .await;
            assert!(follow_up.is_none(), "Summaries never replies");
            assert_eq!(
                h.drain_heals(),
                vec![(h.key, h.new_peer)],
                "once the bytes arrive, the targeted SyncStateToPeer heal MUST \
                 fire — hash-first defers the heal by one round trip, it does \
                 not remove it"
            );
            let pk = h.peer_key_of(h.new_peer);
            assert_eq!(
                h.op_manager
                    .interest_manager
                    .get_peer_summary(&h.key, &pk)
                    .as_deref()
                    .map(|s| s.to_vec()),
                Some(their_summary),
                "their real summary must be cached once received"
            );
        }

        // ===== #5155: bounded, rotating full-bytes summary fallback =====

        /// Host `n` additional contracts, all summarizable by the harness's
        /// stand-in handler, and return them in advertisement order.
        fn host_many(h: &Harness, n: u32) -> Vec<ContractKey> {
            let mut keys = Vec::with_capacity(n as usize);
            for i in 0..n {
                let mut id = [0u8; 32];
                id[0..4].copy_from_slice(&i.to_le_bytes());
                id[4] = 0x5A;
                let mut code = [0u8; 32];
                code[0..4].copy_from_slice(&i.to_le_bytes());
                code[4] = 0xC0;
                let key =
                    ContractKey::from_id_and_code(ContractInstanceId::new(id), CodeHash::new(code));
                let _ = h.op_manager.ring.host_contract(
                    key,
                    128,
                    crate::ring::AccessType::Put,
                    crate::ring::HostingCause::Other,
                );
                h.op_manager.interest_manager.register_local_hosting(&key);
                keys.push(key);
            }
            keys
        }

        /// Advertised hashes for `keys`, asserting they are pairwise distinct
        /// so entry counts can be compared against contract counts.
        fn distinct_hashes(keys: &[ContractKey]) -> Vec<u32> {
            let hashes: Vec<u32> = keys.iter().map(contract_hash).collect();
            assert_eq!(
                hashes.iter().copied().collect::<HashSet<u32>>().len(),
                hashes.len(),
                "premise: the fixture's contract hashes must not collide, or \
                 entry counts stop being comparable to contract counts"
            );
            hashes
        }

        fn summary_bytes_of(entries: &[SummaryEntry]) -> usize {
            entries
                .iter()
                .filter_map(|e| e.summary_bytes.as_ref())
                .map(|b| b.len())
                .sum()
        }

        /// The fallback reply is bounded at the widest shared set observed in
        /// the field (2,448 contracts), not merely at the fleet average.
        ///
        /// This is the whole point of #5155: `get_matching_contracts` returned
        /// one full summary per shared contract with no send-side cap, and the
        /// largest single message seen in production was 1.26 MB. The bound has
        /// to hold at the tail, because the tail is where the bytes are.
        #[tokio::test]
        async fn fallback_reply_is_bounded_at_the_widest_observed_shared_set() {
            let h = build_harness("hf-bound-wide", 17100, vec![7u8; 64]).await;
            let keys = host_many(&h, 2448);
            let hashes = distinct_hashes(&keys);

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.old_peer,
                InterestMessage::Interests {
                    hashes: hashes.clone(),
                },
            )
            .await;

            match reply {
                Some(InterestMessage::Summaries { entries, .. }) => {
                    assert!(
                        !entries.is_empty(),
                        "the reply must still carry something — a bound that \
                         sends nothing is not a bound, it is a silent outage"
                    );
                    // ABSOLUTE literals, deliberately not the constants. An
                    // assertion written against the constant that produced the
                    // value moves with any regression to it and can never fail:
                    // raising the cap to 100k would leave `entries.len() <=
                    // MAX_FALLBACK_SUMMARIES_PER_REPLY` true and this test
                    // green while the field message went back to megabytes.
                    assert!(
                        entries.len() <= 64,
                        "fallback reply carried {} entries against {} shared \
                         contracts; the intended cap is 64",
                        entries.len(),
                        hashes.len()
                    );
                    // The byte bound is `budget + one entry` by construction:
                    // the budget is checked before each entry and never blocks
                    // the first. See `MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`.
                    let bytes = summary_bytes_of(&entries);
                    assert!(
                        bytes <= 9 * 1024 + h.our_summary.len(),
                        "fallback reply carried {bytes} summary bytes, over the \
                         intended 9 KiB budget plus one entry"
                    );
                    // Pin which limit is doing the work in THIS fixture, so a
                    // regression to either one is caught rather than masked by
                    // the other. 64-byte summaries: 64 entries is 4 KB, well
                    // under the byte budget, so the entry cap must bind exactly.
                    assert_eq!(
                        entries.len(),
                        64,
                        "premise: with 64-byte summaries the ENTRY cap should be \
                         the binding constraint at 2,448 shared contracts"
                    );
                }
                other => panic!("a version-unknown peer must get full bytes, got {other:?}"),
            }
        }

        /// The entry cap alone does not bound the message, so the byte budget
        /// has to bind first when summaries are large.
        ///
        /// Summaries are ~16.7 KB for a River room and near-zero for a contract
        /// we do not host. The 143 B/entry the entry cap is derived from is a
        /// fleet AVERAGE; 64 heavy entries is ~1.07 MB, which is the message
        /// class this change exists to remove. Without this test the cap could
        /// regress to entries-only and every other assertion here would still
        /// pass.
        #[tokio::test]
        async fn fallback_byte_budget_binds_before_the_entry_cap() {
            let h = build_harness("hf-bound-bytes", 17110, vec![9u8; 5000]).await;
            let keys = host_many(&h, 200);
            let hashes = distinct_hashes(&keys);

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.old_peer,
                InterestMessage::Interests { hashes },
            )
            .await;

            match reply {
                Some(InterestMessage::Summaries { entries, .. }) => {
                    let bytes = summary_bytes_of(&entries);
                    assert!(
                        bytes > 0,
                        "premise: the fixture's contracts must actually be \
                         summarizable, or the byte budget is untested"
                    );
                    assert!(
                        entries.len() < MAX_FALLBACK_SUMMARIES_PER_REPLY,
                        "with 5 KB summaries the BYTE budget must bind before \
                         the {MAX_FALLBACK_SUMMARIES_PER_REPLY}-entry cap; got \
                         {} entries",
                        entries.len()
                    );
                    assert!(
                        bytes <= MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY + h.our_summary.len(),
                        "{bytes} summary bytes exceeds the budget plus one entry"
                    );
                }
                other => panic!("expected full bytes, got {other:?}"),
            }
        }

        /// A single summary larger than the whole budget still goes out, and
        /// the rotation still advances past it.
        ///
        /// The alternative — refusing any entry that would breach the budget —
        /// makes an oversized contract a permanent hole: it is never
        /// advertised, and because the cursor never passes it, nothing behind
        /// it is either. A stalled rotation is far worse than one oversized
        /// message, so the budget never blocks the first entry.
        #[tokio::test]
        async fn oversized_summary_still_sends_and_the_rotation_advances() {
            let h = build_harness("hf-bound-oversize", 17120, vec![3u8; 20_000]).await;
            assert!(
                h.our_summary.len() > MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY,
                "premise: this fixture's summary must exceed the whole budget"
            );
            let keys = host_many(&h, 8);
            let hashes = distinct_hashes(&keys);

            // Seed the cursor so both rounds are mid-cycle and the starting
            // offset is fixed. Without this the first round would begin at a
            // random cycle-boundary offset (see `summary_window_start`) and a
            // start on the last contract would wrap into a second random draw,
            // making the "moved on to a different contract" assertion flaky.
            let mut sorted = keys.clone();
            sorted.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
            h.op_manager
                .interest_manager
                .record_summary_cursor(&h.peer_key_of(h.old_peer), *sorted[0].id());

            let mut seen: Vec<u32> = Vec::new();
            for round in 0..2 {
                let reply = handle_interest_sync_message(
                    &h.op_manager,
                    h.old_peer,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await;
                match reply {
                    Some(InterestMessage::Summaries { entries, .. }) => {
                        assert_eq!(
                            entries.len(),
                            1,
                            "round {round}: exactly one oversized entry should \
                             fit — more would breach the budget, none would stall"
                        );
                        seen.push(entries[0].hash);
                    }
                    other => panic!("round {round}: expected full bytes, got {other:?}"),
                }
            }
            assert_ne!(
                seen[0], seen[1],
                "the rotation must move on to the next contract rather than \
                 re-sending the same oversized one every heartbeat"
            );
        }

        // ===== #5238: the digest path is bounded by CALLS, not by bytes =====

        /// The digest reply is bounded too, and the observable is the number of
        /// `GetSummaryQuery` round trips it makes — not the entry count.
        ///
        /// This is the regression test for #5238, and it deliberately asserts
        /// on the EXPENSIVE operation rather than on the message. #5155 read
        /// this path as cheap because a digest is 21 bytes; the cost that
        /// stormed is the `summary_if_hosted_or_in_use` call each entry needs
        /// to PRODUCE its digest, which is a contract-handler round trip that
        /// can re-enter WASM. An entry-count assertion alone would stay green
        /// under a regression that trimmed the reply after summarizing
        /// everything, which is exactly the shape that costs nothing to write
        /// and reproduces the storm in full.
        ///
        /// The fixture is the field case: 933 hosted contracts on the NATed
        /// peer in #5238, all shared with one digest-capable neighbour.
        #[tokio::test]
        async fn digest_reply_bounds_the_summarize_calls_not_just_the_entries() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-digest-bound", 17130, vec![7u8; 64]).await;
            let keys = host_many(&h, 933);
            let hashes = distinct_hashes(&keys);

            let before = h.summary_queries.load(Ordering::Relaxed);
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Interests {
                    hashes: hashes.clone(),
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            match reply {
                Some(InterestMessage::SummaryDigests { entries, .. }) => {
                    assert!(
                        !entries.is_empty(),
                        "the reply must still carry something — a bound that \
                         sends nothing is not a bound, it is a silent outage"
                    );
                    // ABSOLUTE literals, not the constants that produced them:
                    // an assertion written against its own constant moves with
                    // any regression to it and can never fail.
                    assert!(
                        entries.len() <= 64,
                        "digest reply carried {} entries against {} shared \
                         contracts; the intended cap is 64",
                        entries.len(),
                        hashes.len()
                    );
                    assert_eq!(
                        entries.len(),
                        64,
                        "premise: at 933 shared contracts the entry cap must be \
                         the binding constraint"
                    );
                }
                other => panic!("a hash-first-capable peer must get digests, got {other:?}"),
            }

            assert!(
                fetches > 0,
                "premise: the fixture must actually reach the summarize path, \
                 or the bound below can never fail. Entry count does not cover \
                 this — entries are pushed with a `None` summary when the gate \
                 declines, so a gate regression would leave `entries.len()` at \
                 64 and `fetches` at 0"
            );
            assert!(
                fetches <= 64,
                "building the digest reply made {fetches} summarize round trips \
                 against {} shared contracts. This is the #5238 storm: bounding \
                 the MESSAGE without bounding the LOOP leaves the CPU cost \
                 exactly where it was, because a digest costs a full summarize \
                 to produce",
                hashes.len()
            );
        }

        /// Successive digest replies rotate, so the whole shared set is still
        /// covered — in `ceil(n / cap)` heartbeats instead of one.
        ///
        /// This is the convergence half of the bound. A cap without rotation
        /// would advertise the same 64 contracts to that peer forever and never
        /// tell it about the rest, converting a CPU fix into permanent silent
        /// divergence. The cost that IS accepted is latency: at the 5-minute
        /// heartbeat, a peer sharing 933 contracts takes 15 rounds — about 75
        /// minutes — for anti-entropy to come back round to a given contract.
        /// (`MAX_DIGEST_SUMMARIES_PER_REPLY` quotes ~40 minutes for the same
        /// node: that is the ~450-contract SHARED set implied by the field
        /// measurement, where 933 is the HOSTED set, i.e. the worst case of a
        /// peer sharing interest in every contract we hold.) Live fan-out and
        /// the event-driven repairs are untouched and still carry every update
        /// anybody actually makes.
        ///
        /// # Why the CURSOR is seeded, not just the RNG
        ///
        /// `ceil(n / cap)` rounds tile the set exactly only while every round
        /// is mid-cycle. A round whose window ENDS on the highest id leaves the
        /// cursor past the end, which `summary_window_start` reads as a cycle
        /// boundary and answers with a fresh random offset. That is deliberate
        /// anti-starvation behaviour rather than a bug, but it re-covers ground
        /// already covered, and the exact-tiling assertion then fails.
        ///
        /// At n=200 and cap=64 that happens for 3 of the 200 possible starts,
        /// so the first version of this test failed about 1% of runs — and
        /// non-reproducibly, because `GlobalRng` falls back to `rand::rng()`
        /// when unseeded. It duly failed on the very next full-suite run.
        /// Seeding the cursor fixes the start at index 1 and removes the
        /// boundary re-draw from the schedule entirely, so the test pins the
        /// tiling property itself rather than one lucky seed. The RNG is seeded
        /// as well, so that any future edit reintroducing a draw stays
        /// reproducible instead of flaky.
        #[tokio::test]
        async fn digest_rotation_covers_the_whole_shared_set() {
            // Guarded rather than bare: `set_seed` also pins THREAD_INDEX to 0,
            // and `config.rs` asks callers to pair it with `clear_seed`. The
            // guard does that on unwind too, so a panicking assertion below
            // cannot leave the seed pinned for whatever runs next on this
            // thread.
            let _seed = crate::config::GlobalRng::seed_guard(0x5238_D16E);
            let h = build_harness("hf-digest-rotate", 17150, vec![7u8; 64]).await;
            let keys = host_many(&h, 200);
            let hashes = distinct_hashes(&keys);
            let expected: HashSet<u32> = hashes.iter().copied().collect();
            let rounds = hashes.len().div_ceil(MAX_DIGEST_SUMMARIES_PER_REPLY);

            let mut sorted = keys.clone();
            sorted.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
            h.op_manager
                .interest_manager
                .record_summary_cursor(&h.peer_key_of(h.new_peer), *sorted[0].id());

            let mut covered: HashSet<u32> = HashSet::new();
            for round in 0..rounds {
                let reply = handle_interest_sync_message(
                    &h.op_manager,
                    h.new_peer,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await;
                match reply {
                    Some(InterestMessage::SummaryDigests { entries, .. }) => {
                        // Holds BY FIXTURE, not by the cap: since #5338 a reply
                        // is bounded at 64 SUMMARIZE CALLS and at
                        // MAX_SUMMARY_ENTRIES_PER_MESSAGE entries, so a reply may
                        // legitimately carry more than 64 entries. Every contract
                        // here is hosted, so every entry is costed and the
                        // summarize budget is what binds. Do not read this as
                        // "replies are capped at 64 entries".
                        assert!(entries.len() <= MAX_DIGEST_SUMMARIES_PER_REPLY);
                        covered.extend(entries.iter().map(|e| e.hash));
                    }
                    other => panic!("round {round}: expected digests, got {other:?}"),
                }
            }

            assert_eq!(
                covered.len(),
                expected.len(),
                "after {rounds} rounds (ceil({}/{MAX_DIGEST_SUMMARIES_PER_REPLY})) \
                 the digest rotation covered {} of {} shared contracts — the \
                 window is not advancing, so the cap is starving the tail",
                hashes.len(),
                covered.len(),
                expected.len()
            );
            assert_eq!(covered, expected);
        }

        // ===== #5338: the window is as wide as it is advertised to be =====

        /// Hashes carried by a `SummaryDigests` reply, in wire order.
        fn digest_hashes(reply: Option<InterestMessage>) -> Vec<u32> {
            match reply {
                Some(InterestMessage::SummaryDigests { entries, .. }) => {
                    entries.iter().map(|e| e.hash).collect()
                }
                other => panic!("expected a digest reply, got {other:?}"),
            }
        }

        /// A peer that resumes on a NEW SOURCE PORT keeps its place in the
        /// rotation.
        ///
        /// The cursor was keyed by `SocketAddr` until #5338, so a NATed peer
        /// reconnecting on a fresh port lost it outright — no LRU pressure
        /// needed. A missing cursor is not a missing optimisation: it re-draws a
        /// RANDOM offset (deliberately, as anti-starvation), which turns the
        /// advertised contiguous `ceil(n / 64)` tiling into coupon-collector,
        /// roughly 90 minutes rather than 40 at n = 450. The population that hit
        /// hardest was the frequently-reconnecting NATed peer #5238 was measured
        /// on, so the headline convergence number was least accurate exactly
        /// where it was validated.
        ///
        /// The assertion is on the EXACT window both rounds carry, not merely
        /// that round 2 differs from round 1: a random re-draw agrees with the
        /// contiguous answer 1 time in 200 here, and "not equal to the previous
        /// window" would pass under the bug on all the other 199.
        #[tokio::test]
        async fn summary_window_cursor_follows_the_peer_not_its_address() {
            // Seeded because BOTH paths draw here: round one has no cursor, so
            // it takes the cycle-boundary arm and draws an offset, and a
            // regression's re-draw on round two must be reproducible rather than
            // flaky-red. (An earlier comment claimed the fixed path never draws,
            // which was wrong — the fix changes WHICH rounds draw, not whether
            // any does.)
            let _seed = crate::config::GlobalRng::seed_guard(0x5338_ADD8);
            let h = build_harness("hf-cursor-identity", 17200, vec![7u8; 64]).await;
            let keys = host_many(&h, 200);
            let hashes = distinct_hashes(&keys);
            let mut sorted = keys.clone();
            sorted.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
            // 64 hashes from `start`, WRAPPING — `rotation_window_indices`
            // wraps, and a window that runs off the end mid-cycle is ordinary.
            let expected_from = |start: usize| -> Vec<u32> {
                (0..64)
                    .map(|i| contract_hash(&sorted[(start + i) % sorted.len()]))
                    .collect()
            };

            // The first round is driven through the handler rather than by
            // seeding the cursor directly, so that the SECOND round's assertion
            // is the one a regression trips. Seeding via
            // `record_summary_cursor` would make a re-keyed cursor miss on
            // round one too, and the test would then fail on its own premise
            // without ever exercising the reconnect.
            let pk = h.peer_key_of(h.new_peer);
            let first = digest_hashes(
                handle_interest_sync_message(
                    &h.op_manager,
                    h.new_peer,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await,
            );
            assert_eq!(
                first.len(),
                64,
                "premise: the first round must fill the whole entry cap"
            );
            // Where the second round MUST resume: immediately after the last id
            // the first one sent. Derived from what round one actually did, so
            // it is an exact expectation without depending on which offset the
            // cycle-boundary draw picked.
            let resume = sorted
                .iter()
                .position(|k| contract_hash(k) == *first.last().expect("64 entries"))
                .expect("the last hash sent must be one of ours")
                + 1;
            assert!(
                resume < sorted.len(),
                "premise: round one must not end on the highest id (resume=\
                 {resume}), or round two is at a CYCLE BOUNDARY and re-draws a \
                 random offset legitimately, proving nothing about the cursor"
            );

            // The SAME peer reappears on a different source port. Public key
            // unchanged — that is what makes it the same peer — and the
            // previous address is left registered because the harness has no
            // disconnect hook; the reply is built for whichever address the
            // request arrives on, which is the whole point.
            let resumed_addr: SocketAddr = "127.0.0.1:17209".parse().unwrap();
            assert!(
                h.op_manager.ring.connection_manager.add_connection(
                    crate::ring::Location::new(0.42),
                    resumed_addr,
                    pk.0.clone(),
                    false,
                ),
                "the reconnecting peer must be admitted to the ring"
            );
            h.op_manager.ring.connection_manager.record_remote_version(
                resumed_addr,
                Some(crate::node::HASH_FIRST_SUMMARIES_MIN_VERSION),
            );
            assert_eq!(
                h.peer_key_of(resumed_addr),
                pk,
                "premise: the new address must resolve to the SAME peer key, or \
                 this test is about two different peers and proves nothing"
            );

            let second = digest_hashes(
                handle_interest_sync_message(
                    &h.op_manager,
                    resumed_addr,
                    InterestMessage::Interests { hashes },
                )
                .await,
            );
            assert_eq!(
                second,
                expected_from(resume),
                "the rotation must resume where the previous reply to this PEER \
                 stopped. Keying the cursor by address instead loses it on every \
                 reconnect and restarts the cycle at a random offset"
            );
            assert_eq!(
                h.op_manager
                    .interest_manager
                    .peek_summary_cursor(&pk)
                    .as_ref(),
                Some(sorted[(resume + 63) % sorted.len()].id()),
                "and the advanced cursor must be stored against the peer, not \
                 against the address it happened to arrive from"
            );
        }

        /// Contracts we track but do NOT host must not consume slots of the
        /// summarize budget — and must still be advertised.
        ///
        /// `matching` comes from `contract_hash_index`, which peer interest
        /// registrations populate as well as our own hosting, so it contains
        /// contracts we track and cannot summarize. Those return `None` through
        /// the in-memory gate with no contract-handler round trip, yet the build
        /// loop charged them a slot of a budget that exists to bound exactly
        /// that round trip. Convergence latency for the contracts we CAN
        /// advertise therefore scaled with `|matching|` while the cost being
        /// bounded scaled only with the hosted subset.
        ///
        /// # What each assertion rules out
        ///
        /// The fixture alternates hosted and tracked-only contracts in id order,
        /// so a slot-charging regression halves the hosted coverage. Three
        /// independent observables are pinned, because no one of them
        /// distinguishes the two ways this can go wrong:
        ///
        /// - `fetches` — the budget is still spent in full, and still only 64.
        ///   Goes to 32 if free entries are charged again.
        /// - the `Some` entries — the advertised set is the whole hosted
        ///   subset, not half of it.
        /// - the `None` entries — the free entries are still SENT. They drive
        ///   `clear_peer_summary` through `DigestVerdict::PeerHasNoState`, so
        ///   "don't charge it" must not become "don't send it"; that regression
        ///   would leave `fetches` at 64 and only this assertion would notice.
        #[tokio::test]
        async fn non_hosted_entries_do_not_consume_summarize_slots() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-free-entries", 17220, vec![7u8; 64]).await;
            let tracker = h.peer_key_of(h.old_peer);

            // Big-endian index in the leading bytes, so sorted-by-id order is
            // exactly `j` order and the interleave is a property of the fixture
            // rather than of how the ids happen to hash.
            let mut all = Vec::with_capacity(128);
            let mut hosted = Vec::new();
            let mut tracked = Vec::new();
            for j in 0..128u32 {
                let mut id = [0u8; 32];
                id[0..4].copy_from_slice(&j.to_be_bytes());
                id[4] = 0x99;
                let key = ContractKey::from_id_and_code(
                    ContractInstanceId::new(id),
                    CodeHash::new([0xC1; 32]),
                );
                if j % 2 == 0 {
                    let _ = h.op_manager.ring.host_contract(
                        key,
                        128,
                        crate::ring::AccessType::Put,
                        crate::ring::HostingCause::Other,
                    );
                    h.op_manager.interest_manager.register_local_hosting(&key);
                    hosted.push(key);
                } else {
                    // Indexed by a DIFFERENT peer's interest and never hosted
                    // here: the exact shape the build loop was charging for.
                    h.op_manager.interest_manager.register_peer_interest_from(
                        &key,
                        tracker.clone(),
                        None,
                        false,
                        crate::ring::interest::InterestRegistrationSource::Interests,
                    );
                    tracked.push(key);
                }
                all.push(key);
            }
            let hashes = distinct_hashes(&all);
            assert_eq!((hosted.len(), tracked.len()), (64, 64));

            // An id below every contract in the fixture, so the window starts
            // at index 0 with no random draw.
            let pk = h.peer_key_of(h.new_peer);
            h.op_manager
                .interest_manager
                .record_summary_cursor(&pk, ContractInstanceId::new([0u8; 32]));

            let before = h.summary_queries.load(Ordering::Relaxed);
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Interests { hashes },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            let entries = match reply {
                Some(InterestMessage::SummaryDigests { entries, .. }) => entries,
                other => panic!("expected a digest reply, got {other:?}"),
            };
            let advertised: Vec<u32> = entries
                .iter()
                .filter(|e| e.summary_digest.is_some())
                .map(|e| e.hash)
                .collect();
            let free: Vec<u32> = entries
                .iter()
                .filter(|e| e.summary_digest.is_none())
                .map(|e| e.hash)
                .collect();

            assert_eq!(
                fetches, 64,
                "the reply must spend its whole 64-call budget on contracts it \
                 can actually summarize; charging the tracked-only contracts a \
                 slot leaves half the budget buying nothing"
            );
            assert_eq!(
                advertised,
                hosted.iter().map(contract_hash).collect::<Vec<u32>>(),
                "one round must advertise all 64 hosted contracts, in id order"
            );
            assert_eq!(
                free,
                tracked[..63]
                    .iter()
                    .map(contract_hash)
                    .collect::<Vec<u32>>(),
                "the tracked-only contracts encountered on the way must still be \
                 SENT — they are what clears a peer's stale belief that we hold \
                 state (`DigestVerdict::PeerHasNoState`). Not charging them a \
                 slot must not turn into not sending them"
            );
            assert_eq!(
                entries.len(),
                127,
                "64 charged entries plus the 63 free ones interleaved between \
                 them; the loop stops on the 64th charged entry rather than \
                 running on to collect free entries nobody asked for"
            );
            assert_eq!(
                h.op_manager
                    .interest_manager
                    .peek_summary_cursor(&pk)
                    .as_ref(),
                Some(hosted[63].id()),
                "the cursor advances to the last entry SENT, so the next round \
                 resumes after it"
            );
        }

        /// BOTH receive arms charge their comparison budget for the entries
        /// that COST a comparison, not for every entry.
        ///
        /// This is the half of #5338 the issue did not name, and without it the
        /// send-side fix above delivers nothing. `MAX_SUMMARY_COMPARISONS_PER_MESSAGE`
        /// used to cap the number of ENTRIES a message could contribute, so a
        /// reply carrying 64 charged entries plus free ones would be randomly
        /// truncated back to 64 entries by the receiver — the free entries would
        /// crowd out the very digests the sender spent its CPU budget producing,
        /// and the send window's contiguous tiling would arrive as a random
        /// sample. In the worst case (half the shared set not hosted) the sender
        /// would summarize 64 contracts to deliver 32, which is what it delivers
        /// today: double the CPU for identical coverage.
        ///
        /// An entry carrying no summary is settled without our summary being
        /// consulted — `classify_summary_digest`'s `(_, None)` arm on the digest
        /// leg, the `_ => false` staleness arm on the full-bytes one — so it
        /// genuinely costs nothing here either. The free entries are placed
        /// FIRST so that a budget-charged-per-entry regression consumes the
        /// whole budget on them and reaches none of the costed entries behind.
        ///
        /// # Why both forms, and why that is not symmetry for its own sake
        ///
        /// The two arms carry near-identical logic that was written twice, so
        /// covering one proves nothing about the other. And the full-bytes arm
        /// is not the legacy path its name suggests. It is reached two ways:
        ///
        /// - Directly, by any peer below [`HASH_FIRST_SUMMARIES_MIN_VERSION`].
        /// - **As the second leg of every digest exchange that finds a
        ///   disagreement**, for every peer at every version: a `SummaryRequest`
        ///   is answered with a plain `Summaries`, which lands here. The digest
        ///   arm defers its heal decision to this one by design.
        ///
        /// So the arm whose new `costs_a_summarize` branch had no coverage at
        /// all was the one with the widest live reach in the change.
        ///
        /// The per-form observable differs because the arms answer differently:
        /// a disagreeing digest provokes a `SummaryRequest`, while disagreeing
        /// BYTES are resolved in place and recorded by `upsert_peer_summary_from`.
        /// Both are proof that the costed entries were reached at all, which is
        /// what a per-entry budget would have prevented.
        #[tokio::test]
        async fn free_entries_do_not_consume_the_receive_budget() {
            use std::sync::atomic::Ordering;

            for (label, port) in [("digests", 17240u16), ("full-bytes", 17260)] {
                // Seeded because the pre-fix path rotates by a random offset
                // before truncating; a regression must fail reproducibly.
                let _seed = crate::config::GlobalRng::seed_guard(0x5338_5EC0);
                let h = build_harness(&format!("hf-recv-free-{label}"), port, vec![7u8; 64]).await;
                // Exactly MAX_SUMMARY_ENTRIES_PER_MESSAGE entries: 64 free then
                // 64 costed. Sized to sit ON the ceiling rather than over it,
                // so the receive leg's over-ceiling truncation is not what this
                // test measures — a bigger fixture would drop costed entries at
                // random and the assertions below would be testing the
                // truncation instead of the budget.
                let keys = host_many(&h, 128);
                let hashes = distinct_hashes(&keys);
                let pk = h.peer_key_of(h.new_peer);

                // A belief about the peer that the free entries must clear.
                // Without it, "processed for free" and "silently dropped" look
                // identical from the costed-entry observable alone.
                h.op_manager.interest_manager.upsert_peer_summary_from(
                    &keys[127],
                    &pk,
                    StateSummary::from(vec![4u8; 8]),
                    crate::ring::interest::SummaryPopulationSource::InterestSummary,
                );

                // Disagrees with the harness's `vec![7u8; 64]`, so nothing
                // short-circuits and every costed entry reaches the comparison.
                let theirs = vec![9u8; 8];
                let message = if label == "digests" {
                    let mut entries: Vec<SummaryDigestEntry> = hashes[64..]
                        .iter()
                        .map(|&hash| SummaryDigestEntry {
                            hash,
                            summary_digest: None,
                        })
                        .collect();
                    entries.extend(hashes[..64].iter().map(|&hash| SummaryDigestEntry {
                        hash,
                        summary_digest: Some(summary_digest(&theirs)),
                    }));
                    assert_eq!(
                        entries.len(),
                        128,
                        "{label} premise: 64 free entries, then 64 costed ones"
                    );
                    InterestMessage::SummaryDigests {
                        entries,
                        emitter: crate::message::SummariesEmitter::InterestsReply,
                    }
                } else {
                    let mut entries: Vec<SummaryEntry> = hashes[64..]
                        .iter()
                        .map(|&hash| SummaryEntry {
                            hash,
                            summary_bytes: None,
                        })
                        .collect();
                    entries.extend(hashes[..64].iter().map(|&hash| SummaryEntry {
                        hash,
                        summary_bytes: Some(theirs.clone()),
                    }));
                    assert_eq!(
                        entries.len(),
                        128,
                        "{label} premise: 64 free entries, then 64 costed ones"
                    );
                    InterestMessage::Summaries {
                        entries,
                        emitter: crate::message::SummariesEmitter::InterestsReply,
                    }
                };

                let before = h.summary_queries.load(Ordering::Relaxed);
                let reply = handle_interest_sync_message(&h.op_manager, h.new_peer, message).await;
                let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

                // Per-form proof that the costed entries were reached.
                if label == "digests" {
                    match reply {
                        Some(InterestMessage::SummaryRequest { hashes: requested }) => {
                            assert_eq!(
                                requested.iter().copied().collect::<HashSet<u32>>(),
                                hashes[..64].iter().copied().collect::<HashSet<u32>>(),
                                "every digest that disagreed must be followed up. \
                                 A budget charged per ENTRY is spent on the 64 \
                                 free ones first and never reaches these at all"
                            );
                        }
                        other => panic!(
                            "{label}: a message full of disagreeing digests must \
                             ask for bytes, got {other:?}"
                        ),
                    }
                } else {
                    assert!(
                        reply.is_none(),
                        "{label}: the `Summaries` arm resolves in place and sends \
                         no reply, got {reply:?}"
                    );
                    assert_eq!(
                        h.op_manager
                            .interest_manager
                            .get_peer_interest(&keys[0], &pk)
                            .and_then(|i| i.summary)
                            .map(|s| s.as_ref().to_vec()),
                        Some(theirs.clone()),
                        "{label}: a costed entry must be compared and its bytes \
                         cached against the peer. A budget charged per ENTRY is \
                         spent on the 64 free entries first, so this one is \
                         never processed and no summary is recorded"
                    );
                }

                assert_eq!(
                    fetches, 64,
                    "{label}: exactly one summarize per costed entry, and none at \
                     all for the free ones — the receive budget must bound the \
                     round trips, not the entry count"
                );
                assert_eq!(
                    h.op_manager
                        .interest_manager
                        .get_peer_interest(&keys[127], &pk)
                        .and_then(|i| i.summary),
                    None,
                    "{label}: a free entry still carries its repair — it clears \
                     our cached belief that the peer holds state. Not charging \
                     it must not mean discarding it"
                );
            }
        }

        /// The simulation-only `emit_confirmed` path still summarizes for a free
        /// entry, where production does not. Pinned deliberately, in both
        /// directions, because each half protects something different.
        ///
        /// **Why production must skip.** The verdict for an entry carrying no
        /// summary cannot depend on ours, so the round trip is pure cost — the
        /// whole point of #5338's receive half.
        ///
        /// **Why simulation must NOT skip.** The `StateConfirmed` events these
        /// fetches produce are consumed, and a search for the variant name at
        /// the consumers will tell you they are not. The chain is:
        /// `EventKind::stored_state_hash()` returns the hash for this variant;
        /// `SimNetwork::check_convergence` and `check_convergence_from_logs`
        /// fold it with `contract_key()` into a per-(peer, contract) latest-hash
        /// map; several `simulation_integration` assertions read the same
        /// accessor directly; and `StateVerifier::build_histories` admits the
        /// event through `contract_key()`. The direct runner calls
        /// `SimulationIdleTimeout::enable()` for EVERY simulation, so this is
        /// not a corner.
        ///
        /// Removing it fails in both directions. A peer's last recorded hash
        /// goes stale, which reads as divergence that is not there (flaky red);
        /// or the peer leaves `contract_states` for that contract, dropping it
        /// below the two-peer threshold the check requires, so the contract is
        /// skipped without comment and the suite **passes vacuously**. The
        /// second is the one that matters — a green signal that has quietly
        /// stopped checking.
        ///
        /// #5338 removed this exception once and had to put it back. The
        /// reasoning was "nothing reads `StateConfirmed`", from a grep for that
        /// string across the consumers; the consumption is through a generic
        /// accessor, so the string is not there to find. If you are about to
        /// delete this test because the divergence looks untidy, trace
        /// `stored_state_hash()` first.
        ///
        /// The cost is real and is recorded at
        /// `MAX_SUMMARY_COMPARISONS_PER_MESSAGE`: under the flag the receive
        /// leg's round trips are bounded by `MAX_SUMMARY_ENTRIES_PER_MESSAGE`
        /// instead, so a simulation's summarize count reads high.
        #[tokio::test]
        async fn emit_confirmed_keeps_the_free_entry_summarize_production_skips() {
            use std::sync::atomic::Ordering;

            /// Restores the thread-local flag even if an assertion panics, so a
            /// failure here cannot silently change what the next test on this
            /// thread observes.
            struct FlagGuard;
            impl Drop for FlagGuard {
                fn drop(&mut self) {
                    crate::config::SimulationIdleTimeout::disable();
                }
            }

            for (label, port) in [("digests", 17320u16), ("full-bytes", 17340)] {
                let h = build_harness(&format!("hf-emit-conf-{label}"), port, vec![7u8; 64]).await;
                let keys = host_many(&h, 64);
                let hashes = distinct_hashes(&keys);

                // Every entry free, so the fetch count is exactly the number of
                // free-entry summarize calls and nothing else.
                let message = || {
                    if label == "digests" {
                        InterestMessage::SummaryDigests {
                            entries: hashes
                                .iter()
                                .map(|&hash| SummaryDigestEntry {
                                    hash,
                                    summary_digest: None,
                                })
                                .collect(),
                            emitter: crate::message::SummariesEmitter::InterestsReply,
                        }
                    } else {
                        InterestMessage::Summaries {
                            entries: hashes
                                .iter()
                                .map(|&hash| SummaryEntry {
                                    hash,
                                    summary_bytes: None,
                                })
                                .collect(),
                            emitter: crate::message::SummariesEmitter::InterestsReply,
                        }
                    }
                };

                assert!(
                    !crate::config::SimulationIdleTimeout::is_enabled(),
                    "{label} premise: the production path is the default, or the \
                     two halves of this test measure the same thing"
                );
                let before = h.summary_queries.load(Ordering::Relaxed);
                let _ = handle_interest_sync_message(&h.op_manager, h.new_peer, message()).await;
                let production = h.summary_queries.load(Ordering::Relaxed) - before;

                let guard = FlagGuard;
                crate::config::SimulationIdleTimeout::enable();
                let before = h.summary_queries.load(Ordering::Relaxed);
                let _ = handle_interest_sync_message(&h.op_manager, h.new_peer, message()).await;
                let simulated = h.summary_queries.load(Ordering::Relaxed) - before;
                drop(guard);

                assert_eq!(
                    production, 0,
                    "{label}: production must make no summarize round trip for an \
                     entry that carries no summary — the verdict cannot depend on \
                     ours, so the call is pure cost"
                );
                assert_eq!(
                    simulated, 64,
                    "{label}: and simulation must still make it. These fetches are \
                     what produce the StateConfirmed events the convergence \
                     checker folds into its per-peer state map; without them a \
                     contract can fall below the two-peer threshold and be \
                     skipped silently, so the suite passes while checking less"
                );
            }
        }

        /// A source we hold no connection for gets a reply, and gets no cursor.
        ///
        /// `peer_key` is an `Option`, and #5338 made the rotation depend on it.
        /// The degraded branch is the one the change's safety argument leans on,
        /// so it is worth exercising rather than asserting: with no stable
        /// identity there is nothing to rotate against, so the reply draws a
        /// fresh random offset and records nothing. That is the same degraded
        /// mode an evicted cursor already gets, and it is acceptable for the
        /// same reason — a source with no connection entry is also a source
        /// whose interest this handler declines to register.
        ///
        /// The load-bearing assertion is the third one: a fabricated
        /// address-derived key would make round two resume contiguously after
        /// round one. It is an inequality because "holds no memory of this
        /// source" is the property, and pinning which random offset it draws
        /// would pin `GlobalRng`'s internals rather than the behaviour.
        ///
        /// Note which WIRE FORM this path produces, because it is not the one
        /// you would guess: `summary_reply_form` fails closed on an unknown
        /// peer version, and a source with no connection entry has no recorded
        /// version, so an unresolvable source gets FULL BYTES. Asserted rather
        /// than tolerated — it means this degraded path also spends the
        /// full-bytes byte budget, which is worth knowing if the fail-closed
        /// default ever moves.
        #[tokio::test]
        async fn an_unresolvable_source_gets_a_reply_but_no_cursor() {
            let _seed = crate::config::GlobalRng::seed_guard(0x5338_0FFC);
            let h = build_harness("hf-no-peer-key", 17280, vec![7u8; 64]).await;
            let keys = host_many(&h, 200);
            let hashes = distinct_hashes(&keys);
            let mut sorted = keys.clone();
            sorted.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));

            // Never added to the ring, so `get_peer_key_from_addr` returns None.
            let stranger: SocketAddr = "127.0.0.1:17289".parse().unwrap();
            assert!(
                h.op_manager
                    .ring
                    .connection_manager
                    .get_peer_by_addr(stranger)
                    .is_none(),
                "premise: this address must have no connection entry, or the \
                 test exercises the resolvable path instead"
            );

            // A known peer's cursor, which the stranger must not touch.
            let known = h.peer_key_of(h.new_peer);
            h.op_manager
                .interest_manager
                .record_summary_cursor(&known, *sorted[5].id());

            // Full bytes, not digests: the version gate fails closed on a
            // source we have no connection entry for.
            let full_bytes_hashes = |reply: Option<InterestMessage>| -> Vec<u32> {
                match reply {
                    Some(InterestMessage::Summaries { entries, .. }) => {
                        entries.iter().map(|e| e.hash).collect()
                    }
                    other => panic!(
                        "an unresolvable source has no recorded version, so the \
                         fail-closed gate must send full bytes, got {other:?}"
                    ),
                }
            };

            let first = full_bytes_hashes(
                handle_interest_sync_message(
                    &h.op_manager,
                    stranger,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await,
            );
            assert_eq!(
                first.len(),
                64,
                "an unresolvable source is still served a full bounded reply — \
                 degrading the rotation must not degrade the answer"
            );

            let resume = sorted
                .iter()
                .position(|k| contract_hash(k) == *first.last().expect("64 entries"))
                .expect("the last hash sent must be one of ours")
                + 1;
            let second = full_bytes_hashes(
                handle_interest_sync_message(
                    &h.op_manager,
                    stranger,
                    InterestMessage::Interests { hashes },
                )
                .await,
            );
            let contiguous: Vec<u32> = (0..64)
                .map(|i| contract_hash(&sorted[(resume + i) % sorted.len()]))
                .collect();
            assert_ne!(
                second, contiguous,
                "with no peer key there is no cursor to resume from, so round \
                 two must NOT continue where round one stopped. Fabricating a \
                 key from the address would make it resume — and would key the \
                 cursor by address again, which is the defect #5338 fixes"
            );

            assert_eq!(
                h.op_manager
                    .interest_manager
                    .peek_summary_cursor(&known)
                    .as_ref(),
                Some(sorted[5].id()),
                "and an unresolvable source must not advance — or read — the \
                 cursor of a peer we DO know"
            );
        }

        /// A window with nothing to summarize walks to the entry ceiling and
        /// stops there, spending no budget at all.
        ///
        /// Two properties in one fixture, both otherwise unpinned:
        ///
        /// - **The `MAX_SUMMARY_ENTRIES_PER_MESSAGE` ceiling.** Every other test
        ///   here stops on the summarize budget well before it, so a regression
        ///   shrinking the ceiling back toward 64 would pass all of them. The
        ///   sizing is the crux of the argument that the send-side fix is not
        ///   a no-op — at 1x the ceiling binds before the budget and the whole
        ///   change does nothing — and an unpinned constant is an argument with
        ///   nothing holding it up.
        /// - **The all-free window.** With no hosted contract anywhere in the
        ///   set the loop never reaches `summarized >= summarize_cap`, so the
        ///   ceiling is the ONLY thing that terminates it. Without one it would
        ///   walk the entire shared set — the unbounded reply #5238 removed.
        #[tokio::test]
        async fn an_all_free_window_stops_at_the_entry_ceiling() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-all-free", 17300, vec![7u8; 64]).await;
            let tracker = h.peer_key_of(h.old_peer);

            // 400 contracts we track and none we host, so every entry is free.
            // Big-endian index so sorted-by-id order is `j` order.
            let mut tracked = Vec::with_capacity(400);
            for j in 0..400u32 {
                let mut id = [0u8; 32];
                id[0..4].copy_from_slice(&j.to_be_bytes());
                id[4] = 0x77;
                let key = ContractKey::from_id_and_code(
                    ContractInstanceId::new(id),
                    CodeHash::new([0xC2; 32]),
                );
                h.op_manager.interest_manager.register_peer_interest_from(
                    &key,
                    tracker.clone(),
                    None,
                    false,
                    crate::ring::interest::InterestRegistrationSource::Interests,
                );
                tracked.push(key);
            }
            let hashes = distinct_hashes(&tracked);

            // An id below every contract in the fixture, so the window starts
            // at index 0 with no random draw.
            let pk = h.peer_key_of(h.new_peer);
            h.op_manager
                .interest_manager
                .record_summary_cursor(&pk, ContractInstanceId::new([0u8; 32]));

            let before = h.summary_queries.load(Ordering::Relaxed);
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Interests { hashes },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            let entries = match reply {
                Some(InterestMessage::SummaryDigests { entries, .. }) => entries,
                other => panic!("expected a digest reply, got {other:?}"),
            };
            assert_eq!(
                fetches, 0,
                "not one of these contracts is hosted, so the gate declines every \
                 one of them without a contract-handler round trip"
            );
            // ABSOLUTE literal, not the constant that produced it: an
            // assertion written against its own constant moves with any
            // regression to it and can never fail.
            assert_eq!(
                entries.len(),
                128,
                "the walk must stop at MAX_SUMMARY_ENTRIES_PER_MESSAGE (2 x 64). \
                 Nothing here charges the summarize budget, so the ceiling is \
                 the only thing that ends the loop — change it and this is the \
                 test that notices, since every other fixture stops on the \
                 budget first"
            );
            assert_eq!(
                entries
                    .iter()
                    .filter(|e| e.summary_digest.is_some())
                    .count(),
                0,
                "premise: an all-free window advertises no summary at all"
            );
            assert_eq!(
                h.op_manager
                    .interest_manager
                    .peek_summary_cursor(&pk)
                    .as_ref(),
                Some(tracked[127].id()),
                "the cursor advances across the whole walked span, so the next \
                 round resumes at 256 rather than re-walking the same prefix"
            );
        }

        /// The RECEIVING leg is bounded independently of the sending one.
        ///
        /// One heartbeat round-trip pays two summarize-heavy loops, not one:
        /// the sender computes a summary per entry to build the digests, and
        /// the receiver computes its OWN summary per entry to compare against
        /// them. Bounding only the send side would halve the storm and leave
        /// the other half reachable from any peer that predates this release —
        /// which, during a rollout, is most of them.
        ///
        /// The old bound on this arm was `MAX_SUMMARY_HASHES_PER_MESSAGE`
        /// (4096), which bounds the message but is far above the point where
        /// the WORK matters.
        #[tokio::test]
        async fn digest_receive_leg_bounds_its_summarize_calls() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-digest-recv-bound", 17160, vec![7u8; 64]).await;
            let keys = host_many(&h, 933);
            let hashes = distinct_hashes(&keys);

            // A pre-#5238 sender: one entry per shared contract, no window.
            // Digests deliberately DISAGREE with ours, so nothing short-
            // circuits the comparison — this measures the loop, not the
            // agreement path.
            let entries: Vec<SummaryDigestEntry> = hashes
                .iter()
                .map(|&hash| SummaryDigestEntry {
                    hash,
                    summary_digest: Some(summary_digest(b"not our summary")),
                })
                .collect();

            let before = h.summary_queries.load(Ordering::Relaxed);
            let _ = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    entries,
                    emitter: crate::message::SummariesEmitter::InterestsReply,
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            assert!(
                fetches > 0,
                "premise: the fixture must actually reach the summarize path, \
                 or this test cannot fail"
            );
            assert!(
                fetches <= 64,
                "comparing an inbound digest message made {fetches} summarize \
                 round trips against {} entries. The receiving leg is the other \
                 half of the #5238 storm and needs its own bound — the sender's \
                 window does not constrain what an un-upgraded peer sends us",
                hashes.len()
            );
        }

        /// The full-bytes `Summaries` receive leg is bounded too.
        ///
        /// It is the twin of `digest_receive_leg_bounds_its_summarize_calls`:
        /// same per-entry summarize cost, different wire form, and this one has
        /// neither the digest arm's per-message memo nor its pair dedup, so an
        /// unbounded version is the cheaper of the two to drive.
        ///
        /// It cannot bind against a peer running #5155 or later, which is why
        /// the fixture builds the message by hand rather than provoking one.
        #[tokio::test]
        async fn summaries_receive_leg_bounds_its_summarize_calls() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-summaries-recv-bound", 17180, vec![7u8; 64]).await;
            let keys = host_many(&h, 933);
            let hashes = distinct_hashes(&keys);

            // Summaries that DISAGREE with ours, so nothing short-circuits.
            let entries: Vec<SummaryEntry> = hashes
                .iter()
                .map(|&hash| {
                    SummaryEntry::from_summary(hash, Some(&StateSummary::from(vec![9u8; 8])))
                })
                .collect();

            let before = h.summary_queries.load(Ordering::Relaxed);
            let _ = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Summaries {
                    entries,
                    emitter: crate::message::SummariesEmitter::InterestsReply,
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            assert!(
                fetches > 0,
                "premise: the fixture must actually reach the summarize path, \
                 or the bound below can never fail"
            );
            assert!(
                fetches <= 64,
                "comparing an inbound Summaries message made {fetches} summarize \
                 round trips against {} entries; this leg has no per-message \
                 memo, so an unbounded version is the cheapest path in the \
                 #5238 family",
                hashes.len()
            );
        }

        /// `ChangeInterests` deduplicates its peer-supplied hash list.
        ///
        /// This arm is deliberately not windowed — it is churn-driven, so a
        /// window could defer a newly-added interest indefinitely rather than
        /// by a bounded number of heartbeats. That leaves it as the cheapest
        /// amplification path in the family once the other four are bounded:
        /// `added` is peer-supplied and uncapped, and every entry costs a
        /// `summary_if_hosted_or_in_use` round trip.
        ///
        /// Dedup is the part that is free. A window can drop a real new
        /// interest; a repeated hash carries nothing the first copy did not,
        /// so collapsing it cannot lose anything. The assertion is therefore
        /// on REPETITION only — a peer naming many genuinely distinct new
        /// interests still pays per interest, by design.
        #[tokio::test]
        async fn change_interests_deduplicates_repeated_hashes() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-change-dedup", 17185, vec![7u8; 64]).await;
            let keys = host_many(&h, 1);
            let hashes = distinct_hashes(&keys);
            // NOT "the fixture hosts exactly one contract" — `build_harness`
            // already hosts and locally-registers its own `h.key` before
            // `host_many` adds this one, so the node hosts two. What matters is
            // narrower, and is what this asserts: `host_many(1)` yielded a
            // single hash, and that hash is the only thing we put in `added`.
            // `h.key`'s hash is never sent, so the arm never reaches it.
            //
            // Near-vacuous by construction (`host_many(1)` returns one key, so
            // `distinct_hashes` returns one hash barring a self-collision), and
            // kept as a guard on the FIXTURE rather than on the behaviour: if
            // someone grows `host_many`'s argument, the expected fetch count
            // below stops being 1, and this fires first with a clearer reason
            // than the equality would give.
            assert_eq!(hashes.len(), 1, "premise: exactly one hash is advertised");

            // 500 copies of ONE hash. Pre-dedup this is 500 sequential
            // summarize round trips for a single contract.
            let repeats = 500usize;
            let added: Vec<u32> = std::iter::repeat_n(hashes[0], repeats).collect();

            let before = h.summary_queries.load(Ordering::Relaxed);
            let _ = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::ChangeInterests {
                    added,
                    removed: Vec::new(),
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            assert!(
                fetches > 0,
                "premise: the fixture must actually reach the summarize path, \
                 or the bound below can never fail"
            );
            // Pin the EXACT value, not `< repeats`. The fixture hosts one
            // contract and `lookup_by_hash` resolves the single hash to it, so
            // correct dedup collapses the loop to exactly one round trip.
            //
            // `< repeats` was the first form and it is far too weak to be worth
            // having: an INVERTED condition (`if seen_added.insert(hash)`,
            // skipping only the FIRST occurrence) processes the other 499 and
            // still passes, as does any batched dedup collapsing to repeats/K.
            // That mutation removes ~1 round trip out of 500 — a dedup that is
            // essentially entirely broken — and only an equality assertion
            // separates it from the fix.
            assert_eq!(
                fetches, 1,
                "{repeats} copies of one hash made {fetches} summarize round \
                 trips, expected exactly 1. Bounding the other four loops makes \
                 this arm the cheapest amplification path in the #5238 family, \
                 and dedup is the one bound here that cannot drop a real new \
                 interest"
            );
        }

        /// The digest path RECORDS a rotation cursor, not just reads one.
        ///
        /// `digest_rotation_covers_the_whole_shared_set` covers this only
        /// indirectly, through the coverage it produces. Asserting the cursor
        /// directly is what the full-bytes sibling already does, and it is the
        /// single line that distinguishes "the window advances" from "the
        /// window happens to have been drawn differently each round".
        #[tokio::test]
        async fn digest_reply_records_the_rotation_cursor() {
            let h = build_harness("hf-digest-cursor", 17190, vec![7u8; 64]).await;
            let keys = host_many(&h, 200);
            let hashes = distinct_hashes(&keys);

            assert!(
                h.op_manager
                    .interest_manager
                    .peek_summary_cursor(&h.peer_key_of(h.new_peer))
                    .is_none(),
                "premise: no cursor before the first reply"
            );

            let _ = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Interests { hashes },
            )
            .await;

            assert!(
                h.op_manager
                    .interest_manager
                    .peek_summary_cursor(&h.peer_key_of(h.new_peer))
                    .is_some(),
                "a digest reply must record where it stopped, or the next round \
                 re-draws at random and the ceil(n / cap) coverage bound is lost \
                 — this was `if form == FullBytes` before #5238"
            );
        }

        /// The `SummaryRequest` reply is bounded too, because bounding
        /// `Interests` made this the cheapest remaining amplification path.
        ///
        /// The recorded reason for leaving this arm unbounded was that a peer
        /// could already force the same work by spamming `Interests`, which ran
        /// the same loop. That was true; it is not any more. With both forms of
        /// `Interests` windowed, an unbounded reply here would be strictly
        /// easier to abuse than the loop this change closes.
        ///
        /// It costs nothing against an upgraded peer: a requester running this
        /// release compares at most 64 entries per message, so it cannot ask
        /// for more hashes than the cap allows.
        #[tokio::test]
        async fn summary_request_reply_bounds_its_summarize_calls() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-request-bound", 17170, vec![7u8; 64]).await;
            let keys = host_many(&h, 933);
            let hashes = distinct_hashes(&keys);

            let before = h.summary_queries.load(Ordering::Relaxed);
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryRequest {
                    hashes: hashes.clone(),
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            match reply {
                Some(InterestMessage::Summaries { entries, .. }) => {
                    assert!(
                        !entries.is_empty(),
                        "a bounded reply must still answer the request"
                    );
                    assert!(entries.len() <= 64, "got {} entries", entries.len());
                }
                other => panic!("a SummaryRequest must be answered with full bytes, got {other:?}"),
            }
            assert!(
                fetches > 0,
                "premise: the fixture must actually reach the summarize path, \
                 or the bound below can never fail"
            );
            assert!(
                fetches <= 64,
                "answering a {}-hash SummaryRequest made {fetches} summarize \
                 round trips; the intended cap is 64",
                hashes.len()
            );
        }

        /// Successive fallback replies rotate, so the whole shared set is
        /// covered within `ceil(n / cap)` rounds rather than the same prefix
        /// being re-sent forever.
        ///
        /// A hard cap with no rotation would be simpler and strictly worse:
        /// everything past the first window would never be advertised to that
        /// peer at all, turning a bandwidth fix into permanent silent
        /// divergence.
        ///
        /// Cursor-seeded for the reason spelled out on
        /// `digest_rotation_covers_the_whole_shared_set`: this test carried the
        /// same ~1% cycle-boundary flake latently since #5155, and it is a
        /// latent flake rather than an observed one only because nothing had
        /// run it enough times to land on one of the three bad starts.
        #[tokio::test]
        async fn fallback_rotation_covers_the_whole_shared_set() {
            // Guarded — see the sibling rotation test for why.
            let _seed = crate::config::GlobalRng::seed_guard(0x5155_0704);
            let h = build_harness("hf-bound-rotate", 17140, vec![7u8; 64]).await;
            let keys = host_many(&h, 200);
            let hashes = distinct_hashes(&keys);
            let expected: HashSet<u32> = hashes.iter().copied().collect();
            let rounds = hashes.len().div_ceil(MAX_FALLBACK_SUMMARIES_PER_REPLY);

            let mut sorted = keys.clone();
            sorted.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
            h.op_manager
                .interest_manager
                .record_summary_cursor(&h.peer_key_of(h.old_peer), *sorted[0].id());

            let mut covered: HashSet<u32> = HashSet::new();
            for round in 0..rounds {
                let reply = handle_interest_sync_message(
                    &h.op_manager,
                    h.old_peer,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await;
                match reply {
                    Some(InterestMessage::Summaries { entries, .. }) => {
                        // Holds BY FIXTURE, not by the cap — same as the digest
                        // twin. Since #5338 a reply is bounded at 64 SUMMARIZE
                        // CALLS and at MAX_SUMMARY_ENTRIES_PER_MESSAGE entries,
                        // so a reply may legitimately carry more than 64
                        // entries. Every contract here is hosted, so every entry
                        // is costed and the summarize budget binds. Do not read
                        // this as "replies are capped at 64 entries".
                        assert!(entries.len() <= MAX_FALLBACK_SUMMARIES_PER_REPLY);
                        covered.extend(entries.iter().map(|e| e.hash));
                    }
                    other => panic!("round {round}: expected full bytes, got {other:?}"),
                }
            }

            assert_eq!(
                covered.len(),
                expected.len(),
                "after {rounds} rounds (ceil({}/{MAX_FALLBACK_SUMMARIES_PER_REPLY})) \
                 the rotation still had not advertised {} of the shared contracts",
                hashes.len(),
                expected.difference(&covered).count()
            );
        }

        /// When summaries are large the byte budget, not the entry cap, sets
        /// the cycle length — and the cycle is then FAR longer than
        /// `ceil(n / 64)` rounds.
        ///
        /// This test exists to stop the comfortable number being quoted. The
        /// PR-level claim is naturally read as "a divergence is noticed within
        /// ceil(266/64) ~= 5 heartbeats, about 25 minutes"; that holds only in
        /// the regime where entries are cheap. Here every summary is 5 KB, so
        /// a round carries two entries and covering 40 contracts takes ~20
        /// rounds, not 1. At a 5-minute heartbeat that is a bit over an hour,
        /// and for a set of River-sized summaries it is longer still.
        ///
        /// If someone later makes the budget adaptive, or reworks the bound so
        /// the entry cap governs again, this test SHOULD fail — and its failure
        /// means the cost story in the PR needs rewriting, not that the test
        /// needs relaxing.
        #[tokio::test]
        async fn heavy_summaries_make_the_cycle_far_longer_than_the_entry_cap_suggests() {
            let h = build_harness("hf-bound-cycle-cost", 17160, vec![4u8; 5000]).await;
            let keys = host_many(&h, 40);
            let hashes = distinct_hashes(&keys);
            let expected: HashSet<u32> = hashes.iter().copied().collect();

            // The optimistic reading: entry cap only.
            let optimistic_rounds = hashes.len().div_ceil(64);
            assert_eq!(
                optimistic_rounds, 1,
                "premise: 40 contracts is one round under the entry cap alone"
            );

            let mut covered: HashSet<u32> = HashSet::new();
            let mut rounds_taken = 0usize;
            for _ in 0..200 {
                rounds_taken += 1;
                let reply = handle_interest_sync_message(
                    &h.op_manager,
                    h.old_peer,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await;
                match reply {
                    Some(InterestMessage::Summaries { entries, .. }) => {
                        covered.extend(entries.iter().map(|e| e.hash));
                    }
                    other => panic!("expected full bytes, got {other:?}"),
                }
                if covered == expected {
                    break;
                }
            }

            assert_eq!(
                covered, expected,
                "the rotation must still reach every contract — slower is the \
                 accepted cost, never-covered is not"
            );
            assert!(
                rounds_taken > optimistic_rounds * 5,
                "premise of this test: with 5 KB summaries the byte budget must \
                 dominate, making the real cycle much longer than the \
                 {optimistic_rounds}-round entry-cap reading. It took \
                 {rounds_taken}. If this now passes quickly, the cost model in \
                 the PR and in MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY is stale."
            );
        }

        /// Two `Interests` from the same peer handled concurrently cost at
        /// most one round of rotation progress, and cannot wedge or corrupt it.
        ///
        /// The handler reads the cursor, awaits up to 64 summary fetches, then
        /// writes the cursor back, so two overlapping invocations for one peer
        /// can both read the same start and build the same window. Inbound
        /// messages are dispatched one task per message, so this is reachable
        /// in production and a peer can provoke it deliberately. The comment at
        /// the cursor-advance site accepts that cost as "one round"; this test
        /// is that claim's evidence rather than its restatement.
        ///
        /// The load-bearing assertion is the progress floor: after `PAIRS`
        /// concurrent pairs run back to back with no sequential round between
        /// them, the union of what was advertised must be at least
        /// `PAIRS * 64` contracts — i.e. every pair advanced the rotation by at
        /// least one window even in the worst interleaving. Losing MORE than
        /// one round per pair, or losing the cursor entirely and re-sending the
        /// same window forever, both fail here. The bound holds for any
        /// interleaving, so the test does not depend on the scheduler
        /// reproducing a particular one: a genuinely concurrent run and a
        /// serialised run both satisfy it, and only a broken rotation does not.
        ///
        /// Multi-threaded flavour with `spawn` on purpose: an earlier version
        /// of this test used `join!` on a current-thread runtime and the two
        /// futures did NOT overlap — it passed while exercising nothing.
        ///
        /// # How that was caught, since the same trap is easy to re-enter
        ///
        /// A concurrency test that passes first time deserves suspicion,
        /// because the cheapest way to pass one is not to be concurrent. The
        /// check that separated "passes" from "is evidence" was to probe for
        /// the race's own signature rather than for the assertions: both
        /// handlers reading the same cursor must produce the SAME window, so
        /// **identical windows prove the overlap happened and different ones
        /// prove it did not**. The `join!` version produced different windows,
        /// which is what exposed it as vacuous.
        ///
        /// If you rewrite this test, re-run that probe rather than trusting a
        /// green result — and do not "simplify" it back to `join!` on the
        /// default runtime, which is where it started.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_interests_from_one_peer_cost_at_most_one_round() {
            const PAIRS: usize = 3;
            let h = build_harness("hf-bound-concurrent", 17170, vec![7u8; 64]).await;
            let keys = host_many(&h, 400);
            let hashes = distinct_hashes(&keys);
            let expected: HashSet<u32> = hashes.iter().copied().collect();
            let ids: HashSet<ContractInstanceId> = keys.iter().map(|k| *k.id()).collect();

            let mut covered: HashSet<u32> = HashSet::new();
            for pair in 0..PAIRS {
                let spawn_one = || {
                    let op_manager = Arc::clone(&h.op_manager);
                    let hashes = hashes.clone();
                    let peer = h.old_peer;
                    tokio::spawn(async move {
                        handle_interest_sync_message(
                            &op_manager,
                            peer,
                            InterestMessage::Interests { hashes },
                        )
                        .await
                    })
                };
                let (first, second) = tokio::join!(spawn_one(), spawn_one());

                for (which, joined) in [("first", first), ("second", second)] {
                    match joined.expect("handler task panicked") {
                        Some(InterestMessage::Summaries { entries, .. }) => {
                            assert!(
                                entries.len() <= 64,
                                "pair {pair} {which} reply carried {} entries; the \
                                 per-reply bound must hold regardless of overlap",
                                entries.len()
                            );
                            assert!(summary_bytes_of(&entries) <= 9 * 1024 + h.our_summary.len());
                            covered.extend(entries.iter().map(|e| e.hash));
                        }
                        other => panic!("pair {pair} {which} reply was {other:?}"),
                    }
                }

                // The cursor must still name a contract that exists, not a
                // value torn between the two writers.
                let cursor = h
                    .op_manager
                    .interest_manager
                    .peek_summary_cursor(&h.peer_key_of(h.old_peer))
                    .expect("a fallback reply must leave a cursor");
                assert!(
                    ids.contains(&cursor),
                    "after pair {pair} the cursor is no longer a member of the \
                     shared set — concurrent writers corrupted the rotation \
                     rather than merely duplicating a window"
                );
            }

            assert!(
                covered.len() >= PAIRS * 64,
                "{PAIRS} concurrent pairs advertised only {} distinct contracts; \
                 each pair must advance the rotation by at least one 64-entry \
                 window, so the overlap cost more than the one round the \
                 cursor-advance comment claims",
                covered.len()
            );

            // And the rotation still completes from there.
            for _ in 0..hashes.len().div_ceil(64) {
                match handle_interest_sync_message(
                    &h.op_manager,
                    h.old_peer,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await
                {
                    Some(InterestMessage::Summaries { entries, .. }) => {
                        covered.extend(entries.iter().map(|e| e.hash));
                    }
                    other => panic!("expected full bytes, got {other:?}"),
                }
            }
            assert_eq!(
                covered.len(),
                expected.len(),
                "{} contracts still unadvertised after the concurrent pairs plus \
                 a full sequential cycle",
                expected.difference(&covered).count()
            );
        }

        /// The rotation bounds what we ADVERTISE, never who we consider a
        /// broadcast target.
        ///
        /// Peer interest is registered for every shared contract, including the
        /// ones outside this round's window. Rotating that too would drop the
        /// peer out of the broadcast set for whatever fell outside, converting
        /// a bandwidth bound into missed updates — a correctness bug wearing a
        /// performance fix's clothes.
        ///
        /// This is the most load-bearing test in the change, because
        /// `INTEREST_TTL` is 20 minutes (4 heartbeats) while the new coverage
        /// cycle is ~40-75 minutes. Had the registration loop been moved inside
        /// the window, `sweep_expired_interests` would have started removing
        /// (contract, peer) pairs in steady state for any peer sharing more
        /// than 4 x 64 contracts — silent, and fatal to live fan-out.
        ///
        /// Be precise about what running BOTH forms buys, because it is easy to
        /// overstate: there is exactly ONE registration loop today, shared by
        /// both forms, so any move of it inside the window breaks BOTH cases
        /// and either alone would catch it. The second case is insurance
        /// against a FUTURE per-form split of that loop, not additional
        /// coverage of the code as it stands.
        #[tokio::test]
        async fn bounding_the_reply_does_not_bound_interest_registration() {
            // #5238: run against BOTH wire forms. Under #5155 only the
            // full-bytes reply was windowed, so `old_peer` was the only case
            // where a contract could be registered-but-unadvertised. Now that
            // both are windowed the invariant matters for every peer, and a
            // test that only covers one form would not notice if the
            // registration loop were moved inside the window for the other.
            for (label, peer) in [("full-bytes", 17150u16), ("digests", 17151)] {
                let h =
                    build_harness(&format!("hf-bound-interest-{label}"), peer, vec![7u8; 64]).await;
                let target = if label == "digests" {
                    h.new_peer
                } else {
                    h.old_peer
                };
                let keys = host_many(&h, 200);
                let hashes = distinct_hashes(&keys);

                let reply = handle_interest_sync_message(
                    &h.op_manager,
                    target,
                    InterestMessage::Interests {
                        hashes: hashes.clone(),
                    },
                )
                .await;
                let advertised: HashSet<u32> = match reply {
                    Some(InterestMessage::Summaries { ref entries, .. }) => {
                        entries.iter().map(|e| e.hash).collect()
                    }
                    Some(InterestMessage::SummaryDigests { ref entries, .. }) => {
                        entries.iter().map(|e| e.hash).collect()
                    }
                    other => panic!("{label}: unexpected reply {other:?}"),
                };
                assert!(
                    advertised.len() < hashes.len(),
                    "{label} premise: this round must NOT have advertised \
                     everything, or the test cannot distinguish \
                     registered-for-all from registered-for-the-window"
                );

                let pk = h.peer_key_of(target);
                let unadvertised: Vec<&ContractKey> = keys
                    .iter()
                    .filter(|k| !advertised.contains(&contract_hash(k)))
                    .collect();
                assert!(
                    !unadvertised.is_empty(),
                    "{label} premise: some contract was cut"
                );
                for key in unadvertised {
                    assert!(
                        h.op_manager
                            .interest_manager
                            .get_peer_interest(key, &pk)
                            .is_some(),
                        "{label}: peer interest must be registered for {key} \
                         even though this round's window did not advertise it"
                    );
                }
            }
        }

        /// Control for the test above: the same `Summaries` message, but
        /// carrying OUR bytes, must NOT heal.
        ///
        /// Without this pair, `mismatching_digest_requests_bytes_and_the_heal_
        /// still_fires` would also pass against a handler that healed
        /// unconditionally — which would be a broadcast storm, not a fix.
        #[tokio::test]
        async fn identical_summary_bytes_do_not_heal() {
            let mut h = build_harness("hf-agree-control", 17030, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::Summaries {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![SummaryEntry {
                        hash,
                        summary_bytes: Some(h.our_summary.clone()),
                    }],
                },
            )
            .await;
            assert!(reply.is_none());
            assert!(
                h.drain_heals().is_empty(),
                "byte-identical summaries must never heal (that is the \
                 property the digest-agreement path inherits)"
            );
        }

        /// A peer that reports no state for a contract (`summary_digest: None`)
        /// must be handled exactly like a `SummaryEntry { summary_bytes: None }`
        /// — cached summary cleared, no bytes requested, no heal.
        ///
        /// Asking for bytes here would be pointless traffic (there are none),
        /// and healing would push state at a peer that should get it through
        /// the normal subscribe/GET flow.
        #[tokio::test]
        async fn peer_reporting_no_state_is_not_asked_for_bytes() {
            let mut h = build_harness("hf-none", 17040, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);
            let pk = h.peer_key_of(h.new_peer);

            // Seed a stale cached summary so the clear is observable.
            h.op_manager.interest_manager.upsert_peer_summary(
                &h.key,
                &pk,
                StateSummary::from(vec![9u8; 8]),
            );
            assert!(
                h.op_manager
                    .interest_manager
                    .get_peer_summary(&h.key, &pk)
                    .is_some(),
                "precondition: a cached summary exists"
            );

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![SummaryDigestEntry {
                        hash,
                        summary_digest: None,
                    }],
                },
            )
            .await;

            assert!(
                reply.is_none(),
                "a peer with no state has no bytes to send; requesting them \
                 would be pure round-trip cost. Got {reply:?}"
            );
            assert!(
                h.op_manager
                    .interest_manager
                    .get_peer_summary(&h.key, &pk)
                    .is_none(),
                "a None report must clear our cached summary for the peer, \
                 same as SummaryEntry {{ summary_bytes: None }} does"
            );
            assert!(h.drain_heals().is_empty());
        }

        /// An unknown contract hash must not produce a request.
        ///
        /// Otherwise a peer could make us emit `SummaryRequest` messages for
        /// contracts we have never heard of, and each answer would be an empty
        /// `Summaries` — traffic amplification from nothing.
        #[tokio::test]
        async fn unknown_contract_hash_produces_no_request() {
            let mut h = build_harness("hf-unknown", 17050, vec![5u8; 128]).await;

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![SummaryDigestEntry {
                        hash: contract_hash(&h.key).wrapping_add(1),
                        summary_digest: Some(summary_digest(b"whatever")),
                    }],
                },
            )
            .await;

            assert!(
                reply.is_none(),
                "a hash that resolves to no locally-tracked contract must be \
                 ignored, not turned into a request. Got {reply:?}"
            );
            assert!(h.drain_heals().is_empty());
        }

        /// Two DISTINCT contracts whose 32-bit `contract_hash` collides must
        /// both be considered — dropping the second is permanent silent
        /// divergence.
        ///
        /// # The bug this pins (codex P2)
        ///
        /// The digest arm deduplicated on `entry.hash` alone, first-wins. The
        /// sender emits one entry per CONTRACT, so two contracts colliding on
        /// FNV-1a produce two entries with the SAME hash and different digests
        /// — and the second was silently dropped.
        ///
        /// That is not merely a missed optimisation. With both local summaries
        /// byte-identical (two freshly-seeded contracts, say), the FIRST
        /// entry's digest agrees against BOTH local contracts, so both are
        /// recorded as converged; the second entry, carrying the digest of the
        /// contract that actually diverged, never runs. No `SummaryRequest`
        /// fires, and every subsequent heartbeat repeats the same outcome:
        /// **permanent divergence, invisible**. That is the stale-copy class
        /// `hosting-invariants.md` invariant 1 forbids.
        ///
        /// It was also a REGRESSION rather than a pre-existing gap: the
        /// full-bytes `Summaries` arm has no such dedup and processes every
        /// entry, so this input class worked before hash-first.
        ///
        /// The fix deduplicates on the `(hash, digest)` PAIR, so a same-hash
        /// different-digest entry still runs. It then mismatches at least one
        /// local summary, which asks for the bytes; the full-bytes reply
        /// resolves ALL contracts for the hash and disambiguates.
        #[tokio::test]
        async fn colliding_contract_hashes_do_not_drop_the_second_entry() {
            use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
            use std::collections::HashMap;

            // Find ANY pair of instance ids colliding under FNV-1a.
            //
            // Two subtleties, both learned by getting them wrong:
            //
            // 1. Search for an ARBITRARY colliding pair, not a collision with a
            //    fixed key — the latter needs ~2^32 trials, the birthday bound
            //    for a pair is ~2^16.
            // 2. Vary MORE than 32 bits of the input. FNV-1a's per-byte step
            //    (xor then multiply by an odd prime mod 2^32) is invertible, so
            //    over fixed-length inputs differing only in a 4-byte window it
            //    is a BIJECTION — distinct 32-bit prefixes provably never
            //    collide, and a search over them runs forever finding nothing.
            //    Spreading a multiplied counter across 8 bytes puts the domain
            //    above 2^32 so collisions exist and appear at the birthday rate.
            let (id_a, id_b) = {
                let mut seen: HashMap<u32, [u8; 32]> = HashMap::new();
                let mut found = None;
                for i in 0u64..2_000_000 {
                    let mut raw = [7u8; 32];
                    // Golden-ratio multiply spreads the counter over 64 bits.
                    raw[..8].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
                    let key = ContractKey::from_id_and_code(
                        ContractInstanceId::new(raw),
                        CodeHash::new([1u8; 32]),
                    );
                    let h = contract_hash(&key);
                    if let Some(prev) = seen.insert(h, raw) {
                        if prev != raw {
                            found = Some((prev, raw));
                            break;
                        }
                    }
                }
                found.expect("an FNV-1a collision must exist within 2M candidates")
            };

            let key_a = ContractKey::from_id_and_code(
                ContractInstanceId::new(id_a),
                CodeHash::new([1; 32]),
            );
            let key_b = ContractKey::from_id_and_code(
                ContractInstanceId::new(id_b),
                CodeHash::new([1; 32]),
            );
            assert_ne!(key_a, key_b, "the two contracts must be distinct");
            assert_eq!(
                contract_hash(&key_a),
                contract_hash(&key_b),
                "premise: the two contracts must COLLIDE under contract_hash, \
                 or this test is not exercising the collision path at all"
            );
            let shared_hash = contract_hash(&key_a);

            // Host BOTH on the node. The stand-in handler answers every
            // GetSummaryQuery with the same bytes, so their local summaries are
            // byte-identical — which is what lets one digest agree against both
            // and makes the dropped entry invisible.
            let h = build_harness("hf-collision", 17080, vec![3u8; 64]).await;
            for k in [key_a, key_b] {
                let _ = h.op_manager.ring.host_contract(
                    k,
                    128,
                    crate::ring::AccessType::Put,
                    crate::ring::HostingCause::Other,
                );
                h.op_manager.interest_manager.register_local_hosting(&k);
            }

            let agreeing = summary_digest(&h.our_summary);
            let diverged = summary_digest(b"a genuinely different state");
            assert_ne!(agreeing, diverged);

            // Exactly what a peer hosting both contracts sends: one entry per
            // CONTRACT, both carrying the same (colliding) hash.
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    emitter: crate::message::SummariesEmitter::Other,
                    entries: vec![
                        SummaryDigestEntry {
                            hash: shared_hash,
                            summary_digest: Some(agreeing),
                        },
                        SummaryDigestEntry {
                            hash: shared_hash,
                            summary_digest: Some(diverged),
                        },
                    ],
                },
            )
            .await;

            match reply {
                Some(InterestMessage::SummaryRequest { hashes }) => {
                    assert!(
                        hashes.contains(&shared_hash),
                        "the diverging entry must provoke a SummaryRequest for \
                         its hash, got {hashes:?}"
                    );
                    assert_eq!(
                        hashes.iter().filter(|x| **x == shared_hash).count(),
                        1,
                        "the hash must still appear at most ONCE — the \
                         collision-inflation bound is independent of the \
                         per-entry dedup and must survive the fix"
                    );
                }
                other => panic!(
                    "the second (diverging) entry was dropped: no SummaryRequest \
                     fired, so this node will report converged forever while the \
                     peer holds different state. Got {other:?}"
                ),
            }
        }

        /// A peer naming ONE hash with many distinct fabricated digests must
        /// not multiply the expensive work (codex P1 on the collision fix).
        ///
        /// # The amplification this bounds
        ///
        /// The collision fix had to dedup on `(hash, digest)` rather than on
        /// hash alone, which was correct — but hash-dedup had been
        /// *incidentally* bounding something else: how many times
        /// `summary_if_hosted_or_in_use` runs. That is a contract-handler round
        /// trip, sequential, on the loop the executor needs responsive. With
        /// pair-dedup and no further bound, one message could name a single
        /// known hash with up to `MAX_SUMMARY_COMPARISONS_PER_MESSAGE` distinct
        /// digests and force that many fetches per matching contract.
        ///
        /// Two mechanisms bound it:
        ///
        /// - a per-message summary cache, so a contract is fetched at most once
        ///   per message however many pairs name it;
        /// - skip-once-requested, so pairs arriving after the hash is already
        ///   on the request list are not processed at all.
        ///
        /// The skip cannot lose a heal: the full-bytes reply already on its way
        /// carries our summaries for ALL contracts matching that hash.
        ///
        /// # What this test does and does NOT discriminate
        ///
        /// Stated because the obvious reading is wrong. Mutation-tested three
        /// ways: removing the cache alone PASSES, removing the skip alone
        /// PASSES, removing BOTH fails at one fetch per pair. So this test
        /// bounds the CONJUNCTION, not either mechanism individually.
        ///
        /// The fixture was 512 pairs until #5238 lowered the per-message
        /// comparison cap to 64; it is now sized just under that cap, so the
        /// cap is still not what this test measures. The amplification factor
        /// it demonstrates shrank with the fixture, but its discrimination did
        /// not: removing both mechanisms still yields one fetch per pair
        /// against an assertion of at most one in total.
        ///
        /// That is not a defect in the test so much as a property of the fix:
        /// each mechanism is independently sufficient for this observable. Only
        /// one digest value can agree with a given local contract (the digest
        /// IS the hash of our summary bytes), so the first fabricated pair
        /// mismatches, arms the skip, and the hash goes inert — which holds the
        /// count to ~1 even with no cache; and the cache holds it to 1 even
        /// with no skip. They are deliberate defence in depth.
        ///
        /// A test that isolated one would need a scenario where the skip cannot
        /// arm (no request fires) yet many pairs still resolve — and pair-dedup
        /// makes that unconstructible, since all `None`-digest pairs for a hash
        /// collapse to one. If a future change makes them separable, split this
        /// test then.
        #[tokio::test]
        async fn many_digests_for_one_hash_do_not_multiply_summary_fetches() {
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-amp", 17090, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);

            // 63 distinct fabricated digests, all naming the ONE hash this
            // node tracks. Every one of them is a real, distinct pair, so
            // pair-dedup does not collapse them.
            let entries: Vec<SummaryDigestEntry> = (0..63u32)
                .map(|i| SummaryDigestEntry {
                    hash,
                    summary_digest: Some(summary_digest(&i.to_le_bytes())),
                })
                .collect();
            let pairs = entries.len();
            assert!(
                pairs < MAX_SUMMARY_COMPARISONS_PER_MESSAGE,
                "premise: stay under the cap so the CAP is not what bounds \
                 this — the cache and the skip must be doing the work"
            );

            let before = h.summary_queries.load(Ordering::Relaxed);
            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    entries,
                    emitter: crate::message::SummariesEmitter::Other,
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            // One contract matches this hash, so one fetch is the floor. The
            // bound is what matters: NOT proportional to the pair count.
            assert!(
                fetches <= 1,
                "{pairs} fabricated digests for one hash caused {fetches} \
                 summary fetches; the per-message cache should hold it to at \
                 most one per matching contract. A count near {pairs} means the \
                 cache is gone and a peer can monopolize the contract loop with \
                 a single message."
            );

            // And the divergence is still handled: the hash IS requested.
            match reply {
                Some(InterestMessage::SummaryRequest { hashes }) => {
                    assert_eq!(
                        hashes,
                        vec![hash],
                        "the hash must be requested exactly once — bounding the \
                         work must not cost the request, and must not let \
                         {pairs} pairs inflate it either"
                    );
                }
                other => panic!(
                    "fabricated digests all mismatch our summary, so the bytes \
                     must be requested; got {other:?}"
                ),
            }
        }

        /// Processing many DISTINCT locally-hosted hashes must not accumulate
        /// their summaries (codex P1, round 3).
        ///
        /// # The retention this bounds
        ///
        /// The local-summary cache holds OWNED `StateSummary` clones. Scoped to
        /// the MESSAGE, it retained one per matched contract until the whole
        /// message finished — so a peer naming many distinct hashes it knows we
        /// host accumulates (matched contracts x summary size). At the
        /// per-message cap with River-scale summaries (~33 KB) that is hundreds
        /// of MB from a single message: the large-value retention class
        /// `contract/executor.rs` byte-bounds its own summary cache for.
        ///
        /// Scoping the cache to the CURRENT hash bounds retention to one hash's
        /// contract set. This test asserts that directly through the peak cache
        /// size, rather than through a fetch count — fetches and retention are
        /// different quantities and only the latter is the OOM risk.
        ///
        /// Note the peak is asserted, not the final size: a cache that grew and
        /// was cleared only at the END of the message would leave a final size
        /// of ~0 while having held everything at once.
        #[tokio::test]
        async fn distinct_hashes_do_not_accumulate_cached_summaries() {
            use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

            let h = build_harness("hf-retain", 17100, vec![5u8; 128]).await;

            // 64 contracts this node hosts, each with its own distinct hash.
            let mut hashes = Vec::new();
            for i in 0u8..64 {
                let k = ContractKey::from_id_and_code(
                    ContractInstanceId::new([i.wrapping_add(100); 32]),
                    CodeHash::new([i; 32]),
                );
                let _ = h.op_manager.ring.host_contract(
                    k,
                    128,
                    crate::ring::AccessType::Put,
                    crate::ring::HostingCause::Other,
                );
                h.op_manager.interest_manager.register_local_hosting(&k);
                hashes.push(contract_hash(&k));
            }
            hashes.sort_unstable();
            hashes.dedup();
            assert!(
                hashes.len() >= 32,
                "premise: the fixture needs many DISTINCT hashes to accumulate, \
                 got {} — if they collided this tests the wrong thing",
                hashes.len()
            );

            crate::config::GlobalTestMetrics::reset();

            // One entry per hash, each digest divergent so every hash resolves
            // and caches its contract's summary.
            let entries: Vec<SummaryDigestEntry> = hashes
                .iter()
                .map(|hash| SummaryDigestEntry {
                    hash: *hash,
                    summary_digest: Some(summary_digest(b"divergent")),
                })
                .collect();

            let _ = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    entries,
                    emitter: crate::message::SummariesEmitter::Other,
                },
            )
            .await;

            let peak = crate::config::GlobalTestMetrics::summary_cache_peak();
            assert!(
                peak <= 4,
                "the local-summary cache peaked at {peak} entries across \
                 {} distinct hashes. It must be bounded by ONE hash's contract \
                 set (1 here), not by how many hashes a peer names — a peak \
                 tracking the hash count means owned summary clones accumulate \
                 for the whole message, which is hundreds of MB at the cap with \
                 real summaries.",
                hashes.len()
            );
        }

        /// INTERLEAVED hashes must cost the same as grouped ones — the
        /// receiver's work must not depend on the order the peer chose.
        ///
        /// # The finding this pins (codex P1, round 4)
        ///
        /// Three earlier findings on this arm were all the same root cause:
        /// with entries walked in WIRE ORDER, a peer choosing an interleaving
        /// (A, B, A, B, ...) forced the per-hash summary cache to clear and
        /// refetch on every revisit, reopening the CPU bound the cache was
        /// added to close.
        ///
        /// A prior comment argued this was impossible because "the skip arms
        /// after a hash's first mismatch, so the hash goes inert". **That
        /// argument was false.** `DigestVerdict::Agree` does not set
        /// `needs_bytes` — only the `NeedBytes` arm does — so a round of
        /// NON-ARMING entries (agreements, or a `None` digest) leaves every
        /// hash live and revisitable. The reasoning held for mismatching pairs
        /// and was wrongly generalised to all pairs.
        ///
        /// The fix groups entries by hash before any work, so ordering is not
        /// the peer's to choose. This test asserts that property directly: the
        /// same entries interleaved must cost ~one fetch per contract, not one
        /// per pair.
        #[tokio::test]
        async fn interleaved_hashes_cost_the_same_as_grouped_ones() {
            use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
            use std::sync::atomic::Ordering;

            let h = build_harness("hf-interleave", 17110, vec![5u8; 128]).await;

            // 21 hosted contracts, each its own hash. Sized so that the three
            // interleaved rounds below total 63 entries — just under #5238's
            // 64-entry per-message comparison cap, which must not be what
            // bounds this test (it was 32 contracts / 96 entries before that
            // cap existed).
            let mut hashes = Vec::new();
            for i in 0u8..21 {
                let k = ContractKey::from_id_and_code(
                    ContractInstanceId::new([i.wrapping_add(160); 32]),
                    CodeHash::new([i.wrapping_add(3); 32]),
                );
                let _ = h.op_manager.ring.host_contract(
                    k,
                    128,
                    crate::ring::AccessType::Put,
                    crate::ring::HostingCause::Other,
                );
                h.op_manager.interest_manager.register_local_hosting(&k);
                hashes.push(contract_hash(&k));
            }
            hashes.sort_unstable();
            hashes.dedup();
            let n = hashes.len();
            assert!(n >= 12, "premise: need many distinct hashes, got {n}");

            // Three INTERLEAVED rounds, and the SHAPES matter — this is where
            // a first attempt at this test went wrong. A round of MISMATCHING
            // digests arms the skip on its first visit, after which the hash is
            // inert and never revisited, so a fixture built only from
            // fabricated digests costs ~one visit per hash even in wire order
            // and cannot detect the bug at all.
            //
            // The revisitable rounds are the NON-ARMING ones: `Agree` (digest
            // equals our summary's) and `PeerHasNoState` (`None`). Neither sets
            // `needs_bytes`, so neither arms the skip. Pair dedup means there is
            // exactly ONE distinct non-arming pair of each kind per hash — the
            // agreeing digest has only one possible value, and so does `None` —
            // which also bounds the real-world amplification to ~3 visits per
            // hash rather than the pair count.
            let agreeing = summary_digest(&h.our_summary);
            let mut entries = Vec::new();
            for hash in &hashes {
                entries.push(SummaryDigestEntry {
                    hash: *hash,
                    summary_digest: Some(agreeing),
                });
            }
            for hash in &hashes {
                entries.push(SummaryDigestEntry {
                    hash: *hash,
                    summary_digest: None,
                });
            }
            for hash in &hashes {
                entries.push(SummaryDigestEntry {
                    hash: *hash,
                    summary_digest: Some(summary_digest(&hash.to_le_bytes())),
                });
            }
            assert!(
                entries.len() < MAX_SUMMARY_COMPARISONS_PER_MESSAGE,
                "premise: stay under the cap so the CAP is not what bounds this"
            );

            let before = h.summary_queries.load(Ordering::Relaxed);
            let _ = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    entries,
                    emitter: crate::message::SummariesEmitter::Other,
                },
            )
            .await;
            let fetches = h.summary_queries.load(Ordering::Relaxed) - before;

            // One contract per hash, so `n` fetches is the content-determined
            // floor. Without grouping this is ~4n (one per round per hash).
            assert!(
                fetches <= n,
                "interleaving {n} hashes over 3 non-arming rounds caused \
                 {fetches} summary \
                 fetches; grouping should hold it to at most {n} — one per \
                 contract. A count scaling with the ROUND COUNT means the peer's \
                 chosen ordering still drives our work."
            );
        }

        /// Repeated hashes must be free: a peer that names the same contract
        /// 20 times must provoke exactly ONE entry in the request.
        ///
        /// Entry count is deliberately kept UNDER
        /// [`MAX_SUMMARY_COMPARISONS_PER_MESSAGE`] so the handler's over-cap
        /// rotation is the identity and this test is deterministic. The
        /// over-cap behaviour is the sibling test below; separating them is
        /// what lets BOTH have hard assertions.
        ///
        /// History: this test previously mixed the two concerns — 5,064
        /// entries against a 256 cap — so its outcome depended on where the
        /// random rotation landed, and its `None` arm was an unconditional
        /// pass. It therefore asserted nothing in roughly 94% of runs, and
        /// `GlobalRng` is unseeded in a plain `#[tokio::test]`, so which runs
        /// those were varied per invocation.
        #[tokio::test]
        async fn repeated_digest_hashes_are_deduplicated() {
            let h = build_harness("hf-dedup", 17060, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);

            let mut entries = vec![
                SummaryDigestEntry {
                    hash,
                    summary_digest: Some(summary_digest(b"divergent")),
                };
                20
            ];
            // A short tail of unknown hashes, still under the cap. Sized to 63
            // entries in total: #5238 lowered the per-message comparison cap to
            // 64, and going over it would reintroduce exactly the
            // rotation-dependent flakiness the History note below describes.
            for i in 0..43u32 {
                entries.push(SummaryDigestEntry {
                    hash: hash.wrapping_add(i + 1),
                    summary_digest: Some(summary_digest(b"divergent")),
                });
            }
            assert!(
                entries.len() < MAX_SUMMARY_ENTRIES_PER_MESSAGE,
                "premise: the fixture must stay under the ENTRY ceiling so no \
                 rotation occurs and this test is deterministic. Anchored to the \
                 ceiling rather than to MAX_SUMMARY_COMPARISONS_PER_MESSAGE \
                 because since #5338 that is the constant the rotation triggers \
                 on — the two were the same number before, so the old anchor \
                 held for the wrong reason"
            );

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    entries,
                    emitter: crate::message::SummariesEmitter::Other,
                },
            )
            .await;

            match reply {
                Some(InterestMessage::SummaryRequest { hashes }) => {
                    assert_eq!(
                        hashes.iter().filter(|h| **h == hash).count(),
                        1,
                        "a hash repeated 20 times must appear in the request \
                         exactly once — repetition must buy the sender nothing"
                    );
                    assert_eq!(
                        hashes.len(),
                        1,
                        "only the one locally-tracked contract may be \
                         requested; the 43 unknown hashes resolve to nothing"
                    );
                }
                other => panic!(
                    "a divergent digest for a tracked contract must provoke a \
                     SummaryRequest, got {other:?}"
                ),
            }
        }

        /// A massively over-cap digest message must stay bounded and must not
        /// panic, and the request it provokes must never exceed the cap.
        ///
        /// `GlobalRng` is seeded explicitly: the handler rotates its processing
        /// window by a random offset when the cap binds, and an unseeded
        /// `#[tokio::test]` falls back to `rand::rng()` (config.rs), making the
        /// outcome vary per invocation. Any test that depends on rotation or
        /// sampling must seed.
        #[tokio::test]
        async fn over_cap_digest_message_stays_bounded() {
            // Guarded, like the two rotation tests. This was the last bare
            // `set_seed` in the file: it pins THREAD_SEED, THREAD_RNG and
            // THREAD_INDEX and never cleared them, on the success path as well
            // as on panic. That is the cross-test-interference class
            // `.claude/rules/bug-prevention-patterns.md` records from #5314,
            // which plain `cargo test` can see and `cargo nextest` structurally
            // cannot.
            let _seed = crate::config::GlobalRng::seed_guard(0x0496_5CA9);
            let h = build_harness("hf-cap", 17065, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);

            // #5238: the KNOWN hash is INTERLEAVED through the unknowns, once
            // every 32 entries, not appended as a block.
            //
            // Without a known hash at all, nothing resolves, the
            // `SummaryRequest` arm below is unreachable and this degenerates
            // into a no-panic smoke test — tolerable when the cap was 4,096 of
            // 6,096 entries, much less so now that only 64 are processed.
            //
            // Appending it as a block does not work either, and the reason is
            // worth recording because it is not the obvious probability
            // argument: pair dedup collapses every copy of the known hash to a
            // SINGLE pair, so a 2,000-entry block still contributes one, and
            // the scan stops after 64 DISTINCT pairs. A rotation landing
            // anywhere in the ~6,000 unknowns therefore fills its quota long
            // before reaching the block. Measured: it missed outright under the
            // seeded rotation. Interleaving at a period below the cap puts a
            // copy inside every possible 64-entry window, so inclusion is
            // structural rather than probabilistic.
            let mut entries = Vec::new();
            for i in 0..(MAX_SUMMARY_HASHES_PER_MESSAGE as u32 + 2_000) {
                entries.push(SummaryDigestEntry {
                    hash: hash.wrapping_add(i + 1),
                    summary_digest: Some(summary_digest(b"divergent")),
                });
                if i % 32 == 0 {
                    entries.push(SummaryDigestEntry {
                        hash,
                        summary_digest: Some(summary_digest(b"divergent")),
                    });
                }
            }
            assert!(
                entries.len() > MAX_SUMMARY_COMPARISONS_PER_MESSAGE,
                "premise: the fixture must EXCEED the cap, or the bound below \
                 is not being exercised"
            );

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryDigests {
                    entries,
                    emitter: crate::message::SummariesEmitter::Other,
                },
            )
            .await;

            // The known hash MUST come back, and it must come back alone.
            //
            // The interleaved shape is what makes this a real assertion rather
            // than a no-panic smoke test. The ~6,096 unknown hashes resolve to
            // nothing whatever the peer does, so before the known hash was
            // introduced this arm was unreachable and the bound assertion ran
            // against an empty set.
            //
            // Inclusion is STRUCTURAL, not probabilistic — see the fixture
            // construction above. The known hash is interleaved at a period
            // below the 64-distinct-pair scan, so a copy falls inside every
            // possible window and no rotation offset can miss it. The obvious
            // alternative, appending the copies as one contiguous block, does
            // NOT work: pair dedup collapses them to a single pair and the scan
            // stops after 64 DISTINCT pairs, so a rotation landing anywhere in
            // the unknowns fills its quota long before reaching the block. That
            // is a near-certainty rather than a low probability, and it failed
            // on the first run rather than flaking.
            //
            // Pair dedup still does the other half of the work: all copies of
            // the known hash share one (hash, digest) pair, so the reply can
            // name it at most once however the rotation lands.
            //
            // What this test is NOT: a regression pin for #5238. Under a full
            // revert to the 4,096 cap the fixture still exceeds it, the known
            // hash is still found by the same gap argument, and the reply is
            // still `vec![hash]` — so it passes either way. It is a repair of a
            // previously VACUOUS test, which is worth having on its own terms,
            // but the bound on this change is pinned by the five tests that do
            // fail on a targeted revert. Do not count this one among them.
            match reply {
                Some(InterestMessage::SummaryRequest { hashes }) => {
                    assert!(
                        hashes.len() <= MAX_SUMMARY_COMPARISONS_PER_MESSAGE,
                        "the request must stay bounded by \
                         MAX_SUMMARY_COMPARISONS_PER_MESSAGE, got {}",
                        hashes.len()
                    );
                    assert_eq!(
                        hashes,
                        vec![hash],
                        "an over-cap message must still answer for the one \
                         contract this node tracks, and must not let 6,096 \
                         unknown hashes inflate the reply"
                    );
                }
                other => panic!(
                    "the known hash must provoke a bounded SummaryRequest; got \
                     {other:?}"
                ),
            }
        }

        /// A `SummaryRequest` naming a huge hash list must not produce an
        /// unbounded reply, and must only ever name contracts we already
        /// track.
        #[tokio::test]
        async fn summary_request_reply_is_bounded_and_scoped_to_known_contracts() {
            let h = build_harness("hf-req-bound", 17070, vec![5u8; 128]).await;
            let hash = contract_hash(&h.key);
            let mut hashes: Vec<u32> = (0..10_000u32).map(|i| hash.wrapping_add(i + 1)).collect();
            hashes.insert(0, hash);

            let reply = handle_interest_sync_message(
                &h.op_manager,
                h.new_peer,
                InterestMessage::SummaryRequest { hashes },
            )
            .await;

            match reply {
                Some(InterestMessage::Summaries { entries, .. }) => {
                    assert_eq!(
                        entries.len(),
                        1,
                        "only the one contract this node actually tracks may be \
                         answered — a peer must not be able to enumerate or \
                         inflate the reply with hashes we know nothing about"
                    );
                    assert_eq!(entries[0].hash, hash);
                }
                other => panic!("expected Summaries for the one known hash, got {other:?}"),
            }
        }

        /// Source pin: production code must never construct
        /// `InterestMessage::Summaries` directly — only through
        /// [`full_summaries_message`], which records the summary bytes it puts
        /// on the wire.
        ///
        /// The whole #4965 falsifier is the reading "`summary_full_bytes() == 0`
        /// means not one summary byte was sent". A construction site that
        /// bypassed the constructor would not make that reading untested — it
        /// would make it FALSE, while every test in this PR stayed green. The
        /// counter is the only thing standing between "we measured the win" and
        /// "we assumed it".
        ///
        /// Scoped to the production halves of the two files that build these
        /// messages, cut at their test modules so the test fixtures below
        /// (which legitimately build `Summaries` by hand to feed the handler)
        /// do not trip it.
        #[test]
        fn no_uninstrumented_full_summaries_construction() {
            // `node.rs`'s FIRST `#[cfg(test)]` is near the top of the file, so
            // cutting there would truncate the production code this pin exists
            // to scan and pass vacuously. Cut at the outer `mod tests` instead.
            let node_src = include_str!("node.rs");
            let node_prod = &node_src[..node_src
                .find("\n#[cfg(test)]\nmod tests {")
                .expect("node.rs outer test module not found")];
            assert!(
                node_prod.contains("fn handle_interest_sync_message("),
                "the production slice must actually contain the handler — if \
                 this fails the cut point moved and the scan below is vacuous"
            );

            let update_src = include_str!("operations/update.rs");
            let update_prod = &update_src[..update_src
                .find("\n#[cfg(test)]")
                .unwrap_or(update_src.len())];

            // The constructor itself is the one legitimate construction site.
            let ctor = node_prod
                .find("pub(crate) fn full_summaries_message(")
                .expect("full_summaries_message constructor not found");
            let ctor_end = ctor
                + node_prod[ctor..]
                    .find("\n}\n")
                    .expect("constructor body end not found");

            for (name, src, allowed) in [
                ("node.rs", node_prod, Some((ctor, ctor_end))),
                ("operations/update.rs", update_prod, None),
            ] {
                let needle = concat!("InterestMessage::Summaries", " {");
                let mut from = 0usize;
                let mut constructions = 0usize;
                while let Some(off) = src[from..].find(needle) {
                    let at = from + off;
                    from = at + needle.len();

                    // Skip MATCH PATTERNS. `InterestMessage::Summaries { .. }`
                    // reads identically whether it destructures or constructs,
                    // and the handler necessarily matches on it. A pattern is
                    // followed by `=>` after its closing brace; a construction
                    // is not.
                    let Some(close) = src[at..].find('}') else {
                        continue;
                    };
                    let after = src[at + close + 1..].trim_start();
                    if after.starts_with("=>") {
                        continue;
                    }

                    constructions += 1;
                    let inside_ctor = allowed.is_some_and(|(a, b)| at >= a && at <= b);
                    assert!(
                        inside_ctor,
                        "{name} constructs `InterestMessage::Summaries` outside \
                         `full_summaries_message` (byte offset {at}). Route it \
                         through the constructor: an uninstrumented site makes \
                         `summary_full_bytes() == 0` mean nothing, silently."
                    );
                }

                // Positive control: node.rs MUST contain the one legitimate
                // construction. Without this the scan passes vacuously if the
                // needle or the pattern-skip ever stops matching anything.
                if allowed.is_some() {
                    assert_eq!(
                        constructions, 1,
                        "expected exactly one `InterestMessage::Summaries` \
                         construction in {name} (inside full_summaries_message); \
                         found {constructions}. Zero means this scan is vacuous."
                    );
                }
            }
        }

        /// Source pin: the `SummaryRequest` arm must build its reply as a plain
        /// `InterestMessage::Summaries`, NEVER through
        /// `summaries_reply_for_peer`.
        ///
        /// Routing it through the encoding chooser would answer a request FOR
        /// BYTES with more digests, and the two sides would ping-pong
        /// (digests → request → digests → …) forever. Behavioural coverage
        /// exists above, but only for the one shape a test can construct; this
        /// pins the decision itself.
        #[test]
        fn summary_request_reply_is_always_full_bytes() {
            let src = include_str!("node.rs");
            let arm = src
                .find("InterestMessage::SummaryRequest { hashes } => {")
                .expect("SummaryRequest arm not found");
            // End at the NEXT arm, not at a later one: the arms between
            // would drag their own `summaries_reply_for_peer` into the window
            // and make the negative assertion below fire on innocent code.
            let end = src[arm..]
                .find("InterestMessage::ChangeInterests { added, removed } => {")
                .expect("end of SummaryRequest arm not found");
            let body = &code_only(&src[arm..arm + end]);
            assert!(
                body.contains("get_matching_contracts"),
                "window extraction is off — the SummaryRequest arm body should \
                 contain its get_matching_contracts lookup"
            );
            assert!(
                body.contains("full_summaries_message("),
                "the SummaryRequest arm must reply through \
                 full_summaries_message — the instrumented full-bytes \
                 constructor"
            );
            // Every entry point to the encoding CHOICE, not just the original
            // one. #5155 split `summaries_reply_for_peer` into
            // `summary_reply_form` + `summaries_reply_in_form`, and neither new
            // name is a substring of the old one — so a future edit routing
            // this arm through the split pair would reintroduce the
            // digests → request → digests loop with this pin still green.
            for banned in [
                "summaries_reply_for_peer",
                "summary_reply_form",
                "summaries_reply_in_form",
            ] {
                assert!(
                    !body.contains(banned),
                    "the SummaryRequest arm must NOT route through {banned}: \
                     answering a request for bytes with digests loops the \
                     exchange (digests → request → digests → …)"
                );
            }
        }

        /// Source pin: the `SummaryDigests` arm must reach the heal through the
        /// SHARED `emit_stale_peer_syncs`, and must not grow its own emission.
        ///
        /// A second copy of the heal path is how "hash-first traded bandwidth
        /// for convergence" happens: the copies drift, the digest arm loses a
        /// guard (the ban check, the budget, the targeting), and nothing fails
        /// until production.
        #[test]
        fn digest_arm_shares_the_single_heal_path() {
            let src = include_str!("node.rs");
            let arm = src
                .find("InterestMessage::SummaryDigests { entries, .. } => {")
                .expect("SummaryDigests arm not found");
            let end = src[arm..]
                .find("InterestMessage::SummaryRequest { hashes } => {")
                .expect("end of SummaryDigests arm not found");
            let body = &code_only(&src[arm..arm + end]);
            assert!(
                body.contains(
                    "emit_stale_peer_syncs(op_manager, source, peer_key.as_ref(), stale_contracts)"
                ),
                "the SummaryDigests arm must delegate healing to the shared \
                 emit_stale_peer_syncs"
            );
            assert!(
                !body.contains("stale_peer_sync_event("),
                "the SummaryDigests arm must not construct heal events itself \
                 — a second emission path drifts from the shared one and \
                 loses its ban check / budget / targeting guards"
            );
            assert!(
                !body.contains("NodeEvent::BroadcastStateChange"),
                "a heal from the digest arm must never become an \
                 all-subscriber fan-out (#3791/#3796)"
            );
            assert!(
                body.contains("summary_indicates_stale_peer"),
                "the SummaryDigests arm must RUN the staleness predicate on \
                 agreement rather than assume its answer — the assumption \
                 ('identical summaries are never stale') is a property of the \
                 predicate, and baking it in here is how a digest match would \
                 come to short-circuit a real heal"
            );
            assert!(
                body.contains("record_summary_comparison"),
                "the digest-agreement path must still record the #4965 \
                 identical/differing comparison, or the telemetry that \
                 justifies this change stops being able to measure it"
            );
        }

        /// The R4b agreement-rate instrument (#5153), driven through the REAL
        /// receive arm.
        ///
        /// R4b would swap the proactive summary notification's full summary
        /// bytes for a 21-byte digest, and that trade wins or loses entirely on
        /// `p`, the agreement rate ON THAT LEG. The fleet-wide rate cannot
        /// answer it: comparisons are per ENTRY, and a notification carries
        /// 1.000 entries/message against the heartbeat's 222.97, so a 99.73%
        /// aggregate is essentially the heartbeat's population.
        ///
        /// The measurement is possible today only because a notification
        /// already arrives here as a single-entry FULL-BYTES `Summaries` and
        /// this arm already performs the comparison a digest would have made.
        /// These tests live at the handler level rather than on `OutboundMix`
        /// because a unit test on the counter cannot tell a live discriminator
        /// from a constant: only driving the real arm can, and what it asserts
        /// on is the rollup body production telemetry would actually ship.
        mod r4b_single_entry_instrument {
            use super::*;

            /// Feed one full-bytes `Summaries` message and return the rollup
            /// body the node would emit for the window.
            async fn report(
                h: &Harness,
                peer: SocketAddr,
                entries: Vec<SummaryEntry>,
            ) -> serde_json::Value {
                handle_interest_sync_message(
                    &h.op_manager,
                    peer,
                    InterestMessage::Summaries {
                        emitter: crate::message::SummariesEmitter::Other,
                        entries,
                    },
                )
                .await;
                h.op_manager.outbound_mix.rollup_body_for_test()
            }

            fn count(body: &serde_json::Value, key: &str) -> u64 {
                body.get(key)
                    .and_then(|v| v.as_u64())
                    .unwrap_or_else(|| panic!("rollup body must carry `{key}`"))
            }

            /// A summary entry for `key` carrying `summary`.
            fn entry_for(key: &ContractKey, summary: &[u8]) -> SummaryEntry {
                SummaryEntry {
                    hash: contract_hash(key),
                    summary_bytes: Some(summary.to_vec()),
                }
            }

            /// A SINGLE-entry message's comparison lands in both the total and
            /// the single-entry bucket; a MULTI-entry one lands in the total
            /// only.
            ///
            /// The two halves are the whole instrument. Only the second can
            /// fail if the discriminator is replaced by a constant `true`, and
            /// only the first if it is replaced by `false` — the realistic
            /// mutations, since either is one token. Asserting the multi-entry
            /// message adds EXACTLY nothing to the single bucket (rather than
            /// merely less) is what stops that bucket from quietly measuring
            /// something other than message shape.
            #[tokio::test]
            async fn message_shape_decides_which_bucket_an_agreement_lands_in() {
                let ours = vec![4u8; 64];
                let h = build_harness("r4b-shape", 17300, ours.clone()).await;
                // A second locally-hosted, summarizable contract, so the
                // multi-entry message is genuinely multi-CONTRACT rather than a
                // repeat the per-message dedup would collapse.
                let second = host_many(&h, 1);
                let hashes = distinct_hashes(&[h.key, second[0]]);
                assert_eq!(hashes.len(), 2, "premise: two distinct advertised hashes");

                // SINGLE entry, agreeing.
                let body = report(&h, h.new_peer, vec![entry_for(&h.key, &ours)]).await;
                assert_eq!(
                    count(&body, "summary_entries_identical"),
                    1,
                    "premise: the fixture must produce a real two-sided \
                     agreement, or neither bucket is under test"
                );
                assert_eq!(
                    count(&body, "summary_entries_identical_single"),
                    1,
                    "a single-entry `Summaries` IS the notification leg's shape \
                     — its agreement must reach the single-entry bucket, or \
                     `p` reads as zero and R4b looks unbuildable"
                );

                // MULTI entry, both agreeing. Counters are cumulative over the
                // window, so the deltas are what matter.
                let body = report(
                    &h,
                    h.new_peer,
                    vec![entry_for(&h.key, &ours), entry_for(&second[0], &ours)],
                )
                .await;
                assert_eq!(
                    count(&body, "summary_entries_identical"),
                    3,
                    "premise: both entries of the multi-entry message must \
                     resolve to a locally-interested, summarizable contract, \
                     or the multi-entry case is not actually exercised"
                );
                assert_eq!(
                    count(&body, "summary_entries_identical_single"),
                    1,
                    "a MULTI-entry message is heartbeat-shaped and must add \
                     nothing to the single-entry bucket — a bucket that grows \
                     here is counting comparisons, not message shape, and `p` \
                     silently becomes the fleet-wide rate the instrument \
                     exists because it cannot use"
                );
            }

            /// The same split applies to DISAGREEMENT, which is `p`'s
            /// denominator.
            ///
            /// Without it a low single-entry agreement COUNT and a low
            /// single-entry comparison VOLUME are indistinguishable — the ratio
            /// would have a numerator and no denominator.
            #[tokio::test]
            async fn message_shape_decides_which_bucket_a_disagreement_lands_in() {
                let ours = vec![4u8; 64];
                let theirs = vec![9u8; 64];
                let h = build_harness("r4b-shape-diff", 17310, ours.clone()).await;
                let second = host_many(&h, 1);

                let body = report(&h, h.new_peer, vec![entry_for(&h.key, &theirs)]).await;
                assert_eq!(count(&body, "summary_entries_differing"), 1);
                assert_eq!(
                    count(&body, "summary_entries_differing_single"),
                    1,
                    "the notification leg's disagreements are `p`'s denominator"
                );

                let body = report(
                    &h,
                    h.new_peer,
                    vec![entry_for(&h.key, &theirs), entry_for(&second[0], &theirs)],
                )
                .await;
                assert_eq!(count(&body, "summary_entries_differing"), 3);
                assert_eq!(
                    count(&body, "summary_entries_differing_single"),
                    1,
                    "a multi-entry message must add nothing to the \
                     single-entry denominator"
                );

                // The per-contract attribution carries the subset too, which is
                // what separates "a few non-converging contracts" (structural
                // `p` = 0) from "the leg disagrees in general".
                let listed = body
                    .get("summary_differing_contracts")
                    .and_then(|v| v.as_array())
                    .expect("summary_differing_contracts must be an array");
                let mine = listed
                    .iter()
                    .find(|e| {
                        e.get("contract").and_then(|v| v.as_str())
                            == Some(h.key.id().to_string().as_str())
                    })
                    .expect("the diverging contract must be named");
                assert_eq!(mine.get("count").and_then(|v| v.as_u64()), Some(2));
                assert_eq!(
                    mine.get("single_count").and_then(|v| v.as_u64()),
                    Some(1),
                    "per-contract attribution must carry the notification-leg \
                     subset, not just the total"
                );
            }

            /// Every `OutboundMix` summary-observation call in this file passes
            /// the message-shape discriminator, and there are exactly as many
            /// such call sites as this pin knows about.
            ///
            /// The behavioural tests above cover the two-sided sites. They
            /// cannot reach the ONE-SIDED sites, which need a contract this node
            /// is interested in but cannot summarize — so a future edit could
            /// hard-code `false` there and stay green. This pin closes that gap,
            /// and fails on a NEW call site too, which is the case that would
            /// otherwise silently under-count.
            ///
            /// Two scoping rules make it non-vacuous, and both were needed:
            ///
            /// * Comments are stripped (`code_only`) — the prose around these
            ///   call sites names `single_entry` repeatedly, so a
            ///   comment-inclusive scan would pass with the argument deleted.
            /// * The scan stops at the test module — otherwise the needles below
            ///   match THEMSELVES (string literals survive `code_only`), which
            ///   is how a self-matching pin comes to count its own text as
            ///   evidence. Confirmed by observation: the unscoped version found
            ///   6 sites, not 4.
            #[test]
            fn every_summary_observation_passes_the_message_shape() {
                const TEST_MOD: &str = "\n#[cfg(test)]\nmod tests {";
                let whole = include_str!("node.rs");
                let production_end = whole
                    .find(TEST_MOD)
                    .expect("the test module anchor must still exist — update this pin");
                let src = code_only(&whole[..production_end]);
                let sites: Vec<usize> = [
                    "outbound_mix.record_summary_comparison(",
                    "outbound_mix.record_summary_one_sided(",
                ]
                .iter()
                .flat_map(|needle| {
                    src.match_indices(needle)
                        .map(|(i, m)| i + m.len())
                        .collect::<Vec<_>>()
                })
                .collect();
                // THREE, not four: the `Summaries` arm's two (comparison +
                // one-sided) and the `SummaryDigests` arm's one (comparison
                // only). The digest arm's one-sided recording was REMOVED as a
                // double count (#5153 review F2), and this count is what will
                // notice if it comes back — it did notice when the fix landed.
                assert_eq!(
                    sites.len(),
                    3,
                    "expected 3 summary-observation call sites (two on the \
                     full-bytes `Summaries` arm, one on `SummaryDigests`); found \
                     {}. A NEW site must pass the message-shape discriminator \
                     too, and a new one-sided recording on the digest arm is the \
                     #5153 F2 double count returning",
                    sites.len()
                );
                let mut full_bytes_legs = 0;
                let mut digest_legs = 0;
                for start in sites {
                    // Bound the window at the call's closing `);` so a match
                    // cannot come from the NEXT call site's arguments.
                    let end = src[start..]
                        .find(");")
                        .map(|off| start + off)
                        .expect("a call site must have a closing `);`");
                    let args = &src[start..end];
                    assert!(
                        args.contains("single_entry"),
                        "a summary-observation call site does not pass the \
                         message-shape discriminator; its args were: {args}"
                    );
                    // The LEG matters as much as the shape: a digest-leg
                    // observation labelled full-bytes silently merges churn
                    // traffic into the R4b population (#5153 review F1), and
                    // both spellings contain `single_entry`, so the assertion
                    // above cannot tell them apart.
                    if args.contains("SummaryObservation::full_bytes(") {
                        full_bytes_legs += 1;
                    } else if args.contains("SummaryObservation::digest(") {
                        digest_legs += 1;
                    } else {
                        panic!(
                            "a summary-observation call site names neither leg \
                             constructor, so which population it lands in is \
                             unpinned; its args were: {args}"
                        );
                    }
                }
                assert_eq!(
                    (full_bytes_legs, digest_legs),
                    (2, 1),
                    "expected 2 full-bytes-leg sites and 1 digest-leg site; a \
                     digest observation recorded as full-bytes would fold \
                     churn-leg traffic into the notification population `p` is \
                     computed over"
                );
            }

            /// THE TWO PROPERTIES THIS WHOLE INSTRUMENT RESTS ON (#5153 review F4).
            ///
            /// `p` is read off single-entry FULL-BYTES `Summaries`, and that is a
            /// proxy for "this is a notification" only while both of these hold:
            ///
            /// 1. A notification ships **full bytes unconditionally** — it must
            ///    not consult the hash-first version gate. If it did, the
            ///    pre-R4b full-bytes buckets would silently go quiet AND the
            ///    claim "a digest single is provably not a notification" would
            ///    INVERT, with every test still green.
            /// 2. A notification is **single-entry** — one contract per send. If
            ///    notifications were ever batched (plausible: coalescing work is
            ///    already in this tree), they would leave the single-entry
            ///    population entirely and `p` would silently become the
            ///    agreement rate of whatever failed to batch.
            ///
            /// Both are true today, verified from code and field: the emitter
            /// calls `full_summaries_message(vec![...], Notification)` with no
            /// reference to the gate, and the field shows mean 41,642 B/msg with
            /// entries exactly equal to msgs and **no rollup anywhere reporting
            /// `max_entries > 1`**. So multi-entry false negatives are impossible
            /// today rather than merely unobserved.
            ///
            /// Neither was pinned. This mirrors
            /// `summary_request_reply_is_always_full_bytes`, which pins exactly
            /// this shape for the request-reply arm — and note that
            /// `no_uninstrumented_full_summaries_construction` does NOT cover it:
            /// that forces `Summaries` through the instrumented constructor but
            /// says nothing about a site switching to the digest gate.
            #[test]
            fn notification_leg_is_always_full_bytes_and_single_entry() {
                let src = include_str!("operations/update.rs");
                let f = src
                    .find("pub(crate) async fn send_proactive_summary_notification(")
                    .expect("notification emitter not found — update this pin");
                // Bound at the next top-level `pub(crate)` item so a sibling's
                // gate usage cannot satisfy or trip the assertions below.
                let end = src[f..]
                    .find("\npub(crate) ")
                    .map(|off| f + off)
                    .unwrap_or(src.len());
                let body = code_only(&src[f..end]);

                assert!(
                    body.contains("full_summaries_message("),
                    "the notification leg must build its message through \
                     full_summaries_message — the instrumented FULL-BYTES \
                     constructor. Window extraction may also be off; check that \
                     first."
                );
                assert!(
                    body.contains("SummariesEmitter::Notification"),
                    "window extraction is off — the notification emitter body \
                     should tag its own emitter"
                );
                // Property 1. Every entry point to the encoding CHOICE, matching
                // the sibling pin: #5155 split `summaries_reply_for_peer` into
                // two names, neither a substring of the old one.
                for banned in [
                    "summaries_reply_for_peer",
                    "summary_reply_form",
                    "summaries_reply_in_form",
                ] {
                    assert!(
                        !body.contains(banned),
                        "the notification leg now consults `{banned}`, i.e. the \
                         hash-first version gate. That moves notifications onto \
                         the digest leg, so the R4b full-bytes single-entry \
                         buckets go quiet AND `SummaryObservation::SingleEntryDigest`'s \
                         'not a notification' premise inverts — silently, with \
                         every other test green (#5153 review F4)"
                    );
                }
                // Property 2. One contract per send. `vec![` with a single
                // element is the shape; a batched send would collect or extend.
                assert!(
                    body.contains("vec![full_entry.clone()]"),
                    "the notification leg no longer sends exactly one entry. If \
                     notifications are now batched they leave the single-entry \
                     population, and `p` becomes the agreement rate of whatever \
                     failed to batch (#5153 review F4). Re-derive the proxy \
                     before changing this."
                );
            }

            /// The `SummaryDigests` arm must NOT record a one-sided observation
            /// (#5153 review F2 — it double-counted).
            ///
            /// It used to: on a digest mismatch with no local summary it recorded
            /// one-sided AND set `needs_bytes`, so the full-bytes reply observed
            /// the same contract again on its `(None, Some(_))` branch. Two
            /// messages, two per-message dedup sets, nothing suppressing the
            /// repeat — one divergence counted twice, inflating a field that is
            /// presented as an R4b cost input.
            ///
            /// Pinned at the source because the property is an ABSENCE, and the
            /// behavioural fixture for it would need a contract this node is
            /// interested in but cannot summarize. The complementary behavioural
            /// half lives in `outbound_message_mix`:
            /// `digest_leg_single_entry_observations_stay_out_of_the_full_bytes_bucket`
            /// asserts a digest-leg observation reaches no single bucket even if
            /// one did arrive.
            ///
            /// Scoped to production code and comment-stripped for the same two
            /// reasons as the pin above; the count assertion is what makes it
            /// non-vacuous, since a needle that matched nothing at all would
            /// otherwise look like a pass.
            #[test]
            fn digest_arm_records_no_one_sided_observation() {
                const TEST_MOD: &str = "\n#[cfg(test)]\nmod tests {";
                let whole = include_str!("node.rs");
                let production_end = whole
                    .find(TEST_MOD)
                    .expect("the test module anchor must still exist — update this pin");
                let src = code_only(&whole[..production_end]);

                let digest_arm = src
                    .find("InterestMessage::SummaryDigests { entries, .. } => {")
                    .expect("SummaryDigests arm not found — update this pin");
                let arm_end = src[digest_arm..]
                    .find("InterestMessage::SummaryRequest { hashes } => {")
                    .map(|off| digest_arm + off)
                    .expect("end of SummaryDigests arm not found — update this pin");
                let arm = &src[digest_arm..arm_end];

                assert!(
                    !arm.contains("record_summary_one_sided"),
                    "the SummaryDigests arm records a one-sided observation \
                     again. The full-bytes `Summaries` arm ALSO records it once \
                     the requested bytes arrive, so this counts one divergence \
                     twice and inflates summary_entries_one_sided(_single) \
                     (#5153 review F2)"
                );
                // Non-vacuity: the arm must still be the arm, and must still
                // contain the sibling observation the instrument depends on.
                assert!(
                    arm.contains("record_summary_comparison"),
                    "window no longer covers the digest arm's observation site — \
                     this pin would pass against unrelated text"
                );
                // And the full-bytes arm must still be the one that records it,
                // or the deferral loses the observation entirely.
                let full_bytes_arm = src
                    .find("InterestMessage::Summaries { entries, .. } => {")
                    .expect("Summaries arm not found — update this pin");
                let full_bytes = &src[full_bytes_arm..digest_arm];
                assert!(
                    full_bytes.contains("record_summary_one_sided"),
                    "nothing records one-sided any more: the digest arm defers to \
                     the full-bytes arm, so the full-bytes arm must still count it"
                );
            }
        }

        /// End-to-end wiring for the SHADOW-MODE futile-repair detector
        /// (`crate::ring::futile_repair`).
        ///
        /// The detector's whole claim is that it measures an OUTCOME — whether
        /// this node's repair actually made the edge converge — rather than
        /// how much repair traffic there is. These tests drive the REAL
        /// `handle_interest_sync_message` and hold the attempt count fixed
        /// while varying only what the peer reports back, so a detector that
        /// counted attempts (or scored every outcome as failure) cannot pass
        /// them.
        mod futile_repair_shadow {
            use super::*;
            use crate::ring::futile_repair::QUARANTINE_THRESHOLD;

            /// Feed one full-bytes `Summaries` report from `peer` and return
            /// the heals it produced.
            async fn report_summary(
                h: &mut Harness,
                peer: SocketAddr,
                summary: &[u8],
            ) -> Vec<(ContractKey, SocketAddr)> {
                let hash = contract_hash(&h.key);
                handle_interest_sync_message(
                    &h.op_manager,
                    peer,
                    InterestMessage::Summaries {
                        emitter: crate::message::SummariesEmitter::Other,
                        entries: vec![SummaryEntry {
                            hash,
                            summary_bytes: Some(summary.to_vec()),
                        }],
                    },
                )
                .await;
                h.drain_heals()
            }

            /// THE test for this feature. Two peers, the SAME number of
            /// repairs emitted to each. The only difference is what the next
            /// summary exchange said: one peer stays diverged forever (the
            /// non-commutative-merge signature), the other converges after
            /// every heal.
            ///
            /// A detector that counted repair ATTEMPTS would score these two
            /// edges identically and report `would_quarantine == 2` with
            /// `productive == 0`. That is exactly the documented mutation:
            /// replacing `!is_stale` with `false` at the `Summaries` arm's
            /// `record_repair_outcome` call makes this test fail on both
            /// counts.
            #[tokio::test]
            async fn a_stuck_edge_is_separated_from_a_converging_one() {
                let ours = vec![5u8; 128];
                let theirs = vec![6u8; 128];
                let mut h = build_harness("futile-shadow", 17100, ours.clone()).await;
                let (stuck_peer, healthy_peer, key) = (h.new_peer, h.old_peer, h.key);

                // STUCK edge: the peer reports a divergent summary every
                // round and never moves — no heal can land.
                //
                // One extra round because the first exchange has no
                // outstanding attempt to settle: round 1 only emits the first
                // heal, rounds 2..=N+1 each settle the previous one as futile.
                for round in 0..=QUARANTINE_THRESHOLD {
                    let heals = report_summary(&mut h, stuck_peer, &theirs).await;
                    assert_eq!(
                        heals.len(),
                        1,
                        "round {round}: shadow mode must not suppress the heal \
                         — every diverged round still emits exactly one \
                         targeted SyncStateToPeer"
                    );
                    assert_eq!(heals[0], (key, stuck_peer), "the heal must stay targeted");
                }

                // CONVERGING edge: same number of heals emitted, but each one
                // lands, so the following round reports OUR summary back.
                for _ in 0..QUARANTINE_THRESHOLD {
                    let heals = report_summary(&mut h, healthy_peer, &theirs).await;
                    assert_eq!(heals.len(), 1, "a diverged round emits one heal");
                    let healed = report_summary(&mut h, healthy_peer, &ours).await;
                    assert!(healed.is_empty(), "a converged round must not emit a heal");
                }

                let snap = h.op_manager.interest_manager.futile_repair_snapshot();
                assert_eq!(
                    snap.attempts,
                    u64::from(QUARANTINE_THRESHOLD) * 2 + 1,
                    "both edges were healed the same number of times \
                     (the stuck edge has one extra opening round)"
                );
                assert_eq!(
                    snap.futile,
                    u64::from(QUARANTINE_THRESHOLD),
                    "only the stuck edge's repairs left the summaries differing"
                );
                assert_eq!(
                    snap.productive,
                    u64::from(QUARANTINE_THRESHOLD),
                    "every repair to the converging edge landed — a detector \
                     that ignored the comparison verdict would report zero here"
                );
                assert_eq!(
                    snap.would_quarantine, 1,
                    "exactly ONE edge reached the shadow threshold; a detector \
                     counting attempts rather than outcomes would report two"
                );
                assert_eq!(
                    snap.edges_at_threshold, 1,
                    "the converging edge must never be at the threshold"
                );
                assert_eq!(
                    snap.evictions, 0,
                    "two edges cannot overflow the LRU — a non-zero eviction \
                     count here would mean the counts above are unreliable"
                );
            }

            /// A repair that lands resets the streak, so an edge that recovers
            /// one round before the threshold never crosses it. This is what
            /// stops a busy-but-healthy contract from being flagged.
            #[tokio::test]
            async fn a_late_recovery_clears_the_streak() {
                let ours = vec![9u8; 64];
                let theirs = vec![8u8; 64];
                let mut h = build_harness("futile-recover", 17110, ours.clone()).await;
                let peer = h.new_peer;

                // Diverge for one round short of the threshold...
                for _ in 0..QUARANTINE_THRESHOLD {
                    report_summary(&mut h, peer, &theirs).await;
                }
                let snap = h.op_manager.interest_manager.futile_repair_snapshot();
                assert_eq!(snap.futile, u64::from(QUARANTINE_THRESHOLD) - 1);
                assert_eq!(snap.would_quarantine, 0, "not yet at the threshold");

                // ...then the heal finally lands.
                report_summary(&mut h, peer, &ours).await;
                let snap = h.op_manager.interest_manager.futile_repair_snapshot();
                assert_eq!(snap.productive, 1);
                assert_eq!(
                    snap.would_quarantine, 0,
                    "one landed repair clears the streak"
                );
                assert_eq!(snap.edges_at_threshold, 0);
            }

            /// A peer that agrees from the outset produces no attempts and no
            /// futility — the detector must not manufacture a signal out of
            /// ordinary anti-entropy traffic. Covers BOTH wire forms, since
            /// the `SummaryDigests` agreement arm is a second observation site
            /// that could drift from the `Summaries` one.
            #[tokio::test]
            async fn an_agreeing_peer_produces_no_futility_on_either_wire_form() {
                let ours = vec![3u8; 32];
                let mut h = build_harness("futile-agree", 17120, ours.clone()).await;
                let hash = contract_hash(&h.key);
                let peer = h.new_peer;

                for _ in 0..QUARANTINE_THRESHOLD {
                    assert!(report_summary(&mut h, peer, &ours).await.is_empty());
                    handle_interest_sync_message(
                        &h.op_manager,
                        peer,
                        InterestMessage::SummaryDigests {
                            emitter: crate::message::SummariesEmitter::Other,
                            entries: vec![SummaryDigestEntry {
                                hash,
                                summary_digest: Some(summary_digest(&ours)),
                            }],
                        },
                    )
                    .await;
                    assert!(h.drain_heals().is_empty());
                }

                let snap = h.op_manager.interest_manager.futile_repair_snapshot();
                assert_eq!(
                    snap.attempts, 0,
                    "nothing was healed, so nothing was attempted"
                );
                assert_eq!(snap.futile, 0);
                assert_eq!(snap.would_quarantine, 0);
                assert_eq!(
                    snap.observations_unpaired,
                    u64::from(QUARANTINE_THRESHOLD) * 2,
                    "both wire forms must reach the detector as observations — \
                     a zero here on either arm means that observation site is \
                     not wired at all"
                );
            }

            /// HIGH-2, the load-correlated false-positive channel, end to end.
            ///
            /// `MAX_STALENESS_PROBES_PER_SUMMARIES` caps semantic-staleness
            /// probes at 32 per `Summaries` message. Past that,
            /// `summary_indicates_stale_peer` takes the conservative
            /// bytes-differ-means-stale DEFAULT — correct as a heal decision,
            /// but it means everything after position 32 reads as stale every
            /// round with no divergence at all. If that fed the detector, the
            /// headline number would grow with how many contracts a peer
            /// reports, i.e. with peer breadth and node load, rather than with
            /// brokenness.
            ///
            /// This drives exactly that shape through the real handler: a
            /// filler contract burns the whole probe budget, and the contract
            /// we track is always the entry past the budget. Its bytes differ
            /// every round, but the contract's own delta is EMPTY — it is
            /// logically converged (the #4857 non-deterministic-summary shape)
            /// and would be scored converged if it were ever probed. Nothing
            /// here is broken.
            ///
            /// Mutation that must fail this test: pass
            /// `OutcomeEvidence::Verdict` unconditionally at the `Summaries`
            /// outcome site — i.e. the pre-fix code, which passed `!is_stale`
            /// alone. The tracked contract then accrues a futile streak of
            /// `QUARANTINE_THRESHOLD` from load alone and reports
            /// `would_quarantine == 1`.
            #[tokio::test]
            async fn probe_budget_exhaustion_is_not_counted_as_futility() {
                let ours = vec![5u8; 128];
                let mut h =
                    build_harness_with("futile-budget", 17130, ours.clone(), DeltaBehavior::Empty)
                        .await;
                let peer = h.new_peer;

                // A second hosted contract, purely to burn the probe budget.
                // The one from the harness (`h.key`) is the one we track.
                let filler = ContractKey::from_id_and_code(
                    ContractInstanceId::new([77u8; 32]),
                    CodeHash::new([78u8; 32]),
                );
                let _ = h.op_manager.ring.host_contract(
                    filler,
                    128,
                    crate::ring::AccessType::Put,
                    crate::ring::HostingCause::Other,
                );
                h.op_manager
                    .interest_manager
                    .register_local_hosting(&filler);

                let filler_hash = contract_hash(&filler);
                let tracked_hash = contract_hash(&h.key);
                // Novel bytes every entry and every round, so every lookup is a
                // genuine cache MISS — a cache hit costs no budget and would
                // answer with a real verdict, defeating the setup.
                let mut nonce = 0u16;
                let mut novel = |len: usize| {
                    nonce += 1;
                    let mut bytes = vec![0u8; len];
                    bytes[0] = (nonce & 0xff) as u8;
                    bytes[1] = (nonce >> 8) as u8;
                    bytes
                };

                // One extra round for the same reason as the stuck-edge test:
                // the opening round has no outstanding attempt to settle.
                for _ in 0..=QUARANTINE_THRESHOLD {
                    let mut entries: Vec<SummaryEntry> = (0..MAX_STALENESS_PROBES_PER_SUMMARIES)
                        .map(|_| SummaryEntry {
                            hash: filler_hash,
                            summary_bytes: Some(novel(64)),
                        })
                        .collect();
                    // The budget is now spent, so THIS entry is defaulted.
                    entries.push(SummaryEntry {
                        hash: tracked_hash,
                        summary_bytes: Some(novel(64)),
                    });
                    handle_interest_sync_message(
                        &h.op_manager,
                        peer,
                        InterestMessage::Summaries {
                            emitter: crate::message::SummariesEmitter::Other,
                            entries,
                        },
                    )
                    .await;
                    // SHADOW MODE: the defaulted verdict still heals, exactly
                    // as before. Only the accounting changes.
                    let heals = h.drain_heals();
                    assert_eq!(
                        heals,
                        vec![(h.key, peer)],
                        "the conservative default must still emit its heal — \
                         this change must not alter behaviour"
                    );
                }

                // The harm first, so a regression names it rather than naming
                // a bookkeeping row.
                let snap = h.op_manager.interest_manager.futile_repair_snapshot();
                assert_eq!(
                    snap.would_quarantine, 0,
                    "a contract that is logically CONVERGED reached the shadow \
                     threshold purely because the peer reported more contracts \
                     than the probe budget covers — the headline is now a load \
                     metric: {snap:?}"
                );
                assert_eq!(
                    snap.futile, 0,
                    "running out of probe budget is not evidence that a repair \
                     failed — counting it makes the headline grow with peer \
                     breadth and node load instead of with brokenness"
                );
                assert_eq!(
                    snap.outcomes_probe_budget_exhausted,
                    u64::from(QUARANTINE_THRESHOLD) + 1,
                    "every round's over-budget entry must be recorded as a \
                     defaulted verdict, one per message"
                );
                assert_eq!(snap.edges_at_threshold, 0);
                assert!(
                    snap.attempts >= u64::from(QUARANTINE_THRESHOLD),
                    "the heals were emitted and charged as attempts; only \
                     their OUTCOMES are unclassifiable, got {snap:?}"
                );
                // The readable signature of the "we cannot tell" regime, and
                // the one an evasion attempt would produce (see the module
                // docs' second known limitation): heals keep going out and
                // keep being superseded unsettled, while `futile` and
                // `productive` both stay flat. A defaulted outcome must NOT
                // consume the attempt, so the count rises.
                assert!(
                    snap.attempts_superseded >= u64::from(QUARANTINE_THRESHOLD) - 1,
                    "an unclassifiable outcome must leave the attempt \
                     outstanding, so the next heal supersedes it — that pairing \
                     of high `attempts_superseded` with flat futile/productive \
                     is how the regime is recognised in the field: {snap:?}"
                );
            }

            /// The other evidence-free provenance: the probe RAN and produced
            /// no verdict (delta error / timeout). `summary_indicates_stale_peer`
            /// falls back to the byte compare and reports stale, so without the
            /// evidence split a contract-side or runtime-side fault would
            /// manufacture a full futility streak on a healthy edge.
            ///
            /// Mutation that must fail this test: as above, pass
            /// `OutcomeEvidence::Verdict` unconditionally — `would_quarantine`
            /// goes to 1 and `futile` to `QUARANTINE_THRESHOLD`.
            #[tokio::test]
            async fn a_failed_delta_probe_is_not_counted_as_futility() {
                let ours = vec![5u8; 128];
                let mut h = build_harness_with(
                    "futile-probe-fail",
                    17140,
                    ours.clone(),
                    DeltaBehavior::Failing,
                )
                .await;
                let peer = h.new_peer;

                let mut rounds = 0u64;
                for round in 0..=QUARANTINE_THRESHOLD {
                    // Novel bytes each round: a repeated pair would be served
                    // from the delta cache rather than re-probed.
                    let theirs = vec![(round as u8).wrapping_add(1); 64];
                    let heals = report_summary(&mut h, peer, &theirs).await;
                    assert_eq!(
                        heals.len(),
                        1,
                        "a failed probe still falls back to the byte compare \
                         and still heals — behaviour is unchanged"
                    );
                    rounds += 1;
                }

                // The harm first, so a regression names it rather than naming
                // a bookkeeping row.
                let snap = h.op_manager.interest_manager.futile_repair_snapshot();
                assert_eq!(
                    snap.would_quarantine, 0,
                    "a contract-side or runtime-side probe fault manufactured a \
                     quarantine candidate on a healthy edge: {snap:?}"
                );
                assert_eq!(
                    snap.futile, 0,
                    "a probe that produced no verdict is not evidence that a \
                     repair failed"
                );
                assert_eq!(
                    snap.outcomes_probe_unavailable, rounds,
                    "each round's unanswerable probe must be recorded as its \
                     own class, separate from budget exhaustion"
                );
                assert_eq!(
                    snap.attempts, rounds,
                    "every round still emitted (and charged) its heal"
                );
            }
        }
    }

    /// The pure digest classifier — the one comparison the whole exchange rests
    /// on. Every arm is asserted, because each maps to a different wire
    /// outcome and getting any of them backwards is silent.
    #[test]
    fn classify_summary_digest_covers_every_case() {
        use freenet_stdlib::prelude::StateSummary;

        let ours = StateSummary::from(vec![1u8, 2, 3]);
        let matching = crate::ring::interest::summary_digest(&[1u8, 2, 3]);
        let differing = crate::ring::interest::summary_digest(&[9u8, 9, 9]);

        assert_eq!(
            classify_summary_digest(Some(&ours), Some(&matching)),
            DigestVerdict::Agree,
            "equal digests mean the peer holds our summary — the 98.1% case"
        );
        assert_eq!(
            classify_summary_digest(Some(&ours), Some(&differing)),
            DigestVerdict::NeedBytes,
            "differing digests must fetch the bytes so the semantic staleness \
             probe (and the heal) can run on real data"
        );
        assert_eq!(
            classify_summary_digest(None, Some(&matching)),
            DigestVerdict::NeedBytes,
            "we hold no summary but the peer does: we still need their bytes \
             to seed the peer-summary cache (#4952), or they stay a full-state \
             broadcast target forever"
        );
        assert_eq!(
            classify_summary_digest(Some(&ours), None),
            DigestVerdict::PeerHasNoState,
            "a peer with no state is not a divergence"
        );
        assert_eq!(
            classify_summary_digest(None, None),
            DigestVerdict::PeerHasNoState,
            "neither side holds state: nothing to exchange, nothing to heal"
        );
    }
}
