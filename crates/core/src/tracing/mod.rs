use std::{path::PathBuf, sync::Arc, time::SystemTime};

use chrono::{DateTime, Utc};
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{
    config::GlobalExecutor,
    generated::ContractChange,
    message::{MessageStats, NetMessage, NetMessageV1, Transaction},
    node::PeerId,
    operations::{
        connect,
        get::{GetMsg, GetMsgResult},
        put::PutMsg,
        subscribe::{SubscribeMsg, SubscribeMsgResult},
        update::UpdateMsg,
    },
    ring::{Location, PeerKeyLocation, Ring},
    router::RouteEvent,
};

#[cfg(feature = "trace-ot")]
pub(crate) use opentelemetry_tracer::OTEventRegister;
pub(crate) use test::TestEventListener;

// Re-export for use in tests
pub use event_aggregator::{
    AOFEventSource, EventLogAggregator, EventSource, RoutingPath, TransactionFlowEvent,
    WebSocketEventCollector,
};

use crate::node::OpManager;

/// An append-only log for network events.
mod aof;

/// Event aggregation across multiple nodes for debugging and testing.
pub mod event_aggregator;

/// Telemetry reporting to central collector.
pub mod telemetry;
pub use telemetry::TelemetryReporter;

/// Compute a full hash of contract state for convergence verification.
/// Returns all 32 bytes of Blake3 hash as 64 hex characters.
///
/// This provides cryptographically strong verification that states are identical.
/// With 256 bits, collision is computationally infeasible.
pub fn state_hash_full(state: &WrappedState) -> String {
    let hash = blake3::hash(state.as_ref());
    hash.to_hex().to_string()
}

/// Compute a short hash of contract state for telemetry display.
/// Returns first 4 bytes of Blake3 hash as 8 hex characters.
///
/// This is designed for quick visual comparison in logs and telemetry dashboards,
/// not for verification. Use `state_hash_full` for convergence checking.
pub fn state_hash_short(state: &WrappedState) -> String {
    let hash = blake3::hash(state.as_ref());
    let bytes = hash.as_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct ListenerLogId(usize);

/// A type that reacts to incoming messages from the network and records information about them.
pub(crate) trait NetEventRegister: std::any::Any + Send + Sync + 'static {
    fn register_events<'a>(
        &'a self,
        events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()>;
    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<'_, ()>;
    fn trait_clone(&self) -> Box<dyn NetEventRegister>;
    fn get_router_events(&self, number: usize) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>>;
}

#[cfg(feature = "trace-ot")]
pub(crate) struct CombinedRegister<const N: usize>([Box<dyn NetEventRegister>; N]);

#[cfg(feature = "trace-ot")]
impl<const N: usize> CombinedRegister<N> {
    pub fn new(registries: [Box<dyn NetEventRegister>; N]) -> Self {
        Self(registries)
    }
}

#[cfg(feature = "trace-ot")]
impl<const N: usize> NetEventRegister for CombinedRegister<N> {
    fn register_events<'a>(
        &'a self,
        events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async move {
            for registry in &self.0 {
                registry.register_events(events.clone()).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<'_, ()> {
        async move {
            for reg in &mut self.0 {
                reg.notify_of_time_out(tx).await;
            }
        }
        .boxed()
    }

    fn get_router_events(&self, number: usize) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
        async move {
            for reg in &self.0 {
                let events = reg.get_router_events(number).await?;
                if !events.is_empty() {
                    return Ok(events);
                }
            }
            Ok(vec![])
        }
        .boxed()
    }
}

#[cfg(feature = "trace-ot")]
impl<const N: usize> Clone for CombinedRegister<N> {
    fn clone(&self) -> Self {
        let mut i = 0;
        let cloned: [Box<dyn NetEventRegister>; N] = [None::<()>; N].map(|_| {
            let cloned = self.0[i].trait_clone();
            i += 1;
            cloned
        });
        Self(cloned)
    }
}

/// A dynamic register that can hold multiple event registers.
/// Unlike CombinedRegister, this uses a Vec so the number of registers
/// can be determined at runtime.
pub(crate) struct DynamicRegister(Vec<Box<dyn NetEventRegister>>);

impl DynamicRegister {
    pub(crate) fn new(registries: Vec<Box<dyn NetEventRegister>>) -> Self {
        Self(registries)
    }
}

impl NetEventRegister for DynamicRegister {
    fn register_events<'a>(
        &'a self,
        events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async move {
            for registry in &self.0 {
                registry.register_events(events.clone()).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<'_, ()> {
        async move {
            for reg in &mut self.0 {
                reg.notify_of_time_out(tx).await;
            }
        }
        .boxed()
    }

    fn get_router_events(&self, number: usize) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
        async move {
            // Aggregate events from all registers up to the requested limit
            let mut collected = Vec::new();
            for reg in &self.0 {
                if collected.len() >= number {
                    break;
                }
                let remaining = number - collected.len();
                let events = reg.get_router_events(remaining).await?;
                collected.extend(events);
            }
            Ok(collected)
        }
        .boxed()
    }
}

impl Clone for DynamicRegister {
    fn clone(&self) -> Self {
        Self(self.0.iter().map(|r| r.trait_clone()).collect())
    }
}

#[derive(Clone)]
pub(crate) struct NetEventLog<'a> {
    tx: &'a Transaction,
    peer_id: PeerId,
    kind: EventKind,
}

impl<'a> NetEventLog<'a> {
    /// Safely get the peer_id from the ring's connection manager.
    /// Returns None if the peer doesn't have a known address (e.g., during startup).
    /// Telemetry should never panic - we just skip events if we can't identify ourselves.
    fn get_own_peer_id(ring: &Ring) -> Option<PeerId> {
        let own_loc = ring.connection_manager.own_location();
        own_loc
            .socket_addr()
            .map(|addr| PeerId::new(addr, own_loc.pub_key().clone()))
    }

    pub fn route_event(
        tx: &'a Transaction,
        ring: &'a Ring,
        route_event: &RouteEvent,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Route(route_event.clone()),
        })
    }

    pub fn connected(ring: &'a Ring, peer: PeerId, _location: Location) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let connected = PeerKeyLocation::new(peer.pub_key.clone(), peer.addr);
        let connection_count = ring.connection_manager.connection_count();
        let is_gateway = ring.connection_manager.is_gateway();
        // Note: location is computed from address, so we don't need to set it separately
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::Connected {
                this: ring.connection_manager.own_location(),
                connected,
                // None since we don't have transaction timing for this path
                elapsed_ms: None,
                connection_type: if is_gateway {
                    ConnectionType::Gateway
                } else {
                    ConnectionType::Direct
                },
                latency_ms: None, // RTT not available at this layer
                this_peer_connection_count: connection_count,
                initiated_by: None, // Not available in this context
            }),
        })
    }

    /// Create a disconnected event with full context.
    ///
    /// # Arguments
    /// * `ring` - The ring containing connection state
    /// * `from` - The peer that was disconnected
    /// * `reason` - Structured reason for disconnection
    /// * `connection_duration_ms` - How long the connection was open
    /// * `bytes_sent` - Bytes sent to the peer during connection
    /// * `bytes_received` - Bytes received from the peer during connection
    pub fn disconnected_with_context(
        ring: &'a Ring,
        from: &'a PeerId,
        reason: DisconnectReason,
        connection_duration_ms: Option<u64>,
        bytes_sent: Option<u64>,
        bytes_received: Option<u64>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Disconnected {
                from: from.clone(),
                reason,
                connection_duration_ms,
                bytes_sent,
                bytes_received,
            },
        })
    }

    /// Create a disconnected event with minimal context (backwards compatible).
    /// Note: Prefer `disconnected_with_context` with explicit `DisconnectReason` variants.
    #[allow(dead_code)] // Kept as simpler API for external callers
    pub fn disconnected(ring: &'a Ring, from: &'a PeerId, reason: Option<String>) -> Option<Self> {
        Self::disconnected_with_context(ring, from, reason.into(), None, None, None)
    }

    /// Create a Put failure event.
    pub fn put_failure(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        reason: OperationFailure,
        hop_count: Option<usize>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Put(PutEvent::PutFailure {
                id: *tx,
                requester: own_loc.clone(),
                target: own_loc,
                key,
                hop_count,
                reason,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a Get failure event.
    pub fn get_failure(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        reason: OperationFailure,
        hop_count: Option<usize>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Get(GetEvent::GetFailure {
                id: *tx,
                requester: own_loc.clone(),
                instance_id,
                target: own_loc,
                hop_count,
                reason,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a ConnectRequest sent event.
    pub fn connect_request_sent(
        tx: &'a Transaction,
        ring: &'a Ring,
        desired_location: Location,
        joiner: PeerKeyLocation,
        target: PeerKeyLocation,
        ttl: u8,
        is_initial: bool,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::RequestSent {
                desired_location,
                joiner,
                target,
                ttl,
                is_initial,
            }),
        })
    }

    /// Create a ConnectRequest received event.
    ///
    /// `from_addr` is the socket address of the upstream peer that sent the request.
    /// We look up the full PeerKeyLocation from the connection manager if available.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_request_received(
        tx: &'a Transaction,
        ring: &'a Ring,
        desired_location: Location,
        joiner: PeerKeyLocation,
        from_addr: std::net::SocketAddr,
        forwarded_to: Option<PeerKeyLocation>,
        accepted: bool,
        ttl: u8,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        // Look up the full peer info from connection manager if the peer is already connected
        let from_peer = ring.connection_manager.get_peer_by_addr(from_addr);
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::RequestReceived {
                desired_location,
                joiner,
                from_addr,
                from_peer,
                forwarded_to,
                accepted,
                ttl,
            }),
        })
    }

    /// Create a ConnectResponse sent event.
    pub fn connect_response_sent(
        tx: &'a Transaction,
        ring: &'a Ring,
        acceptor: PeerKeyLocation,
        joiner: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::ResponseSent { acceptor, joiner }),
        })
    }

    /// Create a ConnectResponse received event.
    pub fn connect_response_received(
        tx: &'a Transaction,
        ring: &'a Ring,
        acceptor: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::ResponseReceived {
                acceptor,
                elapsed_ms: tx.elapsed().as_millis() as u64,
            }),
        })
    }

    // ==================== PUT Operation Helpers ====================

    /// Create a Put request event.
    pub fn put_request(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        target: PeerKeyLocation,
        htl: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Put(PutEvent::Request {
                id: *tx,
                requester: own_loc,
                key,
                target,
                htl,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a Put success event.
    pub fn put_success(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        target: PeerKeyLocation,
        hop_count: Option<usize>,
        state_hash: Option<String>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: *tx,
                requester: own_loc,
                target,
                key,
                hop_count,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash,
            }),
        })
    }

    /// Create a Put broadcast emitted event.
    /// Note: PUT operations don't currently use broadcasting (Update handles that),
    /// but this helper exists for API completeness.
    #[allow(dead_code)]
    pub fn put_broadcast_emitted(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        value: WrappedState,
        broadcast_to: Vec<PeerKeyLocation>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        let broadcasted_to = broadcast_to.len();
        let state_hash = Some(state_hash_short(&value));
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Put(PutEvent::BroadcastEmitted {
                id: *tx,
                upstream: own_loc.clone(),
                broadcast_to,
                broadcasted_to,
                key,
                value,
                sender: own_loc,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash,
            }),
        })
    }

    /// Create a Put broadcast received event.
    /// Note: PUT operations don't currently use broadcasting (Update handles that),
    /// but this helper exists for API completeness.
    #[allow(dead_code)]
    pub fn put_broadcast_received(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        requester: PeerKeyLocation,
        value: WrappedState,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        let state_hash = Some(state_hash_short(&value));
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: *tx,
                key,
                requester,
                value,
                target: own_loc,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash,
            }),
        })
    }

    // ==================== GET Operation Helpers ====================

    /// Create a Get request event.
    pub fn get_request(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        htl: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Get(GetEvent::Request {
                id: *tx,
                requester: own_loc,
                instance_id,
                target,
                htl,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a Get success event.
    pub fn get_success(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        target: PeerKeyLocation,
        hop_count: Option<usize>,
        state_hash: Option<String>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Get(GetEvent::GetSuccess {
                id: *tx,
                requester: own_loc,
                target,
                key,
                hop_count,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash,
            }),
        })
    }

    /// Create a Get not found event.
    pub fn get_not_found(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        hop_count: Option<usize>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Get(GetEvent::GetNotFound {
                id: *tx,
                requester: own_loc.clone(),
                instance_id,
                target: own_loc,
                hop_count,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    // ==================== SUBSCRIBE Operation Helpers ====================

    /// Create a Subscribe request event.
    pub fn subscribe_request(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        htl: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::Request {
                id: *tx,
                requester: own_loc,
                instance_id,
                target,
                htl,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a Subscribe success event.
    pub fn subscribe_success(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        at: PeerKeyLocation,
        hop_count: Option<usize>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::SubscribeSuccess {
                id: *tx,
                key,
                at,
                hop_count,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
                requester: own_loc,
            }),
        })
    }

    /// Create a Subscribe not found event.
    pub fn subscribe_not_found(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        hop_count: Option<usize>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::SubscribeNotFound {
                id: *tx,
                requester: own_loc.clone(),
                instance_id,
                target: own_loc,
                hop_count,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    // ==================== UPDATE Operation Helpers ====================

    /// Create an Update request event.
    pub fn update_request(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        target: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::Request {
                id: *tx,
                requester: own_loc,
                key,
                target,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create an Update success event.
    pub fn update_success(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        target: PeerKeyLocation,
        state_hash_before: Option<String>,
        state_hash_after: Option<String>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::UpdateSuccess {
                id: *tx,
                requester: own_loc,
                target,
                key,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash_before,
                state_hash_after,
            }),
        })
    }

    /// Create an Update broadcast emitted event.
    pub fn update_broadcast_emitted(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        value: WrappedState,
        broadcast_to: Vec<PeerKeyLocation>,
        upstream: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        let broadcasted_to = broadcast_to.len();
        let state_hash = Some(state_hash_short(&value));
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::BroadcastEmitted {
                id: *tx,
                upstream,
                broadcast_to,
                broadcasted_to,
                key,
                value,
                sender: own_loc,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash,
            }),
        })
    }

    /// Create an Update broadcast received event.
    pub fn update_broadcast_received(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        requester: PeerKeyLocation,
        value: WrappedState,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        let state_hash = Some(state_hash_short(&value));
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::BroadcastReceived {
                id: *tx,
                key,
                requester,
                value,
                target: own_loc,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash,
            }),
        })
    }

    /// Create an Update broadcast applied event.
    ///
    /// This captures the state after applying a broadcast update locally,
    /// enabling state convergence monitoring across the network.
    pub fn update_broadcast_applied(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        state_before: &WrappedState,
        state_after: &WrappedState,
        changed: bool,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: *tx,
                key,
                target: own_loc,
                timestamp: chrono::Utc::now().timestamp() as u64,
                state_hash_before: Some(state_hash_short(state_before)),
                state_hash_after: Some(state_hash_short(state_after)),
                changed,
            }),
        })
    }

    /// Create a peer startup event.
    ///
    /// This should be called once when the node starts and is ready to participate in the network.
    /// The event captures version info, platform details, and gateway status.
    pub fn peer_startup(
        ring: &'a Ring,
        version: String,
        git_commit: Option<String>,
        git_dirty: Option<bool>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let is_gateway = ring.connection_manager.is_gateway();

        // Get OS version info
        let os_version = Self::get_os_version();

        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Lifecycle(PeerLifecycleEvent::Startup {
                version,
                git_commit,
                git_dirty,
                arch: std::env::consts::ARCH.to_string(),
                os: std::env::consts::OS.to_string(),
                os_version,
                is_gateway,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a peer shutdown event.
    ///
    /// This should be called when the node is shutting down, either gracefully or due to an error.
    pub fn peer_shutdown(
        ring: &'a Ring,
        graceful: bool,
        reason: Option<String>,
        start_time: tokio::time::Instant,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let uptime_secs = start_time.elapsed().as_secs();
        // Get current connection count at shutdown time
        let total_connections = ring.connection_manager.connection_count() as u64;

        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Lifecycle(PeerLifecycleEvent::Shutdown {
                graceful,
                reason,
                uptime_secs,
                total_connections,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Get OS version information.
    /// Returns None if version detection fails.
    fn get_os_version() -> Option<String> {
        #[cfg(target_os = "linux")]
        {
            // Try to read /etc/os-release
            if let Ok(contents) = std::fs::read_to_string("/etc/os-release") {
                for line in contents.lines() {
                    if line.starts_with("PRETTY_NAME=") {
                        return Some(
                            line.trim_start_matches("PRETTY_NAME=")
                                .trim_matches('"')
                                .to_string(),
                        );
                    }
                }
            }
            None
        }

        #[cfg(target_os = "macos")]
        {
            // Use sw_vers to get macOS version
            std::process::Command::new("sw_vers")
                .arg("-productVersion")
                .output()
                .ok()
                .and_then(|output| {
                    if output.status.success() {
                        String::from_utf8(output.stdout)
                            .ok()
                            .map(|v| format!("macOS {}", v.trim()))
                    } else {
                        None
                    }
                })
        }

        #[cfg(target_os = "windows")]
        {
            // Use ver command or registry
            std::process::Command::new("cmd")
                .args(["/C", "ver"])
                .output()
                .ok()
                .and_then(|output| {
                    if output.status.success() {
                        String::from_utf8(output.stdout)
                            .ok()
                            .map(|v| v.trim().to_string())
                    } else {
                        None
                    }
                })
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            None
        }
    }

    // ==================== Seeding/Subscription Events ====================

    /// Create a seeding_started event when a local client subscribes to a contract.
    pub fn seeding_started(ring: &'a Ring, instance_id: ContractInstanceId) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::SeedingStarted {
                instance_id,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create a seeding_stopped event when the last local client unsubscribes from a contract.
    #[allow(dead_code)] // Helper available for future use
    pub fn seeding_stopped(
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        reason: SeedingStoppedReason,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::SeedingStopped {
                instance_id,
                reason,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create a downstream_added event when a peer subscribes through us.
    pub fn downstream_added(
        ring: &'a Ring,
        key: ContractKey,
        subscriber: PeerKeyLocation,
        downstream_count: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::DownstreamAdded {
                key,
                subscriber,
                downstream_count,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create a downstream_removed event when a downstream subscriber is removed.
    #[allow(dead_code)] // Helper available for future use
    pub fn downstream_removed(
        ring: &'a Ring,
        key: ContractKey,
        subscriber: Option<PeerKeyLocation>,
        reason: DownstreamRemovedReason,
        downstream_count: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::DownstreamRemoved {
                key,
                subscriber,
                reason,
                downstream_count,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create an upstream_set event when we subscribe through another peer.
    pub fn upstream_set(
        ring: &'a Ring,
        key: ContractKey,
        upstream: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::UpstreamSet {
                key,
                upstream,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create an unsubscribed event when we unsubscribe from a contract's upstream.
    #[allow(dead_code)] // Helper available for future use
    pub fn unsubscribed(
        ring: &'a Ring,
        key: ContractKey,
        reason: UnsubscribedReason,
        upstream: Option<PeerKeyLocation>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::Unsubscribed {
                key,
                reason,
                upstream,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create a subscription_state snapshot event.
    pub fn subscription_state(
        ring: &'a Ring,
        key: ContractKey,
        is_seeding: bool,
        upstream: Option<PeerKeyLocation>,
        downstream: Vec<PeerKeyLocation>,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let downstream_count = downstream.len();
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::SubscriptionState {
                key,
                is_seeding,
                upstream,
                downstream_count,
                downstream,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    pub fn from_outbound_msg(
        msg: &'a NetMessage,
        ring: &'a Ring,
        target_addr: Option<std::net::SocketAddr>,
    ) -> Either<Self, Vec<Self>> {
        let own_loc = ring.connection_manager.own_location();
        let Some(own_addr) = own_loc.socket_addr() else {
            return Either::Right(vec![]);
        };
        let peer_id = PeerId::new(own_addr, own_loc.pub_key().clone());
        let connection_count = ring.connection_manager.connection_count();
        let is_gateway = ring.connection_manager.is_gateway();
        let kind = match msg {
            NetMessage::V1(NetMessageV1::Connect(connect::ConnectMsg::Response {
                payload,
                ..
            })) => {
                // With hop-by-hop routing, we (the joiner) are the target.
                // The acceptor is in the payload.
                let this_peer = ring.connection_manager.own_location();
                EventKind::Connect(ConnectEvent::Connected {
                    this: this_peer,
                    connected: payload.acceptor.clone(),
                    elapsed_ms: Some(msg.id().elapsed().as_millis() as u64),
                    connection_type: if is_gateway {
                        ConnectionType::Gateway
                    } else {
                        ConnectionType::Direct
                    },
                    latency_ms: None, // RTT not available in message path
                    this_peer_connection_count: connection_count,
                    initiated_by: Some(peer_id.clone()), // We (the joiner) initiated
                })
            }
            NetMessage::V1(NetMessageV1::Put(PutMsg::Response { id, key })) => {
                // Track outbound Put response for message routing visibility
                let to = target_addr
                    .and_then(|addr| ring.connection_manager.get_peer_by_addr(addr))
                    .unwrap_or_else(|| own_loc.clone()); // Fallback to own location if target unknown
                EventKind::Put(PutEvent::ResponseSent {
                    id: *id,
                    from: own_loc.clone(),
                    to,
                    key: *key,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessage::V1(NetMessageV1::Get(GetMsg::Response { id, .. })) => {
                // Track outbound Get response for message routing visibility
                // Key may not be available for NotFound responses, but include if present
                let key = match msg {
                    NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
                        result: GetMsgResult::Found { key, .. },
                        ..
                    })) => Some(*key),
                    _ => None,
                };
                let to = target_addr
                    .and_then(|addr| ring.connection_manager.get_peer_by_addr(addr))
                    .unwrap_or_else(|| own_loc.clone()); // Fallback to own location if target unknown
                EventKind::Get(GetEvent::ResponseSent {
                    id: *id,
                    from: own_loc.clone(),
                    to,
                    key,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response { id, .. })) => {
                // Track outbound Subscribe response for message routing visibility
                let key = match msg {
                    NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
                        result: SubscribeMsgResult::Subscribed { key },
                        ..
                    })) => Some(*key),
                    _ => None,
                };
                let to = target_addr
                    .and_then(|addr| ring.connection_manager.get_peer_by_addr(addr))
                    .unwrap_or_else(|| own_loc.clone()); // Fallback to own location if target unknown
                EventKind::Subscribe(SubscribeEvent::ResponseSent {
                    id: *id,
                    from: own_loc.clone(),
                    to,
                    key,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            _ => EventKind::Ignored,
        };
        Either::Left(NetEventLog {
            tx: msg.id(),
            peer_id,
            kind,
        })
    }

    pub fn from_inbound_msg_v1(
        msg: &'a NetMessageV1,
        op_manager: &'a OpManager,
    ) -> Either<Self, Vec<Self>> {
        let connection_count = op_manager.ring.connection_manager.connection_count();
        let is_gateway = op_manager.ring.connection_manager.is_gateway();
        let kind = match msg {
            NetMessageV1::Connect(connect::ConnectMsg::Response { payload, .. }) => {
                let acceptor = payload.acceptor.clone();
                // With hop-by-hop routing, the target (joiner) is determined from op_manager
                let this_peer = op_manager.ring.connection_manager.own_location();
                // Skip event if addresses are unknown
                let (Some(acceptor_addr), Some(this_addr)) =
                    (acceptor.socket_addr(), this_peer.socket_addr())
                else {
                    return Either::Right(vec![]);
                };
                let acceptor_peer_id = PeerId::new(acceptor_addr, acceptor.pub_key().clone());
                let this_peer_id = PeerId::new(this_addr, this_peer.pub_key().clone());
                let elapsed_ms = Some(msg.id().elapsed().as_millis() as u64);
                // The joiner (this_peer) initiated the connection
                let initiated_by = Some(this_peer_id.clone());
                let connection_type = if is_gateway {
                    ConnectionType::Gateway
                } else {
                    ConnectionType::Direct
                };
                let events = vec![
                    NetEventLog {
                        tx: msg.id(),
                        peer_id: acceptor_peer_id.clone(),
                        kind: EventKind::Connect(ConnectEvent::Connected {
                            this: acceptor.clone(),
                            connected: this_peer.clone(),
                            elapsed_ms,
                            connection_type,
                            latency_ms: None,
                            this_peer_connection_count: connection_count,
                            initiated_by: initiated_by.clone(),
                        }),
                    },
                    NetEventLog {
                        tx: msg.id(),
                        peer_id: this_peer_id,
                        kind: EventKind::Connect(ConnectEvent::Connected {
                            this: this_peer,
                            connected: acceptor,
                            elapsed_ms,
                            connection_type,
                            latency_ms: None,
                            this_peer_connection_count: connection_count,
                            initiated_by,
                        }),
                    },
                ];
                return Either::Right(events);
            }
            NetMessageV1::Put(PutMsg::Request {
                contract, id, htl, ..
            }) => {
                let this_peer = &op_manager.ring.connection_manager.own_location();
                let key = contract.key();
                EventKind::Put(PutEvent::Request {
                    requester: this_peer.clone(),
                    target: this_peer.clone(), // No embedded target - use own location
                    key,
                    id: *id,
                    htl: *htl,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessageV1::Put(PutMsg::Response { id, key }) => {
                let this_peer = &op_manager.ring.connection_manager.own_location();
                // Calculate hop_count from operation state: max_htl - current_htl
                let hop_count = op_manager.get_current_hop(id).map(|current_htl| {
                    op_manager.ring.max_hops_to_live.saturating_sub(current_htl)
                });
                EventKind::Put(PutEvent::PutSuccess {
                    id: *id,
                    requester: this_peer.clone(),
                    target: this_peer.clone(),
                    key: *key,
                    hop_count,
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                })
            }
            NetMessageV1::Get(GetMsg::Request {
                id,
                instance_id,
                htl,
                ..
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Get(GetEvent::Request {
                    id: *id,
                    requester: this_peer.clone(),
                    instance_id: *instance_id,
                    target: this_peer,
                    htl: *htl,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessageV1::Get(GetMsg::Response {
                id,
                result: GetMsgResult::Found { key, value },
                ..
            }) if value.state.is_some() => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // Calculate hop_count from operation state: max_htl - current_hop
                let hop_count = op_manager.get_current_hop(id).map(|current_hop| {
                    op_manager.ring.max_hops_to_live.saturating_sub(current_hop)
                });
                EventKind::Get(GetEvent::GetSuccess {
                    id: *id,
                    requester: this_peer.clone(),
                    target: this_peer,
                    key: *key,
                    hop_count,
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                })
            }
            NetMessageV1::Get(GetMsg::Response {
                id,
                instance_id,
                result: GetMsgResult::NotFound,
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // Calculate hop_count from operation state: max_htl - current_hop
                let hop_count = op_manager.get_current_hop(id).map(|current_hop| {
                    op_manager.ring.max_hops_to_live.saturating_sub(current_hop)
                });
                EventKind::Get(GetEvent::GetNotFound {
                    id: *id,
                    requester: this_peer.clone(),
                    instance_id: *instance_id,
                    target: this_peer,
                    hop_count,
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessageV1::Subscribe(SubscribeMsg::Request {
                id,
                instance_id,
                htl,
                ..
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Subscribe(SubscribeEvent::Request {
                    id: *id,
                    requester: this_peer.clone(),
                    instance_id: *instance_id,
                    target: this_peer,
                    htl: *htl,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessageV1::Subscribe(SubscribeMsg::Response {
                id,
                result: SubscribeMsgResult::Subscribed { key },
                ..
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Subscribe(SubscribeEvent::SubscribeSuccess {
                    id: *id,
                    key: *key,
                    at: this_peer.clone(),
                    hop_count: None, // TODO: Track hop count from operation state
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    requester: this_peer,
                })
            }
            NetMessageV1::Subscribe(SubscribeMsg::Response {
                id,
                instance_id,
                result: SubscribeMsgResult::NotFound,
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Subscribe(SubscribeEvent::SubscribeNotFound {
                    id: *id,
                    requester: this_peer.clone(),
                    instance_id: *instance_id,
                    target: this_peer,
                    hop_count: None, // TODO: Track hop count from operation state
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessageV1::Update(UpdateMsg::RequestUpdate { key, id, .. }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Update(UpdateEvent::Request {
                    requester: this_peer.clone(),
                    target: this_peer, // With hop-by-hop routing, we are the target
                    key: *key,
                    id: *id,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessageV1::Update(UpdateMsg::Broadcasting {
                new_value,
                broadcast_to,
                broadcasted_to,
                key,
                id,
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Update(UpdateEvent::BroadcastEmitted {
                    id: *id,
                    upstream: this_peer.clone(), // We are the broadcaster
                    broadcast_to: broadcast_to.clone(),
                    broadcasted_to: *broadcasted_to,
                    key: *key,
                    value: new_value.clone(),
                    sender: this_peer, // We are the sender
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                })
            }
            NetMessageV1::Update(UpdateMsg::BroadcastTo { new_value, key, id }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                EventKind::Update(UpdateEvent::BroadcastReceived {
                    id: *id,
                    requester: this_peer.clone(),
                    key: *key,
                    value: new_value.clone(),
                    target: this_peer, // We are the target
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                })
            }
            _ => EventKind::Ignored,
        };
        let own_loc = op_manager.ring.connection_manager.own_location();
        let Some(own_addr) = own_loc.socket_addr() else {
            return Either::Right(vec![]);
        };
        let peer_id = PeerId::new(own_addr, own_loc.pub_key().clone());
        Either::Left(NetEventLog {
            tx: msg.id(),
            peer_id,
            kind,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct NetLogMessage {
    pub tx: Transaction,
    pub datetime: DateTime<Utc>,
    pub peer_id: PeerId,
    pub kind: EventKind,
}

impl NetLogMessage {
    fn to_log_message<'a>(
        log: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> impl Iterator<Item = NetLogMessage> + Send + 'a {
        let erased_iter = match log {
            Either::Left(one) => Box::new([one].into_iter())
                as Box<dyn std::iter::Iterator<Item = NetEventLog<'_>> + Send + 'a>,
            Either::Right(multiple) => Box::new(multiple.into_iter())
                as Box<dyn std::iter::Iterator<Item = NetEventLog<'_>> + Send + 'a>,
        };
        erased_iter.into_iter().map(NetLogMessage::from)
    }

    /// Signals whether this message closes a transaction span.
    ///
    /// In case of isolated events where the span is not being tracked it should return true.
    #[cfg(feature = "trace-ot")]
    fn span_completed(&self) -> bool {
        match &self.kind {
            EventKind::Connect(ConnectEvent::Finished { .. }) => true,
            EventKind::Connect(_) => false,
            EventKind::Put(PutEvent::PutSuccess { .. }) => true,
            EventKind::Put(_) => false,
            EventKind::Get(GetEvent::GetSuccess { .. } | GetEvent::GetNotFound { .. }) => true,
            EventKind::Get(_) => false,
            EventKind::Subscribe(
                SubscribeEvent::SubscribeSuccess { .. } | SubscribeEvent::SubscribeNotFound { .. },
            ) => true,
            EventKind::Subscribe(_) => false,
            _ => false,
        }
    }
}

impl<'a> From<NetEventLog<'a>> for NetLogMessage {
    fn from(log: NetEventLog<'a>) -> NetLogMessage {
        NetLogMessage {
            datetime: Utc::now(),
            tx: *log.tx,
            kind: log.kind,
            peer_id: log.peer_id.clone(),
        }
    }
}

#[cfg(feature = "trace-ot")]
impl<'a> From<&'a NetLogMessage> for Option<Vec<opentelemetry::KeyValue>> {
    fn from(msg: &'a NetLogMessage) -> Self {
        use opentelemetry::KeyValue;
        let map: Option<Vec<KeyValue>> = match &msg.kind {
            EventKind::Connect(ConnectEvent::StartConnection { from, .. }) => Some(vec![
                KeyValue::new("phase", "start"),
                KeyValue::new("initiator", format!("{from}")),
            ]),
            EventKind::Connect(ConnectEvent::Connected {
                this,
                connected,
                elapsed_ms,
                connection_type,
                latency_ms,
                this_peer_connection_count,
                initiated_by,
            }) => {
                let mut attrs = vec![
                    KeyValue::new("phase", "connected"),
                    KeyValue::new("from", format!("{this}")),
                    KeyValue::new("to", format!("{connected}")),
                    KeyValue::new("connection_type", format!("{connection_type:?}")),
                    KeyValue::new("connection_count", *this_peer_connection_count as i64),
                ];
                if let Some(ms) = elapsed_ms {
                    attrs.push(KeyValue::new("elapsed_ms", *ms as i64));
                }
                if let Some(ms) = latency_ms {
                    attrs.push(KeyValue::new("latency_ms", *ms as i64));
                }
                if let Some(initiator) = initiated_by {
                    attrs.push(KeyValue::new("initiated_by", format!("{initiator}")));
                }
                Some(attrs)
            }
            EventKind::Connect(ConnectEvent::Finished {
                initiator,
                location,
                elapsed_ms,
            }) => {
                let mut attrs = vec![
                    KeyValue::new("phase", "finished"),
                    KeyValue::new("initiator", format!("{initiator}")),
                    KeyValue::new("location", location.as_f64()),
                ];
                if let Some(ms) = elapsed_ms {
                    attrs.push(KeyValue::new("elapsed_ms", *ms as i64));
                }
                Some(attrs)
            }
            _ => None,
        };
        map.map(|mut map| {
            map.push(KeyValue::new("peer_id", format!("{}", msg.peer_id)));
            map
        })
    }
}

// Internal message type for the event logger
#[allow(clippy::large_enum_variant)]
enum EventLogCommand {
    Log(NetLogMessage),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Handle for flushing an EventRegister (can be stored separately for testing)
#[derive(Clone)]
pub struct EventFlushHandle {
    sender: mpsc::Sender<EventLogCommand>,
}

impl EventFlushHandle {
    /// Request a flush and wait for completion
    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.sender.send(EventLogCommand::Flush(tx)).await.is_ok() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
        }
    }
}

pub(crate) struct EventRegister {
    log_file: Arc<PathBuf>,
    log_sender: mpsc::Sender<EventLogCommand>,
    // Track the number of clones to know when to flush
    clone_count: Arc<std::sync::atomic::AtomicUsize>,
    // Handle for external flushing
    flush_handle: EventFlushHandle,
}

/// Records from a new session must have higher than this ts.
static NEW_RECORDS_TS: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();

const DEFAULT_METRICS_SERVER_PORT: u16 = 55010;

impl EventRegister {
    pub fn new(event_log_path: PathBuf) -> Self {
        let (log_sender, log_recv) = mpsc::channel(1000);
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let log_file = Arc::new(event_log_path.clone());
        GlobalExecutor::spawn(Self::record_logs(log_recv, log_file.clone()));

        let flush_handle = EventFlushHandle {
            sender: log_sender.clone(),
        };

        Self {
            log_sender,
            log_file: Arc::new(event_log_path),
            clone_count: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            flush_handle,
        }
    }

    /// Get a handle for flushing this EventRegister (for testing)
    pub fn flush_handle(&self) -> EventFlushHandle {
        self.flush_handle.clone()
    }

    async fn record_logs(
        mut log_recv: mpsc::Receiver<EventLogCommand>,
        event_log_path: Arc<PathBuf>,
    ) {
        use futures::StreamExt;

        tokio::time::sleep(std::time::Duration::from_millis(200)).await; // wait for the node to start
        let mut event_log = match aof::LogFile::open(event_log_path.as_path()).await {
            Ok(file) => file,
            Err(err) => {
                tracing::error!("Failed openning log file {:?} with: {err}", event_log_path);
                panic!("Failed openning log file"); // fixme: propagate this to the main event loop
            }
        };

        let mut ws = connect_to_metrics_server().await;

        loop {
            let ws_recv = if let Some(ws) = &mut ws {
                ws.next().boxed()
            } else {
                futures::future::pending().boxed()
            };
            // Note: This uses tokio::select! because the borrow of `ws` in ws_recv
            // conflicts with ws.as_mut() in branch bodies when using deterministic_select!
            tokio::select! {
                cmd = log_recv.recv() => {
                    let Some(cmd) = cmd else { break; };
                    match cmd {
                        EventLogCommand::Log(log) => {
                            if let Some(ws) = ws.as_mut() {
                                send_to_metrics_server(ws, &log).await;
                            }
                            event_log.persist_log(log).await;
                        }
                        EventLogCommand::Flush(reply) => {
                            // Flush any remaining events in the batch
                            Self::flush_batch(&mut event_log).await;
                            // Signal completion
                            let _ = reply.send(());
                        }
                    }
                }
                ws_msg = ws_recv => {
                    if let Some((ws, ws_msg)) = ws.as_mut().zip(ws_msg) {
                        received_from_metrics_server(ws, ws_msg).await;
                    }
                }
            }
        }

        // store remaining logs on channel close
        Self::flush_batch(&mut event_log).await;
    }

    async fn flush_batch(event_log: &mut aof::LogFile) {
        let moved_batch = std::mem::replace(&mut event_log.batch, aof::Batch::new(aof::BATCH_SIZE));
        let batch_writes = moved_batch.num_writes;
        if batch_writes == 0 {
            return;
        }
        match aof::LogFile::encode_batch(&moved_batch) {
            Ok(batch_serialized_data) => {
                if !batch_serialized_data.is_empty()
                    && event_log.write_all(&batch_serialized_data).await.is_err()
                {
                    tracing::error!("Failed writing remaining event log batch");
                }
                event_log.update_recs(batch_writes);
            }
            Err(err) => {
                tracing::error!("Failed encode batch: {err}");
            }
        }
    }
}

impl Clone for EventRegister {
    fn clone(&self) -> Self {
        // Increment the reference count when cloning
        self.clone_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            log_file: self.log_file.clone(),
            log_sender: self.log_sender.clone(),
            clone_count: self.clone_count.clone(),
            flush_handle: self.flush_handle.clone(),
        }
    }
}

impl Drop for EventRegister {
    fn drop(&mut self) {
        // Decrement the reference count
        let prev_count = self
            .clone_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        // If this was the last instance (count was 1, now 0), the sender will be dropped
        // and the channel will close, triggering the flush in record_logs
        if prev_count == 1 {
            tracing::debug!(
                "Last EventRegister instance dropped, channel will close and trigger flush"
            );
        }
    }
}

impl NetEventRegister for EventRegister {
    fn register_events<'a>(
        &'a self,
        logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async {
            for log_msg in NetLogMessage::to_log_message(logs) {
                let _ = self.log_sender.send(EventLogCommand::Log(log_msg)).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<'_, ()> {
        let log_msg = NetLogMessage {
            tx,
            datetime: Utc::now(),
            peer_id: PeerId::random(), // Timeout events don't have a specific peer context
            kind: EventKind::Timeout {
                id: tx,
                timestamp: chrono::Utc::now().timestamp() as u64,
            },
        };
        let sender = self.log_sender.clone();
        async move {
            let _ = sender.send(EventLogCommand::Log(log_msg)).await;
        }
        .boxed()
    }

    fn get_router_events(&self, number: usize) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
        async move { aof::LogFile::get_router_events(number, &self.log_file).await }.boxed()
    }
}

async fn connect_to_metrics_server() -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let port = std::env::var("FDEV_NETWORK_METRICS_SERVER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_METRICS_SERVER_PORT);

    tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/v1/push-stats/"))
        .await
        .map(|(ws_stream, _)| {
            tracing::info!("Connected to network metrics server");
            ws_stream
        })
        .ok()
}

async fn send_to_metrics_server(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    send_msg: &NetLogMessage,
) {
    use crate::generated::PeerChange;
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let res = match &send_msg.kind {
        EventKind::Connect(ConnectEvent::Connected {
            this: this_peer,
            connected: connected_peer,
            ..
        }) => {
            // Both peers must have known locations to send to metrics server
            if let (Some(from_loc), Some(to_loc)) =
                (this_peer.location(), connected_peer.location())
            {
                let this_id = PeerId::new(
                    this_peer
                        .socket_addr()
                        .expect("this peer should have address"),
                    this_peer.pub_key().clone(),
                );
                let connected_id = PeerId::new(
                    connected_peer
                        .socket_addr()
                        .expect("connected peer should have address"),
                    connected_peer.pub_key().clone(),
                );
                let msg = PeerChange::added_connection_msg(
                    (&send_msg.tx != Transaction::NULL).then(|| send_msg.tx.to_string()),
                    (this_id.to_string(), from_loc.as_f64()),
                    (connected_id.to_string(), to_loc.as_f64()),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Disconnected { from, .. } => {
            let msg = PeerChange::removed_connection_msg(
                from.clone().to_string(),
                send_msg.peer_id.clone().to_string(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Put(PutEvent::Request {
            requester,
            key,
            target,
            timestamp,
            ..
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target_addr, target.pub_key().clone());
                let msg = ContractChange::put_request_msg(
                    send_msg.tx.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Put(PutEvent::PutSuccess {
            requester,
            target,
            key,
            timestamp,
            ..
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target_addr, target.pub_key().clone());
                let msg = ContractChange::put_success_msg(
                    send_msg.tx.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Put(PutEvent::BroadcastEmitted {
            id,
            upstream,
            broadcast_to, // broadcast_to n peers
            broadcasted_to,
            key,
            sender,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_emitted_msg(
                id.to_string(),
                upstream.to_string(),
                broadcast_to.iter().map(|p| p.to_string()).collect(),
                *broadcasted_to,
                key.to_string(),
                sender.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Put(PutEvent::BroadcastReceived {
            id,
            target,
            requester,
            key,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_received_msg(
                id.to_string(),
                requester.to_string(),
                target.to_string(),
                key.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Get(GetEvent::GetSuccess {
            id,
            key,
            timestamp,
            requester,
            target,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::get_contract_msg(
                requester.to_string(),
                target.to_string(),
                id.to_string(),
                key.to_string(),
                contract_location.as_f64(),
                *timestamp,
            );

            ws_stream.send(Message::Binary(msg.into())).await
        }
        // GetEvent::Request and GetEvent::GetNotFound fall through to catch-all
        // TODO(#2456): Add FlatBuffer messages for GetEvent::Request and GetEvent::GetNotFound
        // when metrics server is enhanced to support these event types.
        EventKind::Subscribe(SubscribeEvent::SubscribeSuccess {
            id,
            key,
            at,
            timestamp,
            requester,
            ..
        }) => {
            if let (Some(at_addr), Some(at_loc)) = (at.socket_addr(), at.location()) {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let at_id = PeerId::new(at_addr, at.pub_key().clone());
                let msg = ContractChange::subscribed_msg(
                    requester.to_string(),
                    id.to_string(),
                    key.to_string(),
                    contract_location.as_f64(),
                    at_id.to_string(),
                    at_loc.as_f64(),
                    *timestamp,
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        // SubscribeEvent::Request and SubscribeEvent::SubscribeNotFound fall through to catch-all
        // TODO(#2456): Add FlatBuffer messages for SubscribeEvent::Request and SubscribeEvent::SubscribeNotFound
        // when metrics server is enhanced to support these event types.
        EventKind::Update(UpdateEvent::Request {
            id,
            requester,
            key,
            target,
            timestamp,
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target_addr, target.pub_key().clone());
                let msg = ContractChange::update_request_msg(
                    id.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Update(UpdateEvent::UpdateSuccess {
            id,
            requester,
            target,
            key,
            timestamp,
            ..
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target_addr, target.pub_key().clone());
                let msg = ContractChange::update_success_msg(
                    id.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Update(UpdateEvent::BroadcastEmitted {
            id,
            upstream,
            broadcast_to, // broadcast_to n peers
            broadcasted_to,
            key,
            sender,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_emitted_msg(
                id.to_string(),
                upstream.to_string(),
                broadcast_to.iter().map(|p| p.to_string()).collect(),
                *broadcasted_to,
                key.to_string(),
                sender.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Update(UpdateEvent::BroadcastReceived {
            id,
            target,
            requester,
            key,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_received_msg(
                id.to_string(),
                target.to_string(),
                requester.to_string(),
                key.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        _ => Ok(()),
    };
    if let Err(error) = res {
        tracing::warn!(%error, "Error while sending message to network metrics server");
    }
}

async fn received_from_metrics_server(
    ws_stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    msg: tokio_tungstenite::tungstenite::Result<tokio_tungstenite::tungstenite::Message>,
) {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    match msg {
        Ok(Message::Ping(ping)) => {
            let _ = ws_stream.send(Message::Pong(ping)).await;
        }
        Ok(Message::Close(_)) => {
            if let Err(error) = ws_stream.send(Message::Close(None)).await {
                tracing::warn!(%error, "Error while closing websocket with network metrics server");
            }
        }
        _ => {}
    }
}

#[cfg(feature = "trace-ot")]
mod opentelemetry_tracer {
    #[cfg(not(test))]
    use std::collections::HashMap;
    use std::time::Duration;

    use dashmap::DashMap;
    use opentelemetry::{
        global,
        trace::{self, Span},
        KeyValue,
    };

    use super::*;

    struct OTSpan {
        inner: global::BoxedSpan,
        last_log: SystemTime,
    }

    impl OTSpan {
        fn new(transaction: Transaction) -> Self {
            use trace::Tracer;

            let tracer = global::tracer("freenet");
            let tx_bytes = transaction.as_bytes();
            let mut span_id = [0; 8];
            span_id.copy_from_slice(&tx_bytes[8..]);
            let start_time = transaction.started();
            let inner = tracer.build(trace::SpanBuilder {
                name: transaction.transaction_type().description().into(),
                start_time: Some(start_time),
                span_id: Some(trace::SpanId::from_bytes(span_id)),
                trace_id: Some(trace::TraceId::from_bytes(tx_bytes)),
                attributes: Some(vec![
                    KeyValue::new("transaction", transaction.to_string()),
                    KeyValue::new("tx_type", transaction.transaction_type().description()),
                ]),
                ..Default::default()
            });
            OTSpan {
                inner,
                last_log: SystemTime::now(),
            }
        }

        fn add_log(&mut self, log: &NetLogMessage) {
            // NOTE: if we need to add some standard attributes in the future take a look at
            // https://docs.rs/opentelemetry-semantic-conventions/latest/opentelemetry_semantic_conventions/
            let ts = SystemTime::UNIX_EPOCH
                + Duration::from_nanos(
                    ((log.datetime.timestamp() * 1_000_000_000)
                        + log.datetime.timestamp_subsec_nanos() as i64) as u64,
                );
            self.last_log = ts;
            if let Some(log_vals) = <Option<Vec<_>>>::from(log) {
                self.inner.add_event_with_timestamp(
                    log.tx.transaction_type().description(),
                    ts,
                    log_vals,
                );
            }
        }
    }

    impl Drop for OTSpan {
        fn drop(&mut self) {
            self.inner.end_with_timestamp(self.last_log);
        }
    }

    impl trace::Span for OTSpan {
        delegate::delegate! {
            to self.inner {
                fn span_context(&self) -> &trace::SpanContext;
                fn is_recording(&self) -> bool;
                fn set_attribute(&mut self, attribute: opentelemetry::KeyValue);
                fn set_status(&mut self, status: trace::Status);
                fn end_with_timestamp(&mut self, timestamp: SystemTime);
            }
        }

        fn add_event_with_timestamp<T>(
            &mut self,
            _: T,
            _: SystemTime,
            _: Vec<opentelemetry::KeyValue>,
        ) where
            T: Into<std::borrow::Cow<'static, str>>,
        {
            unreachable!("add_event_with_timestamp is not explicitly called on OTSpan")
        }

        fn update_name<T>(&mut self, _: T)
        where
            T: Into<std::borrow::Cow<'static, str>>,
        {
            unreachable!("update_name shouldn't be called on OTSpan as span name is fixed")
        }

        fn add_link(&mut self, span_context: trace::SpanContext, attributes: Vec<KeyValue>) {
            self.inner.add_link(span_context, attributes);
        }
    }

    #[derive(Clone)]
    pub(crate) struct OTEventRegister {
        log_sender: mpsc::Sender<NetLogMessage>,
        finished_tx_notifier: mpsc::Sender<Transaction>,
    }

    /// For tests running in a single process is important that span tracking is global across threads and simulated peers.
    static UNIQUE_REGISTER: std::sync::OnceLock<DashMap<Transaction, OTSpan>> =
        std::sync::OnceLock::new();

    impl OTEventRegister {
        pub fn new() -> Self {
            if cfg!(test) {
                UNIQUE_REGISTER.get_or_init(DashMap::new);
            }
            let (sender, finished_tx_notifier) = mpsc::channel(100);
            let (log_sender, log_recv) = mpsc::channel(1000);
            NEW_RECORDS_TS.get_or_init(SystemTime::now);
            GlobalExecutor::spawn(Self::record_logs(log_recv, finished_tx_notifier));
            Self {
                log_sender,
                finished_tx_notifier: sender,
            }
        }

        async fn record_logs(
            mut log_recv: mpsc::Receiver<NetLogMessage>,
            mut finished_tx_notifier: mpsc::Receiver<Transaction>,
        ) {
            #[cfg(not(test))]
            let mut logs = HashMap::new();

            #[cfg(not(test))]
            fn process_log(logs: &mut HashMap<Transaction, OTSpan>, log: NetLogMessage) {
                let span_completed = log.span_completed();
                match logs.entry(log.tx) {
                    std::collections::hash_map::Entry::Occupied(mut val) => {
                        {
                            let span = val.get_mut();
                            span.add_log(&log);
                        }
                        if span_completed {
                            let (_, _span) = val.remove_entry();
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(empty) => {
                        let span = empty.insert(OTSpan::new(log.tx));
                        // does not make much sense to treat a single isolated event as a span,
                        // so just ignore those in case they were to happen
                        if !span_completed {
                            span.add_log(&log);
                        }
                    }
                }
            }

            #[cfg(test)]
            fn process_log(logs: &DashMap<Transaction, OTSpan>, log: NetLogMessage) {
                let span_completed = log.span_completed();
                match logs.entry(log.tx) {
                    dashmap::mapref::entry::Entry::Occupied(mut val) => {
                        {
                            let span = val.get_mut();
                            span.add_log(&log);
                        }
                        if span_completed {
                            let (_, _span) = val.remove_entry();
                        }
                    }
                    dashmap::mapref::entry::Entry::Vacant(empty) => {
                        let mut span = empty.insert(OTSpan::new(log.tx));
                        // does not make much sense to treat a single isolated event as a span,
                        // so just ignore those in case they were to happen
                        if !span_completed {
                            span.add_log(&log);
                        }
                    }
                }
            }

            #[cfg(not(test))]
            fn cleanup_timed_out(logs: &mut HashMap<Transaction, OTSpan>, tx: Transaction) {
                if let Some(_span) = logs.remove(&tx) {}
            }

            #[cfg(test)]
            fn cleanup_timed_out(logs: &DashMap<Transaction, OTSpan>, tx: Transaction) {
                if let Some((_, _span)) = logs.remove(&tx) {}
            }

            loop {
                crate::deterministic_select! {
                    log_msg = log_recv.recv() => {
                        if let Some(log) = log_msg {
                            #[cfg(not(test))]
                            {
                                process_log(&mut logs, log);
                            }
                            #[cfg(test)]
                            {
                                process_log(UNIQUE_REGISTER.get().expect("should be set"), log);
                            }
                        } else {
                            break;
                        }
                    },
                    finished_tx = finished_tx_notifier.recv() => {
                        if let Some(tx) = finished_tx {
                            #[cfg(not(test))]
                            {
                                cleanup_timed_out(&mut logs, tx);
                            }
                            #[cfg(test)]
                            {
                                cleanup_timed_out(UNIQUE_REGISTER.get().expect("should be set"), tx);
                            }
                        } else {
                            break;
                        }
                    },
                }
            }
        }
    }

    impl NetEventRegister for OTEventRegister {
        fn register_events<'a>(
            &'a self,
            logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> BoxFuture<'a, ()> {
            async {
                for log_msg in NetLogMessage::to_log_message(logs) {
                    let _ = self.log_sender.send(log_msg).await;
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<'_, ()> {
            async move {
                if cfg!(test) {
                    let _ = self.finished_tx_notifier.send(tx).await;
                }
            }
            .boxed()
        }

        fn get_router_events(
            &self,
            _number: usize,
        ) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
            async { Ok(vec![]) }.boxed()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[non_exhaustive]
#[allow(private_interfaces)]
// todo: make this take by ref instead, probably will need an owned version
pub enum EventKind {
    Connect(ConnectEvent),
    Put(PutEvent),
    Get(GetEvent),
    Subscribe(SubscribeEvent),
    Route(RouteEvent),
    Update(UpdateEvent),
    /// Data transfer events for stream-level transfers.
    Transfer(TransferEvent),
    /// Peer lifecycle events (startup, shutdown).
    Lifecycle(PeerLifecycleEvent),
    Ignored,
    Disconnected {
        from: PeerId,
        /// Structured reason for disconnection.
        reason: DisconnectReason,
        /// How long the connection was open before disconnecting (milliseconds).
        /// None if duration tracking is unavailable.
        connection_duration_ms: Option<u64>,
        /// Bytes sent to this peer during the connection.
        /// None if byte tracking is unavailable.
        bytes_sent: Option<u64>,
        /// Bytes received from this peer during the connection.
        /// None if byte tracking is unavailable.
        bytes_received: Option<u64>,
    },
    /// A transaction timed out without completing.
    Timeout {
        /// The transaction that timed out.
        id: Transaction,
        timestamp: u64,
    },
    /// Periodic transport layer metrics snapshot.
    ///
    /// Emitted every N seconds (default 30s) with aggregate transport statistics.
    /// This is more efficient than per-transfer events and provides trend data.
    TransportSnapshot(crate::transport::TransportSnapshot),
}

impl EventKind {
    const CONNECT: u8 = 0;
    const PUT: u8 = 1;
    const GET: u8 = 2;
    const ROUTE: u8 = 3;
    const SUBSCRIBE: u8 = 4;
    const IGNORED: u8 = 5;
    const DISCONNECTED: u8 = 6;
    const UPDATE: u8 = 7;
    const TIMEOUT: u8 = 8;
    const TRANSFER: u8 = 9;
    const LIFECYCLE: u8 = 10;
    const TRANSPORT_SNAPSHOT: u8 = 11;

    const fn varint_id(&self) -> u8 {
        match self {
            EventKind::Connect(_) => Self::CONNECT,
            EventKind::Put(_) => Self::PUT,
            EventKind::Get(_) => Self::GET,
            EventKind::Route(_) => Self::ROUTE,
            EventKind::Subscribe(_) => Self::SUBSCRIBE,
            EventKind::Ignored => Self::IGNORED,
            EventKind::Disconnected { .. } => Self::DISCONNECTED,
            EventKind::Update(_) => Self::UPDATE,
            EventKind::Timeout { .. } => Self::TIMEOUT,
            EventKind::Transfer(_) => Self::TRANSFER,
            EventKind::Lifecycle(_) => Self::LIFECYCLE,
            EventKind::TransportSnapshot(_) => Self::TRANSPORT_SNAPSHOT,
        }
    }

    /// Extracts the contract key from this event, if applicable.
    ///
    /// Returns the key for Put, Get, Subscribe, and Update events.
    pub fn contract_key(&self) -> Option<freenet_stdlib::prelude::ContractKey> {
        match self {
            EventKind::Put(put) => Some(put.contract_key()),
            EventKind::Get(get) => get.contract_key(),
            EventKind::Subscribe(sub) => sub.contract_key(),
            EventKind::Update(upd) => Some(upd.contract_key()),
            _ => None,
        }
    }

    /// Extracts the state hash from this event, if applicable.
    ///
    /// Returns the hash for Put and Update success/broadcast events.
    pub fn state_hash(&self) -> Option<&str> {
        match self {
            EventKind::Put(put) => put.state_hash(),
            EventKind::Update(upd) => upd.state_hash(),
            _ => None,
        }
    }

    /// Returns the variant name of this event kind.
    pub fn variant_name(&self) -> &'static str {
        match self {
            EventKind::Connect(_) => "Connect",
            EventKind::Put(_) => "Put",
            EventKind::Get(_) => "Get",
            EventKind::Subscribe(_) => "Subscribe",
            EventKind::Route(_) => "Route",
            EventKind::Update(_) => "Update",
            EventKind::Transfer(_) => "Transfer",
            EventKind::Lifecycle(_) => "Lifecycle",
            EventKind::Ignored => "Ignored",
            EventKind::Disconnected { .. } => "Disconnected",
            EventKind::Timeout { .. } => "Timeout",
            EventKind::TransportSnapshot(_) => "TransportSnapshot",
        }
    }
}

/// The type of connection between peers.
///
/// This helps understand network topology and debug connectivity issues.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ConnectionType {
    /// Direct UDP connection between peers.
    Direct,
    /// Connection relayed through a gateway or other peer.
    Relayed,
    /// Connection to/from a gateway node.
    Gateway,
    /// Unknown connection type (for backwards compatibility or when detection fails).
    #[default]
    Unknown,
}

/// Reason for a peer disconnection.
///
/// Understanding disconnect reasons is critical for debugging network stability.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum DisconnectReason {
    /// Connection was explicitly closed by local node.
    Explicit(String),
    /// Connection pruned due to topology optimization.
    Pruned,
    /// Connection timed out (no response within timeout period).
    Timeout,
    /// Network error (transport layer failure).
    NetworkError(String),
    /// Peer was unresponsive to keep-alive pings.
    Unresponsive,
    /// Connection dropped by the remote peer.
    RemoteDropped,
    /// Maximum connection limit reached.
    ConnectionLimitReached,
    /// Unknown reason (for backwards compatibility).
    #[default]
    Unknown,
}

/// **Deprecated**: This conversion uses fragile substring matching which can break
/// if error message formats change. Use DisconnectReason enum variants directly instead.
///
/// This impl exists only for backwards compatibility with the `disconnected()` helper.
/// New code should construct DisconnectReason variants directly.
impl From<Option<String>> for DisconnectReason {
    fn from(reason: Option<String>) -> Self {
        match reason {
            Some(s) if s.contains("pruned") => DisconnectReason::Pruned,
            Some(s) if s.contains("timeout") => DisconnectReason::Timeout,
            Some(s) if s.contains("unresponsive") => DisconnectReason::Unresponsive,
            Some(s) if s.contains("limit") => DisconnectReason::ConnectionLimitReached,
            Some(s) => DisconnectReason::Explicit(s),
            None => DisconnectReason::Unknown,
        }
    }
}

/// Connection lifecycle events.
///
/// Note on event format versioning: New variants are added at the end of enums
/// to maintain backwards compatibility with persisted AOF logs within a session.
/// AOF logs are session-scoped and cleared on restart, so cross-version
/// compatibility is not required.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[allow(clippy::large_enum_variant)] // Connected variant needs PeerKeyLocation for observability
enum ConnectEvent {
    /// Initial connection start (legacy event - see RequestSent/RequestReceived for detailed tracking).
    StartConnection {
        from: PeerId,
        /// Whether this is a connection to a gateway node.
        is_gateway: bool,
    },
    Connected {
        this: PeerKeyLocation,
        connected: PeerKeyLocation,
        /// Time elapsed since connection started (milliseconds).
        /// None when timing information is unavailable (e.g., connection events
        /// without transaction context).
        elapsed_ms: Option<u64>,
        /// Type of connection (direct, relayed, gateway).
        connection_type: ConnectionType,
        /// Smoothed RTT to the peer in milliseconds, if available.
        /// This is measured via the transport layer's RTT estimation.
        latency_ms: Option<u64>,
        /// Number of open connections this peer has after this connection.
        /// Helps identify if a peer is having widespread connectivity issues.
        this_peer_connection_count: usize,
        /// The peer that initiated the connection (joiner).
        /// Helps trace connection establishment patterns.
        initiated_by: Option<PeerId>,
    },
    /// Connection process finished (legacy event - see RequestSent/RequestReceived/ResponseReceived for detailed tracking).
    Finished {
        initiator: PeerId,
        location: Location,
        /// Time elapsed since connection started (milliseconds).
        elapsed_ms: Option<u64>,
    },

    // === Protocol Message Events ===
    // These track the actual ConnectRequest/Response messages flowing through the network,
    // enabling visualization of message routing paths in the dashboard.
    /// A ConnectRequest message was sent (by joiner or forwarded by relay).
    RequestSent {
        /// The target ring location this request is routing toward.
        desired_location: Location,
        /// The joiner's identity and location.
        joiner: PeerKeyLocation,
        /// The peer we're sending this request to.
        target: PeerKeyLocation,
        /// Remaining hops-to-live.
        ttl: u8,
        /// True if this is the initial send from the joiner (not a forward).
        is_initial: bool,
    },
    /// A ConnectRequest message was received.
    RequestReceived {
        /// The target ring location this request is routing toward.
        desired_location: Location,
        /// The joiner's identity and location.
        joiner: PeerKeyLocation,
        /// The socket address of the peer that sent us this request.
        from_addr: std::net::SocketAddr,
        /// The peer that sent us this request (if we have them in our connection table).
        /// None when receiving from a new joiner who isn't connected yet.
        from_peer: Option<PeerKeyLocation>,
        /// Where we're forwarding to (None if we're at terminus).
        forwarded_to: Option<PeerKeyLocation>,
        /// Whether we accepted this connection request.
        accepted: bool,
        /// Remaining hops-to-live when received.
        ttl: u8,
    },
    /// A ConnectResponse message was sent (acceptor sending back to joiner).
    ResponseSent {
        /// The acceptor (us) who is accepting the connection.
        acceptor: PeerKeyLocation,
        /// The joiner we're accepting.
        joiner: PeerKeyLocation,
    },
    /// A ConnectResponse message was received (joiner receiving from acceptor).
    ResponseReceived {
        /// The acceptor who accepted our connection request.
        acceptor: PeerKeyLocation,
        /// Time elapsed since we sent the original request (milliseconds).
        elapsed_ms: u64,
    },
}

/// Reason for operation failure.
///
/// Understanding failure reasons helps debug network and contract issues.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum OperationFailure {
    /// Connection to peer dropped during operation.
    ConnectionDropped,
    /// Operation exceeded maximum retries.
    MaxRetriesExceeded { retries: usize },
    /// HTL (hops to live) exhausted before finding result.
    HtlExhausted,
    /// No peers available in the ring to route to.
    NoPeersAvailable,
    /// Contract handler returned an error.
    ContractError(String),
    /// Operation timed out.
    Timeout,
    /// Other failure with description.
    Other(String),
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Direct => write!(f, "direct"),
            ConnectionType::Relayed => write!(f, "relayed"),
            ConnectionType::Gateway => write!(f, "gateway"),
            ConnectionType::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisconnectReason::Explicit(reason) => write!(f, "explicit: {}", reason),
            DisconnectReason::Pruned => write!(f, "pruned"),
            DisconnectReason::Timeout => write!(f, "timeout"),
            DisconnectReason::NetworkError(err) => write!(f, "network_error: {}", err),
            DisconnectReason::Unresponsive => write!(f, "unresponsive"),
            DisconnectReason::RemoteDropped => write!(f, "remote_dropped"),
            DisconnectReason::ConnectionLimitReached => write!(f, "connection_limit_reached"),
            DisconnectReason::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::fmt::Display for OperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationFailure::ConnectionDropped => write!(f, "connection_dropped"),
            OperationFailure::MaxRetriesExceeded { retries } => {
                write!(f, "max_retries_exceeded: {}", retries)
            }
            OperationFailure::HtlExhausted => write!(f, "htl_exhausted"),
            OperationFailure::NoPeersAvailable => write!(f, "no_peers_available"),
            OperationFailure::ContractError(err) => write!(f, "contract_error: {}", err),
            OperationFailure::Timeout => write!(f, "timeout"),
            OperationFailure::Other(reason) => write!(f, "other: {}", reason),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum PutEvent {
    Request {
        id: Transaction,
        requester: PeerKeyLocation,
        key: ContractKey,
        target: PeerKeyLocation,
        /// Hops to live - remaining hops before request fails.
        htl: usize,
        timestamp: u64,
    },
    PutSuccess {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        /// Number of hops the request traversed (initial HTL - remaining HTL).
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
        /// Short hash of the stored state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    /// Put operation failed.
    PutFailure {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        /// Number of hops the request traversed before failure.
        hop_count: Option<usize>,
        /// Reason for the failure.
        reason: OperationFailure,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Put response being sent back to the requester.
    ///
    /// Tracks when this peer sends a successful put response to an upstream peer.
    /// This provides sender-side visibility for debugging message routing.
    ResponseSent {
        id: Transaction,
        /// The peer sending the response.
        from: PeerKeyLocation,
        /// The peer receiving the response.
        to: PeerKeyLocation,
        key: ContractKey,
        timestamp: u64,
    },
    BroadcastEmitted {
        id: Transaction,
        upstream: PeerKeyLocation,
        /// subscribed peers
        broadcast_to: Vec<PeerKeyLocation>,
        broadcasted_to: usize,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
        sender: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the broadcast state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    BroadcastReceived {
        id: Transaction,
        /// peer who started the broadcast op
        requester: PeerKeyLocation,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
        /// target peer
        target: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the received state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
}

impl PutEvent {
    /// Returns the contract key for this event.
    fn contract_key(&self) -> ContractKey {
        match self {
            PutEvent::Request { key, .. }
            | PutEvent::PutSuccess { key, .. }
            | PutEvent::PutFailure { key, .. }
            | PutEvent::ResponseSent { key, .. }
            | PutEvent::BroadcastEmitted { key, .. }
            | PutEvent::BroadcastReceived { key, .. } => *key,
        }
    }

    /// Returns the state hash if available.
    fn state_hash(&self) -> Option<&str> {
        match self {
            PutEvent::PutSuccess { state_hash, .. }
            | PutEvent::BroadcastEmitted { state_hash, .. }
            | PutEvent::BroadcastReceived { state_hash, .. } => state_hash.as_deref(),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum UpdateEvent {
    Request {
        id: Transaction,
        requester: PeerKeyLocation,
        key: ContractKey,
        target: PeerKeyLocation,
        timestamp: u64,
    },
    UpdateSuccess {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        timestamp: u64,
        /// Short hash of state before update (first 4 bytes of Blake3, 8 hex chars).
        state_hash_before: Option<String>,
        /// Short hash of state after update (first 4 bytes of Blake3, 8 hex chars).
        state_hash_after: Option<String>,
    },
    BroadcastEmitted {
        id: Transaction,
        upstream: PeerKeyLocation,
        /// subscribed peers
        broadcast_to: Vec<PeerKeyLocation>,
        broadcasted_to: usize,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was updated
        value: WrappedState,
        sender: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the broadcast state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    BroadcastReceived {
        id: Transaction,
        /// peer who started the broadcast op
        requester: PeerKeyLocation,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was updated
        value: WrappedState,
        /// target peer
        target: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the received state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    /// Emitted after a broadcast update has been applied locally.
    /// This captures the resulting state after the delta/merge operation,
    /// enabling state convergence monitoring across the network.
    BroadcastApplied {
        id: Transaction,
        /// key of the contract which value was updated
        key: ContractKey,
        /// this peer (where the update was applied)
        target: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the incoming broadcast state (before merge).
        state_hash_before: Option<String>,
        /// Short hash of the resulting state (after merge).
        state_hash_after: Option<String>,
        /// Whether the local state actually changed after applying the update.
        changed: bool,
    },
}

impl UpdateEvent {
    /// Returns the contract key for this event.
    fn contract_key(&self) -> ContractKey {
        match self {
            UpdateEvent::Request { key, .. }
            | UpdateEvent::UpdateSuccess { key, .. }
            | UpdateEvent::BroadcastEmitted { key, .. }
            | UpdateEvent::BroadcastReceived { key, .. }
            | UpdateEvent::BroadcastApplied { key, .. } => *key,
        }
    }

    /// Returns the state hash if available (uses state_hash_after for success/applied events).
    fn state_hash(&self) -> Option<&str> {
        match self {
            UpdateEvent::UpdateSuccess {
                state_hash_after, ..
            }
            | UpdateEvent::BroadcastApplied {
                state_hash_after, ..
            } => state_hash_after.as_deref(),
            UpdateEvent::BroadcastEmitted { state_hash, .. }
            | UpdateEvent::BroadcastReceived { state_hash, .. } => state_hash.as_deref(),
            _ => None,
        }
    }
}

/// GET operation events for tracking the lifecycle of contract retrieval.
///
/// Similar to `PutEvent`, this enum captures the full sequence of a Get operation:
/// - Request initiation
/// - Success when contract is found
/// - NotFound when contract doesn't exist after search
/// - Failure when operation fails due to network/system errors
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum GetEvent {
    /// A Get request was initiated or received.
    Request {
        id: Transaction,
        /// The peer initiating or receiving the request.
        requester: PeerKeyLocation,
        /// Contract instance being requested (full key may not be known yet).
        instance_id: ContractInstanceId,
        /// Target peer (with hop-by-hop routing, this is the current node).
        target: PeerKeyLocation,
        /// Hops remaining before giving up.
        htl: usize,
        timestamp: u64,
    },
    /// Contract was successfully retrieved.
    GetSuccess {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        /// Full contract key (only available after successful retrieval).
        key: ContractKey,
        /// Number of hops the request traversed.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
        /// Short hash of the retrieved state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    /// Contract was not found after exhaustive search.
    GetNotFound {
        id: Transaction,
        requester: PeerKeyLocation,
        /// Contract instance that was searched for.
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        /// Number of hops the request traversed before exhaustion.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Get operation failed due to network or system error.
    GetFailure {
        id: Transaction,
        requester: PeerKeyLocation,
        /// Contract instance that was searched for.
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        /// Number of hops the request traversed before failure.
        hop_count: Option<usize>,
        /// Reason for the failure.
        reason: OperationFailure,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Get response being sent back to the requester.
    ///
    /// Tracks when this peer sends a get response (success or not found) to an upstream peer.
    /// This provides sender-side visibility for debugging message routing and understanding
    /// how search results propagate back through the network.
    ResponseSent {
        id: Transaction,
        /// The peer sending the response.
        from: PeerKeyLocation,
        /// The peer receiving the response.
        to: PeerKeyLocation,
        key: Option<ContractKey>,
        timestamp: u64,
    },
}

impl GetEvent {
    /// Returns the contract key for this event if available.
    /// Only GetSuccess and ResponseSent may have the full key; other variants have instance_id.
    fn contract_key(&self) -> Option<ContractKey> {
        match self {
            GetEvent::GetSuccess { key, .. } => Some(*key),
            GetEvent::ResponseSent { key, .. } => *key,
            _ => None,
        }
    }
}

/// SUBSCRIBE operation events for tracking the lifecycle of contract subscriptions.
///
/// Similar to `GetEvent` and `PutEvent`, this enum captures the full sequence of a Subscribe operation:
/// - Request initiation
/// - Success when subscription is established
/// - NotFound when contract doesn't exist after search
/// - Seeding state changes (local client subscriptions)
/// - Subscription tree structure changes (upstream/downstream relationships)
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum SubscribeEvent {
    /// A Subscribe request was initiated or received.
    Request {
        id: Transaction,
        /// The peer initiating or receiving the request.
        requester: PeerKeyLocation,
        /// Contract instance being subscribed to (full key may not be known yet).
        instance_id: ContractInstanceId,
        /// Target peer (with hop-by-hop routing, this is the current node).
        target: PeerKeyLocation,
        /// Hops remaining before giving up.
        htl: usize,
        timestamp: u64,
    },
    /// Subscription was successfully established.
    SubscribeSuccess {
        id: Transaction,
        /// Full contract key (only available after successful subscription).
        key: ContractKey,
        /// Location where subscription was established.
        at: PeerKeyLocation,
        /// Number of hops the request traversed.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
        requester: PeerKeyLocation,
    },
    /// Contract was not found after exhaustive search.
    SubscribeNotFound {
        id: Transaction,
        requester: PeerKeyLocation,
        /// Contract instance that was searched for.
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        /// Number of hops the request traversed before exhaustion.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Subscribe response being sent back to the requester.
    ///
    /// Tracks when this peer sends a subscribe response (success or not found) to an upstream peer.
    /// This provides sender-side visibility for debugging subscription propagation.
    ResponseSent {
        id: Transaction,
        /// The peer sending the response.
        from: PeerKeyLocation,
        /// The peer receiving the response.
        to: PeerKeyLocation,
        key: Option<ContractKey>,
        timestamp: u64,
    },
    /// A local client started seeding a contract (via WebSocket subscription).
    ///
    /// This event fires when a local application subscribes to a contract,
    /// indicating this peer is now interested in receiving updates for the contract.
    SeedingStarted {
        /// Contract instance being seeded.
        instance_id: ContractInstanceId,
        timestamp: u64,
    },
    /// A local client stopped seeding a contract (last WebSocket client unsubscribed).
    ///
    /// This event fires when the last local client unsubscribes from a contract,
    /// indicating this peer no longer has local interest in the contract.
    SeedingStopped {
        /// Contract instance that is no longer being seeded locally.
        instance_id: ContractInstanceId,
        /// Reason for stopping seeding.
        reason: SeedingStoppedReason,
        timestamp: u64,
    },
    /// A downstream subscriber was added for a contract.
    ///
    /// This event fires when another peer subscribes through us,
    /// meaning we need to forward updates to them.
    DownstreamAdded {
        /// Contract for which subscriber was added.
        key: ContractKey,
        /// The new downstream subscriber.
        subscriber: PeerKeyLocation,
        /// Current count of downstream subscribers after this addition.
        downstream_count: usize,
        timestamp: u64,
    },
    /// A downstream subscriber was removed for a contract.
    ///
    /// This event fires when a peer that subscribed through us disconnects or unsubscribes.
    DownstreamRemoved {
        /// Contract from which subscriber was removed.
        key: ContractKey,
        /// The removed downstream subscriber (if known).
        subscriber: Option<PeerKeyLocation>,
        /// Reason for removal.
        reason: DownstreamRemovedReason,
        /// Remaining downstream subscribers after removal.
        downstream_count: usize,
        timestamp: u64,
    },
    /// An upstream source was set for a contract.
    ///
    /// This event fires when we subscribe to a contract through another peer,
    /// meaning we will receive updates from them.
    UpstreamSet {
        /// Contract for which upstream was set.
        key: ContractKey,
        /// The upstream peer we subscribed through.
        upstream: PeerKeyLocation,
        timestamp: u64,
    },
    /// We unsubscribed from a contract's upstream.
    ///
    /// This event fires when we no longer need to receive updates for a contract,
    /// typically because all local clients and downstream subscribers have gone.
    Unsubscribed {
        /// Contract we unsubscribed from.
        key: ContractKey,
        /// Reason for unsubscribing.
        reason: UnsubscribedReason,
        /// The upstream peer we unsubscribed from (if any).
        upstream: Option<PeerKeyLocation>,
        timestamp: u64,
    },
    /// Snapshot of subscription state for a contract.
    ///
    /// This periodic or on-change event provides visibility into the full
    /// subscription tree structure for debugging update propagation.
    SubscriptionState {
        /// Contract this state is for.
        key: ContractKey,
        /// Whether we are locally seeding this contract (local client subscribed).
        is_seeding: bool,
        /// Our upstream source for updates (if any).
        upstream: Option<PeerKeyLocation>,
        /// Number of downstream subscribers.
        downstream_count: usize,
        /// List of downstream subscribers.
        downstream: Vec<PeerKeyLocation>,
        timestamp: u64,
    },
}

impl SubscribeEvent {
    /// Returns the contract key for this event if available.
    /// Only some variants have the full key; Request and some others have instance_id.
    fn contract_key(&self) -> Option<ContractKey> {
        match self {
            SubscribeEvent::SubscribeSuccess { key, .. }
            | SubscribeEvent::DownstreamAdded { key, .. }
            | SubscribeEvent::DownstreamRemoved { key, .. }
            | SubscribeEvent::UpstreamSet { key, .. }
            | SubscribeEvent::Unsubscribed { key, .. }
            | SubscribeEvent::SubscriptionState { key, .. } => Some(*key),
            SubscribeEvent::ResponseSent { key, .. } => *key,
            _ => None,
        }
    }
}

/// Reason why local seeding stopped for a contract.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum SeedingStoppedReason {
    /// Last local client unsubscribed.
    LastClientUnsubscribed,
    /// Client disconnected.
    ClientDisconnected,
}

/// Reason why a downstream subscriber was removed.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum DownstreamRemovedReason {
    /// Peer explicitly unsubscribed.
    PeerUnsubscribed,
    /// Peer disconnected from the network.
    PeerDisconnected,
}

/// Reason why we unsubscribed from upstream.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum UnsubscribedReason {
    /// Last local client unsubscribed.
    LastClientUnsubscribed,
    /// Last downstream subscriber disconnected and no local clients.
    NoRemainingInterest,
    /// Upstream peer disconnected.
    UpstreamDisconnected,
}

/// Direction of data transfer.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum TransferDirection {
    /// Sending data to a peer.
    Send,
    /// Receiving data from a peer.
    Receive,
}

/// Data transfer events for tracking stream-level transfers.
///
/// These events track the start and completion of data transfers between peers,
/// including LEDBAT congestion control metrics. This enables:
/// - Monitoring transfer throughput and latency
/// - Debugging LEDBAT behavior (slowdowns, cwnd evolution)
/// - Identifying bottlenecks in data propagation
///
/// Note: Individual packets are NOT reported to avoid flooding the telemetry system.
/// Only transfer-level events (start/complete/failed) are emitted.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum TransferEvent {
    /// A data stream transfer has started.
    Started {
        /// Unique identifier for this stream.
        stream_id: u64,
        /// The remote peer's socket address.
        /// Note: Uses SocketAddr rather than PeerKeyLocation because the transport
        /// layer doesn't always have access to the peer's public key (especially
        /// for inbound connections at gateways before identity is established).
        peer_addr: std::net::SocketAddr,
        /// Total expected bytes to transfer.
        expected_bytes: u64,
        /// Whether we're sending or receiving.
        direction: TransferDirection,
        /// Transaction ID associated with this transfer (if available).
        /// May be None when the transport layer doesn't have transaction context.
        tx_id: Option<Transaction>,
        timestamp: u64,
    },
    /// A data stream transfer completed successfully.
    Completed {
        /// Unique identifier for this stream.
        stream_id: u64,
        /// The remote peer's socket address.
        peer_addr: std::net::SocketAddr,
        /// Actual bytes transferred.
        bytes_transferred: u64,
        /// Time elapsed from start to completion (milliseconds).
        elapsed_ms: u64,
        /// Average throughput (bytes per second).
        avg_throughput_bps: u64,
        /// Peak congestion window during transfer (bytes).
        peak_cwnd_bytes: Option<u32>,
        /// Final congestion window at completion (bytes).
        final_cwnd_bytes: Option<u32>,
        /// Number of LEDBAT slowdowns triggered during transfer.
        /// A high count indicates congestion or competing flows.
        slowdowns_triggered: Option<u32>,
        /// Final smoothed RTT at completion (milliseconds).
        final_srtt_ms: Option<u32>,
        /// Final slow start threshold at completion (bytes).
        /// Key diagnostic for death spiral: if ssthresh collapses to min_cwnd,
        /// slow start can't recover useful throughput.
        final_ssthresh_bytes: Option<u32>,
        /// Effective minimum ssthresh floor (bytes).
        /// This floor prevents ssthresh from collapsing too low during timeouts.
        min_ssthresh_floor_bytes: Option<u32>,
        /// Total retransmission timeouts (RTO events) during transfer.
        /// High values indicate severe congestion or path issues.
        total_timeouts: Option<u32>,
        /// Whether we were sending or receiving.
        direction: TransferDirection,
        timestamp: u64,
    },
    /// A data stream transfer failed.
    Failed {
        /// Unique identifier for this stream.
        stream_id: u64,
        /// The remote peer's socket address.
        peer_addr: std::net::SocketAddr,
        /// Bytes transferred before failure.
        bytes_transferred: u64,
        /// Reason for failure.
        reason: String,
        /// Time elapsed before failure (milliseconds).
        elapsed_ms: u64,
        /// Whether we were sending or receiving.
        direction: TransferDirection,
        timestamp: u64,
    },
}

/// Peer lifecycle events for tracking node startup and shutdown.
///
/// These events help with:
/// - Monitoring fleet health (which peers are online/offline)
/// - Understanding version distribution across the network
/// - Debugging issues specific to certain OS/architecture combinations
/// - Tracking graceful vs ungraceful shutdowns
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum PeerLifecycleEvent {
    /// Peer has started and is ready to participate in the network.
    Startup {
        /// Freenet core version (from Cargo.toml).
        version: String,
        /// Git commit hash (if available).
        git_commit: Option<String>,
        /// Whether the build has uncommitted changes.
        git_dirty: Option<bool>,
        /// Target architecture (e.g., "x86_64", "aarch64").
        arch: String,
        /// Operating system (e.g., "linux", "macos", "windows").
        os: String,
        /// OS version/release (e.g., "Ubuntu 22.04", "macOS 14.0").
        os_version: Option<String>,
        /// Whether this peer is configured as a gateway.
        is_gateway: bool,
        /// Timestamp when the peer started.
        timestamp: u64,
    },
    /// Peer is shutting down.
    Shutdown {
        /// Whether this is a graceful shutdown (true) or unexpected (false).
        graceful: bool,
        /// Reason for shutdown if available.
        reason: Option<String>,
        /// Total uptime in seconds.
        uptime_secs: u64,
        /// Total connections made during uptime.
        total_connections: u64,
        /// Timestamp when shutdown was initiated.
        timestamp: u64,
    },
}

#[cfg(feature = "trace")]
pub mod tracer {
    use std::io::IsTerminal;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use tracing::level_filters::LevelFilter;
    use tracing_appender::non_blocking::WorkerGuard;
    use tracing_appender::rolling::{RollingFileAppender, Rotation};
    use tracing_subscriber::{Layer, Registry};

    /// Number of hours to keep log files (using hourly rotation)
    /// Keeps logs small to prevent disk fill-up from log spam
    const LOG_RETENTION_HOURS: usize = 3;

    /// Guards for non-blocking file appenders - must be kept alive for the lifetime of the program
    static LOG_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();

    /// Get the log directory for the current platform.
    /// Used by both the tracer (for writing logs) and report command (for reading logs).
    pub fn get_log_dir() -> Option<PathBuf> {
        #[cfg(target_os = "linux")]
        {
            dirs::home_dir().map(|h| h.join(".local/state/freenet"))
        }

        #[cfg(target_os = "macos")]
        {
            dirs::home_dir().map(|h| h.join("Library/Logs/freenet"))
        }

        #[cfg(target_os = "windows")]
        {
            dirs::data_local_dir().map(|d| d.join("freenet").join("logs"))
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            None
        }
    }

    /// Clean up old log files on startup.
    /// Removes any log files older than LOG_RETENTION_HOURS, including legacy daily log files.
    fn cleanup_old_logs(log_dir: &std::path::Path) {
        use std::time::{Duration, SystemTime};

        let retention = Duration::from_secs(LOG_RETENTION_HOURS as u64 * 3600);
        let cutoff = SystemTime::now() - retention;

        let Ok(entries) = std::fs::read_dir(log_dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();

            // Only process log files
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with("freenet") || !name.ends_with(".log") {
                continue;
            }

            // Check file modification time
            if let Ok(metadata) = path.metadata() {
                if let Ok(modified) = metadata.modified() {
                    if modified < cutoff {
                        if let Err(e) = std::fs::remove_file(&path) {
                            eprintln!("Failed to remove old log file {}: {}", path.display(), e);
                        }
                    }
                }
            }
        }
    }

    pub fn init_tracer(
        level: Option<LevelFilter>,
        _endpoint: Option<String>,
    ) -> anyhow::Result<()> {
        // Initialize console subscriber if enabled
        #[cfg(feature = "console-subscriber")]
        {
            if std::env::var("TOKIO_CONSOLE").is_ok() {
                console_subscriber::init();
                tracing::info!(
                    "Tokio console subscriber initialized. Connect with 'tokio-console' command."
                );
                return Ok(());
            }
        }

        let default_filter = if cfg!(any(test, debug_assertions)) {
            LevelFilter::DEBUG
        } else {
            LevelFilter::INFO
        };
        let default_filter = level.unwrap_or(default_filter);

        use tracing_subscriber::layer::SubscriberExt;

        let disabled_logs = std::env::var("FREENET_DISABLE_LOGS").is_ok();
        if disabled_logs {
            return Ok(());
        }

        let to_stderr = std::env::var("FREENET_LOG_TO_STDERR").is_ok();
        let force_file_logs = std::env::var("FREENET_LOG_TO_FILE").is_ok();
        let use_json = std::env::var("FREENET_LOG_FORMAT")
            .map(|v| v.to_lowercase() == "json")
            .unwrap_or(false);

        // Determine if we should write to files:
        // - If FREENET_LOG_TO_FILE is set, always use file logging
        // - If stdout is not a terminal (running as service), use file logging
        // - If FREENET_LOG_TO_STDERR is set, use stderr instead
        // - Otherwise, use stdout (interactive mode)
        let use_file_logging = force_file_logs || (!to_stderr && !std::io::stdout().is_terminal());

        // Build filter
        let filter_layer = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(default_filter.into())
            .from_env_lossy()
            .add_directive("stretto=off".parse().expect("infallible"))
            .add_directive("sqlx=error".parse().expect("infallible"));

        // Get rate limit from environment or use default (1000 events/sec)
        let rate_limit: u64 = std::env::var("FREENET_LOG_RATE_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(crate::util::rate_limit_layer::DEFAULT_MAX_EVENTS_PER_SECOND);

        // Rate limiting is disabled in tests and debug builds to avoid masking issues
        let rate_limit_enabled = !cfg!(any(test, debug_assertions))
            && std::env::var("FREENET_DISABLE_LOG_RATE_LIMIT").is_err();

        // Create rate limiter (shared across all layers)
        let rate_limiter = if rate_limit_enabled {
            Some(crate::util::rate_limit_layer::RateLimiter::new(rate_limit))
        } else {
            None
        };

        if use_file_logging {
            if let Some(log_dir) = get_log_dir() {
                // Create log directory if it doesn't exist
                if let Err(e) = std::fs::create_dir_all(&log_dir) {
                    eprintln!("Warning: Failed to create log directory: {e}");
                    // Fall back to stdout logging
                    return init_stdout_tracer(
                        default_filter,
                        to_stderr,
                        use_json,
                        filter_layer,
                        rate_limiter,
                    );
                }

                // Clean up old log files (including legacy daily logs) on startup
                cleanup_old_logs(&log_dir);

                // Create rolling file appender for main log (hourly rotation, 3 hour retention)
                let main_appender = RollingFileAppender::builder()
                    .rotation(Rotation::HOURLY)
                    .max_log_files(LOG_RETENTION_HOURS)
                    .filename_prefix("freenet")
                    .filename_suffix("log")
                    .build(&log_dir)
                    .map_err(|e| anyhow::anyhow!("Failed to create log appender: {e}"))?;

                // Create rolling file appender for error log (hourly rotation, 3 hour retention)
                let error_appender = RollingFileAppender::builder()
                    .rotation(Rotation::HOURLY)
                    .max_log_files(LOG_RETENTION_HOURS)
                    .filename_prefix("freenet.error")
                    .filename_suffix("log")
                    .build(&log_dir)
                    .map_err(|e| anyhow::anyhow!("Failed to create error log appender: {e}"))?;

                let (main_writer, main_guard) = tracing_appender::non_blocking(main_appender);
                let (error_writer, error_guard) = tracing_appender::non_blocking(error_appender);

                // Store guards to keep writers alive; fail if already initialized
                if LOG_GUARDS.set(vec![main_guard, error_guard]).is_err() {
                    return Err(anyhow::anyhow!(
                        "LOG_GUARDS already initialized; tracer cannot be re-initialized"
                    ));
                }

                // Apply rate limiting as a global filter if enabled
                // Layers must be created after the rate filter to ensure type compatibility
                if let Some(rate_limiter) = rate_limiter.clone() {
                    let rate_filter =
                        tracing_subscriber::filter::filter_fn(move |_| rate_limiter.should_allow());
                    let base = Registry::default().with(rate_filter);

                    // Create layers for main and error logs (typed against rate-filtered registry)
                    let main_layer = tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .with_ansi(false)
                        .with_writer(main_writer.clone())
                        .with_filter(filter_layer);

                    let error_filter = tracing_subscriber::EnvFilter::builder()
                        .with_default_directive(LevelFilter::WARN.into())
                        .from_env_lossy();

                    let error_layer = tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .with_ansi(false)
                        .with_writer(error_writer.clone())
                        .with_filter(error_filter);

                    let subscriber = base.with(main_layer).with(error_layer);
                    tracing::subscriber::set_global_default(subscriber)
                        .expect("Error setting subscriber");
                } else {
                    // Create layers for main and error logs (typed against plain registry)
                    let main_layer = tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .with_ansi(false)
                        .with_writer(main_writer)
                        .with_filter(filter_layer);

                    let error_filter = tracing_subscriber::EnvFilter::builder()
                        .with_default_directive(LevelFilter::WARN.into())
                        .from_env_lossy();

                    let error_layer = tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .with_ansi(false)
                        .with_writer(error_writer)
                        .with_filter(error_filter);

                    let subscriber = Registry::default().with(main_layer).with(error_layer);
                    tracing::subscriber::set_global_default(subscriber)
                        .expect("Error setting subscriber");
                }

                return Ok(());
            }
        }

        // Fall back to stdout/stderr logging
        init_stdout_tracer(
            default_filter,
            to_stderr,
            use_json,
            filter_layer,
            rate_limiter,
        )
    }

    fn init_stdout_tracer(
        _default_filter: LevelFilter,
        to_stderr: bool,
        use_json: bool,
        filter_layer: tracing_subscriber::EnvFilter,
        rate_limiter: Option<crate::util::rate_limit_layer::RateLimiter>,
    ) -> anyhow::Result<()> {
        use tracing_subscriber::layer::SubscriberExt;

        // Helper to create the format layer
        fn make_layer<
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        >(
            to_stderr: bool,
            use_json: bool,
        ) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync> {
            if to_stderr {
                if use_json {
                    tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .json()
                        .with_file(cfg!(any(test, debug_assertions)))
                        .with_line_number(cfg!(any(test, debug_assertions)))
                        .with_writer(std::io::stderr)
                        .boxed()
                } else {
                    let layer = tracing_subscriber::fmt::layer().with_level(true).pretty();
                    let layer = if cfg!(any(test, debug_assertions)) {
                        layer.with_file(true).with_line_number(true)
                    } else {
                        layer
                    };
                    layer.with_writer(std::io::stderr).boxed()
                }
            } else if use_json {
                tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .json()
                    .with_file(cfg!(any(test, debug_assertions)))
                    .with_line_number(cfg!(any(test, debug_assertions)))
                    .boxed()
            } else {
                let layer = tracing_subscriber::fmt::layer().with_level(true).pretty();
                if cfg!(any(test, debug_assertions)) {
                    layer.with_file(true).with_line_number(true).boxed()
                } else {
                    layer.boxed()
                }
            }
        }

        // Apply rate limiting as a global filter if enabled
        // Layers must be created after the rate filter to ensure type compatibility
        if let Some(rate_limiter) = rate_limiter {
            let rate_filter =
                tracing_subscriber::filter::filter_fn(move |_| rate_limiter.should_allow());
            let base = Registry::default().with(rate_filter);
            let layer = make_layer(to_stderr, use_json);
            let subscriber = base.with(layer.with_filter(filter_layer));
            tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
        } else {
            let layer = make_layer(to_stderr, use_json);
            let subscriber = Registry::default().with(layer.with_filter(filter_layer));
            tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
        }
        Ok(())
    }
}

pub(super) mod test {
    use dashmap::DashMap;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering::SeqCst},
    };

    use super::*;
    use crate::{node::testing_impl::NodeLabel, ring::Distance, transport::TransportPublicKey};

    static LOG_ID: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    pub(crate) struct TestEventListener {
        node_labels: Arc<DashMap<NodeLabel, TransportPublicKey>>,
        tx_log: Arc<DashMap<Transaction, Vec<ListenerLogId>>>,
        pub(crate) logs: Arc<tokio::sync::Mutex<Vec<NetLogMessage>>>,
        network_metrics_server:
            Arc<tokio::sync::Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    }

    impl TestEventListener {
        pub async fn new() -> Self {
            TestEventListener {
                node_labels: Arc::new(DashMap::new()),
                tx_log: Arc::new(DashMap::new()),
                logs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                network_metrics_server: Arc::new(tokio::sync::Mutex::new(
                    connect_to_metrics_server().await,
                )),
            }
        }

        pub fn add_node(&mut self, label: NodeLabel, peer: TransportPublicKey) {
            self.node_labels.insert(label, peer);
        }

        pub fn is_connected(&self, peer: &TransportPublicKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            logs.iter().any(|log| {
                &log.peer_id.pub_key == peer
                    && matches!(log.kind, EventKind::Connect(ConnectEvent::Connected { .. }))
            })
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(
            &self,
            key: &TransportPublicKey,
        ) -> Box<dyn Iterator<Item = (PeerId, Distance)>> {
            let Ok(logs) = self.logs.try_lock() else {
                return Box::new([].into_iter());
            };
            let disconnects = logs
                .iter()
                .filter(|l| matches!(l.kind, EventKind::Disconnected { .. }))
                .fold(HashMap::<_, Vec<_>>::new(), |mut map, log| {
                    map.entry(log.peer_id.clone())
                        .or_default()
                        .push(log.datetime);
                    map
                });

            let iter = logs
                .iter()
                .filter_map(|l| {
                    if let EventKind::Connect(ConnectEvent::Connected {
                        this, connected, ..
                    }) = &l.kind
                    {
                        let connected_id =
                            PeerId::new(connected.socket_addr()?, connected.pub_key().clone());
                        let disconnected = disconnects
                            .get(&connected_id)
                            .iter()
                            .flat_map(|dcs| dcs.iter())
                            .any(|dc| dc > &l.datetime);
                        if let Some((this_loc, conn_loc)) =
                            this.location().zip(connected.location())
                        {
                            if this.pub_key() == key && !disconnected {
                                return Some((connected_id, conn_loc.distance(this_loc)));
                            }
                        }
                    }
                    None
                })
                .collect::<HashMap<_, _>>()
                .into_iter();
            Box::new(iter)
        }

        fn create_log(log: NetEventLog) -> (NetLogMessage, ListenerLogId) {
            let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
            let NetEventLog { peer_id, kind, .. } = log;
            let msg_log = NetLogMessage {
                datetime: Utc::now(),
                tx: *log.tx,
                peer_id: peer_id.clone(),
                kind,
            };
            (msg_log, log_id)
        }
    }

    impl super::NetEventRegister for TestEventListener {
        fn register_events<'a>(
            &'a self,
            logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> BoxFuture<'a, ()> {
            async {
                match logs {
                    Either::Left(log) => {
                        let tx = log.tx;
                        let (msg_log, log_id) = Self::create_log(log);
                        if let Some(conn) = &mut *self.network_metrics_server.lock().await {
                            send_to_metrics_server(conn, &msg_log).await;
                        }
                        self.logs.lock().await.push(msg_log);
                        self.tx_log.entry(*tx).or_default().push(log_id);
                    }
                    Either::Right(logs) => {
                        let logs_list = &mut *self.logs.lock().await;
                        let mut lock = self.network_metrics_server.lock().await;
                        for log in logs {
                            let tx = log.tx;
                            let (msg_log, log_id) = Self::create_log(log);
                            if let Some(conn) = &mut *lock {
                                send_to_metrics_server(conn, &msg_log).await;
                            }
                            logs_list.push(msg_log);
                            self.tx_log.entry(*tx).or_default().push(log_id);
                        }
                    }
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(&mut self, _: Transaction) -> BoxFuture<'_, ()> {
            async {}.boxed()
        }

        fn get_router_events(
            &self,
            _number: usize,
        ) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
            async { Ok(vec![]) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_get_connections() -> anyhow::Result<()> {
        let peer_id = PeerId::random();
        let tx = Transaction::new::<connect::ConnectMsg>();
        // Create other peers - location is now computed from their addresses
        let other_peers = [PeerId::random(), PeerId::random(), PeerId::random()];

        let listener = TestEventListener::new().await;
        let futs = futures::stream::FuturesUnordered::from_iter(other_peers.iter().map(|other| {
            listener.register_events(Either::Left(NetEventLog {
                tx: &tx,
                peer_id: peer_id.clone(),
                kind: EventKind::Connect(ConnectEvent::Connected {
                    this: PeerKeyLocation::new(peer_id.pub_key.clone(), peer_id.addr),
                    connected: PeerKeyLocation::new(other.pub_key.clone(), other.addr),
                    elapsed_ms: None,
                    connection_type: ConnectionType::Direct,
                    latency_ms: None,
                    this_peer_connection_count: 0,
                    initiated_by: None,
                }),
            }))
        }));

        futures::future::join_all(futs).await;

        let distances: Vec<_> = listener.connections(&peer_id.pub_key).collect();
        assert_eq!(distances.len(), 3, "Should have 3 connections");
        // Verify each distance is valid (between 0 and 0.5 on the ring)
        for (_, dist) in &distances {
            assert!(
                dist.as_f64() >= 0.0 && dist.as_f64() <= 0.5,
                "Distance should be valid ring distance"
            );
        }
        Ok(())
    }

    #[test]
    fn test_state_hash_short() {
        use freenet_stdlib::prelude::WrappedState;

        // Test with known input produces consistent 8-char hex output
        let state = WrappedState::new(vec![1, 2, 3, 4, 5]);
        let hash = super::state_hash_short(&state);

        // Should be exactly 8 hex chars (4 bytes)
        assert_eq!(hash.len(), 8, "Hash should be 8 hex characters");
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "Hash should only contain hex digits"
        );

        // Same input produces same output (deterministic)
        assert_eq!(
            hash,
            super::state_hash_short(&state),
            "Hash should be deterministic"
        );

        // Different input produces different output
        let state2 = WrappedState::new(vec![5, 4, 3, 2, 1]);
        assert_ne!(
            hash,
            super::state_hash_short(&state2),
            "Different states should produce different hashes"
        );

        // Empty state still produces valid 8-char hash
        let empty = WrappedState::new(vec![]);
        let empty_hash = super::state_hash_short(&empty);
        assert_eq!(
            empty_hash.len(),
            8,
            "Empty state should still produce 8-char hash"
        );
    }
}
