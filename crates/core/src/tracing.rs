use std::{path::PathBuf, sync::Arc, time::SystemTime};

use chrono::{DateTime, Utc};
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{FutureExt, future::BoxFuture};
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
pub use state_verifier::{StateVerifier, VerificationReport};

use crate::node::OpManager;

/// An append-only log for network events.
mod aof;

/// Event aggregation across multiple nodes for debugging and testing.
pub mod event_aggregator;

/// Telemetry reporting to central collector.
pub mod telemetry;
pub use telemetry::TelemetryReporter;

/// Automatic state verification through telemetry linearization.
pub mod state_verifier;

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
    fn notify_of_time_out(
        &mut self,
        tx: Transaction,
        op_type: &str,
        target_peer: Option<String>,
    ) -> BoxFuture<'_, ()>;
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

    fn notify_of_time_out(
        &mut self,
        tx: Transaction,
        op_type: &str,
        target_peer: Option<String>,
    ) -> BoxFuture<'_, ()> {
        let op_type = op_type.to_string();
        async move {
            for reg in &mut self.0 {
                reg.notify_of_time_out(tx, &op_type, target_peer.clone())
                    .await;
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

    fn notify_of_time_out(
        &mut self,
        tx: Transaction,
        op_type: &str,
        target_peer: Option<String>,
    ) -> BoxFuture<'_, ()> {
        let op_type = op_type.to_string();
        async move {
            for reg in &mut self.0 {
                reg.notify_of_time_out(tx, &op_type, target_peer.clone())
                    .await;
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

// GET telemetry migrated to `record_get_access` /
// `mark_local_client_access` and operator-facing tracing spans;
// these `NetEventLog` constructors are no longer emitted but kept
// for symmetry with the surviving counterparts.
#[allow(dead_code)]
impl<'a> NetEventLog<'a> {
    /// Safely get the peer_id from the ring's connection manager.
    /// Returns None if the peer doesn't have a known address (e.g., during startup).
    /// Telemetry should never panic - we just skip events if we can't identify ourselves.
    fn get_own_peer_id(ring: &Ring) -> Option<PeerId> {
        let own_loc = ring.connection_manager.own_location();
        own_loc
            .socket_addr()
            .map(|addr| PeerId::new(own_loc.pub_key().clone(), addr))
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
        state_size: Option<usize>,
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
                state_size,
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

    /// Create a ForwardingAck sent event.
    pub fn get_forwarding_ack_sent(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        to: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Get(GetEvent::ForwardingAckSent {
                id: *tx,
                from: own_loc,
                to,
                instance_id,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a ForwardingAck received event.
    pub fn get_forwarding_ack_received(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Get(GetEvent::ForwardingAckReceived {
                id: *tx,
                receiver: own_loc,
                instance_id,
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

    /// Create a Subscribe timeout event (#3445).
    ///
    /// Emitted from the client-initiated subscribe driver when it exhausts
    /// all candidate peers without ever receiving a terminal reply (every
    /// attempt timed out or errored). Mirrors [`Self::subscribe_not_found`]
    /// but records the timeout outcome so the dashboard pairs every
    /// `subscribe_request` with an outcome instead of leaving it dangling.
    pub fn subscribe_timeout(
        tx: &'a Transaction,
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        retries: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::SubscribeTimeout {
                id: *tx,
                requester: own_loc,
                instance_id,
                retries,
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
        state_size: Option<usize>,
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
                state_size,
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
                state_hash_before: Some(state_hash_full(state_before)),
                state_hash_after: Some(state_hash_full(state_after)),
                changed,
                state_size: state_after.len(),
            }),
        })
    }

    /// Create an Update broadcast emitted event.
    ///
    /// Emitted when this node broadcasts a state update to subscribed peers,
    /// capturing which peers were targeted and how many sends succeeded.
    pub fn update_broadcast_emitted(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        broadcast_to: Vec<PeerKeyLocation>,
        broadcasted_to: usize,
        value: WrappedState,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        let state_hash = Some(state_hash_short(&value));
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::BroadcastEmitted {
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

    /// Create a broadcast delivery summary event with the full breakdown of
    /// why each potential target was or was not sent the broadcast (issue #3046).
    pub fn broadcast_delivery_summary(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        target_result: &crate::operations::update::BroadcastTargetResult,
        skipped_summary_match: usize,
        targets_sent: usize,
        send_failed: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::BroadcastDeliverySummary {
                key,
                proximity_found: target_result.proximity_found,
                proximity_resolve_failed: target_result.proximity_resolve_failed,
                interest_found: target_result.interest_found,
                interest_resolve_failed: target_result.interest_resolve_failed,
                skipped_self: target_result.skipped_self,
                skipped_sender: target_result.skipped_sender,
                skipped_summary_match,
                targets_sent,
                send_failed,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create an Update failure event.
    pub fn update_failure(
        tx: &'a Transaction,
        ring: &'a Ring,
        key: ContractKey,
        reason: OperationFailure,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        let own_loc = ring.connection_manager.own_location();
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Update(UpdateEvent::UpdateFailure {
                id: *tx,
                requester: own_loc.clone(),
                target: own_loc,
                key,
                reason,
                elapsed_ms: tx.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a Connect rejected event.
    ///
    /// Emitted when a peer sends a Rejected message upstream for a connect request.
    pub fn connect_rejected(
        tx: &'a Transaction,
        ring: &'a Ring,
        desired_location: Location,
        reason: &str,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::Rejected {
                desired_location,
                reason: reason.to_string(),
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

    // ==================== Hosting/Subscription Events ====================

    /// Create a hosting_started event when a local client subscribes to a contract.
    pub fn hosting_started(ring: &'a Ring, instance_id: ContractInstanceId) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::HostingStarted {
                instance_id,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create a hosting_stopped event when the last local client unsubscribes from a contract.
    #[allow(dead_code)] // Helper available for future use
    pub fn hosting_stopped(
        ring: &'a Ring,
        instance_id: ContractInstanceId,
        reason: HostingStoppedReason,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Subscribe(SubscribeEvent::HostingStopped {
                instance_id,
                reason,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            }),
        })
    }

    /// Create a router snapshot event for periodic telemetry.
    pub fn router_snapshot(
        ring: &'a Ring,
        snapshot: crate::router::RouterSnapshotInfo,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::RouterSnapshot(snapshot),
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
        let peer_id = PeerId::new(own_loc.pub_key().clone(), own_addr);
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
            NetMessage::V1(NetMessageV1::Put(PutMsg::Response { id, key, .. })) => {
                // Track outbound Put response for message routing visibility
                let to = target_addr
                    .and_then(|addr| ring.connection_manager.get_peer_by_addr(addr))
                    .unwrap_or_else(|| own_loc.clone()); // Fallback to own location if target unknown
                EventKind::Put(PutEvent::ResponseSent {
                    id: *id,
                    from: own_loc,
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
                    from: own_loc,
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
                    from: own_loc,
                    to,
                    key,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Unsubscribe {
                id,
                instance_id,
            })) => {
                let to = target_addr
                    .and_then(|addr| ring.connection_manager.get_peer_by_addr(addr))
                    .unwrap_or_else(|| own_loc.clone());
                EventKind::Subscribe(SubscribeEvent::UnsubscribeSent {
                    id: *id,
                    instance_id: *instance_id,
                    from: own_loc,
                    to,
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
        source_addr: Option<std::net::SocketAddr>,
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
                let acceptor_peer_id = PeerId::new(acceptor.pub_key().clone(), acceptor_addr);
                let this_peer_id = PeerId::new(this_peer.pub_key().clone(), this_addr);
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
                        peer_id: acceptor_peer_id,
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
            NetMessageV1::Put(PutMsg::Response { id, key, hop_count }) => {
                let this_peer = &op_manager.ring.connection_manager.own_location();
                // hop_count is carried on the wire Response: set by the storer
                // (max_htl - htl_at_storer) and preserved by relays bubbling up.
                // Clamp to ring.max_hops_to_live so a malicious or buggy peer
                // sending hop_count = usize::MAX can't pollute telemetry.
                // Mirrors the GET extraction logic (PR #4245).
                let max_htl = op_manager.ring.max_hops_to_live;
                EventKind::Put(PutEvent::PutSuccess {
                    id: *id,
                    requester: this_peer.clone(),
                    target: this_peer.clone(),
                    key: *key,
                    hop_count: Some((*hop_count).min(max_htl)),
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                    state_size: None, // Size not available from message
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
                hop_count,
                ..
            }) if value.state.is_some() => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // hop_count is carried on the wire Response: set by the storer
                // (max_htl - htl_at_storer) and preserved by relays bubbling up.
                // Clamp to ring.max_hops_to_live so a malicious or buggy peer
                // sending hop_count = usize::MAX can't pollute telemetry.
                let max_htl = op_manager.ring.max_hops_to_live;
                EventKind::Get(GetEvent::GetSuccess {
                    id: *id,
                    requester: this_peer.clone(),
                    target: this_peer,
                    key: *key,
                    hop_count: Some((*hop_count).min(max_htl)),
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                })
            }
            NetMessageV1::Get(GetMsg::Response {
                id,
                instance_id,
                result: GetMsgResult::NotFound,
                hop_count,
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // hop_count is carried on the wire Response (same semantics
                // as GetSuccess: forward-path depth from originator to the
                // node that produced the NotFound).
                //
                // Caveat for NotFound interpretation: the value is the
                // forward depth of *the relay that produced the NotFound*,
                // which is the deepest peer the request reached before
                // exhaustion / store-miss.  For analytics, treat NotFound
                // hop_count as "exhaustion depth", not "path depth to the
                // contract location" — there is no storer.
                //
                // Same clamp as GetSuccess: bound by max_hops_to_live to
                // guard against malicious or buggy peer values.
                let max_htl = op_manager.ring.max_hops_to_live;
                EventKind::Get(GetEvent::GetNotFound {
                    id: *id,
                    requester: this_peer.clone(),
                    instance_id: *instance_id,
                    target: this_peer,
                    hop_count: Some((*hop_count).min(max_htl)),
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
                hop_count,
                ..
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // hop_count is carried on the wire Response: set by the
                // hosting peer (max_htl - htl_at_storer) and preserved by
                // relays bubbling up.  Clamp to ring.max_hops_to_live so
                // a malicious or buggy peer sending hop_count = usize::MAX
                // can't pollute telemetry.  Mirrors GET (PR #4245).
                let max_htl = op_manager.ring.max_hops_to_live;
                EventKind::Subscribe(SubscribeEvent::SubscribeSuccess {
                    id: *id,
                    key: *key,
                    at: this_peer.clone(),
                    hop_count: Some((*hop_count).min(max_htl)),
                    elapsed_ms: id.elapsed().as_millis() as u64,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    requester: this_peer,
                })
            }
            NetMessageV1::Subscribe(SubscribeMsg::Response {
                id,
                instance_id,
                result: SubscribeMsgResult::NotFound,
                hop_count,
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // hop_count is carried on the wire Response (same semantics
                // as SubscribeSuccess: forward-path depth from originator
                // to the node that produced the response).
                //
                // Caveat for NotFound interpretation: the value is the
                // forward depth of *the relay that produced the NotFound*
                // (HTL exhaustion, no candidates, or downstream forward
                // failure).  For analytics, treat NotFound hop_count as
                // "exhaustion depth", not "depth to the hosting peer".
                //
                // Same clamp as SubscribeSuccess: bound by max_hops_to_live
                // to guard against malicious or buggy peer values.
                let max_htl = op_manager.ring.max_hops_to_live;
                EventKind::Subscribe(SubscribeEvent::SubscribeNotFound {
                    id: *id,
                    requester: this_peer.clone(),
                    instance_id: *instance_id,
                    target: this_peer,
                    hop_count: Some((*hop_count).min(max_htl)),
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
            NetMessageV1::Update(UpdateMsg::BroadcastTo {
                payload, key, id, ..
            }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                // Convert payload to WrappedState for telemetry
                let value = match payload {
                    crate::message::DeltaOrFullState::FullState(bytes) => {
                        WrappedState::from(bytes.clone())
                    }
                    crate::message::DeltaOrFullState::Delta(bytes) => {
                        WrappedState::from(bytes.clone())
                    }
                };
                EventKind::Update(UpdateEvent::BroadcastReceived {
                    id: *id,
                    requester: this_peer.clone(),
                    key: *key,
                    value,
                    target: this_peer, // We are the target
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    state_hash: None, // Hash not available from message
                })
            }
            NetMessageV1::Subscribe(SubscribeMsg::Unsubscribe { id, instance_id }) => {
                let this_peer = op_manager.ring.connection_manager.own_location();
                let from = source_addr
                    .and_then(|addr| op_manager.ring.connection_manager.get_peer_by_addr(addr))
                    .unwrap_or_else(|| this_peer.clone());
                EventKind::Subscribe(SubscribeEvent::UnsubscribeReceived {
                    id: *id,
                    instance_id: *instance_id,
                    from,
                    at: this_peer,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                })
            }
            // ForwardingAck is advisory — no telemetry event needed
            NetMessageV1::Get(GetMsg::ForwardingAck { .. })
            | NetMessageV1::Subscribe(SubscribeMsg::ForwardingAck { .. }) => EventKind::Ignored,
            NetMessageV1::Connect(_)
            | NetMessageV1::Put(_)
            | NetMessageV1::Get(_)
            | NetMessageV1::Update(_)
            | NetMessageV1::Aborted(_)
            | NetMessageV1::NeighborHosting { .. }
            | NetMessageV1::InterestSync { .. }
            | NetMessageV1::ReadyState { .. }
            | NetMessageV1::SubscribeHint(_) => EventKind::Ignored,
        };
        let own_loc = op_manager.ring.connection_manager.own_location();
        let Some(own_addr) = own_loc.socket_addr() else {
            return Either::Right(vec![]);
        };
        let peer_id = PeerId::new(own_loc.pub_key().clone(), own_addr);
        Either::Left(NetEventLog {
            tx: msg.id(),
            peer_id,
            kind,
        })
    }

    /// Create a ResyncRequestReceived event.
    ///
    /// This is emitted when we receive a ResyncRequest from a peer, indicating
    /// they failed to apply a delta we sent them. High counts may indicate
    /// incorrect summary caching (see PR #2763).
    pub fn resync_request_received(
        ring: &'a Ring,
        key: ContractKey,
        from_peer: PeerKeyLocation,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
                key,
                from_peer,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a ResyncResponseSent event.
    ///
    /// This is emitted when we send a ResyncResponse (full state) to a peer
    /// after they requested a resync due to delta application failure.
    pub fn resync_response_sent(
        ring: &'a Ring,
        key: ContractKey,
        to_peer: PeerKeyLocation,
        state_size: usize,
    ) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::InterestSync(InterestSyncEvent::ResyncResponseSent {
                key,
                to_peer,
                state_size,
                timestamp: chrono::Utc::now().timestamp() as u64,
            }),
        })
    }

    /// Create a StateConfirmed event.
    ///
    /// Emitted during Summaries handler processing to record the actual
    /// current state hash for a contract. This ensures the convergence
    /// checker has accurate data even when other telemetry events are stale.
    pub fn state_confirmed(ring: &'a Ring, key: ContractKey, state_hash: String) -> Option<Self> {
        let peer_id = Self::get_own_peer_id(ring)?;
        Some(NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::InterestSync(InterestSyncEvent::StateConfirmed { key, state_hash }),
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
                SubscribeEvent::SubscribeSuccess { .. }
                | SubscribeEvent::SubscribeNotFound { .. }
                | SubscribeEvent::SubscribeTimeout { .. },
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
            peer_id: log.peer_id,
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
            // Best-effort flush: timeout or channel error is acceptable
            let _flush_result = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
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
        GlobalExecutor::spawn(Self::record_logs(log_recv, log_file));

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
                tracing::error!("Failed opening event log file {:?}: {err}", event_log_path);
                eprintln!(
                    "CRITICAL: Failed opening event log file {:?}: {err} - event logging disabled",
                    event_log_path
                );
                // Drain the channel without logging rather than crashing the node
                while log_recv.recv().await.is_some() {}
                return;
            }
        };

        let mut ws = connect_to_metrics_server().await;

        loop {
            let ws_recv = if let Some(ws) = &mut ws {
                ws.next().boxed()
            } else {
                futures::future::pending().boxed()
            };
            crate::deterministic_select! {
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
                            // Signal completion; receiver may have timed out
                            #[allow(clippy::let_underscore_must_use)]
                            let _ = reply.send(());
                        }
                    }
                },
                ws_msg = ws_recv => {
                    if let Some((ws, ws_msg)) = ws.as_mut().zip(ws_msg) {
                        received_from_metrics_server(ws, ws_msg).await;
                    }
                },
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
                if let Err(e) = self.log_sender.send(EventLogCommand::Log(log_msg)).await {
                    tracing::debug!(error = %e, "event log channel closed");
                    break;
                }
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(
        &mut self,
        tx: Transaction,
        op_type: &str,
        target_peer: Option<String>,
    ) -> BoxFuture<'_, ()> {
        let log_msg = NetLogMessage {
            tx,
            datetime: Utc::now(),
            peer_id: PeerId::random(), // Timeout events don't have a specific peer context
            kind: EventKind::Timeout {
                id: tx,
                timestamp: chrono::Utc::now().timestamp() as u64,
                op_type: op_type.to_string(),
                target_peer,
            },
        };
        let sender = self.log_sender.clone();
        async move {
            if let Err(e) = sender.send(EventLogCommand::Log(log_msg)).await {
                tracing::debug!(error = %e, "event log channel closed during timeout notification");
            }
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
                    this_peer.pub_key().clone(),
                    this_peer
                        .socket_addr()
                        .expect("this peer should have address"),
                );
                let connected_id = PeerId::new(
                    connected_peer.pub_key().clone(),
                    connected_peer
                        .socket_addr()
                        .expect("connected peer should have address"),
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
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
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
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
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
                let at_id = PeerId::new(at.pub_key().clone(), at_addr);
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
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
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
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
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
        EventKind::Connect(_)
        | EventKind::Put(_)
        | EventKind::Get(_)
        | EventKind::Subscribe(_)
        | EventKind::Route(_)
        | EventKind::Update(_)
        | EventKind::Transfer(_)
        | EventKind::Lifecycle(_)
        | EventKind::Ignored
        | EventKind::Timeout { .. }
        | EventKind::TransportSnapshot(_)
        | EventKind::InterestSync(_)
        | EventKind::RoutingDecision(_)
        | EventKind::RouterSnapshot(_) => Ok(()),
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
            if let Err(e) = ws_stream.send(Message::Pong(ping)).await {
                tracing::debug!(error = %e, "failed to send pong to metrics server");
            }
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
        Context, KeyValue, global,
        trace::{self, Span, TraceContextExt},
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
            // opentelemetry 0.32 removed the `trace_id`/`span_id` fields from
            // `SpanBuilder`; trace identity is now seeded from the parent
            // `Context`. We anchor the span on a deterministic remote
            // `SpanContext` derived from the transaction bytes so all events of
            // a transaction continue to share a stable trace_id. The child span
            // receives a fresh span_id from the SDK id generator (no longer
            // settable through the public API), with our deterministic span_id
            // recorded as the parent span_id.
            let parent_span_context = trace::SpanContext::new(
                trace::TraceId::from_bytes(tx_bytes),
                trace::SpanId::from_bytes(span_id),
                trace::TraceFlags::SAMPLED,
                true,
                trace::TraceState::default(),
            );
            let parent_cx = Context::current().with_remote_span_context(parent_span_context);
            let builder = trace::SpanBuilder::from_name(
                transaction.transaction_type().description().to_string(),
            )
            .with_start_time(start_time)
            .with_attributes(vec![
                KeyValue::new("transaction", transaction.to_string()),
                KeyValue::new("tx_type", transaction.transaction_type().description()),
            ]);
            let inner = tracer.build_with_context(builder, &parent_cx);
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
                    let _sent = self.log_sender.send(log_msg).await;
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(
            &mut self,
            tx: Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> BoxFuture<'_, ()> {
            async move {
                if cfg!(test) {
                    let _sent = self.finished_tx_notifier.send(tx).await;
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
        /// The operation type (e.g. "connect", "put", "get", "subscribe", "update").
        op_type: String,
        /// The target peer for the operation, if known from routing info.
        target_peer: Option<String>,
    },
    /// Periodic transport layer metrics snapshot.
    ///
    /// Emitted every N seconds (default 30s) with aggregate transport statistics.
    /// This is more efficient than per-transfer events and provides trend data.
    TransportSnapshot(crate::transport::TransportSnapshot),
    /// Interest sync events for delta-based state synchronization.
    ///
    /// Tracks ResyncRequests and ResyncResponses which indicate delta application
    /// failures. Useful for monitoring the health of the delta sync protocol.
    InterestSync(InterestSyncEvent),
    /// A routing decision snapshot: which peers were considered and why one was selected.
    ///
    /// Currently emitted via `tracing::debug!` at call sites (sync context prevents
    /// async event registration). This variant is available for future async callers
    /// that want to emit routing decisions through OTLP with sampling.
    RoutingDecision(crate::router::RoutingDecisionInfo),
    /// Periodic snapshot of the router model (isotonic regression curves, event counts).
    RouterSnapshot(crate::router::RouterSnapshotInfo),
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
    const INTEREST_SYNC: u8 = 12;
    const ROUTING_DECISION: u8 = 13;
    const ROUTER_SNAPSHOT: u8 = 14;

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
            EventKind::InterestSync(_) => Self::INTEREST_SYNC,
            EventKind::RoutingDecision(_) => Self::ROUTING_DECISION,
            EventKind::RouterSnapshot(_) => Self::ROUTER_SNAPSHOT,
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
            EventKind::Connect(_)
            | EventKind::Route(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
            EventKind::InterestSync(InterestSyncEvent::StateConfirmed { key, .. }) => Some(*key),
            EventKind::InterestSync(_) => None,
        }
    }

    /// Extracts the state hash from this event, if applicable.
    ///
    /// Returns the hash for Put and Update success/broadcast events.
    pub fn state_hash(&self) -> Option<&str> {
        match self {
            EventKind::Put(put) => put.state_hash(),
            EventKind::Update(upd) => upd.state_hash(),
            EventKind::Connect(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns the state hash only for events representing stored state changes.
    ///
    /// This is critical for convergence checking - it only returns hashes for events
    /// where state was actually stored locally:
    /// - PutSuccess: State was stored after PUT operation
    /// - UpdateSuccess: State was updated locally
    /// - BroadcastApplied: State was stored after applying received broadcast
    ///
    /// Does NOT return hashes for:
    /// - BroadcastReceived: Incoming state that may not have been applied yet
    /// - BroadcastEmitted: Outgoing state being sent to others
    ///
    /// Using `state_hash()` for convergence checking would include BroadcastReceived
    /// events, which record the incoming state hash before application, not the
    /// actual stored state after merge/application.
    pub fn stored_state_hash(&self) -> Option<&str> {
        match self {
            EventKind::Put(put) => put.stored_state_hash(),
            EventKind::Update(upd) => upd.stored_state_hash(),
            EventKind::Connect(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
            EventKind::InterestSync(InterestSyncEvent::StateConfirmed { state_hash, .. }) => {
                Some(state_hash.as_str())
            }
            EventKind::InterestSync(_) => None,
        }
    }

    /// Returns true if this is an Update BroadcastEmitted event.
    pub fn is_update_broadcast_emitted(&self) -> bool {
        matches!(
            self,
            EventKind::Update(UpdateEvent::BroadcastEmitted { .. })
        )
    }

    /// Returns router snapshot summary data for `RouterSnapshot` events.
    ///
    /// Returns `(failure_events, success_events, prediction_active)`.
    /// `failure_events` counts all events in the failure estimator (successes scored 0.0,
    /// failures scored 1.0). `success_events` counts only timed GET successes.
    /// `prediction_active` is true when `failure_events >= 50`.
    pub fn router_snapshot_summary(&self) -> Option<(usize, usize, bool)> {
        match self {
            EventKind::RouterSnapshot(info) => Some((
                info.failure_events,
                info.success_events,
                info.prediction_active,
            )),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_) => None,
        }
    }

    /// Returns whether this is a route outcome event, and if so, whether it succeeded.
    ///
    /// Returns `Some(true)` for `RouteOutcome::Success` or `SuccessUntimed`,
    /// `Some(false)` for `RouteOutcome::Failure`, `None` for non-Route events.
    pub fn route_outcome_is_success(&self) -> Option<bool> {
        match self {
            EventKind::Route(re) => Some(match re.outcome {
                crate::router::RouteOutcome::Success { .. }
                | crate::router::RouteOutcome::SuccessUntimed => true,
                crate::router::RouteOutcome::Failure => false,
            }),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns the outcome of a GET operation event.
    ///
    /// Returns `Some(true)` for `GetSuccess`, `Some(false)` for `GetNotFound` or `GetFailure`,
    /// `None` for all other events (including GET requests and responses).
    pub fn get_outcome(&self) -> Option<bool> {
        match self {
            EventKind::Get(GetEvent::GetSuccess { .. }) => Some(true),
            EventKind::Get(GetEvent::GetNotFound { .. } | GetEvent::GetFailure { .. }) => {
                Some(false)
            }
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns the elapsed time in milliseconds for a completed GET operation.
    ///
    /// Returns `Some(ms)` for `GetSuccess`, `GetNotFound`, or `GetFailure`,
    /// `None` for all other events.
    pub fn get_elapsed_ms(&self) -> Option<u64> {
        match self {
            EventKind::Get(GetEvent::GetSuccess { elapsed_ms, .. })
            | EventKind::Get(GetEvent::GetNotFound { elapsed_ms, .. })
            | EventKind::Get(GetEvent::GetFailure { elapsed_ms, .. }) => Some(*elapsed_ms),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns `true` if this is a ForwardingAck received event.
    pub fn is_forwarding_ack_received(&self) -> bool {
        matches!(self, EventKind::Get(GetEvent::ForwardingAckReceived { .. }))
    }

    /// Returns `true` if this is a GET response sent event (contract found and response dispatched).
    pub fn is_get_response_sent(&self) -> bool {
        matches!(self, EventKind::Get(GetEvent::ResponseSent { .. }))
    }

    /// Returns `true` if this is a GET request event.
    pub fn is_get_request(&self) -> bool {
        matches!(self, EventKind::Get(GetEvent::Request { .. }))
    }

    /// Returns the HTL carried by a GET request event, `None` for all
    /// other events.
    ///
    /// Useful for distinguishing originator-side dispatch from relay
    /// hops in test analysis: the client driver's loopback Request is
    /// registered at the originating node with `htl == max_hops_to_live`,
    /// while every relay-received Request has already been decremented
    /// (#4361 — dispatched-vs-scheduled accounting).
    // Wildcard is deliberate, mirroring `hop_count`: this accessor cares
    // about exactly one variant; new variants should not require updates.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn get_request_htl(&self) -> Option<usize> {
        match self {
            EventKind::Get(GetEvent::Request { htl, .. }) => Some(*htl),
            _ => None,
        }
    }

    /// Returns whether this is a subscribe outcome event (success or failure).
    ///
    /// Returns `Some(true)` for `SubscribeSuccess`, `Some(false)` for the
    /// failure outcomes `SubscribeNotFound` and `SubscribeTimeout` (#3445),
    /// `None` for all other events (including subscribe requests/responses).
    pub fn subscribe_outcome(&self) -> Option<bool> {
        match self {
            EventKind::Subscribe(SubscribeEvent::SubscribeSuccess { .. }) => Some(true),
            EventKind::Subscribe(SubscribeEvent::SubscribeNotFound { .. })
            | EventKind::Subscribe(SubscribeEvent::SubscribeTimeout { .. }) => Some(false),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns `true` if this event is an update broadcast received by a peer.
    ///
    /// Matches `UpdateEvent::BroadcastReceived` — the moment a peer receives
    /// an update broadcast (before application). Used to verify that subscription
    /// interest is maintained across lease cycles.
    pub fn is_update_broadcast_received(&self) -> bool {
        matches!(
            self,
            EventKind::Update(UpdateEvent::BroadcastReceived { .. })
        )
    }

    /// Returns true if this is a Connect event.
    pub fn is_connect(&self) -> bool {
        matches!(self, EventKind::Connect(_))
    }

    /// Returns true if this is a Disconnected event.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, EventKind::Disconnected { .. })
    }

    /// Returns true if this is a ConnectEvent::RequestReceived where accepted=true.
    pub fn is_connect_accepted(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived { accepted: true, .. })
        )
    }

    /// Returns true if this is a ConnectEvent::RequestReceived where accepted=false.
    pub fn is_connect_not_accepted(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                accepted: false,
                ..
            })
        )
    }

    /// Returns true if this is a ConnectEvent::Connected.
    pub fn is_connect_connected(&self) -> bool {
        matches!(self, EventKind::Connect(ConnectEvent::Connected { .. }))
    }

    /// Returns true if this is a ConnectEvent::RequestSent with is_initial=true.
    pub fn is_connect_initiated(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestSent {
                is_initial: true,
                ..
            })
        )
    }

    /// Returns true if this is a terminus acceptance (accepted=true, forwarded_to=None).
    pub fn is_connect_terminus_accepted(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                accepted: true,
                forwarded_to: None,
                ..
            })
        )
    }

    /// Returns true if this is a terminus rejection (accepted=false, forwarded_to=None).
    pub fn is_connect_terminus_rejected(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                accepted: false,
                forwarded_to: None,
                ..
            })
        )
    }

    /// Returns true if this is a forwarded request (forwarded_to=Some).
    pub fn is_connect_forwarded(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                forwarded_to: Some(_),
                ..
            })
        )
    }

    /// Returns the connection count from a ConnectEvent::Connected event.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn connect_peer_connection_count(&self) -> Option<usize> {
        match self {
            EventKind::Connect(ConnectEvent::Connected {
                this_peer_connection_count,
                ..
            }) => Some(*this_peer_connection_count),
            _ => None,
        }
    }

    /// Returns the contract instance id if this is an UnsubscribeReceived event.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn unsubscribe_received_instance_id(&self) -> Option<&ContractInstanceId> {
        match self {
            EventKind::Subscribe(SubscribeEvent::UnsubscribeReceived { instance_id, .. }) => {
                Some(instance_id)
            }
            _ => None,
        }
    }

    /// Returns the contract instance id if this is an UnsubscribeSent event.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn unsubscribe_sent_instance_id(&self) -> Option<&ContractInstanceId> {
        match self {
            EventKind::Subscribe(SubscribeEvent::UnsubscribeSent { instance_id, .. }) => {
                Some(instance_id)
            }
            _ => None,
        }
    }

    /// Returns the `hop_count` recorded in this event, if any.
    ///
    /// Populated for terminal GET events (`GetSuccess`, `GetNotFound`,
    /// `GetFailure`) since PR #4245, and for terminal PUT/SUBSCRIBE
    /// events (`PutSuccess`, `SubscribeSuccess`, `SubscribeNotFound`)
    /// since PR #4248.  The value is the number of forward hops the
    /// originating request traversed before reaching its terminal state.
    /// Returns `None` for non-terminal events (e.g. `Request`) and for
    /// events that don't carry a hop_count field.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn hop_count(&self) -> Option<usize> {
        match self {
            EventKind::Get(GetEvent::GetSuccess { hop_count, .. })
            | EventKind::Get(GetEvent::GetNotFound { hop_count, .. })
            | EventKind::Get(GetEvent::GetFailure { hop_count, .. })
            | EventKind::Put(PutEvent::PutSuccess { hop_count, .. })
            | EventKind::Subscribe(SubscribeEvent::SubscribeSuccess { hop_count, .. })
            | EventKind::Subscribe(SubscribeEvent::SubscribeNotFound { hop_count, .. }) => {
                *hop_count
            }
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
            EventKind::InterestSync(_) => "InterestSync",
            EventKind::RoutingDecision(_) => "RoutingDecision",
            EventKind::RouterSnapshot(_) => "RouterSnapshot",
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
pub(crate) enum ConnectEvent {
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
    /// A ConnectRequest was rejected (sent back to upstream).
    Rejected {
        /// The target ring location that was being requested.
        desired_location: Location,
        /// Reason for rejection.
        reason: String,
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
        /// Size of the stored state in bytes.
        state_size: Option<usize>,
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
            PutEvent::Request { .. }
            | PutEvent::PutFailure { .. }
            | PutEvent::ResponseSent { .. } => None,
        }
    }

    /// Returns the state hash only for events representing stored state.
    ///
    /// Only returns hash for PutSuccess (state actually stored locally).
    /// Does NOT return hash for BroadcastEmitted/BroadcastReceived (state in transit).
    fn stored_state_hash(&self) -> Option<&str> {
        match self {
            PutEvent::PutSuccess { state_hash, .. } => state_hash.as_deref(),
            PutEvent::Request { .. }
            | PutEvent::PutFailure { .. }
            | PutEvent::ResponseSent { .. }
            | PutEvent::BroadcastEmitted { .. }
            | PutEvent::BroadcastReceived { .. } => None,
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
        /// Size of the state after update in bytes.
        state_size: Option<usize>,
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
    /// Update operation failed.
    UpdateFailure {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        /// Reason for the failure.
        reason: OperationFailure,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Emitted after broadcasting completes with delta sync statistics.
    /// This provides telemetry for monitoring delta sync effectiveness.
    BroadcastComplete {
        id: Transaction,
        key: ContractKey,
        /// Number of peers that received a delta update.
        delta_sends: usize,
        /// Number of peers that received full state (no cached summary or delta failed).
        full_state_sends: usize,
        /// Total bytes saved by sending deltas instead of full state.
        /// Calculated as sum of (state_size - delta_size) for each delta send.
        bytes_saved: u64,
        /// Size of the full state in bytes.
        state_size: usize,
        timestamp: u64,
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
        /// Size of the state after applying the update in bytes.
        state_size: usize,
    },
    /// Emitted after handle_broadcast_state_change() completes with a full
    /// breakdown of why each potential peer was or was not sent the broadcast.
    /// This enables diagnosing missed broadcast deliveries (issue #3046).
    BroadcastDeliverySummary {
        key: ContractKey,
        proximity_found: usize,
        proximity_resolve_failed: usize,
        interest_found: usize,
        interest_resolve_failed: usize,
        skipped_self: usize,
        skipped_sender: usize,
        skipped_summary_match: usize,
        targets_sent: usize,
        send_failed: usize,
        timestamp: u64,
    },
}

impl UpdateEvent {
    /// Returns the contract key for this event.
    fn contract_key(&self) -> ContractKey {
        match self {
            UpdateEvent::Request { key, .. }
            | UpdateEvent::UpdateSuccess { key, .. }
            | UpdateEvent::BroadcastEmitted { key, .. }
            | UpdateEvent::BroadcastComplete { key, .. }
            | UpdateEvent::BroadcastReceived { key, .. }
            | UpdateEvent::BroadcastApplied { key, .. }
            | UpdateEvent::UpdateFailure { key, .. }
            | UpdateEvent::BroadcastDeliverySummary { key, .. } => *key,
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
            UpdateEvent::Request { .. }
            | UpdateEvent::UpdateFailure { .. }
            | UpdateEvent::BroadcastComplete { .. }
            | UpdateEvent::BroadcastDeliverySummary { .. } => None,
        }
    }

    /// Returns the state hash only for events representing stored state.
    ///
    /// Only returns hash for:
    /// - UpdateSuccess: State was updated locally
    /// - BroadcastApplied: State was stored after applying received broadcast
    ///
    /// Does NOT return hash for:
    /// - BroadcastReceived: Incoming state not yet applied
    /// - BroadcastEmitted: Outgoing state being sent
    fn stored_state_hash(&self) -> Option<&str> {
        match self {
            UpdateEvent::UpdateSuccess {
                state_hash_after, ..
            }
            | UpdateEvent::BroadcastApplied {
                state_hash_after, ..
            } => state_hash_after.as_deref(),
            UpdateEvent::Request { .. }
            | UpdateEvent::UpdateFailure { .. }
            | UpdateEvent::BroadcastEmitted { .. }
            | UpdateEvent::BroadcastComplete { .. }
            | UpdateEvent::BroadcastReceived { .. }
            | UpdateEvent::BroadcastDeliverySummary { .. } => None,
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
    /// A relay peer sent a ForwardingAck to its upstream peer.
    ///
    /// Emitted when a relay peer forwards a GET request deeper and ACKs the upstream
    /// to signal "I'm working on it". This prevents the upstream's GC task from treating
    /// the relay as dead — but also disables speculative retry (#3570).
    ForwardingAckSent {
        id: Transaction,
        /// The relay peer sending the ACK.
        from: PeerKeyLocation,
        /// The upstream peer receiving the ACK.
        to: PeerKeyLocation,
        instance_id: ContractInstanceId,
        timestamp: u64,
    },
    /// An upstream peer received a ForwardingAck from a downstream relay.
    ///
    /// When received, `ack_received` is set to `true`, which prevents the GC task
    /// from launching speculative retries. If the downstream chain then stalls
    /// (downstream peer never formally disconnects), the originator waits the full
    /// OPERATION_TTL with no recovery (#3570). Formal disconnect of the immediate
    /// downstream peer now wakes the parked driver sub-ms via
    /// `handle_orphaned_transactions` (#4154); the no-recovery window remains for
    /// silent stalls / slow-loris where no disconnect signal arrives.
    ForwardingAckReceived {
        id: Transaction,
        /// The peer that received the ACK (originator or intermediate relay).
        receiver: PeerKeyLocation,
        instance_id: ContractInstanceId,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
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
            GetEvent::Request { .. }
            | GetEvent::GetNotFound { .. }
            | GetEvent::GetFailure { .. }
            | GetEvent::ForwardingAckSent { .. }
            | GetEvent::ForwardingAckReceived { .. } => None,
        }
    }
}

/// SUBSCRIBE operation events for tracking the lifecycle of contract subscriptions.
///
/// Similar to `GetEvent` and `PutEvent`, this enum captures the full sequence of a Subscribe operation:
/// - Request initiation
/// - Success when subscription is established
/// - NotFound when contract doesn't exist after search
/// - Hosting state changes (local client subscriptions)
///
/// # Serialization Compatibility
///
/// Discriminants 6-10 are reserved for removed variants (2026-01 lease-based refactor).
/// New variants should be added after `_Reserved10` (discriminant 11+).
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
    /// A local client started hosting a contract (via WebSocket subscription).
    ///
    /// This event fires when a local application subscribes to a contract,
    /// indicating this peer is now interested in receiving updates for the contract.
    HostingStarted {
        /// Contract instance being hosted.
        instance_id: ContractInstanceId,
        timestamp: u64,
    },
    /// A local client stopped hosting a contract (last WebSocket client unsubscribed).
    ///
    /// This event fires when the last local client unsubscribes from a contract,
    /// indicating this peer no longer has local interest in the contract.
    HostingStopped {
        /// Contract instance that is no longer being hosted locally.
        instance_id: ContractInstanceId,
        /// Reason for stopping hosting.
        reason: HostingStoppedReason,
        timestamp: u64,
    },
    // Reserved discriminants 6-10 for removed variants (2026-01 lease-based refactor).
    // These placeholders ensure old stored events with these discriminants fail
    // deserialization cleanly rather than being misinterpreted as new variants.
    // New variants should be added after _Reserved10.
    #[doc(hidden)]
    _Reserved6,
    #[doc(hidden)]
    _Reserved7,
    #[doc(hidden)]
    _Reserved8,
    #[doc(hidden)]
    _Reserved9,
    #[doc(hidden)]
    _Reserved10,
    /// An explicit Unsubscribe message was sent upstream for fast cleanup.
    UnsubscribeSent {
        id: Transaction,
        /// Contract instance being unsubscribed from.
        instance_id: ContractInstanceId,
        /// The peer sending the unsubscribe.
        from: PeerKeyLocation,
        /// The upstream peer receiving the unsubscribe.
        to: PeerKeyLocation,
        timestamp: u64,
    },
    /// An explicit Unsubscribe message was received from a downstream peer.
    UnsubscribeReceived {
        id: Transaction,
        /// Contract instance being unsubscribed from.
        instance_id: ContractInstanceId,
        /// The downstream peer that sent the unsubscribe.
        from: PeerKeyLocation,
        /// This peer (the upstream that received it).
        at: PeerKeyLocation,
        timestamp: u64,
    },
    /// A client-initiated Subscribe operation gave up without a terminal
    /// reply (every candidate peer timed out / errored before any of them
    /// returned Subscribed or NotFound). This is a terminal outcome, like
    /// `SubscribeSuccess`/`SubscribeNotFound`, but distinct because the
    /// originator never heard back from the network at all.
    ///
    /// Issue #3445: without this event a timed-out subscribe left a
    /// `subscribe_request` on the dashboard with no paired outcome, making
    /// the failure invisible (the River container contract showed 196
    /// requests and 0 outcomes — all silent timeouts).
    SubscribeTimeout {
        id: Transaction,
        /// The peer that initiated the subscribe (this node).
        requester: PeerKeyLocation,
        /// Contract instance that was being subscribed to (the full key is
        /// not known on the originator until a successful Subscribed reply).
        instance_id: ContractInstanceId,
        /// Number of routing rounds attempted before giving up.
        retries: usize,
        /// Time elapsed since the operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
}

impl SubscribeEvent {
    /// Returns the contract key for this event if available.
    /// Only some variants have the full key; Request and some others have instance_id.
    fn contract_key(&self) -> Option<ContractKey> {
        match self {
            SubscribeEvent::SubscribeSuccess { key, .. } => Some(*key),
            SubscribeEvent::ResponseSent { key, .. } => *key,
            SubscribeEvent::Request { .. }
            | SubscribeEvent::SubscribeNotFound { .. }
            | SubscribeEvent::HostingStarted { .. }
            | SubscribeEvent::HostingStopped { .. }
            | SubscribeEvent::_Reserved6
            | SubscribeEvent::_Reserved7
            | SubscribeEvent::_Reserved8
            | SubscribeEvent::_Reserved9
            | SubscribeEvent::_Reserved10
            | SubscribeEvent::UnsubscribeSent { .. }
            | SubscribeEvent::UnsubscribeReceived { .. }
            | SubscribeEvent::SubscribeTimeout { .. } => None,
        }
    }
}

/// Reason why local hosting stopped for a contract.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum HostingStoppedReason {
    /// Last local client unsubscribed.
    LastClientUnsubscribed,
    /// Client disconnected.
    ClientDisconnected,
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

/// Interest sync events for delta-based state synchronization.
///
/// These events track the interest sync protocol operations, particularly
/// ResyncRequests which indicate delta application failures. High ResyncRequest
/// counts may indicate incorrect summary caching (see PR #2763).
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum InterestSyncEvent {
    /// A ResyncRequest was received from a peer.
    ///
    /// This indicates the peer failed to apply a delta we sent them,
    /// likely due to version mismatch. We respond with full state.
    ResyncRequestReceived {
        /// Contract for which resync was requested.
        key: ContractKey,
        /// The peer that sent the ResyncRequest.
        from_peer: PeerKeyLocation,
        /// Timestamp of the event.
        timestamp: u64,
    },
    /// A ResyncResponse (full state) was sent to a peer.
    ///
    /// This is the response to a ResyncRequest, providing the peer
    /// with our full state so they can recover from the delta failure.
    ResyncResponseSent {
        /// Contract for which resync response was sent.
        key: ContractKey,
        /// The peer we sent the response to.
        to_peer: PeerKeyLocation,
        /// Size of the state sent (bytes).
        state_size: usize,
        /// Timestamp of the event.
        timestamp: u64,
    },
    /// Periodic confirmation of a contract's current state hash.
    ///
    /// Emitted by the Summaries handler during interest-sync heartbeat
    /// processing. This ensures the convergence checker has up-to-date
    /// state hashes even when state changes occur through code paths
    /// that don't emit PutSuccess/UpdateSuccess/BroadcastApplied events
    /// (e.g., CRDT merge with version-based comparison).
    StateConfirmed {
        /// Contract whose state was confirmed.
        key: ContractKey,
        /// Blake3 hash of the current state (hex string).
        state_hash: String,
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

    /// Number of hours to keep log files (using hourly rotation).
    /// At typical gateway log rates (~500KB/hour), 72 hours ≈ 36MB.
    const LOG_RETENTION_HOURS: usize = 72;

    /// Startup-only backstop on the total bytes the freenet log directory
    /// may occupy. When log volume spikes above the steady-state assumption
    /// baked into `LOG_RETENTION_HOURS` (e.g., the executor-queue overflow
    /// in issue #4251 producing thousands of events per second), the
    /// time-based retention alone cannot bound disk usage within a single
    /// session. This cap deletes oldest-first after the time pass to bring
    /// the directory back under the limit on every restart.
    ///
    /// Caveat: enforcement runs only when the tracer initializes (i.e.,
    /// node start). A long-uptime node under sustained runaway logging can
    /// still exceed this cap until its next restart. Periodic enforcement
    /// is a candidate follow-up.
    const LOG_DIR_MAX_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

    /// Match the rolling-appender naming convention used by
    /// `RollingFileAppender::Rotation::HOURLY` for the `freenet` /
    /// `freenet.error` prefixes:
    ///
    ///   freenet.YYYY-MM-DD-HH.log
    ///   freenet.error.YYYY-MM-DD-HH.log
    ///
    /// Intentionally does NOT match:
    /// - `freenet.log` / `freenet.error.log` — legacy systemd /launchd
    ///   StandardOutput targets that the OS holds open; deleting them
    ///   leaks an unlinked-but-open inode (Linux) or errors (Windows)
    ///   and does not free disk space until restart.
    /// - `freenet.error.log.last` — transient per-launch scratch file the
    ///   macOS wrapper overwrites each iteration.
    fn is_rotating_freenet_log(name: &str) -> bool {
        // freenet.error.YYYY-MM-DD-HH.log → after stripping the prefix and
        // suffix, the remainder must be the date-hour stem. We don't parse
        // the stem strictly; cheap shape check: at least one '-' and all
        // remaining characters in [0-9-].
        let stem = if let Some(rest) = name.strip_prefix("freenet.error.") {
            rest
        } else if let Some(rest) = name.strip_prefix("freenet.") {
            rest
        } else {
            return false;
        };
        let Some(date_part) = stem.strip_suffix(".log") else {
            return false;
        };
        !date_part.is_empty()
            && date_part.contains('-')
            && date_part.chars().all(|c| c.is_ascii_digit() || c == '-')
    }

    /// Guards for non-blocking file appenders - must be kept alive for the lifetime of the program
    static LOG_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();

    /// Get the default log directory for the current platform.
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
    ///
    /// First pass: remove files older than `LOG_RETENTION_HOURS`.
    /// Second pass: if the total size of remaining `freenet*.log` files
    /// still exceeds `LOG_DIR_MAX_BYTES`, delete oldest-first until under
    /// the limit. The size cap is a backstop for runaway log rates that
    /// the time-based retention alone can't bound.
    fn cleanup_old_logs(log_dir: &std::path::Path) {
        use std::time::{Duration, SystemTime};

        let retention = Duration::from_secs(LOG_RETENTION_HOURS as u64 * 3600);
        let cutoff = SystemTime::now() - retention;

        let Ok(entries) = std::fs::read_dir(log_dir) else {
            return;
        };

        // First pass: time-based deletion, collect survivors for the
        // size-cap pass.
        let mut survivors: Vec<(std::path::PathBuf, SystemTime, u64)> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();

            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !is_rotating_freenet_log(name) {
                continue;
            }

            let Ok(metadata) = path.metadata() else {
                continue;
            };
            let Ok(modified) = metadata.modified() else {
                continue;
            };
            if modified < cutoff {
                if let Err(e) = std::fs::remove_file(&path) {
                    eprintln!("Failed to remove old log file {}: {}", path.display(), e);
                }
                continue;
            }
            survivors.push((path, modified, metadata.len()));
        }

        enforce_log_dir_size_cap(survivors, LOG_DIR_MAX_BYTES);
    }

    /// Delete oldest log files until the total size of the supplied list is
    /// at or below `max_bytes`. Mutates the filesystem; the input vector
    /// is consumed. Parameterized for test isolation.
    ///
    /// The most-recently-modified file is preserved unconditionally even
    /// when it alone exceeds `max_bytes`: it is the file currently being
    /// written by `RollingFileAppender`. On Linux, removing it would leave
    /// the appender writing to an unlinked inode (disk space not reclaimed
    /// until the next rotation); on Windows, `remove_file` would simply
    /// fail. Either way the live file should not be a cleanup target.
    fn enforce_log_dir_size_cap(
        mut files: Vec<(std::path::PathBuf, std::time::SystemTime, u64)>,
        max_bytes: u64,
    ) {
        let total: u64 = files.iter().map(|(_, _, size)| *size).sum();
        if total <= max_bytes {
            return;
        }

        // Oldest first; remove from this end and stop before the newest.
        files.sort_by_key(|(_, modified, _)| *modified);
        let live = files.pop(); // newest mtime — never deleted
        let live_size = live.as_ref().map(|(_, _, size)| *size).unwrap_or(0);
        let mut non_live_remaining: u64 = files.iter().map(|(_, _, size)| *size).sum();

        for (path, _, size) in files {
            // Final on-disk size after additional deletions =
            //   live_size + non_live_remaining (decreasing each loop).
            if live_size.saturating_add(non_live_remaining) <= max_bytes {
                break;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    non_live_remaining = non_live_remaining.saturating_sub(size);
                }
                Err(e) => {
                    eprintln!(
                        "Failed to enforce log dir size cap on {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }
    }

    pub fn init_tracer(
        level: Option<LevelFilter>,
        _endpoint: Option<String>,
        log_dir: Option<&std::path::Path>,
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
        let use_json = std::env::var("FREENET_LOG_FORMAT")
            .map(|v| v.eq_ignore_ascii_case("json"))
            .unwrap_or(false);

        // Determine if we should write to files:
        // - Always write to files when a log directory is available (ensures diagnostic reports work)
        // - Can be disabled with FREENET_LOG_TO_STDERR (uses stderr instead)
        // - The FREENET_DISABLE_LOGS env var disables all logging
        //
        // Note: On Windows especially, logs must go to files because Task Scheduler
        // doesn't capture stdout, making `freenet service report` unable to collect logs.
        let use_file_logging = !to_stderr && log_dir.is_some();

        // Build filter (we'll create separate instances for each layer since filters are consumed)
        fn build_filter(default_filter: LevelFilter) -> tracing_subscriber::EnvFilter {
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(default_filter.into())
                .from_env_lossy()
                .add_directive("moka=off".parse().expect("infallible"))
                .add_directive("sqlx=error".parse().expect("infallible"))
        }

        let filter_layer = build_filter(default_filter);

        // Also output to console when running interactively (stdout is a terminal)
        // This restores the expected console output while keeping file logging for diagnostic reports
        let also_log_to_console = std::io::stdout().is_terminal();

        // Get rate limit from environment or use default (1000 events/sec)
        let rate_limit: u64 = std::env::var("FREENET_LOG_RATE_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(crate::util::rate_limit_layer::DEFAULT_MAX_EVENTS_PER_SECOND);

        // Per-callsite cap (issue #4251 follow-up). Stops a single misbehaving
        // tracing macro from dominating the log even when its rate stays
        // below the global aggregate cap. Configurable via
        // FREENET_LOG_RATE_LIMIT_PER_CALLSITE.
        let per_callsite_limit: u64 = std::env::var("FREENET_LOG_RATE_LIMIT_PER_CALLSITE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(crate::util::rate_limit_layer::DEFAULT_MAX_EVENTS_PER_CALLSITE_PER_SECOND);

        // Rate limiting is disabled in tests and debug builds to avoid masking issues
        let rate_limit_enabled = !cfg!(any(test, debug_assertions))
            && std::env::var("FREENET_DISABLE_LOG_RATE_LIMIT").is_err();

        // Create rate limiters (shared across all layers)
        let rate_limiter = if rate_limit_enabled {
            Some(crate::util::rate_limit_layer::RateLimiter::new(rate_limit))
        } else {
            None
        };
        let per_callsite_limiter = if rate_limit_enabled {
            Some(crate::util::rate_limit_layer::PerCallsiteRateLimiter::new(
                per_callsite_limit,
            ))
        } else {
            None
        };

        if use_file_logging {
            if let Some(log_dir) = log_dir {
                // Create log directory if it doesn't exist
                if let Err(e) = std::fs::create_dir_all(log_dir) {
                    eprintln!("Warning: Failed to create log directory: {e}");
                    // Fall back to stdout logging
                    return init_stdout_tracer(
                        default_filter,
                        to_stderr,
                        use_json,
                        filter_layer,
                        rate_limiter,
                        per_callsite_limiter,
                    );
                }

                // Clean up old log files (including legacy daily logs) on startup
                cleanup_old_logs(log_dir);

                // Create rolling file appender for main log (hourly rotation)
                let main_appender = RollingFileAppender::builder()
                    .rotation(Rotation::HOURLY)
                    .max_log_files(LOG_RETENTION_HOURS)
                    .filename_prefix("freenet")
                    .filename_suffix("log")
                    .build(log_dir)
                    .map_err(|e| anyhow::anyhow!("Failed to create log appender: {e}"))?;

                // Create rolling file appender for error log (hourly rotation)
                let error_appender = RollingFileAppender::builder()
                    .rotation(Rotation::HOURLY)
                    .max_log_files(LOG_RETENTION_HOURS)
                    .filename_prefix("freenet.error")
                    .filename_suffix("log")
                    .build(log_dir)
                    .map_err(|e| anyhow::anyhow!("Failed to create error log appender: {e}"))?;

                let (main_writer, main_guard) = tracing_appender::non_blocking(main_appender);
                let (error_writer, error_guard) = tracing_appender::non_blocking(error_appender);

                // Store guards to keep writers alive; fail if already initialized
                if LOG_GUARDS.set(vec![main_guard, error_guard]).is_err() {
                    return Err(anyhow::anyhow!(
                        "LOG_GUARDS already initialized; tracer cannot be re-initialized"
                    ));
                }

                // Apply rate limiting as a global filter if enabled.
                //
                // We MUST use `DynFilterFn` here, NOT `filter_fn`. The latter
                // assumes the closure is callsite-cacheable (no Context arg)
                // and so calls `callsite_enabled` ONCE per callsite, caching
                // the first result as `Interest::always`/`never`. That makes
                // every stateful rate-limit filter a no-op for the second and
                // subsequent events from the same macro — exactly the bug
                // that let issue #4251 spam slip past the pre-existing global
                // `RateLimiter`. `DynFilterFn` defaults to `Interest::sometimes`,
                // so `enabled` is invoked per event. (Caught by codex review on
                // PR #4273 — see the PR thread.)
                if let Some(rate_limiter) = rate_limiter.clone() {
                    let per_callsite = per_callsite_limiter.clone();
                    let rate_filter =
                        tracing_subscriber::filter::DynFilterFn::new(move |meta, _cx| {
                            per_callsite
                                .as_ref()
                                .map(|pc| pc.should_allow(meta))
                                .unwrap_or(true)
                                && rate_limiter.should_allow()
                        });
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

                    // Add console layer if running interactively
                    if also_log_to_console {
                        let console_filter = build_filter(default_filter);
                        let console_layer = tracing_subscriber::fmt::layer()
                            .with_level(true)
                            .pretty()
                            .with_filter(console_filter);

                        let subscriber =
                            base.with(main_layer).with(error_layer).with(console_layer);
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("Error setting subscriber");
                    } else {
                        let subscriber = base.with(main_layer).with(error_layer);
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("Error setting subscriber");
                    }
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

                    // Add console layer if running interactively
                    if also_log_to_console {
                        let console_filter = build_filter(default_filter);
                        let console_layer = tracing_subscriber::fmt::layer()
                            .with_level(true)
                            .pretty()
                            .with_filter(console_filter);

                        let subscriber = Registry::default()
                            .with(main_layer)
                            .with(error_layer)
                            .with(console_layer);
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("Error setting subscriber");
                    } else {
                        let subscriber = Registry::default().with(main_layer).with(error_layer);
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("Error setting subscriber");
                    }
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
            per_callsite_limiter,
        )
    }

    fn init_stdout_tracer(
        _default_filter: LevelFilter,
        to_stderr: bool,
        use_json: bool,
        filter_layer: tracing_subscriber::EnvFilter,
        rate_limiter: Option<crate::util::rate_limit_layer::RateLimiter>,
        per_callsite_limiter: Option<crate::util::rate_limit_layer::PerCallsiteRateLimiter>,
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

        // Apply rate limiting as a global filter if enabled.
        // See the equivalent block in `init_tracer` above for why this MUST
        // use `DynFilterFn` rather than `filter_fn`.
        if let Some(rate_limiter) = rate_limiter {
            let per_callsite = per_callsite_limiter.clone();
            let rate_filter = tracing_subscriber::filter::DynFilterFn::new(move |meta, _cx| {
                per_callsite
                    .as_ref()
                    .map(|pc| pc.should_allow(meta))
                    .unwrap_or(true)
                    && rate_limiter.should_allow()
            });
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

    #[cfg(test)]
    mod cleanup_tests {
        use super::{cleanup_old_logs, enforce_log_dir_size_cap};
        use std::fs;
        use std::time::{Duration, SystemTime};

        /// Writes `path` with `size` bytes and sets its mtime to `mtime`.
        fn write_with_mtime(path: &std::path::Path, size: usize, mtime: SystemTime) {
            fs::write(path, vec![b'.'; size]).unwrap();
            let times = std::fs::FileTimes::new().set_modified(mtime);
            let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            f.set_times(times).unwrap();
        }

        /// Regression for issue #4251: when log volume blows past the
        /// time-based retention's implicit assumption (~500 KB/h), the
        /// size cap must delete oldest-first until the directory is
        /// under the supplied limit.
        #[test]
        fn size_cap_deletes_oldest_first_until_under_limit() {
            let dir = tempfile::tempdir().unwrap();
            let now = SystemTime::now();

            let oldest = dir.path().join("freenet.2026-05-25-12.log");
            let middle = dir.path().join("freenet.2026-05-25-13.log");
            let newest = dir.path().join("freenet.2026-05-25-14.log");

            // 4 KiB each; total 12 KiB. Cap at 8 KiB → oldest must go.
            write_with_mtime(&oldest, 4096, now - Duration::from_secs(3600));
            write_with_mtime(&middle, 4096, now - Duration::from_secs(60));
            write_with_mtime(&newest, 4096, now - Duration::from_secs(30));

            let files = vec![
                (oldest.clone(), now - Duration::from_secs(3600), 4096),
                (middle.clone(), now - Duration::from_secs(60), 4096),
                (newest.clone(), now - Duration::from_secs(30), 4096),
            ];
            enforce_log_dir_size_cap(files, 8192);

            assert!(
                !oldest.exists(),
                "oldest file should be deleted by size cap"
            );
            assert!(middle.exists(), "middle file should survive");
            assert!(newest.exists(), "newest file should survive");
        }

        /// Under-cap directories must not lose any files.
        #[test]
        fn size_cap_is_noop_when_under_limit() {
            let dir = tempfile::tempdir().unwrap();
            let now = SystemTime::now();
            let small = dir.path().join("freenet.2026-05-25-15.log");
            write_with_mtime(&small, 1024, now);

            let files = vec![(small.clone(), now, 1024)];
            enforce_log_dir_size_cap(files, 1024 * 1024 * 1024);

            assert!(small.exists(), "file under cap must survive");
        }

        /// The time-based pass in `cleanup_old_logs` still removes files
        /// older than the retention window, even when total size is under
        /// the cap.
        #[test]
        fn time_pass_removes_files_older_than_retention() {
            let dir = tempfile::tempdir().unwrap();
            // 100 days old, 1 KiB — well under size cap but past time cap.
            let ancient = dir.path().join("freenet.2026-02-14-00.log");
            write_with_mtime(
                &ancient,
                1024,
                SystemTime::now() - Duration::from_secs(100 * 24 * 3600),
            );

            cleanup_old_logs(dir.path());

            assert!(
                !ancient.exists(),
                "ancient file must be removed by time pass"
            );
        }

        /// Non-`freenet*` files in the same directory must be ignored.
        #[test]
        fn cleanup_ignores_non_freenet_files() {
            let dir = tempfile::tempdir().unwrap();
            let other = dir.path().join("other.log");
            fs::write(&other, b"unrelated").unwrap();

            cleanup_old_logs(dir.path());

            assert!(other.exists(), "non-freenet files must not be touched");
        }

        /// The size cap must NEVER delete the most-recently-modified file
        /// (the live file the rolling appender is currently writing to).
        /// Removing it would leave the appender writing to an unlinked inode
        /// on Linux, or fail on Windows. Regression for review findings on
        /// issue #4251.
        #[test]
        fn size_cap_preserves_most_recently_modified_file() {
            let dir = tempfile::tempdir().unwrap();
            let now = SystemTime::now();

            // Single oversized file — also the newest. Must NOT be deleted.
            let live = dir.path().join("freenet.2026-05-25-18.log");
            write_with_mtime(&live, 16 * 1024, now);

            let files = vec![(live.clone(), now, 16 * 1024)];
            enforce_log_dir_size_cap(files, 1024); // cap below file size

            assert!(
                live.exists(),
                "live file must survive even when alone it exceeds the cap"
            );
        }

        /// Even with the live file preserved, older files must be deleted
        /// to bring the total down. Regression for the live-file fix
        /// composing correctly with the eviction loop.
        #[test]
        fn size_cap_deletes_oldest_but_keeps_live() {
            let dir = tempfile::tempdir().unwrap();
            let now = SystemTime::now();

            // Two oversized files: cap at 5 KiB, live=4 KiB, old=4 KiB,
            // total 8 KiB → old gets deleted, live survives, final = 4 KiB.
            let old = dir.path().join("freenet.2026-05-25-12.log");
            let live = dir.path().join("freenet.2026-05-25-18.log");
            write_with_mtime(&old, 4096, now - Duration::from_secs(3600));
            write_with_mtime(&live, 4096, now);

            let files = vec![
                (old.clone(), now - Duration::from_secs(3600), 4096),
                (live.clone(), now, 4096),
            ];
            enforce_log_dir_size_cap(files, 5120);

            assert!(!old.exists(), "older file must be deleted");
            assert!(live.exists(), "live file must survive");
        }

        /// `cleanup_old_logs` must NOT touch the legacy bare
        /// `freenet.log` / `freenet.error.log` paths — systemd/launchd
        /// hold them open and deletion leaks the inode (Linux) or fails
        /// (Windows). Only the rolling-appender date-suffixed files are
        /// eligible. Regression for review findings on issue #4251.
        #[test]
        fn cleanup_skips_legacy_bare_freenet_log_names() {
            let dir = tempfile::tempdir().unwrap();
            // Make these old so they'd be deleted by the time pass if it
            // applied to them.
            let bare = dir.path().join("freenet.log");
            let bare_err = dir.path().join("freenet.error.log");
            let scratch = dir.path().join("freenet.error.log.last");
            for p in [&bare, &bare_err, &scratch] {
                write_with_mtime(
                    p,
                    1024,
                    SystemTime::now() - Duration::from_secs(30 * 24 * 3600),
                );
            }

            cleanup_old_logs(dir.path());

            assert!(
                bare.exists(),
                "legacy freenet.log must not be deleted (systemd-owned)"
            );
            assert!(
                bare_err.exists(),
                "legacy freenet.error.log must not be deleted (systemd-owned)"
            );
            assert!(
                scratch.exists(),
                "transient freenet.error.log.last must not be deleted (wrapper-owned)"
            );
        }
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
                log.peer_id.pub_key() == peer
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
                            PeerId::new(connected.pub_key().clone(), connected.socket_addr()?);
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
                peer_id,
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

        fn notify_of_time_out(
            &mut self,
            _: Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> BoxFuture<'_, ()> {
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
                    this: peer_id.as_peer_key_location(),
                    connected: other.as_peer_key_location(),
                    elapsed_ms: None,
                    connection_type: ConnectionType::Direct,
                    latency_ms: None,
                    this_peer_connection_count: 0,
                    initiated_by: None,
                }),
            }))
        }));

        futures::future::join_all(futs).await;

        let distances: Vec<_> = listener.connections(peer_id.pub_key()).collect();
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

/// Per-attempt-transaction GET outcome summary (#4361).
///
/// The raw event stream multi-counts GET outcomes:
///
/// - a failed attempt registers a `GetNotFound` TWICE on the originator's
///   own node — once directly from the relay driver's exhaustion branch
///   and once when the loopback `Response{NotFound}` re-enters inbound
///   dispatch (`from_inbound_msg_v1`);
/// - multi-hop responses register one outcome event at every hop they
///   bubble through, so a single terminal outcome can appear N times.
///
/// Counting raw events therefore measures message traversal, not
/// operation outcomes. Grouping by attempt transaction — with success
/// dominating any co-registered failure events — yields exactly one
/// outcome per attempt.
///
/// Per-tx classification precedence: success > failure (any
/// `GetFailure` event) > timeout (max elapsed >=
/// [`GET_TIMEOUT_CLASSIFICATION_MS`]) > not_found.
///
/// Semantics caveat: these are WIRE-level attempt outcomes, not
/// client-visible outcomes. A `Found` that bubbles up after the
/// originator's per-attempt timeout still registers `GetSuccess` for
/// that attempt tx and counts as a success here, even though the
/// client saw NotFound; conversely each failed attempt of an
/// ultimately-successful GET counts as its own not_found. Suitable for
/// reliability diagnostics; not a client-SLA metric.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GetOutcomeSummary {
    pub successes: u64,
    pub not_found: u64,
    pub failures: u64,
    pub timeouts: u64,
    /// Subset of `successes` whose wire `hop_count >= 1` — the GET
    /// actually traversed the network rather than completing on a node
    /// that already held the contract locally.
    pub network_successes: u64,
    /// Elapsed ms per successful attempt (max across the hops that
    /// registered the success — the originator registers last, with the
    /// largest elapsed). Sorted ascending for deterministic output.
    pub success_elapsed_ms: Vec<u64>,
}

impl GetOutcomeSummary {
    pub fn total(&self) -> u64 {
        self.successes + self.not_found + self.failures + self.timeouts
    }
}

/// Failed GET outcomes with elapsed time at or above this threshold are
/// classified as timeouts rather than NotFound (close to the 60s
/// `OPERATION_TTL`).
pub const GET_TIMEOUT_CLASSIFICATION_MS: u64 = 55_000;

/// Summarize GET outcomes from an event log, deduplicated per attempt
/// transaction. See [`GetOutcomeSummary`] for why raw event counting is
/// wrong.
// Wildcard is deliberate, mirroring `hop_count`: only the three terminal
// GET variants matter here; new variants should not require updates.
#[allow(clippy::wildcard_enum_match_arm)]
pub fn summarize_get_outcomes_per_tx(logs: &[NetLogMessage]) -> GetOutcomeSummary {
    use std::collections::HashMap;

    #[derive(Default)]
    struct TxAgg {
        success: bool,
        success_elapsed: Option<u64>,
        max_hop: Option<usize>,
        saw_failure: bool,
        max_failure_elapsed: Option<u64>,
    }

    let mut per_tx: HashMap<Transaction, TxAgg> = HashMap::new();
    for log in logs {
        // Match variants directly rather than going through
        // `get_outcome()`, which collapses `GetNotFound` and `GetFailure`
        // into the same bucket — classifying genuine network/system
        // failures as contract absence (Codex review of #4364).
        let agg = match &log.kind {
            EventKind::Get(GetEvent::GetSuccess { .. }) => {
                let agg = per_tx.entry(log.tx).or_default();
                agg.success = true;
                if let Some(ms) = log.kind.get_elapsed_ms() {
                    agg.success_elapsed = Some(agg.success_elapsed.map_or(ms, |cur| cur.max(ms)));
                }
                if let Some(hops) = log.kind.hop_count() {
                    agg.max_hop = Some(agg.max_hop.map_or(hops, |cur| cur.max(hops)));
                }
                continue;
            }
            EventKind::Get(GetEvent::GetNotFound { .. }) => per_tx.entry(log.tx).or_default(),
            EventKind::Get(GetEvent::GetFailure { .. }) => {
                let agg = per_tx.entry(log.tx).or_default();
                agg.saw_failure = true;
                agg
            }
            _ => continue,
        };
        if let Some(ms) = log.kind.get_elapsed_ms() {
            agg.max_failure_elapsed = Some(agg.max_failure_elapsed.map_or(ms, |cur| cur.max(ms)));
        }
    }

    let mut summary = GetOutcomeSummary::default();
    for agg in per_tx.values() {
        if agg.success {
            summary.successes += 1;
            if agg.max_hop.unwrap_or(0) >= 1 {
                summary.network_successes += 1;
            }
            if let Some(ms) = agg.success_elapsed {
                summary.success_elapsed_ms.push(ms);
            }
        } else if agg.saw_failure {
            summary.failures += 1;
        } else if let Some(ms) = agg.max_failure_elapsed {
            if ms >= GET_TIMEOUT_CLASSIFICATION_MS {
                summary.timeouts += 1;
            } else {
                summary.not_found += 1;
            }
        } else {
            // Unreachable today (all three terminal GET events carry
            // elapsed_ms) but kept as a defensive bucket.
            summary.failures += 1;
        }
    }
    summary.success_elapsed_ms.sort_unstable();
    summary
}

#[cfg(test)]
mod get_outcome_summary_tests {
    use super::*;
    use crate::operations::get::GetMsg;
    use crate::ring::PeerKeyLocation;
    use crate::transport::TransportPublicKey;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
    use std::net::SocketAddr;

    fn make_peer_id(port: u16) -> PeerId {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let key = TransportPublicKey::from_bytes([port as u8; 32]);
        PeerId::new(key, addr)
    }

    fn make_pkl(port: u16) -> PeerKeyLocation {
        let key = TransportPublicKey::from_bytes([port as u8; 32]);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        PeerKeyLocation::new(key, addr)
    }

    fn make_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn base_time() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn not_found_event(tx: Transaction, port: u16, elapsed_ms: u64) -> NetLogMessage {
        NetLogMessage {
            tx,
            datetime: base_time(),
            peer_id: make_peer_id(port),
            kind: EventKind::Get(GetEvent::GetNotFound {
                id: tx,
                requester: make_pkl(port),
                instance_id: *make_key().id(),
                target: make_pkl(port),
                hop_count: Some(0),
                elapsed_ms,
                timestamp: 100,
            }),
        }
    }

    fn success_event(
        tx: Transaction,
        port: u16,
        hop_count: Option<usize>,
        elapsed_ms: u64,
    ) -> NetLogMessage {
        NetLogMessage {
            tx,
            datetime: base_time(),
            peer_id: make_peer_id(port),
            kind: EventKind::Get(GetEvent::GetSuccess {
                id: tx,
                requester: make_pkl(port),
                target: make_pkl(port),
                key: make_key(),
                hop_count,
                elapsed_ms,
                timestamp: 100,
                state_hash: None,
            }),
        }
    }

    /// Regression for #4361: one failed GET attempt registers TWO
    /// `GetNotFound` events on the originator node (relay-direct +
    /// loopback-Response inbound). Per-tx dedup must count it once.
    #[test]
    fn failed_attempt_double_registration_counts_once() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![not_found_event(tx, 3001, 5), not_found_event(tx, 3001, 6)];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(
            summary.not_found, 1,
            "double-registered NotFound must dedup"
        );
        assert_eq!(summary.total(), 1);
    }

    /// A success bubbling through multiple hops registers one event per
    /// hop; it is still one outcome — and success dominates any
    /// co-registered NotFound on the same tx (a relay that exhausted one
    /// branch before another found the contract).
    #[test]
    fn multi_hop_success_counts_once_and_dominates() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![
            not_found_event(tx, 3003, 4),
            success_event(tx, 3002, Some(2), 10),
            success_event(tx, 3001, Some(2), 15),
        ];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.successes, 1);
        assert_eq!(summary.not_found, 0, "success must dominate per tx");
        assert_eq!(
            summary.network_successes, 1,
            "hop_count >= 1 is a network success"
        );
        assert_eq!(
            summary.success_elapsed_ms,
            vec![15],
            "originator-side (max) elapsed wins"
        );
    }

    /// hop_count == 0 means the GET completed on a node that already had
    /// the contract — counted as success but NOT as a network success.
    #[test]
    fn local_hit_is_not_a_network_success() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![success_event(tx, 3001, Some(0), 1)];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.successes, 1);
        assert_eq!(summary.network_successes, 0);
    }

    fn failure_event(tx: Transaction, port: u16, elapsed_ms: u64) -> NetLogMessage {
        NetLogMessage {
            tx,
            datetime: base_time(),
            peer_id: make_peer_id(port),
            kind: EventKind::Get(GetEvent::GetFailure {
                id: tx,
                requester: make_pkl(port),
                instance_id: *make_key().id(),
                target: make_pkl(port),
                hop_count: Some(0),
                reason: OperationFailure::ConnectionDropped,
                elapsed_ms,
                timestamp: 100,
            }),
        }
    }

    /// `GetFailure` events classify as failures — not as not_found —
    /// regardless of elapsed time. Regression for the Codex review
    /// finding on #4364: classifying by elapsed time alone collapsed
    /// genuine network/system failures into "contract absent".
    #[test]
    fn get_failure_classifies_as_failure_not_not_found() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![failure_event(tx, 3001, 10)];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.failures, 1, "GetFailure must land in failures");
        assert_eq!(summary.not_found, 0);
        assert_eq!(summary.total(), 1);
    }

    /// failure > timeout precedence: a `GetFailure` at or above the
    /// timeout threshold is still a failure — the timeout bucket is a
    /// heuristic for NotFound without an explicit reason. Pins the
    /// branch order in the per-tx fold (#4364 testing review).
    #[test]
    fn get_failure_above_timeout_threshold_stays_failure() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![failure_event(
            tx,
            3001,
            GET_TIMEOUT_CLASSIFICATION_MS + 1_000,
        )];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(
            summary.failures, 1,
            "failure must outrank the timeout heuristic"
        );
        assert_eq!(summary.timeouts, 0);
        assert_eq!(summary.total(), 1);
    }

    /// success > failure precedence on the same tx — and a mixed
    /// NotFound + Failure tx resolves to failure.
    #[test]
    fn success_dominates_failure_and_failure_dominates_not_found() {
        let tx1 = Transaction::new::<GetMsg>();
        let tx2 = Transaction::new::<GetMsg>();
        let logs = vec![
            failure_event(tx1, 3002, 5),
            success_event(tx1, 3001, Some(2), 20),
            not_found_event(tx2, 3003, 5),
            failure_event(tx2, 3003, 6),
        ];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.successes, 1, "success must dominate failure per tx");
        assert_eq!(
            summary.failures, 1,
            "failure must dominate not_found per tx"
        );
        assert_eq!(summary.not_found, 0);
        assert_eq!(summary.total(), 2);
    }

    /// Failed outcomes at or above the timeout threshold classify as
    /// timeouts; distinct transactions stay distinct.
    #[test]
    fn timeout_classification_and_distinct_txs() {
        let tx1 = Transaction::new::<GetMsg>();
        let tx2 = Transaction::new::<GetMsg>();
        let logs = vec![
            not_found_event(tx1, 3001, GET_TIMEOUT_CLASSIFICATION_MS),
            not_found_event(tx2, 3002, 10),
        ];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.timeouts, 1);
        assert_eq!(summary.not_found, 1);
        assert_eq!(summary.total(), 2);
    }
}
