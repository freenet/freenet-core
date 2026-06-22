use std::{path::PathBuf, sync::Arc, time::SystemTime};

use chrono::{DateTime, Utc};
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{FutureExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{
    config::GlobalExecutor,
    message::{MessageStats, Transaction},
    node::PeerId,
    ring::{PeerKeyLocation, Ring},
    router::RouteEvent,
};

// Items from sibling submodules (via root re-exports) are accessible via `use super::*`.
use super::*;

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct ListenerLogId(pub(crate) usize);

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
    pub(crate) tx: &'a Transaction,
    pub(crate) peer_id: PeerId,
    pub(crate) kind: EventKind,
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
            kind: EventKind::RouterSnapshot(Box::new(snapshot)),
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
    pub(crate) fn to_log_message<'a>(
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
    // Terminal variants above complete a span; the wildcard is deliberate so
    // non-span events (and any future variant) are treated as not-completing.
    // `pub(crate)`: called from the sibling `metrics_client` module's
    // OpenTelemetry span processing. Was private, which broke the `trace-ot`
    // build after the tracing module split (see #4225).
    #[cfg(feature = "trace-ot")]
    #[allow(clippy::wildcard_enum_match_arm)]
    pub(crate) fn span_completed(&self) -> bool {
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
            EventKind::Update(
                UpdateEvent::UpdateSuccess { .. } | UpdateEvent::UpdateFailure { .. },
            ) => true,
            EventKind::Update(_) => false,
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
    // Wildcard is deliberate: only the event kinds mapped above carry
    // span attributes; every other event (and any new variant) maps to None.
    #[allow(clippy::wildcard_enum_match_arm)]
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
        // DELIBERATE blocking send (channel-safety.md "same-runtime internal
        // consumer" exception): `flush` is a shutdown/test synchronization
        // barrier — it MUST wait for `record_logs` to drain, not drop. It is
        // never called from the network event loop (only shutdown/test paths),
        // and the reply wait below is timeout-bounded. This is intentionally
        // NOT converted to `try_send` like the hot-path event-log sends.
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
pub(crate) static NEW_RECORDS_TS: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();

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

    /// Build a register around an existing sender WITHOUT spawning the
    /// `record_logs` drain task. Tests use this to saturate the bounded log
    /// channel and assert `register_events` never blocks the caller (the
    /// event-log backpressure deadlock regression).
    #[cfg(test)]
    fn from_sender_for_test(log_sender: mpsc::Sender<EventLogCommand>) -> Self {
        let flush_handle = EventFlushHandle {
            sender: log_sender.clone(),
        };
        Self {
            log_sender,
            log_file: Arc::new(PathBuf::from("event-log-no-drain-test")),
            clone_count: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            flush_handle,
        }
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

/// Count of telemetry event-log messages dropped because the bounded
/// `record_logs` channel was full. Telemetry is best-effort: dropping under
/// load keeps the node alive, whereas blocking the event loop on a stalled log
/// consumer deadlocked the entire node (every thread parked on a futex at 0%
/// CPU). Logged at power-of-two milestones so a persistent stall stays visible
/// without spamming the log.
static DROPPED_EVENT_LOGS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn note_dropped_event_log() {
    use std::sync::atomic::Ordering;
    let dropped = DROPPED_EVENT_LOGS.fetch_add(1, Ordering::Relaxed) + 1;
    if dropped.is_power_of_two() {
        tracing::warn!(
            dropped_total = dropped,
            "event log channel full; dropping telemetry event(s). The node is \
             healthy — telemetry is intentionally lossy under load rather than \
             blocking the event loop (see .claude/rules/channel-safety.md)."
        );
    }
}

#[cfg(test)]
fn dropped_event_log_count() -> u64 {
    DROPPED_EVENT_LOGS.load(std::sync::atomic::Ordering::Relaxed)
}

impl NetEventRegister for EventRegister {
    fn register_events<'a>(
        &'a self,
        logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async {
            for log_msg in NetLogMessage::to_log_message(logs) {
                // Best-effort telemetry MUST NOT block the caller. This future is
                // awaited from the network event loop's hot outbound path (see the
                // `OutboundMessageWithTarget` and disconnect handlers in
                // p2p_protoc.rs). A blocking `.send().await` on the bounded log
                // channel wedged the whole node when `record_logs` stalled on its
                // metrics WebSocket or AOF write: the channel filled, the event
                // loop blocked here forever, and every thread parked on a futex at
                // 0% CPU. Drop on full instead (see channel-safety.md).
                match self.log_sender.try_send(EventLogCommand::Log(log_msg)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => note_dropped_event_log(),
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        tracing::debug!("event log channel closed");
                        break;
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
            // Non-blocking for the same reason as `register_events`: a stalled
            // log consumer must never wedge a caller on the bounded log channel
            // (see channel-safety.md). Best-effort telemetry — drop on full.
            match sender.try_send(EventLogCommand::Log(log_msg)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => note_dropped_event_log(),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("event log channel closed during timeout notification");
                }
            }
        }
        .boxed()
    }

    fn get_router_events(&self, number: usize) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
        async move { aof::LogFile::get_router_events(number, &self.log_file).await }.boxed()
    }
}

#[cfg(test)]
mod eventlog_backpressure_tests {
    use super::*;

    /// A self-contained `Disconnected` event for backpressure tests. It borrows
    /// nothing: `Transaction::NULL` is a `&'static Transaction` and the other
    /// fields are owned, so the log is `NetEventLog<'static>`.
    fn dummy_event() -> NetEventLog<'static> {
        NetEventLog {
            tx: Transaction::NULL,
            peer_id: PeerId::random(),
            kind: EventKind::Disconnected {
                from: PeerId::random(),
                reason: DisconnectReason::RemoteDropped,
                connection_duration_ms: None,
                bytes_sent: None,
                bytes_received: None,
            },
        }
    }

    /// A capacity-1 log channel that is full but still OPEN: the single slot is
    /// pre-filled and the receiver is returned so the caller can keep it alive
    /// (dropping it would close the channel and mask the deadlock as an early
    /// `Closed` return). Returns `(register, rx_guard, filler_guard)`.
    fn saturated_register() -> (
        EventRegister,
        mpsc::Receiver<EventLogCommand>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (tx, rx) = mpsc::channel::<EventLogCommand>(1);
        let (filler_tx, filler_rx) = tokio::sync::oneshot::channel::<()>();
        tx.try_send(EventLogCommand::Flush(filler_tx))
            .expect("first slot accepts the filler");
        (EventRegister::from_sender_for_test(tx), rx, filler_rx)
    }

    /// Regression: the network event loop awaits `register_events` on the hot
    /// outbound path (`p2p_protoc.rs` `OutboundMessageWithTarget`). When
    /// `record_logs` stalled on its metrics WebSocket or AOF write, a blocking
    /// `.send().await` on the bounded log channel filled the buffer and wedged
    /// the entire node — every thread parked on a futex at 0% CPU. The send
    /// must drop on full instead of blocking. Without the fix the loop parks on
    /// the full channel; under `start_paused` the runtime then auto-advances
    /// virtual time to the 5s timeout, so the test fails deterministically
    /// (`Elapsed`) in ~0ms rather than hanging in wall-clock time.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn register_events_does_not_block_when_log_channel_full() {
        let (register, _rx, _filler) = saturated_register();

        let fire_many = async {
            for _ in 0..2_000 {
                register.register_events(Either::Left(dummy_event())).await;
            }
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), fire_many)
            .await
            .expect("register_events must never block when the log channel is full");
    }

    /// `notify_of_time_out` writes to the same bounded log channel and is also
    /// reachable from hot paths, so it must drop on full rather than block.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn notify_of_time_out_does_not_block_when_log_channel_full() {
        let (mut register, _rx, _filler) = saturated_register();

        let fire_many = async {
            for _ in 0..2_000 {
                register
                    .notify_of_time_out(*Transaction::NULL, "get", None)
                    .await;
            }
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), fire_many)
            .await
            .expect("notify_of_time_out must never block when the log channel is full");
    }

    /// The `Full` arm must count the drop so a persistent stall is observable.
    /// `DROPPED_EVENT_LOGS` is a process-global counter, so other tests in the
    /// same binary may also increment it concurrently; assert a lower bound
    /// (our K drops are always counted; concurrent drops only add to it) so the
    /// test stays order-independent under parallel execution.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn register_events_counts_dropped_messages_when_full() {
        let (register, _rx, _filler) = saturated_register();

        const K: u64 = 16;
        let before = dropped_event_log_count();
        for _ in 0..K {
            register.register_events(Either::Left(dummy_event())).await;
        }
        let after = dropped_event_log_count();
        assert!(
            after >= before + K,
            "expected >= {K} new drops counted, saw {}",
            after.saturating_sub(before)
        );
    }

    /// A closed channel must not block or panic: `register_events` returns
    /// promptly (its loop `break`s on `Closed`), exercising the non-`Full`
    /// error arm over a multi-event batch.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn register_events_handles_closed_channel() {
        let (tx, rx) = mpsc::channel::<EventLogCommand>(4);
        drop(rx); // channel now closed
        let register = EventRegister::from_sender_for_test(tx);

        let batch = vec![dummy_event(), dummy_event(), dummy_event()];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            register.register_events(Either::Right(batch)),
        )
        .await
        .expect("register_events must return promptly on a closed channel");
    }
}

#[cfg(all(test, feature = "trace-ot"))]
mod span_completed_tests {
    use super::*;

    fn msg(kind: EventKind) -> NetLogMessage {
        NetLogMessage {
            datetime: Utc::now(),
            tx: *Transaction::NULL,
            peer_id: PeerId::random(),
            kind,
        }
    }

    fn contract_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    // Regression for #4225: UpdateSuccess / UpdateFailure are terminal UPDATE
    // events that close their OpenTelemetry span. They previously fell through
    // the `_ => false` arm of `span_completed`, so their spans were never
    // marked complete and leaked under the `trace-ot` feature.
    #[test]
    fn update_terminal_events_complete_span() {
        let tx = Transaction::new::<crate::operations::update::UpdateMsg>();
        let key = contract_key();

        let success = msg(EventKind::Update(UpdateEvent::UpdateSuccess {
            id: tx,
            requester: PeerKeyLocation::random(),
            target: PeerKeyLocation::random(),
            key,
            timestamp: 0,
            state_hash_before: None,
            state_hash_after: None,
            state_size: None,
        }));
        assert!(
            success.span_completed(),
            "UpdateSuccess must close its span"
        );

        let failure = msg(EventKind::Update(UpdateEvent::UpdateFailure {
            id: tx,
            requester: PeerKeyLocation::random(),
            target: PeerKeyLocation::random(),
            key,
            reason: OperationFailure::Timeout,
            elapsed_ms: 0,
            timestamp: 0,
        }));
        assert!(
            failure.span_completed(),
            "UpdateFailure must close its span"
        );
    }

    // A non-terminal UPDATE event must NOT close the span — guards against
    // over-broadly classifying the whole `Update` group as span-completing.
    #[test]
    fn non_terminal_update_event_does_not_complete_span() {
        let tx = Transaction::new::<crate::operations::update::UpdateMsg>();

        let request = msg(EventKind::Update(UpdateEvent::Request {
            id: tx,
            requester: PeerKeyLocation::random(),
            key: contract_key(),
            target: PeerKeyLocation::random(),
            timestamp: 0,
        }));
        assert!(
            !request.span_completed(),
            "UpdateEvent::Request is not terminal and must not close the span"
        );
    }
}
