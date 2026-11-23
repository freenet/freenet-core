//! Bound on the memory held by GET responses this node is sending.
//!
//! A node answers a GET for a contract it holds by serializing the state (and
//! code) and queueing the bytes on the requester's connection, where they stay
//! until the transport has sent them at that connection's rate. The relay
//! dedup gate (`OpManager::active_relay_get_txs`) limits requests, not the
//! bytes of the responses queued for them.
//!
//! [`OutboundPayloadBudget`] charges each streamed response before anything is
//! sent:
//!
//! - **Node-wide**, at most `G` bytes held, where `G` is
//!   [`outbound_payload_budget_for`] the node's memory limit.
//! - **Per source**, at most `P = G / 4` bytes queued toward a source that
//!   already holds something. A source is the recipient's IPv4 address or
//!   IPv6 /64 ([`crate::operations::put::stream_admission::Source`]), so
//!   several peers behind one IPv4 address share one `P`.
//! - **Reserve**: the last `G / 4` of the node budget can be drawn only by a
//!   source with nothing outstanding, so sources that already hold responses
//!   cannot take the whole budget from a source that holds none.
//! - **Idle source**: a source holding nothing is not checked against `P`; it
//!   may be sent one response of any size that fits in the node budget it can
//!   draw on.
//! - **Oversize slot**: a response that does not fit the node budget is still
//!   admitted when its source holds nothing, no other oversize response is
//!   in flight, and it is at most [`OVERSIZE_RESPONSE_MAX_BYTES`]. This keeps
//!   a maximum-size contract servable on a host whose `G` is small. The node
//!   can then hold up to `G + OVERSIZE_RESPONSE_MAX_BYTES`, with one such
//!   response at a time. Other streams do not join an oversize copy, so a
//!   host whose `G` is small serves one oversize requester at a time.
//!
//! Streamed responses share one serialized copy per (contract, state hash,
//! includes-contract) while any stream holds it
//! ([`OutboundPayloadBudget::try_reserve_shared`]). The node budget counts
//! each distinct copy once; each source is still charged the full length per
//! stream.
//!
//! A charge is owned by the bytes handed to the transport
//! (`bytes::Bytes::from_owner`), so it is returned when the last reference to
//! them is dropped: the stream finished, was abandoned, or the send was
//! dropped on the way. The memory bound therefore holds whatever the peer
//! does. How long a response is held is bounded on QUIC paths only
//! (`QuicPath::stream_deadlines`).
//!
//! A refused response is not sent. The caller returns an error, which the GET
//! driver answers upstream with `NotFound`, and the requester tries another
//! peer. A requester cannot tell a refusal from a missing contract. No wire
//! change. Refusals are counted (`freenet.get.outbound_budget_refusals`).
//!
//! Scope: only streamed GET responses (above the streaming threshold) are
//! charged here. A GET response at or below the threshold, and every other
//! outbound message or stream (PUT, UPDATE, broadcast, forwarded streams), is
//! not budgeted. Limits are per source.
//!
//! Not counted: the state and code a driver loaded before asking (dropped at
//! once on a refusal), and the packets the transport keeps for
//! retransmission.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use freenet_stdlib::prelude::ContractKey;

use crate::operations::put::stream_admission::Source;

/// The node budget as a share of the node's memory limit: one sixteenth,
/// the same share as the QUIC buffer budget
/// (`transport::quic::config::quic_buffer_budget_for`).
const OUTBOUND_PAYLOAD_BUDGET_DIVISOR: usize = 16;

/// Smallest node budget. Large enough that `P` (a quarter of it) holds a few
/// multi-megabyte responses to one source. A response larger than the room
/// this leaves is served through the oversize slot.
pub(crate) const OUTBOUND_PAYLOAD_BUDGET_MIN_BYTES: usize = 64 * 1024 * 1024;

/// The largest single response the oversize slot admits: a maximum-size state
/// (`MAX_STATE_SIZE`, 50 MiB) plus stored code and parameters.
pub(crate) const OVERSIZE_RESPONSE_MAX_BYTES: u64 = 128 * 1024 * 1024;

/// Largest node budget, reached at 8 GiB of memory.
pub(crate) const OUTBOUND_PAYLOAD_BUDGET_MAX_BYTES: usize = 512 * 1024 * 1024;

/// The node-wide budget `G` for responses being sent:
/// `memory_limit / 16`, clamped to 64..=512 MiB.
pub(crate) fn outbound_payload_budget_for(memory_limit: usize) -> usize {
    (memory_limit / OUTBOUND_PAYLOAD_BUDGET_DIVISOR).clamp(
        OUTBOUND_PAYLOAD_BUDGET_MIN_BYTES,
        OUTBOUND_PAYLOAD_BUDGET_MAX_BYTES,
    )
}

/// Memory assumed when the node's cannot be read (1 GiB), as the QUIC
/// settings assume (`config::QUIC_FALLBACK_TOTAL_RAM`).
pub(crate) const OUTBOUND_PAYLOAD_FALLBACK_TOTAL_RAM_BYTES: usize = 1024 * 1024 * 1024;

/// The per-source cap `P` for a node budget `G`: a quarter of it.
pub(crate) fn per_source_cap_for(node_budget: usize) -> usize {
    node_budget / 4
}

/// What identifies one serialized streamed GET response that any number of
/// streams may share: the same contract, the same state (by hash), and
/// whether the code is included.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SharedPayloadKey {
    pub contract: ContractKey,
    pub state_hash: [u8; 32],
    pub includes_contract: bool,
}

impl SharedPayloadKey {
    /// A key distinct for each `n` (tests only).
    #[cfg(test)]
    pub(crate) fn for_test(n: u8) -> Self {
        Self {
            contract: ContractKey::from_id_and_code(
                freenet_stdlib::prelude::ContractInstanceId::new([n; 32]),
                freenet_stdlib::prelude::CodeHash::new([n; 32]),
            ),
            state_hash: [n; 32],
            includes_contract: true,
        }
    }
}

/// Why a response was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The recipient's source already holds `P`, or the response is larger
    /// than `P` and the source holds something.
    SourceCap,
    /// The node budget (or, for a source holding something, the node budget
    /// less the reserve) has no room.
    NodeCap,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::SourceCap => f.write_str("per-source outbound payload cap reached"),
            Refusal::NodeCap => f.write_str("node outbound payload budget exhausted"),
        }
    }
}

/// The error of [`OutboundPayloadBudget::try_reserve_shared`]: refused by the
/// budget, or the caller's serialization failed.
#[derive(Debug)]
pub(crate) enum SharedReserveError<E> {
    Refused(Refusal),
    Serialize(E),
    /// `serialize` returned a different number of bytes than the length
    /// charged, so the charge would not match the memory held.
    LengthMismatch {
        charged: u64,
        produced: u64,
    },
}

/// Responses refused since the process started, by reason, for telemetry.
static REFUSED_SOURCE_TOTAL: AtomicU64 = AtomicU64::new(0);
static REFUSED_NODE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `(per-source refusals, node-wide refusals)` since the process started.
pub(crate) fn refused_totals_process() -> (u64, u64) {
    (
        REFUSED_SOURCE_TOTAL.load(Ordering::Relaxed),
        REFUSED_NODE_TOTAL.load(Ordering::Relaxed),
    )
}

/// Bytes of responses this node has serialized for sending and not yet
/// released. One per node (`OpManager::outbound_payload_budget`).
pub(crate) struct OutboundPayloadBudget {
    /// `G`. Atomics only so a test can shrink the caps of a node's budget
    /// (`set_caps_for_test`); they are fixed at construction otherwise.
    node_cap: AtomicU64,
    /// `P`.
    per_source_cap: AtomicU64,
    /// The largest response the oversize slot admits; fixed at
    /// [`OVERSIZE_RESPONSE_MAX_BYTES`] outside tests.
    oversize_max: AtomicU64,
    state: parking_lot::Mutex<State>,
    refused_source: AtomicU64,
    refused_node: AtomicU64,
}

/// One lock for the counters and the shared-copy table, so an admission
/// decision sees a consistent node total, source charge and copy.
///
/// LOCK ORDER: nothing that can drop a [`SharedCopy`] or a charge runs while
/// this lock is held, because their `Drop` takes it.
#[derive(Default)]
struct State {
    node_total: u64,
    per_source: HashMap<Source, u64>,
    shared: HashMap<SharedPayloadKey, (u64, Weak<SharedCopy>)>,
    next_copy_id: u64,
    /// The oversize slot is held by a copy now in flight.
    oversize_in_use: bool,
}

impl OutboundPayloadBudget {
    /// The budget for a node with `memory_limit` bytes of memory.
    pub(crate) fn for_memory_limit(memory_limit: usize) -> Arc<Self> {
        let node = outbound_payload_budget_for(memory_limit);
        Self::with_caps(node as u64, per_source_cap_for(node) as u64)
    }

    /// Explicit caps; the reserve is a quarter of `node_cap`.
    pub(crate) fn with_caps(node_cap: u64, per_source_cap: u64) -> Arc<Self> {
        Arc::new(Self {
            node_cap: AtomicU64::new(node_cap),
            per_source_cap: AtomicU64::new(per_source_cap),
            oversize_max: AtomicU64::new(OVERSIZE_RESPONSE_MAX_BYTES),
            state: parking_lot::Mutex::new(State::default()),
            refused_source: AtomicU64::new(0),
            refused_node: AtomicU64::new(0),
        })
    }

    pub(crate) fn per_source_cap(&self) -> u64 {
        self.per_source_cap.load(Ordering::Relaxed)
    }

    /// Set the largest response the oversize slot admits (tests only).
    #[cfg(test)]
    pub(crate) fn set_oversize_max_for_test(&self, bytes: u64) {
        self.oversize_max.store(bytes, Ordering::Relaxed);
    }

    /// Replace the caps of an idle budget (tests only).
    #[cfg(test)]
    pub(crate) fn set_caps_for_test(&self, node_cap: u64, per_source_cap: u64) {
        self.node_cap.store(node_cap, Ordering::Relaxed);
        self.per_source_cap.store(per_source_cap, Ordering::Relaxed);
    }

    #[cfg(test)]
    /// Bytes charged to the node now.
    pub(crate) fn node_total(&self) -> u64 {
        self.state.lock().node_total
    }

    #[cfg(test)]
    /// Bytes charged to `to`'s source now.
    pub(crate) fn charged_to(&self, to: SocketAddr) -> u64 {
        self.state
            .lock()
            .per_source
            .get(&Source::of(to))
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    /// Responses refused, by reason, since the node started.
    pub(crate) fn refused_totals(&self) -> (u64, u64) {
        (
            self.refused_source.load(Ordering::Relaxed),
            self.refused_node.load(Ordering::Relaxed),
        )
    }

    /// The admission rule. `source_bytes` is charged to the source,
    /// `node_bytes` to the node (zero when the bytes already exist as a
    /// shared copy). `Ok(true)` means the node charge takes the oversize slot.
    fn admit(
        &self,
        state: &State,
        source: Source,
        source_bytes: u64,
        node_bytes: u64,
    ) -> Result<bool, Refusal> {
        let node_cap = self.node_cap.load(Ordering::Relaxed);
        let held = state.per_source.get(&source).copied().unwrap_or(0);
        if held > 0 && held.saturating_add(source_bytes) > self.per_source_cap() {
            return Err(Refusal::SourceCap);
        }
        // A stream of an existing shared copy adds no node memory.
        if node_bytes == 0 {
            return Ok(false);
        }
        // An idle source may draw on the reserve (the last quarter of `G`);
        // a source holding something may not.
        let node_limit = if held == 0 {
            node_cap
        } else {
            node_cap - node_cap / 4
        };
        if state.node_total.saturating_add(node_bytes) <= node_limit {
            return Ok(false);
        }
        // No room under the ordinary rules: one oversize response at a time,
        // to a source with nothing outstanding.
        if held == 0
            && node_bytes <= self.oversize_max.load(Ordering::Relaxed)
            && !state.oversize_in_use
        {
            return Ok(true);
        }
        Err(Refusal::NodeCap)
    }

    fn count_refusal(&self, refusal: Refusal) {
        match refusal {
            Refusal::SourceCap => self.refused_source.fetch_add(1, Ordering::Relaxed),
            Refusal::NodeCap => self.refused_node.fetch_add(1, Ordering::Relaxed),
        };
        // Process-wide totals for telemetry (`tracing::otel`).
        match refusal {
            Refusal::SourceCap => &REFUSED_SOURCE_TOTAL,
            Refusal::NodeCap => &REFUSED_NODE_TOTAL,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// Refuse a response of `len` bytes to `to` that the budget has no room
    /// for, before the caller does the work of identifying it (hashing the
    /// state): the same rules as [`Self::try_reserve_shared`], assuming a
    /// shared copy only when one is held for this contract and code
    /// inclusion. `Ok` is not a reservation.
    pub(crate) fn precheck(
        &self,
        to: SocketAddr,
        contract: ContractKey,
        includes_contract: bool,
        len: u64,
    ) -> Result<(), Refusal> {
        let source = Source::of(to);
        let state = self.state.lock();
        // The same test `try_reserve_shared` applies to a copy it finds.
        // The upgraded copies are dropped after the lock is released: if one
        // is the last reference, its `Drop` takes the lock.
        let live: Vec<Arc<SharedCopy>> = state
            .shared
            .iter()
            .filter(|(key, _)| {
                key.contract == contract && key.includes_contract == includes_contract
            })
            .filter_map(|(_, (_, weak))| weak.upgrade())
            .collect();
        let candidate = live.iter().any(|copy| copy.shareable(len));
        let result = self
            .admit(&state, source, len, if candidate { 0 } else { len })
            .map(|_| ());
        drop(state);
        drop(live);
        if let Err(refusal) = result {
            self.count_refusal(refusal);
        }
        result
    }

    /// Charge a streamed response of `len` bytes to `to`'s source, sharing
    /// the serialized copy for `key` when one is held. When none is, the node
    /// is charged `len` as well and `serialize` builds the copy, outside the
    /// lock; it must produce exactly `len` bytes (bincode's
    /// `serialized_size`), and a different length is an error.
    ///
    /// The returned bytes keep the source charge, and a reference to the
    /// shared copy, for as long as any slice of them is alive.
    pub(crate) fn try_reserve_shared<E>(
        self: &Arc<Self>,
        to: SocketAddr,
        key: SharedPayloadKey,
        len: u64,
        serialize: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<bytes::Bytes, SharedReserveError<E>> {
        let source = Source::of(to);
        let mut state = self.state.lock();
        // A copy of another length is not this response. It is kept alive
        // here and dropped after the lock is released: if it is the last
        // reference, its `Drop` takes the lock.
        let found = state.shared.get(&key).and_then(|(_, weak)| weak.upgrade());
        let (existing, mismatched) = match found {
            Some(copy) if copy.shareable(len) => (Some(copy), None),
            other => (None, other),
        };
        let node_bytes = if existing.is_some() { 0 } else { len };
        let oversize = match self.admit(&state, source, len, node_bytes) {
            Ok(oversize) => oversize,
            Err(refusal) => {
                drop(state);
                drop(mismatched);
                self.count_refusal(refusal);
                return Err(SharedReserveError::Refused(refusal));
            }
        };
        *state.per_source.entry(source).or_insert(0) += len;
        state.node_total += node_bytes;
        state.oversize_in_use |= oversize;
        drop(state);
        drop(mismatched);
        let source_charge = SourceCharge {
            budget: self.clone(),
            source,
            bytes: len,
        };
        let copy = match existing {
            Some(copy) => copy,
            None => {
                let node_charge = NodeCharge {
                    budget: self.clone(),
                    bytes: len,
                    oversize,
                };
                // On an error both charges drop here and are returned.
                let bytes = serialize().map_err(SharedReserveError::Serialize)?;
                if bytes.len() as u64 != len {
                    return Err(SharedReserveError::LengthMismatch {
                        charged: len,
                        produced: bytes.len() as u64,
                    });
                }
                self.publish_copy(key, bytes, node_charge)
            }
        };
        Ok(bytes::Bytes::from_owner(ChargedStream {
            copy,
            _source: source_charge,
        }))
    }

    /// Record a freshly serialized copy for `key`, unless another stream
    /// published one while this one was serializing, in which case that one
    /// is used and this copy (and its node charge) is dropped.
    fn publish_copy(
        self: &Arc<Self>,
        key: SharedPayloadKey,
        bytes: Vec<u8>,
        node: NodeCharge,
    ) -> Arc<SharedCopy> {
        let mut state = self.state.lock();
        let len = bytes.len();
        let found = state.shared.get(&key).and_then(|(_, weak)| weak.upgrade());
        // As in `try_reserve_shared`, a copy of another length is dropped
        // only after the lock is released.
        let (raced, mismatched) = match found {
            Some(copy) if copy.shareable(len as u64) => (Some(copy), None),
            other => (None, other),
        };
        if let Some(raced) = raced {
            drop(state);
            drop(mismatched);
            drop(node);
            return raced;
        }
        let id = state.next_copy_id;
        state.next_copy_id += 1;
        let copy = Arc::new(SharedCopy {
            bytes,
            key,
            id,
            node,
        });
        // Replaces a dead entry for the same key, if any (a live one of
        // another length is `mismatched`, and its own `Drop` leaves this
        // entry alone because the id differs).
        state.shared.insert(key, (id, Arc::downgrade(&copy)));
        drop(state);
        drop(mismatched);
        copy
    }

    /// Live shared copies (test observation).
    #[cfg(test)]
    pub(crate) fn shared_copies(&self) -> usize {
        self.state
            .lock()
            .shared
            .values()
            .filter(|(_, weak)| weak.strong_count() > 0)
            .count()
    }

    /// Entries in the shared-copy table, live or not (test observation).
    #[cfg(test)]
    pub(crate) fn shared_entries(&self) -> usize {
        self.state.lock().shared.len()
    }

    /// Streams holding the shared copy for `key` (test observation).
    #[cfg(test)]
    pub(crate) fn shared_strong_count(&self, key: &SharedPayloadKey) -> usize {
        self.state
            .lock()
            .shared
            .get(key)
            .map(|(_, weak)| weak.strong_count())
            .unwrap_or(0)
    }
}

/// Bytes charged to one source, returned on drop.
struct SourceCharge {
    budget: Arc<OutboundPayloadBudget>,
    source: Source,
    bytes: u64,
}

impl Drop for SourceCharge {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        if let Some(held) = state.per_source.get_mut(&self.source) {
            *held = held.saturating_sub(self.bytes);
            if *held == 0 {
                state.per_source.remove(&self.source);
            }
        }
    }
}

/// Bytes charged to the node, returned on drop.
struct NodeCharge {
    budget: Arc<OutboundPayloadBudget>,
    bytes: u64,
    /// This charge holds the oversize slot.
    oversize: bool,
}

impl Drop for NodeCharge {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.node_total = state.node_total.saturating_sub(self.bytes);
        if self.oversize {
            state.oversize_in_use = false;
        }
    }
}

/// One serialized streamed response shared by every stream sending it. Holds
/// the node charge for its bytes; removes its table entry when the last
/// stream drops it.
struct SharedCopy {
    bytes: Vec<u8>,
    key: SharedPayloadKey,
    id: u64,
    node: NodeCharge,
}

impl SharedCopy {
    /// Whether a stream of `len` bytes may join this copy. A copy holding
    /// the oversize slot is not joined.
    fn shareable(&self, len: u64) -> bool {
        self.bytes.len() as u64 == len && !self.node.oversize
    }
}

impl Drop for SharedCopy {
    fn drop(&mut self) {
        let mut state = self.node.budget.state.lock();
        // Only this copy's own entry: a newer copy for the same key may have
        // replaced it.
        if state
            .shared
            .get(&self.key)
            .is_some_and(|(id, _)| *id == self.id)
        {
            state.shared.remove(&self.key);
        }
        // `node` drops after this, taking the lock again on its own.
    }
}

/// One stream's view of a shared copy, with its source charge.
struct ChargedStream {
    copy: Arc<SharedCopy>,
    _source: SourceCharge,
}

impl AsRef<[u8]> for ChargedStream {
    fn as_ref(&self) -> &[u8] {
        &self.copy.bytes
    }
}

#[cfg(test)]
mod tests;
