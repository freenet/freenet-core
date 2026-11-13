//! What a connection has handed to the node, kept to refuse repeats: the
//! registry that receives its inbound streams, the hashes of the messages it
//! dispatched, and the ids of the streams it completed. Moved out of
//! `PeerConnection` unchanged, so a connection with no UDP session can keep
//! the same record.

use std::net::SocketAddr;

use super::StreamId;
use super::inbound_budget::{InboundKey, SharedInboundBudget};
use crate::transport::quic::path::{InboundShort, InboundStreamArrival};

/// What became of a stream that arrived on a QUIC path.
pub(super) enum TakenStream {
    /// Its id was in use on UDP or used before: cancelled and released.
    IdInUse,
    /// It ended while it waited, or there is no registry: cancelled and
    /// released.
    Dropped,
    /// Registered with the node; `message` is its announcing message, unless
    /// there was none or it was already dispatched.
    Registered {
        stream_id: StreamId,
        message: Option<Vec<u8>>,
    },
}

pub(super) struct InboundDispatch {
    /// Registry that receives inbound operations streams, handling the race
    /// between stream fragments and metadata messages
    /// (RequestStreaming/ResponseStreaming).
    /// Set by the node layer after connection establishment.
    pub(super) inbound_stream_registry:
        Option<std::sync::Arc<dyn crate::transport::InboundStreamRegistry>>,
    /// LRU cache of recently dispatched metadata hashes (capacity 1000), used to dedup
    /// embedded-metadata-in-fragment-#1 against the separate ShortMessage.
    ///
    /// Uses `DefaultHasher` (not cryptographic). A hash collision would cause
    /// a silent metadata drop, leading to a 60-second operation timeout for
    /// the affected stream -- acceptable given the small working set and
    /// negligible collision probability. See issue #3317.
    pub(super) dispatched_msg_hashes: lru::LruCache<u64, ()>,
    /// Recently completed (and released) inbound streams, so a late
    /// retransmission does not start one again.
    pub(super) completed_inbound_streams: lru::LruCache<StreamId, ()>,
}

impl InboundDispatch {
    /// Compute a fast hash of the given bytes for dedup tracking.
    pub(super) fn msg_hash(bytes: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        h.finish()
    }

    /// Returns true if this message was already dispatched (is a duplicate).
    /// Only used on the embedded-metadata-in-fragment-#1 path to suppress
    /// duplicates when the same metadata was already dispatched as a ShortMessage.
    pub(super) fn is_duplicate_dispatch(&mut self, bytes: &[u8]) -> bool {
        self.dispatched_msg_hashes
            .put(Self::msg_hash(bytes), ())
            .is_some()
    }

    /// A Short taken from a QUIC path: its charge released and its hash
    /// remembered. Moved from `PeerConnection::take_quic_short`.
    pub(super) fn take_short(
        &mut self,
        short: InboundShort,
        budget: &SharedInboundBudget,
    ) -> Vec<u8> {
        let (payload, key) = short.into_parts();
        if let Some(key) = key {
            budget.lock().release_if_gone(key);
        }
        let bytes = Vec::from(payload);
        // Only so that the same metadata embedded in a stream is suppressed; a
        // Short itself is always delivered, as on UDP.
        self.dispatched_msg_hashes.put(Self::msg_hash(&bytes), ());
        bytes
    }

    /// A stream arrival taken from a QUIC path. Moved from
    /// `PeerConnection::take_quic_stream`; the caller records the outcome and
    /// traces the delivery.
    pub(super) fn take_stream(
        &mut self,
        arrival: InboundStreamArrival,
        budget: &SharedInboundBudget,
        remote_addr: SocketAddr,
    ) -> TakenStream {
        let InboundStreamArrival {
            stream_id,
            metadata,
            handle,
        } = arrival;
        // The stream's payload length, counted here because the reader task
        // completes the stream later, with no way to count it from there.
        // UDP counts at completion; this counts at arrival, and a stream that
        // later fails is counted too.
        let total = handle.total_bytes();
        let seen_on_udp = budget.lock().contains(InboundKey::Udp(stream_id));
        // An id in use on UDP, or used before on either path: only a faulty peer
        // or a bug sends one, and the shared budget bounds what it costs.
        if seen_on_udp || self.completed_inbound_streams.contains(&stream_id) {
            handle.cancel();
            drop(handle);
            budget.lock().release_if_gone(InboundKey::Quic(stream_id));
            tracing::debug!(
                peer_addr = %remote_addr,
                stream_id = %stream_id,
                seen_on_udp,
                "Refusing a QUIC stream whose id was already used"
            );
            return TakenStream::IdInUse;
        }
        // So a later UDP fragment with this id is refused as `AlreadyCompleted`.
        self.completed_inbound_streams.put(stream_id, ());
        // It ended while it waited in the channel. Its metadata would send an
        // operation after a stream that is gone, and the sender treats it as failed.
        if handle.is_cancelled() {
            drop(handle);
            budget.lock().release_if_gone(InboundKey::Quic(stream_id));
            return TakenStream::Dropped;
        }
        let Some(registry) = &self.inbound_stream_registry else {
            tracing::error!(
                peer_addr = %remote_addr,
                stream_id = %stream_id,
                "A QUIC stream arrived but inbound_stream_registry is None; cancelling it"
            );
            handle.cancel();
            drop(handle);
            budget.lock().release_if_gone(InboundKey::Quic(stream_id));
            return TakenStream::Dropped;
        };
        registry.register_inbound_stream(
            remote_addr,
            stream_id,
            crate::transport::IncomingStream::from(handle),
        );
        crate::transport::TRANSPORT_METRICS.record_inbound_completed(total);
        let message = metadata
            .map(|metadata| metadata.to_vec())
            .filter(|bytes| !self.is_duplicate_dispatch(bytes));
        TakenStream::Registered { stream_id, message }
    }
}
