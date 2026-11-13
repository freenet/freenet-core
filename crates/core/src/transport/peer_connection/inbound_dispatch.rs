//! What a connection has handed to the node, kept to refuse repeats: the
//! registry that receives its inbound streams, the hashes of the messages it
//! dispatched, and the ids of the streams it completed. Moved out of
//! `PeerConnection` unchanged, so a connection with no UDP session can keep
//! the same record.

use super::StreamId;

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
}
