use bytes::Bytes;
use std::collections::BTreeMap;

use tokio::sync::mpsc;

use crate::transport::peer_connection::outbound_stream::SerializedStream;

use super::StreamId;

type FragmentIdx = u32;

pub(super) async fn recv_stream(
    stream_id: StreamId,
    mut receiver: mpsc::Receiver<(FragmentIdx, Bytes)>,
    mut stream: InboundStream,
) -> Result<(StreamId, Vec<u8>), StreamId> {
    while let Some((fragment_number, payload)) = receiver.recv().await {
        if let Some(msg) = stream.push_fragment(fragment_number, payload) {
            return Ok((stream_id, msg));
        }
    }
    Err(stream_id)
}

pub(super) struct InboundStream {
    total_length_bytes: u64,
    /// Fragment numbers are 1-indexed
    last_contiguous_fragment_idx: FragmentIdx,
    /// Out-of-order fragments stored until they can be appended
    non_contiguous_fragments: BTreeMap<FragmentIdx, Bytes>,
    /// Accumulated payload bytes
    payload: Vec<u8>,
}

impl InboundStream {
    pub fn new(total_length_bytes: u64) -> Self {
        Self {
            total_length_bytes,
            last_contiguous_fragment_idx: 0,
            non_contiguous_fragments: BTreeMap::new(),
            payload: Vec::with_capacity(total_length_bytes as usize),
        }
    }

    /// Returns some if the message has been completely streamed, none otherwise.
    pub fn push_fragment(
        &mut self,
        fragment_number: FragmentIdx,
        fragment: SerializedStream,
    ) -> Option<Vec<u8>> {
        // tracing::trace!(
        //     %fragment_number,
        //     last = %self.last_contiguous_fragment_idx,
        //     non_contig = ?self.non_contiguous_fragments.keys().collect::<Vec<_>>(),
        //     "received stream fragment"
        // );

        // Idempotency guard: a fragment at or below the contiguous frontier has
        // already been appended to `payload`, so a replay of it carries no new
        // bytes. Dropping it is not merely an optimisation: buffering it in
        // `non_contiguous_fragments` WEDGES the drain loop below permanently,
        // because that loop stops at the first key that is not
        // `last_contiguous_fragment_idx + 1` and a stale key can never become
        // that again (the frontier only grows). Every later out-of-order
        // fragment then sits behind the stale key forever and the stream never
        // completes, even once all its bytes have arrived.
        //
        // Reachability: `PeerConnection::recv` drops same-`packet_id` replays
        // via `ReceivedPacketTracker` before they get here, so this is
        // defence-in-depth rather than a live production failure, but
        // reassembly is documented as order-insensitive and idempotent, and
        // without this guard it is only the former.
        if fragment_number <= self.last_contiguous_fragment_idx {
            return None;
        }

        if fragment_number == self.last_contiguous_fragment_idx + 1 {
            self.last_contiguous_fragment_idx = fragment_number;
            self.payload.extend_from_slice(&fragment);
        } else {
            self.non_contiguous_fragments
                .insert(fragment_number, fragment);
        }
        while let Some((idx, v)) = self.non_contiguous_fragments.pop_first() {
            if idx == self.last_contiguous_fragment_idx + 1 {
                self.last_contiguous_fragment_idx += 1;
                self.payload.extend_from_slice(&v);
            } else {
                self.non_contiguous_fragments.insert(idx, v);
                break;
            }
        }
        self.get_and_clear()
    }

    fn get_and_clear(&mut self) -> Option<Vec<u8>> {
        if self.payload.len() as u64 == self.total_length_bytes {
            Some(std::mem::take(&mut self.payload))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_sequence() {
        let mut stream = InboundStream::new(6);
        assert_eq!(
            stream.push_fragment(1, Bytes::from_static(&[1, 2, 3])),
            None
        );
        assert_eq!(
            stream.push_fragment(2, Bytes::from_static(&[4, 5, 6])),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_1() {
        let mut stream = InboundStream::new(6);
        assert_eq!(stream.push_fragment(1, Bytes::from_static(&[1, 2])), None);
        assert_eq!(stream.push_fragment(3, Bytes::from_static(&[5, 6])), None);
        assert_eq!(
            stream.push_fragment(2, Bytes::from_static(&[3, 4])),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_2() {
        let mut stream = InboundStream::new(6);
        assert_eq!(stream.push_fragment(2, Bytes::from_static(&[3, 4])), None);
        assert_eq!(stream.push_fragment(3, Bytes::from_static(&[5, 6])), None);
        assert_eq!(
            stream.push_fragment(1, Bytes::from_static(&[1, 2])),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    /// A replay of an already-consumed fragment must not stall the stream.
    ///
    /// Without the idempotency guard in `push_fragment`, the replayed fragment
    /// is buffered under a key at or below the contiguous frontier. The drain
    /// loop pops the smallest pending key first, sees it is not
    /// `frontier + 1`, puts it back and breaks, so that stale key blocks every
    /// later out-of-order fragment forever and the stream never completes even
    /// though all of its bytes have arrived.
    ///
    /// The replay must also not corrupt the payload: the reassembled bytes are
    /// asserted byte-for-byte, not merely by length.
    #[test]
    fn test_duplicate_fragment_does_not_wedge_reassembly() {
        let mut stream = InboundStream::new(10);
        assert_eq!(stream.push_fragment(1, Bytes::from_static(&[1, 1])), None);
        assert_eq!(stream.push_fragment(2, Bytes::from_static(&[2, 2])), None);
        // Replay of a fragment already folded into `payload`.
        assert_eq!(stream.push_fragment(1, Bytes::from_static(&[1, 1])), None);
        // Remaining fragments arrive out of order behind the replay.
        assert_eq!(stream.push_fragment(5, Bytes::from_static(&[5, 5])), None);
        assert_eq!(stream.push_fragment(4, Bytes::from_static(&[4, 4])), None);
        assert_eq!(
            stream.push_fragment(3, Bytes::from_static(&[3, 3])),
            Some(vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5]),
            "replayed fragment must not strand the fragments queued behind it"
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    /// A replay of a fragment still sitting in the out-of-order buffer is a
    /// plain overwrite and must leave the reassembled bytes unchanged.
    #[test]
    fn test_duplicate_pending_fragment_is_idempotent() {
        let mut stream = InboundStream::new(6);
        assert_eq!(stream.push_fragment(3, Bytes::from_static(&[5, 6])), None);
        assert_eq!(stream.push_fragment(3, Bytes::from_static(&[5, 6])), None);
        assert_eq!(stream.push_fragment(2, Bytes::from_static(&[3, 4])), None);
        assert_eq!(
            stream.push_fragment(1, Bytes::from_static(&[1, 2])),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
    }
}
