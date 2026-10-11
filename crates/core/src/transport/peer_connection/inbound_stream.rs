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

        // Reject an out-of-range fragment number outright. Without this, a
        // fragment far past the end of the stream is inserted into
        // `non_contiguous_fragments`, where nothing ever removes it: the drain
        // loop below only pops keys that reach `frontier + 1`, so the entry -
        // and this whole `InboundStream`, its spawned `recv_stream` task, its
        // channel, and the partial payload - lives until the connection
        // closes. Both sibling reassemblers already bound this
        // (`streaming_buffer.rs::insert`, `piped_stream.rs::push_fragment`).
        //
        // The bound is the largest fragment number the sender could legitimately
        // emit: one per `MAX_DATA_SIZE` of payload, plus one, because embedded
        // metadata shortens fragment #1 and can push the tail into an extra
        // fragment (fix #2757 - the same `+1` `streaming_buffer.rs` allocates
        // as its overflow slot).
        let max_fragment_number = (self.total_length_bytes as usize)
            .div_ceil(super::MAX_DATA_SIZE)
            .saturating_add(1) as FragmentIdx;
        if fragment_number == 0 || fragment_number > max_fragment_number {
            return None;
        }

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
        // Duplicate semantics differ by side of the frontier, and neither is
        // "the payload is rebuilt from the newer copy":
        //   - at or below the frontier: FIRST writer wins, because the bytes
        //     are already in `payload` and the replay is dropped here;
        //   - above the frontier: LAST writer wins, because the `insert` below
        //     overwrites the pending entry.
        // A replay above the frontier that carries a DIFFERENT length steps
        // `payload.len()` past `total_length_bytes`, and `get_and_clear`
        // compares with `==`, so the stream never completes. Only a broken or
        // hostile sender produces that; an honest retransmit is byte-identical.
        //
        // Reachability: `ReceivedPacketTracker` (see
        // `received_packet_tracker.rs:62`) dedups by `packet_id` only, and an
        // honest retransmit reuses the original `packet_id`, so loss recovery
        // never reaches here. A peer that replays the same `fragment_number`
        // under fresh `packet_id`s does, which makes this a guard against a
        // hostile or broken peer rather than defence-in-depth against a
        // condition nothing can produce.
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

    /// A full fragment payload, matching what `send_stream` actually emits.
    ///
    /// The fixtures below are sized in whole fragments rather than in bytes.
    /// A 6-byte stream carried in three 2-byte fragments - the shape these
    /// tests used to assert on - is not a stream any sender produces, and
    /// `push_fragment`'s out-of-range bound is derived from
    /// `total_length_bytes`, so an impossible shape now reads as an
    /// out-of-range fragment number rather than as the ordering case the test
    /// means to cover.
    const FRAG: usize = super::super::MAX_DATA_SIZE;

    /// One full fragment of `marker` bytes.
    fn frag(marker: u8) -> Bytes {
        Bytes::from(vec![marker; FRAG])
    }

    /// The payload `markers` reassembles to, in order.
    fn joined(markers: &[u8]) -> Vec<u8> {
        markers
            .iter()
            .flat_map(|marker| std::iter::repeat_n(*marker, FRAG))
            .collect()
    }

    #[test]
    fn test_simple_sequence() {
        let mut stream = InboundStream::new((2 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), Some(joined(&[1, 2])));
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_1() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), Some(joined(&[1, 2, 3])));
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_2() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        assert_eq!(stream.push_fragment(1, frag(1)), Some(joined(&[1, 2, 3])));
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
        let mut stream = InboundStream::new((5 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        // Replay of a fragment already folded into `payload`.
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        // Remaining fragments arrive out of order behind the replay.
        assert_eq!(stream.push_fragment(5, frag(5)), None);
        assert_eq!(stream.push_fragment(4, frag(4)), None);
        assert_eq!(
            stream.push_fragment(3, frag(3)),
            Some(joined(&[1, 2, 3, 4, 5])),
            "replayed fragment must not strand the fragments queued behind it"
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    /// A replay of a fragment still sitting in the out-of-order buffer is a
    /// plain overwrite and must leave the reassembled bytes unchanged.
    #[test]
    fn test_duplicate_pending_fragment_is_idempotent() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        assert_eq!(stream.push_fragment(1, frag(1)), Some(joined(&[1, 2, 3])));
        assert!(stream.non_contiguous_fragments.is_empty());
    }

    /// A fragment number past anything the stream could need must be dropped,
    /// not parked in `non_contiguous_fragments` for the life of the connection.
    ///
    /// `non_contiguous_fragments` is only ever drained by the frontier reaching
    /// a key, so an entry above the highest reachable fragment number is never
    /// removed: it pins the `InboundStream`, its `recv_stream` task, its
    /// channel, and the partial payload until the connection closes. An
    /// authenticated peer can send fragment 4,000,000,000 as often as it likes,
    /// because `ReceivedPacketTracker` dedups by `packet_id` rather than by
    /// fragment number, so a fresh `packet_id` per replay walks straight
    /// through.
    #[test]
    fn test_out_of_range_fragment_is_dropped_not_buffered() {
        let mut stream = InboundStream::new((2 * FRAG) as u64);

        assert_eq!(stream.push_fragment(u32::MAX, frag(9)), None);
        assert_eq!(stream.push_fragment(4_000_000_000, frag(9)), None);
        // One past the highest number a sender could emit for this size:
        // 2 fragments, plus 1 for a metadata-shortened fragment #1.
        assert_eq!(stream.push_fragment(4, frag(9)), None);
        assert_eq!(stream.push_fragment(0, frag(9)), None);
        assert!(
            stream.non_contiguous_fragments.is_empty(),
            "an unreachable fragment number must not occupy the reassembly buffer"
        );

        // The in-range overflow fragment (#3, used when embedded metadata
        // shortens fragment #1) is still accepted, and the stream completes.
        let metadata_overhead = 1 + 8 + 256;
        assert_eq!(
            stream.push_fragment(1, Bytes::from(vec![1u8; FRAG - metadata_overhead])),
            None
        );
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        let assembled = stream
            .push_fragment(3, Bytes::from(vec![3u8; metadata_overhead]))
            .expect("the metadata-overflow fragment is in range and completes the stream");
        assert_eq!(assembled.len(), 2 * FRAG);
        assert_eq!(
            &assembled[..FRAG - metadata_overhead],
            &vec![1u8; FRAG - metadata_overhead][..]
        );
        assert_eq!(
            &assembled[2 * FRAG - metadata_overhead..],
            &vec![3u8; metadata_overhead][..]
        );
        assert!(stream.non_contiguous_fragments.is_empty());
    }
}
