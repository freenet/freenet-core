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
        // An evicted stream stops at once, even with fragments still queued.
        if stream.is_cancelled() {
            return Err(stream_id);
        }
        if let Some(msg) = stream.push_fragment(fragment_number, payload) {
            return Ok((stream_id, msg));
        }
    }
    Err(stream_id)
}

/// Reassembles one legacy stream.
///
/// Fragments are held as received (each in its own `Bytes`) until the stream
/// is complete, then copied once into a buffer of exactly the stream's
/// length. The memory held is the fragments' own allocations plus
/// per-fragment map overhead, both covered by the stream's charge (see
/// `inbound_budget::ChargeOverhead`). The operations path
/// (`LockFreeStreamBuffer`) holds its fragments the same way.
pub(super) struct InboundStream {
    total_length_bytes: u64,
    /// Fragment numbers are 1-indexed. Every fragment up to this one is held.
    last_contiguous_fragment_idx: FragmentIdx,
    /// Payload bytes in fragments `1..=last_contiguous_fragment_idx`.
    contiguous_bytes: u64,
    /// Every fragment received and not yet assembled.
    fragments: BTreeMap<FragmentIdx, Bytes>,
    /// Total length of `fragments`' values.
    held_bytes: u64,
    /// Accounting token: told the bytes received; carries cancellation.
    token: Option<std::sync::Arc<super::inbound_budget::LegacyStreamToken>>,
}

impl InboundStream {
    pub fn new(total_length_bytes: u64) -> Self {
        Self {
            total_length_bytes,
            last_contiguous_fragment_idx: 0,
            contiguous_bytes: 0,
            fragments: BTreeMap::new(),
            held_bytes: 0,
            token: None,
        }
    }

    /// Whether the stream's token has been cancelled (it was evicted).
    fn is_cancelled(&self) -> bool {
        self.token.as_ref().is_some_and(|t| t.is_cancelled())
    }

    /// Report the bytes this stream holds to `token` as they arrive.
    pub(super) fn with_token(
        mut self,
        token: std::sync::Arc<super::inbound_budget::LegacyStreamToken>,
    ) -> Self {
        self.token = Some(token);
        self
    }

    /// Returns some if the message has been completely streamed, none otherwise.
    pub fn push_fragment(
        &mut self,
        fragment_number: FragmentIdx,
        fragment: SerializedStream,
    ) -> Option<Vec<u8>> {
        // Reject an out-of-range fragment number outright. Without this, a
        // fragment far past the end of the stream is held in `fragments`, where
        // the stream can never complete past it, and the entry - and this
        // whole `InboundStream`, its spawned `recv_stream` task and its channel
        // - lives until the stream is evicted. Both sibling reassemblers
        // already bound this (`streaming_buffer.rs::insert`,
        // `piped_stream.rs::push_fragment`).
        //
        // The bound is the largest fragment number the sender could legitimately
        // emit: one per `MAX_DATA_SIZE` of payload, plus one, because embedded
        // metadata shortens fragment #1 and can push the tail into an extra
        // fragment (fix #2757 - the same `+1` `streaming_buffer.rs` allocates
        // as its overflow slot). The per-fragment charge relies on this bound.
        let max_fragment_number = (self.total_length_bytes as usize)
            .div_ceil(super::MAX_DATA_SIZE)
            .saturating_add(1) as FragmentIdx;
        if fragment_number == 0 || fragment_number > max_fragment_number {
            return None;
        }

        // Idempotency guard: a fragment at or below the contiguous frontier is
        // already held, so a replay of it carries no new bytes. Duplicate
        // semantics differ by side of the frontier:
        //   - at or below the frontier: FIRST writer wins (the replay is
        //     dropped here), so the frontier's byte count stays exact;
        //   - above the frontier: LAST writer wins (the `insert` below
        //     overwrites the held entry).
        // Either way the bytes held never exceed `total_length_bytes`: a
        // fragment that would take them past it is dropped below.
        //
        // Reachability: `ReceivedPacketTracker` dedups by `packet_id` only, and
        // an honest retransmit reuses the original `packet_id`, so loss
        // recovery never reaches here. A peer that replays the same
        // `fragment_number` under fresh `packet_id`s does.
        if fragment_number <= self.last_contiguous_fragment_idx {
            return None;
        }

        // Every fragment carries payload except possibly fragment #1 (older
        // senders could leave it empty beside embedded metadata), and the
        // payload of all fragments sums to exactly `total_length_bytes`.
        let replaced = self
            .fragments
            .get(&fragment_number)
            .map_or(0, |b| b.len() as u64);
        let held = self.held_bytes - replaced;
        if (fragment.is_empty() && fragment_number != 1)
            || held + fragment.len() as u64 > self.total_length_bytes
        {
            return None;
        }
        self.held_bytes = held + fragment.len() as u64;
        self.fragments.insert(fragment_number, fragment);

        while let Some(next) = self.fragments.get(&(self.last_contiguous_fragment_idx + 1)) {
            self.contiguous_bytes += next.len() as u64;
            self.last_contiguous_fragment_idx += 1;
        }
        if let Some(token) = &self.token {
            token.report(self.held_bytes);
        }
        if self.contiguous_bytes == self.total_length_bytes {
            // Completion and eviction race; whichever is recorded first wins.
            if self.token.as_ref().is_some_and(|t| !t.mark_complete()) {
                return None;
            }
            Some(self.assemble())
        } else {
            None
        }
    }

    /// Copy the complete stream into one buffer of exactly its length,
    /// releasing each fragment as it is copied.
    fn assemble(&mut self) -> Vec<u8> {
        // Contiguous bytes equal the total and held bytes cannot exceed it, so
        // every held fragment is below the frontier.
        debug_assert_eq!(self.held_bytes, self.total_length_bytes);
        let mut payload = Vec::with_capacity(self.total_length_bytes as usize);
        while let Some((_, fragment)) = self.fragments.pop_first() {
            payload.extend_from_slice(&fragment);
        }
        self.held_bytes = 0;
        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        /// Bytes allocated on this thread (never decremented).
        static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    }

    /// The system allocator, counting what each thread allocates, so a test
    /// can see an allocation it did not expect. Tests on other threads are
    /// unaffected.
    struct CountingAllocator;

    fn count(bytes: usize) {
        // Unavailable only while the thread is being torn down.
        if ALLOCATED.try_with(|n| n.set(n.get() + bytes)).is_err() {}
    }

    // SAFETY: delegates every call to `System` unchanged.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count(layout.size());
            // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: `ptr` was allocated by `System` with `layout`.
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            count(new_size);
            // SAFETY: `ptr` was allocated by `System` with `layout`.
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    /// Bytes allocated on this thread while running `f`.
    fn allocated_by<R>(f: impl FnOnce() -> R) -> (R, usize) {
        let before = ALLOCATED.with(Cell::get);
        let result = f();
        (result, ALLOCATED.with(Cell::get) - before)
    }

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

    /// The memory a stream holds is the fragments received, however they
    /// arrive; constructing it allocates nothing, and the message is one
    /// exact allocation, made once every byte has arrived.
    #[test]
    fn reassembly_holds_only_the_fragments_received() {
        let declared = super::super::streaming::MAX_STREAM_TOTAL_BYTES;
        let (mut stream, at_construction) = allocated_by(|| InboundStream::new(declared));
        assert_eq!(at_construction, 0, "construction allocates nothing");
        let first = frag(1);
        let (_, at_first) = allocated_by(|| stream.push_fragment(1, first));
        assert!(
            at_first < 4096,
            "a fragment costs a map entry: {at_first} B"
        );

        // In order up to past half, then the rest except one, out of order.
        const N: usize = 40;
        let total = N * FRAG;
        let mut stream = InboundStream::new(total as u64);
        let gap = N / 2 + 2;
        let order: Vec<usize> = (1..gap).chain((gap + 1..=N).rev()).chain([gap]).collect();
        for (step, &i) in order.iter().enumerate() {
            let done = stream.push_fragment(i as u32, frag(i as u8));
            let held: usize = stream.fragments.values().map(|b| b.len()).sum();
            if step + 1 < order.len() {
                assert!(done.is_none());
                assert_eq!(held as u64, stream.held_bytes);
                assert_eq!(held, (step + 1) * FRAG, "only the fragments received");
            } else {
                let msg = done.expect("complete");
                assert_eq!(msg, joined(&(1..=N as u8).collect::<Vec<_>>()));
                assert_eq!(msg.capacity(), total, "one exact allocation");
                assert!(stream.fragments.is_empty(), "fragments released");
                assert_eq!(stream.held_bytes, 0);
            }
        }
    }

    /// The bytes held never exceed the stream's total: an oversized fragment,
    /// an empty one, and many short ones cannot complete or overfill it.
    #[test]
    fn fragments_cannot_exceed_or_shortcut_the_declared_total() {
        let total = (3 * FRAG) as u64;
        let mut stream = InboundStream::new(total);
        assert_eq!(stream.push_fragment(2, Bytes::new()), None);
        assert!(stream.fragments.is_empty(), "empty #2 is dropped");

        // One-byte fragments up to the fragment-number bound never complete it.
        for i in 1..=4 {
            assert_eq!(stream.push_fragment(i, Bytes::from_static(b"x")), None);
        }
        assert_eq!(stream.held_bytes, 4);

        // A pending fragment that would overfill the total is dropped...
        let mut stream = InboundStream::new(total);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        let too_big = Bytes::from(vec![9u8; FRAG + 1]);
        assert_eq!(stream.push_fragment(4, too_big.clone()), None);
        assert!(!stream.fragments.contains_key(&4));
        // ...and so is a contiguous one.
        assert_eq!(stream.push_fragment(1, too_big), None);
        assert_eq!(stream.last_contiguous_fragment_idx, 0);
        assert_eq!(stream.held_bytes, (2 * FRAG) as u64);

        // The genuine fragment still completes it.
        assert_eq!(stream.push_fragment(1, frag(1)), Some(joined(&[1, 2, 3])));
    }

    /// A stream completes only when its bytes are contiguous from fragment
    /// #1: a sender that fills the total from #2 on (using the extra fragment
    /// number) never completes it, and fragment #1 cannot then fit.
    #[test]
    fn bytes_past_a_gap_do_not_complete_a_stream() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        for i in 2..=4 {
            assert_eq!(stream.push_fragment(i, frag(i as u8)), None);
        }
        assert_eq!(
            stream.held_bytes,
            (3 * FRAG) as u64,
            "premise: the total is held"
        );
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.last_contiguous_fragment_idx, 0);
    }

    /// A replay of a fragment at or below the contiguous frontier, even with
    /// a different length, changes neither the bytes held nor the message.
    #[test]
    fn a_replay_below_the_frontier_cannot_change_the_message() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        assert_eq!(stream.push_fragment(1, Bytes::from_static(b"short")), None);
        assert_eq!(stream.held_bytes, (2 * FRAG) as u64);
        assert_eq!(stream.push_fragment(3, frag(3)), Some(joined(&[1, 2, 3])));
    }

    /// The per-fragment charge covers what a legacy stream's fragment map
    /// actually allocates per entry, however the fragments arrive.
    #[test]
    fn the_per_fragment_charge_covers_the_fragment_map() {
        use super::super::inbound_budget::{PAYLOAD_SIZE_CLASS_SLACK, PER_FRAGMENT_OVERHEAD};
        const N: u32 = 20_000;
        let ascending: Vec<u32> = (1..=N).collect();
        let descending: Vec<u32> = (1..=N).rev().collect();
        // Every other one, then the gaps: splits leave nodes partly empty.
        let interleaved: Vec<u32> = (1..=N).step_by(2).chain((2..=N).step_by(2)).collect();
        let scrambled: Vec<u32> = (0..N).map(|i| (i * 7919) % N + 1).collect();
        for (name, order) in [
            ("ascending", ascending),
            ("descending", descending),
            ("interleaved", interleaved),
            ("scrambled", scrambled),
        ] {
            let (map, bytes) = allocated_by(|| {
                let mut map = BTreeMap::new();
                for &i in &order {
                    map.insert(i, Bytes::new());
                }
                map
            });
            let per_entry = (bytes as u64).div_ceil(map.len() as u64);
            assert!(
                per_entry + PAYLOAD_SIZE_CLASS_SLACK <= PER_FRAGMENT_OVERHEAD,
                "{name}: {per_entry} B per entry"
            );
        }
    }

    /// Completion and cancellation exclude each other: a stream cancelled
    /// before its last fragment is pushed is not delivered, and one that
    /// completed first cannot then be cancelled.
    #[test]
    fn completion_and_cancellation_exclude_each_other() {
        use super::super::inbound_budget::LegacyStreamToken;
        let token = LegacyStreamToken::new();
        let mut stream =
            InboundStream::new((2 * FRAG) as u64).with_token(std::sync::Arc::clone(&token));
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert!(token.cancel());
        assert_eq!(stream.push_fragment(2, frag(2)), None, "cancelled first");

        let token = LegacyStreamToken::new();
        let mut stream =
            InboundStream::new((2 * FRAG) as u64).with_token(std::sync::Arc::clone(&token));
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), Some(joined(&[1, 2])));
        assert!(!token.cancel(), "completed first");
        assert!(!token.is_cancelled());
    }

    /// Older senders left fragment #1 empty when its embedded metadata used
    /// the whole packet, and sent the payload from fragment #2 on (one more
    /// fragment than usual). Such a stream still completes.
    #[test]
    fn an_empty_first_fragment_from_an_older_sender_is_accepted() {
        let total = (2 * FRAG) as u64;
        let mut stream = InboundStream::new(total);
        assert_eq!(stream.push_fragment(1, Bytes::new()), None);
        assert_eq!(stream.last_contiguous_fragment_idx, 1);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), Some(joined(&[2, 3])));
    }

    #[test]
    fn test_simple_sequence() {
        let mut stream = InboundStream::new((2 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), Some(joined(&[1, 2])));
        assert!(stream.fragments.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_1() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), Some(joined(&[1, 2, 3])));
        assert!(stream.fragments.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_2() {
        let mut stream = InboundStream::new((3 * FRAG) as u64);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        assert_eq!(stream.push_fragment(3, frag(3)), None);
        assert_eq!(stream.push_fragment(1, frag(1)), Some(joined(&[1, 2, 3])));
        assert!(stream.fragments.is_empty());
    }

    /// A replay of an already-consumed fragment must not stall the stream.
    ///
    /// Without the idempotency guard in `push_fragment`, the replayed fragment
    /// would be counted again in the contiguous bytes, so the stream would
    /// either complete short or be refused its genuine last fragment.
    ///
    /// The replay must also not corrupt the payload: the reassembled bytes are
    /// asserted byte-for-byte, not merely by length.
    #[test]
    fn test_duplicate_fragment_does_not_wedge_reassembly() {
        let mut stream = InboundStream::new((5 * FRAG) as u64);
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        assert_eq!(stream.push_fragment(2, frag(2)), None);
        // Replay of a fragment already below the contiguous frontier.
        assert_eq!(stream.push_fragment(1, frag(1)), None);
        // Remaining fragments arrive out of order behind the replay.
        assert_eq!(stream.push_fragment(5, frag(5)), None);
        assert_eq!(stream.push_fragment(4, frag(4)), None);
        assert_eq!(
            stream.push_fragment(3, frag(3)),
            Some(joined(&[1, 2, 3, 4, 5])),
            "replayed fragment must not strand the fragments queued behind it"
        );
        assert!(stream.fragments.is_empty());
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
        assert!(stream.fragments.is_empty());
    }

    /// A fragment number past anything the stream could need must be dropped,
    /// not held in `fragments`.
    ///
    /// The frontier can never reach an entry above the highest reachable
    /// fragment number, so it would never be released: it pins the
    /// `InboundStream`, its `recv_stream` task and its channel until the
    /// stream is evicted, and is not covered by the per-fragment charge. An
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
            stream.fragments.is_empty(),
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
        assert!(stream.fragments.is_empty());
    }
}
