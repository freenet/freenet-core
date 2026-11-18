//! Per-thread id counters that cannot run into another thread's ids.
//!
//! Each thread draws ids from a block of [`ID_BLOCK`] values. The first block
//! of a thread is `thread_index * ID_BLOCK` (plus the counter's `offset`), so
//! ids stay deterministic for simulations and tests that pin their thread
//! index (#3044, parallel test isolation). A thread that uses up its block
//! takes a replacement from a process-wide allocator whose blocks start at
//! block number [`REFILL_BASE_BLOCK`], above any block a thread index maps to.
//! Without the replacement the thread kept counting into the next thread's
//! block, and two live clients or requests could receive the same id
//! (ANT-2026-8LFV3MG8).

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of ids in one block.
pub(crate) const ID_BLOCK: u64 = 1_000_000;

/// Block number where replacement blocks start. Thread indices count up from
/// zero, one per thread ever created, so they stay far below `2^32`.
const REFILL_BASE_BLOCK: u64 = 1 << 32;

/// Next unused replacement block, counted from [`REFILL_BASE_BLOCK`].
static NEXT_REFILL_BLOCK: AtomicU64 = AtomicU64::new(0);

/// A thread's current block of ids: `next` is the next id to hand out and
/// `end` is the first id past the block.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IdBlock {
    next: u64,
    end: u64,
    /// Added to every block start, so counters that begin at 1 keep doing so.
    offset: u64,
}

impl IdBlock {
    /// The first block of thread `thread_index`.
    pub(crate) fn for_thread(thread_index: u64, offset: u64) -> Self {
        Self::at_block(thread_index, offset)
    }

    fn at_block(block: u64, offset: u64) -> Self {
        let start = block.wrapping_mul(ID_BLOCK).wrapping_add(offset);
        Self {
            next: start,
            end: start.wrapping_add(ID_BLOCK),
            offset,
        }
    }

    /// Hand out the next id, moving to an unused replacement block when this
    /// one is exhausted.
    pub(crate) fn take(&mut self) -> u64 {
        if self.next == self.end {
            let n = NEXT_REFILL_BLOCK.fetch_add(1, Ordering::Relaxed);
            *self = Self::at_block(REFILL_BASE_BLOCK.wrapping_add(n), self.offset);
        }
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        id
    }

    #[cfg(test)]
    fn with_remaining(mut self, remaining: u64) -> Self {
        self.next = self.end - remaining;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_block_starts_at_thread_block_plus_offset() {
        let mut b = IdBlock::for_thread(3, 1);
        assert_eq!(b.take(), 3 * ID_BLOCK + 1);
        assert_eq!(b.take(), 3 * ID_BLOCK + 2);
        let mut b = IdBlock::for_thread(3, 0);
        assert_eq!(b.take(), 3 * ID_BLOCK);
    }

    #[test]
    fn exhausted_block_moves_to_a_block_no_thread_index_maps_to() {
        let mut b = IdBlock::for_thread(5, 1).with_remaining(2);
        assert_eq!(b.take(), 6 * ID_BLOCK - 1);
        assert_eq!(b.take(), 6 * ID_BLOCK);
        let next = b.take();
        assert!(
            next >= REFILL_BASE_BLOCK * ID_BLOCK,
            "replacement id {next} is inside the thread-index range"
        );
        assert_eq!(b.take(), next + 1, "the replacement block is contiguous");
    }

    #[test]
    fn replacement_blocks_are_never_shared() {
        let mut a = IdBlock::for_thread(1, 0).with_remaining(0);
        let mut b = IdBlock::for_thread(2, 0).with_remaining(0);
        let (first_a, first_b) = (a.take(), b.take());
        assert_ne!(first_a / ID_BLOCK, first_b / ID_BLOCK);
    }
}
