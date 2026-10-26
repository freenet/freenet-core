//! The lib test binary's global allocator: the system allocator, counting
//! per thread what is allocated and what is still live, so a test can
//! measure the heap its own code uses.
//!
//! Live bytes are counted twice: as requested, and rounded up to the size
//! class jemalloc (the `freenet` binary's allocator) would serve each
//! request from ([`jemalloc_class`]), which is what the node actually
//! spends. Small requests round up by far more than large ones (17 B takes
//! 32 B, 33 B takes 48 B), so a measurement of many small allocations must
//! use the rounded count.
//!
//! Counters are thread-local, so tests on other threads do not disturb a
//! measurement, and the cost to every other test is a few thread-local
//! additions per allocation. A measurement is only sound when everything it
//! covers allocates and frees on the measuring thread (a current-thread
//! runtime, for async code); memory allocated on one thread and freed on
//! another moves the live count of both.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Bytes allocated on this thread (never decremented).
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    /// Bytes allocated on this thread minus bytes freed on it.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// [`LIVE`] with every allocation rounded up to its jemalloc size class.
    static LIVE_ROUNDED: Cell<isize> = const { Cell::new(0) };
    /// The highest [`LIVE_ROUNDED`] since [`peak_delta`] last started.
    static PEAK_ROUNDED: Cell<isize> = const { Cell::new(0) };
}

/// The system allocator, counting what each thread allocates and frees.
struct CountingAllocator;

// The counters are unavailable only while the thread is being torn down;
// an allocation then goes uncounted.
fn count_alloc(bytes: usize) {
    ALLOCATED.try_with(|n| n.set(n.get() + bytes)).ok();
    LIVE.try_with(|n| n.set(n.get() + bytes as isize)).ok();
    LIVE_ROUNDED
        .try_with(|n| {
            let live = n.get() + jemalloc_class(bytes) as isize;
            n.set(live);
            PEAK_ROUNDED.try_with(|p| p.set(p.get().max(live))).ok();
        })
        .ok();
}

fn count_free(bytes: usize) {
    LIVE.try_with(|n| n.set(n.get() - bytes as isize)).ok();
    LIVE_ROUNDED
        .try_with(|n| n.set(n.get() - jemalloc_class(bytes) as isize))
        .ok();
}

// SAFETY: delegates every call to `System` unchanged.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_alloc(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_alloc(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count_free(layout.size());
        // SAFETY: `ptr` was allocated by `System` with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_alloc(new_size);
        count_free(layout.size());
        // SAFETY: `ptr` was allocated by `System` with `layout`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The size jemalloc serves a request of `n` bytes from: 8 and 16, then
/// four classes per doubling (16 B apart up to 128, then a quarter of the
/// power of two). Alignment above 16 B is not modelled.
pub(crate) fn jemalloc_class(n: usize) -> usize {
    if n <= 8 {
        return 8;
    }
    if n <= 16 {
        return 16;
    }
    let k = usize::BITS as usize - 1 - (n - 1).leading_zeros() as usize; // 2^k < n <= 2^(k+1)
    let step = 1usize << (k - 2).max(4);
    n.div_ceil(step) * step
}

/// Bytes allocated on this thread while running `f`, whether or not they
/// were freed again.
pub(crate) fn allocated_by<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCATED.with(Cell::get);
    let result = f();
    (result, ALLOCATED.with(Cell::get) - before)
}

/// Bytes allocated and not yet freed on this thread, as requested and as
/// jemalloc would round them. Differences between two readings are what a
/// test uses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Live {
    pub(crate) requested: isize,
    pub(crate) rounded: isize,
}

impl std::ops::Sub for Live {
    type Output = Live;

    fn sub(self, other: Live) -> Live {
        Live {
            requested: self.requested - other.requested,
            rounded: self.rounded - other.rounded,
        }
    }
}

impl std::ops::AddAssign for Live {
    fn add_assign(&mut self, other: Live) {
        self.requested += other.requested;
        self.rounded += other.rounded;
    }
}

/// This thread's live bytes now.
pub(crate) fn live_bytes() -> Live {
    Live {
        requested: LIVE.with(Cell::get),
        rounded: LIVE_ROUNDED.with(Cell::get),
    }
}

/// The change in [`live_bytes`] while running `f`: what it left allocated,
/// less what it freed that was allocated before.
pub(crate) fn live_delta<R>(f: impl FnOnce() -> R) -> (R, Live) {
    let before = live_bytes();
    let result = f();
    (result, live_bytes() - before)
}

/// [`live_delta`], plus the highest the rounded live count rose above its
/// starting value at any point while `f` ran: memory `f` held only
/// briefly, which a reading taken after it returns cannot see.
pub(crate) fn peak_delta<R>(f: impl FnOnce() -> R) -> (R, Live, isize) {
    let before = live_bytes();
    PEAK_ROUNDED.with(|p| p.set(before.rounded));
    let result = f();
    let peak = PEAK_ROUNDED.with(Cell::get) - before.rounded;
    (result, live_bytes() - before, peak)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;

    /// The live counts follow allocation, growth and release on this thread.
    #[test]
    fn live_bytes_follow_alloc_realloc_and_free() {
        let (mut v, grown) = live_delta(|| black_box(Vec::<u8>::with_capacity(1000)));
        assert_eq!(grown.requested, 1000);
        assert_eq!(grown.rounded, 1024);
        let (_, regrown) = live_delta(|| v.reserve_exact(3000));
        assert_eq!(
            regrown.requested, 2000,
            "a realloc counts the new size less the old"
        );
        assert_eq!(regrown.rounded, 3072 - 1024);
        let (_, freed) = live_delta(|| drop(black_box(v)));
        assert_eq!(
            freed,
            Live {
                requested: -3000,
                rounded: -3072
            }
        );
        let (_, small) = live_delta(|| black_box(Box::new([0u8; 17])));
        assert_eq!(
            small,
            Live {
                requested: 17,
                rounded: 32
            }
        );
        let (_, total) = allocated_by(|| drop(black_box(Vec::<u8>::with_capacity(500))));
        assert_eq!(total, 500, "allocated_by counts what was freed too");
        let (_, net, peak) = peak_delta(|| drop(black_box(Vec::<u8>::with_capacity(4096))));
        assert_eq!(
            (net.rounded, peak),
            (0, 4096),
            "the peak sees a freed allocation"
        );
    }

    /// The size classes jemalloc documents, at their edges.
    #[test]
    fn jemalloc_classes_at_their_edges() {
        for (n, class) in [
            (1, 8),
            (8, 8),
            (9, 16),
            (17, 32),
            (33, 48),
            (129, 160),
            (1200, 1280),
            (4097, 5120),
            (64 * 1024 + 1, 80 * 1024),
        ] {
            assert_eq!(jemalloc_class(n), class, "class of {n} B");
        }
    }
}
