//! The lib test binary's global allocator: the system allocator, counting
//! per thread what is allocated and what is still live, so a test can
//! measure the heap its own code uses.
//!
//! Counters are thread-local, so tests on other threads do not disturb a
//! measurement, and the cost to every other test is two thread-local
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
}

/// The system allocator, counting what each thread allocates and frees.
struct CountingAllocator;

// The counters are unavailable only while the thread is being torn down;
// an allocation then goes uncounted.
fn count_alloc(bytes: usize) {
    ALLOCATED.try_with(|n| n.set(n.get() + bytes)).ok();
    LIVE.try_with(|n| n.set(n.get() + bytes as isize)).ok();
}

fn count_free(bytes: usize) {
    LIVE.try_with(|n| n.set(n.get() - bytes as isize)).ok();
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

/// Bytes allocated on this thread while running `f`, whether or not they
/// were freed again.
pub(crate) fn allocated_by<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCATED.with(Cell::get);
    let result = f();
    (result, ALLOCATED.with(Cell::get) - before)
}

/// Bytes allocated and not yet freed on this thread, since it started.
/// Differences between two readings are what a test uses.
pub(crate) fn live_bytes() -> isize {
    LIVE.with(Cell::get)
}

/// The change in [`live_bytes`] while running `f`: what it left allocated,
/// less what it freed that was allocated before.
pub(crate) fn live_delta<R>(f: impl FnOnce() -> R) -> (R, isize) {
    let before = live_bytes();
    let result = f();
    (result, live_bytes() - before)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The live count follows allocation, growth and release on this thread.
    #[test]
    fn live_bytes_follow_alloc_realloc_and_free() {
        let (mut v, grown) = live_delta(|| Vec::<u8>::with_capacity(1000));
        assert_eq!(grown, 1000);
        let (_, regrown) = live_delta(|| v.reserve_exact(3000));
        assert_eq!(regrown, 2000, "a realloc counts the new size less the old");
        let (_, freed) = live_delta(|| drop(v));
        assert_eq!(freed, -3000);
        let (_, total) = allocated_by(|| drop(Vec::<u8>::with_capacity(500)));
        assert_eq!(total, 500, "allocated_by counts what was freed too");
    }
}
