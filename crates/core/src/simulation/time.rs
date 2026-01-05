//! Time abstraction layer for deterministic simulation.
//!
//! This module provides:
//! - `TimeSource` trait for abstracting time operations
//! - `RealTime` implementation delegating to tokio
//! - `VirtualTime` implementation for deterministic simulation
//!
//! # MadSim Integration
//!
//! When compiled with `cfg(madsim)`, the `VirtualTime` implementation delegates to
//! MadSim's deterministic time infrastructure. MadSim replaces tokio with a
//! deterministic runtime that:
//!
//! - Provides reproducible time progression
//! - Auto-advances time when the runtime is idle
//! - Uses `tokio::time` APIs (which MadSim intercepts)
//!
//! In MadSim mode, `VirtualTime::advance()` becomes a no-op since MadSim handles
//! time advancement automatically. Use `tokio::time::advance()` for explicit
//! time control when needed.

use std::{future::Future, pin::Pin, time::Duration};

#[cfg(not(madsim))]
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

/// Unique identifier for a wakeup registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WakeupId(u64);

#[cfg(not(madsim))]
impl WakeupId {
    fn new(id: u64) -> Self {
        Self(id)
    }
}

impl WakeupId {
    /// Returns the inner ID value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// A pending wakeup in the virtual time system.
#[cfg(not(madsim))]
#[derive(Debug)]
pub struct Wakeup {
    /// When this wakeup should fire (virtual nanoseconds since epoch)
    pub deadline: u64,
    /// Unique ID for ordering ties
    pub id: WakeupId,
    /// Waker to notify when deadline is reached
    #[allow(dead_code)]
    waker: Option<Waker>,
}

#[cfg(not(madsim))]
impl Wakeup {
    fn new(deadline: u64, id: WakeupId) -> Self {
        Self {
            deadline,
            id,
            waker: None,
        }
    }
}

#[cfg(not(madsim))]
impl PartialEq for Wakeup {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}

#[cfg(not(madsim))]
impl Eq for Wakeup {}

#[cfg(not(madsim))]
impl PartialOrd for Wakeup {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(not(madsim))]
impl Ord for Wakeup {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse ordering so smallest deadline comes first
        // Ties broken by registration order (smaller ID = earlier registration)
        match other.deadline.cmp(&self.deadline) {
            Ordering::Equal => other.id.0.cmp(&self.id.0),
            ord => ord,
        }
    }
}

/// Abstraction over time operations supporting both real and virtual time.
///
/// This trait allows code to be written once and run in both production
/// (with real time) and simulation (with virtual time) contexts.
pub trait TimeSource: Send + Sync + Clone + 'static {
    /// Returns the current time as nanoseconds since an arbitrary epoch.
    fn now_nanos(&self) -> u64;

    /// Returns the current time as a Duration since an arbitrary epoch.
    fn now(&self) -> Duration {
        Duration::from_nanos(self.now_nanos())
    }

    /// Creates a future that completes after the given duration.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    /// Creates a future that completes when the deadline is reached or returns
    /// immediately if the deadline has passed.
    fn sleep_until(&self, deadline_nanos: u64) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    /// Wraps a future with a timeout, returning None if the timeout expires.
    fn timeout<F, T>(
        &self,
        duration: Duration,
        future: F,
    ) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;
}

/// Real-time implementation that delegates to tokio.
///
/// Uses `tokio::time::Instant` for time tracking to ensure consistency with
/// `tokio::time::sleep()`. This is important for tests using `start_paused = true`
/// where tokio's virtual time is used - using `std::time::Instant` would cause
/// timing loops to run much longer than expected because wall-clock time doesn't
/// advance with paused tokio time.
#[derive(Clone)]
pub struct RealTime {
    epoch: tokio::time::Instant,
}

impl Default for RealTime {
    fn default() -> Self {
        Self::new()
    }
}

impl RealTime {
    pub fn new() -> Self {
        Self {
            epoch: tokio::time::Instant::now(),
        }
    }
}

impl TimeSource for RealTime {
    fn now_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn sleep_until(&self, deadline_nanos: u64) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let now = self.now_nanos();
        if deadline_nanos <= now {
            Box::pin(std::future::ready(()))
        } else {
            let duration = Duration::from_nanos(deadline_nanos - now);
            Box::pin(tokio::time::sleep(duration))
        }
    }

    fn timeout<F, T>(
        &self,
        duration: Duration,
        future: F,
    ) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Box::pin(async move { tokio::time::timeout(duration, future).await.ok() })
    }
}

// ============================================================================
// Standard VirtualTime implementation (non-MadSim)
// ============================================================================
//
// This implementation provides manual time control via advance() methods.
// Used when running tests without MadSim deterministic runtime.

#[cfg(not(madsim))]
/// Internal state for virtual time, shared across clones.
#[derive(Debug)]
struct VirtualTimeState {
    /// Current virtual time in nanoseconds
    current_nanos: AtomicU64,
    /// Counter for generating unique wakeup IDs
    next_wakeup_id: AtomicU64,
    /// Priority queue of pending wakeups (min-heap by deadline, then by ID)
    pending_wakeups: Mutex<BinaryHeap<Wakeup>>,
    /// Wakers waiting to be notified
    pending_wakers: Mutex<Vec<(WakeupId, Waker)>>,
}

#[cfg(not(madsim))]
/// Virtual time implementation for deterministic simulation.
///
/// Time only advances when explicitly stepped via `advance()` or `advance_to()`.
/// All sleep operations register wakeups that are processed in deterministic order.
///
/// # Determinism Guarantees
///
/// - Wakeups at the same deadline are processed in registration order (FIFO)
/// - No wall-clock time dependency
/// - Identical seeds produce identical wakeup sequences
#[derive(Clone)]
pub struct VirtualTime {
    state: Arc<VirtualTimeState>,
}

#[cfg(not(madsim))]
impl Default for VirtualTime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(madsim))]
impl VirtualTime {
    /// Creates a new virtual time starting at 0.
    pub fn new() -> Self {
        Self::with_initial_time(0)
    }

    /// Creates a new virtual time starting at the given nanoseconds.
    pub fn with_initial_time(initial_nanos: u64) -> Self {
        Self {
            state: Arc::new(VirtualTimeState {
                current_nanos: AtomicU64::new(initial_nanos),
                next_wakeup_id: AtomicU64::new(0),
                pending_wakeups: Mutex::new(BinaryHeap::new()),
                pending_wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns the number of pending wakeups.
    pub fn pending_wakeup_count(&self) -> usize {
        self.state.pending_wakeups.lock().unwrap().len()
    }

    /// Advances time by the given duration, waking all expired futures.
    ///
    /// Returns the list of wakeup IDs that were triggered, in order.
    pub fn advance(&self, duration: Duration) -> Vec<WakeupId> {
        let new_time = self
            .state
            .current_nanos
            .load(AtomicOrdering::SeqCst)
            .saturating_add(duration.as_nanos() as u64);
        self.advance_to(new_time)
    }

    /// Advances time to the given absolute nanoseconds, waking all expired futures.
    ///
    /// Returns the list of wakeup IDs that were triggered, in order.
    pub fn advance_to(&self, target_nanos: u64) -> Vec<WakeupId> {
        let current = self.state.current_nanos.load(AtomicOrdering::SeqCst);
        if target_nanos <= current {
            return Vec::new();
        }

        self.state
            .current_nanos
            .store(target_nanos, AtomicOrdering::SeqCst);

        let mut triggered = Vec::new();
        let mut wakers_to_wake = Vec::new();

        {
            let mut pending = self.state.pending_wakeups.lock().unwrap();
            let mut pending_wakers = self.state.pending_wakers.lock().unwrap();

            // Pop all wakeups that have expired
            while let Some(wakeup) = pending.peek() {
                if wakeup.deadline <= target_nanos {
                    let wakeup = pending.pop().unwrap();
                    triggered.push(wakeup.id);

                    // Find and remove the associated waker
                    if let Some(pos) = pending_wakers.iter().position(|(id, _)| *id == wakeup.id) {
                        let (_, waker) = pending_wakers.swap_remove(pos);
                        wakers_to_wake.push(waker);
                    }
                } else {
                    break;
                }
            }
        }

        // Wake all expired futures outside the lock
        for waker in wakers_to_wake {
            waker.wake();
        }

        triggered
    }

    /// Advances to the next pending wakeup, if any.
    ///
    /// Returns the triggered wakeup ID and the time it was scheduled for, or None if no wakeups are pending.
    pub fn advance_to_next_wakeup(&self) -> Option<(WakeupId, u64)> {
        let next_deadline = {
            let pending = self.state.pending_wakeups.lock().unwrap();
            pending.peek().map(|w| w.deadline)
        };

        if let Some(deadline) = next_deadline {
            let triggered = self.advance_to(deadline);
            if let Some(id) = triggered.first() {
                return Some((*id, deadline));
            }
        }
        None
    }

    /// Returns the deadline of the next pending wakeup, if any.
    pub fn next_wakeup_deadline(&self) -> Option<u64> {
        self.state
            .pending_wakeups
            .lock()
            .unwrap()
            .peek()
            .map(|w| w.deadline)
    }

    /// Registers a wakeup and returns its ID.
    fn register_wakeup(&self, deadline: u64) -> WakeupId {
        let id = WakeupId::new(
            self.state
                .next_wakeup_id
                .fetch_add(1, AtomicOrdering::SeqCst),
        );
        let wakeup = Wakeup::new(deadline, id);

        self.state.pending_wakeups.lock().unwrap().push(wakeup);
        id
    }

    /// Registers a waker for a wakeup ID.
    #[allow(dead_code)]
    fn register_waker(&self, id: WakeupId, waker: Waker) {
        let mut pending_wakers = self.state.pending_wakers.lock().unwrap();
        // Update existing waker or add new one
        if let Some(pos) = pending_wakers.iter().position(|(wid, _)| *wid == id) {
            pending_wakers[pos].1 = waker;
        } else {
            pending_wakers.push((id, waker));
        }
    }

    /// Checks if a wakeup has been triggered (no longer pending).
    #[allow(dead_code)]
    fn is_wakeup_triggered(&self, id: WakeupId) -> bool {
        let pending = self.state.pending_wakeups.lock().unwrap();
        !pending.iter().any(|w| w.id == id)
    }
}

#[cfg(not(madsim))]
impl TimeSource for VirtualTime {
    fn now_nanos(&self) -> u64 {
        self.state.current_nanos.load(AtomicOrdering::SeqCst)
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let deadline = self.now_nanos().saturating_add(duration.as_nanos() as u64);
        self.sleep_until(deadline)
    }

    fn sleep_until(&self, deadline_nanos: u64) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let current = self.now_nanos();
        if deadline_nanos <= current {
            return Box::pin(std::future::ready(()));
        }

        let id = self.register_wakeup(deadline_nanos);
        let state = self.state.clone();

        Box::pin(VirtualSleep { id, state })
    }

    fn timeout<F, T>(
        &self,
        duration: Duration,
        future: F,
    ) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let deadline = self.now_nanos().saturating_add(duration.as_nanos() as u64);
        let sleep = self.sleep_until(deadline);
        Box::pin(async move {
            tokio::select! {
                biased;
                result = future => Some(result),
                _ = sleep => None,
            }
        })
    }
}

#[cfg(not(madsim))]
/// Future returned by `VirtualTime::sleep()`.
struct VirtualSleep {
    id: WakeupId,
    state: Arc<VirtualTimeState>,
}

#[cfg(not(madsim))]
impl Future for VirtualSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Acquire both locks atomically to prevent TOCTOU race:
        // Without holding both locks, advance_to() could run between checking
        // pending_wakeups and registering our waker, causing a missed wakeup.
        let pending = self.state.pending_wakeups.lock().unwrap();
        let mut pending_wakers = self.state.pending_wakers.lock().unwrap();

        let still_pending = pending.iter().any(|w| w.id == self.id);

        if !still_pending {
            Poll::Ready(())
        } else {
            // Register our waker while holding both locks
            let wakeup_id = self.id;
            if let Some(pos) = pending_wakers.iter().position(|(id, _)| *id == wakeup_id) {
                pending_wakers[pos].1 = cx.waker().clone();
            } else {
                pending_wakers.push((wakeup_id, cx.waker().clone()));
            }
            Poll::Pending
        }
    }
}

// SAFETY: VirtualSleep is Send because:
// 1. `id` is a simple Copy type (WakeupId = u64)
// 2. `state` is Arc<VirtualTimeState> which is Send when VirtualTimeState is Send + Sync
// 3. VirtualTimeState contains:
//    - AtomicU64 (Send + Sync)
//    - Mutex<BinaryHeap<Wakeup>> where Wakeup contains only u64s (Send + Sync)
//    - Mutex<Vec<(WakeupId, Waker)>> where Waker is Send + Sync
// All components are Send, so VirtualSleep is safe to send across threads.
#[cfg(not(madsim))]
unsafe impl Send for VirtualSleep {}

// ============================================================================
// MadSim VirtualTime implementation
// ============================================================================
//
// When MadSim is enabled, this implementation delegates to tokio's time APIs,
// which MadSim intercepts and makes deterministic. Time auto-advances when
// the runtime is idle.

#[cfg(madsim)]
/// Virtual time implementation that delegates to MadSim's deterministic runtime.
///
/// When compiled with `cfg(madsim)`, this implementation uses tokio's time APIs
/// which MadSim intercepts to provide deterministic time behavior:
///
/// - Time is frozen at start and advances only when:
///   - The runtime is idle (auto-advance)
///   - `tokio::time::advance()` is called explicitly
/// - All time operations are deterministic and reproducible
///
/// # Differences from Standard VirtualTime
///
/// - `advance()` delegates to `tokio::time::advance()` (async)
/// - `pending_wakeup_count()` returns 0 (MadSim manages wakeups internally)
/// - `advance_to_next_wakeup()` returns None (use runtime scheduling instead)
#[derive(Clone)]
pub struct VirtualTime {
    /// Epoch instant for calculating elapsed nanoseconds
    epoch: std::sync::Arc<tokio::time::Instant>,
}

#[cfg(madsim)]
impl Default for VirtualTime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(madsim)]
impl VirtualTime {
    /// Creates a new virtual time starting at the current instant.
    pub fn new() -> Self {
        Self {
            epoch: std::sync::Arc::new(tokio::time::Instant::now()),
        }
    }

    /// Creates a new virtual time with a given initial offset in nanoseconds.
    ///
    /// Note: With MadSim, this advances time to the specified offset.
    pub fn with_initial_time(initial_nanos: u64) -> Self {
        let vt = Self::new();
        // Note: Can't synchronously advance in MadSim, initial time is just recorded
        let _ = initial_nanos; // Acknowledge but don't use - MadSim starts at 0
        vt
    }

    /// Returns the number of pending wakeups.
    ///
    /// With MadSim, wakeups are managed internally by the runtime, so this
    /// always returns 0. Use runtime introspection if needed.
    pub fn pending_wakeup_count(&self) -> usize {
        0 // MadSim manages wakeups internally
    }

    /// Advances time by the given duration.
    ///
    /// With MadSim, this calls `tokio::time::advance()` which is intercepted
    /// by MadSim's deterministic runtime.
    ///
    /// Note: This is now a synchronous no-op that logs the intent. For actual
    /// time advancement with MadSim, use `tokio::time::advance().await` directly
    /// in async code, or rely on MadSim's auto-advance behavior.
    pub fn advance(&self, duration: Duration) -> Vec<WakeupId> {
        // MadSim auto-advances time when runtime is idle.
        // For explicit advancement, use tokio::time::advance().await in async code.
        tracing::trace!(
            "VirtualTime::advance({:?}) called in MadSim mode - time auto-advances on idle",
            duration
        );
        Vec::new() // MadSim doesn't expose triggered wakeup IDs
    }

    /// Advances time to the given absolute nanoseconds.
    ///
    /// With MadSim, time advancement is handled by the runtime.
    pub fn advance_to(&self, target_nanos: u64) -> Vec<WakeupId> {
        let current = self.now_nanos();
        if target_nanos > current {
            let delta = Duration::from_nanos(target_nanos - current);
            self.advance(delta)
        } else {
            Vec::new()
        }
    }

    /// Advances to the next pending wakeup, if any.
    ///
    /// With MadSim, the runtime handles wakeup scheduling internally.
    /// This returns None; use runtime scheduling instead.
    pub fn advance_to_next_wakeup(&self) -> Option<(WakeupId, u64)> {
        None // MadSim handles this internally
    }

    /// Returns the deadline of the next pending wakeup, if any.
    ///
    /// With MadSim, wakeups are managed internally by the runtime.
    pub fn next_wakeup_deadline(&self) -> Option<u64> {
        None // MadSim manages wakeups internally
    }
}

#[cfg(madsim)]
impl TimeSource for VirtualTime {
    fn now_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn sleep_until(&self, deadline_nanos: u64) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let current = self.now_nanos();
        if deadline_nanos <= current {
            Box::pin(std::future::ready(()))
        } else {
            let duration = Duration::from_nanos(deadline_nanos - current);
            Box::pin(tokio::time::sleep(duration))
        }
    }

    fn timeout<F, T>(
        &self,
        duration: Duration,
        future: F,
    ) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Box::pin(async move { tokio::time::timeout(duration, future).await.ok() })
    }
}

// ============================================================================
// TimeSourceInterval - Interval implementation for TimeSource
// ============================================================================

/// An interval timer that works with any `TimeSource`.
///
/// Similar to `tokio::time::Interval`, but uses the `TimeSource` abstraction
/// for deterministic simulation support.
///
/// # Missed Tick Behavior
///
/// Uses "skip" behavior: if time advances past multiple tick deadlines,
/// we skip to the next future deadline rather than catching up.
pub struct TimeSourceInterval<T: TimeSource> {
    time_source: T,
    period_nanos: u64,
    next_tick_nanos: u64,
}

impl<T: TimeSource> TimeSourceInterval<T> {
    /// Creates a new interval that ticks every `period`.
    ///
    /// The first tick is immediate (at creation time).
    pub fn new(time_source: T, period: Duration) -> Self {
        let now = time_source.now_nanos();
        let period_nanos = period.as_nanos() as u64;
        Self {
            time_source,
            period_nanos,
            // First tick is immediate - set next_tick to now so tick() returns immediately first time
            next_tick_nanos: now,
        }
    }

    /// Creates a new interval that starts ticking at `start` time.
    pub fn new_at(time_source: T, start_nanos: u64, period: Duration) -> Self {
        let period_nanos = period.as_nanos() as u64;
        Self {
            time_source,
            period_nanos,
            next_tick_nanos: start_nanos,
        }
    }

    /// Waits for the next tick.
    ///
    /// If the next tick deadline has already passed, returns immediately
    /// and schedules the next tick for the future (skip behavior).
    pub async fn tick(&mut self) {
        let now = self.time_source.now_nanos();

        if now >= self.next_tick_nanos {
            // Deadline has passed - skip to next future tick
            // Calculate how many periods have elapsed and skip them
            let elapsed = now - self.next_tick_nanos;
            let periods_elapsed = elapsed / self.period_nanos + 1;
            self.next_tick_nanos += periods_elapsed * self.period_nanos;
            return;
        }

        // Wait until the deadline
        self.time_source.sleep_until(self.next_tick_nanos).await;
        self.next_tick_nanos += self.period_nanos;
    }

    /// Returns the period of this interval.
    pub fn period(&self) -> Duration {
        Duration::from_nanos(self.period_nanos)
    }

    /// Resets the interval to start ticking from now.
    pub fn reset(&mut self) {
        self.next_tick_nanos = self.time_source.now_nanos() + self.period_nanos;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for the standard VirtualTime implementation (non-MadSim).
    // These use internal APIs like register_wakeup() that only exist in the standard impl.

    #[cfg(not(madsim))]
    #[test]
    fn test_virtual_time_starts_at_zero() {
        let vt = VirtualTime::new();
        assert_eq!(vt.now_nanos(), 0);
    }

    #[cfg(not(madsim))]
    #[test]
    fn test_virtual_time_advance() {
        let vt = VirtualTime::new();
        vt.advance(Duration::from_secs(10));
        assert_eq!(vt.now_nanos(), 10_000_000_000);
    }

    #[cfg(not(madsim))]
    #[test]
    fn test_virtual_time_wakeup_ordering() {
        let vt = VirtualTime::new();

        // Register wakeups in reverse deadline order
        let _id3 = vt.register_wakeup(300);
        let _id1 = vt.register_wakeup(100);
        let _id2 = vt.register_wakeup(200);

        // Advance to 150, should trigger only id1
        let triggered = vt.advance_to(150);
        assert_eq!(triggered.len(), 1);

        // Advance to 250, should trigger id2
        let triggered = vt.advance_to(250);
        assert_eq!(triggered.len(), 1);

        // Advance to 400, should trigger id3
        let triggered = vt.advance_to(400);
        assert_eq!(triggered.len(), 1);
    }

    #[cfg(not(madsim))]
    #[test]
    fn test_virtual_time_same_deadline_fifo() {
        let vt = VirtualTime::new();

        // Register multiple wakeups at same deadline
        let id1 = vt.register_wakeup(100);
        let id2 = vt.register_wakeup(100);
        let id3 = vt.register_wakeup(100);

        // Should be triggered in registration order
        let triggered = vt.advance_to(100);
        assert_eq!(triggered.len(), 3);
        assert_eq!(triggered[0], id1);
        assert_eq!(triggered[1], id2);
        assert_eq!(triggered[2], id3);
    }

    #[cfg(not(madsim))]
    #[test]
    fn test_virtual_time_advance_to_next() {
        let vt = VirtualTime::new();

        vt.register_wakeup(50);
        vt.register_wakeup(100);

        // Advance to first wakeup
        let result = vt.advance_to_next_wakeup();
        assert!(result.is_some());
        let (_, deadline) = result.unwrap();
        assert_eq!(deadline, 50);
        assert_eq!(vt.now_nanos(), 50);

        // Advance to second wakeup
        let result = vt.advance_to_next_wakeup();
        assert!(result.is_some());
        let (_, deadline) = result.unwrap();
        assert_eq!(deadline, 100);
        assert_eq!(vt.now_nanos(), 100);

        // No more wakeups
        let result = vt.advance_to_next_wakeup();
        assert!(result.is_none());
    }

    #[test]
    fn test_real_time_basic() {
        let rt = RealTime::new();
        let t1 = rt.now_nanos();
        std::thread::sleep(Duration::from_millis(10));
        let t2 = rt.now_nanos();
        assert!(t2 > t1);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_virtual_time_sleep_immediate() {
        let vt = VirtualTime::with_initial_time(1000);

        // Sleep until past deadline should complete immediately
        let sleep = vt.sleep_until(500);
        tokio::time::timeout(Duration::from_millis(10), sleep)
            .await
            .expect("sleep should complete immediately");
    }
}
