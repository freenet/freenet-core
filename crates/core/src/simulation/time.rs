//! Time abstraction layer for deterministic simulation.
//!
//! This module provides:
//! - `TimeSource` trait for abstracting time operations
//! - `RealTime` implementation delegating to tokio
//! - `VirtualTime` implementation for deterministic simulation

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

/// Unique identifier for a wakeup registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WakeupId(u64);

impl WakeupId {
    fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the inner ID value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// A pending wakeup in the virtual time system.
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

impl Wakeup {
    fn new(deadline: u64, id: WakeupId) -> Self {
        Self {
            deadline,
            id,
            waker: None,
        }
    }
}

impl PartialEq for Wakeup {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}

impl Eq for Wakeup {}

impl PartialOrd for Wakeup {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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
    fn timeout<F, T>(&self, duration: Duration, future: F) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;
}

/// Real-time implementation that delegates to tokio.
#[derive(Clone)]
pub struct RealTime {
    epoch: std::time::Instant,
}

impl Default for RealTime {
    fn default() -> Self {
        Self::new()
    }
}

impl RealTime {
    pub fn new() -> Self {
        Self {
            epoch: std::time::Instant::now(),
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

    fn timeout<F, T>(&self, duration: Duration, future: F) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Box::pin(async move {
            tokio::time::timeout(duration, future).await.ok()
        })
    }
}

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

impl Default for VirtualTime {
    fn default() -> Self {
        Self::new()
    }
}

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

        Box::pin(VirtualSleep {
            id,
            state,
            registered: false,
        })
    }

    fn timeout<F, T>(&self, duration: Duration, future: F) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
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

/// Future returned by `VirtualTime::sleep()`.
struct VirtualSleep {
    id: WakeupId,
    state: Arc<VirtualTimeState>,
    registered: bool,
}

impl Future for VirtualSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check if our wakeup has been triggered
        let pending = self.state.pending_wakeups.lock().unwrap();
        let still_pending = pending.iter().any(|w| w.id == self.id);
        drop(pending);

        if !still_pending {
            Poll::Ready(())
        } else {
            // Register our waker
            if !self.registered {
                self.registered = true;
            }
            let mut pending_wakers = self.state.pending_wakers.lock().unwrap();
            if let Some(pos) = pending_wakers.iter().position(|(id, _)| *id == self.id) {
                pending_wakers[pos].1 = cx.waker().clone();
            } else {
                pending_wakers.push((self.id, cx.waker().clone()));
            }
            Poll::Pending
        }
    }
}

// Ensure VirtualSleep is Send
unsafe impl Send for VirtualSleep {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_virtual_time_starts_at_zero() {
        let vt = VirtualTime::new();
        assert_eq!(vt.now_nanos(), 0);
    }

    #[test]
    fn test_virtual_time_advance() {
        let vt = VirtualTime::new();
        vt.advance(Duration::from_secs(10));
        assert_eq!(vt.now_nanos(), 10_000_000_000);
    }

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
