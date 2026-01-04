use std::sync::atomic::{AtomicBool, AtomicPtr};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

const UPDATE_CACHED_TIME_EVERY: Duration = Duration::from_millis(10);

pub trait TimeSource {
    fn now(&self) -> Instant;
}

/// A simple time source that returns the current time using `Instant::now()`.
#[derive(Clone, Copy)]
pub struct InstantTimeSrc(());

impl InstantTimeSrc {
    pub fn new() -> Self {
        InstantTimeSrc(())
    }
}

impl TimeSource for InstantTimeSrc {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

// Implement simulation::TimeSource for backward compatibility with InstantTimeSrc.
// NOTE: This is currently unused since InstantTimeSrc is only used with the old
// TimeSource trait (not simulation::TimeSource). We keep this implementation for
// potential future use and to ensure InstantTimeSrc could be used with components
// that expect simulation::TimeSource. The commented-out implementation is preserved
// for reference if this ever needs to be enabled.
//
// In the future, if InstantTimeSrc needs to be used with simulation::TimeSource,
// this implementation would need to be fixed to use a proper epoch (like RealTime
// does) instead of `Instant::now().elapsed()` which always returns ~0.
/*
impl crate::simulation::TimeSource for InstantTimeSrc {
    fn now_nanos(&self) -> u64 {
        // Would need to use an epoch like RealTime does
        // Currently this is buggy and would return ~0
        let elapsed = Instant::now().elapsed();
        elapsed.as_nanos() as u64
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn sleep_until(&self, deadline_nanos: u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let now = Instant::now().elapsed().as_nanos() as u64;
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<T>> + Send>>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Box::pin(async move { tokio::time::timeout(duration, future).await.ok() })
    }
}
*/

/// A time source that caches the current time in a global state to reduce
/// overhead in performance-critical sections.
///
/// **Warning**: This time source will only be accurate to within about 20ms,
/// any usage should be tested carefully to verify that this inaccuracy is acceptable.
/// In the absence of such testing use [`InstantTimeSrc`] instead.
#[derive(Clone, Copy)]
pub(crate) struct CachingSystemTimeSrc(());

// Global atomic pointer to the cached time. Initialized as a null pointer.
static GLOBAL_TIME_STATE: AtomicPtr<Instant> = AtomicPtr::new(std::ptr::null_mut());

impl CachingSystemTimeSrc {
    #![allow(unused)]
    // Creates a new instance and ensures only one updater task is spawned.
    pub(crate) fn new() -> Self {
        let mut current_unix_epoch_ts = Instant::now();

        // Attempt to set the global time state if it's currently null.
        // This ensures only the first thread to execute this will spawn the updater task.
        if GLOBAL_TIME_STATE
            .compare_exchange(
                std::ptr::null_mut(),
                (&mut current_unix_epoch_ts) as *mut _,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            // Use a flag to synchronize the updater task's initialization.
            let drop_guard = Arc::new(AtomicBool::new(false));

            // Spawn the updater task asynchronously.
            let drop_guard_clone = drop_guard.clone();
            thread::spawn(move || Self::update_instant(drop_guard_clone));

            // Wait until the updater task signals it's safe to proceed.
            while !drop_guard.load(std::sync::atomic::Ordering::Acquire) {
                std::hint::spin_loop();
            }
        }

        CachingSystemTimeSrc(())
    }

    // Asynchronously updates the global time state every UPDATE_CACHED_TIME_EVERY (10ms).
    fn update_instant(drop_guard: Arc<AtomicBool>) {
        let mut now = Instant::now();

        // Initially set the global time state and notify the constructor to proceed.
        GLOBAL_TIME_STATE.store(&mut now, std::sync::atomic::Ordering::Release);
        drop_guard.store(true, std::sync::atomic::Ordering::Release);

        loop {
            // Update the time and store it in the global state.
            now = Instant::now();
            GLOBAL_TIME_STATE.store(&mut now, std::sync::atomic::Ordering::Release);

            // Wait for 20ms before the next update.
            sleep(UPDATE_CACHED_TIME_EVERY);
        }
    }
}

impl TimeSource for CachingSystemTimeSrc {
    // Returns the current time from the global state.
    fn now(&self) -> Instant {
        // Unsafe dereference is required for the raw pointer.
        unsafe { *GLOBAL_TIME_STATE.load(std::sync::atomic::Ordering::Acquire) }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub struct MockTimeSource {
    current_instant: Instant,
}

#[cfg(test)]
impl MockTimeSource {
    pub fn new(start_instant: Instant) -> Self {
        MockTimeSource {
            current_instant: start_instant,
        }
    }

    pub fn advance_time(&mut self, duration: Duration) {
        self.current_instant += duration;
    }
}

#[cfg(test)]
impl TimeSource for MockTimeSource {
    fn now(&self) -> Instant {
        self.current_instant
    }
}

/// A shared mock time source for testing components that need external time control.
///
/// Unlike `MockTimeSource`, this variant uses `Arc<Mutex<Instant>>` internally,
/// allowing multiple clones to share the same underlying time. This enables tests
/// to advance time after creating a component that holds a clone of the time source.
///
/// # Example
/// ```ignore
/// let time_source = SharedMockTimeSource::new();
/// let component = Component::new(time_source.clone());
///
/// // Advance time after component creation
/// time_source.advance_time(Duration::from_secs(60));
///
/// // Component now sees the advanced time
/// component.do_something_time_dependent();
/// ```
#[cfg(test)]
#[derive(Clone)]
pub struct SharedMockTimeSource {
    /// The epoch when time started (used as baseline for elapsed calculations)
    epoch: Instant,
    /// Current elapsed time from the epoch
    elapsed: Arc<std::sync::Mutex<Duration>>,
}

#[cfg(test)]
impl SharedMockTimeSource {
    /// Create a new shared mock time source starting at zero elapsed time.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            elapsed: Arc::new(std::sync::Mutex::new(Duration::ZERO)),
        }
    }

    /// Create a new shared mock time source with a specific initial elapsed time.
    pub fn with_instant(start: Instant) -> Self {
        Self {
            epoch: start,
            elapsed: Arc::new(std::sync::Mutex::new(Duration::ZERO)),
        }
    }

    /// Advance the shared time by the given duration.
    ///
    /// All clones of this time source will see the advanced time.
    pub fn advance_time(&self, duration: Duration) {
        let mut guard = self.elapsed.lock().unwrap();
        *guard += duration;
    }

    /// Get the current time value (for assertions).
    #[allow(dead_code)] // Utility for future test expansion
    pub fn current_time(&self) -> Instant {
        self.epoch + *self.elapsed.lock().unwrap()
    }
}

#[cfg(test)]
impl Default for SharedMockTimeSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl TimeSource for SharedMockTimeSource {
    fn now(&self) -> Instant {
        self.epoch + *self.elapsed.lock().unwrap()
    }
}

#[cfg(test)]
impl crate::simulation::TimeSource for SharedMockTimeSource {
    fn now_nanos(&self) -> u64 {
        let elapsed = *self.elapsed.lock().unwrap();
        elapsed.as_nanos() as u64
    }

    fn sleep(
        &self,
        _duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        // For testing with SharedMockTimeSource, we need to manually advance time
        // This is a simplified implementation that doesn't actually sleep
        // Tests using SharedMockTimeSource should call advance_time() manually
        Box::pin(async move {
            // Sleeping is a no-op in SharedMockTimeSource - tests must advance time manually
        })
    }

    fn sleep_until(
        &self,
        _deadline_nanos: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        // Similar to sleep - tests must advance time manually
        Box::pin(async move {
            // Tests must advance time manually to trigger this deadline
        })
    }

    fn timeout<F, T>(
        &self,
        _duration: Duration,
        future: F,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<T>> + Send>>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // For tests with SharedMockTimeSource, timeout is non-functional
        // Tests should not rely on timeout behavior with this mock
        Box::pin(async move { Some(future.await) })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_instant_is_updated() {
        let time_source = CachingSystemTimeSrc::new();
        let first_instant = time_source.now();

        assert!(first_instant.elapsed().as_millis() < 30);

        sleep(Duration::from_millis(120));
        let second_instant = time_source.now();

        assert!(second_instant.elapsed().as_millis() < 30);

        assert!(second_instant > first_instant);
    }
}
