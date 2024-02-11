use std::sync::atomic::{AtomicBool, AtomicPtr};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub trait TimeSource {
    fn now(&self) -> Instant;
}

/// A time source that caches the current time in a global state to reduce
/// overhead in performance-critical sections.
///
/// **Note**: This time source will only be accurate to within about 20ms.
///
/// **Warning**: If the Tokio runtime is restarted then the time updater task
/// will stop and **will not** be restarted. This should not be an issue in
/// practice as the Tokio runtime should only be started once.
#[derive(Clone, Copy)]
pub(crate) struct CachingSystemTimeSrc(());

// Global atomic pointer to the cached time. Initialized as a null pointer.
static GLOBAL_TIME_STATE: AtomicPtr<Instant> = AtomicPtr::new(std::ptr::null_mut());

impl CachingSystemTimeSrc {
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
            tokio::spawn(Self::update_instant(drop_guard.clone()));

            // Wait until the updater task signals it's safe to proceed.
            while !drop_guard.load(std::sync::atomic::Ordering::Acquire) {
                std::hint::spin_loop();
            }
        }

        CachingSystemTimeSrc(())
    }

    // Asynchronously updates the global time state every 20ms.
    async fn update_instant(drop_guard: Arc<AtomicBool>) {
        let mut now = Instant::now();

        // Initially set the global time state and notify the constructor to proceed.
        GLOBAL_TIME_STATE.store(&mut now, std::sync::atomic::Ordering::Release);
        drop_guard.store(true, std::sync::atomic::Ordering::Release);

        loop {
            // Update the time and store it in the global state.
            now = Instant::now();
            GLOBAL_TIME_STATE.store(&mut now, std::sync::atomic::Ordering::Release);

            // Wait for 20ms before the next update.
            tokio::time::sleep(Duration::from_millis(20)).await;
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    /*
     * If the Tokio runtime is restarted, as in the case of running multiple
     * tests in parallel, the global state will be reset and the updater task
     * will be killed and not respawned, leading to the time not updating,
     * so it's important that there is only one Tokio runtime. This shouldn't
     * be an issue in practice.
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_instant_is_updated() {
        let time_source = CachingSystemTimeSrc::new();
        let first_instant = time_source.now();

        assert!(first_instant.elapsed().as_millis() < 30);

        tokio::time::sleep(Duration::from_millis(120)).await;
        let second_instant = time_source.now();

        assert!(second_instant.elapsed().as_millis() < 30);

        assert!(second_instant > first_instant);
    }
}
