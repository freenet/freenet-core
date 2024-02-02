use std::sync::atomic::{AtomicBool, AtomicPtr};
use std::sync::Arc;
use std::time::SystemTime;
use std::{
    collections::{BTreeMap, HashSet},
    time::{Duration, Instant},
};

use rand::{
    prelude::{Rng, StdRng},
    SeedableRng,
};

use crate::node::PeerId;

pub fn set_cleanup_on_exit() -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        tracing::info!("Received Ctrl+C. Cleaning up...");

        let Ok(path) = crate::config::ConfigPaths::app_data_dir() else {
            std::process::exit(0);
        };
        tracing::info!("Removing content stored at {path:?}");

        if path.exists() {
            let rm = std::fs::remove_dir_all(&path).map_err(|err| {
                tracing::warn!("Failed cleaning up directory: {err}");
                err
            });
            if rm.is_err() {
                tracing::error!("Failed to remove content at {path:?}");
                std::process::exit(-1);
            }
        }
        tracing::info!("Successful cleanup");

        std::process::exit(0);
    })
}

pub struct ExponentialBackoff {
    attempt: usize,
    max_attempts: usize,
    base: Duration,
    ceiling: Duration,
}

impl ExponentialBackoff {
    pub fn new(base: Duration, ceiling: Duration, max_attempts: usize) -> Self {
        ExponentialBackoff {
            attempt: 0,
            max_attempts,
            base,
            ceiling,
        }
    }

    /// Record that we made an attempt and sleep for the appropriate amount
    /// of time. If the max number of attempts was reached returns none.
    pub async fn sleep(&mut self) -> Option<()> {
        if self.attempt == self.max_attempts {
            None
        } else {
            tokio::time::sleep(self.next_attempt()).await;
            Some(())
        }
    }

    pub fn retries(&self) -> usize {
        self.attempt
    }

    fn delay(&self) -> Duration {
        let mut delay = self.base.saturating_mul(1 << self.attempt);
        if delay > self.ceiling {
            delay = self.ceiling;
        }
        delay
    }

    fn next_attempt(&mut self) -> Duration {
        let delay = self.delay();
        self.attempt += 1;
        delay
    }
}

// This is extremely inefficient for large sizes but is not what
// we are really using this for so this ok for now.
// TODO: if necessary implement in the future randomization via `modular multiplicative inverse` method
pub(crate) struct Shuffle<Iter, T>
where
    Iter: Iterator<Item = T>,
{
    inner: Iter,
    rng: StdRng,
    memorized: BTreeMap<usize, T>,
    done: HashSet<usize>,
    done_counter: usize,
    counter: usize,
    size: usize,
}

impl<Iter, T> Iterator for Shuffle<Iter, T>
where
    Iter: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.counter == self.size {
            return None;
        }
        let pick = loop {
            let pick = self.rng.gen_range(0..self.size);
            if !self.done.contains(&pick) {
                self.done.insert(pick);
                break pick;
            }
        };
        if let Some(element) = self.memorized.remove(&pick) {
            self.counter += 1;
            Some(element)
        } else {
            for i in self.done_counter..=pick {
                if let Some(e) = self.inner.next() {
                    self.memorized.insert(i, e);
                }
            }
            self.done_counter = pick + 1;
            self.counter += 1;
            self.memorized.remove(&pick)
        }
    }
}

pub(crate) trait IterExt: Iterator
where
    Self: Sized,
{
    /// # Panic
    /// Requires that the size of the iterator is known and exact, panics otherwise.
    fn shuffle(self) -> Shuffle<Self, <Self as Iterator>::Item> {
        let (size, upper) = self.size_hint();
        assert!(matches!(upper, Some(s) if s == size));
        Shuffle {
            inner: self,
            rng: StdRng::from_entropy(),
            memorized: BTreeMap::new(),
            done: HashSet::with_capacity(size),
            done_counter: 0,
            counter: 0,
            size,
        }
    }
}

impl<T> IterExt for T where T: Iterator {}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use rand::Rng;

    #[test]
    fn randomize_iter() {
        let iter = [0, 1, 2, 3, 4, 5];
        let mut times_equal = 0;
        for _ in 0..100 {
            let shuffled: Vec<_> = iter.into_iter().shuffle().collect();
            assert_eq!(shuffled.len(), iter.len());
            //     println!("shuffled: {:?}", shuffled);
            if iter.as_ref() == shuffled {
                times_equal += 1;
            }
        }
        // probability of being equal are 1/720
        // for 100 iterations the probability that it happens more than twice is slim
        assert!(times_equal < 3);
    }

    macro_rules! rnd_bytes {
        ($size:tt -> $name:tt) => {
            #[inline]
            #[allow(unused_braces)]
            pub(crate) fn $name() -> [u8; $size] {
                let mut rng = rand::thread_rng();
                let mut rnd_bytes = [0u8; $size];
                rng.fill(rnd_bytes.as_mut_slice());
                rnd_bytes
            }
        };
        (large: $size:tt -> $name:tt) => {
            #[inline]
            #[allow(unused_braces)]
            pub(crate) fn $name() -> Vec<u8> {
                let mut rng = rand::thread_rng();
                let mut rnd_bytes = vec![0u8; $size];
                rng.fill(rnd_bytes.as_mut_slice());
                rnd_bytes
            }
        };
    }

    rnd_bytes!(1024 -> random_bytes_1kb);
    rnd_bytes!(large: { 1024 * 1024 * 2 } -> random_bytes_2mb);
}

#[derive(Clone, Copy, serde::Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum EncodingProtocol {
    /// Flatbuffers
    Flatbuffers,
    /// Rust native types
    Native,
}

impl std::fmt::Display for EncodingProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingProtocol::Flatbuffers => write!(f, "flatbuffers"),
            EncodingProtocol::Native => write!(f, "native"),
        }
    }
}

pub(crate) trait Contains<T> {
    fn has_element(&self, target: &T) -> bool;
}

impl<'x> Contains<PeerId> for &'x [PeerId] {
    fn has_element(&self, target: &PeerId) -> bool {
        self.contains(target)
    }
}

impl<'x> Contains<PeerId> for &'x [&PeerId] {
    fn has_element(&self, target: &PeerId) -> bool {
        self.contains(&target)
    }
}

impl<'x> Contains<PeerId> for &'x Vec<&PeerId> {
    fn has_element(&self, target: &PeerId) -> bool {
        self.contains(&target)
    }
}

pub trait TimeSource {
    fn now(&self) -> Instant;
}

// A time source that caches the current time in a global state to reduce overhead in performance-critical sections.
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_timesources() {
        let mut ts = vec![];
        for i in 1..100 {
            ts.push(CachingSystemTimeSrc::new());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_now_returns_instant() {
        let time_source = CachingSystemTimeSrc::new();
        let now = time_source.now();
        assert!(now.elapsed() >= Duration::from_secs(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_instant_is_updated() {
        let time_source = CachingSystemTimeSrc::new();
        let first_instant = time_source.now();
        tokio::time::sleep(Duration::from_millis(120)).await;
        let second_instant = time_source.now();
        assert!(second_instant > first_instant);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caching_system_time_is_thread_safe() {
        let mut prev = Instant::now();
        let time_source = CachingSystemTimeSrc::new();
        let mut handles = vec![];
        for _ in 0..10 {
            handles.push(std::thread::spawn(move || {
                let time = Instant::now();
                while time.elapsed() < Duration::from_secs(1) {
                    let now = time_source.now();
                    assert!(prev <= now);
                    prev = now;
                    std::thread::sleep(Duration::from_millis(25));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    /// Use this to guarantee unique directory names in case you are running multiple tests in parallel.
    pub fn get_temp_dir() -> TempDir {
        let dir = tempfile::Builder::new()
            .tempdir()
            .expect("Failed to create a temporary directory");
        // eprintln!("Created temp dir: {:?}", dir.path());
        dir
    }
}
