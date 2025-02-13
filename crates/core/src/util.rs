pub(crate) mod time_source;

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    hash::Hash,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};

use crate::{config::ConfigPaths, node::PeerId};
use rand::{
    prelude::{Rng, StdRng},
    SeedableRng,
};

pub fn set_cleanup_on_exit(config: Arc<ConfigPaths>) -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        tracing::info!("Received Ctrl+C. Cleaning up...");

        #[cfg(debug_assertions)]
        {
            let paths = config.iter();
            for (is_dir, path) in paths {
                if path.exists() {
                    tracing::info!("Removing content stored at {path:?}");
                    let rm = if is_dir {
                        std::fs::remove_dir_all(path).map_err(|err| {
                            tracing::warn!("Failed cleaning up directory: {err}");
                            err
                        })
                    } else {
                        std::fs::remove_file(path).map_err(|err| {
                            tracing::warn!("Failed cleaning up file: {err}");
                            err
                        })
                    };

                    match rm {
                        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                            tracing::error!("Failed to remove directory at {path:?}");
                            std::process::exit(-1);
                        }
                        _ => {}
                    }
                }
            }
        }
        let _ = config;
        tracing::info!("Successful cleanup");

        std::process::exit(0);
    })
}

#[derive(Debug)]
pub struct Backoff {
    attempt: usize,
    max_attempts: usize,
    base: Duration,
    ceiling: Duration,
    strategy: BackoffStrategy,
    interval_reduction_factor: Option<f64>,
}

#[derive(Debug)]
enum BackoffStrategy {
    Exponential,
    Logarithmic,
}

impl Backoff {
    pub fn new(base: Duration, ceiling: Duration, max_attempts: usize) -> Self {
        Backoff {
            attempt: 0,
            max_attempts,
            base,
            ceiling,
            strategy: BackoffStrategy::Exponential,
            interval_reduction_factor: None,
        }
    }

    pub fn logarithmic(mut self) -> Self {
        self.strategy = BackoffStrategy::Logarithmic;
        self
    }

    pub fn with_interval_reduction_factor(mut self, factor: f64) -> Self {
        self.interval_reduction_factor = Some(factor);
        self
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
        let mut delay = match self.strategy {
            BackoffStrategy::Exponential => self.exponential_delay(),
            BackoffStrategy::Logarithmic => self.logarithmic_delay(),
        };
        if delay > self.ceiling {
            delay = self.ceiling;
        }
        delay
    }

    fn exponential_delay(&self) -> Duration {
        self.base.saturating_mul(1 << self.attempt)
    }

    fn logarithmic_delay(&self) -> Duration {
        const LOG_BASE: f64 = 2.0;
        const INTERVAL_REDUCTION_FACTOR: f64 = 1.0;
        Duration::from_millis(
            (self.base.as_millis() as f64 * (1.0 + (self.attempt as f64).log(LOG_BASE))
                / self
                    .interval_reduction_factor
                    .unwrap_or(INTERVAL_REDUCTION_FACTOR)) as u64,
        )
    }

    fn next_attempt(&mut self) -> Duration {
        let delay = self.delay();
        self.attempt += 1;
        delay
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.attempt == self.max_attempts {
            None
        } else {
            Some(self.next_attempt())
        }
    }
}

#[allow(clippy::result_unit_err)]
pub fn get_free_port() -> Result<u16, ()> {
    let mut port;
    for _ in 0..100 {
        port = get_dynamic_port();
        let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        if let Ok(conn) = TcpListener::bind(bind_addr) {
            std::mem::drop(conn);
            return Ok(port);
        }
    }
    Err(())
}

fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    rand::thread_rng().gen_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
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

    #[test]
    fn backoff_logarithmic() {
        let base = Duration::from_millis(200);
        let ceiling = Duration::from_secs(2);
        let max_attempts = 40;
        let backoff = Backoff::new(base, ceiling, max_attempts)
            .logarithmic()
            .with_interval_reduction_factor(2.0);
        let total = backoff
            .into_iter()
            .reduce(|acc, x| {
                // println!("next: {:?}", x);
                acc + x
            })
            .unwrap();
        assert!(
            total < Duration::from_secs(18) && total > Duration::from_secs(20),
            "total: {:?}",
            total
        );

        let base = Duration::from_millis(600);
        let ceiling = Duration::from_secs(30);
        let max_attempts = 40;
        let backoff = Backoff::new(base, ceiling, max_attempts).logarithmic();

        // const MAX: Duration = Duration::from_secs(30);
        let _ = backoff
            .into_iter()
            .reduce(|acc, x| {
                // println!("next: {:?}", x);
                acc + x
            })
            .unwrap();
        // println!("total: {:?}", total);
    }

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
    fn has_element(&self, target: T) -> bool;
}

impl Contains<PeerId> for &[PeerId] {
    fn has_element(&self, target: PeerId) -> bool {
        self.contains(&target)
    }
}

impl Contains<&PeerId> for &[PeerId] {
    fn has_element(&self, target: &PeerId) -> bool {
        self.contains(target)
    }
}

impl Contains<PeerId> for &[&PeerId] {
    fn has_element(&self, target: PeerId) -> bool {
        self.contains(&&target)
    }
}

impl Contains<&PeerId> for &[&PeerId] {
    fn has_element(&self, target: &PeerId) -> bool {
        self.contains(&target)
    }
}

impl<Q, T> Contains<Q> for &HashSet<T>
where
    T: Borrow<Q> + Eq + Hash,
    Q: Eq + Hash,
{
    fn has_element(&self, target: Q) -> bool {
        self.contains(&target)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use tempfile::TempDir;

    /// Use this to guarantee unique directory names in case you are running multiple tests in parallel.
    pub fn get_temp_dir() -> TempDir {
        let dir = tempfile::Builder::new()
            .tempdir()
            .expect("Failed to create a temporary directory");
        // eprintln!("Created temp dir: {:?}", dir.path());
        dir
    }
}
