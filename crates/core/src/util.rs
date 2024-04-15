pub(crate) mod time_source;

use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
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
