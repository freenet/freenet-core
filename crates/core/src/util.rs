pub(crate) mod backoff;
pub mod deterministic_select;
pub(crate) mod rate_limit_layer;
pub(crate) mod time_source;
pub(crate) mod workspace;

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    hash::Hash,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::Arc,
};

use crate::{
    config::{ConfigPaths, GlobalRng},
    node::PeerId,
};
use rand::{
    prelude::{Rng, StdRng},
    SeedableRng,
};

use std::sync::atomic::{AtomicBool, Ordering};

static CLEANUP_HANDLER_SET: AtomicBool = AtomicBool::new(false);

pub fn set_cleanup_on_exit(config: Arc<ConfigPaths>) -> Result<(), ctrlc::Error> {
    // Only set the handler once per process
    if CLEANUP_HANDLER_SET
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        // Handler already set, skip
        return Ok(());
    }

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
    GlobalRng::random_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
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
            let pick = self.rng.random_range(0..self.size);
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
            rng: StdRng::seed_from_u64(GlobalRng::random_u64()),
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
                use crate::config::GlobalRng;
                let mut rnd_bytes = [0u8; $size];
                GlobalRng::fill_bytes(&mut rnd_bytes);
                rnd_bytes
            }
        };
        (large: $size:tt -> $name:tt) => {
            #[inline]
            #[allow(unused_braces)]
            pub(crate) fn $name() -> Vec<u8> {
                use crate::config::GlobalRng;
                let mut rnd_bytes = vec![0u8; $size];
                GlobalRng::fill_bytes(&mut rnd_bytes);
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

impl Contains<std::net::SocketAddr> for &[std::net::SocketAddr] {
    fn has_element(&self, target: std::net::SocketAddr) -> bool {
        self.contains(&target)
    }
}

impl Contains<&std::net::SocketAddr> for &[std::net::SocketAddr] {
    fn has_element(&self, target: &std::net::SocketAddr) -> bool {
        self.contains(target)
    }
}

impl Contains<std::net::SocketAddr> for &Vec<std::net::SocketAddr> {
    fn has_element(&self, target: std::net::SocketAddr) -> bool {
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
