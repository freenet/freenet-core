use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use rand::{
    prelude::{Rng, StdRng},
    SeedableRng,
};

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
    pub async fn sleep_async(&mut self) -> Option<()> {
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

#[cfg(test)]
pub(crate) mod test {
    use rand::Rng;

    // Would be nice to use const generics for this but there is a bound for the allowed
    // array size parameter which cannot be expressed with current version of const generics.
    // So using this macro hack.
    macro_rules! rnd_bytes {
        ($size:tt -> $name:tt) => {
            #[inline]
            pub(crate) fn $name() -> [u8; $size] {
                let mut rng = rand::thread_rng();
                let mut rnd_bytes = [0u8; $size];
                rng.fill(&mut rnd_bytes);
                rnd_bytes
            }
        };
    }

    rnd_bytes!(128 -> random_bytes_128);
    #[cfg(test)]
    rnd_bytes!(1024 -> random_bytes_1024);
}
