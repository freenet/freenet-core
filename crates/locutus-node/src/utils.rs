use std::collections::{BTreeMap, HashSet};

use rand::{SeedableRng, prelude::{Rng, StdRng}};

// This is extremely inefficient for large sizes but is not what
// we are really using this for so this ok for now.
// todo: if necessary implement in the future randomization via `modular multiplicative inverse`
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

pub(crate) trait ExtendedIter: Iterator
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

impl<T> ExtendedIter for T where T: Iterator {}

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
