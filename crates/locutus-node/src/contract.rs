//! Main abstraction for representing a contract in binary form.
use std::convert::TryFrom;

use blake2::{Blake2b, Digest};

use crate::ring::Location;

#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct Contract {
    data: Vec<u8>,
    key: [u8; 64],
}

impl Contract {
    fn new(data: Vec<u8>) -> Self {
        let mut hasher = Blake2b::new();
        hasher.update(&data);
        let key_arr = hasher.finalize();
        debug_assert_eq!((&key_arr[..]).len(), 64);
        let mut key = [0; 64];
        key.copy_from_slice(&key_arr);

        Self { data, key }
    }

    fn assigned_location(&self) -> Location {
        Location::from(self.key.as_ref())
    }
}
