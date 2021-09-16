// #![allow(unused)] // FIXME: remove this attr

use std::{convert::TryFrom, fmt::Display, hash::Hasher};

use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{self, PeerKey, PeerKeyLocation},
    StdResult,
};

type Result<T> = StdResult<T, RingProtoError>;

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct Location(f64);

type Distance = Location;

impl Location {
    /// Returns a new random location.
    pub fn random() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }

    /// Compute the distance between two locations.
    pub fn distance(&self, other: &Location) -> Distance {
        let d = (self.0 - other.0).abs();
        if d < 0.5 {
            Location(d)
        } else {
            Location(1.0 - d)
        }
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.to_string().as_str())?;
        Ok(())
    }
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// Since we don't allow NaN values in the construction of Location
/// we can safely assume that an equivalence relation holds.  
impl Eq for Location {}

impl Ord for Location {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("always should return a cmp value")
    }
}

impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::hash::Hash for Location {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits = self.0.to_bits();
        state.write_u64(bits);
        state.finish();
    }
}

impl TryFrom<f64> for Location {
    type Error = ();

    fn try_from(value: f64) -> StdResult<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            Err(())
        } else {
            Ok(Location(value))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingProtoError {
    #[error("failed while attempting to join a ring")]
    Join,
    #[error(transparent)]
    ConnError(#[from] conn_manager::ConnError),
}

pub(crate) mod messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinRequest {
        Initial {
            key: PeerKey,
            hops_to_live: usize,
        },
        Proxy {
            joiner: PeerKeyLocation,
            hops_to_live: usize,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinResponse {
        Initial {
            accepted_by: Vec<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        Proxy {
            accepted_by: Vec<PeerKeyLocation>,
        },
    }

    /// A stateful connection attempt.
    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum OpenConnection {
        OCReceived,
        Connecting,
        Connected,
    }

    impl OpenConnection {
        pub fn is_initiated(&self) -> bool {
            matches!(self, OpenConnection::Connecting)
        }

        pub fn is_connected(&self) -> bool {
            matches!(self, OpenConnection::Connected)
        }

        pub(super) fn transition(&mut self, other_host_state: Self) {
            match (*self, other_host_state) {
                (Self::Connected, _) => {}
                (_, Self::Connecting) => *self = Self::OCReceived,
                (_, Self::OCReceived | Self::Connected) => *self = Self::Connected,
            }
        }
    }

    impl Display for OpenConnection {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "OpenConnection::{:?}", self)
        }
    }
}
