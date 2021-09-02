use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::ring_proto::Location;

// TODO: check if this aapproach is a good one:
// right now using a trait and passing trait objects in call backs;
// in the Kotlin version the handler is reified with the type (allowing monomorphization)
// however as we are also making conn manager a trait object (for simplicity) we cannot
// use a trait bound along with a generic type...
// may change to an enum later on if it makes sense or... if we don't need ConnectionManager
// to be dynamically dispatch we can go back to use generics for Message types

pub(crate) trait Message {}

#[derive(Serialize, Deserialize)]
pub(crate) struct ProbeRequest;

impl Message for ProbeRequest {}

#[derive(Serialize, Deserialize)]
pub(crate) struct ProbeResponse {
    pub visits: Vec<Visit>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Visit {
    pub hop: u8,
    pub latency: Duration,
    pub location: Location,
}
