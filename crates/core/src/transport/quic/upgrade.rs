//! Upgrading a UDP transport session to a QUIC path (#5940, step e5a).
//!
//! The policy ([`policy`]) says whether the node may upgrade, the state machine
//! ([`machine`]) decides each step of a negotiation, and the driver ([`driver`])
//! carries those steps out for every UDP session of a node. A pair negotiates
//! data only inside its release's slice ([`slice`]). No code sends data on a path
//! yet: a path carries control frames, and application traffic stays on UDP.

pub(in crate::transport) mod driver;
pub(in crate::transport) mod machine;
pub(crate) mod policy;
#[cfg(any(test, feature = "testing"))]
pub(crate) mod sim_trace;
pub(in crate::transport) mod slice;
