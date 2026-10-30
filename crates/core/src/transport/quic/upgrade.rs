//! Upgrading a UDP transport session to a QUIC path (#5940, step e5a).
//!
//! The policy ([`policy`]) and the negotiation's state machine ([`machine`])
//! exist so far. The wiring into the peer connection comes in a later step;
//! until then nothing in the node reads either, and no session is upgraded.

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::transport) mod machine;
pub(crate) mod policy;
