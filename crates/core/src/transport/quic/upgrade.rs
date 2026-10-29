//! Upgrading a UDP transport session to a QUIC path (#5940, step e5a).
//!
//! Only the policy exists so far ([`policy`]). The negotiation, its state
//! machine and the wiring into the peer connection come in later steps; until
//! then nothing in the node reads the policy, and no session is upgraded.

pub(crate) mod policy;
