//! QUIC-native connections (#5940, design v4 section 2): a QUIC connection
//! set up directly by the connect op, with no UDP session under it.
//!
//! [`setup`] sets one up: punching ([`punch`]), the handshake with the native
//! ALPN, and the [`hello`] exchange ([`exchange`]). The attempt router hands
//! native attempts to [`waiters`].
//!
//! Unreachable until A5 wires the setup into the connect op: the entry points
//! (`setup::connect_native`, `setup::accept_native`) and the endpoint's native
//! side (`endpoint`: allowing a native peer, dialling one, sending a punch)
//! are visible to this module only, and the only way to register a waiter is
//! private to `setup`. Widening any of them is a visible diff. The connect op
//! also gates on `connect::native::NATIVE_DIAL_READY`, which is false.
//!
//! The `dead_code` allows below go when A5 calls the setup.

#[cfg_attr(not(test), allow(dead_code))]
mod endpoint;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod exchange;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod hello;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod punch;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod setup;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod waiters;
