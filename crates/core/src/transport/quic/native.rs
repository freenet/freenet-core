//! QUIC-native connections (#5940, design v4 section 2): a QUIC connection
//! set up directly by the connect op, with no UDP session under it.
//!
//! This module holds the wire pieces so far: the [`hello`] each side sends
//! first on a native connection. The entry points that set one up come in a
//! later step; nothing here is reachable from the connect op while
//! `connect::native::NATIVE_DIAL_READY` is false.

// Read and written only by the native entry points, which come in the next
// step; until then only its tests use it.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod hello;
