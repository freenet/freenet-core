//! LEDBAT++ (Low Extra Delay Background Transport) congestion controller.
//!
//! Implementation based on draft-irtf-iccrg-ledbat-plus-plus, which improves upon
//! RFC 6817 with better inter-flow fairness and latecomer handling.
//!
//! ## Why LEDBAT++?
//!
//! Freenet runs as a background daemon and should not interfere with foreground
//! applications (video calls, web browsing, etc.). LEDBAT++ is designed exactly
//! for this use case - it's used by BitTorrent (uTP) and Apple for similar reasons.
//!
//! ## Key Improvements over RFC 6817 LEDBAT
//!
//! | Feature | RFC 6817 | LEDBAT++ |
//! |---------|----------|----------|
//! | Target delay | 100ms | 60ms |
//! | Gain | Fixed 1.0 | Dynamic based on base_delay |
//! | Decrease formula | Linear | Multiplicative with -W/2 cap |
//! | Inter-flow fairness | Poor (latecomer advantage) | Good (periodic slowdowns) |
//! | Slow start exit | 50% of target | 75% of target |
//!
//! ## Periodic Slowdown Mechanism
//!
//! LEDBAT++ introduces periodic slowdowns to solve the "latecomer advantage" problem:
//! - After initial slow start exit, wait 2 RTTs then reduce cwnd by 4x (not to minimum)
//! - This proportional reduction is consistent with our large initial window (IW26)
//! - Freeze cwnd for 2 RTTs to allow base delay re-measurement
//! - Ramp back up using slow start until reaching previous cwnd
//! - Schedule next slowdown at 9x the slowdown duration (maintains ≤10% utilization impact)
//!
//! ## Lock-Free Design
//!
//! This implementation is fully lock-free, using atomic operations for all state:
//! - Atomic ring buffers for delay filtering and base delay history
//! - AtomicU64 for Duration values (stored as nanoseconds)
//! - Epoch-based timing for rate-limiting updates

mod atomic;
mod config;
mod controller;
mod state;
mod stats;

#[cfg(test)]
mod tests;

// Re-export public API
pub use config::LedbatConfig;
pub use controller::LedbatController;
pub use stats::LedbatStats;
