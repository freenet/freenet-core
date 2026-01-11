// TODO: Remove this once BBR is integrated into peer_connection.rs
#![allow(dead_code)]
#![allow(unused_imports)]

//! BBRv3 (Bottleneck Bandwidth and Round-trip propagation time) congestion controller.
//!
//! Implementation based on the IETF draft:
//! https://datatracker.ietf.org/doc/html/draft-ietf-ccwg-bbr
//!
//! ## Why BBRv3?
//!
//! BBRv3 is a model-based congestion control algorithm that explicitly estimates
//! the network path's bottleneck bandwidth and round-trip propagation time.
//! Unlike loss-based algorithms (like TCP Reno) or delay-based algorithms
//! (like LEDBAT), BBR tolerates packet loss without drastically reducing throughput,
//! making it ideal for lossy network paths.
//!
//! ## Key Concepts
//!
//! - **max_bw**: Maximum bandwidth estimate (bottleneck bandwidth)
//! - **min_rtt**: Minimum RTT estimate (propagation delay)
//! - **BDP**: Bandwidth-Delay Product = max_bw × min_rtt (optimal inflight)
//! - **pacing_rate**: Rate at which to send packets = max_bw × pacing_gain
//! - **cwnd**: Congestion window = BDP × cwnd_gain
//!
//! ## State Machine
//!
//! BBRv3 has four primary states:
//!
//! | State | Purpose | Pacing Gain | CWND Gain |
//! |-------|---------|-------------|-----------|
//! | Startup | Find max bandwidth | 2.77 | 2.0 |
//! | Drain | Drain Startup queue | 0.36 | 2.0 |
//! | ProbeBW | Steady-state probing | 0.9-1.25 | 2.0 |
//! | ProbeRTT | Measure min_rtt | 1.0 | 0.5 |
//!
//! ## Lock-Free Design
//!
//! This implementation uses atomic operations for all state, enabling
//! concurrent access from sender and ACK processing paths without locks.

mod bandwidth;
mod config;
mod controller;
mod delivery_rate;
mod rtt;
mod state;
mod stats;

#[cfg(test)]
mod tests;

// Re-export public API
pub use config::BbrConfig;
pub use controller::BbrController;
pub use delivery_rate::DeliveryRateToken;
pub use state::{BbrState, ProbeBwPhase};
pub use stats::BbrStats;
