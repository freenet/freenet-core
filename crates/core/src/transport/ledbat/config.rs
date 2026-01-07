//! LEDBAT++ configuration and constants.
//!
//! This module contains the configuration struct and tuning constants for the
//! LEDBAT++ congestion control algorithm.

use std::time::Duration;

use crate::transport::packet_data::MAX_DATA_SIZE;

/// Maximum segment size (actual packet data capacity)
pub(crate) const MSS: usize = MAX_DATA_SIZE;

/// Target queuing delay (LEDBAT++ uses 60ms, RFC 6817 used 100ms)
pub(crate) const TARGET: Duration = Duration::from_millis(60);

/// Maximum GAIN divisor for dynamic GAIN calculation (LEDBAT++ Section 4.2)
pub(crate) const MAX_GAIN_DIVISOR: u32 = 16;

/// Base delay history size (RFC 6817 recommendation)
pub(crate) const BASE_HISTORY_SIZE: usize = 10;

/// Delay filter sample count (RFC 6817 recommendation)
pub(crate) const DELAY_FILTER_SIZE: usize = 4;

/// Default slow start threshold (1 MB)
///
/// Higher values allow slow start to reach useful throughput on high-BDP paths
/// before transitioning to congestion avoidance. With 135ms RTT (e.g., US to EU),
/// 100KB ssthresh limits throughput to ~6 Mbit/s. 1MB allows ~60 Mbit/s.
pub(crate) const DEFAULT_SSTHRESH: usize = 1_048_576;

/// Periodic slowdown interval multiplier (LEDBAT++ Section 4.4)
/// Next slowdown is scheduled at 9x the previous slowdown duration,
/// maintaining ≤10% utilization impact.
pub(crate) const SLOWDOWN_INTERVAL_MULTIPLIER: u32 = 9;

/// Number of RTTs to wait after slow start exit before initial slowdown
pub(crate) const SLOWDOWN_DELAY_RTTS: u32 = 2;

/// Number of RTTs to freeze cwnd at minimum during slowdown
pub(crate) const SLOWDOWN_FREEZE_RTTS: u32 = 2;

/// Slowdown reduction factor: cwnd is divided by this during periodic slowdowns.
/// We use 16 to be consistent with our large initial window (IW26).
/// Factor of 4 provides a more gradual slowdown:
/// - 300KB cwnd drops to 75KB (not 18KB with factor 16)
/// - Avoids throughput "cliff" while still probing for fairness
/// - Faster recovery time (fewer RTTs to ramp back up)
pub(crate) const SLOWDOWN_REDUCTION_FACTOR: usize = 4;

/// Path change detection threshold: 50% RTT shift invalidates BDP proxy.
///
/// A ratio > 1.5 means the base_delay changed by more than 50% in either direction,
/// indicating a likely route change where the old BDP estimate no longer applies.
/// The threshold balances sensitivity (detecting real path changes) against stability
/// (tolerating normal RTT jitter).
pub(crate) const PATH_CHANGE_RTT_THRESHOLD: f64 = 1.5;

/// RTT-based scaling factor: bytes per millisecond of base RTT.
///
/// When no BDP proxy is available but min_ssthresh is configured, we estimate
/// a reasonable floor based on RTT: 1KB per ms. This heuristic assumes typical
/// backbone capacity where higher RTT paths generally have more capacity.
/// - 50ms RTT → 50KB floor
/// - 135ms RTT → 135KB floor
/// - 200ms RTT → 200KB floor
pub(crate) const RTT_SCALING_BYTES_PER_MS: usize = 1024;

/// Configuration for LEDBAT++ with slow start and periodic slowdowns
#[derive(Debug, Clone)]
pub struct LedbatConfig {
    /// Initial congestion window (bytes)
    pub initial_cwnd: usize,
    /// Minimum congestion window (bytes)
    pub min_cwnd: usize,
    /// Maximum congestion window (bytes)
    pub max_cwnd: usize,
    /// Slow start threshold (bytes)
    pub ssthresh: usize,
    /// Enable slow start phase
    pub enable_slow_start: bool,
    /// Delay threshold to exit slow start (fraction of TARGET)
    /// Default: 0.75 (LEDBAT++ exits when queuing_delay > 3/4 * TARGET = 45ms)
    pub delay_exit_threshold: f64,
    /// Randomize ssthresh to prevent synchronization (±20% jitter)
    /// Default: true
    pub randomize_ssthresh: bool,
    /// Enable periodic slowdowns for inter-flow fairness (LEDBAT++ feature)
    /// Default: true
    pub enable_periodic_slowdown: bool,
    /// Minimum ssthresh floor for timeout recovery (bytes).
    ///
    /// After a timeout, ssthresh is normally set to max(cwnd/2, 2*min_cwnd).
    /// This can cause a "death spiral" on high-BDP paths where repeated timeouts
    /// keep halving ssthresh until slow start can't recover useful throughput.
    ///
    /// Set this to a higher value (e.g., initial_ssthresh) to allow slow start
    /// to recover to reasonable throughput after timeouts on high-bandwidth paths.
    ///
    /// Default: None (uses spec-compliant 2*min_cwnd floor)
    pub min_ssthresh: Option<usize>,
}

impl Default for LedbatConfig {
    fn default() -> Self {
        Self {
            // IW26: 26 * MSS = 38,000 bytes
            // Reaches 300KB cwnd in 3 RTTs (38KB → 76KB → 152KB → 304KB)
            // This enables >3 MB/s throughput at 60ms RTT
            initial_cwnd: 38_000,           // 26 * MSS (IW26)
            min_cwnd: 2_848,                // 2 * MSS
            max_cwnd: 1_000_000_000,        // 1 GB
            ssthresh: DEFAULT_SSTHRESH,     // 1 MB
            enable_slow_start: true,        // Enable by default
            delay_exit_threshold: 0.75,     // LEDBAT++: exit at 3/4 * TARGET (45ms)
            randomize_ssthresh: true,       // Enable jitter by default
            enable_periodic_slowdown: true, // LEDBAT++: enable for inter-flow fairness
            min_ssthresh: None,             // Use spec-compliant 2*min_cwnd floor
        }
    }
}
