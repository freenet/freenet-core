//! LEDBAT++ statistics and telemetry.
//!
//! This module provides the statistics snapshot struct for monitoring
//! and debugging LEDBAT congestion control behavior.

use std::time::Duration;

/// LEDBAT++ congestion control statistics.
///
/// Provides a snapshot of the controller state for telemetry and debugging.
#[derive(Debug, Clone)]
pub struct LedbatStats {
    /// Current congestion window size (bytes).
    pub cwnd: usize,
    /// Current bytes in flight (unacknowledged).
    pub flightsize: usize,
    /// Current queuing delay estimate.
    pub queuing_delay: Duration,
    /// Minimum observed RTT (base delay).
    pub base_delay: Duration,
    /// Peak congestion window reached during controller lifetime.
    pub peak_cwnd: usize,
    /// Total number of cwnd increases.
    pub total_increases: usize,
    /// Total number of cwnd decreases.
    pub total_decreases: usize,
    /// Total packet losses detected.
    pub total_losses: usize,
    /// Times cwnd hit minimum value.
    pub min_cwnd_events: usize,
    /// Times slow start phase exited.
    pub slow_start_exits: usize,
    /// Number of periodic slowdowns completed (LEDBAT++ feature).
    pub periodic_slowdowns: usize,
    /// Current slow start threshold (bytes).
    /// When cwnd >= ssthresh, we exit slow start and enter congestion avoidance.
    pub ssthresh: usize,
    /// Effective minimum ssthresh floor (bytes).
    /// This is the floor that prevents ssthresh death spiral on high-BDP paths.
    /// Based on explicit config, BDP proxy, or RTT scaling.
    pub min_ssthresh_floor: usize,
    /// Total retransmission timeouts (RTO events).
    /// High timeout counts indicate severe congestion or path issues.
    pub total_timeouts: usize,
}
