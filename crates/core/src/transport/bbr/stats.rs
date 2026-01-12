//! BBRv3 statistics for telemetry and debugging.
//!
//! This module provides a snapshot of the BBR controller state for
//! logging, metrics, and debugging purposes.

use std::time::Duration;

use super::state::{BbrState, ProbeBwPhase};

/// Snapshot of BBR controller state for telemetry.
#[derive(Debug, Clone)]
pub struct BbrStats {
    /// Current BBR state (Startup, Drain, ProbeBW, ProbeRTT).
    pub state: BbrState,

    /// Current ProbeBW sub-phase (only meaningful in ProbeBW state).
    pub probe_bw_phase: ProbeBwPhase,

    /// Current congestion window (bytes).
    pub cwnd: usize,

    /// Current bytes in flight.
    pub flightsize: usize,

    /// Current pacing rate (bytes/sec).
    pub pacing_rate: u64,

    /// Maximum bandwidth estimate (bytes/sec).
    pub max_bw: u64,

    /// Minimum RTT estimate.
    pub min_rtt: Option<Duration>,

    /// Bandwidth-Delay Product estimate (bytes).
    pub bdp: usize,

    /// Total bytes delivered.
    pub delivered: u64,

    /// Total bytes lost.
    pub lost: u64,

    /// Current round count.
    pub round_count: u64,

    /// Number of rounds without bandwidth growth (for Startup exit).
    pub full_bw_count: u32,

    /// Peak bandwidth seen during Startup.
    pub full_bw: u64,

    /// Number of times we've entered ProbeRTT.
    pub probe_rtt_rounds: u64,

    /// Number of timeouts experienced.
    pub timeouts: u64,

    /// Maximum BDP ever seen (for adaptive timeout floor).
    pub max_bdp_seen: usize,

    /// Whether currently application-limited.
    pub is_app_limited: bool,

    /// Current pacing gain.
    pub pacing_gain: f64,

    /// Current cwnd gain.
    pub cwnd_gain: f64,
}

impl Default for BbrStats {
    fn default() -> Self {
        Self {
            state: BbrState::Startup,
            probe_bw_phase: ProbeBwPhase::Cruise,
            cwnd: 0,
            flightsize: 0,
            pacing_rate: 0,
            max_bw: 0,
            min_rtt: None,
            bdp: 0,
            delivered: 0,
            lost: 0,
            round_count: 0,
            full_bw_count: 0,
            full_bw: 0,
            probe_rtt_rounds: 0,
            timeouts: 0,
            max_bdp_seen: 0,
            is_app_limited: false,
            pacing_gain: 1.0,
            cwnd_gain: 1.0,
        }
    }
}

impl std::fmt::Display for BbrStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BBR[{:?}] cwnd={} flight={} rate={}/s bw={}/s rtt={:?} bdp={}",
            self.state,
            format_bytes(self.cwnd),
            format_bytes(self.flightsize),
            format_bytes(self.pacing_rate as usize),
            format_bytes(self.max_bw as usize),
            self.min_rtt,
            format_bytes(self.bdp),
        )
    }
}

/// Format bytes in human-readable form.
fn format_bytes(bytes: usize) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_display() {
        let stats = BbrStats {
            state: BbrState::ProbeBW,
            cwnd: 150_000,
            flightsize: 100_000,
            pacing_rate: 10_000_000,
            max_bw: 10_000_000,
            min_rtt: Some(Duration::from_millis(50)),
            bdp: 500_000,
            ..Default::default()
        };

        let display = format!("{}", stats);
        assert!(display.contains("ProbeBW"));
        assert!(display.contains("150.0KB"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500B");
        assert_eq!(format_bytes(1500), "1.5KB");
        assert_eq!(format_bytes(1_500_000), "1.5MB");
    }
}
