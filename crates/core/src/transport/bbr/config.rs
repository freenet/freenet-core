//! BBRv3 configuration and constants.
//!
//! This module contains the configuration struct and tuning constants for the
//! BBRv3 congestion control algorithm, based on the IETF draft:
//! https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control

use std::time::Duration;

use crate::transport::packet_data::MAX_DATA_SIZE;

/// Maximum segment size (actual packet data capacity)
pub(crate) const MSS: usize = MAX_DATA_SIZE;

// =============================================================================
// BBRv3 Pacing and CWND Gains
// =============================================================================

/// Startup pacing gain: 2/ln(2) ≈ 2.77
/// This allows BBR to double its sending rate each round trip during Startup.
pub(crate) const STARTUP_PACING_GAIN: f64 = 2.77;

/// Minimum pacing rate during Startup: 25 MB/s.
///
/// This floor prevents the "bootstrap death spiral" where:
/// 1. Low pacing_rate limits how fast we send
/// 2. BBR measures bandwidth from our limited sends
/// 3. Low measured bandwidth → lower pacing_rate → stuck
///
/// By maintaining a high floor during Startup, we can discover the actual
/// available bandwidth. Once BBR exits Startup (via bandwidth plateau or loss),
/// it uses the measured bandwidth without this floor.
///
/// 25 MB/s is chosen to handle high-bandwidth links while still being safe
/// for most network paths (congestion will trigger loss-based exit from Startup).
pub(crate) const STARTUP_MIN_PACING_RATE: u64 = 25_000_000;

/// Startup cwnd gain: 2.0
/// Provides headroom for ACK aggregation during startup.
pub(crate) const STARTUP_CWND_GAIN: f64 = 2.0;

/// Drain pacing gain: 1/2.77 ≈ 0.36
/// Drains the queue built during Startup by pacing below estimated bandwidth.
pub(crate) const DRAIN_PACING_GAIN: f64 = 0.36;

/// ProbeBW UP phase pacing gain: probe for more bandwidth.
pub(crate) const PROBE_BW_UP_PACING_GAIN: f64 = 1.25;

/// ProbeBW DOWN phase pacing gain: drain any queue.
pub(crate) const PROBE_BW_DOWN_PACING_GAIN: f64 = 0.9;

/// ProbeBW CRUISE/REFILL phase pacing gain: maintain steady state.
pub(crate) const PROBE_BW_CRUISE_PACING_GAIN: f64 = 1.0;

/// ProbeRTT cwnd gain: reduce cwnd to measure true min_rtt.
pub(crate) const PROBE_RTT_CWND_GAIN: f64 = 0.5;

/// ProbeRTT minimum cwnd: 4 packets per BBRv3 spec.
/// This ensures we can still make forward progress during RTT probing.
pub(crate) const PROBE_RTT_MIN_CWND: usize = 4 * MSS;

/// Default cwnd gain for ProbeBW phases.
pub(crate) const PROBE_BW_CWND_GAIN: f64 = 2.0;

// =============================================================================
// BBRv3 Timing Parameters
// =============================================================================

/// Minimum RTT filter window: 10 seconds.
/// BBR tracks min_rtt over this window to estimate path propagation delay.
pub(crate) const MIN_RTT_FILTER_WINDOW: Duration = Duration::from_secs(10);

/// ProbeRTT interval: how often to enter ProbeRTT to refresh min_rtt.
/// BBRv3 uses ~5 seconds (2.5s-7.5s range with randomization).
pub(crate) const PROBE_RTT_INTERVAL: Duration = Duration::from_secs(5);

/// ProbeRTT duration: how long to stay in ProbeRTT.
/// 200ms is enough to drain queues and get a clean RTT sample.
pub(crate) const PROBE_RTT_DURATION: Duration = Duration::from_millis(200);

/// Bandwidth filter window size (number of max_bw samples to track).
/// BBRv3 typically uses 2 round trips worth of samples.
pub(crate) const BW_FILTER_SIZE: usize = 10;

// =============================================================================
// BBRv3 Startup Exit Detection
// =============================================================================

/// Number of rounds without bandwidth growth to exit Startup.
/// BBR exits Startup when max_bw hasn't grown by more than 25% for 3 rounds.
pub(crate) const STARTUP_FULL_BW_ROUNDS: u32 = 3;

/// Bandwidth growth threshold for Startup exit (25% growth).
/// If max_bw doesn't grow by at least this factor, count as a "full bandwidth" round.
pub(crate) const STARTUP_FULL_BW_THRESHOLD: f64 = 1.25;

// =============================================================================
// BBRv3 Loss Response
// =============================================================================

/// Loss threshold for Startup exit: 2% packet loss indicates buffer overflow.
pub(crate) const STARTUP_LOSS_THRESHOLD: f64 = 0.02;

/// Beta: multiplicative decrease factor for loss response.
/// BBRv3 uses 0.7 (reduce by 30% on loss).
pub(crate) const BETA: f64 = 0.7;

// =============================================================================
// ProbeBW Cycle Parameters
// =============================================================================

/// Number of rounds in ProbeBW cycle before probing up.
/// Typically 6-8 rounds of Cruise before attempting Up.
pub(crate) const PROBE_BW_CRUISE_ROUNDS_MIN: u32 = 6;

/// Duration of Down phase in ProbeBW (in round trips).
pub(crate) const PROBE_BW_DOWN_ROUNDS: u32 = 1;

/// Duration of Refill phase in ProbeBW (in round trips).
pub(crate) const PROBE_BW_REFILL_ROUNDS: u32 = 1;

/// Duration of Up phase in ProbeBW (in round trips).
pub(crate) const PROBE_BW_UP_ROUNDS: u32 = 1;

// =============================================================================
// Configuration Struct
// =============================================================================

/// Configuration for BBRv3 congestion control.
#[derive(Debug, Clone)]
pub struct BbrConfig {
    /// Initial congestion window (bytes).
    /// BBR typically starts with 10 MSS (IW10) per RFC 6928.
    pub initial_cwnd: usize,

    /// Minimum congestion window (bytes).
    /// During ProbeRTT, cwnd is reduced but never below this.
    pub min_cwnd: usize,

    /// Maximum congestion window (bytes).
    /// Upper bound to prevent runaway growth.
    pub max_cwnd: usize,

    /// Enable ProbeRTT phase for min_rtt measurement.
    /// Disabling this may cause min_rtt to become stale.
    pub enable_probe_rtt: bool,

    /// ProbeRTT interval randomization range (±).
    /// Adds jitter to prevent synchronized ProbeRTT across flows.
    pub probe_rtt_jitter: Duration,
}

impl Default for BbrConfig {
    fn default() -> Self {
        Self {
            // IW10: 10 * MSS = ~14,240 bytes
            // More conservative than LEDBAT's IW26, but BBR ramps faster
            initial_cwnd: 10 * MSS,
            // 4 packets minimum (BBRv3 recommendation)
            min_cwnd: 4 * MSS,
            // 1 GB maximum (same as LEDBAT)
            max_cwnd: 1_000_000_000,
            // Enable ProbeRTT by default
            enable_probe_rtt: true,
            // ±2.5s jitter on ProbeRTT interval
            probe_rtt_jitter: Duration::from_millis(2500),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_pacing_gain() {
        // 2/ln(2) ≈ 2.885, but BBR uses 2.77 for practical reasons
        assert!((STARTUP_PACING_GAIN - 2.77).abs() < 0.01);
    }

    #[test]
    fn test_drain_pacing_gain() {
        // Drain gain should be approximately 1/STARTUP_PACING_GAIN
        let expected = 1.0 / STARTUP_PACING_GAIN;
        assert!((DRAIN_PACING_GAIN - expected).abs() < 0.05);
    }

    #[test]
    fn test_probe_rtt_interval() {
        assert_eq!(PROBE_RTT_INTERVAL, Duration::from_secs(5));
    }

    #[test]
    fn test_min_rtt_filter_window() {
        assert_eq!(MIN_RTT_FILTER_WINDOW, Duration::from_secs(10));
    }

    #[test]
    fn test_default_config() {
        let config = BbrConfig::default();
        assert_eq!(config.initial_cwnd, 10 * MSS);
        assert_eq!(config.min_cwnd, 4 * MSS);
        assert!(config.enable_probe_rtt);
    }
}
