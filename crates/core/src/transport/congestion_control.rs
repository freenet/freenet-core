//! Congestion control interface for the transport layer.
//!
//! This module provides a pluggable interface for congestion control algorithms,
//! allowing the transport layer to switch between different algorithms at runtime.
//!
//! ## Supported Algorithms
//!
//! - **LEDBAT++** (default): Low Extra Delay Background Transport, optimized for
//!   background traffic that should yield to foreground applications.
//!
//! ## Usage
//!
//! ```ignore
//! use freenet_core::transport::congestion_control::{
//!     CongestionController, CongestionControlConfig, CongestionControlAlgorithm,
//! };
//!
//! // Create a LEDBAT++ controller (default)
//! let config = CongestionControlConfig::default();
//! let controller = config.build();
//!
//! // Or explicitly select an algorithm
//! let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
//! let controller = config.build();
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};

use super::ledbat::{LedbatConfig, LedbatController, LedbatStats};

// =============================================================================
// Algorithm Identification
// =============================================================================

/// Identifies the congestion control algorithm in use.
///
/// This enum allows runtime inspection of which algorithm is being used,
/// which is useful for logging, telemetry, and debugging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CongestionControlAlgorithm {
    /// LEDBAT++ (Low Extra Delay Background Transport)
    ///
    /// Delay-based congestion control optimized for background traffic.
    /// Yields to loss-based flows (TCP) and maintains low queuing delay.
    /// Based on draft-irtf-iccrg-ledbat-plus-plus.
    Ledbat,
    // Future algorithms can be added here:
    // TcpReno,
    // TcpCubic,
    // Bbr,
}

impl fmt::Display for CongestionControlAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CongestionControlAlgorithm::Ledbat => write!(f, "LEDBAT++"),
        }
    }
}

// =============================================================================
// Algorithm-Agnostic Statistics
// =============================================================================

/// Algorithm-agnostic congestion control statistics.
///
/// These statistics are common across all congestion control algorithms and
/// provide a unified interface for telemetry and monitoring.
///
/// For algorithm-specific statistics, use the `algorithm_stats()` method
/// on the controller to get the native stats type.
#[derive(Debug, Clone)]
pub struct CongestionControlStats {
    /// Algorithm identifier.
    pub algorithm: CongestionControlAlgorithm,
    /// Current congestion window size (bytes).
    pub cwnd: usize,
    /// Current bytes in flight (unacknowledged).
    pub flightsize: usize,
    /// Current queuing delay estimate.
    pub queuing_delay: Duration,
    /// Minimum observed RTT (base delay).
    pub base_delay: Duration,
    /// Peak congestion window reached.
    pub peak_cwnd: usize,
    /// Total packet losses detected.
    pub total_losses: usize,
    /// Total retransmission timeouts.
    pub total_timeouts: usize,
    /// Current slow start threshold (bytes).
    pub ssthresh: usize,
}

impl CongestionControlStats {
    /// Calculate effective bandwidth based on cwnd and RTT.
    ///
    /// Returns bytes per second, or 0 if RTT is zero.
    pub fn effective_bandwidth(&self, rtt: Duration) -> usize {
        if rtt.is_zero() {
            return 0;
        }
        let rtt_secs = rtt.as_secs_f64();
        (self.cwnd as f64 / rtt_secs) as usize
    }
}

// =============================================================================
// Congestion Controller Trait
// =============================================================================

/// Trait for congestion control algorithms.
///
/// This trait defines the interface that all congestion control algorithms must
/// implement. It provides methods for:
///
/// - **Event handling**: Responding to network events (sends, ACKs, losses, timeouts)
/// - **State queries**: Getting current congestion state (cwnd, flightsize, delays)
/// - **Statistics**: Collecting algorithm-agnostic metrics
///
/// ## Thread Safety
///
/// Implementations must be `Send + Sync` to allow sharing across async tasks.
/// Individual method calls are expected to be atomic or lock-free, but compound
/// operations (e.g., state change + associated updates) may not be atomic.
///
/// ## Example Implementation
///
/// ```ignore
/// struct MyController { /* ... */ }
///
/// impl CongestionController for MyController {
///     fn on_send(&self, bytes: usize) {
///         // Track bytes in flight
///     }
///
///     fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
///         // Update RTT estimates and adjust cwnd
///     }
///
///     // ... other methods
/// }
/// ```
pub trait CongestionController: Send + Sync {
    // =========================================================================
    // Event Handlers
    // =========================================================================

    /// Called when bytes are sent.
    ///
    /// The implementation should update its internal flight size tracking.
    /// This is called after the packet is successfully queued for sending.
    fn on_send(&self, bytes: usize);

    /// Called when an ACK is received with RTT sample.
    ///
    /// This is the primary feedback mechanism for congestion control.
    /// The implementation should:
    /// - Update RTT estimates (base delay, smoothed RTT)
    /// - Adjust congestion window based on algorithm rules
    /// - Decrement flight size by `bytes_acked`
    ///
    /// # Arguments
    ///
    /// * `rtt_sample` - Round-trip time for the acknowledged packet
    /// * `bytes_acked` - Number of bytes acknowledged
    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize);

    /// Called when an ACK is received for a retransmitted packet.
    ///
    /// Per Karn's algorithm, RTT samples from retransmitted packets should not
    /// be used to update RTT estimates (we can't know which transmission was ACKed).
    /// However, the flight size should still be decremented.
    ///
    /// # Arguments
    ///
    /// * `bytes_acked` - Number of bytes acknowledged
    fn on_ack_without_rtt(&self, bytes_acked: usize);

    /// Called when packet loss is detected (e.g., via duplicate ACKs).
    ///
    /// The implementation should reduce the congestion window according to
    /// its algorithm's loss response (e.g., multiplicative decrease).
    fn on_loss(&self);

    /// Called when a retransmission timeout (RTO) occurs.
    ///
    /// This indicates severe congestion or path failure. Implementations
    /// typically respond more aggressively than to normal loss, often
    /// resetting to minimum cwnd and re-entering slow start.
    fn on_timeout(&self);

    // =========================================================================
    // State Queries
    // =========================================================================

    /// Returns the current congestion window in bytes.
    ///
    /// This is the maximum number of bytes that can be in flight at once.
    fn current_cwnd(&self) -> usize;

    /// Returns the current sending rate in bytes per second.
    ///
    /// This is typically calculated as `cwnd / rtt`, representing the
    /// sustainable throughput at the current congestion state.
    ///
    /// # Arguments
    ///
    /// * `rtt` - Current RTT estimate (typically base delay or smoothed RTT)
    fn current_rate(&self, rtt: Duration) -> usize;

    /// Returns the current flight size in bytes.
    ///
    /// Flight size is the number of bytes sent but not yet acknowledged.
    /// Sending is blocked when `flightsize >= cwnd`.
    fn flightsize(&self) -> usize;

    /// Returns the base delay (minimum observed RTT).
    ///
    /// Base delay represents the propagation delay of the path without
    /// queuing. It's used by delay-based algorithms like LEDBAT to
    /// estimate queuing delay.
    fn base_delay(&self) -> Duration;

    /// Returns the current queuing delay estimate.
    ///
    /// Queuing delay is the estimated time packets spend in network queues,
    /// calculated as `current_rtt - base_delay`.
    fn queuing_delay(&self) -> Duration;

    /// Returns the peak congestion window achieved.
    ///
    /// This is useful for diagnostics to understand the maximum throughput
    /// achieved during the connection lifetime.
    fn peak_cwnd(&self) -> usize;

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Returns algorithm-agnostic statistics.
    ///
    /// These statistics provide a unified view across all algorithms,
    /// suitable for general telemetry and monitoring.
    fn stats(&self) -> CongestionControlStats;

    /// Returns the algorithm identifier.
    fn algorithm(&self) -> CongestionControlAlgorithm;
}

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for creating congestion controllers.
///
/// This struct allows selecting and configuring congestion control algorithms.
/// Algorithm-specific configuration is provided via the `algorithm_config` field.
#[derive(Debug, Clone)]
pub struct CongestionControlConfig {
    /// Which algorithm to use.
    pub algorithm: CongestionControlAlgorithm,
    /// Initial congestion window (bytes).
    pub initial_cwnd: usize,
    /// Minimum congestion window (bytes).
    pub min_cwnd: usize,
    /// Maximum congestion window (bytes).
    pub max_cwnd: usize,
    /// Initial slow start threshold (bytes).
    pub ssthresh: usize,
    /// Minimum ssthresh floor for timeout recovery.
    pub min_ssthresh: Option<usize>,
    /// Algorithm-specific configuration.
    pub algorithm_config: AlgorithmConfig,
}

/// Algorithm-specific configuration options.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AlgorithmConfig {
    /// LEDBAT++ specific configuration.
    Ledbat {
        /// Enable slow start phase.
        enable_slow_start: bool,
        /// Delay threshold to exit slow start (fraction of TARGET).
        delay_exit_threshold: f64,
        /// Enable periodic slowdowns for inter-flow fairness.
        enable_periodic_slowdown: bool,
        /// Randomize ssthresh to prevent synchronization.
        randomize_ssthresh: bool,
    },
}

impl Default for CongestionControlConfig {
    fn default() -> Self {
        let ledbat_config = LedbatConfig::default();
        Self {
            algorithm: CongestionControlAlgorithm::Ledbat,
            initial_cwnd: ledbat_config.initial_cwnd,
            min_cwnd: ledbat_config.min_cwnd,
            max_cwnd: ledbat_config.max_cwnd,
            ssthresh: ledbat_config.ssthresh,
            min_ssthresh: ledbat_config.min_ssthresh,
            algorithm_config: AlgorithmConfig::Ledbat {
                enable_slow_start: ledbat_config.enable_slow_start,
                delay_exit_threshold: ledbat_config.delay_exit_threshold,
                enable_periodic_slowdown: ledbat_config.enable_periodic_slowdown,
                randomize_ssthresh: ledbat_config.randomize_ssthresh,
            },
        }
    }
}

impl CongestionControlConfig {
    /// Create a new configuration for the specified algorithm.
    pub fn new(algorithm: CongestionControlAlgorithm) -> Self {
        match algorithm {
            CongestionControlAlgorithm::Ledbat => Self::default(),
        }
    }

    /// Create a configuration from a LedbatConfig.
    pub fn from_ledbat_config(config: LedbatConfig) -> Self {
        Self {
            algorithm: CongestionControlAlgorithm::Ledbat,
            initial_cwnd: config.initial_cwnd,
            min_cwnd: config.min_cwnd,
            max_cwnd: config.max_cwnd,
            ssthresh: config.ssthresh,
            min_ssthresh: config.min_ssthresh,
            algorithm_config: AlgorithmConfig::Ledbat {
                enable_slow_start: config.enable_slow_start,
                delay_exit_threshold: config.delay_exit_threshold,
                enable_periodic_slowdown: config.enable_periodic_slowdown,
                randomize_ssthresh: config.randomize_ssthresh,
            },
        }
    }

    /// Set the minimum ssthresh floor.
    pub fn with_min_ssthresh(mut self, min_ssthresh: Option<usize>) -> Self {
        self.min_ssthresh = min_ssthresh;
        self
    }

    /// Build a congestion controller from this configuration.
    ///
    /// Returns an `Arc<dyn CongestionController>` that can be shared across tasks.
    pub fn build(&self) -> Arc<dyn CongestionController> {
        self.build_with_time_source(RealTime::new())
    }

    /// Build a congestion controller with a custom time source.
    ///
    /// This is useful for deterministic testing with virtual time.
    pub fn build_with_time_source<T: TimeSource>(
        &self,
        time_source: T,
    ) -> Arc<dyn CongestionController>
    where
        LedbatController<T>: CongestionController,
    {
        match self.algorithm {
            CongestionControlAlgorithm::Ledbat => {
                let AlgorithmConfig::Ledbat {
                    enable_slow_start,
                    delay_exit_threshold,
                    enable_periodic_slowdown,
                    randomize_ssthresh,
                } = &self.algorithm_config;

                let ledbat_config = LedbatConfig {
                    initial_cwnd: self.initial_cwnd,
                    min_cwnd: self.min_cwnd,
                    max_cwnd: self.max_cwnd,
                    ssthresh: self.ssthresh,
                    min_ssthresh: self.min_ssthresh,
                    enable_slow_start: *enable_slow_start,
                    delay_exit_threshold: *delay_exit_threshold,
                    enable_periodic_slowdown: *enable_periodic_slowdown,
                    randomize_ssthresh: *randomize_ssthresh,
                };

                Arc::new(LedbatController::new_with_time_source(
                    ledbat_config,
                    time_source,
                ))
            }
        }
    }

    /// Convert to the native LedbatConfig.
    ///
    /// Returns `Some(LedbatConfig)` if this is a LEDBAT configuration,
    /// `None` otherwise.
    pub fn as_ledbat_config(&self) -> Option<LedbatConfig> {
        match &self.algorithm_config {
            AlgorithmConfig::Ledbat {
                enable_slow_start,
                delay_exit_threshold,
                enable_periodic_slowdown,
                randomize_ssthresh,
            } => Some(LedbatConfig {
                initial_cwnd: self.initial_cwnd,
                min_cwnd: self.min_cwnd,
                max_cwnd: self.max_cwnd,
                ssthresh: self.ssthresh,
                min_ssthresh: self.min_ssthresh,
                enable_slow_start: *enable_slow_start,
                delay_exit_threshold: *delay_exit_threshold,
                enable_periodic_slowdown: *enable_periodic_slowdown,
                randomize_ssthresh: *randomize_ssthresh,
            }),
        }
    }
}

// =============================================================================
// LedbatController Implementation of CongestionController
// =============================================================================

impl<T: TimeSource> CongestionController for LedbatController<T> {
    fn on_send(&self, bytes: usize) {
        // Delegate to inherent method
        self.track_send(bytes)
    }

    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        // Delegate to inherent method
        self.process_ack(rtt_sample, bytes_acked)
    }

    fn on_ack_without_rtt(&self, bytes_acked: usize) {
        // Delegate to inherent method
        self.process_ack_no_rtt(bytes_acked)
    }

    fn on_loss(&self) {
        // Delegate to inherent method
        self.handle_loss()
    }

    fn on_timeout(&self) {
        // Delegate to inherent method
        self.handle_timeout()
    }

    fn current_cwnd(&self) -> usize {
        self.get_cwnd()
    }

    fn current_rate(&self, rtt: Duration) -> usize {
        self.get_rate(rtt)
    }

    fn flightsize(&self) -> usize {
        self.get_flightsize()
    }

    fn base_delay(&self) -> Duration {
        self.get_base_delay()
    }

    fn queuing_delay(&self) -> Duration {
        self.get_queuing_delay()
    }

    fn peak_cwnd(&self) -> usize {
        self.get_peak_cwnd()
    }

    fn stats(&self) -> CongestionControlStats {
        let ledbat_stats = self.get_stats();
        CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Ledbat,
            cwnd: ledbat_stats.cwnd,
            flightsize: ledbat_stats.flightsize,
            queuing_delay: ledbat_stats.queuing_delay,
            base_delay: ledbat_stats.base_delay,
            peak_cwnd: ledbat_stats.peak_cwnd,
            total_losses: ledbat_stats.total_losses,
            total_timeouts: ledbat_stats.total_timeouts,
            ssthresh: ledbat_stats.ssthresh,
        }
    }

    fn algorithm(&self) -> CongestionControlAlgorithm {
        CongestionControlAlgorithm::Ledbat
    }
}

// =============================================================================
// Extension Trait for Algorithm-Specific Stats
// =============================================================================

/// Extension trait for accessing algorithm-specific statistics.
///
/// This trait provides a way to get native statistics types when the
/// underlying algorithm is known.
pub trait CongestionControllerExt: CongestionController {
    /// Get LEDBAT-specific statistics if this is a LEDBAT controller.
    fn ledbat_stats(&self) -> Option<LedbatStats>;
}

impl<T: TimeSource> CongestionControllerExt for LedbatController<T> {
    fn ledbat_stats(&self) -> Option<LedbatStats> {
        Some(LedbatController::stats(self))
    }
}

// Default implementation for dyn CongestionController - returns None
// since we can't know the concrete type at runtime without downcasting
impl CongestionControllerExt for dyn CongestionController {
    fn ledbat_stats(&self) -> Option<LedbatStats> {
        // Would need downcasting to get algorithm-specific stats
        // For now, return None for the trait object case
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    #[test]
    fn test_default_config_creates_ledbat() {
        let config = CongestionControlConfig::default();
        assert_eq!(config.algorithm, CongestionControlAlgorithm::Ledbat);
    }

    #[test]
    fn test_build_controller() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
        assert!(controller.current_cwnd() > 0);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_build_with_virtual_time() {
        let config = CongestionControlConfig::default();
        let time_source = VirtualTime::new();
        let controller = config.build_with_time_source(time_source);

        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
    }

    #[test]
    fn test_basic_send_ack_flow() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        // Send some bytes
        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);

        // Receive ACK
        controller.on_ack(Duration::from_millis(50), 1000);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_stats_conversion() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        let stats = controller.stats();
        assert_eq!(stats.algorithm, CongestionControlAlgorithm::Ledbat);
        assert!(stats.cwnd > 0);
    }

    #[test]
    fn test_from_ledbat_config() {
        let ledbat_config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 5_000,
            max_cwnd: 500_000,
            ssthresh: 100_000,
            min_ssthresh: Some(25_000),
            enable_slow_start: false,
            delay_exit_threshold: 0.5,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
        };

        let config = CongestionControlConfig::from_ledbat_config(ledbat_config.clone());
        let controller = config.build();

        assert_eq!(controller.current_cwnd(), 50_000);
        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
    }

    #[test]
    fn test_algorithm_display() {
        assert_eq!(
            format!("{}", CongestionControlAlgorithm::Ledbat),
            "LEDBAT++"
        );
    }

    #[test]
    fn test_effective_bandwidth() {
        let stats = CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Ledbat,
            cwnd: 100_000,
            flightsize: 0,
            queuing_delay: Duration::ZERO,
            base_delay: Duration::ZERO,
            peak_cwnd: 100_000,
            total_losses: 0,
            total_timeouts: 0,
            ssthresh: 100_000,
        };

        // 100KB cwnd / 100ms RTT = 1MB/s
        let bandwidth = stats.effective_bandwidth(Duration::from_millis(100));
        assert_eq!(bandwidth, 1_000_000);

        // Zero RTT returns 0
        let bandwidth_zero = stats.effective_bandwidth(Duration::ZERO);
        assert_eq!(bandwidth_zero, 0);
    }
}
