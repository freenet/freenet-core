//! Congestion control interface for the transport layer.
//!
//! This module provides a pluggable interface for congestion control algorithms,
//! allowing selection between different algorithms via configuration.
//!
//! ## Design
//!
//! The design uses enum dispatch rather than trait objects since all algorithm
//! types are known at compile time. This provides:
//! - Zero-cost abstraction (no vtable indirection)
//! - Full access to algorithm-specific statistics via pattern matching
//! - Type-safe configuration
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
//! // Use the controller
//! controller.on_send(1000);
//! controller.on_ack(Duration::from_millis(50), 1000);
//!
//! // Access algorithm-specific stats via pattern matching
//! if let CongestionController::Ledbat(ledbat) = &*controller {
//!     let stats = ledbat.stats();
//!     println!("Periodic slowdowns: {}", stats.periodic_slowdowns);
//! }
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
/// This enum is used for:
/// - Configuration: Selecting which algorithm to use
/// - Telemetry: Identifying the algorithm in statistics
/// - Logging: Human-readable algorithm names
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum CongestionControlAlgorithm {
    /// LEDBAT++ (Low Extra Delay Background Transport)
    ///
    /// Delay-based congestion control optimized for background traffic.
    /// Yields to loss-based flows (TCP) and maintains low queuing delay.
    /// Based on draft-irtf-iccrg-ledbat-plus-plus.
    #[default]
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
/// For algorithm-specific statistics, pattern match on the `CongestionController`
/// enum to access the native stats type.
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
    /// Returns bytes per second. Returns 0 if RTT is zero to avoid division by zero.
    /// For very small RTTs with large windows, the result is capped at `usize::MAX`.
    /// Handles infinity and NaN values that may arise from extreme inputs.
    pub fn effective_bandwidth(&self, rtt: Duration) -> usize {
        if rtt.is_zero() {
            return 0;
        }
        let rtt_secs = rtt.as_secs_f64();
        let bandwidth = self.cwnd as f64 / rtt_secs;
        // Handle infinity, NaN, or values exceeding usize::MAX
        if !bandwidth.is_finite() || bandwidth > usize::MAX as f64 {
            usize::MAX
        } else if bandwidth < 0.0 {
            0
        } else {
            bandwidth as usize
        }
    }
}

// =============================================================================
// Congestion Control Trait
// =============================================================================

/// Trait defining the interface for congestion control algorithms.
///
/// This trait specifies what operations a congestion controller must support.
/// It's implemented by concrete algorithm types (e.g., `LedbatController`)
/// and by the `CongestionController` enum for dispatch.
///
/// ## Thread Safety
///
/// Implementations must be `Send + Sync` to allow sharing across async tasks.
/// The `LedbatController` implementation uses lock-free atomics for all
/// hot-path operations to ensure thread safety without contention.
pub trait CongestionControl: Send + Sync {
    // =========================================================================
    // Event Handlers
    // =========================================================================

    /// Called when bytes are sent.
    ///
    /// Updates internal flight size tracking. Called after the packet
    /// is successfully queued for sending.
    fn on_send(&self, bytes: usize);

    /// Called when an ACK is received with RTT sample.
    ///
    /// This is the primary feedback mechanism. The implementation should:
    /// - Update RTT estimates (base delay, smoothed RTT)
    /// - Adjust congestion window based on algorithm rules
    /// - Decrement flight size by `bytes_acked`
    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize);

    /// Called when an ACK is received for a retransmitted packet.
    ///
    /// Per Karn's algorithm, RTT samples from retransmitted packets are
    /// ambiguous and should not update RTT estimates. However, flight
    /// size must still be decremented.
    fn on_ack_without_rtt(&self, bytes_acked: usize);

    /// Called when packet loss is detected (e.g., via duplicate ACKs).
    ///
    /// The implementation reduces the congestion window according to
    /// its loss response (typically multiplicative decrease).
    fn on_loss(&self);

    /// Called when a retransmission timeout (RTO) occurs.
    ///
    /// This indicates severe congestion. Implementations typically
    /// reset to minimum cwnd and re-enter slow start.
    fn on_timeout(&self);

    // =========================================================================
    // State Queries
    // =========================================================================

    /// Returns the current congestion window in bytes.
    fn current_cwnd(&self) -> usize;

    /// Returns the current sending rate in bytes per second.
    ///
    /// Calculated as `cwnd / rtt`.
    fn current_rate(&self, rtt: Duration) -> usize;

    /// Returns the current flight size (bytes sent but not yet ACKed).
    fn flightsize(&self) -> usize;

    /// Returns the base delay (minimum observed RTT).
    fn base_delay(&self) -> Duration;

    /// Returns the current queuing delay estimate.
    fn queuing_delay(&self) -> Duration;

    /// Returns the peak congestion window achieved.
    fn peak_cwnd(&self) -> usize;

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Returns algorithm-agnostic statistics.
    fn stats(&self) -> CongestionControlStats;

    /// Returns the algorithm identifier.
    fn algorithm(&self) -> CongestionControlAlgorithm;
}

// =============================================================================
// Congestion Controller Enum (Dispatch)
// =============================================================================

/// Congestion controller that dispatches to the configured algorithm.
///
/// This enum wraps concrete algorithm implementations and provides a unified
/// interface. Using an enum instead of trait objects because:
/// - All algorithm types are known at compile time
/// - Zero-cost dispatch (no vtable indirection in hot paths)
/// - Pattern matching enables access to algorithm-specific features
///
/// ## Example: Accessing Algorithm-Specific Stats
///
/// ```ignore
/// let controller: Arc<CongestionController> = config.build();
///
/// // Get algorithm-agnostic stats (always available)
/// let generic_stats = controller.stats();
///
/// // Get LEDBAT-specific stats via pattern matching
/// match &*controller {
///     CongestionController::Ledbat(ledbat) => {
///         let ledbat_stats = ledbat.stats();
///         println!("Periodic slowdowns: {}", ledbat_stats.periodic_slowdowns);
///     }
/// }
/// ```
pub enum CongestionController<T: TimeSource = RealTime> {
    /// LEDBAT++ congestion controller.
    Ledbat(LedbatController<T>),
    // Future algorithms:
    // TcpReno(TcpRenoController<T>),
    // Bbr(BbrController<T>),
}

impl<T: TimeSource> fmt::Debug for CongestionController<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ledbat(c) => f
                .debug_struct("CongestionController::Ledbat")
                .field("cwnd", &c.current_cwnd())
                .field("flightsize", &c.flightsize())
                .finish_non_exhaustive(),
        }
    }
}

impl<T: TimeSource> CongestionControl for CongestionController<T> {
    fn on_send(&self, bytes: usize) {
        match self {
            Self::Ledbat(c) => c.on_send(bytes),
        }
    }

    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        match self {
            Self::Ledbat(c) => c.on_ack(rtt_sample, bytes_acked),
        }
    }

    fn on_ack_without_rtt(&self, bytes_acked: usize) {
        match self {
            Self::Ledbat(c) => c.on_ack_without_rtt(bytes_acked),
        }
    }

    fn on_loss(&self) {
        match self {
            Self::Ledbat(c) => c.on_loss(),
        }
    }

    fn on_timeout(&self) {
        match self {
            Self::Ledbat(c) => c.on_timeout(),
        }
    }

    fn current_cwnd(&self) -> usize {
        match self {
            Self::Ledbat(c) => c.current_cwnd(),
        }
    }

    fn current_rate(&self, rtt: Duration) -> usize {
        match self {
            Self::Ledbat(c) => c.current_rate(rtt),
        }
    }

    fn flightsize(&self) -> usize {
        match self {
            Self::Ledbat(c) => c.flightsize(),
        }
    }

    fn base_delay(&self) -> Duration {
        match self {
            Self::Ledbat(c) => c.base_delay(),
        }
    }

    fn queuing_delay(&self) -> Duration {
        match self {
            Self::Ledbat(c) => c.queuing_delay(),
        }
    }

    fn peak_cwnd(&self) -> usize {
        match self {
            Self::Ledbat(c) => c.peak_cwnd(),
        }
    }

    fn stats(&self) -> CongestionControlStats {
        match self {
            Self::Ledbat(c) => {
                let s = c.stats();
                CongestionControlStats {
                    algorithm: CongestionControlAlgorithm::Ledbat,
                    cwnd: s.cwnd,
                    flightsize: s.flightsize,
                    queuing_delay: s.queuing_delay,
                    base_delay: s.base_delay,
                    peak_cwnd: s.peak_cwnd,
                    total_losses: s.total_losses,
                    total_timeouts: s.total_timeouts,
                    ssthresh: s.ssthresh,
                }
            }
        }
    }

    fn algorithm(&self) -> CongestionControlAlgorithm {
        match self {
            Self::Ledbat(_) => CongestionControlAlgorithm::Ledbat,
        }
    }
}

// =============================================================================
// LedbatController Implementation of CongestionControl
// =============================================================================

/// Direct implementation of `CongestionControl` for `LedbatController`.
///
/// This allows using `LedbatController` directly with the trait interface,
/// enabling custom algorithm implementations to be added without modifying
/// the `CongestionController` enum.
impl<T: TimeSource> CongestionControl for LedbatController<T> {
    fn on_send(&self, bytes: usize) {
        LedbatController::on_send(self, bytes)
    }

    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        LedbatController::on_ack(self, rtt_sample, bytes_acked)
    }

    fn on_ack_without_rtt(&self, bytes_acked: usize) {
        LedbatController::on_ack_without_rtt(self, bytes_acked)
    }

    fn on_loss(&self) {
        LedbatController::on_loss(self)
    }

    fn on_timeout(&self) {
        LedbatController::on_timeout(self)
    }

    fn current_cwnd(&self) -> usize {
        LedbatController::current_cwnd(self)
    }

    fn current_rate(&self, rtt: Duration) -> usize {
        LedbatController::current_rate(self, rtt)
    }

    fn flightsize(&self) -> usize {
        LedbatController::flightsize(self)
    }

    fn base_delay(&self) -> Duration {
        LedbatController::base_delay(self)
    }

    fn queuing_delay(&self) -> Duration {
        LedbatController::queuing_delay(self)
    }

    fn peak_cwnd(&self) -> usize {
        LedbatController::peak_cwnd(self)
    }

    fn stats(&self) -> CongestionControlStats {
        let s = LedbatController::stats(self);
        CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Ledbat,
            cwnd: s.cwnd,
            flightsize: s.flightsize,
            queuing_delay: s.queuing_delay,
            base_delay: s.base_delay,
            peak_cwnd: s.peak_cwnd,
            total_losses: s.total_losses,
            total_timeouts: s.total_timeouts,
            ssthresh: s.ssthresh,
        }
    }

    fn algorithm(&self) -> CongestionControlAlgorithm {
        CongestionControlAlgorithm::Ledbat
    }
}

impl<T: TimeSource> CongestionController<T> {
    /// Get LEDBAT-specific statistics if this is a LEDBAT controller.
    ///
    /// Returns `Some(LedbatStats)` for LEDBAT controllers, enabling access
    /// to LEDBAT-specific metrics like periodic slowdown counts.
    pub fn ledbat_stats(&self) -> Option<LedbatStats> {
        match self {
            Self::Ledbat(c) => Some(c.stats()),
        }
    }

    /// Get a reference to the inner LEDBAT controller if applicable.
    pub fn as_ledbat(&self) -> Option<&LedbatController<T>> {
        match self {
            Self::Ledbat(c) => Some(c),
        }
    }
}

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for creating congestion controllers.
///
/// Specifies which algorithm to use and its parameters. The configuration
/// is validated and used to construct the appropriate controller variant.
///
/// ## Example
///
/// ```ignore
/// // Default configuration (LEDBAT++)
/// let config = CongestionControlConfig::default();
/// let controller = config.build();
///
/// // Custom LEDBAT configuration
/// let config = CongestionControlConfig::default()
///     .with_initial_cwnd(50_000)
///     .with_min_ssthresh(Some(25_000));
/// let controller = config.build();
/// ```
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
    /// Create a new configuration for the specified algorithm with defaults.
    pub fn new(algorithm: CongestionControlAlgorithm) -> Self {
        match algorithm {
            CongestionControlAlgorithm::Ledbat => Self::default(),
        }
    }

    /// Create a configuration from an existing LedbatConfig.
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

    /// Set the initial congestion window.
    pub fn with_initial_cwnd(mut self, cwnd: usize) -> Self {
        self.initial_cwnd = cwnd;
        self
    }

    /// Set the minimum congestion window.
    pub fn with_min_cwnd(mut self, cwnd: usize) -> Self {
        self.min_cwnd = cwnd;
        self
    }

    /// Set the maximum congestion window.
    pub fn with_max_cwnd(mut self, cwnd: usize) -> Self {
        self.max_cwnd = cwnd;
        self
    }

    /// Set the initial slow start threshold.
    pub fn with_ssthresh(mut self, ssthresh: usize) -> Self {
        self.ssthresh = ssthresh;
        self
    }

    /// Set the minimum ssthresh floor for timeout recovery.
    pub fn with_min_ssthresh(mut self, min_ssthresh: Option<usize>) -> Self {
        self.min_ssthresh = min_ssthresh;
        self
    }

    /// Build a congestion controller from this configuration.
    ///
    /// Returns an owned `CongestionController`. Use `build_arc()` if you need
    /// shared ownership via `Arc`.
    pub fn build(&self) -> CongestionController<RealTime> {
        self.build_with_time_source(RealTime::new())
    }

    /// Build a congestion controller wrapped in `Arc` for shared ownership.
    ///
    /// This is a convenience method equivalent to `Arc::new(config.build())`.
    pub fn build_arc(&self) -> Arc<CongestionController<RealTime>> {
        Arc::new(self.build())
    }

    /// Build a congestion controller with a custom time source.
    ///
    /// This is useful for deterministic testing with virtual time.
    pub fn build_with_time_source<T: TimeSource>(&self, time_source: T) -> CongestionController<T> {
        self.build_inner(time_source)
    }

    /// Build an Arc-wrapped congestion controller with a custom time source.
    ///
    /// Combines `build_with_time_source` with `Arc` wrapping.
    pub fn build_arc_with_time_source<T: TimeSource>(
        &self,
        time_source: T,
    ) -> Arc<CongestionController<T>> {
        Arc::new(self.build_with_time_source(time_source))
    }

    /// Internal builder implementation.
    fn build_inner<T: TimeSource>(&self, time_source: T) -> CongestionController<T> {
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

                CongestionController::Ledbat(LedbatController::new_with_time_source(
                    ledbat_config,
                    time_source,
                ))
            }
        }
    }

    /// Convert to the native LedbatConfig if this is a LEDBAT configuration.
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
    fn test_build_arc() {
        let config = CongestionControlConfig::default();
        let controller = config.build_arc();

        // Can be cloned (Arc)
        let _clone = controller.clone();
        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
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
    fn test_ledbat_specific_stats_access() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        // Access LEDBAT-specific stats via method
        let ledbat_stats = controller.ledbat_stats();
        assert!(ledbat_stats.is_some());

        // Access via pattern matching
        match &controller {
            CongestionController::Ledbat(ledbat) => {
                let stats = ledbat.stats();
                assert!(stats.cwnd > 0);
                // LEDBAT-specific field
                assert_eq!(stats.periodic_slowdowns, 0);
            }
        }
    }

    #[test]
    fn test_as_ledbat_accessor() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        let ledbat = controller.as_ledbat();
        assert!(ledbat.is_some());
        assert!(ledbat.unwrap().current_cwnd() > 0);
    }

    #[test]
    fn test_ledbat_controller_implements_trait() {
        // Test that LedbatController directly implements CongestionControl
        let ledbat = LedbatController::new(10_000, 2_000, 1_000_000);
        let controller: &dyn CongestionControl = &ledbat;

        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
        assert_eq!(controller.current_cwnd(), 10_000);

        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);
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

        let config = CongestionControlConfig::from_ledbat_config(ledbat_config);
        let controller = config.build();

        assert_eq!(controller.current_cwnd(), 50_000);
        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
    }

    #[test]
    fn test_builder_methods() {
        let config = CongestionControlConfig::default()
            .with_initial_cwnd(60_000)
            .with_min_cwnd(3_000)
            .with_max_cwnd(2_000_000)
            .with_ssthresh(500_000)
            .with_min_ssthresh(Some(50_000));

        assert_eq!(config.initial_cwnd, 60_000);
        assert_eq!(config.min_cwnd, 3_000);
        assert_eq!(config.max_cwnd, 2_000_000);
        assert_eq!(config.ssthresh, 500_000);
        assert_eq!(config.min_ssthresh, Some(50_000));

        let controller = config.build();
        assert_eq!(controller.current_cwnd(), 60_000);
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

    #[test]
    fn test_effective_bandwidth_overflow_protection() {
        let stats = CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Ledbat,
            cwnd: usize::MAX,
            flightsize: 0,
            queuing_delay: Duration::ZERO,
            base_delay: Duration::ZERO,
            peak_cwnd: usize::MAX,
            total_losses: 0,
            total_timeouts: 0,
            ssthresh: usize::MAX,
        };

        // Very small RTT with max cwnd should not overflow
        let bandwidth = stats.effective_bandwidth(Duration::from_nanos(1));
        assert_eq!(bandwidth, usize::MAX);
    }

    #[test]
    fn test_on_loss_reduces_cwnd() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        let initial_cwnd = controller.current_cwnd();
        controller.on_loss();
        let after_loss_cwnd = controller.current_cwnd();

        // Loss should reduce cwnd (typically by half)
        assert!(after_loss_cwnd < initial_cwnd);
    }

    #[test]
    fn test_on_timeout_reduces_cwnd() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        let initial_cwnd = controller.current_cwnd();
        controller.on_timeout();
        let after_timeout_cwnd = controller.current_cwnd();

        // Timeout should reduce cwnd significantly
        assert!(after_timeout_cwnd < initial_cwnd);
    }

    #[test]
    fn test_effective_bandwidth_handles_nan() {
        let stats = CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Ledbat,
            cwnd: 0,
            flightsize: 0,
            queuing_delay: Duration::ZERO,
            base_delay: Duration::ZERO,
            peak_cwnd: 0,
            total_losses: 0,
            total_timeouts: 0,
            ssthresh: 0,
        };

        // Should handle edge cases gracefully
        let bandwidth = stats.effective_bandwidth(Duration::from_nanos(1));
        assert_eq!(bandwidth, 0); // 0 / small_rtt = 0
    }
}
