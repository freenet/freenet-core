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
//! - **FixedRate** (default): Non-adaptive congestion control that transmits at
//!   a constant rate (default 100 Mbps). A pragmatic fallback while adaptive
//!   algorithms are being stabilized.
//! - **BBR**: Model-based congestion control that estimates bandwidth
//!   and RTT, tolerating packet loss as long as bandwidth remains stable.
//! - **LEDBAT++**: Low Extra Delay Background Transport, optimized for
//!   background traffic that should yield to foreground applications.
//!
//! ## Usage
//!
//! ```ignore
//! use freenet_core::transport::congestion_control::{
//!     CongestionController, CongestionControlConfig, CongestionControlAlgorithm,
//! };
//!
//! // Create a BBR controller (default)
//! let config = CongestionControlConfig::default();
//! let controller = config.build();
//!
//! // Use the controller
//! controller.on_send(1000);
//! controller.on_ack(Duration::from_millis(50), 1000);
//!
//! // Access algorithm-specific stats via pattern matching
//! if let CongestionController::Bbr(bbr) = &*controller {
//!     let stats = bbr.stats();
//!     println!("Max bandwidth: {} bytes/sec", stats.max_bw);
//! }
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};

use super::bbr::DeliveryRateToken;
use super::bbr::{BbrConfig, BbrController, BbrStats};
use super::fixed_rate::{FixedRateConfig, FixedRateController};
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
    /// BBR (Bottleneck Bandwidth and Round-trip propagation time)
    ///
    /// Model-based congestion control that estimates bandwidth and RTT.
    /// Tolerates packet loss as long as bandwidth remains stable.
    /// Better suited for lossy or high-latency paths.
    Bbr,

    /// LEDBAT++ (Low Extra Delay Background Transport)
    ///
    /// Delay-based congestion control optimized for background traffic.
    /// Yields to loss-based flows (TCP) and maintains low queuing delay.
    /// Based on draft-irtf-iccrg-ledbat-plus-plus.
    Ledbat,

    /// Fixed-rate (non-adaptive)
    ///
    /// Transmits at a constant rate regardless of network feedback.
    /// A pragmatic fallback when adaptive algorithms have bugs or instabilities.
    /// Default rate: 100 Mbps.
    #[default]
    FixedRate,
}

impl fmt::Display for CongestionControlAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CongestionControlAlgorithm::Bbr => write!(f, "BBR"),
            CongestionControlAlgorithm::Ledbat => write!(f, "LEDBAT++"),
            CongestionControlAlgorithm::FixedRate => write!(f, "FixedRate"),
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
    /// Current slow start threshold (bytes) or BDP for BBR.
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
/// // Get BBR-specific stats via pattern matching
/// match &*controller {
///     CongestionController::Bbr(bbr) => {
///         let stats = bbr.stats();
///         println!("Max bandwidth: {} bytes/sec", stats.max_bw);
///     }
///     CongestionController::Ledbat(ledbat) => {
///         let stats = ledbat.stats();
///         println!("Periodic slowdowns: {}", stats.periodic_slowdowns);
///     }
/// }
/// ```
pub enum CongestionController<T: TimeSource = RealTime> {
    /// BBR congestion controller.
    Bbr(BbrController<T>),
    /// LEDBAT++ congestion controller.
    Ledbat(LedbatController<T>),
    /// Fixed-rate congestion controller.
    FixedRate(FixedRateController<T>),
}

impl<T: TimeSource> fmt::Debug for CongestionController<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bbr(c) => f
                .debug_struct("CongestionController::Bbr")
                .field("cwnd", &c.current_cwnd())
                .field("flightsize", &c.flightsize())
                .finish_non_exhaustive(),
            Self::Ledbat(c) => f
                .debug_struct("CongestionController::Ledbat")
                .field("cwnd", &c.current_cwnd())
                .field("flightsize", &c.flightsize())
                .finish_non_exhaustive(),
            Self::FixedRate(c) => f
                .debug_struct("CongestionController::FixedRate")
                .field("rate", &c.rate())
                .field("flightsize", &c.flightsize())
                .finish_non_exhaustive(),
        }
    }
}

impl<T: TimeSource> CongestionControl for CongestionController<T> {
    fn on_send(&self, bytes: usize) {
        match self {
            Self::Bbr(c) => {
                c.on_send(bytes);
            }
            Self::Ledbat(c) => c.on_send(bytes),
            Self::FixedRate(c) => c.on_send(bytes),
        }
    }

    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        match self {
            Self::Bbr(c) => c.on_ack(rtt_sample, bytes_acked),
            Self::Ledbat(c) => c.on_ack(rtt_sample, bytes_acked),
            Self::FixedRate(c) => c.on_ack(rtt_sample, bytes_acked),
        }
    }

    fn on_ack_without_rtt(&self, bytes_acked: usize) {
        match self {
            Self::Bbr(c) => c.on_ack_without_rtt(bytes_acked),
            Self::Ledbat(c) => c.on_ack_without_rtt(bytes_acked),
            Self::FixedRate(c) => c.on_ack_without_rtt(bytes_acked),
        }
    }

    fn on_loss(&self) {
        match self {
            Self::Bbr(c) => c.on_loss(0), // BBR on_loss takes bytes_lost, use 0 for generic interface
            Self::Ledbat(c) => c.on_loss(),
            Self::FixedRate(c) => c.on_loss(),
        }
    }

    fn on_timeout(&self) {
        match self {
            Self::Bbr(c) => c.on_timeout(),
            Self::Ledbat(c) => c.on_timeout(),
            Self::FixedRate(c) => c.on_timeout(),
        }
    }

    fn current_cwnd(&self) -> usize {
        match self {
            Self::Bbr(c) => c.current_cwnd(),
            Self::Ledbat(c) => c.current_cwnd(),
            Self::FixedRate(c) => c.current_cwnd(),
        }
    }

    fn current_rate(&self, rtt: Duration) -> usize {
        match self {
            Self::Bbr(c) => c.current_rate(rtt) as usize,
            Self::Ledbat(c) => c.current_rate(rtt),
            Self::FixedRate(c) => c.current_rate(rtt),
        }
    }

    fn flightsize(&self) -> usize {
        match self {
            Self::Bbr(c) => c.flightsize(),
            Self::Ledbat(c) => c.flightsize(),
            Self::FixedRate(c) => c.flightsize(),
        }
    }

    fn base_delay(&self) -> Duration {
        match self {
            Self::Bbr(c) => c.base_delay().unwrap_or(Duration::ZERO),
            Self::Ledbat(c) => c.base_delay(),
            Self::FixedRate(c) => c.base_delay(),
        }
    }

    fn queuing_delay(&self) -> Duration {
        match self {
            Self::Bbr(c) => c.queuing_delay().unwrap_or(Duration::ZERO),
            Self::Ledbat(c) => c.queuing_delay(),
            Self::FixedRate(c) => c.queuing_delay(),
        }
    }

    fn peak_cwnd(&self) -> usize {
        match self {
            Self::Bbr(c) => c.current_cwnd(), // BBR doesn't track peak separately
            Self::Ledbat(c) => c.peak_cwnd(),
            Self::FixedRate(c) => c.peak_cwnd(),
        }
    }

    fn stats(&self) -> CongestionControlStats {
        match self {
            Self::Bbr(c) => {
                let s = c.stats();
                CongestionControlStats {
                    algorithm: CongestionControlAlgorithm::Bbr,
                    cwnd: s.cwnd,
                    flightsize: s.flightsize,
                    queuing_delay: Duration::ZERO, // BBR doesn't track queuing delay
                    base_delay: s.min_rtt.unwrap_or(Duration::ZERO),
                    peak_cwnd: s.cwnd, // BBR doesn't track peak separately
                    total_losses: s.lost as usize,
                    total_timeouts: s.timeouts as usize,
                    ssthresh: s.bdp, // Use BDP as equivalent to ssthresh
                }
            }
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
            Self::FixedRate(c) => CongestionControlStats {
                algorithm: CongestionControlAlgorithm::FixedRate,
                cwnd: c.current_cwnd(),
                flightsize: c.flightsize(),
                queuing_delay: Duration::ZERO,
                base_delay: Duration::ZERO,
                peak_cwnd: c.current_cwnd(),
                total_losses: 0, // Fixed rate doesn't track losses
                total_timeouts: 0,
                ssthresh: c.rate(), // Use rate as ssthresh equivalent
            },
        }
    }

    fn algorithm(&self) -> CongestionControlAlgorithm {
        match self {
            Self::Bbr(_) => CongestionControlAlgorithm::Bbr,
            Self::Ledbat(_) => CongestionControlAlgorithm::Ledbat,
            Self::FixedRate(_) => CongestionControlAlgorithm::FixedRate,
        }
    }
}

// =============================================================================
// BbrController Implementation of CongestionControl
// =============================================================================

/// Direct implementation of `CongestionControl` for `BbrController`.
impl<T: TimeSource> CongestionControl for BbrController<T> {
    fn on_send(&self, bytes: usize) {
        BbrController::on_send(self, bytes);
    }

    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        BbrController::on_ack(self, rtt_sample, bytes_acked)
    }

    fn on_ack_without_rtt(&self, bytes_acked: usize) {
        BbrController::on_ack_without_rtt(self, bytes_acked)
    }

    fn on_loss(&self) {
        BbrController::on_loss(self, 0)
    }

    fn on_timeout(&self) {
        BbrController::on_timeout(self)
    }

    fn current_cwnd(&self) -> usize {
        BbrController::current_cwnd(self)
    }

    fn current_rate(&self, rtt: Duration) -> usize {
        BbrController::current_rate(self, rtt) as usize
    }

    fn flightsize(&self) -> usize {
        BbrController::flightsize(self)
    }

    fn base_delay(&self) -> Duration {
        BbrController::base_delay(self).unwrap_or(Duration::ZERO)
    }

    fn queuing_delay(&self) -> Duration {
        BbrController::queuing_delay(self).unwrap_or(Duration::ZERO)
    }

    fn peak_cwnd(&self) -> usize {
        BbrController::current_cwnd(self)
    }

    fn stats(&self) -> CongestionControlStats {
        let s = BbrController::stats(self);
        CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Bbr,
            cwnd: s.cwnd,
            flightsize: s.flightsize,
            queuing_delay: Duration::ZERO,
            base_delay: s.min_rtt.unwrap_or(Duration::ZERO),
            peak_cwnd: s.cwnd,
            total_losses: s.lost as usize,
            total_timeouts: s.timeouts as usize,
            ssthresh: s.bdp,
        }
    }

    fn algorithm(&self) -> CongestionControlAlgorithm {
        CongestionControlAlgorithm::Bbr
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

// =============================================================================
// FixedRateController Implementation of CongestionControl
// =============================================================================

/// Direct implementation of `CongestionControl` for `FixedRateController`.
impl<T: TimeSource> CongestionControl for FixedRateController<T> {
    fn on_send(&self, bytes: usize) {
        FixedRateController::on_send(self, bytes)
    }

    fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        FixedRateController::on_ack(self, rtt_sample, bytes_acked)
    }

    fn on_ack_without_rtt(&self, bytes_acked: usize) {
        FixedRateController::on_ack_without_rtt(self, bytes_acked)
    }

    fn on_loss(&self) {
        FixedRateController::on_loss(self)
    }

    fn on_timeout(&self) {
        FixedRateController::on_timeout(self)
    }

    fn current_cwnd(&self) -> usize {
        FixedRateController::current_cwnd(self)
    }

    fn current_rate(&self, rtt: Duration) -> usize {
        FixedRateController::current_rate(self, rtt)
    }

    fn flightsize(&self) -> usize {
        FixedRateController::flightsize(self)
    }

    fn base_delay(&self) -> Duration {
        FixedRateController::base_delay(self)
    }

    fn queuing_delay(&self) -> Duration {
        FixedRateController::queuing_delay(self)
    }

    fn peak_cwnd(&self) -> usize {
        FixedRateController::peak_cwnd(self)
    }

    fn stats(&self) -> CongestionControlStats {
        CongestionControlStats {
            algorithm: CongestionControlAlgorithm::FixedRate,
            cwnd: self.current_cwnd(),
            flightsize: self.flightsize(),
            queuing_delay: Duration::ZERO,
            base_delay: Duration::ZERO,
            peak_cwnd: self.current_cwnd(),
            total_losses: 0,
            total_timeouts: 0,
            ssthresh: self.rate(),
        }
    }

    fn algorithm(&self) -> CongestionControlAlgorithm {
        CongestionControlAlgorithm::FixedRate
    }
}

impl<T: TimeSource> CongestionController<T> {
    /// Get BBR-specific statistics if this is a BBR controller.
    pub fn bbr_stats(&self) -> Option<BbrStats> {
        match self {
            Self::Bbr(c) => Some(c.stats()),
            Self::Ledbat(_) | Self::FixedRate(_) => None,
        }
    }

    /// Get LEDBAT-specific statistics if this is a LEDBAT controller.
    ///
    /// Returns `Some(LedbatStats)` for LEDBAT controllers, enabling access
    /// to LEDBAT-specific metrics like periodic slowdown counts.
    pub fn ledbat_stats(&self) -> Option<LedbatStats> {
        match self {
            Self::Bbr(_) | Self::FixedRate(_) => None,
            Self::Ledbat(c) => Some(c.stats()),
        }
    }

    /// Get a reference to the inner BBR controller if applicable.
    pub fn as_bbr(&self) -> Option<&BbrController<T>> {
        match self {
            Self::Bbr(c) => Some(c),
            Self::Ledbat(_) | Self::FixedRate(_) => None,
        }
    }

    /// Get a reference to the inner LEDBAT controller if applicable.
    pub fn as_ledbat(&self) -> Option<&LedbatController<T>> {
        match self {
            Self::Bbr(_) | Self::FixedRate(_) => None,
            Self::Ledbat(c) => Some(c),
        }
    }

    /// Get a reference to the inner FixedRate controller if applicable.
    pub fn as_fixed_rate(&self) -> Option<&FixedRateController<T>> {
        match self {
            Self::Bbr(_) | Self::Ledbat(_) => None,
            Self::FixedRate(c) => Some(c),
        }
    }

    /// Called when a packet is sent. Returns a delivery rate token for BBR.
    ///
    /// For BBR, the returned token must be stored with the sent packet and passed
    /// to `on_ack_with_token` when the ACK arrives. This enables accurate delivery
    /// rate estimation, which is critical for BBR's bandwidth probing.
    ///
    /// For LEDBAT and FixedRate, returns None (they don't use per-packet tokens).
    pub fn on_send_with_token(&self, bytes: usize) -> Option<DeliveryRateToken> {
        match self {
            Self::Bbr(c) => Some(c.on_send(bytes)),
            Self::Ledbat(c) => {
                c.on_send(bytes);
                None
            }
            Self::FixedRate(c) => {
                c.on_send(bytes);
                None
            }
        }
    }

    /// Called when an ACK is received, with optional delivery rate token.
    ///
    /// For BBR, passing the token from the original send enables accurate delivery
    /// rate computation. Without the token, BBR falls back to a per-packet estimate
    /// (bytes_acked / rtt) which is less accurate and can cause cwnd to stall.
    ///
    /// For LEDBAT and FixedRate, the token is ignored.
    pub fn on_ack_with_token(
        &self,
        rtt_sample: Duration,
        bytes_acked: usize,
        token: Option<DeliveryRateToken>,
    ) {
        match self {
            Self::Bbr(c) => c.on_ack_with_token(rtt_sample, bytes_acked, token),
            Self::Ledbat(c) => c.on_ack(rtt_sample, bytes_acked),
            Self::FixedRate(c) => c.on_ack(rtt_sample, bytes_acked),
        }
    }

    /// Returns the configured fixed rate in bytes/sec, or 0 for adaptive algorithms.
    ///
    /// This is useful for telemetry to distinguish between fixed-rate transfers
    /// (where the rate is a configuration choice) and adaptive transfers
    /// (where the rate adapts to network conditions).
    pub fn configured_rate(&self) -> usize {
        match self {
            Self::Bbr(_) => 0,
            Self::Ledbat(_) => 0,
            Self::FixedRate(c) => c.rate(),
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
/// // Default configuration (BBR)
/// let config = CongestionControlConfig::default();
/// let controller = config.build();
///
/// // Custom LEDBAT configuration
/// let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat)
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
    /// BBR specific configuration.
    Bbr,
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
    /// Fixed-rate specific configuration.
    FixedRate {
        /// Target transmission rate in bytes per second.
        rate_bytes_per_sec: usize,
    },
}

impl Default for CongestionControlConfig {
    fn default() -> Self {
        // Default to FixedRate (100 Mbps) - pragmatic choice while adaptive algorithms are unstable
        use super::fixed_rate::DEFAULT_RATE_BYTES_PER_SEC;
        Self {
            algorithm: CongestionControlAlgorithm::FixedRate,
            initial_cwnd: usize::MAX / 2, // Not used by FixedRate
            min_cwnd: usize::MAX / 2,
            max_cwnd: usize::MAX / 2,
            ssthresh: DEFAULT_RATE_BYTES_PER_SEC,
            min_ssthresh: None,
            algorithm_config: AlgorithmConfig::FixedRate {
                rate_bytes_per_sec: DEFAULT_RATE_BYTES_PER_SEC,
            },
        }
    }
}

impl CongestionControlConfig {
    /// Create a new configuration for the specified algorithm with defaults.
    pub fn new(algorithm: CongestionControlAlgorithm) -> Self {
        match algorithm {
            CongestionControlAlgorithm::Bbr => Self::from_bbr_config(BbrConfig::default()),
            CongestionControlAlgorithm::Ledbat => Self::from_ledbat_config(LedbatConfig::default()),
            CongestionControlAlgorithm::FixedRate => Self::default(),
        }
    }

    /// Create a configuration for fixed-rate with the specified rate in bytes/sec.
    pub fn fixed_rate(rate_bytes_per_sec: usize) -> Self {
        Self::from_fixed_rate_config(FixedRateConfig::new(rate_bytes_per_sec))
    }

    /// Create a configuration for fixed-rate with the specified rate in Mbps.
    pub fn fixed_rate_mbps(mbps: usize) -> Self {
        Self::from_fixed_rate_config(FixedRateConfig::from_mbps(mbps))
    }

    /// Create a configuration from an existing FixedRateConfig.
    pub fn from_fixed_rate_config(config: FixedRateConfig) -> Self {
        Self {
            algorithm: CongestionControlAlgorithm::FixedRate,
            initial_cwnd: usize::MAX / 2,
            min_cwnd: usize::MAX / 2,
            max_cwnd: usize::MAX / 2,
            ssthresh: config.rate_bytes_per_sec,
            min_ssthresh: None,
            algorithm_config: AlgorithmConfig::FixedRate {
                rate_bytes_per_sec: config.rate_bytes_per_sec,
            },
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

    /// Create a configuration from an existing BbrConfig.
    pub fn from_bbr_config(config: BbrConfig) -> Self {
        Self {
            algorithm: CongestionControlAlgorithm::Bbr,
            initial_cwnd: config.initial_cwnd,
            min_cwnd: config.min_cwnd,
            max_cwnd: config.max_cwnd,
            ssthresh: 1_000_000,
            min_ssthresh: None,
            algorithm_config: AlgorithmConfig::Bbr,
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
            CongestionControlAlgorithm::Bbr => {
                let bbr_config = BbrConfig {
                    initial_cwnd: self.initial_cwnd,
                    min_cwnd: self.min_cwnd,
                    max_cwnd: self.max_cwnd,
                    ..Default::default()
                };

                CongestionController::Bbr(BbrController::new_with_time_source(
                    bbr_config,
                    time_source,
                ))
            }
            CongestionControlAlgorithm::Ledbat => {
                let AlgorithmConfig::Ledbat {
                    enable_slow_start,
                    delay_exit_threshold,
                    enable_periodic_slowdown,
                    randomize_ssthresh,
                } = &self.algorithm_config
                else {
                    // Shouldn't happen, but fallback to defaults
                    let ledbat_config = LedbatConfig {
                        initial_cwnd: self.initial_cwnd,
                        min_cwnd: self.min_cwnd,
                        max_cwnd: self.max_cwnd,
                        ssthresh: self.ssthresh,
                        min_ssthresh: self.min_ssthresh,
                        ..Default::default()
                    };
                    return CongestionController::Ledbat(LedbatController::new_with_time_source(
                        ledbat_config,
                        time_source,
                    ));
                };

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
            CongestionControlAlgorithm::FixedRate => {
                let rate = match &self.algorithm_config {
                    AlgorithmConfig::FixedRate { rate_bytes_per_sec } => *rate_bytes_per_sec,
                    _ => super::fixed_rate::DEFAULT_RATE_BYTES_PER_SEC,
                };

                let fixed_rate_config = FixedRateConfig::new(rate);
                CongestionController::FixedRate(FixedRateController::new_with_time_source(
                    fixed_rate_config,
                    time_source,
                ))
            }
        }
    }

    /// Convert to the native LedbatConfig if this is a LEDBAT configuration.
    pub fn as_ledbat_config(&self) -> Option<LedbatConfig> {
        match &self.algorithm_config {
            AlgorithmConfig::Bbr | AlgorithmConfig::FixedRate { .. } => None,
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

    /// Convert to the native BbrConfig if this is a BBR configuration.
    pub fn as_bbr_config(&self) -> Option<BbrConfig> {
        match &self.algorithm_config {
            AlgorithmConfig::Bbr => Some(BbrConfig {
                initial_cwnd: self.initial_cwnd,
                min_cwnd: self.min_cwnd,
                max_cwnd: self.max_cwnd,
                ..Default::default()
            }),
            AlgorithmConfig::Ledbat { .. } | AlgorithmConfig::FixedRate { .. } => None,
        }
    }

    /// Convert to the native FixedRateConfig if this is a FixedRate configuration.
    pub fn as_fixed_rate_config(&self) -> Option<FixedRateConfig> {
        match &self.algorithm_config {
            AlgorithmConfig::FixedRate { rate_bytes_per_sec } => {
                Some(FixedRateConfig::new(*rate_bytes_per_sec))
            }
            AlgorithmConfig::Bbr | AlgorithmConfig::Ledbat { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    #[test]
    fn test_default_config_creates_fixed_rate() {
        let config = CongestionControlConfig::default();
        assert_eq!(config.algorithm, CongestionControlAlgorithm::FixedRate);
    }

    #[test]
    fn test_build_fixed_rate_controller() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        assert_eq!(
            controller.algorithm(),
            CongestionControlAlgorithm::FixedRate
        );
        assert!(controller.current_cwnd() > 0);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_build_bbr_controller() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Bbr);
        let controller = config.build();

        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Bbr);
        assert!(controller.current_cwnd() > 0);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_build_ledbat_controller() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
        let controller = config.build();

        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Ledbat);
        assert!(controller.current_cwnd() > 0);
    }

    #[test]
    fn test_build_arc() {
        let config = CongestionControlConfig::default();
        let controller = config.build_arc();

        // Can be cloned (Arc)
        let _clone = controller.clone();
        assert_eq!(
            controller.algorithm(),
            CongestionControlAlgorithm::FixedRate
        );
    }

    #[test]
    fn test_build_with_virtual_time() {
        let config = CongestionControlConfig::default();
        let time_source = VirtualTime::new();
        let controller = config.build_with_time_source(time_source);

        assert_eq!(
            controller.algorithm(),
            CongestionControlAlgorithm::FixedRate
        );
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
        assert_eq!(stats.algorithm, CongestionControlAlgorithm::FixedRate);
        assert!(stats.cwnd > 0);
    }

    #[test]
    fn test_fixed_rate_specific_stats_access() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        // Access FixedRate-specific stats via method
        let fixed_rate = controller.as_fixed_rate();
        assert!(fixed_rate.is_some());

        // Access via pattern matching
        match &controller {
            CongestionController::FixedRate(fr) => {
                assert_eq!(
                    fr.rate(),
                    super::super::fixed_rate::DEFAULT_RATE_BYTES_PER_SEC
                );
            }
            _ => panic!("Expected FixedRate"),
        }
    }

    #[test]
    fn test_bbr_specific_stats_access() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Bbr);
        let controller = config.build();

        // Access BBR-specific stats via method
        let bbr_stats = controller.bbr_stats();
        assert!(bbr_stats.is_some());

        // Access via pattern matching
        match &controller {
            CongestionController::Bbr(bbr) => {
                let stats = bbr.stats();
                assert!(stats.cwnd > 0);
            }
            _ => panic!("Expected BBR"),
        }
    }

    #[test]
    fn test_ledbat_specific_stats_access() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
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
            _ => panic!("Expected LEDBAT"),
        }
    }

    #[test]
    fn test_as_bbr_accessor() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Bbr);
        let controller = config.build();

        let bbr = controller.as_bbr();
        assert!(bbr.is_some());
        assert!(bbr.unwrap().current_cwnd() > 0);
    }

    #[test]
    fn test_as_ledbat_accessor() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
        let controller = config.build();

        let ledbat = controller.as_ledbat();
        assert!(ledbat.is_some());
        assert!(ledbat.unwrap().current_cwnd() > 0);
    }

    #[test]
    fn test_as_fixed_rate_accessor() {
        let config = CongestionControlConfig::default();
        let controller = config.build();

        let fixed_rate = controller.as_fixed_rate();
        assert!(fixed_rate.is_some());
        assert_eq!(
            fixed_rate.unwrap().rate(),
            super::super::fixed_rate::DEFAULT_RATE_BYTES_PER_SEC
        );
    }

    #[test]
    fn test_fixed_rate_mbps_constructor() {
        let config = CongestionControlConfig::fixed_rate_mbps(50);
        let controller = config.build();

        assert_eq!(
            controller.algorithm(),
            CongestionControlAlgorithm::FixedRate
        );
        let fixed_rate = controller.as_fixed_rate().unwrap();
        assert_eq!(fixed_rate.rate(), 100 * 1_000_000 / 8); // 100 Mbps in bytes/sec
    }

    #[test]
    fn test_bbr_controller_implements_trait() {
        // Test that BbrController directly implements CongestionControl
        let bbr = BbrController::new(BbrConfig::default());
        let controller: &dyn CongestionControl = &bbr;

        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Bbr);
        assert!(controller.current_cwnd() > 0);

        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);
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
    fn test_from_bbr_config() {
        let bbr_config = BbrConfig {
            initial_cwnd: 60_000,
            min_cwnd: 3_000,
            max_cwnd: 2_000_000,
            ..Default::default()
        };

        let config = CongestionControlConfig::from_bbr_config(bbr_config);
        let controller = config.build();

        assert_eq!(controller.current_cwnd(), 60_000);
        assert_eq!(controller.algorithm(), CongestionControlAlgorithm::Bbr);
    }

    #[test]
    fn test_builder_methods() {
        // Use BBR explicitly since it respects cwnd settings
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Bbr)
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
        assert_eq!(format!("{}", CongestionControlAlgorithm::Bbr), "BBR");
        assert_eq!(
            format!("{}", CongestionControlAlgorithm::Ledbat),
            "LEDBAT++"
        );
    }

    #[test]
    fn test_effective_bandwidth() {
        let stats = CongestionControlStats {
            algorithm: CongestionControlAlgorithm::Bbr,
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
            algorithm: CongestionControlAlgorithm::Bbr,
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
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
        let controller = config.build();

        let initial_cwnd = controller.current_cwnd();
        controller.on_loss();
        let after_loss_cwnd = controller.current_cwnd();

        // Loss should reduce cwnd (typically by half)
        assert!(after_loss_cwnd < initial_cwnd);
    }

    #[test]
    fn test_on_timeout_reduces_cwnd() {
        let config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
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
            algorithm: CongestionControlAlgorithm::Bbr,
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

    /// Regression test: BBR's complete state reset on timeout vs LEDBAT's adaptive behavior.
    ///
    /// This test documents the behavioral difference that causes BBR to suffer "timeout storms"
    /// on high-latency networks (935 timeouts in 10s observed in production).
    ///
    /// Key issue: BBR resets ALL state on timeout (cwnd, bandwidth estimates, min_rtt),
    /// which means it cannot recover in scenarios where spurious timeouts keep occurring.
    /// LEDBAT maintains an adaptive floor based on past measurements.
    #[test]
    fn test_bbr_vs_ledbat_timeout_recovery() {
        let time = VirtualTime::new();

        // Create BBR controller (explicitly request BBR)
        let bbr_config = CongestionControlConfig::new(CongestionControlAlgorithm::Bbr);
        let bbr = bbr_config.build_with_time_source(time.clone());

        // Create LEDBAT controller
        let ledbat_config = CongestionControlConfig::new(CongestionControlAlgorithm::Ledbat);
        let ledbat = ledbat_config.build_with_time_source(time.clone());

        // BBR's initial_cwnd (10 * MSS)
        let bbr_initial = BbrConfig::default().initial_cwnd;

        // Simulate traffic to build up state, then trigger repeated timeouts
        // This simulates the "timeout storm" seen in production

        // Phase 1: Build up cwnd with normal traffic
        for _ in 0..50 {
            for _ in 0..20 {
                bbr.on_send(1400);
                ledbat.on_send(1400);
            }
            time.advance(Duration::from_millis(50));
            for _ in 0..20 {
                bbr.on_ack(Duration::from_millis(50), 1400);
                ledbat.on_ack(Duration::from_millis(50), 1400);
            }
        }

        let ledbat_cwnd_after_warmup = ledbat.current_cwnd();

        // LEDBAT should have grown significantly
        assert!(
            ledbat_cwnd_after_warmup > 100_000,
            "LEDBAT cwnd ({}) should have grown significantly during warmup",
            ledbat_cwnd_after_warmup
        );

        // Phase 2: Simulate "timeout storm" - 10 consecutive timeouts
        for _ in 0..10 {
            bbr.on_timeout();
            ledbat.on_timeout();

            // BBR always resets to initial_cwnd - no memory of past state
            assert_eq!(
                bbr.current_cwnd(),
                bbr_initial,
                "BBR should reset to initial_cwnd on every timeout"
            );
        }

        // BBR is stuck at initial_cwnd after timeout storm
        assert_eq!(
            bbr.current_cwnd(),
            bbr_initial,
            "BBR remains stuck at initial_cwnd after timeout storm"
        );

        // LEDBAT maintains higher cwnd through its adaptive floor
        assert!(
            ledbat.current_cwnd() > bbr.current_cwnd(),
            "LEDBAT should maintain higher cwnd than BBR after timeout storm"
        );
    }
}
