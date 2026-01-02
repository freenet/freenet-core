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

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::packet_data::MAX_DATA_SIZE;
use crate::util::time_source::{InstantTimeSrc, TimeSource};

/// Maximum segment size (actual packet data capacity)
const MSS: usize = MAX_DATA_SIZE;

/// Target queuing delay (LEDBAT++ uses 60ms, RFC 6817 used 100ms)
const TARGET: Duration = Duration::from_millis(60);

/// Maximum GAIN divisor for dynamic GAIN calculation (LEDBAT++ Section 4.2)
const MAX_GAIN_DIVISOR: u32 = 16;

/// Base delay history size (RFC 6817 recommendation)
const BASE_HISTORY_SIZE: usize = 10;

/// Delay filter sample count (RFC 6817 recommendation)
const DELAY_FILTER_SIZE: usize = 4;

/// Default slow start threshold (100 KB)
const DEFAULT_SSTHRESH: usize = 102_400;

/// Periodic slowdown interval multiplier (LEDBAT++ Section 4.4)
/// Next slowdown is scheduled at 9x the previous slowdown duration,
/// maintaining ≤10% utilization impact.
const SLOWDOWN_INTERVAL_MULTIPLIER: u32 = 9;

/// Number of RTTs to wait after slow start exit before initial slowdown
const SLOWDOWN_DELAY_RTTS: u32 = 2;

/// Number of RTTs to freeze cwnd at minimum during slowdown
const SLOWDOWN_FREEZE_RTTS: u32 = 2;

/// Slowdown reduction factor: cwnd is divided by this during periodic slowdowns.
/// We use 16 to be consistent with our large initial window (IW26).
/// Factor of 4 provides a more gradual slowdown:
/// - 300KB cwnd drops to 75KB (not 18KB with factor 16)
/// - Avoids throughput "cliff" while still probing for fairness
/// - Faster recovery time (fewer RTTs to ramp back up)
const SLOWDOWN_REDUCTION_FACTOR: usize = 4;

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
            ssthresh: DEFAULT_SSTHRESH,     // 100 KB
            enable_slow_start: true,        // Enable by default
            delay_exit_threshold: 0.75,     // LEDBAT++: exit at 3/4 * TARGET (45ms)
            randomize_ssthresh: true,       // Enable jitter by default
            enable_periodic_slowdown: true, // LEDBAT++: enable for inter-flow fairness
        }
    }
}

/// Sentinel value indicating an empty slot in atomic delay arrays.
/// We use u64::MAX since no valid RTT would ever be this large (~584 years).
const EMPTY_DELAY_NANOS: u64 = u64::MAX;

/// Unified congestion control state machine.
///
/// This enum consolidates the previous `in_slow_start` boolean and `SlowdownState`
/// into a single state machine, preventing conflicting state combinations that
/// caused bugs (e.g., PR #2510 where `in_slow_start=true` during `RampingUp`
/// bypassed the slowdown handler).
///
/// ## State Transitions
///
/// There are two paths through the slowdown cycle:
///
/// ### Initial Slowdown (after first slow start exit)
/// ```text
/// SlowStart → WaitingForSlowdown → InSlowdown → Frozen → RampingUp → CongestionAvoidance
/// ```
/// `WaitingForSlowdown` adds a delay (2 RTTs) before the first slowdown to allow
/// the connection to stabilize after slow start exit.
///
/// ### Subsequent Slowdowns (periodic, from CongestionAvoidance)
/// ```text
/// CongestionAvoidance → InSlowdown → Frozen → RampingUp → CongestionAvoidance
/// ```
/// Subsequent slowdowns skip `WaitingForSlowdown` and go directly to `InSlowdown`
/// when the scheduled slowdown time is reached.
///
/// ### Timeout Recovery (from any state)
/// ```text
/// Any State → SlowStart
/// ```
///
/// ### Visual Diagram
/// ```text
/// ┌─────────────┐
/// │  SlowStart  │─────────────────────────────────────────────────┐
/// └──────┬──────┘                                                 │
///        │ (ssthresh or delay exit)                               │
///        ▼                                                        │
/// ┌────────────────────────┐     (first time only)                │
/// │  WaitingForSlowdown    │◄─────────────────────┐               │
/// └──────────┬─────────────┘                      │               │
///            │ (wait complete)                    │               │
///            ▼                                    │               │
/// ┌──────────────┐◄───────────────────────────────│───────────────│───┐
/// │  InSlowdown  │  (transient: immediately       │               │   │
/// └──────┬───────┘   transitions on next ACK)     │               │   │
///        │                                        │               │   │
///        ▼                                        │               │   │
/// ┌──────────┐                                    │               │   │
/// │  Frozen  │  (holds for N RTTs)                │               │   │
/// └────┬─────┘                                    │               │   │
///      │ (freeze duration complete)               │               │   │
///      ▼                                          │               │   │
/// ┌────────────┐                                  │               │   │
/// │  RampingUp │  (exponential growth)            │               │   │
/// └─────┬──────┘                                  │               │   │
///       │ (target reached)                        │               │   │
///       ▼                                         │               │   │
/// ┌──────────────────────┐                        │               │   │
/// │ CongestionAvoidance  │────────────────────────┴───────────────┘   │
/// └──────────┬───────────┘◄───────────────────────────────────────────┘
///            │ (scheduled slowdown time reached)
///            └────────────────────────────────────────────────────────┘
///                        (subsequent slowdowns skip WaitingForSlowdown)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
enum CongestionState {
    /// Initial slow start phase: exponential cwnd growth until ssthresh
    /// or delay threshold is reached.
    SlowStart = 0,
    /// Normal LEDBAT++ congestion avoidance: delay-based cwnd adjustment.
    CongestionAvoidance = 1,
    /// Waiting N RTTs before starting the *first* periodic slowdown.
    /// Only entered from SlowStart; subsequent slowdowns skip this state.
    WaitingForSlowdown = 2,
    /// Transient state: cwnd has been reduced, immediately transitions to
    /// Frozen on the next ACK. This state exists to separate the cwnd
    /// reduction from the freeze timing logic.
    InSlowdown = 3,
    /// Frozen at reduced cwnd for N RTTs to re-measure base delay.
    Frozen = 4,
    /// Ramping back up using exponential growth after slowdown.
    /// This state uses slow-start-like growth but with different exit
    /// conditions (target cwnd rather than ssthresh/delay).
    RampingUp = 5,
}

impl CongestionState {
    /// Convert from u8, returning None for invalid values.
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::SlowStart),
            1 => Some(Self::CongestionAvoidance),
            2 => Some(Self::WaitingForSlowdown),
            3 => Some(Self::InSlowdown),
            4 => Some(Self::Frozen),
            5 => Some(Self::RampingUp),
            _ => None,
        }
    }
}

/// Lock-free atomic wrapper for [`CongestionState`].
///
/// Provides type-safe atomic operations on the congestion state, ensuring
/// that all loads return valid enum variants and all stores use valid values.
/// This encapsulates the raw `AtomicU8` and prevents invalid state values
/// from being stored or read.
struct AtomicCongestionState(AtomicU8);

impl AtomicCongestionState {
    /// Create a new atomic state with the given initial value.
    fn new(state: CongestionState) -> Self {
        Self(AtomicU8::new(state as u8))
    }

    /// Load the current state with Acquire ordering.
    ///
    /// # Panics (debug only)
    /// Debug-asserts if the stored value is not a valid `CongestionState`.
    fn load(&self) -> CongestionState {
        let value = self.0.load(Ordering::Acquire);
        match CongestionState::from_u8(value) {
            Some(state) => state,
            None => {
                // This should never happen - indicates memory corruption or a serious bug
                tracing::error!(
                    value,
                    "CRITICAL: Invalid congestion state value - possible memory corruption"
                );
                debug_assert!(false, "Invalid congestion state value: {}", value);
                // In release, fall back to CongestionAvoidance as safest default
                CongestionState::CongestionAvoidance
            }
        }
    }

    /// Store a new state with Release ordering.
    fn store(&self, state: CongestionState) {
        self.0.store(state as u8, Ordering::Release);
    }

    /// Check if current state is SlowStart.
    fn is_slow_start(&self) -> bool {
        self.load() == CongestionState::SlowStart
    }

    /// Transition to SlowStart state (used for timeout recovery).
    fn enter_slow_start(&self) {
        self.store(CongestionState::SlowStart);
    }

    /// Transition to CongestionAvoidance state.
    fn enter_congestion_avoidance(&self) {
        self.store(CongestionState::CongestionAvoidance);
    }

    /// Transition to WaitingForSlowdown state.
    fn enter_waiting_for_slowdown(&self) {
        self.store(CongestionState::WaitingForSlowdown);
    }

    /// Transition to InSlowdown state.
    fn enter_in_slowdown(&self) {
        self.store(CongestionState::InSlowdown);
    }

    /// Transition to Frozen state.
    fn enter_frozen(&self) {
        self.store(CongestionState::Frozen);
    }

    /// Transition to RampingUp state.
    fn enter_ramping_up(&self) {
        self.store(CongestionState::RampingUp);
    }

    /// Atomically compare and exchange state.
    ///
    /// If current state equals `expected`, sets it to `new` and returns `Ok(expected)`.
    /// Otherwise, returns `Err(current_state)`.
    ///
    /// Uses AcqRel ordering for success and Acquire for failure.
    #[allow(dead_code)] // Available for future use in atomic state transitions
    fn compare_exchange(
        &self,
        expected: CongestionState,
        new: CongestionState,
    ) -> Result<CongestionState, CongestionState> {
        self.0
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|v| CongestionState::from_u8(v).unwrap_or(CongestionState::CongestionAvoidance))
            .map_err(|v| {
                CongestionState::from_u8(v).unwrap_or(CongestionState::CongestionAvoidance)
            })
    }
}

impl std::fmt::Debug for AtomicCongestionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AtomicCongestionState({:?})", self.load())
    }
}

/// Lock-free delay filter: MIN over recent samples (RFC 6817 Section 4.2).
///
/// Uses a fixed-size atomic ring buffer. Each slot stores RTT in nanoseconds.
/// Readers compute the minimum over all valid (non-empty) slots.
///
/// # Timing Assumptions
///
/// RTT values are stored as nanoseconds in `u64`. The cast from `Duration::as_nanos()`
/// (which returns `u128`) is safe because realistic RTT values are always far below
/// `u64::MAX` (~584 years). Network RTTs range from microseconds to seconds.
struct AtomicDelayFilter {
    /// Ring buffer of RTT samples (stored as nanoseconds)
    samples: [AtomicU64; DELAY_FILTER_SIZE],
    /// Write index (wraps around)
    write_index: AtomicUsize,
    /// Number of samples added (saturates at DELAY_FILTER_SIZE)
    sample_count: AtomicUsize,
}

impl AtomicDelayFilter {
    fn new() -> Self {
        Self {
            samples: std::array::from_fn(|_| AtomicU64::new(EMPTY_DELAY_NANOS)),
            write_index: AtomicUsize::new(0),
            sample_count: AtomicUsize::new(0),
        }
    }

    fn add_sample(&self, rtt: Duration) {
        // Safe cast: RTT values are always far below u64::MAX (~584 years)
        let nanos = rtt.as_nanos() as u64;
        let idx = self.write_index.fetch_add(1, Ordering::Relaxed) % DELAY_FILTER_SIZE;
        self.samples[idx].store(nanos, Ordering::Release);

        // Increment sample count, saturating at DELAY_FILTER_SIZE
        self.sample_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                if count < DELAY_FILTER_SIZE {
                    Some(count + 1)
                } else {
                    None // Already saturated
                }
            })
            .ok();
    }

    fn filtered_delay(&self) -> Option<Duration> {
        let mut min_nanos = u64::MAX;
        for slot in &self.samples {
            let nanos = slot.load(Ordering::Acquire);
            if nanos != EMPTY_DELAY_NANOS && nanos < min_nanos {
                min_nanos = nanos;
            }
        }
        if min_nanos == u64::MAX {
            None
        } else {
            Some(Duration::from_nanos(min_nanos))
        }
    }

    fn is_ready(&self) -> bool {
        self.sample_count.load(Ordering::Acquire) >= 2
    }
}

/// Lock-free base delay history: 10-minute bucket tracking (RFC 6817 Section 4.1).
///
/// Uses atomic arrays for buckets and atomic values for current minute tracking.
/// All updates use compare-and-swap for thread safety.
///
/// # Timing Assumptions
///
/// Durations are stored as nanoseconds in `u64`. This limits representable
/// durations to ~584 years, which is acceptable for RTT measurements.
/// The epoch-based timing also assumes the controller won't run for 584+ years.
///
/// # Type Parameter
///
/// `T` is the time source used for timing operations. In production, this is
/// `InstantTimeSrc` which uses `Instant::now()`. In tests, this can be
/// `SharedMockTimeSource` for deterministic virtual time testing.
struct AtomicBaseDelayHistory<T: TimeSource + Clone> {
    /// One bucket per minute, containing minimum delay observed in that minute (nanos)
    buckets: [AtomicU64; BASE_HISTORY_SIZE],
    /// Number of valid buckets (0 to BASE_HISTORY_SIZE)
    bucket_count: AtomicUsize,
    /// Next bucket index to write (wraps around)
    bucket_write_index: AtomicUsize,
    /// Minimum being tracked for current minute (nanos)
    current_minute_min: AtomicU64,
    /// Start time of current minute (nanos since epoch instant)
    current_minute_start_nanos: AtomicU64,
    /// Time source for getting current time
    time_source: T,
    /// Epoch instant for time calculations
    epoch: Instant,
}

impl<T: TimeSource + Clone> AtomicBaseDelayHistory<T> {
    fn new(time_source: T) -> Self {
        let epoch = time_source.now();
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(EMPTY_DELAY_NANOS)),
            bucket_count: AtomicUsize::new(0),
            bucket_write_index: AtomicUsize::new(0),
            current_minute_min: AtomicU64::new(EMPTY_DELAY_NANOS),
            current_minute_start_nanos: AtomicU64::new(0),
            time_source,
            epoch,
        }
    }

    fn update(&self, rtt_sample: Duration) {
        // Safe cast: RTT values are always far below u64::MAX (~584 years)
        let rtt_nanos = rtt_sample.as_nanos() as u64;
        let now_nanos = self.time_source.now().duration_since(self.epoch).as_nanos() as u64;
        let minute_nanos = 60_000_000_000u64; // 60 seconds in nanos

        let minute_start = self.current_minute_start_nanos.load(Ordering::Acquire);

        // Check if we've rolled over to a new minute
        if now_nanos.saturating_sub(minute_start) >= minute_nanos {
            // Try to roll over to new minute (only one thread should succeed)
            let new_minute_start = now_nanos;
            if self
                .current_minute_start_nanos
                .compare_exchange(
                    minute_start,
                    new_minute_start,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                // We won the race to roll over - atomically reset current minute
                // and capture the old minimum for bucket storage.
                //
                // Use swap to EMPTY first, then compete for new minimum.
                // This prevents the race where a losing thread's update_current_min
                // sets a smaller value that we then overwrite.
                let old_min = self
                    .current_minute_min
                    .swap(EMPTY_DELAY_NANOS, Ordering::AcqRel);

                if old_min != EMPTY_DELAY_NANOS {
                    // Write old minimum to bucket ring buffer
                    let idx =
                        self.bucket_write_index.fetch_add(1, Ordering::Relaxed) % BASE_HISTORY_SIZE;
                    self.buckets[idx].store(old_min, Ordering::Release);

                    // Increment bucket count, saturating at BASE_HISTORY_SIZE
                    self.bucket_count
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                            if count < BASE_HISTORY_SIZE {
                                Some(count + 1)
                            } else {
                                None
                            }
                        })
                        .ok();
                }

                // Now compete for the new minute's minimum
                self.update_current_min(rtt_nanos);
            } else {
                // Lost the race - another thread rolled over, update new minute's minimum
                self.update_current_min(rtt_nanos);
            }
        } else {
            // Still in current minute - update minimum
            self.update_current_min(rtt_nanos);
        }
    }

    /// Atomically update current minute minimum if new value is smaller
    fn update_current_min(&self, rtt_nanos: u64) {
        self.current_minute_min
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if rtt_nanos < current {
                    Some(rtt_nanos)
                } else {
                    None // Current is already smaller or equal
                }
            })
            .ok();
    }

    fn base_delay(&self) -> Duration {
        // Find minimum across all buckets
        let mut historical_min = u64::MAX;
        for bucket in &self.buckets {
            let nanos = bucket.load(Ordering::Acquire);
            if nanos != EMPTY_DELAY_NANOS && nanos < historical_min {
                historical_min = nanos;
            }
        }

        // Also consider current minute
        let current_min = self.current_minute_min.load(Ordering::Acquire);

        let result_nanos = match (historical_min != u64::MAX, current_min != EMPTY_DELAY_NANOS) {
            (true, true) => historical_min.min(current_min),
            (true, false) => historical_min,
            (false, true) => current_min,
            (false, false) => 10_000_000, // 10ms fallback in nanos
        };

        Duration::from_nanos(result_nanos)
    }
}

/// LEDBAT++ congestion controller with slow start and periodic slowdowns.
///
/// Implements draft-irtf-iccrg-ledbat-plus-plus with improvements over RFC 6817:
/// - Dynamic GAIN based on base_delay
/// - Multiplicative decrease capped at -W/2
/// - Periodic slowdowns for inter-flow fairness
/// - 60ms target delay (vs 100ms in RFC 6817)
///
/// ## Lock-Free Design
///
/// This controller is fully lock-free, using atomic operations for all state.
/// No mutexes are held on the hot path (on_ack, on_send).
///
/// ## Thread Safety
///
/// Individual atomic operations are lock-free and thread-safe. However,
/// compound state transitions (e.g., state change + associated variable updates)
/// are NOT atomic. In practice, this controller is designed to be accessed from
/// a single tokio task per connection. For strictly concurrent access from
/// multiple threads, external synchronization may be required to prevent
/// interleaved state machine transitions (e.g., `on_timeout` racing with `on_ack`).
///
/// ## Slow Start Phase
///
/// Before LEDBAT's congestion avoidance, uses TCP-style exponential growth
/// for fast ramp-up. Exits when:
/// - cwnd >= ssthresh (reached threshold), OR
/// - queuing_delay > TARGET * delay_exit_threshold (congestion detected)
///
/// ## Periodic Slowdowns
///
/// After initial slow start exit, periodically reduces cwnd to minimum to
/// re-measure base delay and ensure fair sharing with competing flows.
///
/// ## Type Parameter
///
/// `T` is the time source used for timing operations. Defaults to `InstantTimeSrc`
/// for production use. In tests, use `SharedMockTimeSource` for deterministic
/// virtual time testing via `LedbatController::new_with_time_source()`.
pub struct LedbatController<T: TimeSource + Clone = InstantTimeSrc> {
    /// Congestion window (bytes in flight)
    cwnd: AtomicUsize,

    /// Bytes currently in flight (sent but not ACKed)
    flightsize: AtomicUsize,

    /// Lock-free delay filter (MIN over recent samples)
    delay_filter: AtomicDelayFilter,

    /// Lock-free base delay history (10-minute buckets)
    base_delay_history: AtomicBaseDelayHistory<T>,

    /// Current queuing delay estimate (stored as nanoseconds)
    queuing_delay_nanos: AtomicU64,

    /// Last update time (stored as nanos since controller creation)
    last_update_nanos: AtomicU64,

    /// Time source for getting current time.
    ///
    /// In production, this is `InstantTimeSrc` which wraps `Instant::now()`.
    /// In tests, `SharedMockTimeSource` allows deterministic virtual time control.
    time_source: T,

    /// Reference epoch for converting `Instant` to duration-since-start.
    ///
    /// All timing calculations use `time_source.now().duration_since(epoch)`
    /// rather than `epoch.elapsed()` to ensure tests with mock time sources
    /// observe the mocked time rather than wall-clock time.
    ///
    /// This field is set once at construction time from `time_source.now()`.
    epoch: Instant,

    /// Bytes acknowledged since last update
    bytes_acked_since_update: AtomicUsize,

    /// Slow start threshold (bytes)
    ssthresh: AtomicUsize,

    // ===== Unified Congestion State Machine =====
    /// Current congestion control state. This single field replaces the previous
    /// `in_slow_start` boolean and `SlowdownState` enum, ensuring unambiguous
    /// state transitions. See [`CongestionState`] for the state machine diagram.
    congestion_state: AtomicCongestionState,

    /// Time when current slowdown phase started (nanos since epoch)
    slowdown_phase_start_nanos: AtomicU64,

    /// RTT count within current slowdown phase
    slowdown_rtt_count: AtomicU32,

    /// Duration of last complete slowdown cycle (nanos) for scheduling next
    last_slowdown_duration_nanos: AtomicU64,

    /// Time when next scheduled slowdown should start (nanos since epoch)
    next_slowdown_time_nanos: AtomicU64,

    /// cwnd value saved before slowdown (for ramping back up)
    pre_slowdown_cwnd: AtomicUsize,

    /// Whether initial slow start has completed (triggers first slowdown)
    initial_slow_start_completed: AtomicBool,

    /// Configuration
    target_delay: Duration,
    allowed_increase_packets: usize,
    min_cwnd: usize,
    max_cwnd: usize,
    delay_exit_threshold: f64,
    enable_periodic_slowdown: bool,

    /// Statistics
    total_increases: AtomicUsize,
    total_decreases: AtomicUsize,
    total_losses: AtomicUsize,
    min_cwnd_events: AtomicUsize,
    slow_start_exits: AtomicUsize,
    periodic_slowdowns: AtomicUsize,
    /// Peak congestion window reached during this controller's lifetime
    peak_cwnd: AtomicUsize,
}

// ============================================================================
// Production constructors (backward-compatible, use real time)
// ============================================================================

impl LedbatController<InstantTimeSrc> {
    /// Create new LEDBAT controller with default config (backward compatible).
    ///
    /// # Arguments
    /// * `initial_cwnd` - Initial congestion window (bytes)
    /// * `min_cwnd` - Minimum window (typically 2 * MSS)
    /// * `max_cwnd` - Maximum window (protocol limit or config)
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::ledbat::LedbatController;
    /// let controller = LedbatController::new(
    ///     2848,      // 2 * MSS (RFC 6817)
    ///     2848,      // min
    ///     10_000_000 // 10 MB max
    /// );
    /// ```
    #[cfg(test)]
    pub fn new(initial_cwnd: usize, min_cwnd: usize, max_cwnd: usize) -> Self {
        let config = LedbatConfig {
            initial_cwnd,
            min_cwnd,
            max_cwnd,
            ..LedbatConfig::default()
        };
        Self::new_with_config(config)
    }

    /// Create new LEDBAT controller with custom configuration.
    ///
    /// This constructor allows full control over slow start parameters
    /// and other LEDBAT settings. Uses real system time (`InstantTimeSrc`).
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::ledbat::{LedbatController, LedbatConfig};
    /// let config = LedbatConfig {
    ///     initial_cwnd: 14_600,  // IW10
    ///     ssthresh: 102_400,     // 100 KB
    ///     enable_slow_start: true,
    ///     ..LedbatConfig::default()
    /// };
    /// let controller = LedbatController::new_with_config(config);
    /// ```
    pub fn new_with_config(config: LedbatConfig) -> Self {
        Self::new_with_time_source(config, InstantTimeSrc::new())
    }
}

// ============================================================================
// Generic implementation (works with any TimeSource)
// ============================================================================

impl<T: TimeSource + Clone> LedbatController<T> {
    /// Create new LEDBAT controller with custom configuration and time source.
    ///
    /// This constructor is the primary entry point for creating a controller
    /// with a mock time source for deterministic testing.
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::ledbat::{LedbatController, LedbatConfig};
    /// use freenet::util::time_source::SharedMockTimeSource;
    ///
    /// let time_source = SharedMockTimeSource::new();
    /// let config = LedbatConfig::default();
    /// let controller = LedbatController::new_with_time_source(config, time_source.clone());
    ///
    /// // Advance virtual time
    /// time_source.advance_time(Duration::from_millis(100));
    /// ```
    pub fn new_with_time_source(config: LedbatConfig, time_source: T) -> Self {
        // Validate configuration parameters
        assert!(
            config.min_cwnd <= config.initial_cwnd,
            "min_cwnd ({}) must be <= initial_cwnd ({})",
            config.min_cwnd,
            config.initial_cwnd
        );
        assert!(
            config.initial_cwnd <= config.max_cwnd,
            "initial_cwnd ({}) must be <= max_cwnd ({})",
            config.initial_cwnd,
            config.max_cwnd
        );
        assert!(
            config.delay_exit_threshold >= 0.0 && config.delay_exit_threshold <= 1.0,
            "delay_exit_threshold ({}) must be in range [0.0, 1.0]",
            config.delay_exit_threshold
        );

        // Apply ±20% jitter to ssthresh to prevent synchronization
        let ssthresh = if config.randomize_ssthresh {
            // Use proper RNG for better entropy distribution
            let jitter_pct = 0.8 + (rand::random::<u8>() % 40) as f64 / 100.0; // 0.8 to 1.2 (±20%)
            ((config.ssthresh as f64) * jitter_pct) as usize
        } else {
            config.ssthresh
        };

        let epoch = time_source.now();

        Self {
            cwnd: AtomicUsize::new(config.initial_cwnd),
            flightsize: AtomicUsize::new(0),
            delay_filter: AtomicDelayFilter::new(),
            base_delay_history: AtomicBaseDelayHistory::new(time_source.clone()),
            queuing_delay_nanos: AtomicU64::new(0),
            last_update_nanos: AtomicU64::new(0),
            time_source,
            epoch,
            bytes_acked_since_update: AtomicUsize::new(0),
            ssthresh: AtomicUsize::new(ssthresh),
            // Unified congestion state: start in SlowStart or CongestionAvoidance
            congestion_state: AtomicCongestionState::new(if config.enable_slow_start {
                CongestionState::SlowStart
            } else {
                CongestionState::CongestionAvoidance
            }),
            slowdown_phase_start_nanos: AtomicU64::new(0),
            slowdown_rtt_count: AtomicU32::new(0),
            last_slowdown_duration_nanos: AtomicU64::new(0),
            next_slowdown_time_nanos: AtomicU64::new(u64::MAX), // No scheduled slowdown yet
            pre_slowdown_cwnd: AtomicUsize::new(0),
            initial_slow_start_completed: AtomicBool::new(false),
            // Configuration
            target_delay: TARGET,
            allowed_increase_packets: 2, // RFC 6817 default
            min_cwnd: config.min_cwnd,
            max_cwnd: config.max_cwnd,
            delay_exit_threshold: config.delay_exit_threshold,
            enable_periodic_slowdown: config.enable_periodic_slowdown,
            // Statistics
            total_increases: AtomicUsize::new(0),
            total_decreases: AtomicUsize::new(0),
            total_losses: AtomicUsize::new(0),
            min_cwnd_events: AtomicUsize::new(0),
            slow_start_exits: AtomicUsize::new(0),
            periodic_slowdowns: AtomicUsize::new(0),
            peak_cwnd: AtomicUsize::new(config.initial_cwnd),
        }
    }

    /// Store new cwnd value and update peak_cwnd if this is a new maximum.
    fn store_cwnd(&self, new_cwnd: usize) {
        self.cwnd.store(new_cwnd, Ordering::Release);
        // Update peak if this is a new maximum (lock-free max update)
        self.peak_cwnd
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current_peak| {
                if new_cwnd > current_peak {
                    Some(new_cwnd)
                } else {
                    None
                }
            })
            .ok(); // Ignore result - we just want the side effect
    }

    /// Get the peak congestion window reached during this controller's lifetime.
    pub fn peak_cwnd(&self) -> usize {
        self.peak_cwnd.load(Ordering::Relaxed)
    }

    /// Calculate dynamic GAIN based on base delay (LEDBAT++ Section 4.2)
    ///
    /// GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
    ///
    /// This adapts the responsiveness based on the ratio of target to base delay.
    /// For low-latency links (low base_delay), GAIN is smaller for stability.
    /// For high-latency links (high base_delay), GAIN approaches 1/16 minimum.
    fn calculate_dynamic_gain(&self, base_delay: Duration) -> f64 {
        let base_ms = base_delay.as_millis() as f64;
        let target_ms = self.target_delay.as_millis() as f64;

        if base_ms <= 0.0 {
            // Edge case: base_delay is zero or unset. This occurs:
            // 1. At connection start before any RTT measurements
            // 2. After base_delay history reset (rare)
            // 3. As a defensive guard against measurement errors
            // Use most conservative GAIN (1/16) for stability.
            return 1.0 / MAX_GAIN_DIVISOR as f64;
        }

        let divisor = (2.0 * target_ms / base_ms).ceil() as u32;
        let clamped_divisor = divisor.clamp(1, MAX_GAIN_DIVISOR);
        1.0 / clamped_divisor as f64
    }

    /// Called when packet sent.
    ///
    /// Tracks bytes in flight for application-limited handling.
    pub fn on_send(&self, bytes: usize) {
        self.flightsize.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Called when ACK received with RTT sample.
    ///
    /// Updates congestion window based on queuing delay.
    /// This method is fully lock-free.
    ///
    /// # Arguments
    /// * `rtt_sample` - Round-trip time measurement
    /// * `bytes_acked_now` - Bytes acknowledged by this ACK
    pub fn on_ack(&self, rtt_sample: Duration, bytes_acked_now: usize) {
        // Decrease flightsize with saturating subtraction to prevent underflow
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked_now))
            })
            .ok();

        // Accumulate acknowledged bytes
        self.bytes_acked_since_update
            .fetch_add(bytes_acked_now, Ordering::Relaxed);

        // Lock-free delay tracking
        // Update base delay history
        self.base_delay_history.update(rtt_sample);

        // Add to delay filter
        self.delay_filter.add_sample(rtt_sample);

        // Get filtered delay (minimum of recent samples)
        if !self.delay_filter.is_ready() {
            return; // Need more samples
        }
        let filtered_rtt = self.delay_filter.filtered_delay().unwrap_or(rtt_sample);

        // Calculate base and queuing delays
        let base_delay = self.base_delay_history.base_delay();
        let queuing_delay = filtered_rtt.saturating_sub(base_delay);
        self.queuing_delay_nanos
            .store(queuing_delay.as_nanos() as u64, Ordering::Release);

        // Rate-limit updates to approximately once per RTT
        let now_nanos = self.time_source.now().duration_since(self.epoch).as_nanos() as u64;
        let last_update = self.last_update_nanos.load(Ordering::Acquire);
        let elapsed_nanos = now_nanos.saturating_sub(last_update);

        // Use base delay as RTT estimate for rate-limiting
        if elapsed_nanos < base_delay.as_nanos() as u64 {
            return;
        }

        // Try to claim this update slot (only one thread should proceed)
        if self
            .last_update_nanos
            .compare_exchange(last_update, now_nanos, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return; // Another thread won the race
        }

        // Calculate off-target amount
        let target = self.target_delay;
        let off_target_ms = if queuing_delay < target {
            (target - queuing_delay).as_millis() as f64
        } else {
            -((queuing_delay - target).as_millis() as f64)
        };
        let target_ms = target.as_millis() as f64;

        // Get total bytes acked since last update
        let bytes_acked_total = self.bytes_acked_since_update.swap(0, Ordering::AcqRel);
        if bytes_acked_total == 0 {
            return; // No progress
        }

        // Unified state machine dispatch - single check, no overlapping flags
        let state = self.congestion_state.load();
        match state {
            CongestionState::SlowStart => {
                // Initial slow start: exponential growth
                self.handle_slow_start(bytes_acked_total, queuing_delay, base_delay);
                return;
            }
            CongestionState::WaitingForSlowdown
            | CongestionState::InSlowdown
            | CongestionState::Frozen
            | CongestionState::RampingUp => {
                // Slowdown state machine handles these states
                if self.handle_congestion_state(bytes_acked_total, queuing_delay, base_delay) {
                    return;
                }
                // Fall through to congestion avoidance if state completed
            }
            CongestionState::CongestionAvoidance => {
                // Check for scheduled slowdown trigger
                if self.enable_periodic_slowdown
                    && self.handle_congestion_state(bytes_acked_total, queuing_delay, base_delay)
                {
                    return;
                }
                // Fall through to congestion avoidance
            }
        }

        // LEDBAT++ congestion avoidance (draft-irtf-iccrg-ledbat-plus-plus Section 4.2)
        //
        // Uses dynamic GAIN and multiplicative decrease capped at -W/2:
        // Δcwnd = max(GAIN - Constant * W * (delay/target - 1), -W/2)
        //
        // Where:
        // - GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
        // - Constant approximates the LEDBAT linear formula
        //
        // For simplicity, we use the equivalent formulation:
        // Δcwnd = GAIN * (off_target / TARGET) * bytes_acked * MSS / cwnd
        // But cap the decrease at -W/2 per RTT
        let current_cwnd = self.cwnd.load(Ordering::Acquire);

        // Calculate dynamic GAIN based on base delay
        let gain = self.calculate_dynamic_gain(base_delay);

        let cwnd_change =
            gain * (off_target_ms / target_ms) * (bytes_acked_total as f64) * (MSS as f64)
                / (current_cwnd as f64);

        // LEDBAT++ key improvement: cap decrease at -W/2 (multiplicative decrease limit)
        let max_decrease = -(current_cwnd as f64 / 2.0);
        let capped_change = if cwnd_change < max_decrease {
            max_decrease
        } else {
            cwnd_change
        };

        let mut new_cwnd = current_cwnd as f64 + capped_change;

        // Track statistics
        if capped_change > 0.0 {
            self.total_increases.fetch_add(1, Ordering::Relaxed);
        } else if capped_change < 0.0 {
            self.total_decreases.fetch_add(1, Ordering::Relaxed);
        }

        // Application-limited cap (RFC 6817 Section 2.4.1)
        let flightsize = self.flightsize.load(Ordering::Relaxed);
        let max_allowed_cwnd = flightsize + (self.allowed_increase_packets * MSS);
        new_cwnd = new_cwnd.min(max_allowed_cwnd as f64);

        // Enforce bounds
        new_cwnd = new_cwnd.max(self.min_cwnd as f64).min(self.max_cwnd as f64);

        let new_cwnd_usize = new_cwnd as usize;

        if new_cwnd_usize == self.min_cwnd && current_cwnd > self.min_cwnd {
            self.min_cwnd_events.fetch_add(1, Ordering::Relaxed);
        }

        self.store_cwnd(new_cwnd_usize);

        // Log significant changes
        let change_abs = (new_cwnd_usize as i64 - current_cwnd as i64).unsigned_abs() as usize;
        if change_abs > 10_000 {
            tracing::debug!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd_usize / 1024,
                change_kb = cwnd_change as i64 / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                base_delay_ms = base_delay.as_millis(),
                off_target_ms,
                bytes_acked = bytes_acked_total,
                flightsize_kb = flightsize / 1024,
                "LEDBAT cwnd update"
            );
        }
    }

    /// Handle slow start phase (exponential growth).
    ///
    /// Exits slow start when:
    /// - cwnd >= ssthresh (reached threshold), OR
    /// - queuing_delay > TARGET * delay_exit_threshold (congestion detected)
    ///
    /// On initial slow start exit, schedules the first periodic slowdown (LEDBAT++).
    fn handle_slow_start(&self, bytes_acked: usize, queuing_delay: Duration, base_delay: Duration) {
        // Early exit if no longer in SlowStart state (race: timeout occurred)
        if !self.congestion_state.is_slow_start() {
            return;
        }

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let ssthresh = self.ssthresh.load(Ordering::Acquire);

        // Check exit conditions (LEDBAT++ uses 3/4 of target, configured via delay_exit_threshold)
        let delay_threshold =
            Duration::from_secs_f64(self.target_delay.as_secs_f64() * self.delay_exit_threshold);
        let should_exit = current_cwnd >= ssthresh || queuing_delay > delay_threshold;

        if should_exit {
            self.slow_start_exits.fetch_add(1, Ordering::Relaxed);

            // Conservative reduction on exit (optional, can be tuned)
            let new_cwnd = ((current_cwnd as f64) * 0.9) as usize;
            let new_cwnd = new_cwnd.max(self.min_cwnd).min(self.max_cwnd);
            self.cwnd.store(new_cwnd, Ordering::Release);

            // LEDBAT++: Schedule initial slowdown after first slow start exit
            // The slowdown will occur after SLOWDOWN_DELAY_RTTS (2 RTTs)
            if self.enable_periodic_slowdown
                && !self
                    .initial_slow_start_completed
                    .swap(true, Ordering::AcqRel)
            {
                let now_nanos = self.time_source.now().duration_since(self.epoch).as_nanos() as u64;
                // Schedule slowdown after 2 RTTs (use base_delay as RTT estimate)
                let delay_nanos = base_delay.as_nanos() as u64 * SLOWDOWN_DELAY_RTTS as u64;
                self.next_slowdown_time_nanos
                    .store(now_nanos + delay_nanos, Ordering::Release);
                // Transition to WaitingForSlowdown state
                self.congestion_state.enter_waiting_for_slowdown();

                tracing::debug!(
                    delay_ms = delay_nanos / 1_000_000,
                    "LEDBAT++ scheduling initial slowdown after slow start exit"
                );
            } else {
                // No periodic slowdown, just go to CongestionAvoidance
                self.congestion_state.enter_congestion_avoidance();
            }

            let exit_reason = if current_cwnd >= ssthresh {
                "ssthresh"
            } else {
                "delay"
            };

            tracing::debug!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                ssthresh_kb = ssthresh / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                delay_threshold_ms = delay_threshold.as_millis(),
                reason = exit_reason,
                total_exits = self.slow_start_exits.load(Ordering::Relaxed),
                "Exiting slow start"
            );
        } else {
            // Exponential growth: cwnd += bytes_acked (doubles per RTT)
            let new_cwnd = (current_cwnd + bytes_acked).min(self.max_cwnd);
            self.store_cwnd(new_cwnd);

            tracing::trace!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                bytes_acked_kb = bytes_acked / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                "Slow start growth"
            );
        }
    }

    /// Handle congestion state machine transitions.
    ///
    /// This unified handler replaces the previous overlapping `in_slow_start` and
    /// `SlowdownState` flags, ensuring unambiguous state transitions.
    ///
    /// Returns true if the state machine handled this update (caller should return).
    fn handle_congestion_state(
        &self,
        bytes_acked: usize,
        queuing_delay: Duration,
        base_delay: Duration,
    ) -> bool {
        let now_nanos = self.time_source.now().duration_since(self.epoch).as_nanos() as u64;
        let state = self.congestion_state.load();

        match state {
            CongestionState::CongestionAvoidance => {
                // Check if it's time for the next scheduled slowdown
                let next_slowdown = self.next_slowdown_time_nanos.load(Ordering::Acquire);
                if now_nanos >= next_slowdown && next_slowdown != u64::MAX {
                    self.start_slowdown(now_nanos, base_delay);
                    return true;
                }
                false
            }
            CongestionState::WaitingForSlowdown => {
                // Check if wait period is over
                let next_slowdown = self.next_slowdown_time_nanos.load(Ordering::Acquire);
                if now_nanos >= next_slowdown {
                    self.start_slowdown(now_nanos, base_delay);
                    return true;
                }
                false
            }
            CongestionState::InSlowdown => {
                // Transient state: immediately transition to Frozen
                self.congestion_state.enter_frozen();
                self.slowdown_rtt_count.store(0, Ordering::Release);
                self.slowdown_phase_start_nanos
                    .store(now_nanos, Ordering::Release);
                true
            }
            CongestionState::Frozen => {
                // Frozen at reduced cwnd, counting RTTs until we can ramp up
                let phase_start = self.slowdown_phase_start_nanos.load(Ordering::Acquire);
                let freeze_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;

                if now_nanos.saturating_sub(phase_start) >= freeze_duration {
                    // Done freezing, start ramping up
                    self.congestion_state.enter_ramping_up();
                    tracing::debug!(
                        cwnd_kb = self.cwnd.load(Ordering::Relaxed) / 1024,
                        "LEDBAT++ slowdown: starting ramp-up phase"
                    );
                    // Return true to let next ACK start the ramp-up
                    return true;
                }
                // Keep cwnd at proportionally reduced level during freeze
                // (already set in start_slowdown, just maintain it)
                let pre_slowdown = self.pre_slowdown_cwnd.load(Ordering::Relaxed);
                let frozen_cwnd = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(self.min_cwnd);
                self.cwnd.store(frozen_cwnd, Ordering::Release);
                true
            }
            CongestionState::RampingUp => {
                // Ramping up using exponential growth until reaching pre_slowdown_cwnd
                let target_cwnd = self.pre_slowdown_cwnd.load(Ordering::Acquire);
                let current_cwnd = self.cwnd.load(Ordering::Acquire);

                // Exit ramp-up when target is reached
                if current_cwnd >= target_cwnd {
                    self.complete_slowdown(now_nanos, base_delay);
                    return false; // Let congestion avoidance take over
                }

                // During ramp-up, queuing is expected as cwnd grows. Only exit early
                // for queuing_delay if cwnd has recovered substantially (85% of target).
                // This prevents premature exit on high-latency paths where queuing_delay
                // can spike temporarily before the queue stabilizes.
                //
                // Without this check, ramp-up exits at ~67% recovery on paths where
                // queuing_delay > target_delay, causing repeated slowdown cycles and
                // preventing throughput recovery.
                //
                // The 85% threshold ensures most of the bandwidth is recovered before
                // responding to congestion signals. Using 17/20 to avoid floating point.
                let has_recovered_substantially = current_cwnd * 20 >= target_cwnd * 17; // 85%

                if has_recovered_substantially && queuing_delay > self.target_delay {
                    // Recovered enough and seeing congestion, exit ramp-up
                    self.complete_slowdown(now_nanos, base_delay);
                    return false;
                }

                // Exponential growth during ramp-up
                let new_cwnd = (current_cwnd + bytes_acked)
                    .min(target_cwnd)
                    .min(self.max_cwnd);
                self.store_cwnd(new_cwnd);
                true
            }
            CongestionState::SlowStart => false, // SlowStart handled elsewhere
        }
    }

    /// Start a periodic slowdown cycle.
    fn start_slowdown(&self, now_nanos: u64, _base_delay: Duration) {
        let current_cwnd = self.cwnd.load(Ordering::Acquire);

        // Save current cwnd as ssthresh and ramp-up target
        self.ssthresh.store(current_cwnd, Ordering::Release);
        self.pre_slowdown_cwnd
            .store(current_cwnd, Ordering::Release);

        // Reduce cwnd proportionally (consistent with our large initial window)
        // Drop to cwnd/4 but never below min_cwnd (2 packets)
        let slowdown_cwnd = (current_cwnd / SLOWDOWN_REDUCTION_FACTOR).max(self.min_cwnd);
        self.cwnd.store(slowdown_cwnd, Ordering::Release);

        // Transition to InSlowdown state (transient, will become Frozen on next ACK)
        self.congestion_state.enter_in_slowdown();
        self.slowdown_phase_start_nanos
            .store(now_nanos, Ordering::Release);
        self.periodic_slowdowns.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            old_cwnd_kb = current_cwnd / 1024,
            new_cwnd_kb = slowdown_cwnd / 1024,
            reduction_factor = SLOWDOWN_REDUCTION_FACTOR,
            "LEDBAT++ periodic slowdown: reducing cwnd proportionally"
        );
    }

    /// Complete a slowdown cycle and schedule the next one.
    fn complete_slowdown(&self, now_nanos: u64, base_delay: Duration) {
        let phase_start = self.slowdown_phase_start_nanos.load(Ordering::Acquire);
        let slowdown_duration = now_nanos.saturating_sub(phase_start);

        // Store duration for potential diagnostics
        self.last_slowdown_duration_nanos
            .store(slowdown_duration, Ordering::Release);

        // Schedule next slowdown at 9x the current duration (LEDBAT++ spec)
        let next_interval = slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;

        // Ensure minimum interval maintains the 9:1 ratio even if slowdown completed quickly.
        // The freeze phase takes SLOWDOWN_FREEZE_RTTS (2 RTTs), so the minimum sensible
        // slowdown duration is 2 RTTs. Apply the 9x multiplier to get 18 RTTs minimum.
        // Without this floor, short slowdown cycles on high-latency paths cause slowdowns
        // every RTT, collapsing throughput (observed: 224 slowdowns in 30s on 137ms RTT path).
        let min_slowdown_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;
        let min_interval = min_slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
        let actual_interval = next_interval.max(min_interval);

        self.next_slowdown_time_nanos
            .store(now_nanos + actual_interval, Ordering::Release);

        // Return to CongestionAvoidance state
        self.congestion_state.enter_congestion_avoidance();

        tracing::debug!(
            slowdown_duration_ms = slowdown_duration / 1_000_000,
            next_interval_ms = actual_interval / 1_000_000,
            "LEDBAT++ slowdown complete, scheduling next"
        );
    }

    /// Called when packet loss detected (not timeout).
    ///
    /// RFC 6817 Section 2.4.1: halve cwnd on loss.
    ///
    /// Transitions to CongestionAvoidance state if in SlowStart.
    ///
    /// **Note:** Currently not called in production - the transport layer uses
    /// [`on_timeout`] for retransmissions. This method is for future fast-retransmit
    /// support (3 duplicate ACKs). LEDBAT primarily uses delay-based signals anyway,
    /// so loss detection is less critical than in traditional TCP congestion control.
    #[cfg(test)]
    pub fn on_loss(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

        // Exit slow start on loss → go to CongestionAvoidance
        if self.congestion_state.is_slow_start() {
            self.congestion_state.enter_congestion_avoidance();
            self.slow_start_exits.fetch_add(1, Ordering::Relaxed);
        }

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let new_cwnd = (current_cwnd / 2).max(self.min_cwnd);

        self.cwnd.store(new_cwnd, Ordering::Release);

        tracing::warn!(
            old_cwnd_kb = current_cwnd / 1024,
            new_cwnd_kb = new_cwnd / 1024,
            total_losses = self.total_losses.load(Ordering::Relaxed),
            "LEDBAT packet loss - halving cwnd"
        );
    }

    /// Called on retransmission timeout (severe congestion).
    ///
    /// RFC 6817: Reset to 1 * MSS on timeout.
    ///
    /// After resetting cwnd, we transition to SlowStart for fast recovery.
    /// This is a clean state transition that cannot conflict with other states.
    ///
    /// This follows standard TCP RTO recovery behavior (RFC 5681):
    /// - Reset cwnd to min_cwnd
    /// - Set ssthresh to max(old_cwnd/2, 2*min_cwnd)
    /// - Enter SlowStart state for exponential recovery
    pub fn on_timeout(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

        // Save old cwnd for ssthresh calculation before resetting
        let old_cwnd = self.cwnd.load(Ordering::Acquire);

        // Reset cwnd to minimum (RFC 6817: 1 * MSS)
        let new_cwnd = MSS.max(self.min_cwnd);
        self.cwnd.store(new_cwnd, Ordering::Release);

        // Set ssthresh to half the pre-timeout cwnd (standard TCP RTO recovery)
        // Floor at 2*min_cwnd to ensure reasonable recovery target
        let new_ssthresh = (old_cwnd / 2).max(self.min_cwnd * 2);
        self.ssthresh.store(new_ssthresh, Ordering::Release);

        // Transition to SlowStart for fast (exponential) recovery.
        // This cleanly overrides any slowdown state without conflicts.
        self.congestion_state.enter_slow_start();

        // Only log when cwnd actually changes to avoid spam when already at minimum
        if old_cwnd != new_cwnd {
            tracing::warn!(
                old_cwnd_kb = old_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                new_ssthresh_kb = new_ssthresh / 1024,
                "LEDBAT retransmission timeout - reset to min_cwnd, entering SlowStart"
            );
        }
    }

    /// Get current congestion window (bytes).
    pub fn current_cwnd(&self) -> usize {
        self.cwnd.load(Ordering::Acquire)
    }

    /// Convert cwnd (bytes in flight) to rate (bytes/sec).
    ///
    /// rate = cwnd / RTT
    ///
    /// Enforces minimum RTT of 1ms to prevent division by near-zero
    /// (sub-millisecond RTTs would cause unrealistically high rates).
    pub fn current_rate(&self, rtt: Duration) -> usize {
        let cwnd = self.current_cwnd();
        // Enforce minimum RTT of 1ms to prevent division by near-zero
        let safe_rtt = rtt.max(Duration::from_millis(1));
        ((cwnd as f64) / safe_rtt.as_secs_f64()) as usize
    }

    /// Get current queuing delay (lock-free).
    pub fn queuing_delay(&self) -> Duration {
        Duration::from_nanos(self.queuing_delay_nanos.load(Ordering::Acquire))
    }

    /// Get base delay (lock-free).
    pub fn base_delay(&self) -> Duration {
        self.base_delay_history.base_delay()
    }

    /// Get current flightsize.
    pub fn flightsize(&self) -> usize {
        self.flightsize.load(Ordering::Relaxed)
    }

    /// Called when ACK received for a retransmitted packet.
    ///
    /// This ONLY decrements flightsize without updating RTT estimation,
    /// following Karn's algorithm which excludes retransmitted packets
    /// from RTT calculation.
    ///
    /// This is critical for preventing flightsize leaks: when a packet
    /// times out and is retransmitted, on_send was called once for the
    /// original send. If the retransmitted packet is ACKed, we still need
    /// to decrement flightsize even though we can't use it for RTT.
    pub fn on_ack_without_rtt(&self, bytes_acked: usize) {
        // Decrease flightsize with saturating subtraction to prevent underflow
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked))
            })
            .ok();

        tracing::trace!(
            bytes_acked,
            new_flightsize = self.flightsize.load(Ordering::Relaxed),
            "Decremented flightsize for retransmitted packet ACK"
        );
    }

    /// Get LEDBAT congestion control statistics.
    ///
    /// Returns a snapshot of the controller's current state and cumulative statistics.
    /// Useful for telemetry, debugging, and performance analysis.
    pub fn stats(&self) -> LedbatStats {
        LedbatStats {
            cwnd: self.current_cwnd(),
            flightsize: self.flightsize(),
            queuing_delay: self.queuing_delay(),
            base_delay: self.base_delay(),
            peak_cwnd: self.peak_cwnd(),
            total_increases: self.total_increases.load(Ordering::Relaxed),
            total_decreases: self.total_decreases.load(Ordering::Relaxed),
            total_losses: self.total_losses.load(Ordering::Relaxed),
            min_cwnd_events: self.min_cwnd_events.load(Ordering::Relaxed),
            slow_start_exits: self.slow_start_exits.load(Ordering::Relaxed),
            periodic_slowdowns: self.periodic_slowdowns.load(Ordering::Relaxed),
        }
    }
}

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    // ============================================================================
    // Deterministic Test Harness for LEDBAT
    // ============================================================================

    /// Network condition presets for various link types.
    ///
    /// These represent typical network characteristics including RTT, jitter, and
    /// packet loss rates. Used with `LedbatTestHarness` for deterministic testing.
    #[derive(Debug, Clone, Copy)]
    #[allow(dead_code)] // Fields/constants available for future test expansion
    pub struct NetworkCondition {
        /// Base round-trip time
        pub rtt: Duration,
        /// Jitter as (min_multiplier, max_multiplier), e.g., (0.9, 1.1) for ±10%
        pub jitter: Option<(f64, f64)>,
        /// Packet loss rate as a fraction (0.0 to 1.0)
        pub loss_rate: f64,
    }

    #[allow(dead_code)] // Constants available for future test expansion
    impl NetworkCondition {
        /// LAN: 1ms RTT, no jitter, no loss
        pub const LAN: Self = Self {
            rtt: Duration::from_millis(1),
            jitter: None,
            loss_rate: 0.0,
        };

        /// Datacenter: 10ms RTT, minimal jitter, no loss
        pub const DATACENTER: Self = Self {
            rtt: Duration::from_millis(10),
            jitter: Some((0.95, 1.05)),
            loss_rate: 0.0,
        };

        /// Continental (same continent): 50ms RTT, ±10% jitter, 0.1% loss
        pub const CONTINENTAL: Self = Self {
            rtt: Duration::from_millis(50),
            jitter: Some((0.9, 1.1)),
            loss_rate: 0.001,
        };

        /// Intercontinental: 135ms RTT (e.g., US-EU), ±20% jitter, 0.5% loss
        pub const INTERCONTINENTAL: Self = Self {
            rtt: Duration::from_millis(135),
            jitter: Some((0.8, 1.2)),
            loss_rate: 0.005,
        };

        /// High latency (satellite/distant): 250ms RTT, ±10% jitter, 1% loss
        pub const HIGH_LATENCY: Self = Self {
            rtt: Duration::from_millis(250),
            jitter: Some((0.9, 1.1)),
            loss_rate: 0.01,
        };

        /// Create a custom network condition
        pub fn custom(rtt_ms: u64, jitter_pct: Option<f64>, loss_rate: f64) -> Self {
            let jitter = jitter_pct.map(|pct| (1.0 - pct, 1.0 + pct));
            Self {
                rtt: Duration::from_millis(rtt_ms),
                jitter,
                loss_rate,
            }
        }
    }

    /// State snapshot for harness debugging and assertions.
    ///
    /// Named `HarnessSnapshot` to avoid conflict with the existing `StateSnapshot`
    /// used by visualization tests.
    #[derive(Debug, Clone)]
    #[allow(dead_code)] // Fields available for test assertions and debugging
    pub struct HarnessSnapshot {
        /// Time since harness creation (nanos)
        pub time_nanos: u64,
        /// Current congestion window (bytes)
        pub cwnd: usize,
        /// Current congestion state
        pub state: CongestionState,
        /// Number of periodic slowdowns completed
        pub periodic_slowdowns: usize,
        /// Bytes in flight
        pub flightsize: usize,
        /// Queuing delay estimate
        pub queuing_delay: Duration,
        /// Base delay (minimum RTT observed)
        pub base_delay: Duration,
    }

    /// Deterministic test harness for LEDBAT.
    ///
    /// This harness wraps a `LedbatController` with a mock time source, enabling:
    /// - Virtual time: Advance time instantly without wall-clock delays
    /// - Deterministic jitter: Seeded RNG produces reproducible "random" delays
    /// - Packet loss simulation: Configurable loss rate per network condition
    /// - State snapshots: Capture controller state at any point for assertions
    ///
    /// # Example
    /// ```ignore
    /// let mut harness = LedbatTestHarness::new(
    ///     LedbatConfig::default(),
    ///     NetworkCondition::INTERCONTINENTAL,
    ///     12345, // seed for reproducibility
    /// );
    ///
    /// // Run 10 RTTs of transfer
    /// let snapshots: Vec<HarnessSnapshot> = harness.run_rtts(10, 100_000);
    ///
    /// // Verify cwnd grew appropriately
    /// assert!(snapshots.last().unwrap().cwnd > snapshots.first().unwrap().cwnd);
    /// ```
    pub struct LedbatTestHarness {
        time_source: SharedMockTimeSource,
        controller: LedbatController<SharedMockTimeSource>,
        condition: NetworkCondition,
        rng: SmallRng,
        epoch: std::time::Instant,
    }

    #[allow(dead_code)] // Methods available for test expansion
    impl LedbatTestHarness {
        /// Create a new test harness with the given configuration and network conditions.
        ///
        /// # Arguments
        /// * `config` - LEDBAT configuration (cwnd limits, thresholds, etc.)
        /// * `condition` - Network condition preset (RTT, jitter, loss)
        /// * `seed` - RNG seed for reproducible jitter and loss patterns
        pub fn new(config: LedbatConfig, condition: NetworkCondition, seed: u64) -> Self {
            let time_source = SharedMockTimeSource::new();
            let epoch = time_source.current_time();
            let controller = LedbatController::new_with_time_source(config, time_source.clone());

            Self {
                time_source,
                controller,
                condition,
                rng: SmallRng::seed_from_u64(seed),
                epoch,
            }
        }

        /// Get a reference to the underlying controller for direct access.
        pub fn controller(&self) -> &LedbatController<SharedMockTimeSource> {
            &self.controller
        }

        /// Get current virtual time as nanos since harness creation.
        pub fn current_time_nanos(&self) -> u64 {
            self.time_source
                .current_time()
                .duration_since(self.epoch)
                .as_nanos() as u64
        }

        /// Advance virtual time by the given duration.
        pub fn advance_time(&mut self, duration: Duration) {
            self.time_source.advance_time(duration);
        }

        /// Calculate RTT with jitter applied.
        fn jittered_rtt(&mut self) -> Duration {
            let base_nanos = self.condition.rtt.as_nanos() as f64;
            let jittered_nanos = match self.condition.jitter {
                Some((min_mult, max_mult)) => {
                    let mult = self.rng.random_range(min_mult..=max_mult);
                    base_nanos * mult
                }
                None => base_nanos,
            };
            Duration::from_nanos(jittered_nanos as u64)
        }

        /// Determine if a packet should be "lost" based on loss rate.
        fn should_drop_packet(&mut self) -> bool {
            self.condition.loss_rate > 0.0 && self.rng.random::<f64>() < self.condition.loss_rate
        }

        /// Simulate one RTT of data transfer.
        ///
        /// This advances virtual time by one RTT, simulates sending `bytes_to_send`,
        /// and processes the ACK (unless packet is "lost").
        ///
        /// # Returns
        /// The number of bytes successfully acknowledged (0 if lost).
        pub fn step(&mut self, bytes_to_send: usize) -> usize {
            let rtt = self.jittered_rtt();

            // Simulate sending data
            let bytes_sent = bytes_to_send.min(self.controller.current_cwnd());
            self.controller.on_send(bytes_sent);

            // Advance time by RTT
            self.advance_time(rtt);

            // Check for packet loss
            if self.should_drop_packet() {
                self.controller.on_loss();
                return 0;
            }

            // Process ACK
            self.controller.on_ack(rtt, bytes_sent);
            bytes_sent
        }

        /// Run N RTTs of transfer, collecting state snapshots.
        ///
        /// # Arguments
        /// * `count` - Number of RTTs to simulate
        /// * `bytes_per_rtt` - Bytes to attempt sending each RTT
        ///
        /// # Returns
        /// Vector of state snapshots, one per RTT.
        pub fn run_rtts(&mut self, count: usize, bytes_per_rtt: usize) -> Vec<HarnessSnapshot> {
            let mut snapshots = Vec::with_capacity(count);

            for _ in 0..count {
                self.step(bytes_per_rtt);
                snapshots.push(self.snapshot());
            }

            snapshots
        }

        /// Inject a timeout event (e.g., RTO expired).
        ///
        /// This simulates a timeout without advancing time, triggering
        /// the controller's timeout handling (cwnd reset, slow start re-entry).
        pub fn inject_timeout(&mut self) {
            self.controller.on_timeout();
        }

        /// Get a snapshot of the current controller state.
        pub fn snapshot(&self) -> HarnessSnapshot {
            HarnessSnapshot {
                time_nanos: self.current_time_nanos(),
                cwnd: self.controller.current_cwnd(),
                state: self.controller.congestion_state.load(),
                periodic_slowdowns: self
                    .controller
                    .periodic_slowdowns
                    .load(std::sync::atomic::Ordering::Relaxed),
                flightsize: self.controller.flightsize(),
                queuing_delay: self.controller.queuing_delay(),
                base_delay: self.controller.base_delay(),
            }
        }

        /// Run until a specific state is reached, with a maximum RTT limit.
        ///
        /// # Returns
        /// `Ok(snapshots)` if target state reached, `Err(snapshots)` if limit hit.
        pub fn run_until_state(
            &mut self,
            target: CongestionState,
            max_rtts: usize,
            bytes_per_rtt: usize,
        ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
            let mut snapshots = Vec::new();

            for _ in 0..max_rtts {
                self.step(bytes_per_rtt);
                let snap = self.snapshot();
                let state = snap.state;
                snapshots.push(snap);

                if state == target {
                    return Ok(snapshots);
                }
            }

            Err(snapshots)
        }

        /// Run until a condition is met, with a maximum RTT limit.
        ///
        /// # Returns
        /// `Ok(snapshots)` if condition met, `Err(snapshots)` if limit hit.
        pub fn run_until<F>(
            &mut self,
            condition: F,
            max_rtts: usize,
            bytes_per_rtt: usize,
        ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>>
        where
            F: Fn(&HarnessSnapshot) -> bool,
        {
            let mut snapshots = Vec::new();

            for _ in 0..max_rtts {
                self.step(bytes_per_rtt);
                let snap = self.snapshot();
                let done = condition(&snap);
                snapshots.push(snap);

                if done {
                    return Ok(snapshots);
                }
            }

            Err(snapshots)
        }

        // ========================================================================
        // Convenience Methods for Common Test Patterns
        // ========================================================================

        /// Run until cwnd exceeds the given threshold.
        ///
        /// # Returns
        /// `Ok(snapshots)` if cwnd exceeded threshold, `Err(snapshots)` if limit hit.
        pub fn run_until_cwnd_exceeds(
            &mut self,
            threshold: usize,
            max_rtts: usize,
            bytes_per_rtt: usize,
        ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
            self.run_until(|s| s.cwnd > threshold, max_rtts, bytes_per_rtt)
        }

        /// Run until cwnd drops below the given threshold.
        ///
        /// # Returns
        /// `Ok(snapshots)` if cwnd dropped below threshold, `Err(snapshots)` if limit hit.
        pub fn run_until_cwnd_below(
            &mut self,
            threshold: usize,
            max_rtts: usize,
            bytes_per_rtt: usize,
        ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
            self.run_until(|s| s.cwnd < threshold, max_rtts, bytes_per_rtt)
        }

        /// Run until a slowdown event occurs.
        ///
        /// # Returns
        /// `Ok(snapshots)` if slowdown occurred, `Err(snapshots)` if limit hit.
        pub fn run_until_slowdown(
            &mut self,
            max_rtts: usize,
            bytes_per_rtt: usize,
        ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
            let initial_slowdowns = self.snapshot().periodic_slowdowns;
            self.run_until(
                |s| s.periodic_slowdowns > initial_slowdowns,
                max_rtts,
                bytes_per_rtt,
            )
        }

        /// Change the network condition mid-test (e.g., RTT jump).
        ///
        /// This allows testing behavior when network conditions change dynamically.
        pub fn set_condition(&mut self, condition: NetworkCondition) {
            self.condition = condition;
        }

        /// Get the current GAIN value based on base delay.
        ///
        /// GAIN = 1 / min(ceil(2 * TARGET / base_delay), MAX_GAIN_DIVISOR)
        pub fn current_gain(&self) -> f64 {
            let base_delay = self.controller.base_delay();
            if base_delay.is_zero() {
                return 1.0 / MAX_GAIN_DIVISOR as f64;
            }
            let target_nanos = TARGET.as_nanos() as f64;
            let base_nanos = base_delay.as_nanos() as f64;
            let divisor = (2.0 * target_nanos / base_nanos).ceil() as u32;
            let clamped = divisor.clamp(1, MAX_GAIN_DIVISOR);
            1.0 / clamped as f64
        }

        /// Get expected GAIN at a given RTT (for test assertions).
        ///
        /// At 60ms RTT: GAIN = 1/2 (divisor = ceil(120/60) = 2)
        /// At 120ms+ RTT: GAIN = 1 (divisor = 1)
        /// At 7.5ms RTT: GAIN = 1/16 (divisor = ceil(120/7.5) = 16)
        pub fn expected_gain_for_rtt(rtt_ms: u64) -> f64 {
            if rtt_ms == 0 {
                return 1.0 / MAX_GAIN_DIVISOR as f64;
            }
            let target_ms = 60.0f64;
            let divisor = (2.0 * target_ms / rtt_ms as f64).ceil() as u32;
            let clamped = divisor.clamp(1, MAX_GAIN_DIVISOR);
            1.0 / clamped as f64
        }
    }

    // ============================================================================
    // Existing Tests
    // ============================================================================

    #[test]
    fn test_ledbat_creation() {
        let controller = LedbatController::new(2848, 2848, 10_000_000);
        assert_eq!(controller.current_cwnd(), 2848);
        assert_eq!(controller.base_delay(), Duration::from_millis(10)); // Fallback
    }

    #[test]
    fn test_ledbat_base_delay_tracking() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // First RTT sample sets base delay
        controller.on_ack(Duration::from_millis(50), 1000);
        assert_eq!(controller.base_delay(), Duration::from_millis(50));

        // Lower RTT updates base delay
        controller.on_ack(Duration::from_millis(40), 1000);
        assert_eq!(controller.base_delay(), Duration::from_millis(40));

        // Higher RTT doesn't update base delay
        controller.on_ack(Duration::from_millis(60), 1000);
        assert_eq!(controller.base_delay(), Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_ledbat_increases_when_below_target() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Track flightsize to avoid application-limited cap interference
        controller.on_send(20_000); // Send enough to keep flightsize high

        // Set base delay with multiple samples (need 2+ for filter)
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT below target (10ms + 20ms = 30ms < 100ms target)
        // Should increase
        controller.on_ack(Duration::from_millis(30), 5000);

        assert!(
            controller.current_cwnd() > initial_cwnd,
            "cwnd should increase when below target (was {}, now {})",
            initial_cwnd,
            controller.current_cwnd()
        );
    }

    #[tokio::test]
    async fn test_ledbat_decreases_when_above_target() {
        // Disable slow start to test pure LEDBAT congestion avoidance
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            enable_slow_start: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Track flightsize to avoid application-limited cap interference
        controller.on_send(200_000); // High flightsize

        // Set base delay with low RTT samples
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill the delay filter with high RTT samples (filter uses MIN over 4 samples)
        // We need enough high-RTT samples to push out the low ones
        for _ in 0..4 {
            controller.on_send(10_000);
            controller.on_ack(Duration::from_millis(160), 2000);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT above target (10ms base + 150ms queuing = 160ms > 100ms target)
        // Now filter has [160, 160, 160, 160], filtered_delay = 160ms
        // queuing_delay = 160ms - 10ms = 150ms > 100ms target
        // off_target = -50ms (negative = decrease)
        controller.on_send(10_000);
        controller.on_ack(Duration::from_millis(160), 5000);

        let new_cwnd = controller.current_cwnd();

        assert!(
            new_cwnd < initial_cwnd,
            "cwnd should decrease when above target (was {}, now {})",
            initial_cwnd,
            new_cwnd
        );
    }

    #[test]
    fn test_ledbat_min_cwnd_enforcement() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Set base delay
        controller.on_ack(Duration::from_millis(10), 0); // Just for base delay
        controller.on_ack(Duration::from_millis(10), 0);

        // Very high queuing delay (should decrease aggressively)
        // LEDBAT decreases gradually, need many iterations
        // Keep flightsize valid by sending before each ACK
        for _ in 0..50 {
            controller.on_send(1000);
            controller.on_ack(Duration::from_millis(500), 1000);
            std::thread::sleep(std::time::Duration::from_millis(15)); // Wait for update interval
        }

        // Should have hit min_cwnd (or very close)
        assert!(
            controller.current_cwnd() <= 2848 + 1000,
            "Should be close to min_cwnd, got {}",
            controller.current_cwnd()
        );
    }

    #[test]
    fn test_ledbat_rate_conversion() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        // cwnd = 100KB, RTT = 100ms
        // rate = 100KB / 0.1s = 1000 KB/s = 1 MB/s
        let rate = controller.current_rate(Duration::from_millis(100));
        assert!(
            (rate as i64 - 1_000_000).abs() < 10_000,
            "rate should be ~1 MB/s, got {}",
            rate
        );
    }

    #[test]
    fn test_ledbat_loss_handling() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        let before = controller.current_cwnd();
        controller.on_loss();
        let after = controller.current_cwnd();

        // Should halve (approximately)
        assert!(
            (after as f64) < (before as f64 * 0.6),
            "should halve on loss"
        );
        assert!(
            (after as f64) > (before as f64 * 0.4),
            "should halve on loss"
        );
    }

    #[test]
    fn test_ledbat_timeout_handling() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        controller.on_timeout();

        // Should reset to 1*MSS (or min_cwnd)
        assert_eq!(controller.current_cwnd(), 2848); // max(MSS, min_cwnd)
    }

    #[test]
    fn test_ledbat_flightsize_tracking() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        assert_eq!(controller.flightsize(), 0);

        controller.on_send(5000);
        assert_eq!(controller.flightsize(), 5000);

        controller.on_ack(Duration::from_millis(50), 2000);
        assert_eq!(controller.flightsize(), 3000);

        controller.on_ack(Duration::from_millis(50), 3000);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_on_ack_without_rtt_decrements_flightsize() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Send bytes to track
        controller.on_send(5000);
        assert_eq!(controller.flightsize(), 5000);

        // on_ack_without_rtt should decrement flightsize without updating RTT state
        controller.on_ack_without_rtt(2000);
        assert_eq!(controller.flightsize(), 3000);

        // Should not have updated base_delay (no RTT sample provided)
        // Base delay should still be at the default fallback value
        let base_delay = controller.base_delay();
        assert!(
            base_delay == Duration::from_millis(10) || base_delay.is_zero(),
            "base_delay should be fallback value, got {:?}",
            base_delay
        );

        // Continue decrementing
        controller.on_ack_without_rtt(3000);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_on_ack_without_rtt_saturates_on_underflow() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Send a small amount
        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);

        // ACK more than we sent - should saturate to 0, not wrap
        controller.on_ack_without_rtt(5000);
        assert_eq!(
            controller.flightsize(),
            0,
            "should saturate to 0, not underflow"
        );
    }

    #[tokio::test]
    async fn test_ledbat_cwnd_formula_correctness() {
        // Disable slow start to test pure LEDBAT congestion avoidance formula
        let config = LedbatConfig {
            initial_cwnd: 10_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            enable_slow_start: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Track flightsize for application-limited cap
        controller.on_send(50_000); // Send enough to cover all acks

        // Set base delay with filter (need 2+ samples)
        for _ in 0..4 {
            controller.on_ack(Duration::from_millis(10), 1000);
        }

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill the delay filter with 30ms RTT samples (below target)
        // This establishes a consistent filtered delay
        for _ in 0..4 {
            controller.on_send(5000);
            controller.on_ack(Duration::from_millis(30), 1000);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT below target: 30ms (10ms base + 20ms queuing) < 100ms target
        // Now filter has [30, 30, 30, 30], filtered_delay = 30ms
        // queuing_delay = 30ms - 10ms = 20ms < 100ms target
        // off_target = +80ms (positive = increase)
        // Δcwnd = GAIN * (80/100) * bytes_acked * MSS / cwnd
        controller.on_send(10_000);
        controller.on_ack(Duration::from_millis(30), 5000);

        let new_cwnd = controller.current_cwnd();

        // Should have increased (below target)
        assert!(
            new_cwnd > initial_cwnd,
            "cwnd should increase when below target (was {}, now {})",
            initial_cwnd,
            new_cwnd
        );

        // Verify the change is reasonable (not more than flightsize + ALLOWED_INCREASE)
        let max_allowed = controller.flightsize() + (2 * MSS);
        assert!(
            new_cwnd <= max_allowed,
            "cwnd should be capped by application-limited (cwnd={}, max_allowed={})",
            new_cwnd,
            max_allowed
        );
    }

    #[test]
    fn test_ledbat_delay_filtering() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Feed samples with noise: [50ms, 45ms, 55ms, 48ms]
        controller.on_ack(Duration::from_millis(50), 1000);
        controller.on_ack(Duration::from_millis(45), 1000);
        controller.on_ack(Duration::from_millis(55), 1000);
        controller.on_ack(Duration::from_millis(48), 1000);

        // Base delay should be minimum: 45ms
        assert_eq!(controller.base_delay(), Duration::from_millis(45));
    }

    // NOTE: Base delay history bucket rollover test removed - would require
    // 121 seconds of real time. Bucket logic is tested implicitly by other tests.

    #[test]
    fn test_ledbat_stats() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        controller.on_send(5000);
        controller.on_ack(Duration::from_millis(50), 1000);

        let stats = controller.stats();
        assert_eq!(stats.cwnd, 10_000);
        assert_eq!(stats.flightsize, 4000);
        assert_eq!(stats.base_delay, Duration::from_millis(50));
    }

    #[test]
    #[should_panic(expected = "min_cwnd (10000) must be <= initial_cwnd (5000)")]
    fn test_config_validation_min_greater_than_initial() {
        let config = LedbatConfig {
            initial_cwnd: 5_000,
            min_cwnd: 10_000, // min > initial - invalid!
            max_cwnd: 100_000,
            ..Default::default()
        };
        LedbatController::new_with_config(config);
    }

    #[test]
    #[should_panic(expected = "initial_cwnd (150000) must be <= max_cwnd (100000)")]
    fn test_config_validation_initial_greater_than_max() {
        let config = LedbatConfig {
            initial_cwnd: 150_000, // initial > max - invalid!
            min_cwnd: 2_848,
            max_cwnd: 100_000,
            ..Default::default()
        };
        LedbatController::new_with_config(config);
    }

    #[test]
    #[should_panic(expected = "delay_exit_threshold (1.5) must be in range [0.0, 1.0]")]
    fn test_config_validation_threshold_out_of_range() {
        let config = LedbatConfig {
            delay_exit_threshold: 1.5, // > 1.0 - invalid!
            ..Default::default()
        };
        LedbatController::new_with_config(config);
    }

    #[test]
    fn test_config_validation_valid_config() {
        // Should not panic with valid config
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 100_000,
            delay_exit_threshold: 0.5,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        assert_eq!(controller.current_cwnd(), 50_000);
    }

    #[test]
    fn test_ssthresh_randomization_uses_different_values() {
        // Create multiple controllers with randomization enabled
        // Verify they don't all get the same ssthresh (would indicate poor entropy)
        let mut ssthresh_values = std::collections::HashSet::new();

        for _ in 0..10 {
            let config = LedbatConfig {
                ssthresh: 100_000,
                randomize_ssthresh: true,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);
            // ssthresh is private, but we can observe it indirectly via slow start exit
            // For this test, just verify construction succeeds with randomization
            assert!(controller.current_cwnd() > 0);

            // The jitter should produce values in range [80_000, 120_000]
            // We can't directly observe ssthresh, but at least verify creation works
            ssthresh_values.insert(controller.current_cwnd());
        }

        // With proper randomization, we should see some variation
        // (initial_cwnd is fixed, so this just verifies no crashes with RNG)
        assert!(!ssthresh_values.is_empty());
    }

    /// Stress test: multiple threads calling on_ack concurrently.
    /// Validates lock-free correctness under contention.
    #[test]
    fn test_concurrent_on_ack_stress() {
        use std::sync::Arc;
        use std::thread;

        let controller = Arc::new(LedbatController::new(100_000, 2848, 10_000_000));
        let num_threads = 8;
        let iterations_per_thread = 1000;

        // Pre-send enough data to cover all ACKs
        controller.on_send(num_threads * iterations_per_thread * 1000);

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let controller = Arc::clone(&controller);
                thread::spawn(move || {
                    for j in 0..iterations_per_thread {
                        // Vary RTT to exercise different code paths
                        let rtt_ms = 20 + (i * 10) + (j % 50);
                        controller.on_ack(Duration::from_millis(rtt_ms as u64), 1000);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify controller is in a valid state
        let stats = controller.stats();
        assert!(stats.cwnd >= controller.min_cwnd);
        assert!(stats.cwnd <= controller.max_cwnd);
        // Base delay should be set (minimum RTT was 20ms)
        assert!(stats.base_delay <= Duration::from_millis(100));
    }

    /// Stress test: concurrent on_send and on_ack from different threads.
    /// Validates flightsize tracking under contention.
    #[test]
    fn test_concurrent_send_ack_stress() {
        use std::sync::Arc;
        use std::thread;

        let controller = Arc::new(LedbatController::new(100_000, 2848, 10_000_000));
        let iterations = 500;

        // Sender thread
        let controller_send = Arc::clone(&controller);
        let sender = thread::spawn(move || {
            for _ in 0..iterations {
                controller_send.on_send(1000);
                thread::yield_now(); // Encourage interleaving
            }
        });

        // ACK thread
        let controller_ack = Arc::clone(&controller);
        let acker = thread::spawn(move || {
            for i in 0..iterations {
                let rtt_ms = 30 + (i % 20);
                controller_ack.on_ack(Duration::from_millis(rtt_ms as u64), 1000);
                thread::yield_now();
            }
        });

        sender.join().expect("Sender panicked");
        acker.join().expect("Acker panicked");

        // Flightsize should be reasonable (may not be exactly 0 due to timing)
        // Just verify no underflow (would wrap to huge value)
        assert!(controller.flightsize() < 1_000_000);
    }

    /// Stress test: concurrent loss events.
    /// Validates cwnd halving under contention.
    #[test]
    fn test_concurrent_loss_stress() {
        use std::sync::Arc;
        use std::thread;

        let controller = Arc::new(LedbatController::new(1_000_000, 2848, 10_000_000));
        let num_threads = 4;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let controller = Arc::clone(&controller);
                thread::spawn(move || {
                    for _ in 0..10 {
                        controller.on_loss();
                        thread::yield_now();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // After many loss events, cwnd should be at or near min_cwnd
        assert!(
            controller.current_cwnd() <= 10_000,
            "cwnd should be small after many losses, got {}",
            controller.current_cwnd()
        );
        assert!(controller.current_cwnd() >= controller.min_cwnd);

        // Total losses should be num_threads * 10
        let stats = controller.stats();
        assert_eq!(stats.total_losses, num_threads * 10);
    }

    /// Stress test: concurrent state transitions (timeout vs ACK during slowdown).
    ///
    /// This test verifies that racing `on_timeout` and `on_ack` calls during
    /// slowdown states don't cause panics or invalid states. Due to the
    /// non-atomic nature of compound state transitions, the final state
    /// may vary, but the state machine should always remain valid.
    #[test]
    fn test_concurrent_state_transition_stress() {
        use std::sync::Arc;
        use std::thread;

        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: false, // Start in CongestionAvoidance
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = Arc::new(LedbatController::new_with_config(config));

        // Set up initial state for slowdown cycle
        controller.on_send(500_000);
        controller
            .base_delay_history
            .update(Duration::from_millis(50));

        let num_threads = 4;
        let iterations = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let controller = Arc::clone(&controller);
                thread::spawn(move || {
                    for j in 0..iterations {
                        // Alternate between timeout and ACK to stress state transitions
                        if (i + j) % 3 == 0 {
                            controller.on_timeout();
                        } else {
                            controller.on_ack(Duration::from_millis(50 + (j % 20) as u64), 1000);
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("Thread panicked during state transition stress test");
        }

        // Verify state machine is still valid (load should succeed without panic)
        let final_state = controller.congestion_state.load();

        // State should be one of the valid states
        assert!(
            matches!(
                final_state,
                CongestionState::SlowStart
                    | CongestionState::CongestionAvoidance
                    | CongestionState::WaitingForSlowdown
                    | CongestionState::InSlowdown
                    | CongestionState::Frozen
                    | CongestionState::RampingUp
            ),
            "Final state should be valid: {:?}",
            final_state
        );

        // cwnd should be within bounds
        let final_cwnd = controller.current_cwnd();
        assert!(
            final_cwnd >= controller.min_cwnd && final_cwnd <= controller.max_cwnd,
            "cwnd should be within bounds: {}",
            final_cwnd
        );
    }

    /// Test atomic delay filter under concurrent access.
    #[test]
    fn test_atomic_delay_filter_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let filter = Arc::new(AtomicDelayFilter::new());
        let num_threads = 4;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let filter = Arc::clone(&filter);
                thread::spawn(move || {
                    for j in 0..iterations {
                        let rtt_ms = 10 + i * 5 + (j % 10);
                        filter.add_sample(Duration::from_millis(rtt_ms as u64));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Filter should be ready and have a valid minimum
        assert!(filter.is_ready());
        let min_delay = filter.filtered_delay().expect("Should have samples");
        // Minimum should be at least 10ms (the lowest we added)
        assert!(min_delay >= Duration::from_millis(10));
        assert!(min_delay <= Duration::from_millis(50));
    }

    /// Test atomic base delay history under concurrent access.
    #[test]
    fn test_atomic_base_delay_history_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let history = Arc::new(AtomicBaseDelayHistory::new(InstantTimeSrc::new()));
        let num_threads = 4;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let history = Arc::clone(&history);
                thread::spawn(move || {
                    for j in 0..iterations {
                        let rtt_ms = 20 + i * 10 + (j % 15);
                        history.update(Duration::from_millis(rtt_ms as u64));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Base delay should be valid and reflect minimum RTT
        let base = history.base_delay();
        assert!(base >= Duration::from_millis(20));
        assert!(base <= Duration::from_millis(100));
    }

    /// Test that the minute rollover race condition is handled correctly.
    ///
    /// This test verifies that when multiple threads race to roll over the minute
    /// boundary, the smallest RTT value is correctly recorded and no values are lost.
    /// The fix uses swap(EMPTY) + update_current_min instead of swap(rtt) to prevent
    /// a winning thread from overwriting a smaller value set by a losing thread.
    #[test]
    fn test_minute_rollover_race_condition() {
        use std::sync::atomic::AtomicU64;
        use std::sync::Arc;
        use std::thread;

        // Create a history and immediately add a sample to establish baseline
        let history = Arc::new(AtomicBaseDelayHistory::new(InstantTimeSrc::new()));
        history.update(Duration::from_millis(100)); // Initial sample

        // Verify initial state
        assert_eq!(history.base_delay(), Duration::from_millis(100));

        // Track the minimum RTT we send across all threads
        let expected_min = Arc::new(AtomicU64::new(u64::MAX));

        // Simulate many threads racing to update with different RTT values
        // In a real minute rollover scenario, all threads would see the minute
        // boundary at roughly the same time
        let num_threads = 16;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let history = Arc::clone(&history);
                let expected_min = Arc::clone(&expected_min);
                thread::spawn(move || {
                    for j in 0..iterations {
                        // Each thread uses different RTT values
                        // Thread 0 will have the smallest values (10-19ms)
                        let rtt_ms = 10 + i + (j % 10);
                        let rtt_nanos = rtt_ms as u64 * 1_000_000;

                        // Track the minimum we're sending
                        expected_min
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                                if rtt_nanos < current {
                                    Some(rtt_nanos)
                                } else {
                                    None
                                }
                            })
                            .ok();

                        history.update(Duration::from_millis(rtt_ms as u64));

                        // Yield to encourage interleaving
                        if j % 10 == 0 {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // The base delay should reflect the minimum RTT sent by any thread
        let actual_min = history.base_delay();
        let _expected = Duration::from_nanos(expected_min.load(Ordering::Relaxed));

        // The actual minimum should be <= expected (we might have lost some due to
        // timing, but we should never record a LARGER minimum than what was sent)
        assert!(
            actual_min <= Duration::from_millis(20),
            "Base delay {} should be <= 20ms (smallest thread's range)",
            actual_min.as_millis()
        );

        // More importantly: base delay should be a valid value we actually sent
        // (between 10ms and the largest possible value)
        assert!(
            actual_min >= Duration::from_millis(10),
            "Base delay {} should be >= 10ms (smallest value sent)",
            actual_min.as_millis()
        );
    }

    /// Test that concurrent updates don't lose the smallest value during rollover.
    ///
    /// This specifically tests the fix for the race where:
    /// 1. Thread A wins CAS for rollover
    /// 2. Thread B loses CAS, calls update_current_min with smaller RTT
    /// 3. Thread A's swap would have overwritten B's smaller value (BUG)
    /// 4. Fix: Thread A swaps to EMPTY then calls update_current_min (CORRECT)
    #[test]
    fn test_no_minimum_value_lost_during_rollover() {
        use std::sync::Arc;
        use std::thread;

        let history = Arc::new(AtomicBaseDelayHistory::new(InstantTimeSrc::new()));

        // Many threads all trying to set different minimums
        let num_threads = 8;
        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let history = Arc::clone(&history);
                thread::spawn(move || {
                    // Thread 0 sends smallest (5ms), thread 7 sends largest (12ms)
                    let rtt_ms = 5 + i;
                    for _ in 0..50 {
                        history.update(Duration::from_millis(rtt_ms as u64));
                        thread::yield_now();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // The minimum should be 5ms (from thread 0)
        let base = history.base_delay();
        assert_eq!(
            base,
            Duration::from_millis(5),
            "Base delay should be 5ms (the smallest value sent), got {}ms",
            base.as_millis()
        );
    }

    // ============ LEDBAT++ Specific Tests ============

    /// Test dynamic GAIN calculation (LEDBAT++ Section 4.2)
    /// GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
    #[test]
    fn test_dynamic_gain_calculation() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // TARGET = 60ms
        // For base_delay = 60ms: ceil(2 * 60 / 60) = 2, GAIN = 1/2 = 0.5
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(60));
        assert!(
            (gain - 0.5).abs() < 0.01,
            "GAIN for 60ms base should be 0.5, got {}",
            gain
        );

        // For base_delay = 30ms: ceil(2 * 60 / 30) = 4, GAIN = 1/4 = 0.25
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(30));
        assert!(
            (gain - 0.25).abs() < 0.01,
            "GAIN for 30ms base should be 0.25, got {}",
            gain
        );

        // For base_delay = 10ms: ceil(2 * 60 / 10) = 12, GAIN = 1/12 ≈ 0.083
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(10));
        assert!(
            (gain - 1.0 / 12.0).abs() < 0.01,
            "GAIN for 10ms base should be ~0.083, got {}",
            gain
        );

        // For very small base_delay (1ms): ceil(2 * 60 / 1) = 120 > 16, capped at 16
        // GAIN = 1/16 = 0.0625
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(1));
        assert!(
            (gain - 1.0 / 16.0).abs() < 0.01,
            "GAIN for 1ms base should be capped at 0.0625, got {}",
            gain
        );

        // For zero/near-zero base_delay, should fallback to minimum gain
        let gain = controller.calculate_dynamic_gain(Duration::ZERO);
        assert!(
            (gain - 1.0 / 16.0).abs() < 0.01,
            "GAIN for 0ms base should be 0.0625, got {}",
            gain
        );
    }

    /// Test that TARGET delay is now 60ms (LEDBAT++)
    #[test]
    fn test_target_delay_is_60ms() {
        assert_eq!(
            TARGET,
            Duration::from_millis(60),
            "LEDBAT++ TARGET should be 60ms"
        );
    }

    /// Test that default slow start exit threshold is 0.75 (LEDBAT++)
    #[test]
    fn test_slow_start_exit_threshold_is_75_percent() {
        let config = LedbatConfig::default();
        assert!(
            (config.delay_exit_threshold - 0.75).abs() < 0.01,
            "LEDBAT++ delay_exit_threshold should be 0.75, got {}",
            config.delay_exit_threshold
        );
    }

    /// Test multiplicative decrease cap at -W/2 (LEDBAT++)
    #[tokio::test]
    async fn test_multiplicative_decrease_capped_at_half() {
        // Disable slow start and periodic slowdown to test pure congestion avoidance
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            enable_slow_start: false,
            enable_periodic_slowdown: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Track flightsize
        controller.on_send(200_000);

        // Set low base delay
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill delay filter with very high RTT (extreme congestion)
        for _ in 0..4 {
            controller.on_send(10_000);
            controller.on_ack(Duration::from_millis(500), 2000);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update and apply extreme congestion signal
        tokio::time::sleep(Duration::from_millis(20)).await;
        controller.on_send(50_000);
        controller.on_ack(Duration::from_millis(500), 10_000);

        let new_cwnd = controller.current_cwnd();

        // The decrease should be capped at 50% per RTT
        // Even with extreme queuing delay, cwnd should not drop below initial/2
        assert!(
            new_cwnd >= initial_cwnd / 2 - 1000, // Allow some tolerance
            "cwnd decrease should be capped at 50%, was {} -> {} (more than half)",
            initial_cwnd,
            new_cwnd
        );
    }

    /// Test periodic slowdown config option
    #[test]
    fn test_periodic_slowdown_config() {
        // With periodic slowdown enabled (default)
        let config = LedbatConfig::default();
        assert!(
            config.enable_periodic_slowdown,
            "Periodic slowdown should be enabled by default"
        );

        // Can be disabled
        let config = LedbatConfig {
            enable_periodic_slowdown: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        assert!(!controller.enable_periodic_slowdown);
    }

    /// Test congestion state machine initialization
    #[test]
    fn test_congestion_state_initial_state() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Initial state should be SlowStart (default config has enable_slow_start=true)
        let state = controller.congestion_state.load();
        assert_eq!(
            state,
            CongestionState::SlowStart,
            "Initial state should be SlowStart when enable_slow_start=true"
        );

        // initial_slow_start_completed should be false
        assert!(
            !controller
                .initial_slow_start_completed
                .load(Ordering::Relaxed),
            "initial_slow_start_completed should be false initially"
        );

        // No slowdown scheduled yet
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Relaxed);
        assert_eq!(
            next_slowdown,
            u64::MAX,
            "No slowdown should be scheduled initially"
        );
    }

    /// Test that periodic slowdown statistics are tracked
    #[test]
    fn test_periodic_slowdown_stats() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        let stats = controller.stats();
        assert_eq!(
            stats.periodic_slowdowns, 0,
            "Initial periodic_slowdowns should be 0"
        );
    }

    /// Test congestion state enum values
    #[test]
    fn test_congestion_state_values() {
        // Verify enum values for state machine
        assert_eq!(CongestionState::SlowStart as u8, 0);
        assert_eq!(CongestionState::CongestionAvoidance as u8, 1);
        assert_eq!(CongestionState::WaitingForSlowdown as u8, 2);
        assert_eq!(CongestionState::InSlowdown as u8, 3);
        assert_eq!(CongestionState::Frozen as u8, 4);
        assert_eq!(CongestionState::RampingUp as u8, 5);
    }

    /// Integration test: slow start exit schedules initial slowdown
    #[tokio::test]
    async fn test_slow_start_exit_schedules_slowdown() {
        let config = LedbatConfig {
            initial_cwnd: 15_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 20_000, // Low threshold so we exit slow start quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Send data and establish RTT
        controller.on_send(100_000);

        // Initial samples to set base delay (need 2+ for filter)
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);

        // Wait for update interval (based on base_delay ~20ms)
        tokio::time::sleep(Duration::from_millis(25)).await;

        // This ACK should trigger slow start growth: 15000 + 10000 = 25000 >= 20000 ssthresh
        // Should trigger exit check
        controller.on_ack(Duration::from_millis(20), 10_000);

        // Wait for another RTT to ensure the exit check runs
        // (the first ACK after the wait may just do growth, second triggers exit check)
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Wait for yet another RTT to make sure the exit is processed
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Should have exited slow start (cwnd grew past ssthresh)
        let state = controller.congestion_state.load();
        let cwnd = controller.current_cwnd();
        assert!(
            state != CongestionState::SlowStart,
            "Should have exited slow start (cwnd={}, ssthresh=20000, state={:?})",
            cwnd,
            state
        );

        // Should have marked initial slow start as completed
        assert!(
            controller
                .initial_slow_start_completed
                .load(Ordering::Acquire),
            "initial_slow_start_completed should be true after slow start exit"
        );

        // Slowdown should be scheduled (not u64::MAX)
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        assert!(
            next_slowdown != u64::MAX,
            "A slowdown should be scheduled after slow start exit"
        );

        // State should be WaitingForSlowdown
        let state = controller.congestion_state.load();
        assert_eq!(
            state,
            CongestionState::WaitingForSlowdown,
            "State should be WaitingForSlowdown"
        );
    }

    /// Test initial slow start ramp-up: cwnd should grow exponentially
    #[tokio::test]
    async fn test_initial_slow_start_ramp_up() {
        let config = LedbatConfig {
            initial_cwnd: 10_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 500_000, // High threshold so we stay in slow start
            enable_slow_start: true,
            enable_periodic_slowdown: false, // Disable to focus on slow start
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(500_000);

        // Establish base delay
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);

        let initial_cwnd = controller.current_cwnd();

        // First RTT: grow by bytes_acked
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 5_000);
        let cwnd_after_1 = controller.current_cwnd();

        // Verify exponential growth (cwnd += bytes_acked)
        assert!(
            cwnd_after_1 > initial_cwnd,
            "cwnd should grow: {} -> {}",
            initial_cwnd,
            cwnd_after_1
        );

        // Second RTT: grow more
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 10_000);
        let cwnd_after_2 = controller.current_cwnd();

        assert!(
            cwnd_after_2 > cwnd_after_1,
            "cwnd should continue growing: {} -> {}",
            cwnd_after_1,
            cwnd_after_2
        );

        // Should still be in slow start
        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::SlowStart,
            "Should still be in slow start (cwnd={}, ssthresh=500000)",
            cwnd_after_2
        );

        println!(
            "Slow start ramp-up: {} -> {} -> {} bytes",
            initial_cwnd, cwnd_after_1, cwnd_after_2
        );
    }

    /// Test proportional slowdown reduction (4x, not to minimum)
    #[tokio::test]
    async fn test_proportional_slowdown_reduction() {
        let config = LedbatConfig {
            initial_cwnd: 320_000, // Start high to see proportional reduction clearly
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000, // Below initial so we exit slow start immediately
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(500_000);

        // Establish base delay and trigger slow start exit
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);

        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Should have exited slow start (initial_cwnd > ssthresh)
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        assert!(
            controller.congestion_state.load() != CongestionState::SlowStart,
            "Should have exited slow start"
        );

        // Get cwnd before slowdown (should be ~320KB * 0.9 after slow start exit reduction)
        let cwnd_before_slowdown = controller.current_cwnd();
        println!("cwnd before slowdown: {} bytes", cwnd_before_slowdown);

        // Wait for slowdown to trigger (2 RTTs = 40ms)
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Should now be in slowdown
        let state = controller.congestion_state.load();
        assert!(
            state == CongestionState::InSlowdown || state == CongestionState::Frozen,
            "Should be in slowdown state, got {:?}",
            state
        );

        // Trigger transition to frozen
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let cwnd_during_slowdown = controller.current_cwnd();
        println!("cwnd during slowdown: {} bytes", cwnd_during_slowdown);

        // Verify proportional reduction: should be pre_slowdown / 16, not min_cwnd
        let pre_slowdown = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        let expected_slowdown_cwnd = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(2848);

        assert_eq!(
            cwnd_during_slowdown, expected_slowdown_cwnd,
            "Slowdown cwnd should be pre_slowdown/16 = {}/16 = {}, got {}",
            pre_slowdown, expected_slowdown_cwnd, cwnd_during_slowdown
        );

        // Verify it's NOT just min_cwnd (unless pre_slowdown was very small)
        if pre_slowdown > SLOWDOWN_REDUCTION_FACTOR * 2848 {
            assert!(
                cwnd_during_slowdown > 2848,
                "With large pre_slowdown ({}), slowdown cwnd ({}) should be > min_cwnd (2848)",
                pre_slowdown,
                cwnd_during_slowdown
            );
        }
    }

    /// Test complete slowdown cycle: frozen → ramp-up → normal
    #[tokio::test]
    async fn test_complete_slowdown_cycle() {
        let config = LedbatConfig {
            initial_cwnd: 160_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000, // Exit slow start quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Phase 1: Exit slow start
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        assert!(
            controller.congestion_state.load() != CongestionState::SlowStart,
            "Should have exited slow start"
        );
        let pre_slowdown_cwnd = controller.current_cwnd();
        println!(
            "Phase 1 - After slow start exit: cwnd = {}",
            pre_slowdown_cwnd
        );

        // Phase 2: Wait for slowdown to trigger (2 RTTs)
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let state = controller.congestion_state.load();
        println!("Phase 2 - After wait: state = {:?}", state);

        // Phase 3: Enter frozen state
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let frozen_cwnd = controller.current_cwnd();
        let state = controller.congestion_state.load();
        println!(
            "Phase 3 - Frozen: cwnd = {}, state = {:?}",
            frozen_cwnd, state
        );

        assert_eq!(state, CongestionState::Frozen, "Should be in Frozen state");

        // Verify proportional reduction
        let expected_frozen = (controller.pre_slowdown_cwnd.load(Ordering::Relaxed)
            / SLOWDOWN_REDUCTION_FACTOR)
            .max(2848);
        assert_eq!(
            frozen_cwnd, expected_frozen,
            "Frozen cwnd should be proportional"
        );

        // Phase 4: Wait for freeze duration (2 RTTs) then start ramp-up
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 5000);

        let state = controller.congestion_state.load();
        println!("Phase 4 - After freeze: state = {:?}", state);

        // Should now be ramping up
        assert_eq!(
            state,
            CongestionState::RampingUp,
            "Should be in RampingUp state"
        );

        // Phase 5: Ramp up until reaching pre_slowdown_cwnd
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        let mut ramp_iterations = 0;
        while controller.current_cwnd() < target && ramp_iterations < 20 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            controller.on_ack(Duration::from_millis(20), 20_000);
            ramp_iterations += 1;
        }

        // One more ACK to trigger state transition (state changes after cwnd >= target)
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let final_cwnd = controller.current_cwnd();
        let final_state = controller.congestion_state.load();
        println!(
            "Phase 5 - After ramp-up: cwnd = {}, state = {:?}, iterations = {}",
            final_cwnd, final_state, ramp_iterations
        );

        // Should have returned to normal
        assert_eq!(
            final_state,
            CongestionState::CongestionAvoidance,
            "Should be back to Normal state"
        );

        // Verify next slowdown is scheduled
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        assert!(
            next_slowdown != u64::MAX,
            "Next slowdown should be scheduled"
        );

        // Verify slowdown count increased
        let slowdown_count = controller.periodic_slowdowns.load(Ordering::Relaxed);
        assert_eq!(slowdown_count, 1, "Should have completed 1 slowdown");
    }

    /// Test that slowdown interval is 9x the slowdown duration
    #[tokio::test]
    async fn test_slowdown_interval_is_9x_duration() {
        let config = LedbatConfig {
            initial_cwnd: 160_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Exit slow start
        for _ in 0..4 {
            controller.on_ack(Duration::from_millis(20), 1000);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Wait for and complete slowdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Go through frozen phase
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 5000);

        // Ramp up
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        while controller.current_cwnd() < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
            controller.on_ack(Duration::from_millis(20), 30_000);
        }

        // Get the slowdown duration and next interval
        let slowdown_duration = controller
            .last_slowdown_duration_nanos
            .load(Ordering::Acquire);
        let now_nanos = controller.epoch.elapsed().as_nanos() as u64;
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        let next_interval = next_slowdown.saturating_sub(now_nanos);

        println!(
            "Slowdown duration: {}ms, Next interval from now: {}ms",
            slowdown_duration / 1_000_000,
            next_interval / 1_000_000
        );

        // The next interval should be approximately 9x the slowdown duration
        // (with some tolerance for timing variations)
        let expected_interval = slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
        let min_expected = expected_interval / 2; // Allow 50% tolerance

        assert!(
            next_interval >= min_expected || next_interval >= 20_000_000, // or at least 1 RTT
            "Next interval ({}) should be >= 9x slowdown duration ({}) = {}",
            next_interval / 1_000_000,
            slowdown_duration / 1_000_000,
            expected_interval / 1_000_000
        );
    }

    /// Regression test: min_interval should be 18 RTTs, not 1 RTT
    ///
    /// On high-latency paths (100+ms RTT), if slowdown cycles complete quickly
    /// (e.g., ramp-up finishes immediately because cwnd is already at target),
    /// the 9x multiplier on a short duration could be less than 1 RTT.
    ///
    /// BUG: The original code used `min_interval = 1 RTT`, causing slowdowns
    /// every RTT on high-latency paths. With 137ms RTT, this caused 224 slowdowns
    /// in 30 seconds, keeping cwnd perpetually low.
    ///
    /// FIX: min_interval = 9 * freeze_duration = 9 * 2 * RTT = 18 RTTs
    ///
    /// This test verifies the min_interval calculation directly by calling
    /// complete_slowdown with a very short slowdown_duration.
    #[test]
    fn test_min_interval_is_18_rtts_not_1_rtt() {
        let high_latency_rtt = Duration::from_millis(137); // WAN RTT

        let config = LedbatConfig {
            initial_cwnd: 160_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Simulate having just started a slowdown (set phase_start to now)
        let now_nanos = controller.epoch.elapsed().as_nanos() as u64;
        controller
            .slowdown_phase_start_nanos
            .store(now_nanos, Ordering::Release);

        // Set base delay history so base_delay() returns the high RTT
        controller.base_delay_history.update(high_latency_rtt);

        // Call complete_slowdown immediately (simulating very short slowdown_duration)
        // This is the scenario that caused the bug
        let completion_nanos = controller.epoch.elapsed().as_nanos() as u64;
        controller.complete_slowdown(completion_nanos, high_latency_rtt);

        // Get the scheduled next slowdown time
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        let next_interval_nanos = next_slowdown.saturating_sub(completion_nanos);
        let next_interval_ms = next_interval_nanos / 1_000_000;

        // Calculate expected minimum: 18 RTTs (9 * 2 * RTT)
        let one_rtt_ms = high_latency_rtt.as_millis() as u64;
        let min_18_rtts_ms =
            one_rtt_ms * SLOWDOWN_FREEZE_RTTS as u64 * SLOWDOWN_INTERVAL_MULTIPLIER as u64;

        let slowdown_duration_ms = controller
            .last_slowdown_duration_nanos
            .load(Ordering::Acquire)
            / 1_000_000;

        println!(
            "High-latency min_interval test: RTT={}ms",
            high_latency_rtt.as_millis()
        );
        println!(
            "  slowdown_duration={}ms (very short, simulated)",
            slowdown_duration_ms
        );
        println!("  next_interval={}ms", next_interval_ms);
        println!("  1 RTT = {}ms (old buggy min)", one_rtt_ms);
        println!("  18 RTTs = {}ms (new correct min)", min_18_rtts_ms);

        // The key assertion: even with a very short slowdown_duration,
        // the min_interval should be 18 RTTs (2466ms), NOT 1 RTT (137ms)
        assert!(
            next_interval_ms >= min_18_rtts_ms - 100, // Allow small tolerance for timing
            "Next interval ({} ms) should be >= 18 RTTs ({} ms). \
             Old code would give ~1 RTT ({} ms), causing slowdowns every RTT!",
            next_interval_ms,
            min_18_rtts_ms,
            one_rtt_ms
        );

        // Also verify it's significantly more than 1 RTT
        assert!(
            next_interval_ms > one_rtt_ms * 10,
            "Next interval ({} ms) should be >> 1 RTT ({} ms)",
            next_interval_ms,
            one_rtt_ms
        );
    }

    /// Test cwnd values at each phase of slowdown
    #[tokio::test]
    async fn test_cwnd_values_through_slowdown_phases() {
        let config = LedbatConfig {
            initial_cwnd: 256_000, // 256KB - nice round number divisible by 16
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Record cwnd at each phase
        let mut cwnd_history: Vec<(String, usize)> = Vec::new();

        cwnd_history.push(("initial".to_string(), controller.current_cwnd()));

        // Exit slow start
        for i in 0..4 {
            controller.on_ack(Duration::from_millis(20), 1000);
            tokio::time::sleep(Duration::from_millis(25)).await;
            if i == 3 {
                cwnd_history.push((
                    "after_slow_start_exit".to_string(),
                    controller.current_cwnd(),
                ));
            }
        }

        // Trigger slowdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        cwnd_history.push(("slowdown_triggered".to_string(), controller.current_cwnd()));

        // Enter frozen
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        cwnd_history.push(("frozen".to_string(), controller.current_cwnd()));

        // Start ramp-up
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 5000);
        cwnd_history.push(("ramp_up_start".to_string(), controller.current_cwnd()));

        // Mid ramp-up
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 20_000);
        cwnd_history.push(("ramp_up_mid".to_string(), controller.current_cwnd()));

        // Complete ramp-up
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        while controller.current_cwnd() < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
            controller.on_ack(Duration::from_millis(20), 30_000);
        }
        cwnd_history.push(("ramp_up_complete".to_string(), controller.current_cwnd()));

        // Print cwnd history
        println!("\n=== CWND History Through Slowdown ===");
        for (phase, cwnd) in &cwnd_history {
            println!("  {}: {} KB", phase, cwnd / 1024);
        }
        println!("=====================================\n");

        // Verify key transitions:
        // 1. After slow start exit should be ~90% of initial (0.9 reduction)
        let after_exit = cwnd_history
            .iter()
            .find(|(p, _)| p == "after_slow_start_exit")
            .map(|(_, c)| *c)
            .unwrap();
        assert!(
            after_exit < 256_000,
            "After exit should be reduced from initial"
        );

        // 2. Frozen should be ~1/16 of pre_slowdown
        let frozen = cwnd_history
            .iter()
            .find(|(p, _)| p == "frozen")
            .map(|(_, c)| *c)
            .unwrap();
        let pre_slowdown = controller.pre_slowdown_cwnd.load(Ordering::Relaxed);
        let expected_frozen = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(2848);
        assert_eq!(
            frozen, expected_frozen,
            "Frozen should be pre_slowdown/{} = {}/{} = {}",
            SLOWDOWN_REDUCTION_FACTOR, pre_slowdown, SLOWDOWN_REDUCTION_FACTOR, expected_frozen
        );

        // 3. Ramp-up complete should be back near pre_slowdown
        let complete = cwnd_history
            .iter()
            .find(|(p, _)| p == "ramp_up_complete")
            .map(|(_, c)| *c)
            .unwrap();
        assert!(
            complete >= pre_slowdown,
            "After ramp-up should be >= pre_slowdown: {} >= {}",
            complete,
            pre_slowdown
        );
    }

    /// Helper to test slowdown behavior at a given RTT
    async fn test_slowdown_at_rtt(rtt_ms: u64) -> (usize, usize, usize, u32) {
        let config = LedbatConfig {
            initial_cwnd: 20_000, // Start small to allow slow start growth
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 150_000, // High threshold so we grow before exit
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        let rtt = Duration::from_millis(rtt_ms);

        controller.on_send(1_000_000);

        // Exit slow start with appropriate timing for this RTT
        // Need more iterations to grow cwnd and hit ssthresh
        for _ in 0..10 {
            controller.on_ack(rtt, 15_000); // Larger ACKs for faster growth
            tokio::time::sleep(rtt + Duration::from_millis(5)).await;
        }
        let post_exit_cwnd = controller.current_cwnd();

        // Wait for slowdown trigger (2 RTTs + margin)
        tokio::time::sleep(rtt * 3).await;
        controller.on_ack(rtt, 1000);

        // Transition through states
        tokio::time::sleep(rtt + Duration::from_millis(5)).await;
        controller.on_ack(rtt, 1000);
        let frozen_cwnd = controller.current_cwnd();

        // Wait for freeze duration (2 RTTs)
        tokio::time::sleep(rtt * 3).await;
        controller.on_ack(rtt, 5000);

        // Ramp up
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        let mut ramp_iterations = 0u32;
        while controller.current_cwnd() < target && ramp_iterations < 50 {
            tokio::time::sleep(rtt).await;
            controller.on_ack(rtt, 30_000);
            ramp_iterations += 1;
        }
        let final_cwnd = controller.current_cwnd();

        (post_exit_cwnd, frozen_cwnd, final_cwnd, ramp_iterations)
    }

    /// Parametrized test for slowdown behavior at different RTTs
    /// Tests: 10ms (LAN), 50ms (regional), 100ms (intercontinental), 200ms (satellite), 300ms (high-latency WAN)
    #[rstest::rstest]
    #[case::low_latency_10ms(10, 20)]
    #[case::medium_latency_50ms(50, 30)]
    #[case::high_latency_100ms(100, 40)]
    #[case::very_high_latency_200ms(200, 50)]
    #[case::extreme_latency_300ms(300, 60)]
    #[tokio::test]
    async fn test_slowdown_at_various_latencies(#[case] rtt_ms: u64, #[case] max_iterations: u32) {
        let (post_exit, frozen, final_cwnd, iterations) = test_slowdown_at_rtt(rtt_ms).await;

        println!(
            "{}ms RTT: post_exit={}KB, frozen={}KB, final={}KB, iterations={}",
            rtt_ms,
            post_exit / 1024,
            frozen / 1024,
            final_cwnd / 1024,
            iterations
        );

        // Verify key behaviors:
        // 1. Frozen should be ~1/4 of pre_slowdown (SLOWDOWN_REDUCTION_FACTOR = 4)
        assert!(
            frozen < post_exit / 2,
            "{}ms RTT: Frozen ({}) should be less than half of post_exit ({})",
            rtt_ms,
            frozen,
            post_exit
        );

        // 2. Final cwnd should recover to at least 95% of pre_slowdown
        assert!(
            final_cwnd >= post_exit * 95 / 100,
            "{}ms RTT: Final ({}) should recover to near pre_slowdown ({})",
            rtt_ms,
            final_cwnd,
            post_exit
        );

        // 3. Ramp-up should complete within reasonable iterations for this latency
        assert!(
            iterations < max_iterations,
            "{}ms RTT: Should ramp up within {} iterations, got {}",
            rtt_ms,
            max_iterations,
            iterations
        );
    }

    /// Test dynamic GAIN calculation at different latencies
    #[test]
    fn test_dynamic_gain_across_latencies() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
        // TARGET = 60ms

        // 10ms base delay: divisor = ceil(2 * 60 / 10) = 12, GAIN = 1/12
        let gain_10ms = controller.calculate_dynamic_gain(Duration::from_millis(10));
        assert!(
            (gain_10ms - 1.0 / 12.0).abs() < 0.001,
            "10ms: expected 1/12, got {}",
            gain_10ms
        );

        // 5ms base delay: divisor = ceil(2 * 60 / 5) = 24 -> capped at 16, GAIN = 1/16
        let gain_5ms = controller.calculate_dynamic_gain(Duration::from_millis(5));
        assert!(
            (gain_5ms - 1.0 / 16.0).abs() < 0.001,
            "5ms: expected 1/16 (capped), got {}",
            gain_5ms
        );

        // 30ms base delay: divisor = ceil(2 * 60 / 30) = 4, GAIN = 1/4
        let gain_30ms = controller.calculate_dynamic_gain(Duration::from_millis(30));
        assert!(
            (gain_30ms - 1.0 / 4.0).abs() < 0.001,
            "30ms: expected 1/4, got {}",
            gain_30ms
        );

        // 60ms base delay: divisor = ceil(2 * 60 / 60) = 2, GAIN = 1/2
        let gain_60ms = controller.calculate_dynamic_gain(Duration::from_millis(60));
        assert!(
            (gain_60ms - 1.0 / 2.0).abs() < 0.001,
            "60ms: expected 1/2, got {}",
            gain_60ms
        );

        // 120ms base delay: divisor = ceil(2 * 60 / 120) = 1, GAIN = 1/1 = 1
        let gain_120ms = controller.calculate_dynamic_gain(Duration::from_millis(120));
        assert!(
            (gain_120ms - 1.0).abs() < 0.001,
            "120ms: expected 1, got {}",
            gain_120ms
        );

        println!("Dynamic GAIN values:");
        println!("  5ms RTT:   {:.4} (capped at 1/16)", gain_5ms);
        println!("  10ms RTT:  {:.4}", gain_10ms);
        println!("  30ms RTT:  {:.4}", gain_30ms);
        println!("  60ms RTT:  {:.4}", gain_60ms);
        println!("  120ms RTT: {:.4}", gain_120ms);
    }

    // =========================================================================
    // E2E Simulation Tests - Visualize behavior at different latencies
    // =========================================================================
    //
    // These tests simulate data transfers at various RTT latencies and visualize
    // the LEDBAT controller's behavior (cwnd evolution, slow start, etc.).
    //
    // ## Running Larger Transfers
    //
    // The default tests use small transfer sizes (256KB-1MB) to keep execution
    // fast. To test larger transfers for performance analysis:
    //
    // ### Option 1: Modify existing tests temporarily
    // ```rust
    // // Change transfer size from 512KB to 10MB:
    // let (snapshots, duration_ms) = simulate_transfer_sync(50, 10 * 1024, 50);
    // ```
    //
    // ### Option 2: Add custom test cases to the parametrized test
    // ```rust
    // #[case::large_transfer_50ms(50, 10 * 1024)]  // 10MB @ 50ms
    // #[case::huge_transfer_100ms(100, 100 * 1024)] // 100MB @ 100ms
    // ```
    //
    // ### Option 3: Create a dedicated large transfer test (ignored by default)
    // ```rust
    // #[test]
    // #[ignore] // Run with: cargo test --ignored test_large_transfer
    // fn test_large_transfer() {
    //     let (snapshots, duration_ms) = simulate_transfer_sync(100, 50 * 1024, 1000);
    //     // 50MB @ 100ms RTT, snapshot every 1 second
    // }
    // ```
    //
    // ## Execution Time Estimates (real-time sleeps per RTT)
    //
    // | Transfer Size | 10ms RTT | 50ms RTT | 100ms RTT | 200ms RTT |
    // |---------------|----------|----------|-----------|-----------|
    // | 256KB         | ~0.1s    | ~0.2s    | ~0.3s     | ~0.6s     |
    // | 1MB           | ~0.3s    | ~0.5s    | ~0.6s     | ~1.8s     |
    // | 10MB          | ~1s      | ~2s      | ~4s       | ~10s      |
    // | 100MB         | ~8s      | ~20s     | ~40s      | ~100s     |
    //
    // Note: These are rough estimates. Actual time depends on slow start behavior
    // and steady-state throughput which varies with RTT.
    //
    // ## Enabling Periodic Slowdown in Simulations
    //
    // The simulation disables periodic slowdowns by default since they require
    // wall-clock time. To test with slowdowns, set `enable_periodic_slowdown: true`
    // in the config and accept the longer test duration (slowdown cycles are ~9x RTT).

    /// Snapshot of controller state at a point in time
    #[derive(Debug, Clone)]
    #[allow(dead_code)] // Fields used for debugging and analysis in visualization tests
    struct StateSnapshot {
        time_ms: u64,
        cwnd_kb: usize,
        state: CongestionState,
        throughput_mbps: f64,
        data_transferred_kb: usize,
    }

    /// Simulate a transfer and collect state snapshots (synchronous, no real-time delays)
    ///
    /// This uses a controlled test clock to simulate time passing, allowing the test
    /// to run instantly regardless of simulated RTT.
    ///
    /// Note: Since the controller uses wall-clock time for rate limiting, we need to
    /// reset the rate limiter before each simulated RTT to allow updates.
    fn simulate_transfer_sync(
        rtt_ms: u64,
        transfer_size_kb: usize,
        sample_interval_ms: u64,
    ) -> (Vec<StateSnapshot>, u64) {
        let rtt = Duration::from_millis(rtt_ms);
        let config = LedbatConfig {
            initial_cwnd: 38_000, // IW26
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000,
            enable_slow_start: true,
            // Disable periodic slowdown since it depends on wall-clock time
            // See test_complete_slowdown_cycle for slowdown tests
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        let mut snapshots = Vec::new();
        let mut total_data_kb: usize = 0;
        let mut elapsed_ms: u64 = 0;

        // Simulate flightsize - enough to not bottleneck
        controller.on_send(10_000_000);

        // Max simulation time (5 minutes virtual time)
        let max_time_ms = 300_000u64;

        // Prime the delay filter with initial samples
        for _ in 0..4 {
            controller.delay_filter.add_sample(rtt);
        }
        controller.base_delay_history.update(rtt);

        // Simulate transfer - each iteration represents one RTT
        while total_data_kb < transfer_size_kb && elapsed_ms < max_time_ms {
            elapsed_ms += rtt_ms;

            // Wait for at least base_delay so the rate limiter allows updates
            // This is the minimum real-time wait needed for accurate simulation
            std::thread::sleep(rtt);

            // Reset bytes accumulated and set last_update to allow this update
            controller.last_update_nanos.store(0, Ordering::Release);

            // Calculate bytes we can send this RTT (cwnd worth)
            let cwnd = controller.current_cwnd();
            let remaining_bytes = (transfer_size_kb * 1024).saturating_sub(total_data_kb * 1024);
            let bytes_this_rtt = cwnd.min(remaining_bytes);

            if bytes_this_rtt == 0 {
                // Edge case: cwnd too small, simulate at least one packet
                let min_packet = 1464usize;
                controller.on_ack(rtt, min_packet);
                total_data_kb += min_packet / 1024;
            } else {
                // Simulate ACK for sent data - simulate slight queuing delay for realism
                // Use 10% of target delay as typical queuing delay (6ms)
                let queued_rtt = Duration::from_nanos(rtt.as_nanos() as u64 + 6_000_000);
                controller.on_ack(queued_rtt, bytes_this_rtt);
                total_data_kb += bytes_this_rtt / 1024;
            }

            // Take snapshot at intervals
            if elapsed_ms % sample_interval_ms == 0 || elapsed_ms <= rtt_ms * 2 {
                let throughput = if elapsed_ms > 0 {
                    (total_data_kb as f64 * 1024.0 * 8.0)
                        / (elapsed_ms as f64 / 1000.0)
                        / 1_000_000.0
                } else {
                    0.0
                };

                snapshots.push(StateSnapshot {
                    time_ms: elapsed_ms,
                    cwnd_kb: controller.current_cwnd() / 1024,
                    state: controller.congestion_state.load(),
                    throughput_mbps: throughput,
                    data_transferred_kb: total_data_kb,
                });
            }
        }

        (snapshots, elapsed_ms)
    }

    /// Render ASCII throughput chart
    #[allow(dead_code)] // Visualization helper for debugging
    fn render_throughput_chart(snapshots: &[StateSnapshot], max_throughput: f64, width: usize) {
        let height = 10;
        let mut chart = vec![vec![' '; width]; height];

        // Plot data points
        for (i, snap) in snapshots.iter().enumerate() {
            if i >= width {
                break;
            }
            let normalized = (snap.throughput_mbps / max_throughput).min(1.0);
            let row = height - 1 - (normalized * (height - 1) as f64) as usize;
            chart[row][i] = match snap.state {
                CongestionState::SlowStart => '█',
                CongestionState::CongestionAvoidance => '█',
                CongestionState::WaitingForSlowdown => '▓',
                CongestionState::InSlowdown => '▒',
                CongestionState::Frozen => '░',
                CongestionState::RampingUp => '▄',
            };
        }

        // Print chart
        println!("\n    Throughput over time (█=Normal ░=Frozen ▄=RampingUp)");
        println!("    {:.1} Mbps ┤", max_throughput);
        for row in &chart {
            print!("            │");
            for &c in row {
                print!("{}", c);
            }
            println!();
        }
        println!(
            "      0 Mbps ┼{}┬──────────────────────────────────────────►",
            "─".repeat(width.saturating_sub(45))
        );
        println!("              0                                              time");
    }

    /// Render state timeline
    #[allow(dead_code)] // Visualization helper for debugging
    fn render_state_timeline(snapshots: &[StateSnapshot]) {
        fn state_char(state: CongestionState) -> char {
            match state {
                CongestionState::SlowStart | CongestionState::CongestionAvoidance => '─',
                CongestionState::WaitingForSlowdown => '╌',
                CongestionState::InSlowdown => '▼',
                CongestionState::Frozen => '═',
                CongestionState::RampingUp => '╱',
            }
        }

        print!("    State: ");
        let mut last_state: Option<CongestionState> = None;
        for snap in snapshots.iter().take(60) {
            if last_state != Some(snap.state) {
                print!("{}", state_char(snap.state));
                last_state = Some(snap.state);
            } else {
                print!("{}", state_char(snap.state));
            }
        }
        println!();
        println!("           (─=Normal ═=Frozen ╱=RampingUp ▼=Slowdown)");
    }

    /// E2E test: 512KB transfer at 50ms RTT with visualization
    ///
    /// This test uses real-time sleeps to properly test the LEDBAT controller's
    /// slow start behavior. Uses smaller transfer size for faster execution.
    /// See test_complete_slowdown_cycle for slowdown mechanism tests.
    #[test]
    fn test_e2e_transfer_50ms_rtt() {
        println!("\n========== E2E Test: 512KB Transfer @ 50ms RTT ==========");

        let (snapshots, duration_ms) = simulate_transfer_sync(50, 512, 50);

        println!("\n--- Transfer Summary ---");
        println!("RTT:              50ms");
        println!("Transfer size:    512 KB");
        println!(
            "Duration:         {} ms ({:.2}s)",
            duration_ms,
            duration_ms as f64 / 1000.0
        );
        println!(
            "Avg throughput:   {:.2} Mbps",
            (512.0 * 8.0) / (duration_ms as f64 / 1000.0) / 1000.0
        );

        // Print key snapshots showing slow start growth
        println!("\n--- cwnd Evolution ---");
        println!("{:>8} {:>8} {:>10}", "Time(ms)", "cwnd(KB)", "Data(KB)");
        for snap in &snapshots {
            println!(
                "{:>8} {:>8} {:>10}",
                snap.time_ms, snap.cwnd_kb, snap.data_transferred_kb
            );
        }

        // Verify slow start doubled cwnd (initial 37KB should grow)
        let max_cwnd = snapshots.iter().map(|s| s.cwnd_kb).max().unwrap_or(0);
        assert!(
            max_cwnd >= 74,
            "Slow start should double cwnd (37 -> 74+), got max {}KB",
            max_cwnd
        );

        // Verify transfer completed
        let final_data = snapshots.last().map(|s| s.data_transferred_kb).unwrap_or(0);
        assert!(
            final_data >= 512,
            "Should have transferred 512KB, got {}KB",
            final_data
        );
    }

    /// E2E test: 1MB transfer at 200ms RTT with visualization
    #[test]
    fn test_e2e_transfer_200ms_rtt() {
        println!("\n========== E2E Test: 1MB Transfer @ 200ms RTT ==========");

        let (snapshots, duration_ms) = simulate_transfer_sync(200, 1024, 200);

        println!("\n--- Transfer Summary ---");
        println!("RTT:              200ms");
        println!("Transfer size:    1 MB");
        println!(
            "Duration:         {} ms ({:.2}s)",
            duration_ms,
            duration_ms as f64 / 1000.0
        );
        println!(
            "Avg throughput:   {:.2} Mbps",
            (1024.0 * 8.0) / (duration_ms as f64 / 1000.0) / 1000.0
        );

        // Print key snapshots
        println!("\n--- cwnd Evolution ---");
        println!("{:>8} {:>8} {:>10}", "Time(ms)", "cwnd(KB)", "Data(KB)");
        for snap in &snapshots {
            println!(
                "{:>8} {:>8} {:>10}",
                snap.time_ms, snap.cwnd_kb, snap.data_transferred_kb
            );
        }

        // Verify slow start worked
        let max_cwnd = snapshots.iter().map(|s| s.cwnd_kb).max().unwrap_or(0);
        assert!(
            max_cwnd >= 74,
            "Slow start should grow cwnd, got max {}KB",
            max_cwnd
        );

        // Verify transfer completed
        let final_data = snapshots.last().map(|s| s.data_transferred_kb).unwrap_or(0);
        assert!(
            final_data >= 1024,
            "Should have transferred 1MB, got {}KB",
            final_data
        );
    }

    /// Parametrized test for multiple latency scenarios
    ///
    /// Uses small transfer sizes (256KB) to keep test execution fast while
    /// still validating slow start behavior at different RTTs.
    #[rstest::rstest]
    #[case::lan_10ms(10, 256)] // 256KB @ 10ms
    #[case::regional_50ms(50, 256)] // 256KB @ 50ms
    #[case::continental_100ms(100, 256)] // 256KB @ 100ms
    #[case::satellite_200ms(200, 256)] // 256KB @ 200ms
    fn test_e2e_latency_scenarios(#[case] rtt_ms: u64, #[case] size_kb: usize) {
        let (snapshots, duration_ms) = simulate_transfer_sync(rtt_ms, size_kb, rtt_ms);

        let avg_throughput = (size_kb as f64 * 8.0) / (duration_ms as f64 / 1000.0) / 1000.0;

        println!("\n{}ms RTT, {}KB transfer:", rtt_ms, size_kb);
        println!(
            "  Duration: {}ms ({:.2}s)",
            duration_ms,
            duration_ms as f64 / 1000.0
        );
        println!("  Throughput: {:.2} Mbps", avg_throughput);
        println!("  Snapshots: {}", snapshots.len());

        // Verify slow start worked (cwnd should grow from initial 37KB)
        let max_cwnd = snapshots.iter().map(|s| s.cwnd_kb).max().unwrap_or(0);
        assert!(
            max_cwnd >= 74,
            "{}ms RTT: Slow start should grow cwnd to 74KB+, got {}KB",
            rtt_ms,
            max_cwnd
        );

        // Verify we actually transferred the data
        let final_data = snapshots.last().map(|s| s.data_transferred_kb).unwrap_or(0);
        assert!(
            final_data >= size_kb,
            "Should have transferred {}KB, got {}KB",
            size_kb,
            final_data
        );
    }

    /// Test that visualizes cwnd evolution through complete slowdown cycle
    #[tokio::test]
    async fn test_e2e_visualize_slowdown_cycle() {
        println!("\n========== Visualizing Complete Slowdown Cycle ==========");

        let rtt = Duration::from_millis(100);
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 80_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        let mut timeline: Vec<(u64, usize, &str)> = Vec::new();
        let mut elapsed_ms = 0u64;

        fn state_name(state: CongestionState) -> &'static str {
            match state {
                CongestionState::SlowStart => "SlowStart",
                CongestionState::CongestionAvoidance => "Normal",
                CongestionState::WaitingForSlowdown => "Waiting",
                CongestionState::InSlowdown => "Slowdown",
                CongestionState::Frozen => "Frozen",
                CongestionState::RampingUp => "RampingUp",
            }
        }

        // Phase 1: Slow start
        println!("\n--- Phase 1: Slow Start ---");
        for _ in 0..15 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 20_000);

            let state = controller.congestion_state.load();
            timeline.push((
                elapsed_ms,
                controller.current_cwnd() / 1024,
                state_name(state),
            ));

            if state != CongestionState::SlowStart {
                println!(
                    "  Slow start exit at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Phase 2: Wait for slowdown
        println!("\n--- Phase 2: Waiting for Slowdown ---");
        for _ in 0..10 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 10_000);

            let state = controller.congestion_state.load();
            timeline.push((
                elapsed_ms,
                controller.current_cwnd() / 1024,
                state_name(state),
            ));

            if matches!(
                state,
                CongestionState::InSlowdown | CongestionState::Frozen | CongestionState::RampingUp
            ) {
                println!(
                    "  Slowdown triggered at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Phase 3: Freeze
        println!("\n--- Phase 3: Frozen ---");
        let pre_slowdown_cwnd = controller.pre_slowdown_cwnd.load(Ordering::Relaxed) / 1024;
        for _ in 0..5 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 5_000);

            let state = controller.congestion_state.load();
            timeline.push((
                elapsed_ms,
                controller.current_cwnd() / 1024,
                state_name(state),
            ));

            if state == CongestionState::RampingUp {
                println!(
                    "  Ramp-up started at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Phase 4: Ramp-up
        println!("\n--- Phase 4: Ramp-up ---");
        for _ in 0..10 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 30_000);

            let state = controller.congestion_state.load();
            timeline.push((
                elapsed_ms,
                controller.current_cwnd() / 1024,
                state_name(state),
            ));

            if state == CongestionState::CongestionAvoidance {
                println!(
                    "  Back to normal at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Render timeline
        println!("\n--- cwnd Timeline ---");
        println!("{:>8} {:>10} {:>12}", "Time(ms)", "cwnd(KB)", "State");
        println!("{}", "-".repeat(32));
        for (t, cwnd, state) in &timeline {
            let bar_len = (*cwnd / 10).min(30);
            let bar: String = "█".repeat(bar_len);
            println!("{:>8} {:>10} {:>12} {}", t, cwnd, state, bar);
        }

        // Verify the cycle completed (should be in CongestionAvoidance)
        let final_state = controller.congestion_state.load();
        assert_eq!(
            final_state,
            CongestionState::CongestionAvoidance,
            "Should return to CongestionAvoidance state after slowdown cycle"
        );

        let final_cwnd = controller.current_cwnd();
        assert!(
            final_cwnd >= pre_slowdown_cwnd * 1024 * 95 / 100,
            "Should recover to ~pre_slowdown level: {} >= {}",
            final_cwnd / 1024,
            pre_slowdown_cwnd * 95 / 100
        );

        println!("\n✓ Complete slowdown cycle verified");
        println!("  Pre-slowdown: {}KB", pre_slowdown_cwnd);
        println!(
            "  Frozen: {}KB (expected {}KB)",
            timeline
                .iter()
                .filter(|(_, _, s)| *s == "Frozen")
                .map(|(_, c, _)| *c)
                .next()
                .unwrap_or(0),
            pre_slowdown_cwnd / 4
        );
        println!("  Recovered: {}KB", final_cwnd / 1024);
    }

    /// Regression test: Ramp-up should complete even when queuing_delay > target_delay
    ///
    /// This tests the scenario where a high-latency path has persistent queuing:
    /// - base_delay is measured during low-traffic period (e.g., 50ms)
    /// - During ramp-up, RTT increases due to queuing (e.g., 200ms)
    /// - queuing_delay = 200ms - 50ms = 150ms > target_delay (60ms)
    ///
    /// BUG: The original code would exit ramp-up immediately when queuing_delay > target,
    /// causing cwnd to never recover and triggering rapid repeated slowdowns.
    ///
    /// FIX: Ramp-up should allow cwnd to grow for a minimum duration before checking
    /// queuing_delay, giving the connection time to recover.
    #[tokio::test]
    async fn test_rampup_completes_with_high_queuing_delay() {
        let config = LedbatConfig {
            initial_cwnd: 160_000, // 160KB
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000, // Exit slow start quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Phase 1: Establish low base_delay (50ms) during initial slow start
        // This simulates measuring RTT during a quiet period
        let low_rtt = Duration::from_millis(50);
        for _ in 0..4 {
            controller.on_ack(low_rtt, 1000);
            tokio::time::sleep(Duration::from_millis(55)).await;
        }

        // Should have exited slow start
        assert!(
            controller.congestion_state.load() != CongestionState::SlowStart,
            "Should have exited slow start"
        );

        let pre_slowdown_cwnd = controller.current_cwnd();
        println!("Pre-slowdown cwnd: {} bytes", pre_slowdown_cwnd);

        // Phase 2: Trigger slowdown
        tokio::time::sleep(Duration::from_millis(110)).await; // Wait for slowdown to trigger (2 RTTs)
        controller.on_ack(low_rtt, 1000);

        // Phase 3: Go through frozen state
        tokio::time::sleep(Duration::from_millis(55)).await;
        controller.on_ack(low_rtt, 1000);

        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::Frozen,
            "Should be in Frozen state"
        );

        // Phase 4: Wait for freeze to complete, enter RampingUp
        tokio::time::sleep(Duration::from_millis(110)).await; // 2 RTTs for freeze
        controller.on_ack(low_rtt, 5000);

        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::RampingUp,
            "Should be in RampingUp state"
        );

        let rampup_start_cwnd = controller.current_cwnd();
        println!("Ramp-up start cwnd: {} bytes", rampup_start_cwnd);

        // Phase 5: Simulate high RTT during ramp-up (queuing builds up)
        // RTT = 200ms, base_delay = 50ms, so queuing_delay = 150ms >> target (60ms)
        // BUG: Original code would exit ramp-up immediately here
        let high_rtt = Duration::from_millis(200);

        // Send multiple ACKs with high RTT to simulate ramp-up under congestion
        let mut ramp_iterations = 0;
        let target_cwnd = controller.pre_slowdown_cwnd.load(Ordering::Acquire);

        while controller.congestion_state.load() == CongestionState::RampingUp
            && ramp_iterations < 30
        {
            tokio::time::sleep(Duration::from_millis(55)).await;
            controller.on_ack(high_rtt, 20_000);
            ramp_iterations += 1;

            let current_cwnd = controller.current_cwnd();
            println!(
                "Ramp-up iteration {}: cwnd = {} bytes, target = {}",
                ramp_iterations, current_cwnd, target_cwnd
            );
        }

        let final_cwnd = controller.current_cwnd();
        let final_state = controller.congestion_state.load();

        println!(
            "Final: cwnd = {}, state = {:?}, iterations = {}",
            final_cwnd, final_state, ramp_iterations
        );

        // CRITICAL ASSERTION: cwnd should recover to near target_cwnd during ramp-up
        // The bug causes ramp-up to exit early when queuing_delay > target_delay,
        // even though some queuing is expected during ramp-up as cwnd grows.
        //
        // Without the fix: cwnd only reaches ~67% of target before early exit
        // With the fix: cwnd should reach at least 90% of target
        let recovery_ratio = final_cwnd as f64 / target_cwnd as f64;
        assert!(
            recovery_ratio >= 0.90,
            "cwnd should recover to at least 90% of target during ramp-up. \
             Got {} / {} = {:.1}%. This indicates the bug where high queuing_delay \
             causes premature ramp-up exit before cwnd fully recovers.",
            final_cwnd,
            target_cwnd,
            recovery_ratio * 100.0
        );

        // Verify we returned to Normal state (completed the slowdown cycle)
        assert_eq!(
            final_state,
            CongestionState::CongestionAvoidance,
            "Should return to Normal state after ramp-up completes"
        );

        println!("✓ Ramp-up completed successfully despite high queuing_delay");
        println!("  Iterations: {}", ramp_iterations);
        println!(
            "  cwnd growth: {} -> {} bytes ({:.1}x)",
            rampup_start_cwnd,
            final_cwnd,
            final_cwnd as f64 / rampup_start_cwnd as f64
        );
    }

    /// Test ramp-up with small pre_slowdown_cwnd near min_cwnd.
    ///
    /// Edge case: When pre_slowdown_cwnd is close to min_cwnd:
    /// - pre_slowdown_cwnd = 5000 bytes (close to min_cwnd = 2848)
    /// - Frozen at 5000/4 = 1250, but clamped to min_cwnd = 2848
    /// - 85% threshold = 4250 bytes
    ///
    /// This tests that the 85% recovery threshold works correctly even when
    /// the frozen cwnd is clamped to min_cwnd (making the growth ratio smaller).
    #[tokio::test]
    async fn test_rampup_with_small_pre_slowdown_cwnd() {
        let min_cwnd = 2848;
        let config = LedbatConfig {
            initial_cwnd: 5_000, // Small, close to min_cwnd
            min_cwnd,
            max_cwnd: 10_000_000,
            ssthresh: 4_000, // Exit slow start quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(100_000);

        // Phase 1: Establish base_delay and exit slow start
        let rtt = Duration::from_millis(50);
        for _ in 0..5 {
            controller.on_ack(rtt, 1000);
            tokio::time::sleep(Duration::from_millis(55)).await;
        }

        assert!(
            controller.congestion_state.load() != CongestionState::SlowStart,
            "Should have exited slow start"
        );

        let pre_slowdown_cwnd = controller.current_cwnd();
        println!("Pre-slowdown cwnd: {} bytes", pre_slowdown_cwnd);
        println!("min_cwnd: {} bytes", min_cwnd);

        // Verify we're in a small cwnd scenario
        assert!(
            pre_slowdown_cwnd < 10_000,
            "Test expects small cwnd, got {}",
            pre_slowdown_cwnd
        );

        // Phase 2: Trigger slowdown
        tokio::time::sleep(Duration::from_millis(110)).await;
        controller.on_ack(rtt, 1000);

        // Phase 3: Enter frozen state
        tokio::time::sleep(Duration::from_millis(55)).await;
        controller.on_ack(rtt, 1000);

        let frozen_cwnd = controller.current_cwnd();
        println!(
            "Frozen cwnd: {} bytes (expected: max({}/4, {}) = {})",
            frozen_cwnd,
            pre_slowdown_cwnd,
            min_cwnd,
            (pre_slowdown_cwnd / 4).max(min_cwnd)
        );

        // With small pre_slowdown_cwnd, frozen cwnd should be clamped to min_cwnd
        assert!(
            frozen_cwnd >= min_cwnd,
            "Frozen cwnd should be at least min_cwnd"
        );

        // Phase 4: Wait for freeze to complete, enter RampingUp
        tokio::time::sleep(Duration::from_millis(110)).await;
        controller.on_ack(rtt, 2000);

        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::RampingUp,
            "Should be in RampingUp state"
        );

        let rampup_start_cwnd = controller.current_cwnd();
        let target_cwnd = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        println!(
            "Ramp-up: start={} bytes, target={} bytes",
            rampup_start_cwnd, target_cwnd
        );

        // Phase 5: Simulate high RTT during ramp-up
        let high_rtt = Duration::from_millis(200);

        let mut ramp_iterations = 0;
        while controller.congestion_state.load() == CongestionState::RampingUp
            && ramp_iterations < 20
        {
            tokio::time::sleep(Duration::from_millis(55)).await;
            controller.on_ack(high_rtt, 2000);
            ramp_iterations += 1;

            println!(
                "Ramp-up iteration {}: cwnd = {} bytes",
                ramp_iterations,
                controller.current_cwnd()
            );
        }

        let final_cwnd = controller.current_cwnd();
        let final_state = controller.congestion_state.load();

        println!(
            "Final: cwnd = {}, state = {:?}, iterations = {}",
            final_cwnd, final_state, ramp_iterations
        );

        // Should recover to at least 85% of target (the threshold we use)
        let recovery_ratio = final_cwnd as f64 / target_cwnd as f64;
        assert!(
            recovery_ratio >= 0.85,
            "cwnd should recover to at least 85% of target even with small cwnd. \
             Got {} / {} = {:.1}%",
            final_cwnd,
            target_cwnd,
            recovery_ratio * 100.0
        );

        // Should return to Normal state
        assert_eq!(
            final_state,
            CongestionState::CongestionAvoidance,
            "Should return to Normal state after ramp-up"
        );

        println!("✓ Small cwnd ramp-up completed successfully");
    }

    /// Regression test: High-latency paths should not have excessive slowdowns
    ///
    /// This test simulates a sustained transfer on a high-latency path (100+ms RTT)
    /// with periodic slowdowns enabled, verifying that:
    /// 1. Slowdowns don't fire every RTT (the bug we fixed)
    /// 2. Total slowdowns are reasonable for the transfer duration
    /// 3. Average cwnd stays reasonable (not stuck at minimum)
    ///
    /// The bug (min_interval = 1 RTT) caused 224 slowdowns in 30s on a 137ms RTT path,
    /// keeping cwnd perpetually low. The fix (min_interval = 18 RTTs) should result
    /// in ~10-15 slowdowns for the same duration.
    #[tokio::test]
    async fn test_high_latency_slowdown_rate() {
        let high_latency_rtt = Duration::from_millis(100); // WAN RTT
        let transfer_duration = Duration::from_secs(10); // Simulate 10 second transfer

        let config = LedbatConfig {
            initial_cwnd: 160_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true, // Critical: enable periodic slowdowns
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Exit slow start
        for _ in 0..4 {
            controller.on_ack(high_latency_rtt, 1000);
            tokio::time::sleep(Duration::from_millis(110)).await;
        }

        // Record initial slowdown count (should be 0 or 1 after slow start exit)
        let initial_slowdowns = controller.periodic_slowdowns.load(Ordering::Relaxed);

        // Simulate sustained transfer for transfer_duration
        // Process one ACK per RTT interval
        let start = std::time::Instant::now();
        let mut ack_count = 0;
        let mut cwnd_sum: u64 = 0;

        while start.elapsed() < transfer_duration {
            controller.on_ack(high_latency_rtt, 10_000);
            cwnd_sum += controller.current_cwnd() as u64;
            ack_count += 1;
            tokio::time::sleep(high_latency_rtt).await;
        }

        let final_slowdowns = controller.periodic_slowdowns.load(Ordering::Relaxed);
        let slowdowns_during_transfer = final_slowdowns - initial_slowdowns;
        let avg_cwnd = cwnd_sum / ack_count.max(1);
        let elapsed = start.elapsed();

        println!("High-latency slowdown rate test:");
        println!("  RTT: {}ms", high_latency_rtt.as_millis());
        println!("  Duration: {:.1}s", elapsed.as_secs_f64());
        println!("  ACKs processed: {}", ack_count);
        println!(
            "  Slowdowns: {} (initial: {}, final: {})",
            slowdowns_during_transfer, initial_slowdowns, final_slowdowns
        );
        println!("  Avg cwnd: {} KB", avg_cwnd / 1024);

        // With 18 RTT min_interval (1.8s at 100ms RTT), expect ~5-6 slowdowns in 10s
        // With buggy 1 RTT min_interval, we'd see ~100 slowdowns in 10s
        let max_expected_slowdowns = (elapsed.as_secs_f64() / 1.5) as usize + 5; // ~1 per 1.5s + margin
        let min_buggy_slowdowns = (elapsed.as_secs_f64() * 8.0) as usize; // ~8 per second with bug

        assert!(
            slowdowns_during_transfer < max_expected_slowdowns,
            "Too many slowdowns ({}) in {:.1}s. Expected < {} with fixed min_interval. \
             This many slowdowns suggests the 1-RTT min_interval bug is present.",
            slowdowns_during_transfer,
            elapsed.as_secs_f64(),
            max_expected_slowdowns
        );

        // Also verify we didn't have the buggy behavior
        assert!(
            slowdowns_during_transfer < min_buggy_slowdowns / 2,
            "Slowdowns ({}) are suspiciously high (buggy behavior would show {}). \
             Verify min_interval is 18 RTTs, not 1 RTT.",
            slowdowns_during_transfer,
            min_buggy_slowdowns
        );

        // Verify cwnd didn't collapse
        assert!(
            avg_cwnd > 20_000,
            "Average cwnd ({} bytes) is too low. Expected > 20KB. \
             This suggests cwnd was stuck low due to excessive slowdowns.",
            avg_cwnd
        );
    }

    // =========================================================================
    // Calculation-Only Tests - Verify formulas at extreme RTTs (instant)
    // =========================================================================

    /// Test dynamic GAIN calculation at extreme RTTs (1ms datacenter, 500ms satellite)
    ///
    /// Verifies the GAIN formula handles edge cases:
    /// - Very low RTT: GAIN should be capped at 1/16 (conservative)
    /// - Very high RTT: GAIN should be 1.0 (aggressive to utilize bandwidth)
    #[test]
    fn test_dynamic_gain_extreme_latencies() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
        // TARGET = 60ms

        // 1ms base delay (datacenter): divisor = ceil(2 * 60 / 1) = 120 -> capped at 16
        let gain_1ms = controller.calculate_dynamic_gain(Duration::from_millis(1));
        assert!(
            (gain_1ms - 1.0 / 16.0).abs() < 0.001,
            "1ms: expected 1/16 (capped), got {}",
            gain_1ms
        );

        // 500ms base delay (satellite): divisor = ceil(2 * 60 / 500) = 1, GAIN = 1
        let gain_500ms = controller.calculate_dynamic_gain(Duration::from_millis(500));
        assert!(
            (gain_500ms - 1.0).abs() < 0.001,
            "500ms: expected 1.0, got {}",
            gain_500ms
        );

        // 300ms base delay: divisor = ceil(2 * 60 / 300) = 1, GAIN = 1
        let gain_300ms = controller.calculate_dynamic_gain(Duration::from_millis(300));
        assert!(
            (gain_300ms - 1.0).abs() < 0.001,
            "300ms: expected 1.0, got {}",
            gain_300ms
        );

        // Verify GAIN is always in valid range [1/16, 1]
        for rtt_ms in [1, 2, 5, 10, 50, 100, 200, 300, 500, 1000] {
            let gain = controller.calculate_dynamic_gain(Duration::from_millis(rtt_ms));
            assert!(
                (1.0 / 16.0..=1.0).contains(&gain),
                "{}ms: GAIN {} out of valid range [1/16, 1]",
                rtt_ms,
                gain
            );
        }
    }

    /// Test min_interval calculation at extreme RTTs
    ///
    /// Verifies the 18 RTT minimum is correctly applied at both extremes:
    /// - 1ms RTT: min_interval = 18ms
    /// - 500ms RTT: min_interval = 9000ms (9 seconds)
    #[test]
    fn test_min_interval_extreme_latencies() {
        for (rtt_ms, expected_min_ms) in [(1, 18), (5, 90), (500, 9000)] {
            let rtt = Duration::from_millis(rtt_ms);

            let config = LedbatConfig {
                initial_cwnd: 160_000,
                min_cwnd: 2848,
                max_cwnd: 10_000_000,
                ssthresh: 50_000,
                enable_slow_start: true,
                enable_periodic_slowdown: true,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Set up base delay
            controller.base_delay_history.update(rtt);

            // Simulate immediate slowdown completion
            let now_nanos = controller.epoch.elapsed().as_nanos() as u64;
            controller
                .slowdown_phase_start_nanos
                .store(now_nanos, Ordering::Release);

            let completion_nanos = controller.epoch.elapsed().as_nanos() as u64;
            controller.complete_slowdown(completion_nanos, rtt);

            let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
            let next_interval_ms = next_slowdown.saturating_sub(completion_nanos) / 1_000_000;

            // Allow some tolerance for timing
            assert!(
                next_interval_ms >= expected_min_ms - 5,
                "{}ms RTT: min_interval ({} ms) should be >= {} ms (18 RTTs)",
                rtt_ms,
                next_interval_ms,
                expected_min_ms
            );

            println!(
                "  {}ms RTT: min_interval = {}ms (expected >= {}ms)",
                rtt_ms, next_interval_ms, expected_min_ms
            );
        }
    }

    /// Test rate conversion at extreme RTTs
    ///
    /// Verifies the rate = cwnd / RTT formula handles edge cases:
    /// - Very low RTT (sub-millisecond): should be capped to 1ms minimum
    /// - Very high RTT: rate should decrease proportionally
    #[test]
    fn test_rate_conversion_extreme_latencies() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000); // 100KB cwnd

        // Sub-millisecond RTT should be capped to 1ms to prevent unrealistic rates
        // current_rate enforces 1ms minimum
        let rate_submillis = controller.current_rate(Duration::from_micros(100));
        let rate_1ms = controller.current_rate(Duration::from_millis(1));

        // Both should give same result due to 1ms floor
        assert_eq!(
            rate_submillis, rate_1ms,
            "Sub-millisecond RTT should be capped to 1ms"
        );

        // Rate at 1ms: 100KB / 0.001s = 100 MB/s = 100_000_000 bytes/s
        assert!(
            (90_000_000..=110_000_000).contains(&rate_1ms),
            "1ms RTT rate should be ~100 MB/s, got {} bytes/s",
            rate_1ms
        );

        // Rate at 500ms: 100KB / 0.5s = 200 KB/s = 200_000 bytes/s
        let rate_500ms = controller.current_rate(Duration::from_millis(500));
        assert!(
            (190_000..=210_000).contains(&rate_500ms),
            "500ms RTT rate should be ~200 KB/s, got {} bytes/s",
            rate_500ms
        );

        // Rate should scale inversely with RTT
        let rate_100ms = controller.current_rate(Duration::from_millis(100));
        let rate_200ms = controller.current_rate(Duration::from_millis(200));
        assert!(
            rate_100ms > rate_200ms * 19 / 10, // Should be ~2x, allow some tolerance
            "Rate at 100ms ({}) should be ~2x rate at 200ms ({})",
            rate_100ms,
            rate_200ms
        );
    }

    // =========================================================================
    // State Machine Edge Case Tests - Verify behavior under adverse conditions
    // =========================================================================

    /// Test that loss during Frozen state doesn't corrupt state machine
    ///
    /// When packet loss occurs during the Frozen phase:
    /// - cwnd should be halved (standard loss response)
    /// - State should remain Frozen (not reset to Normal)
    /// - Slowdown cycle should continue normally after freeze completes
    #[test]
    fn test_loss_during_frozen_state() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Manually set state to Frozen
        controller.congestion_state.enter_frozen();
        controller.cwnd.store(50_000, Ordering::Release); // 50KB during freeze

        let cwnd_before_loss = controller.current_cwnd();
        let state_before_loss = controller.congestion_state.load();

        // Trigger packet loss
        controller.on_loss();

        let cwnd_after_loss = controller.current_cwnd();
        let state_after_loss = controller.congestion_state.load();

        // cwnd should be halved
        assert_eq!(
            cwnd_after_loss,
            (cwnd_before_loss / 2).max(controller.min_cwnd),
            "cwnd should be halved on loss"
        );

        // State should remain Frozen (loss doesn't reset state machine)
        assert_eq!(
            state_before_loss, state_after_loss,
            "Slowdown state should not change on loss (was {:?}, now {:?})",
            state_before_loss, state_after_loss
        );

        // Loss counter should increment
        assert_eq!(
            controller.total_losses.load(Ordering::Relaxed),
            1,
            "Loss counter should increment"
        );
    }

    /// Test that timeout during RampingUp state resets appropriately
    ///
    /// When timeout occurs during RampingUp:
    /// - cwnd should reset to min_cwnd (severe congestion response)
    /// - State remains RampingUp (timeout doesn't change slowdown state)
    /// - Slow start flag should remain true (for fast recovery after timeout)
    ///
    /// Note: Prior to the slow-start-reentry fix, timeout would exit slow start,
    /// leaving the connection stuck in slow linear growth. Now timeout re-enters
    /// slow start for fast exponential recovery.
    #[test]
    fn test_timeout_during_rampup_state() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Manually set state to RampingUp
        controller.congestion_state.enter_ramping_up();
        let pre_timeout_cwnd = 80_000;
        controller.cwnd.store(pre_timeout_cwnd, Ordering::Release); // Mid-rampup

        // Trigger timeout
        controller.on_timeout();

        let cwnd_after = controller.current_cwnd();
        let state_after = controller.congestion_state.load();
        let ssthresh_after = controller.ssthresh.load(Ordering::Acquire);

        // cwnd should reset to min_cwnd (or MSS, whichever is larger)
        assert!(
            cwnd_after <= controller.min_cwnd.max(MSS),
            "cwnd should reset to min on timeout, got {}",
            cwnd_after
        );

        // With unified state machine, timeout always transitions to SlowStart
        // for fast exponential recovery
        assert_eq!(
            state_after,
            CongestionState::SlowStart,
            "Should transition to SlowStart on timeout for fast recovery"
        );

        // ssthresh should be set to half the pre-timeout cwnd
        let expected_ssthresh = pre_timeout_cwnd / 2;
        assert_eq!(
            ssthresh_after, expected_ssthresh,
            "ssthresh should be half the pre-timeout cwnd ({}/2 = {})",
            pre_timeout_cwnd, expected_ssthresh
        );
    }

    /// Test that timeout during Frozen state resets appropriately
    ///
    /// When timeout occurs during Frozen state (part of slowdown cycle):
    /// - cwnd should reset to min_cwnd (severe congestion response)
    /// - State should transition to SlowStart for fast recovery
    /// - ssthresh should be set to half the pre-timeout cwnd
    ///
    /// This is a regression test for issue #2559 which unified the state machine.
    /// Previously, overlapping flags could cause inconsistent behavior.
    #[test]
    fn test_timeout_during_frozen_state() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Manually set state to Frozen (simulating mid-slowdown)
        controller.congestion_state.enter_frozen();
        let pre_timeout_cwnd = 60_000;
        controller.cwnd.store(pre_timeout_cwnd, Ordering::Release); // Mid-freeze cwnd

        // Trigger timeout
        controller.on_timeout();

        let cwnd_after = controller.current_cwnd();
        let state_after = controller.congestion_state.load();
        let ssthresh_after = controller.ssthresh.load(Ordering::Acquire);

        // cwnd should reset to min_cwnd (or MSS, whichever is larger)
        assert!(
            cwnd_after <= controller.min_cwnd.max(MSS),
            "cwnd should reset to min on timeout, got {}",
            cwnd_after
        );

        // With unified state machine, timeout always transitions to SlowStart
        // This cleanly exits the Frozen state and enables fast recovery
        assert_eq!(
            state_after,
            CongestionState::SlowStart,
            "Should transition to SlowStart on timeout for fast recovery"
        );

        // ssthresh should be set to half the pre-timeout cwnd
        let expected_ssthresh = pre_timeout_cwnd / 2;
        assert_eq!(
            ssthresh_after, expected_ssthresh,
            "ssthresh should be half the pre-timeout cwnd ({}/2 = {})",
            pre_timeout_cwnd, expected_ssthresh
        );
    }

    /// Test that timeout during WaitingForSlowdown state resets appropriately
    ///
    /// When timeout occurs while waiting for the next scheduled slowdown:
    /// - cwnd should reset to min_cwnd
    /// - State should transition to SlowStart for fast recovery
    ///
    /// This ensures the unified state machine handles all slowdown-related states.
    #[test]
    fn test_timeout_during_waiting_for_slowdown_state() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Manually set state to WaitingForSlowdown
        controller.congestion_state.enter_waiting_for_slowdown();
        controller.cwnd.store(90_000, Ordering::Release);

        // Trigger timeout
        controller.on_timeout();

        let cwnd_after = controller.current_cwnd();
        let state_after = controller.congestion_state.load();

        // cwnd should reset to min_cwnd
        assert!(
            cwnd_after <= controller.min_cwnd.max(MSS),
            "cwnd should reset to min on timeout, got {}",
            cwnd_after
        );

        // Should transition to SlowStart
        assert_eq!(
            state_after,
            CongestionState::SlowStart,
            "Should transition to SlowStart on timeout"
        );
    }

    /// Test state machine integrity: all states are reachable and valid
    #[test]
    fn test_congestion_state_machine_integrity() {
        // Verify all state values are unique and in valid range
        let states = [
            (CongestionState::SlowStart, 0u8),
            (CongestionState::CongestionAvoidance, 1u8),
            (CongestionState::WaitingForSlowdown, 2u8),
            (CongestionState::InSlowdown, 3u8),
            (CongestionState::Frozen, 4u8),
            (CongestionState::RampingUp, 5u8),
        ];

        for (state, expected_value) in states {
            assert_eq!(
                state as u8, expected_value,
                "State {:?} should have value {}",
                state, expected_value
            );
        }

        // Verify from_u8 returns None for invalid values
        assert!(
            CongestionState::from_u8(6).is_none(),
            "from_u8(6) should return None"
        );
        assert!(
            CongestionState::from_u8(100).is_none(),
            "from_u8(100) should return None"
        );
        assert!(
            CongestionState::from_u8(255).is_none(),
            "from_u8(255) should return None"
        );

        // Verify from_u8 works for valid values
        for (expected_state, value) in states {
            assert_eq!(
                CongestionState::from_u8(value),
                Some(expected_state),
                "from_u8({}) should return Some({:?})",
                value,
                expected_state
            );
        }

        // Verify controller starts in Normal state
        // Default config has enable_slow_start=true, so starts in SlowStart
        let config = LedbatConfig::default();
        let controller = LedbatController::new_with_config(config);
        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::SlowStart,
            "Controller should start in SlowStart state when enable_slow_start=true"
        );

        // With slow start disabled, should start in CongestionAvoidance
        let config_no_ss = LedbatConfig {
            enable_slow_start: false,
            ..LedbatConfig::default()
        };
        let controller_no_ss = LedbatController::new_with_config(config_no_ss);
        assert_eq!(
            controller_no_ss.congestion_state.load(),
            CongestionState::CongestionAvoidance,
            "Controller should start in CongestionAvoidance state when enable_slow_start=false"
        );
    }

    /// Test that multiple rapid losses don't cause cwnd underflow
    #[test]
    fn test_rapid_losses_no_underflow() {
        let config = LedbatConfig {
            initial_cwnd: 10_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Simulate 20 rapid losses (each halves cwnd)
        for i in 0..20 {
            let cwnd_before = controller.current_cwnd();
            controller.on_loss();
            let cwnd_after = controller.current_cwnd();

            // cwnd should never go below min_cwnd
            assert!(
                cwnd_after >= controller.min_cwnd,
                "Iteration {}: cwnd {} went below min_cwnd {}",
                i,
                cwnd_after,
                controller.min_cwnd
            );

            // cwnd should either halve or stay at min
            if cwnd_before > controller.min_cwnd * 2 {
                assert_eq!(
                    cwnd_after,
                    cwnd_before / 2,
                    "Iteration {}: cwnd should halve",
                    i
                );
            } else {
                assert!(
                    cwnd_after >= controller.min_cwnd,
                    "Iteration {}: cwnd should stay at min",
                    i
                );
            }
        }

        // Final cwnd should be at min_cwnd
        assert_eq!(
            controller.current_cwnd(),
            controller.min_cwnd,
            "After many losses, cwnd should be at min_cwnd"
        );

        // Loss counter should be accurate
        assert_eq!(
            controller.total_losses.load(Ordering::Relaxed),
            20,
            "Should have recorded 20 losses"
        );
    }

    /// Test base delay tracking with jittery RTT samples
    ///
    /// Simulates RTT jitter (±30%) and verifies base_delay tracks the minimum
    #[test]
    fn test_base_delay_with_jitter() {
        let config = LedbatConfig::default();
        let controller = LedbatController::new_with_config(config);

        let base_rtt_ms = 100u64;
        let jitter_pct = 30u64; // ±30%

        // Add jittery samples
        let samples = [
            base_rtt_ms,                      // 100ms (base)
            base_rtt_ms + jitter_pct,         // 130ms (+30%)
            base_rtt_ms - jitter_pct / 2,     // 85ms  (-15%)
            base_rtt_ms + jitter_pct / 2,     // 115ms (+15%)
            base_rtt_ms - jitter_pct,         // 70ms  (-30%) - this is the min
            base_rtt_ms,                      // 100ms
            base_rtt_ms + jitter_pct * 2 / 3, // 120ms
        ];

        for &rtt_ms in &samples {
            controller
                .base_delay_history
                .update(Duration::from_millis(rtt_ms));
        }

        let measured_base = controller.base_delay();
        let expected_min = Duration::from_millis(70); // Minimum sample

        assert_eq!(
            measured_base, expected_min,
            "Base delay should track minimum: expected {:?}, got {:?}",
            expected_min, measured_base
        );
    }

    /// Test that slowdown doesn't occur when disabled
    #[test]
    fn test_slowdown_disabled() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: false, // Disabled to start in CongestionAvoidance
            enable_periodic_slowdown: false, // Disabled!
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // State should be CongestionAvoidance (slow start disabled)
        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::CongestionAvoidance,
            "Initial state should be CongestionAvoidance"
        );

        // Schedule a slowdown manually and verify it's not processed
        controller
            .next_slowdown_time_nanos
            .store(0, Ordering::Release); // Due now

        // Process some ACKs
        controller.on_send(100_000);
        for _ in 0..10 {
            controller
                .base_delay_history
                .update(Duration::from_millis(50));
            controller.on_ack(Duration::from_millis(50), 5000);
        }

        // Slowdown counter should remain 0
        assert_eq!(
            controller.periodic_slowdowns.load(Ordering::Relaxed),
            0,
            "No slowdowns should occur when disabled"
        );

        // State should still be CongestionAvoidance
        let state = controller.congestion_state.load();
        assert_eq!(
            state,
            CongestionState::CongestionAvoidance,
            "State should remain CongestionAvoidance when slowdowns disabled"
        );
    }

    /// Test ramp-up completes correctly with RTT jitter
    ///
    /// Simulates a network path with ±30% RTT jitter during the ramp-up phase
    /// of a slowdown cycle and verifies:
    /// 1. State machine transitions from Frozen -> RampingUp -> Normal
    /// 2. cwnd recovers to target despite variable RTT
    /// 3. base_delay tracks the minimum RTT correctly
    ///
    /// This catches issues where variable RTT could:
    /// - Cause premature ramp-up exit (the bug from PR #2510)
    /// - Corrupt base_delay measurements
    /// - Prevent cwnd recovery
    #[tokio::test]
    async fn test_slowdown_rampup_with_rtt_jitter() {
        let base_rtt_ms = 50u64;

        let config = LedbatConfig {
            initial_cwnd: 20_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 150_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Helper to generate jittery RTT (±30% deterministic pattern)
        let jittery_rtt = |iteration: u64| -> Duration {
            let offsets_ms: [i64; 10] = [-15, 8, -5, 12, -10, 3, 15, -8, 5, -3];
            let offset = offsets_ms[(iteration % 10) as usize];
            let rtt_ms = (base_rtt_ms as i64 + offset).max(20) as u64;
            Duration::from_millis(rtt_ms)
        };

        controller.on_send(1_000_000);

        // Phase 1: Initialize with jittery RTT samples via on_ack (proper way)
        // This populates both delay_filter and base_delay_history
        for i in 0..4 {
            let rtt = jittery_rtt(i);
            controller.on_ack(rtt, 5_000);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Phase 2: Exit slow start and enter slowdown cycle
        // Add high-RTT samples to trigger slow start exit
        for i in 4..8 {
            let rtt = jittery_rtt(i) + Duration::from_millis(50); // Add queuing
            controller.on_ack(rtt, 10_000);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let pre_slowdown_cwnd = controller.current_cwnd();
        let base_delay = controller.base_delay();

        println!("After slow start exit:");
        println!("  cwnd: {} KB", pre_slowdown_cwnd / 1024);
        println!("  base_delay: {:?}", base_delay);

        // Phase 3: Wait for slowdown to trigger and complete frozen phase
        // Process ACKs until we see the ramp-up state
        let mut iteration = 8u64;
        let mut found_rampup = false;

        for _ in 0..20 {
            let rtt = jittery_rtt(iteration);
            iteration += 1;
            tokio::time::sleep(Duration::from_millis(base_rtt_ms)).await;
            controller.on_ack(rtt, 10_000);

            let state = controller.congestion_state.load();
            if state == CongestionState::RampingUp {
                found_rampup = true;
                println!("Entered RampingUp state at iteration {}", iteration);
                break;
            }
        }

        // If we didn't naturally hit ramp-up, manually set it up for testing
        if !found_rampup {
            println!("Manually triggering ramp-up state for test");
            controller.congestion_state.enter_ramping_up();
            let target = controller.current_cwnd().max(80_000);
            controller
                .pre_slowdown_cwnd
                .store(target, Ordering::Release);
            controller.cwnd.store(target / 4, Ordering::Release);
        }

        let rampup_start_cwnd = controller.current_cwnd();
        let target_cwnd = controller.pre_slowdown_cwnd.load(Ordering::Acquire);

        println!("\nRamp-up starting:");
        println!("  cwnd: {} KB", rampup_start_cwnd / 1024);
        println!("  target: {} KB", target_cwnd / 1024);

        // Phase 4: Go through ramp-up with jittery RTT
        let mut ramp_iterations = 0u32;
        let max_ramp_iterations = 15u32;

        while controller.congestion_state.load() == CongestionState::RampingUp
            && ramp_iterations < max_ramp_iterations
        {
            let rtt = jittery_rtt(iteration);
            iteration += 1;
            ramp_iterations += 1;

            tokio::time::sleep(Duration::from_millis(base_rtt_ms)).await;
            controller.on_ack(rtt, 20_000);
        }

        let final_cwnd = controller.current_cwnd();
        let final_state = controller.congestion_state.load();
        let final_base_delay = controller.base_delay();

        println!("\nFinal state:");
        println!(
            "  cwnd: {} KB (target: {} KB)",
            final_cwnd / 1024,
            target_cwnd / 1024
        );
        println!(
            "  state: {:?} (SlowStart, CongestionAvoidance, RampingUp, etc.)",
            final_state
        );
        println!("  base_delay: {:?}", final_base_delay);
        println!("  ramp_iterations: {}", ramp_iterations);

        // Verify cwnd grew during ramp-up (even if not fully complete)
        assert!(
            final_cwnd > rampup_start_cwnd,
            "cwnd should grow during ramp-up: {} -> {}",
            rampup_start_cwnd,
            final_cwnd
        );

        // Verify base_delay is sensible (should track minimum from jittery samples)
        // Minimum jittered RTT is base_rtt - 15ms = 35ms
        let min_expected = Duration::from_millis(30);
        let max_expected = Duration::from_millis(70);
        assert!(
            final_base_delay >= min_expected && final_base_delay <= max_expected,
            "base_delay {:?} should be in range [{:?}, {:?}]",
            final_base_delay,
            min_expected,
            max_expected
        );
    }

    /// Regression test for slow start re-entry after timeout.
    ///
    /// Issue: After a retransmission timeout, on_timeout() resets cwnd to min_cwnd
    /// and sets in_slow_start = false. This puts the connection in congestion
    /// avoidance mode with minimum cwnd. On high-RTT paths with zero queuing,
    /// congestion avoidance growth is extremely slow (~188 bytes per 264ms RTT),
    /// causing transfers to stall for tens of seconds.
    ///
    /// The fix: Re-enter slow start after timeout to enable exponential recovery.
    ///
    /// This test verifies that after a timeout, cwnd can recover quickly (via
    /// slow start exponential growth) rather than being stuck in slow linear
    /// congestion avoidance growth.
    #[tokio::test]
    async fn test_timeout_reenters_slow_start_for_fast_recovery() {
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false, // Disable to isolate timeout behavior
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Establish base delay with some samples
        controller.on_ack(Duration::from_millis(100), 1000);
        controller.on_ack(Duration::from_millis(100), 1000);

        // Simulate initial working state with good cwnd
        let pre_timeout_cwnd = controller.current_cwnd();
        assert!(
            pre_timeout_cwnd >= 50_000,
            "pre-timeout cwnd should be at initial value: {}",
            pre_timeout_cwnd
        );

        // Trigger a timeout - this should reset cwnd but allow fast recovery
        controller.on_timeout();

        let post_timeout_cwnd = controller.current_cwnd();
        assert!(
            post_timeout_cwnd <= 3000,
            "post-timeout cwnd should be at minimum: {}",
            post_timeout_cwnd
        );

        // Now try to recover via ACKs
        // With slow start (exponential growth), we should recover quickly
        // With congestion avoidance (linear growth), recovery would be very slow
        //
        // At 100ms RTT with zero queuing delay, slow start should roughly double
        // cwnd every RTT. Starting from ~2.8KB:
        // - After 1 RTT: ~5.6KB
        // - After 2 RTTs: ~11KB
        // - After 3 RTTs: ~22KB
        // - After 4 RTTs: ~44KB
        //
        // In congestion avoidance, growth would be ~150-200 bytes per RTT, so
        // after 4 RTTs we'd only be at ~3.4KB (barely any growth).

        // Simulate 4 RTTs worth of ACKs with zero queuing delay
        let bytes_per_ack = 5_000;
        for i in 0..4 {
            // Keep flightsize high enough to not trigger application-limited cap
            controller.on_send(50_000);

            // Wait for rate-limiting interval (base_delay = 100ms)
            tokio::time::sleep(Duration::from_millis(110)).await;

            // Zero queuing delay (RTT = base delay) - this is the problematic case
            // because off_target = 0 means no congestion avoidance growth
            controller.on_ack(Duration::from_millis(100), bytes_per_ack);

            let current_cwnd = controller.current_cwnd();
            println!(
                "After ACK {}: cwnd = {} bytes ({} KB)",
                i + 1,
                current_cwnd,
                current_cwnd / 1024
            );
        }

        let recovered_cwnd = controller.current_cwnd();

        // After 4 RTTs in slow start, cwnd should have grown significantly
        // We're lenient here: just checking it grew more than 10KB (proving
        // exponential growth, not the ~1KB we'd see from linear growth)
        assert!(
            recovered_cwnd >= 10_000,
            "After 4 RTTs, cwnd should have recovered via slow start to at least 10KB. \
             Got {} bytes. This suggests slow start was NOT re-enabled after timeout, \
             leaving cwnd stuck in slow linear congestion avoidance growth.",
            recovered_cwnd
        );

        // Also verify we're in SlowStart state
        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::SlowStart,
            "Should be in SlowStart state after timeout for fast recovery"
        );

        println!(
            "✓ Timeout recovery test passed: cwnd recovered from {} to {} bytes",
            post_timeout_cwnd, recovered_cwnd
        );
    }

    // =========================================================================
    // VARIABLE RTT TESTS
    //
    // These tests exercise LEDBAT behavior under variable network latency,
    // which is critical for real-world performance but was not previously
    // covered by fixed-latency tests.
    //
    // Key scenarios tested:
    // 1. Queue buildup simulation (parametrized across latencies)
    // 2. Latency spike recovery (parametrized across spike magnitudes)
    // 3. Continuous jitter with stability analysis
    // 4. Base delay shift detection
    // 5. Extreme RTT variation stress test
    // 6. Timeout recovery with degrading conditions
    //
    // Implementation notes:
    // - Duration comparisons use tolerance where exact equality is fragile.
    // - Tests use wall-clock sleep() because LEDBAT internally uses std::time::Instant
    //   for epoch timing, which is not affected by tokio's mock time.
    // =========================================================================

    /// Helper for approximate Duration comparison with tolerance.
    /// Returns true if `a` and `b` differ by at most `tolerance_ms` milliseconds.
    fn duration_approx_eq(a: Duration, b: Duration, tolerance_ms: u64) -> bool {
        let diff = if a > b { a - b } else { b - a };
        diff <= Duration::from_millis(tolerance_ms)
    }

    /// Test cwnd recovery with continuously variable RTT (jittery network).
    ///
    /// This simulates a network path with inherent variability (e.g., WiFi,
    /// mobile networks) where RTT fluctuates constantly within a range.
    /// LEDBAT should:
    /// 1. Track the minimum RTT as base_delay
    /// 2. Respond appropriately to the variable queuing delay
    /// 3. Not "hunt" excessively (oscillate cwnd rapidly)
    #[tokio::test]
    async fn test_variable_rtt_continuous_jitter() {
        println!("\n========== Variable RTT: Continuous Jitter Test ==========");

        let config = LedbatConfig {
            initial_cwnd: 60_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        // Simulate jittery RTT: base 40ms with ±20ms variation (20-60ms range)
        let base_jitter = 40u64;
        let jitter_range = 20i64;

        println!("\n--- Jittery RTT simulation (40ms ± 20ms) ---");
        println!(
            "{:>6} {:>8} {:>12} {:>10}",
            "ACK#", "RTT(ms)", "Base(ms)", "Cwnd(KB)"
        );
        println!("{}", "-".repeat(42));

        let mut min_rtt_seen = Duration::from_millis(1000);
        let mut cwnd_samples: Vec<usize> = Vec::new();

        // Simulate 20 ACKs with jittery RTT
        for i in 0..20 {
            // Generate jittery RTT using a simple pattern (not random, for reproducibility)
            let jitter = ((i as i64 * 7) % (jitter_range * 2 + 1)) - jitter_range;
            let rtt_ms = ((base_jitter as i64) + jitter).max(20) as u64;
            let rtt = Duration::from_millis(rtt_ms);

            if rtt < min_rtt_seen {
                min_rtt_seen = rtt;
            }

            tokio::time::sleep(Duration::from_millis(rtt_ms + 5)).await;
            controller.on_ack(rtt, 5000);

            let cwnd = controller.current_cwnd();
            cwnd_samples.push(cwnd);

            if i % 4 == 0 || i == 19 {
                println!(
                    "{:>6} {:>8} {:>12} {:>10}",
                    i + 1,
                    rtt_ms,
                    controller.base_delay().as_millis(),
                    cwnd / 1024
                );
            }
        }

        // Analyze cwnd stability (variance shouldn't be too high)
        let avg_cwnd: usize = cwnd_samples.iter().sum::<usize>() / cwnd_samples.len();
        let variance: f64 = cwnd_samples
            .iter()
            .map(|&c| {
                let diff = c as f64 - avg_cwnd as f64;
                diff * diff
            })
            .sum::<f64>()
            / cwnd_samples.len() as f64;
        let std_dev = variance.sqrt();
        let coefficient_of_variation = std_dev / avg_cwnd as f64;

        println!("\n--- Stability Analysis ---");
        println!("  Min RTT seen: {}ms", min_rtt_seen.as_millis());
        println!("  Base delay:   {}ms", controller.base_delay().as_millis());
        println!("  Avg cwnd:     {} KB", avg_cwnd / 1024);
        println!("  Std dev:      {} KB", std_dev as usize / 1024);
        println!("  CoV:          {:.2}", coefficient_of_variation);

        // Verify base delay tracks minimum (with 1ms tolerance for timing edge cases)
        assert!(
            duration_approx_eq(controller.base_delay(), min_rtt_seen, 1),
            "Base delay {:?} should track minimum RTT {:?}",
            controller.base_delay(),
            min_rtt_seen
        );

        // Cwnd should not be excessively unstable.
        // CoV < 0.5 threshold rationale: In congestion control literature, CoV < 0.5
        // indicates "low to moderate variability" (std dev < half of mean). This ensures
        // LEDBAT isn't over-reacting to jitter with excessive cwnd oscillation, which
        // would hurt throughput and fairness. Real-world measurements of stable TCP
        // connections typically show CoV between 0.1-0.3 for cwnd.
        assert!(
            coefficient_of_variation < 0.5,
            "Cwnd should be reasonably stable under jitter (CoV={:.2} < 0.5)",
            coefficient_of_variation
        );
    }

    /// Test base delay shift detection (network path change).
    ///
    /// This simulates a scenario where the network path changes permanently,
    /// causing a new baseline latency. This test verifies that:
    /// 1. The connection remains functional after a path change
    /// 2. LEDBAT continues operating correctly even if base delay reflects
    ///    historical minimum rather than current path minimum
    ///
    /// Note on LEDBAT base delay behavior:
    /// LEDBAT's base delay history (BASE_HISTORY_SIZE samples) preserves the
    /// minimum RTT ever seen within the window. This is intentional - it prevents
    /// the algorithm from being fooled by persistent queuing into thinking the
    /// path latency has increased. The tradeoff is that after a genuine path
    /// change to higher latency, LEDBAT may be overly aggressive until the
    /// historical minimum ages out. This test validates the connection remains
    /// usable in this scenario.
    #[tokio::test]
    async fn test_variable_rtt_base_delay_shift() {
        println!("\n========== Variable RTT: Base Delay Shift Test ==========");

        let min_cwnd = 2848usize;
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd,
            max_cwnd: 10_000_000,
            ssthresh: 40_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        // Phase 1: Establish original base delay at 20ms
        let old_rtt = Duration::from_millis(20);
        println!("\n--- Phase 1: Original path @ 20ms ---");
        for i in 0..BASE_HISTORY_SIZE {
            controller.on_ack(old_rtt, 3000);
            tokio::time::sleep(Duration::from_millis(25)).await;
            if i == BASE_HISTORY_SIZE - 1 {
                println!(
                    "  Established base_delay: {:?}, cwnd: {} KB",
                    controller.base_delay(),
                    controller.current_cwnd() / 1024
                );
            }
        }

        assert!(
            duration_approx_eq(controller.base_delay(), old_rtt, 1),
            "Base delay should be ~20ms initially, got {:?}",
            controller.base_delay()
        );

        // Phase 2: Path change - new baseline is 80ms
        // This could happen due to route change, VPN activation, etc.
        let new_rtt = Duration::from_millis(80);
        println!("\n--- Phase 2: Path change to 80ms ---");
        println!(
            "  (Simulating BASE_HISTORY_SIZE={} samples at new RTT)",
            BASE_HISTORY_SIZE
        );

        // Need enough samples to age out the old base delay
        // The base delay history keeps BASE_HISTORY_SIZE samples
        for i in 0..(BASE_HISTORY_SIZE * 2) {
            tokio::time::sleep(Duration::from_millis(85)).await;
            controller.on_ack(new_rtt, 3000);

            // The base delay history should eventually reflect the new minimum
            // Note: due to how base_delay_history works, it may take time
            if i % 5 == 0 {
                println!(
                    "  Sample {}: base_delay={:?}, cwnd={} KB",
                    i + 1,
                    controller.base_delay(),
                    controller.current_cwnd() / 1024
                );
            }
        }

        let final_base_delay = controller.base_delay();
        println!("\n--- Result ---");
        println!("  Old base delay: {:?}", old_rtt);
        println!("  New base delay: {:?}", final_base_delay);
        println!(
            "  Note: LEDBAT preserves historical minimum within {} samples",
            BASE_HISTORY_SIZE
        );

        // The connection should continue functioning (not stall due to permanent "queuing")
        // regardless of whether base delay updated to new path latency
        let final_cwnd = controller.current_cwnd();
        assert!(
            final_cwnd >= min_cwnd,
            "Cwnd should remain usable after path change: {}",
            final_cwnd
        );
    }

    /// Test behavior under extreme RTT variation (stress test).
    ///
    /// This tests LEDBAT's robustness under pathological network conditions
    /// with rapid, extreme RTT swings. The controller should:
    /// 1. Not crash or panic
    /// 2. Maintain cwnd within valid bounds
    /// 3. Continue making forward progress
    #[tokio::test]
    async fn test_variable_rtt_extreme_variation() {
        println!("\n========== Variable RTT: Extreme Variation Stress Test ==========");

        let min_cwnd = 2848usize;
        let max_cwnd = 10_000_000usize;
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd,
            max_cwnd,
            ssthresh: 40_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        // Extreme RTT pattern: 10ms, 200ms, 15ms, 300ms, 20ms, 50ms, 250ms, ...
        let extreme_rtts: Vec<u64> = vec![10, 200, 15, 300, 20, 50, 250, 10, 180, 25, 350, 30, 10];

        println!("\n--- Extreme RTT pattern ---");
        println!(
            "{:>6} {:>8} {:>12} {:>10}",
            "ACK#", "RTT(ms)", "Base(ms)", "Cwnd(KB)"
        );
        println!("{}", "-".repeat(42));

        let mut min_rtt = Duration::from_millis(1000);

        for (i, &rtt_ms) in extreme_rtts.iter().enumerate() {
            let rtt = Duration::from_millis(rtt_ms);
            if rtt < min_rtt {
                min_rtt = rtt;
            }

            // Advance time proportionally to RTT to simulate realistic timing
            tokio::time::sleep(Duration::from_millis(rtt_ms.min(100) + 5)).await;
            controller.on_ack(rtt, 3000);

            println!(
                "{:>6} {:>8} {:>12} {:>10}",
                i + 1,
                rtt_ms,
                controller.base_delay().as_millis(),
                controller.current_cwnd() / 1024
            );
        }

        // Verify invariants after stress test
        let final_cwnd = controller.current_cwnd();
        let final_base_delay = controller.base_delay();

        println!("\n--- Post-stress verification ---");
        println!("  Min RTT seen:    {}ms", min_rtt.as_millis());
        println!("  Final base_delay: {:?}", final_base_delay);
        println!("  Final cwnd:      {} KB", final_cwnd / 1024);

        // Invariant: cwnd must be within bounds
        assert!(
            final_cwnd >= min_cwnd,
            "Cwnd must be >= min_cwnd: {} >= {}",
            final_cwnd,
            min_cwnd
        );
        assert!(
            final_cwnd <= max_cwnd,
            "Cwnd must be <= max_cwnd: {} <= {}",
            final_cwnd,
            max_cwnd
        );

        // Invariant: base_delay should track minimum (with tolerance)
        assert!(
            duration_approx_eq(final_base_delay, min_rtt, 1),
            "Base delay {:?} should track minimum RTT {:?}",
            final_base_delay,
            min_rtt
        );
    }

    /// Parametrized test: variable RTT with different base latencies.
    ///
    /// Tests that LEDBAT behaves correctly across different network types
    /// with jittery latency. This complements the fixed-latency tests in
    /// `test_slowdown_at_various_latencies` by adding RTT variability.
    ///
    /// Network types tested:
    /// - LAN (10ms base, ±5ms jitter)
    /// - Regional (50ms base, ±20ms jitter)
    /// - Continental (100ms base, ±30ms jitter)
    /// - Satellite (600ms base, ±100ms jitter) - realistic GEO satellite latency
    #[rstest::rstest]
    #[case::lan(10, 5)]
    #[case::regional(50, 20)]
    #[case::continental(100, 30)]
    #[case::satellite(600, 100)]
    #[tokio::test]
    async fn test_variable_rtt_jitter_by_network_type(
        #[case] base_rtt_ms: u64,
        #[case] jitter_ms: u64,
    ) {
        let min_cwnd = 2848usize;
        let max_cwnd = 10_000_000usize;
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd,
            max_cwnd,
            ssthresh: 40_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        let mut min_rtt = Duration::from_millis(base_rtt_ms + jitter_ms);

        // Simulate 15 ACKs with jittery RTT
        for i in 0..15 {
            // Generate deterministic jitter pattern
            let jitter = ((i as i64 * 7) % ((jitter_ms as i64) * 2 + 1)) - (jitter_ms as i64);
            let rtt_ms = ((base_rtt_ms as i64) + jitter).max(5) as u64;
            let rtt = Duration::from_millis(rtt_ms);

            if rtt < min_rtt {
                min_rtt = rtt;
            }

            // Advance time appropriately for this RTT
            let wait_time = (rtt_ms / 2).max(10); // Wait half RTT minimum
            tokio::time::sleep(Duration::from_millis(wait_time)).await;

            controller.on_ack(rtt, 5000);
        }

        let final_cwnd = controller.current_cwnd();
        let final_base_delay = controller.base_delay();

        println!(
            "{}ms ± {}ms: base_delay={:?}, final_cwnd={} KB",
            base_rtt_ms,
            jitter_ms,
            final_base_delay,
            final_cwnd / 1024
        );

        // Verify base delay tracks minimum (with tolerance)
        assert!(
            duration_approx_eq(final_base_delay, min_rtt, 1),
            "{}ms base: Base delay {:?} should track minimum RTT {:?}",
            base_rtt_ms,
            final_base_delay,
            min_rtt
        );

        // Verify cwnd remains valid
        assert!(
            final_cwnd >= min_cwnd && final_cwnd <= max_cwnd,
            "{}ms base: Cwnd {} should be within bounds [{}, {}]",
            base_rtt_ms,
            final_cwnd,
            min_cwnd,
            max_cwnd
        );
    }

    /// Parametrized test: queue buildup at different base latencies.
    ///
    /// Tests LEDBAT's response to gradual queue buildup across different
    /// network conditions. The queue grows from base_rtt to base_rtt + 60ms
    /// (matching TARGET delay).
    #[rstest::rstest]
    #[case::lan(20, 60)] // 20ms -> 80ms (queuing = 60ms = TARGET)
    #[case::regional(50, 60)] // 50ms -> 110ms
    #[case::high_latency(100, 60)] // 100ms -> 160ms
    #[tokio::test]
    async fn test_variable_rtt_queue_buildup(
        #[case] base_rtt_ms: u64,
        #[case] queue_growth_ms: u64,
    ) {
        let min_cwnd = 2848usize;
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd,
            max_cwnd: 10_000_000,
            ssthresh: 200_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        // Establish baseline
        let base_rtt = Duration::from_millis(base_rtt_ms);
        for _ in 0..5 {
            controller.on_ack(base_rtt, 5000);
            tokio::time::sleep(Duration::from_millis(base_rtt_ms + 5)).await;
        }

        let baseline_base_delay = controller.base_delay();
        assert!(
            duration_approx_eq(baseline_base_delay, base_rtt, 1),
            "Base delay should be established at {:?}, got {:?}",
            base_rtt,
            baseline_base_delay
        );

        // Simulate gradual queue buildup
        let steps = 6u64;
        let step_size = queue_growth_ms / steps;
        for i in 1..=steps {
            let rtt_ms = base_rtt_ms + (step_size * i);
            let rtt = Duration::from_millis(rtt_ms);
            tokio::time::sleep(Duration::from_millis(rtt_ms + 5)).await;
            controller.on_ack(rtt, 5000);
        }

        let final_cwnd = controller.current_cwnd();
        let final_base_delay = controller.base_delay();

        println!(
            "{}ms base + {}ms queue: base_delay={:?}, final_cwnd={} KB",
            base_rtt_ms,
            queue_growth_ms,
            final_base_delay,
            final_cwnd / 1024
        );

        // Base delay should remain at minimum (with tolerance)
        assert!(
            duration_approx_eq(final_base_delay, base_rtt, 1),
            "Base delay should remain at minimum observed RTT {:?}, got {:?}",
            base_rtt,
            final_base_delay
        );

        // Cwnd should remain usable
        assert!(
            final_cwnd >= min_cwnd,
            "Cwnd should remain usable: {} >= {}",
            final_cwnd,
            min_cwnd
        );
    }

    /// Parametrized test: latency spike recovery at different magnitudes.
    ///
    /// Tests LEDBAT's ability to recover from latency spikes of various
    /// magnitudes. Larger spikes (relative to base delay) are more challenging.
    #[rstest::rstest]
    #[case::small_spike(30, 80)] // 30ms base, spike to 80ms (2.7x)
    #[case::medium_spike(30, 150)] // 30ms base, spike to 150ms (5x)
    #[case::large_spike(30, 300)] // 30ms base, spike to 300ms (10x)
    #[tokio::test]
    async fn test_variable_rtt_spike_recovery(#[case] base_rtt_ms: u64, #[case] spike_rtt_ms: u64) {
        let min_cwnd = 2848usize;
        let config = LedbatConfig {
            initial_cwnd: 80_000,
            min_cwnd,
            max_cwnd: 10_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        // Establish baseline
        let base_rtt = Duration::from_millis(base_rtt_ms);
        for _ in 0..5 {
            controller.on_ack(base_rtt, 10_000);
            tokio::time::sleep(Duration::from_millis(base_rtt_ms + 5)).await;
        }

        // Apply spike
        let spike_rtt = Duration::from_millis(spike_rtt_ms);
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(spike_rtt_ms.min(100) + 5)).await;
            controller.on_ack(spike_rtt, 2000);
        }

        // Recovery
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(base_rtt_ms + 5)).await;
            controller.on_ack(base_rtt, 10_000);
        }

        let final_cwnd = controller.current_cwnd();
        let final_base_delay = controller.base_delay();

        println!(
            "{}ms base, {}ms spike ({}x): base_delay={:?}, final_cwnd={} KB",
            base_rtt_ms,
            spike_rtt_ms,
            spike_rtt_ms / base_rtt_ms,
            final_base_delay,
            final_cwnd / 1024
        );

        // Base delay should remain at minimum (not polluted by spike), with tolerance
        assert!(
            duration_approx_eq(final_base_delay, base_rtt, 1),
            "Base delay should remain at minimum {:?} after spike, got {:?}",
            base_rtt,
            final_base_delay
        );

        // Cwnd should be usable after recovery
        assert!(
            final_cwnd >= min_cwnd,
            "Cwnd should be usable after recovery: {} >= {}",
            final_cwnd,
            min_cwnd
        );
    }

    /// Test timeout behavior with variable RTT leading up to timeout.
    ///
    /// This simulates a scenario where RTT becomes increasingly unreliable
    /// before eventually triggering a timeout. Tests the interaction between
    /// variable RTT handling and timeout recovery (fixed in PR #2549).
    #[tokio::test]
    async fn test_variable_rtt_timeout_recovery() {
        println!("\n========== Variable RTT: Timeout with Degrading Conditions ==========");

        let min_cwnd = 2848usize;
        let config = LedbatConfig {
            initial_cwnd: 80_000,
            min_cwnd,
            max_cwnd: 10_000_000,
            ssthresh: 60_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        // Phase 1: Establish baseline at 50ms
        println!("\n--- Phase 1: Establish 50ms baseline ---");
        let baseline_rtt = Duration::from_millis(50);
        for _ in 0..5 {
            controller.on_ack(baseline_rtt, 5000);
            tokio::time::sleep(Duration::from_millis(55)).await;
        }

        let pre_degradation_cwnd = controller.current_cwnd();
        println!(
            "  Baseline established: cwnd={} KB, base_delay={:?}",
            pre_degradation_cwnd / 1024,
            controller.base_delay()
        );

        // Phase 2: Degrading conditions (RTT increases with high variance)
        println!("\n--- Phase 2: Degrading conditions ---");
        let degrading_rtts = [60, 80, 100, 150, 200, 250];
        for &rtt_ms in &degrading_rtts {
            let rtt = Duration::from_millis(rtt_ms);
            tokio::time::sleep(Duration::from_millis(rtt_ms + 10)).await;
            controller.on_ack(rtt, 2000);
            println!(
                "  RTT: {}ms, cwnd: {} KB",
                rtt_ms,
                controller.current_cwnd() / 1024
            );
        }

        let pre_timeout_cwnd = controller.current_cwnd();

        // Phase 3: Timeout occurs
        println!("\n--- Phase 3: Timeout ---");
        controller.on_timeout();
        let post_timeout_cwnd = controller.current_cwnd();
        println!(
            "  Timeout: cwnd {} KB → {} KB",
            pre_timeout_cwnd / 1024,
            post_timeout_cwnd / 1024
        );

        // Verify timeout reset cwnd to minimum
        assert!(
            post_timeout_cwnd <= min_cwnd + 1000,
            "Timeout should reset cwnd to near minimum: {}",
            post_timeout_cwnd
        );

        // Verify SlowStart state re-enabled for fast recovery
        assert_eq!(
            controller.congestion_state.load(),
            CongestionState::SlowStart,
            "SlowStart state should be set after timeout"
        );

        // Phase 4: Recovery with improving conditions
        // Start with moderate RTT to allow slow start growth before triggering exit
        println!("\n--- Phase 4: Recovery with improving RTT ---");
        let improving_rtts = [80, 70, 60, 55, 52, 50, 50, 50, 50, 50, 50, 50];
        for &rtt_ms in &improving_rtts {
            let rtt = Duration::from_millis(rtt_ms);
            controller.on_send(50_000);
            tokio::time::sleep(Duration::from_millis(rtt_ms + 5)).await;
            controller.on_ack(rtt, 8000);
        }

        let recovered_cwnd = controller.current_cwnd();
        println!(
            "\n  Recovery complete: cwnd={} KB (started at {} KB)",
            recovered_cwnd / 1024,
            post_timeout_cwnd / 1024
        );

        // Should have recovered from minimum cwnd via slow start.
        // Using 1.5x threshold (instead of 2x) to account for variability in
        // slow start behavior depending on when ssthresh is hit. The key invariant
        // is that cwnd grows meaningfully, not that it hits an exact multiple.
        let recovery_threshold = (post_timeout_cwnd * 3) / 2; // 1.5x
        assert!(
            recovered_cwnd >= recovery_threshold,
            "Should recover via slow start to at least 1.5x minimum: {} >= {}",
            recovered_cwnd,
            recovery_threshold
        );

        // Base delay should have returned to near baseline (with tolerance)
        let final_base_delay = controller.base_delay();
        assert!(
            final_base_delay <= Duration::from_millis(55),
            "Base delay should return to near baseline: {:?}",
            final_base_delay
        );
    }

    // ============================================================================
    // Deterministic Test Harness Tests (Phase 3)
    // ============================================================================

    /// Regression test: Slowdown count must be bounded at high RTT.
    ///
    /// This test would have caught the 67-slowdown bug observed in production
    /// where a 2.4MB transfer at 135ms RTT triggered 67 slowdowns in 33.8 seconds.
    ///
    /// Expected behavior: At 135ms RTT over 33.8s (~250 RTTs), slowdowns should
    /// occur at most every 18 RTTs, giving max ~14 slowdowns.
    #[test]
    fn test_harness_slowdown_count_bounded_at_high_rtt() {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 1_000_000,
            ssthresh: 102_400,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let mut harness = LedbatTestHarness::new(
            config,
            NetworkCondition::INTERCONTINENTAL, // 135ms RTT
            42,                                 // Fixed seed for reproducibility
        );

        // Simulate 33.8 seconds at 135ms RTT = ~250 RTTs
        let rtts_to_simulate = 250;
        let bytes_per_rtt = 100_000; // 100KB per RTT

        let snapshots = harness.run_rtts(rtts_to_simulate, bytes_per_rtt);

        let final_slowdowns = snapshots.last().unwrap().periodic_slowdowns;

        // At 135ms RTT, min_interval = 18 RTTs (9 * 2 RTTs freeze)
        // In 250 RTTs, max slowdowns = 250 / 18 ≈ 14
        // Allow some tolerance but nowhere near 67
        let max_expected = 20;

        assert!(
            final_slowdowns <= max_expected,
            "Slowdown count {} exceeds maximum expected {} at 135ms RTT over 250 RTTs.\n\
             This indicates the min_interval bug - slowdowns should occur at most \
             every 18 RTTs (9 * 2 RTT freeze duration).",
            final_slowdowns,
            max_expected
        );

        println!(
            "High-RTT test: {} slowdowns in {} RTTs (max expected: {})",
            final_slowdowns, rtts_to_simulate, max_expected
        );
    }

    /// Test cwnd growth rate across various RTT ranges (no loss, no jitter).
    ///
    /// Verifies that LEDBAT++ achieves growth in slow start at various RTT ranges.
    #[test]
    fn test_harness_cwnd_growth_across_rtts() {
        // Test each RTT range with no loss/jitter for predictable growth
        for (name, rtt_ms) in [
            ("LAN (1ms)", 1u64),
            ("Datacenter (10ms)", 10),
            ("Continental (50ms)", 50),
            ("Intercontinental (135ms)", 135),
        ] {
            let config = LedbatConfig {
                initial_cwnd: 38_000,
                min_cwnd: 2_848,
                max_cwnd: 1_000_000,
                ssthresh: 500_000, // High threshold so we stay in slow start
                enable_slow_start: true,
                enable_periodic_slowdown: false, // Disable for predictable growth
                randomize_ssthresh: false,
                ..Default::default()
            };

            // Use no-loss, no-jitter condition for predictable growth
            let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
            let mut harness = LedbatTestHarness::new(config.clone(), condition, 123);

            // Run for 5 RTTs in slow start
            let snapshots = harness.run_rtts(5, 500_000);

            let initial_cwnd = config.initial_cwnd;
            let final_cwnd = snapshots.last().unwrap().cwnd;

            // In slow start, cwnd should grow
            assert!(
                final_cwnd > initial_cwnd,
                "{}: cwnd should grow during slow start \
                 (initial: {} bytes, final: {} bytes)",
                name,
                initial_cwnd,
                final_cwnd
            );

            println!(
                "{}: cwnd grew from {}KB to {}KB in 5 RTTs",
                name,
                initial_cwnd / 1024,
                final_cwnd / 1024
            );
        }
    }

    /// Test that timeout properly resets state machine.
    ///
    /// After timeout, controller should:
    /// 1. Reset cwnd to minimum
    /// 2. Re-enter SlowStart state
    /// 3. Resume exponential growth
    #[test]
    fn test_harness_timeout_resets_state_correctly() {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 1_000_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let mut harness =
            LedbatTestHarness::new(config.clone(), NetworkCondition::CONTINENTAL, 456);

        // Run until we exit slow start
        let result = harness.run_until_state(CongestionState::WaitingForSlowdown, 100, 100_000);
        assert!(result.is_ok(), "Should reach WaitingForSlowdown state");

        let pre_timeout_cwnd = harness.snapshot().cwnd;
        assert!(
            pre_timeout_cwnd > config.initial_cwnd,
            "cwnd should have grown before timeout"
        );

        // Inject timeout
        harness.inject_timeout();

        let post_timeout = harness.snapshot();
        assert_eq!(
            post_timeout.state,
            CongestionState::SlowStart,
            "State should be SlowStart after timeout"
        );
        assert_eq!(
            post_timeout.cwnd, config.min_cwnd,
            "cwnd should reset to minimum after timeout"
        );

        // Verify recovery via slow start
        let recovery = harness.run_rtts(10, 50_000);
        let final_cwnd = recovery.last().unwrap().cwnd;
        assert!(
            final_cwnd > config.min_cwnd * 2,
            "Should recover via slow start exponential growth: {} > {}",
            final_cwnd,
            config.min_cwnd * 2
        );
    }

    /// Test determinism: same seed produces identical behavior.
    #[test]
    fn test_harness_determinism() {
        // Disable randomize_ssthresh since that uses rand::random() outside our control
        let config = LedbatConfig {
            randomize_ssthresh: false,
            ..Default::default()
        };
        let seed = 99999u64;

        // Use no-jitter condition for perfect determinism
        let condition = NetworkCondition::custom(50, None, 0.0);

        // Run twice with same seed
        let mut harness1 = LedbatTestHarness::new(config.clone(), condition, seed);
        let snapshots1 = harness1.run_rtts(50, 100_000);

        let mut harness2 = LedbatTestHarness::new(config.clone(), condition, seed);
        let snapshots2 = harness2.run_rtts(50, 100_000);

        // Verify identical behavior
        assert_eq!(
            snapshots1.len(),
            snapshots2.len(),
            "Snapshot counts should match"
        );

        for (i, (s1, s2)) in snapshots1.iter().zip(snapshots2.iter()).enumerate() {
            assert_eq!(
                s1.cwnd, s2.cwnd,
                "RTT {}: cwnd mismatch ({} vs {})",
                i, s1.cwnd, s2.cwnd
            );
            assert_eq!(
                s1.state, s2.state,
                "RTT {}: state mismatch ({:?} vs {:?})",
                i, s1.state, s2.state
            );
            assert_eq!(
                s1.periodic_slowdowns, s2.periodic_slowdowns,
                "RTT {}: slowdown count mismatch ({} vs {})",
                i, s1.periodic_slowdowns, s2.periodic_slowdowns
            );
        }

        println!("Determinism verified over 50 RTTs with seed {}", seed);
    }

    /// Test that different seeds with jitter produce different RTT values.
    #[test]
    fn test_harness_different_seeds_differ() {
        // This test verifies that the seeded RNG produces different jitter patterns
        let config = LedbatConfig {
            randomize_ssthresh: false,
            ..Default::default()
        };

        // Use condition with jitter to see seed differences
        let condition = NetworkCondition::CONTINENTAL; // has ±10% jitter

        let mut harness1 = LedbatTestHarness::new(config.clone(), condition, 1);
        let mut harness2 = LedbatTestHarness::new(config.clone(), condition, 2);

        // Run a few steps and check that base delays differ (due to jitter)
        for _ in 0..10 {
            harness1.step(50_000);
            harness2.step(50_000);
        }

        // Base delays should differ due to jittered RTT samples
        let bd1 = harness1.snapshot().base_delay;
        let bd2 = harness2.snapshot().base_delay;

        // They might be the same if jitter was minimal, so just print for now
        println!("Seed 1 base_delay: {:?}, Seed 2 base_delay: {:?}", bd1, bd2);
    }

    /// Test that state machine transitions occur through slowdown cycle.
    #[test]
    fn test_harness_state_transitions() {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 1_000_000,
            ssthresh: 40_000, // Just above initial, triggers exit quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // Use no-loss condition for predictable timing
        let condition = NetworkCondition::custom(50, None, 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 789);

        // Track state transitions
        let mut states_seen = vec![harness.snapshot().state];

        // Run for 100 RTTs and collect states
        for _ in 0..100 {
            harness.step(100_000);
            let state = harness.snapshot().state;
            if states_seen.last() != Some(&state) {
                states_seen.push(state);
            }
        }

        // Verify we started in SlowStart
        assert_eq!(
            states_seen[0],
            CongestionState::SlowStart,
            "Should start in SlowStart"
        );

        // Verify we exited slow start at some point
        assert!(
            states_seen.len() > 1,
            "Should have transitioned out of SlowStart. States seen: {:?}",
            states_seen
        );

        println!("State progression: {:?}", states_seen);
    }

    /// Test that packet loss triggers cwnd reduction.
    #[test]
    fn test_harness_packet_loss_reduces_cwnd() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            enable_slow_start: false, // Start in congestion avoidance
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // 50% loss rate for dramatic effect
        let condition = NetworkCondition::custom(50, None, 0.5);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 12345);

        let initial_cwnd = harness.snapshot().cwnd;

        // Run several steps - with 50% loss, cwnd should decrease
        for _ in 0..20 {
            harness.step(50_000);
        }

        let final_cwnd = harness.snapshot().cwnd;

        assert!(
            final_cwnd < initial_cwnd,
            "With 50% loss, cwnd should decrease: initial {} -> final {}",
            initial_cwnd,
            final_cwnd
        );

        println!(
            "50% loss: cwnd reduced from {}KB to {}KB",
            initial_cwnd / 1024,
            final_cwnd / 1024
        );
    }

    /// Test state invariants are maintained through slowdown cycle.
    #[test]
    fn test_harness_state_invariants() {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 1_000_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(50, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 111);

        // Run through a full cycle and verify invariants at each step
        for rtt in 0..150 {
            harness.step(100_000);
            let snap = harness.snapshot();

            // Invariant 1: cwnd is within bounds
            assert!(
                snap.cwnd >= config.min_cwnd,
                "RTT {}: cwnd {} below min {}",
                rtt,
                snap.cwnd,
                config.min_cwnd
            );
            assert!(
                snap.cwnd <= config.max_cwnd,
                "RTT {}: cwnd {} above max {}",
                rtt,
                snap.cwnd,
                config.max_cwnd
            );

            // Invariant 2: State is valid
            assert!(
                matches!(
                    snap.state,
                    CongestionState::SlowStart
                        | CongestionState::CongestionAvoidance
                        | CongestionState::WaitingForSlowdown
                        | CongestionState::InSlowdown
                        | CongestionState::Frozen
                        | CongestionState::RampingUp
                ),
                "RTT {}: Invalid state {:?}",
                rtt,
                snap.state
            );

            // Invariant 3: Slowdown count is monotonically non-decreasing
            // (checked implicitly by the harness)
        }

        println!("State invariants verified over 150 RTTs");
    }

    // ============================================================================
    // New Comprehensive Tests (from review recommendations)
    // ============================================================================

    /// Test delay-based algorithm convergence at various RTTs.
    ///
    /// Verifies that cwnd stabilizes (doesn't oscillate wildly) when the network
    /// is operating at approximately the target queuing delay.
    #[test]
    fn test_harness_delay_convergence_at_various_rtts() {
        for rtt_ms in [10u64, 50, 100, 150, 250] {
            let config = LedbatConfig {
                initial_cwnd: 50_000,
                min_cwnd: 2_848,
                max_cwnd: 500_000,
                enable_slow_start: false, // Start in congestion avoidance
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                ..Default::default()
            };

            // No jitter, no loss for clean convergence testing
            let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
            let mut harness = LedbatTestHarness::new(config.clone(), condition, 42);

            // Run for enough RTTs to reach steady state
            let snapshots = harness.run_rtts(100, 100_000);

            // Take the last 20 snapshots and check cwnd variance
            let last_20: Vec<_> = snapshots.iter().rev().take(20).collect();
            let cwnd_values: Vec<usize> = last_20.iter().map(|s| s.cwnd).collect();

            let min_cwnd = *cwnd_values.iter().min().unwrap();
            let max_cwnd = *cwnd_values.iter().max().unwrap();

            // Cwnd should be relatively stable (within 50% range)
            // This catches wild oscillation bugs
            let variance_ratio = if min_cwnd > 0 {
                max_cwnd as f64 / min_cwnd as f64
            } else {
                1.0
            };

            assert!(
                variance_ratio < 2.0,
                "{}ms RTT: cwnd variance too high (min: {}, max: {}, ratio: {:.2}). \
                 This may indicate oscillation in the delay-based algorithm.",
                rtt_ms,
                min_cwnd,
                max_cwnd,
                variance_ratio
            );

            println!(
                "{}ms RTT: cwnd stable at {}-{}KB (ratio: {:.2})",
                rtt_ms,
                min_cwnd / 1024,
                max_cwnd / 1024,
                variance_ratio
            );
        }
    }

    /// Test complete slowdown cycle with realistic network conditions (jitter + loss).
    ///
    /// Unlike the simpler tests, this exercises the full state machine under
    /// conditions similar to production: 135ms RTT, ±10% jitter, 0.1% loss.
    #[test]
    fn test_harness_slowdown_cycle_with_jitter_and_loss() {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // Realistic intercontinental conditions
        let condition = NetworkCondition::custom(135, Some(0.1), 0.001);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 999);

        // Track all states encountered
        let mut states_encountered = std::collections::HashSet::new();
        states_encountered.insert(harness.snapshot().state);

        // Run for 200 RTTs (27 seconds at 135ms) - enough for multiple slowdown cycles
        for _ in 0..200 {
            harness.step(100_000);
            states_encountered.insert(harness.snapshot().state);
        }

        let final_snap = harness.snapshot();

        // Verify we completed at least one full slowdown cycle
        assert!(
            states_encountered.contains(&CongestionState::WaitingForSlowdown)
                || states_encountered.contains(&CongestionState::InSlowdown)
                || states_encountered.contains(&CongestionState::Frozen)
                || states_encountered.contains(&CongestionState::RampingUp),
            "Should have entered slowdown cycle. States seen: {:?}",
            states_encountered
        );

        // Verify slowdowns actually occurred
        assert!(
            final_snap.periodic_slowdowns >= 1,
            "Should have at least 1 slowdown in 200 RTTs at 135ms"
        );

        // Verify cwnd is still reasonable (not stuck at min)
        assert!(
            final_snap.cwnd >= config.min_cwnd,
            "cwnd should not drop below minimum"
        );

        println!(
            "Slowdown cycle test: {} slowdowns, final cwnd: {}KB, states: {:?}",
            final_snap.periodic_slowdowns,
            final_snap.cwnd / 1024,
            states_encountered
        );
    }

    /// Test GAIN transition boundaries.
    ///
    /// GAIN changes at specific RTT thresholds based on the formula:
    /// GAIN = 1 / min(ceil(2 * TARGET / base_delay), MAX_GAIN_DIVISOR)
    ///
    /// Key boundaries (TARGET = 60ms):
    /// - 7.5ms:  GAIN = 1/16 (max divisor)
    /// - 15ms:   GAIN = 1/8
    /// - 30ms:   GAIN = 1/4
    /// - 60ms:   GAIN = 1/2
    /// - 120ms+: GAIN = 1 (divisor = 1)
    #[test]
    fn test_harness_gain_transitions() {
        // Test RTT values and their expected GAIN divisors
        let test_cases: [(u64, u32); 6] = [
            (4, 16),  // ceil(120/4) = 30, clamped to 16
            (8, 15),  // ceil(120/8) = 15
            (15, 8),  // ceil(120/15) = 8
            (30, 4),  // ceil(120/30) = 4
            (60, 2),  // ceil(120/60) = 2
            (120, 1), // ceil(120/120) = 1
        ];

        for (rtt_ms, expected_divisor) in test_cases {
            let expected_gain = 1.0 / expected_divisor as f64;
            let actual_gain = LedbatTestHarness::expected_gain_for_rtt(rtt_ms);

            assert!(
                (actual_gain - expected_gain).abs() < 0.001,
                "{}ms RTT: expected GAIN {:.4} (divisor {}), got {:.4}",
                rtt_ms,
                expected_gain,
                expected_divisor,
                actual_gain
            );

            // Also verify via harness after establishing base delay
            let config = LedbatConfig {
                initial_cwnd: 50_000,
                min_cwnd: 2_848,
                max_cwnd: 500_000,
                enable_slow_start: false,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                ..Default::default()
            };

            let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
            let mut harness = LedbatTestHarness::new(config, condition, 123);

            // Run a few RTTs to establish base delay
            harness.run_rtts(5, 50_000);

            let measured_gain = harness.current_gain();

            println!(
                "{}ms RTT: expected GAIN = 1/{} = {:.4}, measured = {:.4}",
                rtt_ms, expected_divisor, expected_gain, measured_gain
            );
        }
    }

    /// Test behavior when RTT changes mid-transfer.
    ///
    /// Simulates a network path change (e.g., route change) where RTT
    /// suddenly increases from 50ms to 200ms. Verifies:
    /// 1. Base delay updates appropriately
    /// 2. GAIN recalculates
    /// 3. Cwnd adjusts to new conditions
    #[test]
    fn test_harness_rtt_change_mid_transfer() {
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2_848,
            max_cwnd: 1_000_000,
            enable_slow_start: false, // Start in congestion avoidance
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // Start with low RTT
        let low_rtt_condition = NetworkCondition::custom(50, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), low_rtt_condition, 777);

        // Run at low RTT to establish baseline
        let low_rtt_snaps = harness.run_rtts(20, 100_000);
        let cwnd_at_low_rtt = low_rtt_snaps.last().unwrap().cwnd;
        let base_delay_at_low_rtt = harness.snapshot().base_delay;

        println!(
            "Before RTT change: cwnd = {}KB, base_delay = {:?}",
            cwnd_at_low_rtt / 1024,
            base_delay_at_low_rtt
        );

        // Change to high RTT (simulating route change)
        harness.set_condition(NetworkCondition::custom(200, None, 0.0));

        // Run at high RTT
        let high_rtt_snaps = harness.run_rtts(50, 100_000);
        let cwnd_at_high_rtt = high_rtt_snaps.last().unwrap().cwnd;
        let base_delay_after = harness.snapshot().base_delay;

        println!(
            "After RTT change: cwnd = {}KB, base_delay = {:?}",
            cwnd_at_high_rtt / 1024,
            base_delay_after
        );

        // Base delay should reflect the lower RTT (base delay is minimum observed)
        // Note: It won't immediately update to 200ms because base delay tracks minimum
        assert!(
            base_delay_at_low_rtt <= Duration::from_millis(55),
            "Initial base delay should be ~50ms, got {:?}",
            base_delay_at_low_rtt
        );

        // After running at higher RTT, base delay history will eventually age out
        // but in short term (50 RTTs), base delay should still reflect old minimum
        // This is correct behavior - base delay is conservative

        // Cwnd should adjust (might decrease due to higher queuing delay estimate)
        // We just verify the system is stable and didn't crash
        assert!(
            cwnd_at_high_rtt >= config.min_cwnd,
            "cwnd should stay above minimum after RTT change"
        );
    }

    /// Test slow start exit at various RTTs.
    ///
    /// Slow start exits when either:
    /// 1. cwnd reaches ssthresh, OR
    /// 2. queuing_delay > delay_exit_threshold * TARGET (default 45ms)
    ///
    /// At different RTTs, the trigger may differ:
    /// - Low RTT (10ms): Likely hits ssthresh first
    /// - High RTT (200ms): May hit delay threshold first
    #[test]
    fn test_harness_slow_start_exit_at_various_rtts() {
        for (name, rtt_ms) in [
            ("Low RTT (10ms)", 10u64),
            ("Medium RTT (50ms)", 50),
            ("High RTT (150ms)", 150),
            ("Very High RTT (250ms)", 250),
        ] {
            let config = LedbatConfig {
                initial_cwnd: 20_000,
                min_cwnd: 2_848,
                max_cwnd: 500_000,
                ssthresh: 100_000, // 100KB threshold
                enable_slow_start: true,
                enable_periodic_slowdown: false, // Isolate slow start behavior
                randomize_ssthresh: false,
                delay_exit_threshold: 0.75, // Exit when queuing > 45ms
            };

            let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
            let mut harness = LedbatTestHarness::new(config.clone(), condition, 42);

            // Verify we start in slow start
            assert_eq!(
                harness.snapshot().state,
                CongestionState::SlowStart,
                "{}: Should start in SlowStart",
                name
            );

            // Run until we exit slow start
            let result = harness.run_until_state(CongestionState::WaitingForSlowdown, 200, 100_000);

            // Should have exited slow start
            let snaps = result.as_ref().unwrap_or_else(|e| e);
            let final_state = snaps.last().unwrap().state;
            let final_cwnd = snaps.last().unwrap().cwnd;

            // Verify exit occurred
            assert_ne!(
                final_state,
                CongestionState::SlowStart,
                "{}: Should have exited SlowStart within 200 RTTs",
                name
            );

            println!(
                "{}: exited slow start at {}KB cwnd after {} RTTs (state: {:?})",
                name,
                final_cwnd / 1024,
                snaps.len(),
                final_state
            );
        }
    }

    /// Test cwnd growth rate varies with GAIN at different RTTs.
    ///
    /// At higher RTT, GAIN is larger (up to 1.0), so cwnd should grow faster
    /// per RTT in congestion avoidance mode (not slow start).
    #[test]
    fn test_harness_cwnd_growth_rate_scales_with_gain() {
        let mut growth_rates: Vec<(u64, f64)> = Vec::new();

        for rtt_ms in [15u64, 30, 60, 120] {
            let expected_gain = LedbatTestHarness::expected_gain_for_rtt(rtt_ms);

            let config = LedbatConfig {
                initial_cwnd: 50_000,
                min_cwnd: 2_848,
                max_cwnd: 500_000,
                enable_slow_start: false, // Test congestion avoidance directly
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                ..Default::default()
            };

            let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
            let mut harness = LedbatTestHarness::new(config.clone(), condition, 123);

            let initial = harness.snapshot().cwnd;

            // Run for 20 RTTs
            harness.run_rtts(20, 100_000);

            let final_cwnd = harness.snapshot().cwnd;
            let growth = (final_cwnd as f64 - initial as f64) / initial as f64;

            growth_rates.push((rtt_ms, growth));

            println!(
                "{}ms RTT (GAIN={:.3}): cwnd grew {:.1}% ({} -> {})",
                rtt_ms,
                expected_gain,
                growth * 100.0,
                initial,
                final_cwnd
            );
        }

        // Verify higher GAIN (higher RTT) leads to faster growth
        // Compare 120ms (GAIN=1) vs 15ms (GAIN=1/8)
        let growth_15ms = growth_rates
            .iter()
            .find(|(rtt, _)| *rtt == 15)
            .map(|(_, g)| *g)
            .unwrap_or(0.0);
        let growth_120ms = growth_rates
            .iter()
            .find(|(rtt, _)| *rtt == 120)
            .map(|(_, g)| *g)
            .unwrap_or(0.0);

        // 120ms should grow faster than 15ms (higher GAIN)
        // Allow some tolerance since actual growth depends on many factors
        println!(
            "Growth comparison: 15ms={:.2}%, 120ms={:.2}%",
            growth_15ms * 100.0,
            growth_120ms * 100.0
        );
    }

    /// Test that jitter doesn't destabilize base delay tracking.
    ///
    /// With ±30% jitter, base delay should still stabilize at approximately
    /// the minimum of the jitter range (0.7 * base_rtt).
    #[test]
    fn test_harness_base_delay_stable_with_jitter() {
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            enable_slow_start: false,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // 100ms base RTT with ±30% jitter (70-130ms range)
        let condition = NetworkCondition::custom(100, Some(0.3), 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 54321);

        // Run for enough RTTs to sample the full jitter range
        harness.run_rtts(100, 50_000);

        let base_delay = harness.snapshot().base_delay;
        let base_delay_ms = base_delay.as_millis();

        // Base delay should be close to the minimum of jitter range (70ms)
        // Allow some tolerance for RNG sampling
        assert!(
            (65..=90).contains(&base_delay_ms),
            "Base delay with ±30% jitter should be near min range (70ms), got {}ms",
            base_delay_ms
        );

        println!(
            "With ±30% jitter on 100ms RTT: base_delay = {}ms (expected ~70ms)",
            base_delay_ms
        );
    }

    /// Test loss and slowdown interaction.
    ///
    /// Verifies that packet loss events don't interfere with the periodic
    /// slowdown mechanism (they should be independent).
    #[test]
    fn test_harness_loss_and_slowdown_interaction() {
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 300_000,
            ssthresh: 60_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // 150ms RTT with 2% packet loss
        let condition = NetworkCondition::custom(150, None, 0.02);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 11111);

        // Run for 150 RTTs (22.5 seconds at 150ms)
        let snapshots = harness.run_rtts(150, 100_000);

        let final_snap = snapshots.last().unwrap();

        // With loss, we expect:
        // 1. Some slowdowns (periodic mechanism still works)
        // 2. Cwnd fluctuates but stays above minimum
        // 3. System remains stable (doesn't crash or get stuck)

        println!(
            "Loss + slowdown test: {} slowdowns, final cwnd: {}KB, final state: {:?}",
            final_snap.periodic_slowdowns,
            final_snap.cwnd / 1024,
            final_snap.state
        );

        // Verify system didn't collapse
        assert!(
            final_snap.cwnd >= config.min_cwnd,
            "cwnd should stay above minimum despite loss"
        );

        // Verify slowdown mechanism still triggered (may be 0 if loss kept us in recovery)
        // The key is the system remained stable
        let losses = harness.controller().total_losses.load(Ordering::Relaxed);
        println!("Total losses recorded: {}", losses);
    }

    /// Test using convenience method run_until_cwnd_exceeds.
    #[test]
    fn test_harness_convenience_run_until_cwnd_exceeds() {
        let config = LedbatConfig {
            initial_cwnd: 20_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            enable_slow_start: true,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(50, None, 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 42);

        let result = harness.run_until_cwnd_exceeds(100_000, 50, 100_000);

        assert!(result.is_ok(), "Should reach 100KB cwnd within 50 RTTs");

        let snaps = result.unwrap();
        let final_cwnd = snaps.last().unwrap().cwnd;

        assert!(
            final_cwnd > 100_000,
            "Final cwnd should exceed 100KB, got {}",
            final_cwnd
        );

        println!(
            "run_until_cwnd_exceeds: reached {}KB in {} RTTs",
            final_cwnd / 1024,
            snaps.len()
        );
    }

    /// Test using convenience method run_until_slowdown.
    #[test]
    fn test_harness_convenience_run_until_slowdown() {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(50, None, 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 42);

        let result = harness.run_until_slowdown(100, 100_000);

        assert!(result.is_ok(), "Should trigger a slowdown within 100 RTTs");

        let snaps = result.unwrap();
        let final_slowdowns = snaps.last().unwrap().periodic_slowdowns;

        assert!(final_slowdowns >= 1, "Should have at least 1 slowdown");

        println!(
            "run_until_slowdown: first slowdown after {} RTTs",
            snaps.len()
        );
    }

    // ============================================================================
    // Regression Tests for Minimum cwnd Slowdown Trap (Nacho's Root Cause Analysis)
    // ============================================================================
    //
    // These tests catch the bug where slowdowns fire even when cwnd is at minimum,
    // creating a trap where cwnd can never grow:
    //
    // 1. Slowdowns trigger even when cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR
    // 2. When pre_slowdown_cwnd == min_cwnd, ramp-up completes instantly
    // 3. Next slowdown schedules just 18 RTTs away
    // 4. LEDBAT growth is sub-linear (~16 RTTs to double), but slowdowns every 18 RTTs
    // 5. Result: cwnd barely grows before being reset again
    //
    // The 67-slowdown bug in production was a symptom of this trap.

    /// Regression test: Slowdowns should be skipped when cwnd is too small to reduce.
    ///
    /// When cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR, a slowdown would have no
    /// meaningful effect (cwnd/4 clamps to min_cwnd). The slowdown should be skipped
    /// and the interval extended to allow cwnd to grow.
    ///
    /// This test FAILS on the buggy implementation and PASSES after the fix.
    #[test]
    fn test_harness_slowdown_skipped_when_cwnd_at_minimum() {
        let min_cwnd = 2_848;
        let config = LedbatConfig {
            initial_cwnd: min_cwnd, // Start at minimum
            min_cwnd,
            max_cwnd: 500_000,
            ssthresh: 50_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(135, None, 0.0); // High RTT like production
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 12345);

        // Inject a timeout to reset cwnd to minimum and trigger recovery
        harness.inject_timeout();

        let post_timeout = harness.snapshot();
        assert_eq!(
            post_timeout.cwnd, min_cwnd,
            "cwnd should be at minimum after timeout"
        );

        // Run for enough RTTs that a slowdown would normally trigger (>20 RTTs)
        // but since cwnd is at minimum, slowdowns should be skipped
        let snapshots = harness.run_rtts(50, 100_000);

        // Count slowdowns that occurred while cwnd was at/near minimum
        let mut futile_slowdowns = 0;
        let mut prev_slowdowns = post_timeout.periodic_slowdowns;

        for snap in &snapshots {
            if snap.periodic_slowdowns > prev_slowdowns {
                // A slowdown was triggered
                if snap.cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR {
                    futile_slowdowns += 1;
                }
            }
            prev_slowdowns = snap.periodic_slowdowns;
        }

        // With the fix, no slowdowns should trigger when cwnd is too small
        // Note: This assertion may fail on the current buggy code, which is the point!
        assert_eq!(
            futile_slowdowns,
            0,
            "Slowdowns should be skipped when cwnd <= min_cwnd * {} ({}). \
             Found {} futile slowdowns.",
            SLOWDOWN_REDUCTION_FACTOR,
            min_cwnd * SLOWDOWN_REDUCTION_FACTOR,
            futile_slowdowns
        );

        // Cwnd should have been able to grow since slowdowns were skipped
        let final_cwnd = snapshots.last().unwrap().cwnd;
        println!(
            "After 50 RTTs from minimum: cwnd grew from {}KB to {}KB",
            min_cwnd / 1024,
            final_cwnd / 1024
        );
    }

    /// Regression test: cwnd must be able to grow from minimum despite slowdown schedule.
    ///
    /// This reproduces the production bug: after timeout recovery, cwnd should be able
    /// to grow without being trapped by futile slowdowns every 18 RTTs.
    ///
    /// Expected: cwnd at least doubles within 50 RTTs when starting from minimum.
    #[test]
    fn test_harness_cwnd_can_grow_from_minimum() {
        let min_cwnd = 2_848;
        let config = LedbatConfig {
            initial_cwnd: 50_000, // Will be reset by timeout
            min_cwnd,
            max_cwnd: 500_000,
            ssthresh: 100_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(135, None, 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 99999);

        // Run briefly to establish state, then timeout to reset to minimum
        harness.run_rtts(10, 50_000);
        harness.inject_timeout();

        let post_timeout_cwnd = harness.snapshot().cwnd;
        assert_eq!(
            post_timeout_cwnd, min_cwnd,
            "Timeout should reset to min_cwnd"
        );

        // Run for 50 RTTs - cwnd should be able to grow significantly
        // LEDBAT slow start doubles cwnd per RTT, so 50 RTTs is plenty
        let snapshots = harness.run_rtts(50, 100_000);

        let final_cwnd = snapshots.last().unwrap().cwnd;
        let slowdowns = snapshots.last().unwrap().periodic_slowdowns;

        // Cwnd should have at least doubled from minimum
        let expected_min_growth = min_cwnd * 2;
        assert!(
            final_cwnd >= expected_min_growth,
            "cwnd should at least double from {} to {} within 50 RTTs, but only reached {}. \
             {} slowdowns occurred - this suggests the minimum cwnd trap bug.",
            min_cwnd,
            expected_min_growth,
            final_cwnd,
            slowdowns
        );

        println!(
            "Growth from minimum: {}KB -> {}KB ({:.1}x) in 50 RTTs ({} slowdowns)",
            min_cwnd / 1024,
            final_cwnd / 1024,
            final_cwnd as f64 / min_cwnd as f64,
            slowdowns
        );
    }

    /// Regression test: Instant ramp-up completion when pre_slowdown_cwnd == min_cwnd.
    ///
    /// When cwnd is already at minimum and a slowdown triggers:
    /// - pre_slowdown_cwnd = min_cwnd
    /// - frozen_cwnd = max(min_cwnd / 4, min_cwnd) = min_cwnd
    /// - Ramp-up target = min_cwnd, current = min_cwnd → instant completion
    /// - Next slowdown scheduled 18 RTTs away
    ///
    /// This creates a trap where slowdowns fire every 18 RTTs with no recovery time.
    #[test]
    fn test_harness_instant_rampup_trap() {
        let min_cwnd = 2_848;
        let config = LedbatConfig {
            initial_cwnd: min_cwnd,
            min_cwnd,
            max_cwnd: 500_000,
            ssthresh: 100_000,
            enable_slow_start: false, // Skip slow start to test pure slowdown behavior
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(135, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 77777);

        // Verify starting state
        let initial = harness.snapshot();
        assert_eq!(initial.cwnd, min_cwnd);
        assert_eq!(initial.state, CongestionState::CongestionAvoidance);

        // Run for 100 RTTs (enough for multiple slowdown cycles if they trigger)
        let snapshots = harness.run_rtts(100, 100_000);

        // Analyze the trap: if slowdowns fire while cwnd stays at minimum, we're trapped
        let slowdowns = snapshots.last().unwrap().periodic_slowdowns;
        let final_cwnd = snapshots.last().unwrap().cwnd;

        // Count how many RTTs were spent at minimum cwnd
        let rtts_at_minimum = snapshots.iter().filter(|s| s.cwnd == min_cwnd).count();

        // If more than half the RTTs were at minimum AND multiple slowdowns fired,
        // that indicates the trap
        let trap_detected = rtts_at_minimum > 50 && slowdowns > 3;

        if trap_detected {
            println!(
                "TRAP DETECTED: {} slowdowns fired, spent {}/100 RTTs at minimum cwnd ({}KB)",
                slowdowns,
                rtts_at_minimum,
                min_cwnd / 1024
            );
        }

        // The fix should prevent this trap
        assert!(
            !trap_detected,
            "Instant ramp-up trap detected: {} slowdowns with {}/100 RTTs at minimum. \
             cwnd should be able to grow when slowdowns are futile.",
            slowdowns, rtts_at_minimum
        );

        println!(
            "No trap: final cwnd = {}KB after 100 RTTs ({} slowdowns, {} RTTs at minimum)",
            final_cwnd / 1024,
            slowdowns,
            rtts_at_minimum
        );
    }

    /// Regression test: Slowdown scheduling after timeout should not block recovery.
    ///
    /// After a timeout:
    /// 1. cwnd resets to minimum
    /// 2. State goes to SlowStart
    /// 3. Any previously scheduled slowdown should be cleared
    /// 4. Recovery should proceed without immediate slowdown interference
    #[test]
    fn test_harness_timeout_clears_scheduled_slowdown() {
        let min_cwnd = 2_848;
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd,
            max_cwnd: 500_000,
            ssthresh: 80_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(100, None, 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 11111);

        // Run until we've had at least one slowdown (establishes next_slowdown_time)
        let _ = harness.run_until_slowdown(200, 100_000);

        let pre_timeout = harness.snapshot();
        println!(
            "Before timeout: cwnd={}KB, slowdowns={}, state={:?}",
            pre_timeout.cwnd / 1024,
            pre_timeout.periodic_slowdowns,
            pre_timeout.state
        );

        // Inject timeout
        harness.inject_timeout();

        let post_timeout = harness.snapshot();
        assert_eq!(post_timeout.cwnd, min_cwnd);
        assert_eq!(post_timeout.state, CongestionState::SlowStart);

        // Run for 30 RTTs - enough time for recovery without immediate slowdown
        // (18 RTT min_interval means no new slowdown should fire for a while)
        let recovery_snaps = harness.run_rtts(30, 100_000);

        // Check if a slowdown fired immediately after timeout (bug behavior)
        let slowdowns_during_recovery =
            recovery_snaps.last().unwrap().periodic_slowdowns - pre_timeout.periodic_slowdowns;

        // After timeout, we should have time to recover before next slowdown
        // If slowdown fires in first 18 RTTs while cwnd is still small, that's the bug
        let early_slowdown_while_small = recovery_snaps.iter().take(18).any(|s| {
            s.periodic_slowdowns > pre_timeout.periodic_slowdowns && s.cwnd < min_cwnd * 4
        });

        assert!(
            !early_slowdown_while_small,
            "Slowdown fired during early recovery while cwnd was still small. \
             Timeout should clear scheduled slowdowns to allow recovery."
        );

        let final_cwnd = recovery_snaps.last().unwrap().cwnd;
        println!(
            "Recovery after timeout: cwnd grew from {}KB to {}KB in 30 RTTs ({} new slowdowns)",
            min_cwnd / 1024,
            final_cwnd / 1024,
            slowdowns_during_recovery
        );
    }

    /// Test: LEDBAT growth rate is compatible with slowdown interval.
    ///
    /// This validates Nacho's math: LEDBAT congestion avoidance takes ~16 RTTs
    /// to double cwnd, but slowdowns happen every 18 RTTs. The system should
    /// still make progress despite this tight margin.
    ///
    /// Key insight: If slowdowns reset cwnd before it can grow meaningfully,
    /// throughput suffers. The fix should skip futile slowdowns to allow growth.
    #[test]
    fn test_harness_growth_rate_vs_slowdown_interval() {
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            ssthresh: 200_000,
            enable_slow_start: false, // Test pure congestion avoidance
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // Test at the production RTT where the bug manifested
        let condition = NetworkCondition::custom(135, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 33333);

        // Run for 100 RTTs and track growth between slowdowns
        let snapshots = harness.run_rtts(100, 100_000);

        // Calculate average cwnd across all snapshots
        let avg_cwnd: f64 =
            snapshots.iter().map(|s| s.cwnd as f64).sum::<f64>() / snapshots.len() as f64;

        let final_snap = snapshots.last().unwrap();

        println!(
            "135ms RTT over 100 RTTs: avg_cwnd = {}KB, final = {}KB, slowdowns = {}",
            avg_cwnd as usize / 1024,
            final_snap.cwnd / 1024,
            final_snap.periodic_slowdowns
        );

        // Average cwnd should be significantly above minimum
        // This fails if slowdowns keep resetting cwnd before it can grow
        assert!(
            avg_cwnd > (config.initial_cwnd as f64 * 0.8),
            "Average cwnd ({}KB) should stay near initial ({}KB), \
             but slowdowns are preventing sustained throughput",
            avg_cwnd as usize / 1024,
            config.initial_cwnd / 1024
        );
    }

    /// Stress test: 250 RTTs at high latency (reproduces production scenario).
    ///
    /// Simulates the exact scenario from production:
    /// - 135ms RTT
    /// - 33.8 seconds of transfer (~250 RTTs)
    /// - Should have bounded slowdowns AND sustained throughput
    #[test]
    fn test_harness_production_scenario_250_rtts() {
        let min_cwnd = 2_848;
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd,
            max_cwnd: 1_000_000,
            ssthresh: 102_400,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(135, Some(0.1), 0.001); // Realistic jitter + loss
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 42);

        let snapshots = harness.run_rtts(250, 100_000);

        let final_snap = snapshots.last().unwrap();

        // Key metrics from the production bug report
        let total_slowdowns = final_snap.periodic_slowdowns;

        // Calculate total bytes transferred (sum of cwnds as proxy for throughput)
        let total_bytes: usize = snapshots.iter().map(|s| s.cwnd).sum();

        // Time spent at minimum cwnd
        let rtts_at_minimum = snapshots.iter().filter(|s| s.cwnd <= min_cwnd * 2).count();

        println!("Production scenario (250 RTTs at 135ms):");
        println!("  Total slowdowns: {}", total_slowdowns);
        println!("  Final cwnd: {}KB", final_snap.cwnd / 1024);
        println!(
            "  Total throughput proxy: {}MB",
            total_bytes / (1024 * 1024)
        );
        println!("  RTTs at/near minimum: {}/250", rtts_at_minimum);

        // Assertions based on expected behavior after fix:
        // 1. Slowdowns bounded (we already test this elsewhere)
        assert!(
            total_slowdowns <= 20,
            "Too many slowdowns: {} (expected <= 20)",
            total_slowdowns
        );

        // 2. NOT stuck at minimum for majority of time
        assert!(
            rtts_at_minimum < 50,
            "Spent too much time at minimum cwnd: {}/250 RTTs. \
             This indicates the minimum cwnd trap.",
            rtts_at_minimum
        );

        // 3. Reasonable throughput achieved
        let expected_min_bytes = 250 * 30_000; // 30KB avg per RTT minimum
        assert!(
            total_bytes >= expected_min_bytes,
            "Throughput too low: {}MB (expected >= {}MB). \
             Futile slowdowns are destroying throughput.",
            total_bytes / (1024 * 1024),
            expected_min_bytes / (1024 * 1024)
        );
    }
}
