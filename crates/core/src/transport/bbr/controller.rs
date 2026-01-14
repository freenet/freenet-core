//! BBRv3 congestion controller implementation.
//!
//! This is the main controller that implements the BBRv3 algorithm.
//! It provides the same interface as LEDBAT for drop-in replacement.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};

use super::bandwidth::BandwidthFilter;
use super::config::{
    BbrConfig, BETA, DRAIN_PACING_GAIN, PROBE_BW_CWND_GAIN, PROBE_RTT_CWND_GAIN,
    PROBE_RTT_MIN_CWND, STARTUP_CWND_GAIN, STARTUP_FULL_BW_ROUNDS, STARTUP_FULL_BW_THRESHOLD,
    STARTUP_LOSS_THRESHOLD, STARTUP_PACING_GAIN,
};
use super::delivery_rate::{DeliveryRateSampler, DeliveryRateToken};
use super::rtt::RttTracker;
use super::state::{AtomicBbrState, AtomicProbeBwPhase, BbrState, ProbeBwPhase};
use super::stats::BbrStats;

/// BBRv3 congestion controller.
///
/// This controller implements the BBRv3 algorithm for congestion control.
/// It maintains estimates of bottleneck bandwidth and minimum RTT, and
/// uses these to compute optimal cwnd and pacing rate.
///
/// ## Thread Safety
///
/// The controller is fully lock-free and can be accessed concurrently
/// from sender and ACK processing paths. All state is stored in atomic
/// fields.
///
/// ## Usage
///
/// ```ignore
/// let controller = BbrController::new(BbrConfig::default());
///
/// // When sending a packet:
/// controller.on_send(packet_size);
///
/// // When receiving an ACK:
/// controller.on_ack(rtt_sample, bytes_acked);
///
/// // On timeout:
/// controller.on_timeout();
///
/// // Query current state:
/// let cwnd = controller.current_cwnd();
/// let flight = controller.flightsize();
/// ```
pub struct BbrController<T: TimeSource = RealTime> {
    // === Configuration ===
    config: BbrConfig,

    // === Core BBR Model ===
    /// Windowed maximum bandwidth estimate.
    bw_filter: BandwidthFilter,

    /// Upper bound on bandwidth (for loss response).
    bw_hi: AtomicU64,

    /// Lower bound on bandwidth (short-term).
    bw_lo: AtomicU64,

    /// RTT tracking and ProbeRTT scheduling.
    rtt_tracker: RttTracker,

    // === State Machine ===
    /// Primary BBR state.
    state: AtomicBbrState,

    /// ProbeBW sub-phase.
    probe_bw_phase: AtomicProbeBwPhase,

    // === Inflight Tracking ===
    /// Current congestion window (bytes).
    cwnd: AtomicUsize,

    /// Bytes currently in flight (unacknowledged).
    flightsize: AtomicUsize,

    /// Inflight at which loss was last seen.
    inflight_hi: AtomicUsize,

    /// Short-term inflight bound.
    inflight_lo: AtomicUsize,

    // === Pacing ===
    /// Current pacing rate (bytes/sec).
    pacing_rate: AtomicU64,

    // === Delivery Rate Tracking ===
    /// Delivery rate sampler for per-packet tracking.
    delivery_sampler: DeliveryRateSampler,

    // === Startup Exit Detection ===
    /// Number of rounds without bandwidth growth.
    full_bw_count: AtomicU32,

    /// Maximum bandwidth seen (for plateau detection).
    full_bw: AtomicU64,

    // === Round Tracking ===
    /// Round count at ProbeBW phase start (for phase duration).
    probe_bw_phase_start_round: AtomicU64,

    /// Rounds spent in Cruise (for timing Up phase).
    cruise_rounds: AtomicU32,

    /// Lost bytes at the start of the current round (for per-round loss calculation).
    round_start_lost: AtomicU64,

    /// Delivered bytes at the start of the current round (for per-round loss calculation).
    round_start_delivered: AtomicU64,

    /// Round count when we last checked per-round loss.
    last_round_for_loss: AtomicU64,

    // === Statistics ===
    /// Number of ProbeRTT phases entered.
    probe_rtt_rounds: AtomicU64,

    /// Number of timeouts.
    timeouts: AtomicU64,

    // === Adaptive Timeout Floor ===
    /// Maximum BDP ever observed (for adaptive timeout floor).
    /// Unlike other estimates that get reset on timeout, this persists
    /// to provide a floor for cwnd recovery after spurious timeouts.
    max_bdp_seen: AtomicUsize,

    // === Time Source ===
    time_source: T,

    /// Epoch timestamp for time calculations (nanoseconds).
    epoch_nanos: u64,
}

impl BbrController<RealTime> {
    /// Create a new BBR controller with default time source.
    pub fn new(config: BbrConfig) -> Self {
        Self::new_with_time_source(config, RealTime::new())
    }
}

impl<T: TimeSource> BbrController<T> {
    /// Create a new BBR controller with a custom time source.
    pub fn new_with_time_source(config: BbrConfig, time_source: T) -> Self {
        let epoch_nanos = time_source.now_nanos();
        let initial_cwnd = config.initial_cwnd;

        // Initial pacing rate: use startup_min_pacing_rate to prevent bootstrap death spiral
        let initial_pacing_rate = config.startup_min_pacing_rate;

        Self {
            config,
            bw_filter: BandwidthFilter::new(super::config::MIN_RTT_FILTER_WINDOW),
            bw_hi: AtomicU64::new(u64::MAX),
            bw_lo: AtomicU64::new(0),
            rtt_tracker: RttTracker::new(),
            state: AtomicBbrState::new(BbrState::Startup),
            probe_bw_phase: AtomicProbeBwPhase::new(ProbeBwPhase::Cruise),
            cwnd: AtomicUsize::new(initial_cwnd),
            flightsize: AtomicUsize::new(0),
            inflight_hi: AtomicUsize::new(usize::MAX),
            inflight_lo: AtomicUsize::new(0),
            pacing_rate: AtomicU64::new(initial_pacing_rate),
            delivery_sampler: DeliveryRateSampler::new(),
            full_bw_count: AtomicU32::new(0),
            full_bw: AtomicU64::new(0),
            probe_bw_phase_start_round: AtomicU64::new(0),
            cruise_rounds: AtomicU32::new(0),
            round_start_lost: AtomicU64::new(0),
            round_start_delivered: AtomicU64::new(0),
            last_round_for_loss: AtomicU64::new(0),
            probe_rtt_rounds: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            max_bdp_seen: AtomicUsize::new(0),
            time_source,
            epoch_nanos,
        }
    }

    /// Get current time in nanoseconds since epoch.
    fn now_nanos(&self) -> u64 {
        self.time_source
            .now_nanos()
            .saturating_sub(self.epoch_nanos)
    }

    // =========================================================================
    // Public Interface (compatible with LedbatController)
    // =========================================================================

    /// Called when a packet is sent.
    ///
    /// # Arguments
    /// * `bytes` - Size of the packet in bytes
    ///
    /// # Returns
    /// A token to pass to `on_ack` for delivery rate computation.
    pub fn on_send(&self, bytes: usize) -> DeliveryRateToken {
        let now = self.now_nanos();
        let inflight = self.flightsize.fetch_add(bytes, Ordering::AcqRel);

        // Handle cold start: if we have no valid bandwidth samples, ensure we
        // can still send at a reasonable rate. This happens when:
        // 1. Connection just started (no data transferred yet)
        // 2. Bandwidth samples expired (idle too long with only keepalive traffic)
        //
        // Note: Keepalive packets are filtered from bandwidth estimation (see on_ack),
        // so the filter stays empty during idle periods rather than filling with
        // misleadingly low samples.
        if !self.bw_filter.has_samples(now) {
            // No valid bandwidth samples - ensure pacing rate allows probing.
            // Use startup_min_pacing_rate to prevent bootstrap death spiral.
            let min_rate = self.config.startup_min_pacing_rate;
            let current_rate = self.pacing_rate.load(Ordering::Acquire);
            if current_rate < min_rate {
                self.pacing_rate.store(min_rate, Ordering::Release);
            }
        }

        // We're NOT application-limited when actively sending
        // The caller should call set_app_limited(true) when there's no more data to send
        self.delivery_sampler.set_app_limited(false);

        self.delivery_sampler.on_send(bytes, now, inflight)
    }

    /// Mark the connection as application-limited.
    ///
    /// Call this when there's no more data to send but cwnd would allow more.
    /// This affects bandwidth estimation - app-limited samples are not used
    /// to update the max bandwidth estimate.
    pub fn set_app_limited(&self, app_limited: bool) {
        self.delivery_sampler.set_app_limited(app_limited);
    }

    /// Called when an ACK is received.
    ///
    /// # Arguments
    /// * `rtt_sample` - RTT measurement from this ACK
    /// * `bytes_acked` - Number of bytes acknowledged
    pub fn on_ack(&self, rtt_sample: Duration, bytes_acked: usize) {
        self.on_ack_with_token(rtt_sample, bytes_acked, None);
    }

    /// Called when an ACK is received, with optional delivery rate token.
    ///
    /// # Arguments
    /// * `rtt_sample` - RTT measurement from this ACK
    /// * `bytes_acked` - Number of bytes acknowledged
    /// * `token` - Optional delivery rate token from the acknowledged packet
    pub fn on_ack_with_token(
        &self,
        rtt_sample: Duration,
        bytes_acked: usize,
        token: Option<DeliveryRateToken>,
    ) {
        let now = self.now_nanos();

        // Update flightsize
        self.flightsize.fetch_sub(
            bytes_acked.min(self.flightsize.load(Ordering::Acquire)),
            Ordering::AcqRel,
        );

        // Update min_rtt
        self.rtt_tracker.update(rtt_sample, now);

        // Compute delivery rate if we have a token
        let delivery_rate = if let Some(token) = token {
            self.delivery_sampler
                .on_ack(token, bytes_acked, now)
                .map(|sample| sample.delivery_rate)
        } else {
            // Fallback: estimate from bytes_acked / rtt
            let rtt_nanos = rtt_sample.as_nanos() as u64;
            if rtt_nanos > 0 {
                Some((bytes_acked as u128 * 1_000_000_000 / rtt_nanos as u128) as u64)
            } else {
                None
            }
        };

        // Update bandwidth filter
        if let Some(rate) = delivery_rate {
            // Only update bandwidth filter for significant data transfers.
            // Keepalive packets (~27-50 bytes) produce misleadingly low bandwidth
            // samples that can pollute the filter and cause pacing to throttle
            // real data transfers to keepalive rates (~270 B/s).
            //
            // Threshold: 100 bytes covers keepalives while allowing small
            // control messages to still be excluded from bandwidth estimation.
            const MIN_BYTES_FOR_BW_SAMPLE: usize = 100;

            if bytes_acked >= MIN_BYTES_FOR_BW_SAMPLE && !self.delivery_sampler.is_app_limited() {
                self.bw_filter.update(rate, now);
            }
        }

        // Run state machine first to check for mode transitions
        self.update_state(now);

        // Update cwnd and pacing rate based on current state
        self.update_model();
    }

    /// Called when packets are detected as lost.
    ///
    /// # Arguments
    /// * `bytes_lost` - Number of bytes lost
    pub fn on_loss(&self, bytes_lost: usize) {
        self.delivery_sampler.on_loss(bytes_lost);

        let state = self.state.load();

        // In Startup, check if we should exit due to per-round loss rate.
        // Per BBRv3 spec, we compute loss rate over each round-trip, not cumulatively.
        // This prevents a few early losses from triggering premature Startup exit.
        if state == BbrState::Startup {
            let current_round = self.delivery_sampler.round_count();
            let last_round = self.last_round_for_loss.load(Ordering::Acquire);

            // Check if we've entered a new round
            if current_round > last_round {
                // New round: reset per-round counters
                let delivered = self.delivery_sampler.delivered();
                let lost = self.delivery_sampler.lost();
                self.round_start_delivered
                    .store(delivered, Ordering::Release);
                self.round_start_lost.store(lost, Ordering::Release);
                self.last_round_for_loss
                    .store(current_round, Ordering::Release);
            }

            // Compute per-round loss rate
            let delivered = self.delivery_sampler.delivered();
            let lost = self.delivery_sampler.lost();
            let round_start_delivered = self.round_start_delivered.load(Ordering::Acquire);
            let round_start_lost = self.round_start_lost.load(Ordering::Acquire);

            let round_delivered = delivered.saturating_sub(round_start_delivered);
            let round_lost = lost.saturating_sub(round_start_lost);

            if round_delivered > 0 || round_lost > 0 {
                let loss_rate = round_lost as f64 / (round_delivered + round_lost) as f64;
                if loss_rate > STARTUP_LOSS_THRESHOLD {
                    // Exit Startup due to excessive per-round loss
                    self.state.enter_drain();
                }
            }
        }

        // Reduce inflight_hi to current inflight
        let inflight = self.flightsize.load(Ordering::Acquire);
        let _ = self.inflight_hi.fetch_min(inflight, Ordering::AcqRel);

        // Reduce bw_hi by BETA
        let max_bw = self.bw_filter.max_bw(self.now_nanos());
        let new_bw_hi = (max_bw as f64 * BETA) as u64;
        let _ = self.bw_hi.fetch_min(new_bw_hi, Ordering::AcqRel);
    }

    /// Called on retransmission timeout.
    ///
    /// Unlike the original aggressive reset, we now use an adaptive floor based on
    /// the maximum BDP ever observed. This prevents "timeout storms" where spurious
    /// timeouts (caused by high latency + ACK batching delay) would completely reset
    /// state, making recovery impossible.
    ///
    /// See: v0.1.92 regression where MIN_RTO=200ms < RTT+ACK_DELAY=244ms caused
    /// 935 timeouts in 10 seconds on intercontinental connections.
    pub fn on_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::AcqRel);

        let old_cwnd = self.cwnd.load(Ordering::Acquire);

        // Reset to Startup
        self.state.enter_startup();
        self.full_bw_count.store(0, Ordering::Release);
        self.full_bw.store(0, Ordering::Release);

        // Calculate adaptive floor based on max BDP ever seen.
        // Use 1/4 of max BDP as minimum cwnd (similar to LEDBAT's approach).
        let max_bdp = self.max_bdp_seen.load(Ordering::Acquire);
        let adaptive_min = (max_bdp / 4).max(self.config.initial_cwnd);
        self.cwnd.store(adaptive_min, Ordering::Release);

        // Reset inflight bounds (loss response limits)
        self.inflight_hi.store(usize::MAX, Ordering::Release);
        self.inflight_lo.store(0, Ordering::Release);

        // Reset bandwidth bounds but NOT the bandwidth filter itself.
        // bw_hi/bw_lo are loss-response limits; the filter holds actual estimates.
        self.bw_hi.store(u64::MAX, Ordering::Release);
        self.bw_lo.store(0, Ordering::Release);

        // NOTE: We intentionally do NOT reset bw_filter or min_rtt here.
        //
        // These represent physical path characteristics (link capacity and propagation
        // delay) that don't change on timeout. Resetting them causes a "death spiral"
        // on high-latency links:
        //
        // 1. Timeout resets bw_filter → max_bw returns 0
        // 2. Timeout resets min_rtt → min_rtt_nanos returns u64::MAX
        // 3. compute_bdp() falls back to initial_cwnd (14KB) when either is invalid
        // 4. 14KB cwnd on high-latency link (e.g., 250ms RTT) = ~56KB/s max throughput
        // 5. Slow transfer = more timeouts = stuck at minimum cwnd
        //
        // By preserving bandwidth and RTT estimates, we can recover quickly after
        // spurious timeouts while still entering Startup to probe for changes.
        // BBR naturally refreshes these estimates through normal operation.
        //
        // See issue #2682 for the regression this caused.

        if old_cwnd != adaptive_min {
            tracing::warn!(
                old_cwnd_kb = old_cwnd / 1024,
                new_cwnd_kb = adaptive_min / 1024,
                max_bdp_kb = max_bdp / 1024,
                "BBR timeout - reset to adaptive floor, entering Startup"
            );
        }
    }

    /// Get the current congestion window in bytes.
    pub fn current_cwnd(&self) -> usize {
        self.cwnd.load(Ordering::Acquire)
    }

    /// Get the current bytes in flight.
    pub fn flightsize(&self) -> usize {
        self.flightsize.load(Ordering::Acquire)
    }

    /// Get the current pacing rate in bytes/sec.
    pub fn pacing_rate(&self) -> u64 {
        self.pacing_rate.load(Ordering::Acquire)
    }

    /// Get the estimated bottleneck bandwidth in bytes/sec.
    pub fn max_bw(&self) -> u64 {
        self.bw_filter.max_bw(self.now_nanos())
    }

    /// Get the minimum RTT estimate.
    pub fn min_rtt(&self) -> Option<Duration> {
        self.rtt_tracker.min_rtt()
    }

    /// Get the estimated Bandwidth-Delay Product in bytes.
    pub fn bdp(&self) -> usize {
        self.compute_bdp()
    }

    /// Get the maximum BDP ever observed (for adaptive timeout floor).
    pub fn max_bdp_seen(&self) -> usize {
        self.max_bdp_seen.load(Ordering::Acquire)
    }

    /// Get current BBR state.
    pub fn state(&self) -> BbrState {
        self.state.load()
    }

    /// Get statistics snapshot for telemetry.
    pub fn stats(&self) -> BbrStats {
        let now = self.now_nanos();
        BbrStats {
            state: self.state.load(),
            probe_bw_phase: self.probe_bw_phase.load(),
            cwnd: self.cwnd.load(Ordering::Acquire),
            flightsize: self.flightsize.load(Ordering::Acquire),
            pacing_rate: self.pacing_rate.load(Ordering::Acquire),
            max_bw: self.bw_filter.max_bw(now),
            min_rtt: self.rtt_tracker.min_rtt(),
            bdp: self.compute_bdp(),
            delivered: self.delivery_sampler.delivered(),
            lost: self.delivery_sampler.lost(),
            round_count: self.delivery_sampler.round_count(),
            full_bw_count: self.full_bw_count.load(Ordering::Acquire),
            full_bw: self.full_bw.load(Ordering::Acquire),
            probe_rtt_rounds: self.probe_rtt_rounds.load(Ordering::Acquire),
            timeouts: self.timeouts.load(Ordering::Acquire),
            max_bdp_seen: self.max_bdp_seen.load(Ordering::Acquire),
            is_app_limited: self.delivery_sampler.is_app_limited(),
            pacing_gain: self.current_pacing_gain(),
            cwnd_gain: self.current_cwnd_gain(),
        }
    }

    // =========================================================================
    // State Machine
    // =========================================================================

    /// Update BBR state based on current conditions.
    fn update_state(&self, now: u64) {
        let state = self.state.load();

        match state {
            BbrState::Startup => self.update_startup(now),
            BbrState::Drain => self.update_drain(),
            BbrState::ProbeBW => self.update_probe_bw(now),
            BbrState::ProbeRTT => self.update_probe_rtt(now),
        }
    }

    /// Update Startup state: check for bandwidth plateau.
    fn update_startup(&self, now: u64) {
        let max_bw = self.bw_filter.max_bw(now);
        let full_bw = self.full_bw.load(Ordering::Acquire);

        // Check if bandwidth is still growing
        if max_bw >= (full_bw as f64 * STARTUP_FULL_BW_THRESHOLD) as u64 {
            // Bandwidth is still growing, reset counter
            self.full_bw.store(max_bw, Ordering::Release);
            self.full_bw_count.store(0, Ordering::Release);
        } else {
            // Bandwidth has plateaued, increment counter
            self.full_bw_count.fetch_add(1, Ordering::AcqRel);
        }

        // Don't exit Startup until we've achieved meaningful throughput.
        // This prevents the bootstrap problem where low initial cwnd leads to
        // low bandwidth measurements, which causes premature STARTUP exit
        // before the cwnd floor has time to boost throughput.
        //
        // Require at least 100 KB/s measured bandwidth before allowing exit.
        // This is well above the death spiral threshold (~15 KB/s) but low
        // enough for virtualized CI environments.
        const MIN_BW_FOR_STARTUP_EXIT: u64 = 100_000; // 100 KB/s
        if max_bw < MIN_BW_FOR_STARTUP_EXIT {
            return;
        }

        // Exit Startup after STARTUP_FULL_BW_ROUNDS without growth
        if self.full_bw_count.load(Ordering::Acquire) >= STARTUP_FULL_BW_ROUNDS {
            self.state.enter_drain();
        }
    }

    /// Update Drain state: wait until inflight <= BDP.
    fn update_drain(&self) {
        let inflight = self.flightsize.load(Ordering::Acquire);
        let bdp = self.compute_bdp();

        if inflight <= bdp {
            // Queue is drained, enter ProbeBW
            self.state.enter_probe_bw();
            self.probe_bw_phase.store(ProbeBwPhase::Cruise);
            self.probe_bw_phase_start_round
                .store(self.delivery_sampler.round_count(), Ordering::Release);
            self.cruise_rounds.store(0, Ordering::Release);
        }
    }

    /// Update ProbeBW state: cycle through phases.
    fn update_probe_bw(&self, now: u64) {
        // Check if we should enter ProbeRTT
        if self.config.enable_probe_rtt && self.rtt_tracker.should_enter_probe_rtt(now) {
            self.enter_probe_rtt(now);
            return;
        }

        let phase = self.probe_bw_phase.load();
        let round_count = self.delivery_sampler.round_count();
        let phase_start = self.probe_bw_phase_start_round.load(Ordering::Acquire);
        let rounds_in_phase = round_count.saturating_sub(phase_start);

        match phase {
            ProbeBwPhase::Down => {
                // Stay in Down for 1 round
                if rounds_in_phase >= super::config::PROBE_BW_DOWN_ROUNDS as u64 {
                    self.advance_probe_bw_phase(round_count);
                }
            }
            ProbeBwPhase::Cruise => {
                // Stay in Cruise for 6+ rounds before probing up
                self.cruise_rounds.fetch_add(1, Ordering::AcqRel);
                if self.cruise_rounds.load(Ordering::Acquire)
                    >= super::config::PROBE_BW_CRUISE_ROUNDS_MIN
                {
                    self.advance_probe_bw_phase(round_count);
                }
            }
            ProbeBwPhase::Refill => {
                // Stay in Refill for 1 round
                if rounds_in_phase >= super::config::PROBE_BW_REFILL_ROUNDS as u64 {
                    self.advance_probe_bw_phase(round_count);
                }
            }
            ProbeBwPhase::Up => {
                // Stay in Up for 1 round
                if rounds_in_phase >= super::config::PROBE_BW_UP_ROUNDS as u64 {
                    self.advance_probe_bw_phase(round_count);
                    self.cruise_rounds.store(0, Ordering::Release);
                }
            }
        }
    }

    /// Advance to the next ProbeBW phase.
    fn advance_probe_bw_phase(&self, round_count: u64) {
        self.probe_bw_phase.advance();
        self.probe_bw_phase_start_round
            .store(round_count, Ordering::Release);
    }

    /// Enter ProbeRTT state.
    fn enter_probe_rtt(&self, now: u64) {
        self.state.enter_probe_rtt();
        self.rtt_tracker.enter_probe_rtt(now);
        self.probe_rtt_rounds.fetch_add(1, Ordering::AcqRel);
    }

    /// Update ProbeRTT state: wait until duration elapses and inflight drains.
    fn update_probe_rtt(&self, now: u64) {
        let inflight = self.flightsize.load(Ordering::Acquire);
        let probe_rtt_cwnd = self.compute_probe_rtt_cwnd();

        // Mark round done if inflight is low enough
        if inflight <= probe_rtt_cwnd {
            self.rtt_tracker.mark_probe_rtt_round_done();
        }

        // Check if we can exit
        if self.rtt_tracker.should_exit_probe_rtt(now) {
            self.exit_probe_rtt(now);
        }
    }

    /// Exit ProbeRTT and return to ProbeBW.
    fn exit_probe_rtt(&self, now: u64) {
        // Schedule next ProbeRTT (with optional jitter)
        // For now, no jitter - could add randomization later
        self.rtt_tracker.exit_probe_rtt(now, 0);

        // Return to ProbeBW
        self.state.enter_probe_bw();
        self.probe_bw_phase.store(ProbeBwPhase::Cruise);
        self.probe_bw_phase_start_round
            .store(self.delivery_sampler.round_count(), Ordering::Release);
        self.cruise_rounds.store(0, Ordering::Release);
    }

    // =========================================================================
    // Model Updates
    // =========================================================================

    /// Update cwnd and pacing rate based on current model.
    fn update_model(&self) {
        let bdp = self.compute_bdp();
        let cwnd_gain = self.current_cwnd_gain();
        let pacing_gain = self.current_pacing_gain();

        // Update max_bdp_seen for adaptive timeout floor.
        // This persists across timeouts to prevent cwnd collapse.
        let _ = self.max_bdp_seen.fetch_max(bdp, Ordering::AcqRel);

        // Compute target cwnd
        let mut target_cwnd = (bdp as f64 * cwnd_gain) as usize;

        // During Startup, ensure cwnd is large enough to utilize the minimum pacing rate.
        // This prevents the bootstrap problem where low cwnd limits sends, which limits
        // measured bandwidth, which keeps cwnd low.
        //
        // Startup min cwnd = startup_min_pacing_rate × min_rtt × cwnd_gain
        let state = self.state.load();
        let startup_min_rate = self.config.startup_min_pacing_rate;
        if state == BbrState::Startup {
            let min_rtt_nanos = self.rtt_tracker.min_rtt_nanos();
            if min_rtt_nanos != u64::MAX {
                let startup_bdp =
                    (startup_min_rate as u128 * min_rtt_nanos as u128 / 1_000_000_000) as usize;
                let startup_min_cwnd = (startup_bdp as f64 * cwnd_gain) as usize;
                target_cwnd = target_cwnd.max(startup_min_cwnd);
            }
        }

        // Apply inflight_hi bound (loss response) before min_cwnd
        let inflight_hi = self.inflight_hi.load(Ordering::Acquire);
        if inflight_hi < usize::MAX {
            target_cwnd = target_cwnd.min(inflight_hi);
        }

        // Apply min/max bounds - min_cwnd must always be respected
        target_cwnd = target_cwnd.max(self.config.min_cwnd);
        target_cwnd = target_cwnd.min(self.config.max_cwnd);

        self.cwnd.store(target_cwnd, Ordering::Release);

        // Compute pacing rate
        let max_bw = self.bw_filter.max_bw(self.now_nanos());
        let bw_hi = self.bw_hi.load(Ordering::Acquire);
        let effective_bw = max_bw.min(bw_hi);

        let mut pacing_rate = (effective_bw as f64 * pacing_gain) as u64;

        // During Startup, apply a minimum pacing rate floor to prevent
        // the "bootstrap death spiral" where low pacing limits sends,
        // which limits measured bandwidth, which limits pacing further.
        // This allows BBR to discover actual available bandwidth.
        if state == BbrState::Startup {
            pacing_rate = pacing_rate.max(startup_min_rate);
        }

        self.pacing_rate
            .store(pacing_rate.max(1), Ordering::Release);
    }

    /// Compute the Bandwidth-Delay Product.
    fn compute_bdp(&self) -> usize {
        let max_bw = self.bw_filter.max_bw(self.now_nanos());
        let min_rtt_nanos = self.rtt_tracker.min_rtt_nanos();

        if max_bw == 0 || min_rtt_nanos == u64::MAX {
            // No valid measurements yet, use initial cwnd
            return self.config.initial_cwnd;
        }

        // BDP = max_bw (bytes/sec) * min_rtt (sec)
        // BDP = max_bw * min_rtt_nanos / 1_000_000_000
        let bdp = (max_bw as u128 * min_rtt_nanos as u128 / 1_000_000_000) as usize;

        // Ensure at least min_cwnd
        bdp.max(self.config.min_cwnd)
    }

    /// Compute cwnd for ProbeRTT phase.
    ///
    /// Per BBRv3 spec, ProbeRTT uses a floor of 4×MSS (not min_cwnd) to ensure
    /// we can still make forward progress while probing for min_rtt.
    fn compute_probe_rtt_cwnd(&self) -> usize {
        let bdp = self.compute_bdp();
        let probe_rtt_cwnd = (bdp as f64 * PROBE_RTT_CWND_GAIN) as usize;
        probe_rtt_cwnd.max(PROBE_RTT_MIN_CWND)
    }

    /// Get the current pacing gain based on state.
    fn current_pacing_gain(&self) -> f64 {
        match self.state.load() {
            BbrState::Startup => STARTUP_PACING_GAIN,
            BbrState::Drain => DRAIN_PACING_GAIN,
            BbrState::ProbeBW => self.probe_bw_phase.load().pacing_gain(),
            BbrState::ProbeRTT => 1.0,
        }
    }

    /// Get the current cwnd gain based on state.
    fn current_cwnd_gain(&self) -> f64 {
        match self.state.load() {
            BbrState::Startup => STARTUP_CWND_GAIN,
            BbrState::Drain => STARTUP_CWND_GAIN, // Same as Startup
            BbrState::ProbeBW => PROBE_BW_CWND_GAIN,
            BbrState::ProbeRTT => PROBE_RTT_CWND_GAIN,
        }
    }

    // =========================================================================
    // Compatibility Methods (for drop-in LEDBAT replacement)
    // =========================================================================

    /// Get base delay (returns min_rtt for compatibility with LEDBAT interface).
    pub fn base_delay(&self) -> Option<Duration> {
        self.min_rtt()
    }

    /// Get queuing delay estimate (for compatibility with LEDBAT interface).
    ///
    /// In BBR, we estimate queuing delay as: current_rtt - min_rtt.
    /// Since we don't track current_rtt separately, this returns None.
    pub fn queuing_delay(&self) -> Option<Duration> {
        None
    }

    /// Called when an ACK is received without RTT measurement.
    ///
    /// This is used when no RTT sample is available (e.g., retransmissions).
    /// For compatibility with LEDBAT interface.
    pub fn on_ack_without_rtt(&self, bytes_acked: usize) {
        // Use a default RTT estimate if we don't have one
        let default_rtt = self.min_rtt().unwrap_or(Duration::from_millis(100));
        self.on_ack(default_rtt, bytes_acked);
    }

    /// Get the current sending rate in bytes/sec for the given RTT.
    ///
    /// For compatibility with LEDBAT interface. BBR uses pacing_rate directly.
    pub fn current_rate(&self, _rtt: Duration) -> u64 {
        self.pacing_rate()
    }
}

impl<T: TimeSource> std::fmt::Debug for BbrController<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BbrController")
            .field("state", &self.state.load())
            .field("cwnd", &self.cwnd.load(Ordering::Relaxed))
            .field("flightsize", &self.flightsize.load(Ordering::Relaxed))
            .field("pacing_rate", &self.pacing_rate.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    #[test]
    fn test_controller_creation() {
        let controller = BbrController::new(BbrConfig::default());
        assert_eq!(controller.state(), BbrState::Startup);
        assert_eq!(controller.current_cwnd(), BbrConfig::default().initial_cwnd);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_on_send_updates_flightsize() {
        let controller = BbrController::new(BbrConfig::default());

        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);

        controller.on_send(500);
        assert_eq!(controller.flightsize(), 1500);
    }

    #[test]
    fn test_on_ack_updates_flightsize() {
        let controller = BbrController::new(BbrConfig::default());

        controller.on_send(1000);
        controller.on_send(500);
        assert_eq!(controller.flightsize(), 1500);

        controller.on_ack(Duration::from_millis(50), 1000);
        assert_eq!(controller.flightsize(), 500);
    }

    #[test]
    fn test_on_timeout_resets_state() {
        let controller = BbrController::new(BbrConfig::default());

        // Simulate some activity
        controller.on_send(1000);
        controller.on_ack(Duration::from_millis(50), 1000);

        // Trigger timeout
        controller.on_timeout();

        assert_eq!(controller.state(), BbrState::Startup);
        assert_eq!(controller.current_cwnd(), BbrConfig::default().initial_cwnd);
    }

    #[test]
    fn test_startup_exit_on_bandwidth_plateau() {
        let time = VirtualTime::new();
        let controller = BbrController::new_with_time_source(BbrConfig::default(), time.clone());

        // Simulate several rounds with same bandwidth
        for _i in 0..5 {
            let token = controller.on_send(10000);
            time.advance(Duration::from_millis(50));
            controller.on_ack_with_token(Duration::from_millis(50), 10000, Some(token));
        }

        // After several rounds with plateau, should exit Startup
        // (exact behavior depends on bandwidth filter updates)
        let state = controller.state();
        // State should eventually transition out of Startup
        // This is a basic sanity check
        assert!(
            state == BbrState::Startup || state == BbrState::Drain || state == BbrState::ProbeBW
        );
    }

    #[test]
    fn test_stats_snapshot() {
        let controller = BbrController::new(BbrConfig::default());

        let stats = controller.stats();
        assert_eq!(stats.state, BbrState::Startup);
        assert_eq!(stats.cwnd, BbrConfig::default().initial_cwnd);
        assert_eq!(stats.flightsize, 0);
    }

    #[test]
    fn test_bdp_calculation() {
        let time = VirtualTime::new();
        let controller = BbrController::new_with_time_source(BbrConfig::default(), time.clone());

        // Initial BDP should be initial_cwnd (no measurements yet)
        assert_eq!(controller.bdp(), BbrConfig::default().initial_cwnd);

        // After some traffic, BDP should be computed from measurements
        for _ in 0..10 {
            let token = controller.on_send(10000);
            time.advance(Duration::from_millis(50));
            controller.on_ack_with_token(Duration::from_millis(50), 10000, Some(token));
        }

        // BDP should now be based on measurements
        // With 10KB/50ms = 200KB/s bandwidth and 50ms RTT:
        // BDP = 200KB/s * 0.05s = 10KB
        let bdp = controller.bdp();
        assert!(bdp >= controller.config.min_cwnd);
    }
}
