//! Token bucket rate limiter for smooth packet pacing.
//!
//! Provides algorithm-agnostic rate limiting that works with any congestion control
//! algorithm (LEDBAT, AIMD, etc.). The token bucket ensures smooth packet pacing
//! without bursts, and supports dynamic rate updates from the congestion controller.

use parking_lot::Mutex;
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};

/// Token bucket for rate limiting packet transmission.
///
/// Uses a reserve + consume pattern to prevent TOCTOU (time-of-check-time-of-use) races:
/// 1. Reserve tokens (may require waiting)
/// 2. Wait if needed
/// 3. Consume the reservation
///
/// Thread-safe via internal mutex. Supports virtual time for deterministic testing.
pub struct TokenBucket<T: TimeSource = RealTime> {
    time_source: T,
    state: Mutex<BucketState>,
}

struct BucketState {
    /// Maximum tokens (burst capacity in bytes)
    capacity: usize,
    /// Current available tokens (bytes). Can be negative to track "debt" from
    /// concurrent reservations - this ensures proper rate limiting when multiple
    /// tasks reserve tokens simultaneously.
    tokens: isize,
    /// Fractional tokens (prevents precision loss at high rates)
    fractional_tokens: f64,
    /// Refill rate (bytes/second)
    rate: usize,
    /// Last refill timestamp (nanoseconds)
    last_refill_nanos: u64,
}

// Production constructor (backward-compatible, uses real time)
#[cfg(test)]
impl TokenBucket<RealTime> {
    /// Create a new token bucket with real time.
    ///
    /// # Arguments
    /// * `capacity` - Maximum burst capacity (bytes)
    /// * `rate` - Refill rate (bytes/second)
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::token_bucket::TokenBucket;
    /// let bucket = TokenBucket::new(
    ///     1_000_000,   // 1 MB burst
    ///     1_000_000,   // 1 MB/s rate
    /// );
    /// ```
    pub fn new(capacity: usize, rate: usize) -> Self {
        Self::new_with_time_source(capacity, rate, RealTime::new())
    }
}

// Generic implementation (works with any TimeSource)
impl<T: TimeSource> TokenBucket<T> {
    /// Create a new token bucket with a custom time source.
    ///
    /// # Arguments
    /// * `capacity` - Maximum burst capacity (bytes)
    /// * `rate` - Refill rate (bytes/second)
    /// * `time_source` - TimeSource for getting current time (RealTime for production, VirtualTime for tests)
    pub fn new_with_time_source(capacity: usize, rate: usize, time_source: T) -> Self {
        let last_refill_nanos = time_source.now_nanos();
        Self {
            time_source,
            state: Mutex::new(BucketState {
                capacity,
                tokens: capacity as isize, // Start full
                fractional_tokens: 0.0,
                rate,
                last_refill_nanos,
            }),
        }
    }

    /// Reserve tokens for transmission.
    ///
    /// Immediately deducts tokens and returns wait time if needed.
    /// Caller should wait before transmitting if wait time > 0.
    ///
    /// # Arguments
    /// * `bytes` - Number of bytes to send
    ///
    /// # Returns
    /// Duration to wait before transmitting
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::token_bucket::TokenBucket;
    /// use std::time::Duration;
    /// tokio_test::block_on(async {
    ///     let bucket = TokenBucket::new(1000, 1_000_000);
    ///
    ///     let wait = bucket.reserve(500);
    ///     if wait > Duration::ZERO {
    ///         tokio::time::sleep(wait).await;
    ///     }
    ///     // Tokens already deducted, can transmit now
    /// });
    /// ```
    pub fn reserve(&self, bytes: usize) -> Duration {
        let mut state = self.state.lock();
        self.refill_state(&mut state);

        let bytes_isize = bytes as isize;

        // Calculate wait time BEFORE deducting
        let wait_time = if state.tokens >= bytes_isize {
            // Sufficient tokens available
            Duration::ZERO
        } else if state.rate == 0 {
            // Rate is 0 (not yet initialized or set to 0) - no rate limiting
            Duration::ZERO
        } else {
            // Need to wait for more tokens. Deficit accounts for any existing
            // debt (negative tokens) from previous concurrent reservations.
            let deficit = bytes_isize - state.tokens;
            let wait_secs = deficit as f64 / state.rate as f64;
            Duration::from_secs_f64(wait_secs)
        };

        // Deduct tokens - may go negative to track "debt" from concurrent reservations.
        // This ensures subsequent reservations see the accumulated deficit and wait longer.
        state.tokens -= bytes_isize;

        wait_time
    }

    /// [`Self::reserve`], also returning when the reservation is due: the
    /// time on this bucket's clock (ns) at which the wait ends.
    pub fn reserve_due(&self, bytes: usize) -> (Duration, u64) {
        let wait = self.reserve(bytes);
        let due_at_ns = self
            .time_source
            .now_nanos()
            .saturating_add(u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX));
        (wait, due_at_ns)
    }

    /// Return a reservation made with [`Self::reserve_due`] for a datagram
    /// that will not be sent, but only while it is not yet past due
    /// (`now <= due_at_ns`). Once it is past due the refill has covered its
    /// slot, and returning it would credit the same tokens twice, so nothing
    /// is returned. Used with the node-wide egress cap (see
    /// `transport::egress_cap`).
    pub fn give_back_unless_past_due(&self, bytes: usize, due_at_ns: u64) {
        if self.time_source.now_nanos() <= due_at_ns {
            self.give_back(bytes);
        }
    }

    /// Return `bytes` reserved with [`Self::reserve`] for a datagram that
    /// will not be sent, never above the capacity.
    pub fn give_back(&self, bytes: usize) {
        let mut state = self.state.lock();
        let capacity = state.capacity as isize;
        state.tokens = state.tokens.saturating_add(bytes as isize).min(capacity);
    }

    /// Refill tokens based on virtual time elapsed.
    fn refill_state(&self, state: &mut BucketState) {
        let now_nanos = self.time_source.now_nanos();
        let elapsed_nanos = now_nanos.saturating_sub(state.last_refill_nanos);

        // Skip refill if no time has elapsed (handles repeated calls in same tick).
        // Removed the 1ms guard that was causing token starvation on fast networks.
        if elapsed_nanos == 0 {
            return;
        }

        // Calculate new tokens with fractional precision
        let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;
        let new_tokens_f64 = state.rate as f64 * elapsed_secs;

        // Add to fractional accumulator
        state.fractional_tokens += new_tokens_f64;

        // Convert whole tokens
        let new_tokens_whole = state.fractional_tokens.floor() as isize;
        if new_tokens_whole > 0 {
            // Add tokens, capped at capacity. Note: tokens may be negative (debt),
            // so we add to it and cap at capacity.
            let capacity_isize = state.capacity as isize;
            state.tokens = (state.tokens + new_tokens_whole).min(capacity_isize);

            // Keep fractional part for next refill
            state.fractional_tokens -= new_tokens_whole as f64;

            state.last_refill_nanos = now_nanos;
        }
    }

    /// Update the refill rate dynamically.
    ///
    /// Called by the congestion controller when the rate changes.
    ///
    /// # Arguments
    /// * `new_rate` - New refill rate (bytes/second)
    pub fn set_rate(&self, new_rate: usize) {
        let mut state = self.state.lock();
        // Enforce minimum rate of 1KB/s to prevent division by zero
        state.rate = new_rate.max(1024);
    }

    /// Get current rate (bytes/second)
    pub fn rate(&self) -> usize {
        self.state.lock().rate
    }

    /// Get current available tokens (bytes)
    #[cfg(test)]
    pub fn available_tokens(&self) -> usize {
        let mut state = self.state.lock();
        self.refill_state(&mut state);
        // Return 0 if we're in debt (negative tokens)
        state.tokens.max(0) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;
    use std::time::Duration;

    /// A reservation given back while it is not yet past due restores the
    /// bucket; one past due is not given back, because the refill already
    /// covered its slot (a refund would credit the same tokens twice).
    #[test]
    fn a_reservation_is_given_back_only_until_it_is_due() {
        let time = VirtualTime::new();
        let bucket = TokenBucket::new_with_time_source(1_000, 1_000, time.clone());
        // Empties the bucket; the next reservation is due at 1 s.
        bucket.reserve(1_000);
        let (wait, due) = bucket.reserve_due(1_000);
        assert_eq!(wait, Duration::from_secs(1));
        assert_eq!(due, 1_000_000_000);
        time.advance(Duration::from_millis(500));
        bucket.give_back_unless_past_due(1_000, due);
        // As if the second reservation never happened: 500 refilled. Not
        // refunded, the bucket would still owe 500.
        assert_eq!(bucket.available_tokens(), 500);

        let (_, due) = bucket.reserve_due(500);
        assert_eq!(due, time.now_nanos(), "premise: due at once");
        time.advance(Duration::from_millis(100));
        bucket.give_back_unless_past_due(500, due);
        assert_eq!(
            bucket.available_tokens(),
            100,
            "a past-due payment was refunded"
        );
    }

    #[test]
    fn test_token_bucket_creation() {
        let bucket = TokenBucket::new(10_000, 1_000_000);
        assert_eq!(bucket.available_tokens(), 10_000);
        assert_eq!(bucket.rate(), 1_000_000);
    }

    #[test]
    fn test_token_bucket_immediate_tokens() {
        // Use VirtualTime to prevent token refill during test
        let time_source = VirtualTime::new();
        let bucket = TokenBucket::new_with_time_source(10_000, 1_000_000, time_source);

        // Reserve less than available - should be immediate
        let wait = bucket.reserve(5_000);
        assert_eq!(wait, Duration::ZERO);

        // Tokens already deducted (no time advanced, so no refill)
        assert_eq!(bucket.available_tokens(), 5_000);
    }

    #[test]
    fn test_token_bucket_requires_wait() {
        let bucket = TokenBucket::new(1_000, 1_000_000); // 1KB burst, 1MB/s rate

        // Reserve more than available
        let wait = bucket.reserve(5_000);

        // Should need to wait for 4KB at 1MB/s
        // wait ≈ 4000 / 1_000_000 = 0.004s = 4ms
        assert!(wait > Duration::ZERO);
        assert!(wait >= Duration::from_millis(3));
        assert!(wait <= Duration::from_millis(5));
    }

    #[test]
    fn test_token_bucket_rate_limiting_with_virtual_time() {
        // Use virtual time for deterministic, fast testing
        let time_source = VirtualTime::new();
        let bucket = TokenBucket::new_with_time_source(1_000, 1_000_000, time_source.clone());

        // Reserve 5KB (need 4KB worth of wait time)
        let wait = bucket.reserve(5_000);
        assert!(wait > Duration::ZERO);
        assert!(wait >= Duration::from_millis(3));
        assert!(wait <= Duration::from_millis(5));

        // Advance virtual time by the wait duration
        time_source.advance(wait);

        // Now we should have tokens available for the next reservation
        let wait2 = bucket.reserve(1_000);
        // Should be close to zero or small (only ~1KB to refill)
        assert!(wait2 < Duration::from_millis(2));
    }

    #[test]
    fn test_token_bucket_dynamic_rate_update() {
        let bucket = TokenBucket::new(10_000, 1_000_000);

        assert_eq!(bucket.rate(), 1_000_000);

        // Update rate
        bucket.set_rate(5_000_000);
        assert_eq!(bucket.rate(), 5_000_000);
    }

    #[test]
    fn test_token_bucket_refill_with_virtual_time() {
        let time_source = VirtualTime::new();
        let bucket = TokenBucket::new_with_time_source(10_000, 10_000_000, time_source.clone());

        // Consume all tokens
        let wait = bucket.reserve(10_000);
        assert_eq!(wait, Duration::ZERO);
        assert_eq!(bucket.available_tokens(), 0);

        // Advance virtual time by 10ms
        // Should refill 100KB at 10MB/s, but capped at capacity (10KB)
        time_source.advance(Duration::from_millis(10));

        // Should be back to capacity
        let available = bucket.available_tokens();
        assert_eq!(available, 10_000, "Should refill to capacity");
    }

    #[test]
    fn test_token_bucket_deducts_immediately() {
        let bucket = TokenBucket::new(5_000, 1_000_000);

        // Reserve more than available
        let wait = bucket.reserve(10_000);
        assert!(wait > Duration::ZERO);

        // Tokens should be depleted (deducted immediately)
        let available = bucket.available_tokens();
        assert_eq!(available, 0);
    }

    #[test]
    fn test_token_bucket_concurrent_debt_tracking() {
        // Test that concurrent reservations properly accumulate debt
        let bucket = std::sync::Arc::new(TokenBucket::new(10_000, 1_000_000));

        // Spawn multiple concurrent tasks (using tokio::task::block_in_place isn't needed here)
        // Reserve more than available from multiple "tasks"
        let wait1 = bucket.reserve(30_000); // Needs ~30ms at 1MB/s
        let wait2 = bucket.reserve(30_000); // Needs ~60ms total now (debt accumulation)
        let wait3 = bucket.reserve(30_000); // Needs ~90ms total now

        // Verify wait times account for accumulated debt
        assert!(wait1 > Duration::ZERO);
        assert!(wait2 > wait1); // Second reservation should wait longer due to debt
        assert!(wait3 > wait2); // Third should wait even longer

        // All tokens should be deducted
        assert_eq!(bucket.available_tokens(), 0);
    }
}
