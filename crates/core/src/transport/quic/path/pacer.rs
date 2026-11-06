//! Pacing a path's data at the connection's bandwidth limit (#5940).
//!
//! `recycle::CappedCubic` holds a QUIC connection to its rate by capping its
//! congestion window at `rate x RTT`, but never below two datagrams. Where
//! `rate x RTT` is under that floor (any round trip under `2 x 1200 / rate`,
//! about 5 ms at 500 kB/s) the window does not bind, and a path ran at 3.4 to
//! 34 MB/s on loopback with `bandwidth-limit` set to 500 kB/s. A QUIC path also
//! took its connection's whole share beside the UDP session's streams, so a
//! peer whose data was split across both got up to twice its share.
//!
//! With a limit set (`bandwidth-limit`, or `total-bandwidth-limit` and its
//! per-connection share), a path's data sends draw on a token bucket of the
//! connection's own, kept on the QUIC side ([`LimitPacer`]), at the limit: the
//! rate `recycle::CappedCubic` caps the window at (`QuicRateLimit::rate`),
//! read again at every reservation so it follows the share as connections
//! come and go. Not the UDP stream bucket: that one runs at the lower of the
//! limit and the UDP congestion controller's rate (FixedRate's 1.25 MB/s by
//! default), so a limit above that would have paced QUIC below what the
//! operator set, slower than no limit at all. The cost, which stays: a peer
//! whose data is on both transports at once (a QUIC path and UDP streams that
//! fell back) can get up to twice its share, as before; the node's egress cap
//! still bounds the node under `total-bandwidth-limit`. The bucket outlives
//! the UDP transport's removal unchanged.
//!
//! Charged: every write of a Stream or pipe (header, metadata and payload),
//! and of a Short past one packet, which UDP sends as a stream. Not charged,
//! as on UDP: Shorts of one packet, and control frames, which must never wait
//! behind data. With no limit nothing is paced.
//!
//! A write takes its bytes before it is made ([`SendPacer::reserve_within`])
//! and waits out the bucket's deficit in the send's own task, never in the
//! connection's task. A reservation whose wait would reach the send's current
//! deadline is not made, as on UDP (`TokenBucket::reserve_within`): a refused
//! reservation takes no place, so it does not delay the sends behind it. The
//! send then ends [`NotOpened::Paced`] before its stream opened, and the
//! message goes on UDP. After the stream opened it ends
//! [`SendFailure::Paced`] and the stream is reset; a Stream or pipe is then
//! dropped, and a Short goes on UDP. Once UDP has sent a message the pacer
//! refused, the whole message is charged to this bucket as debt
//! ([`SendPacer::charge`]), so the next QUIC sends wait for it, up to
//! [`FALLBACK_DEBT`] (half a second at the rate): a larger debt would fail the
//! streams already open on the path. A UDP send that failed is charged nothing.
//!
//! This does not hold the two transports to the limit together. The charge is
//! booked when the UDP transfer ends, and it is capped, so for the whole
//! transfer QUIC keeps sending at the pacer's rate beside UDP: the peer gets
//! up to twice its share for that long (a 10 MB fallback at 100 kB/s runs
//! about 100 s on the UDP bucket, then 50 kB is charged). That is the design-B
//! residual, the same as for data that falls back for any other reason; only a
//! budget shared by both transports would close it. Bytes taken for a write that
//! is not made go back to the bucket.
//!
//! A refusal is never a timeout for the path's stall count (`route::StallCount`):
//! this side held the send back. A wait is not progress either: it only holds
//! writes into quinn's buffer, and acknowledgements still arrive on a live path,
//! so a paced path that goes dark still goes suspect and closes.
//!
//! The bucket and its waits are on the path's clock, the one a send's
//! deadlines run on (the network's virtual time in simulation).
//!
//! [`NotOpened::Paced`]: super::bounded::NotOpened::Paced
//! [`SendFailure::Paced`]: super::bounded::SendFailure::Paced

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::simulation::TimeSource;
use crate::transport::connection_handler::MIN_BUCKET_CAPACITY_BYTES;
use crate::transport::quic::config::QuicRateLimit;
use crate::transport::token_bucket::TokenBucket;

/// What paces a path's data sends.
pub(in crate::transport) trait SendPacer: Send + Sync {
    /// Take `bytes` if the wait for them is shorter than `limit`: `Ok(wait)`
    /// with the bytes taken, or `Err(wait)`, the wait it would have had, with
    /// nothing taken.
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration>;

    /// Return `bytes` taken for a write that was not made.
    fn give_back(&self, bytes: usize);

    /// Charge `bytes` sent another way (a message the pacer refused, sent on
    /// UDP instead), as debt, without waiting: the next reservations wait for
    /// them. The debt is bounded ([`FALLBACK_DEBT`]); past it nothing more is
    /// charged. Returns what was charged.
    fn charge(&self, bytes: usize) -> usize;

    /// Sleep for `wait` on the pacer's clock. Dropping it ends the sleep.
    fn sleep(&self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// The most a UDP fallback's charge may leave the pacer owing: half a second
/// at its rate. The wait a charge adds to the next write is then at most half a
/// second, well inside the gap deadline an open stream works to
/// (`STREAM_INACTIVITY_TIMEOUT`) and the 1 s open deadline of a Short, so a
/// charge does not make a stream already open fail; and QUIC sends again
/// within half a second of an overload ending. Fallback bytes past the bound
/// are not charged: while it binds QUIC stays held at it, and UDP runs at its
/// own bucket, so the peer can get up to twice its share meanwhile (module
/// docs).
pub(in crate::transport) const FALLBACK_DEBT: Duration = Duration::from_millis(500);

/// The rate a pacer's bucket runs at for a limit of `rate`: `TokenBucket`
/// holds every rate at 1 KiB/s or more (`set_rate`), so a share below that is
/// raised here too, and the bucket is not reset on every reservation.
fn bucket_rate(rate: u64) -> usize {
    usize::try_from(rate).unwrap_or(usize::MAX).max(1024)
}

/// The burst a pacer at `rate` holds: a tenth of a second of it, as a UDP
/// connection bucket's, and at least [`MIN_BUCKET_CAPACITY_BYTES`].
fn capacity_for(rate: usize) -> usize {
    (rate / 10).max(MIN_BUCKET_CAPACITY_BYTES)
}

/// A connection's QUIC pacer: a token bucket at the connection's limit
/// ([`QuicRateLimit::rate`]), with a burst of a tenth of that rate, both
/// following the share as it changes.
pub(in crate::transport) struct LimitPacer<T: TimeSource> {
    bucket: TokenBucket<T>,
    limit: QuicRateLimit,
    time: T,
}

impl<T: TimeSource> LimitPacer<T> {
    /// A pacer at `limit`, or `None` when no limit is set.
    pub(in crate::transport) fn new(limit: QuicRateLimit, time: T) -> Option<Self> {
        let rate = bucket_rate(limit.rate()?);
        Some(Self {
            bucket: TokenBucket::new_with_time_source(capacity_for(rate), rate, time.clone()),
            limit,
            time,
        })
    }

    /// The rate it paces at now.
    #[cfg(test)]
    pub(in crate::transport) fn rate(&self) -> usize {
        self.bucket.rate()
    }

    /// Its burst now.
    #[cfg(test)]
    pub(in crate::transport) fn capacity(&self) -> usize {
        self.bucket.capacity()
    }
}

impl<T: TimeSource> SendPacer for LimitPacer<T> {
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        // The share moves with the number of connections; the limit is never
        // unset once set, since the settings do not change while running.
        if let Some(rate) = self.limit.rate() {
            let rate = bucket_rate(rate);
            if rate != self.bucket.rate() {
                self.bucket.set_rate(rate);
                // The burst stays a tenth of the rate: a share that fell keeps
                // no burst of the old share, and one that grew gets its own.
                self.bucket.set_capacity(capacity_for(rate));
            }
        }
        self.bucket.reserve_within(bytes, limit)
    }

    fn give_back(&self, bytes: usize) {
        self.bucket.give_back(bytes);
    }

    fn charge(&self, bytes: usize) -> usize {
        let rate = self.bucket.rate();
        let max_debt = usize::try_from(
            (rate as u128).saturating_mul(FALLBACK_DEBT.as_nanos()) / 1_000_000_000,
        )
        .unwrap_or(usize::MAX);
        self.bucket.charge_bounded(bytes, max_debt)
    }

    fn sleep(&self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.time.sleep_cancelable(wait)
    }
}

/// A path's pacer, for one send.
#[derive(Clone)]
pub(super) struct Pacing {
    pub(super) pacer: Arc<dyn SendPacer>,
}

/// Bytes taken from the pacer for a write not made yet. Dropped before
/// [`Self::spent`], they go back.
pub(super) struct PacedBytes {
    pacer: Arc<dyn SendPacer>,
    bytes: usize,
}

impl PacedBytes {
    /// The write was made: the bytes stay taken.
    pub(super) fn spent(mut self) {
        self.bytes = 0;
    }
}

impl Drop for PacedBytes {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.pacer.give_back(self.bytes);
        }
    }
}

/// What [`Pacing::take`] decided.
pub(super) enum Paced {
    /// The bytes are taken, after waiting `wait`.
    Taken { bytes: PacedBytes, wait: Duration },
    /// The wait would have reached `limit`: nothing taken.
    Refused,
}

impl Pacing {
    /// Take `bytes` with at most `limit` to wait. The wait itself is the
    /// caller's, so it can race whatever the caller must not miss; this only
    /// reserves.
    pub(super) fn take(&self, bytes: usize, limit: Duration) -> Paced {
        match self.pacer.reserve_within(bytes, limit) {
            Ok(wait) => Paced::Taken {
                bytes: PacedBytes {
                    pacer: Arc::clone(&self.pacer),
                    bytes,
                },
                wait,
            },
            Err(_) => Paced::Refused,
        }
    }
}
