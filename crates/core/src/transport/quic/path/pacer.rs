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
//! per-connection share), a path's data sends draw on the connection's token
//! bucket, the one its UDP stream fragments draw on, so both transports of one
//! peer share one rate. Charged: every write of a Stream or pipe (header,
//! metadata and payload), and of a Short past one packet, which UDP sends as a
//! stream. Not charged, as on UDP: Shorts of one packet, and control frames,
//! which must never wait behind data. With no limit nothing is paced: the
//! bucket then runs at the congestion controller's rate, which is not a limit
//! anyone set.
//!
//! A write takes its bytes before it is made ([`SendPacer::reserve_within`])
//! and waits out the bucket's deficit in the send's own task, never in the
//! connection's task. A reservation whose wait would reach the send's current
//! deadline is not made, as on UDP (`TokenBucket::reserve_within`): a refused
//! reservation takes no place, so it does not delay the sends behind it. The
//! send then ends as at that deadline: [`SendFailure::NotOpened`] before its
//! stream opened (the message may go on UDP, where the same bucket decides),
//! and [`SendFailure::TimedOut`] after (the stream is reset). Bytes taken for a
//! write that is not made go back to the bucket.
//!
//! A wait or a refusal is a local hold: the path's stall count takes it as
//! progress (`route::StallCount`), as it does an egress cap hold, so a binding
//! limit never closes a busy path.
//!
//! The bucket and its waits are on the connection's clock, as UDP's are. The
//! send's deadlines are on the path's clock; the two run at one rate in
//! production. In simulation the path runs on the network's virtual clock and
//! the connection on real time, so a paced send there waits in real time.
//!
//! [`SendFailure::NotOpened`]: super::bounded::SendFailure::NotOpened
//! [`SendFailure::TimedOut`]: super::bounded::SendFailure::TimedOut

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::simulation::TimeSource;
use crate::transport::token_bucket::TokenBucket;

/// What paces a path's data sends.
pub(in crate::transport) trait SendPacer: Send + Sync {
    /// Take `bytes` if the wait for them is shorter than `limit`: `Ok(wait)`
    /// with the bytes taken, or `Err(wait)`, the wait it would have had, with
    /// nothing taken.
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration>;

    /// Return `bytes` taken for a write that was not made.
    fn give_back(&self, bytes: usize);

    /// Sleep for `wait` on the pacer's clock. Dropping it ends the sleep.
    fn sleep(&self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// A connection's token bucket as a path's pacer.
pub(in crate::transport) struct BucketPacer<T: TimeSource> {
    bucket: Arc<TokenBucket<T>>,
    time: T,
}

impl<T: TimeSource> BucketPacer<T> {
    pub(in crate::transport) fn new(bucket: Arc<TokenBucket<T>>, time: T) -> Self {
        Self { bucket, time }
    }
}

impl<T: TimeSource> SendPacer for BucketPacer<T> {
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        self.bucket.reserve_within(bytes, limit)
    }

    fn give_back(&self, bytes: usize) {
        self.bucket.give_back(bytes);
    }

    fn sleep(&self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.time.sleep_cancelable(wait)
    }
}

/// A path's pacer and its count of holds, which the stall count reads.
#[derive(Clone)]
pub(super) struct Pacing {
    pub(super) pacer: Arc<dyn SendPacer>,
    pub(super) holds: Arc<AtomicU64>,
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
    /// reserves and counts the hold.
    pub(super) fn take(&self, bytes: usize, limit: Duration) -> Paced {
        match self.pacer.reserve_within(bytes, limit) {
            Ok(wait) => {
                if !wait.is_zero() {
                    self.holds.fetch_add(1, Ordering::Relaxed);
                }
                Paced::Taken {
                    bytes: PacedBytes {
                        pacer: Arc::clone(&self.pacer),
                        bytes,
                    },
                    wait,
                }
            }
            Err(_) => {
                self.holds.fetch_add(1, Ordering::Relaxed);
                Paced::Refused
            }
        }
    }
}
