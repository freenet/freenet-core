//! Sends with a deadline (#5940).
//!
//! A path's sends wait without limit against a peer that does not
//! cooperate (see the `path` module doc), so every send the node makes goes
//! through a `QuicPath::send_*_bounded` method, built on [`bounded_send`].
//! The outcome says what the sender can know:
//!
//! - [`BoundedSend::Sent`]: the peer's QUIC stack acknowledged every byte.
//!   Not a delivery (a full channel at the receiver drops an acknowledged
//!   message), as on UDP.
//! - [`BoundedSend::MaybeDelivered`]: every byte was written and the end of
//!   the stream sent, then the deadline passed or the connection was lost
//!   while waiting for the acknowledgement. The peer may have the whole
//!   message. Nothing is reset: the stream is left to finish.
//! - [`BoundedSend::Failed`]: the message did not reach the peer as a whole.
//!   A deadline that passes earlier drops the send: waiting for a
//!   streamed-message permit or a stream credit, no stream was opened;
//!   waiting for flow control, the stream is reset
//!   (`reset_code::SENDER_ABORTED`), so the receiver discards what arrived.
//!
//! A caller must not treat `MaybeDelivered` as `Failed`: the two sides may
//! disagree about whether the message arrived.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::QuicSendError;
use crate::simulation::TimeSource;

/// What became of a bounded send.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum BoundedSend {
    /// The peer acknowledged every byte.
    Sent,
    /// The whole message was written; whether it arrived is unknown.
    MaybeDelivered(MaybeDelivered),
    /// The message did not arrive whole.
    Failed(SendFailure),
}

/// Why a written message may or may not have arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaybeDelivered {
    /// The deadline passed while waiting for the acknowledgement.
    TimedOut,
    /// The connection was lost while waiting for the acknowledgement.
    ConnectionLost,
}

/// Why a bounded send failed.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum SendFailure {
    /// The deadline passed before the whole message was written; the
    /// stream was reset (or never opened).
    TimedOut,
    /// The send failed (`QuicSendError`), before the end of the stream was
    /// sent, or after it with a stop from the peer.
    Error(QuicSendError),
}

/// Whether a send has written its whole message and sent the end of its
/// stream: after that, the bytes may reach the peer whatever happens to
/// the sender. Set by the path when it finishes the stream.
#[derive(Debug, Clone, Default)]
pub(super) struct SendProgress(Arc<AtomicBool>);

impl SendProgress {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Every byte was written and the end of the stream sent.
    pub(super) fn mark_finished(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn finished(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Run the send `send` builds, until it returns or `timeout` passes on
/// `time`. `send` gets the [`SendProgress`] to pass to the path's `*_with`
/// send, which is how a timeout during the wait for the acknowledgement is
/// told apart from an earlier one. A closure that ignored it would report
/// such a timeout as `Failed`, so this is private to the path, whose
/// `send_*_bounded` methods pass it on.
///
/// Cancel safety: dropping this future drops the send, with the same
/// effect as a timeout at that point.
pub(super) async fn bounded_send<T, F, Fut>(time: &T, timeout: Duration, send: F) -> BoundedSend
where
    T: TimeSource,
    F: FnOnce(SendProgress) -> Fut,
    Fut: Future<Output = Result<(), QuicSendError>>,
{
    let progress = SendProgress::new();
    let send = send(progress.clone());
    tokio::select! {
        result = send => outcome(result, &progress),
        () = time.sleep(timeout) => {
            if progress.finished() {
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
            } else {
                BoundedSend::Failed(SendFailure::TimedOut)
            }
        }
    }
}

/// The outcome of a send that returned.
pub(super) fn outcome(result: Result<(), QuicSendError>, progress: &SendProgress) -> BoundedSend {
    match result {
        Ok(()) => BoundedSend::Sent,
        Err(QuicSendError::ConnectionLost(_)) if progress.finished() => {
            BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost)
        }
        Err(error) => BoundedSend::Failed(SendFailure::Error(error)),
    }
}
