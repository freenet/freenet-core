//! DIAGNOSTIC, tests only: a per-stream timeline of the inbound QUIC path,
//! for finding where a stream whose bytes were all acknowledged spends the
//! consumer's 5 s gap (`tests::hold_realtime`). Off unless [`enable`] was
//! called; then every event takes one lock, which is fine for a measurement
//! and nothing else.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

static ON: AtomicBool = AtomicBool::new(false);
static EVENTS: parking_lot::Mutex<Vec<Event>> = parking_lot::Mutex::new(Vec::new());

/// One thing that happened to a stream or a connection's receive task.
#[derive(Debug, Clone)]
pub(crate) struct Event {
    /// Microseconds since [`enable`].
    pub at_us: u64,
    /// The receiving connection (its `Receiver`'s address).
    pub conn: usize,
    /// The stream, or `u32::MAX` for a receive-task event.
    pub stream: u32,
    pub kind: Kind,
}

#[derive(Debug, Clone)]
pub(crate) enum Kind {
    /// The receive task accepted a stream (before its header is read).
    Accepted,
    /// A chunk read from quinn (stream offset, length).
    Read { offset: u64, len: u32 },
    /// A fragment pushed into the stream's handle.
    Push { number: u32 },
    /// The stream waited for room in the delivery channel.
    WaitedForRoom,
    /// The stream was handed to the consumer channel.
    Announced,
    /// The reader read the stream's end.
    End,
    /// The reader stopped the stream early, and why.
    Failed(String),
    /// The receive task was not polled for this long (microseconds), ending
    /// at `at_us`.
    PollGap { gap_us: u64 },
    /// Test side: the consumer took the arrival off the channel.
    ConsumerTook,
    /// Test side: the consumer's `assemble` returned.
    Assembled { ok: bool },
    /// Test side: the sender's bounded send returned (on the sending side).
    SendReturned { outcome: String },
}

fn epoch() -> &'static std::time::Instant {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    #[allow(clippy::disallowed_methods)] // a real clock, on purpose: a diagnostic
    EPOCH.get_or_init(std::time::Instant::now)
}

/// Start recording, clearing anything recorded before.
pub(crate) fn enable() {
    epoch();
    EVENTS.lock().clear();
    ON.store(true, Ordering::SeqCst);
}

/// Stop recording and return what was recorded.
pub(crate) fn take() -> Vec<Event> {
    ON.store(false, Ordering::SeqCst);
    std::mem::take(&mut *EVENTS.lock())
}

pub(crate) fn on() -> bool {
    ON.load(Ordering::Relaxed)
}

pub(crate) fn now_us() -> u64 {
    epoch().elapsed().as_micros() as u64
}

pub(crate) fn record(conn: usize, stream: u32, kind: Kind) {
    if on() {
        let at_us = now_us();
        EVENTS.lock().push(Event {
            at_us,
            conn,
            stream,
            kind,
        });
    }
}

/// A future that records each gap of at least [`POLL_GAP_US`] between two of
/// its polls.
pub(crate) struct PollGaps<F> {
    pub conn: usize,
    pub last_us: u64,
    pub inner: std::pin::Pin<Box<F>>,
}

pub(crate) const POLL_GAP_US: u64 = 250_000;

impl<F: std::future::Future> std::future::Future for PollGaps<F> {
    type Output = F::Output;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        if on() {
            let now = now_us();
            if self.last_us != 0 && now - self.last_us >= POLL_GAP_US {
                record(
                    self.conn,
                    u32::MAX,
                    Kind::PollGap {
                        gap_us: now - self.last_us,
                    },
                );
            }
            self.last_us = now;
        }
        self.inner.as_mut().poll(cx)
    }
}
