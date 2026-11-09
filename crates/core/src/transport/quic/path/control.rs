//! The path's long-lived control stream (#5940, `control::caps::CONTROL_STREAM`).
//!
//! When both sides set the bit, each opens one uni stream as soon as its side
//! of the path is active ([`QuicPath::open_control_stream_bounded`]), before
//! any data stream: data sends wait in [`QuicPath::open`] until it is open
//! ([`ControlSide::wait_settled`]). The stream starts with a
//! `FrameKind::ControlStream` header and then carries control payloads, each
//! as a big-endian `u16` length (1 to `MAX_CONTROL_BYTES`) and that many bytes.
//! It is never finished while the path lives, so it holds its stream slot for
//! the path's life and never waits for MAX_STREAMS again, whatever the data
//! streams hold.
//!
//! A control write competes with data writes for the connection's flow-control
//! credit and the send window. quinn wakes every blocked writer when credit
//! returns, and a data writer writes one fragment per write call
//! (`Outgoing::write_fragments`), so while a control write is blocked,
//! data writers wait at the [`ControlGate`] between fragments: at most one
//! fragment per data writer already past the gate goes ahead of the control
//! write. The gate holds only while a control write is blocked, never for a
//! stream slot, so it cannot deadlock against `open_uni`. Once written, the
//! control stream's priority ([`CONTROL_STREAM_PRIORITY`]) puts its bytes in the
//! next packet rather than behind buffered data.
//!
//! [`QuicPath::open_control_stream_bounded`]: super::QuicPath::open_control_stream_bounded
//! [`QuicPath::open`]: super::QuicPath

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

use super::super::framing::MAX_CONTROL_BYTES;

/// quinn's send priority for the control stream; data streams keep quinn's
/// default, 0.
pub(crate) const CONTROL_STREAM_PRIORITY: i32 = 1;

/// Bytes of the length that prefixes each control payload on the stream.
pub(crate) const CONTROL_MESSAGE_LEN_BYTES: usize = 2;

/// Whether this path's control stream is expected, being opened, or settled.
#[derive(Debug)]
enum Phase {
    /// Both sides did not set the bit (yet): data sends do not wait for it.
    NotExpected,
    /// Being opened: data sends wait.
    Opening,
    /// Open, with its send side.
    Open(Arc<tokio::sync::Mutex<quinn::SendStream>>),
    /// Opening failed; the path is being closed.
    Failed,
}

/// This side's control stream: its phase and the gate data writers wait at.
#[derive(Debug)]
pub(super) struct ControlSide {
    phase: parking_lot::Mutex<Phase>,
    settled: Notify,
    gate: Arc<ControlGate>,
}

impl ControlSide {
    pub(super) fn new() -> Self {
        Self {
            phase: parking_lot::Mutex::new(Phase::NotExpected),
            settled: Notify::new(),
            gate: Arc::new(ControlGate::default()),
        }
    }

    /// The control stream will be opened: data sends wait for it from now.
    /// True the first time, false if it was already expected.
    pub(super) fn expect(&self) -> bool {
        let mut phase = self.phase.lock();
        match *phase {
            Phase::NotExpected => {
                *phase = Phase::Opening;
                true
            }
            Phase::Opening | Phase::Open(_) | Phase::Failed => false,
        }
    }

    pub(super) fn opened(&self, send: quinn::SendStream) {
        *self.phase.lock() = Phase::Open(Arc::new(tokio::sync::Mutex::new(send)));
        self.settled.notify_waiters();
    }

    pub(super) fn failed(&self) {
        *self.phase.lock() = Phase::Failed;
        self.settled.notify_waiters();
    }

    /// Return once the control stream is open or failed, at once if it is not
    /// expected. A failure closes the path, so a send that goes on then finds
    /// the connection closed.
    pub(super) async fn wait_settled(&self) {
        loop {
            let notified = self.settled.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if !matches!(*self.phase.lock(), Phase::Opening) {
                return;
            }
            notified.await;
        }
    }

    /// The open stream's send side, if it is open.
    pub(super) fn stream(&self) -> Option<Arc<tokio::sync::Mutex<quinn::SendStream>>> {
        match &*self.phase.lock() {
            Phase::Open(send) => Some(Arc::clone(send)),
            Phase::NotExpected | Phase::Opening | Phase::Failed => None,
        }
    }

    pub(super) fn gate(&self) -> Arc<ControlGate> {
        Arc::clone(&self.gate)
    }
}

/// Where data writers wait while a control write is blocked on credit.
#[derive(Debug, Default)]
pub(crate) struct ControlGate {
    /// Control writes blocked now.
    blocked: AtomicUsize,
    released: Notify,
}

impl ControlGate {
    /// Return once no control write is blocked. Cancel-safe.
    pub(super) async fn wait_clear(&self) {
        loop {
            let released = self.released.notified();
            let mut released = std::pin::pin!(released);
            released.as_mut().enable();
            if self.blocked.load(Ordering::Acquire) == 0 {
                return;
            }
            released.await;
        }
    }

    /// A control write is blocked until the returned hold drops.
    pub(super) fn hold(self: &Arc<Self>) -> GateHold {
        self.blocked.fetch_add(1, Ordering::AcqRel);
        GateHold(Arc::clone(self))
    }

    /// Whether a control write is blocked now. Tests only.
    #[cfg(test)]
    pub(super) fn is_held(&self) -> bool {
        self.blocked.load(Ordering::Acquire) > 0
    }
}

/// Holds the [`ControlGate`] shut; dropping it (the write finished, failed or
/// was cancelled) lets data writers go on.
#[derive(Debug)]
pub(super) struct GateHold(Arc<ControlGate>);

impl Drop for GateHold {
    fn drop(&mut self) {
        if self.0.blocked.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.released.notify_waiters();
        }
    }
}

/// Why a control payload cannot go on the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ControlMessageError {
    #[error("control payload of {0} bytes, expected 1 to {MAX_CONTROL_BYTES}")]
    Length(usize),
}

/// `payload` as it goes on the stream: its length, then its bytes.
pub(crate) fn frame_message(payload: &[u8]) -> Result<Vec<u8>, ControlMessageError> {
    let len = payload.len();
    if len == 0 || len as u64 > MAX_CONTROL_BYTES {
        return Err(ControlMessageError::Length(len));
    }
    // At most MAX_CONTROL_BYTES (4 KiB), checked above, so it fits a u16.
    let prefix = (len as u16).to_be_bytes();
    let mut out = Vec::with_capacity(CONTROL_MESSAGE_LEN_BYTES + len);
    out.extend_from_slice(&prefix);
    out.extend_from_slice(payload);
    Ok(out)
}

/// A payload length read off the stream, if it is one a sender may write.
pub(crate) fn message_len(prefix: [u8; CONTROL_MESSAGE_LEN_BYTES]) -> Option<usize> {
    let len = u16::from_be_bytes(prefix);
    (len != 0 && u64::from(len) <= MAX_CONTROL_BYTES).then_some(usize::from(len))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A payload goes on the stream as its big-endian length and its bytes;
    /// empty and oversized payloads are refused both ways.
    #[test]
    fn control_messages_are_length_prefixed_and_bounded() {
        assert_eq!(frame_message(b"abc"), Ok(vec![0, 3, b'a', b'b', b'c']));
        let largest = vec![7u8; MAX_CONTROL_BYTES as usize];
        let framed = frame_message(&largest).expect("the largest payload frames");
        assert_eq!(&framed[..2], &(MAX_CONTROL_BYTES as u16).to_be_bytes());
        assert_eq!(framed.len(), 2 + largest.len());
        assert_eq!(frame_message(&[]), Err(ControlMessageError::Length(0)));
        let over = MAX_CONTROL_BYTES as usize + 1;
        assert_eq!(
            frame_message(&vec![0u8; over]),
            Err(ControlMessageError::Length(over))
        );
        assert_eq!(message_len([0, 3]), Some(3));
        assert_eq!(
            message_len((MAX_CONTROL_BYTES as u16).to_be_bytes()),
            Some(MAX_CONTROL_BYTES as usize)
        );
        assert_eq!(message_len([0, 0]), None);
        assert_eq!(
            message_len(((MAX_CONTROL_BYTES + 1) as u16).to_be_bytes()),
            None
        );
        assert_eq!(message_len([0xff, 0xff]), None);
    }

    /// The gate is clear with no hold, shut while any hold lives, and wakes
    /// its waiters when the last one drops.
    #[tokio::test]
    async fn the_gate_holds_writers_only_while_a_control_write_is_blocked() {
        let gate = Arc::new(ControlGate::default());
        gate.wait_clear().await;
        let first = gate.hold();
        let second = gate.hold();
        assert!(gate.is_held());
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.wait_clear().await })
        };
        tokio::task::yield_now().await;
        drop(first);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "one hold is left");
        drop(second);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("released when the last hold drops")
            .expect("the waiter ran");
        assert!(!gate.is_held());
    }

    /// Data sends wait while the stream is being opened and go on once it is
    /// open or failed; with no stream expected they do not wait at all.
    #[tokio::test]
    async fn data_sends_wait_only_while_the_stream_is_being_opened() {
        let side = Arc::new(ControlSide::new());
        side.wait_settled().await;
        assert!(side.expect());
        assert!(!side.expect(), "expected once");
        let waiter = {
            let side = Arc::clone(&side);
            tokio::spawn(async move { side.wait_settled().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        side.failed();
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("released when the open settles")
            .expect("the waiter ran");
        side.wait_settled().await;
    }
}
