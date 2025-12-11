//! High-performance channel implementation for transport layer.
//!
//! This module provides a hybrid channel that combines:
//! - **crossbeam::channel** for fast, lock-free message passing (~4.4x faster than tokio::sync::mpsc)
//! - **tokio::sync::Notify** for async/await integration
//!
//! # Performance
//!
//! Benchmarks show tokio::sync::mpsc achieves ~2.24 Melem/s while crossbeam achieves ~9.98 Melem/s.
//! This hybrid approach captures most of crossbeam's performance while maintaining tokio compatibility.
//!
//! # Usage
//!
//! ```ignore
//! let (tx, rx) = fast_channel::bounded::<Packet>(100);
//!
//! // Sync send (fastest, use when not in async context)
//! tx.send(packet).unwrap();
//!
//! // Async send (for backpressure in async context)
//! tx.send_async(packet).await.unwrap();
//!
//! // Async receive
//! let packet = rx.recv_async().await.unwrap();
//! ```

use std::sync::Arc;
use tokio::sync::Notify;

/// Error returned when the channel is disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel disconnected")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned when receiving from a disconnected channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel disconnected")
    }
}

impl std::error::Error for RecvError {}

/// The sending half of a fast channel.
///
/// This can be cloned to create multiple senders.
pub struct FastSender<T> {
    inner: crossbeam::channel::Sender<T>,
    /// Notifies receivers that a message is available
    recv_notify: Arc<Notify>,
    /// Notifies senders that space is available (for bounded channels)
    send_notify: Arc<Notify>,
}

impl<T> Clone for FastSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            recv_notify: self.recv_notify.clone(),
            send_notify: self.send_notify.clone(),
        }
    }
}

impl<T> Drop for FastSender<T> {
    fn drop(&mut self) {
        // When sender drops, wake any waiting receivers so they can detect disconnection
        self.recv_notify.notify_waiters();
    }
}

impl<T> FastSender<T> {
    /// Sends a message synchronously.
    ///
    /// This is the fastest send method. Use when:
    /// - You're not in an async context
    /// - You don't need backpressure (willing to block)
    ///
    /// For bounded channels, this will block if the channel is full.
    #[inline]
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self.inner.send(msg) {
            Ok(()) => {
                // Use notify_waiters to ensure receiver wakes up even in high-throughput scenarios
                self.recv_notify.notify_waiters();
                Ok(())
            }
            Err(crossbeam::channel::SendError(msg)) => Err(SendError(msg)),
        }
    }

    /// Sends a message asynchronously with backpressure.
    ///
    /// For bounded channels, this will yield if the channel is full,
    /// allowing other tasks to run while waiting for space.
    pub async fn send_async(&self, msg: T) -> Result<(), SendError<T>> {
        let mut msg = msg;
        loop {
            match self.inner.try_send(msg) {
                Ok(()) => {
                    self.recv_notify.notify_waiters();
                    return Ok(());
                }
                Err(crossbeam::channel::TrySendError::Full(returned)) => {
                    msg = returned;
                    // Wait for space to become available
                    self.send_notify.notified().await;
                }
                Err(crossbeam::channel::TrySendError::Disconnected(returned)) => {
                    return Err(SendError(returned));
                }
            }
        }
    }

    /// Attempts to send a message without blocking.
    ///
    /// Returns `Ok(())` if sent, `Err(TrySendError::Full(msg))` if channel is full,
    /// or `Err(TrySendError::Disconnected(msg))` if the receiver is dropped.
    #[inline]
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        match self.inner.try_send(msg) {
            Ok(()) => {
                self.recv_notify.notify_waiters();
                Ok(())
            }
            Err(crossbeam::channel::TrySendError::Full(msg)) => Err(TrySendError::Full(msg)),
            Err(crossbeam::channel::TrySendError::Disconnected(msg)) => {
                Err(TrySendError::Disconnected(msg))
            }
        }
    }

    /// Returns `true` if the channel is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the channel is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Error returned by `try_send`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The receiver has been dropped.
    Disconnected(T),
}

impl<T> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "channel full"),
            TrySendError::Disconnected(_) => write!(f, "channel disconnected"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for TrySendError<T> {}

/// The receiving half of a fast channel.
pub struct FastReceiver<T> {
    inner: crossbeam::channel::Receiver<T>,
    /// Notifies receivers that a message is available.
    /// Currently unused (using yield_now instead) but kept for potential future optimization.
    #[allow(dead_code)]
    recv_notify: Arc<Notify>,
    /// Notifies senders that space is available (for bounded channels)
    send_notify: Arc<Notify>,
}

impl<T> FastReceiver<T> {
    /// Receives a message asynchronously.
    ///
    /// This is the primary receive method for async contexts.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        loop {
            // Try to receive
            match self.inner.try_recv() {
                Ok(msg) => {
                    self.send_notify.notify_one();
                    return Ok(msg);
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    // Channel is empty - yield to let other tasks run, then check again
                    // This is more efficient than pure spinning while still being responsive
                    tokio::task::yield_now().await;
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    return Err(RecvError);
                }
            }
        }
    }

    /// Attempts to receive a message without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.inner.try_recv() {
            Ok(msg) => {
                self.send_notify.notify_one();
                Ok(msg)
            }
            Err(crossbeam::channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(crossbeam::channel::TryRecvError::Disconnected) => Err(TryRecvError::Disconnected),
        }
    }

    /// Receives a message synchronously (blocking).
    ///
    /// Use sparingly in async contexts as this will block the thread.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.inner.recv() {
            Ok(msg) => {
                self.send_notify.notify_one();
                Ok(msg)
            }
            Err(crossbeam::channel::RecvError) => Err(RecvError),
        }
    }

    /// Returns `true` if the channel is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Error returned by `try_recv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// The sender has been dropped.
    Disconnected,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "channel empty"),
            TryRecvError::Disconnected => write!(f, "channel disconnected"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Creates a bounded fast channel with the given capacity.
///
/// # Example
///
/// ```ignore
/// let (tx, rx) = fast_channel::bounded::<i32>(100);
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv_async().await.unwrap(), 42);
/// ```
pub fn bounded<T>(capacity: usize) -> (FastSender<T>, FastReceiver<T>) {
    let (tx, rx) = crossbeam::channel::bounded(capacity);
    let recv_notify = Arc::new(Notify::new());
    let send_notify = Arc::new(Notify::new());

    let sender = FastSender {
        inner: tx,
        recv_notify: recv_notify.clone(),
        send_notify: send_notify.clone(),
    };

    let receiver = FastReceiver {
        inner: rx,
        recv_notify,
        send_notify,
    };

    (sender, receiver)
}

/// Creates an unbounded fast channel.
///
/// # Warning
///
/// Unbounded channels can grow without limit if the receiver is slower than
/// the sender. Use bounded channels with backpressure for production workloads.
///
/// # Example
///
/// ```ignore
/// let (tx, rx) = fast_channel::unbounded::<i32>();
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv_async().await.unwrap(), 42);
/// ```
pub fn unbounded<T>() -> (FastSender<T>, FastReceiver<T>) {
    let (tx, rx) = crossbeam::channel::unbounded();
    let recv_notify = Arc::new(Notify::new());
    let send_notify = Arc::new(Notify::new());

    let sender = FastSender {
        inner: tx,
        recv_notify: recv_notify.clone(),
        send_notify: send_notify.clone(),
    };

    let receiver = FastReceiver {
        inner: rx,
        recv_notify,
        send_notify,
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bounded_send_recv() {
        let (tx, rx) = bounded::<i32>(10);

        tx.send(42).unwrap();
        tx.send(43).unwrap();

        assert_eq!(rx.recv_async().await.unwrap(), 42);
        assert_eq!(rx.recv_async().await.unwrap(), 43);
    }

    #[tokio::test]
    async fn test_unbounded_send_recv() {
        let (tx, rx) = unbounded::<i32>();

        for i in 0..1000 {
            tx.send(i).unwrap();
        }

        for i in 0..1000 {
            assert_eq!(rx.recv_async().await.unwrap(), i);
        }
    }

    #[tokio::test]
    async fn test_async_send() {
        let (tx, rx) = bounded::<i32>(10);

        tx.send_async(42).await.unwrap();
        assert_eq!(rx.recv_async().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_try_send_full() {
        let (tx, rx) = bounded::<i32>(2);

        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(TrySendError::Full(3))));

        // Drain one
        assert_eq!(rx.recv_async().await.unwrap(), 1);

        // Now we can send again
        assert!(tx.try_send(3).is_ok());
    }

    #[tokio::test]
    async fn test_sender_clone() {
        let (tx, rx) = bounded::<i32>(10);
        let tx2 = tx.clone();

        tx.send(1).unwrap();
        tx2.send(2).unwrap();

        assert_eq!(rx.recv_async().await.unwrap(), 1);
        assert_eq!(rx.recv_async().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_disconnection() {
        let (tx, rx) = bounded::<i32>(10);

        tx.send(42).unwrap();
        drop(tx);

        // Can still receive buffered messages
        assert_eq!(rx.recv_async().await.unwrap(), 42);

        // Then get disconnected error
        assert!(rx.recv_async().await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_send_recv() {
        // Use unbounded to avoid blocking sender
        let (tx, rx) = unbounded::<i32>();
        let count = 1000;

        let sender = tokio::spawn(async move {
            for i in 0..count {
                tx.send(i).unwrap();
            }
        });

        let receiver = tokio::spawn(async move {
            let mut received = 0;
            while rx.recv_async().await.is_ok() {
                received += 1;
                if received == count {
                    break;
                }
            }
            received
        });

        sender.await.unwrap();
        let received = receiver.await.unwrap();
        assert_eq!(received, count);
    }
}
