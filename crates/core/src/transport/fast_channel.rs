//! High-performance bounded channel implementation for transport layer.
//!
//! This module provides a hybrid channel that combines:
//! - **crossbeam::channel** for fast, lock-free message passing (~2x faster than tokio::sync::mpsc)
//! - **tokio::sync::Notify** for async/await integration
//!
//! # Performance
//!
//! Benchmarks with 1M iterations of 1400-byte packets show:
//!
//! | Channel Type              | Throughput   |
//! |---------------------------|--------------|
//! | Crossbeam bounded(1000)   | ~2.88 Melem/s |
//! | Crossbeam bounded(100)    | ~2.49 Melem/s |
//! | Crossbeam unbounded       | ~2.84 Melem/s |
//! | Tokio MPSC (1000)         | ~1.33 Melem/s |
//!
//! # Design Decisions
//!
//! **Why bounded channels?**
//! - Unbounded channels risk OOM if sender outpaces receiver
//! - Bounded channels provide backpressure with negligible performance penalty (~1% vs unbounded)
//! - Ring buffer in bounded channels has slightly better cache locality
//!
//! **Why capacity 1000?**
//! - Sweet spot between throughput (larger = less contention) and memory usage
//! - ~15% better throughput than capacity 100
//! - ~1.4MB memory footprint per channel (1000 × 1400 byte packets)
//!
//! # Usage
//!
//! ```ignore
//! let (tx, rx) = fast_channel::bounded::<Packet>(1000);
//!
//! // Async send (yields if full)
//! tx.send_async(packet).await.unwrap();
//!
//! // Non-blocking try_send
//! tx.try_send(packet)?;
//!
//! // Async receive
//! let packet = rx.recv_async().await.unwrap();
//! ```

#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Error returned by `try_send`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The receiver has been dropped.
    Disconnected(T),
}

/// Error returned by `try_recv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// All senders have been dropped.
    Disconnected,
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

/// The sending half of a fast bounded channel.
///
/// This can be cloned to create multiple senders.
pub struct FastSender<T> {
    inner: crossbeam::channel::Sender<T>,
    /// Notifies senders that space is available (for backpressure)
    send_notify: Arc<Notify>,
    /// Tracks if the receiver has been dropped
    closed: Arc<AtomicBool>,
}

impl<T> Clone for FastSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            send_notify: self.send_notify.clone(),
            closed: self.closed.clone(),
        }
    }
}

impl<T> FastSender<T> {
    /// Sends a message synchronously (blocking).
    ///
    /// For bounded channels, this blocks if the channel is full.
    /// Prefer `send_async` in async contexts.
    #[cfg(test)]
    #[inline]
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self.inner.send(msg) {
            Ok(()) => Ok(()),
            Err(crossbeam::channel::SendError(msg)) => Err(SendError(msg)),
        }
    }

    /// Sends a message asynchronously with backpressure.
    ///
    /// For bounded channels, this yields if the channel is full,
    /// allowing other tasks to run while waiting for space.
    pub async fn send_async(&self, msg: T) -> Result<(), SendError<T>> {
        let mut msg = msg;
        loop {
            match self.inner.try_send(msg) {
                Ok(()) => {
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
            Ok(()) => Ok(()),
            Err(crossbeam::channel::TrySendError::Full(msg)) => Err(TrySendError::Full(msg)),
            Err(crossbeam::channel::TrySendError::Disconnected(msg)) => {
                Err(TrySendError::Disconnected(msg))
            }
        }
    }

    /// Returns `true` if the channel is full.
    #[cfg(test)]
    #[inline]
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns `true` if the receiver has been dropped.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// The receiving half of a fast bounded channel.
pub struct FastReceiver<T> {
    inner: crossbeam::channel::Receiver<T>,
    /// Notifies senders that space is available
    send_notify: Arc<Notify>,
    /// Tracks if the receiver has been dropped (shared with senders for is_closed())
    closed: Arc<AtomicBool>,
    /// Counts recv_async poll iterations (test-only, for verifying no busy-loop)
    #[cfg(test)]
    poll_count: Arc<AtomicU64>,
}

impl<T> Drop for FastReceiver<T> {
    fn drop(&mut self) {
        // Mark channel as closed so senders can detect it via is_closed()
        self.closed.store(true, Ordering::Release);
        // Wake any senders waiting on backpressure so they can detect disconnection
        self.send_notify.notify_waiters();
    }
}

impl<T> FastReceiver<T> {
    /// Receives a message asynchronously.
    ///
    /// This is the primary receive method for async contexts.
    /// Uses exponential backoff when the channel is empty.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        let mut backoff_us = 1u64;
        const MAX_BACKOFF_US: u64 = 1000; // 1ms max

        loop {
            #[cfg(test)]
            self.poll_count.fetch_add(1, Ordering::Relaxed);

            match self.inner.try_recv() {
                Ok(msg) => {
                    // Notify senders that space is available
                    self.send_notify.notify_one();
                    return Ok(msg);
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    // Channel is empty - wait with exponential backoff
                    tokio::time::sleep(std::time::Duration::from_micros(backoff_us)).await;
                    backoff_us = (backoff_us * 2).min(MAX_BACKOFF_US);
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    return Err(RecvError);
                }
            }
        }
    }

    /// Returns the number of poll iterations in recv_async (test-only).
    #[cfg(test)]
    pub fn poll_count(&self) -> u64 {
        self.poll_count.load(Ordering::Relaxed)
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

    /// Attempts to receive a message without blocking.
    ///
    /// Returns immediately with `Ok(msg)` if a message is available,
    /// or `Err(TryRecvError::Empty)` if the channel is empty,
    /// or `Err(TryRecvError::Disconnected)` if all senders have been dropped.
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
}

/// Creates a bounded fast channel with the given capacity.
///
/// Bounded channels provide backpressure - senders will block/yield when the
/// channel is full, preventing unbounded memory growth.
///
/// # Example
///
/// ```ignore
/// let (tx, rx) = fast_channel::bounded::<i32>(1000);
/// tx.send_async(42).await.unwrap();
/// assert_eq!(rx.recv_async().await.unwrap(), 42);
/// ```
pub fn bounded<T>(capacity: usize) -> (FastSender<T>, FastReceiver<T>) {
    let (tx, rx) = crossbeam::channel::bounded(capacity);
    let send_notify = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));

    let sender = FastSender {
        inner: tx,
        send_notify: send_notify.clone(),
        closed: closed.clone(),
    };

    let receiver = FastReceiver {
        inner: rx,
        send_notify,
        closed,
        #[cfg(test)]
        poll_count: Arc::new(AtomicU64::new(0)),
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_recv() {
        let (tx, rx) = bounded::<i32>(10);

        tx.send(42).unwrap();
        tx.send(43).unwrap();

        assert_eq!(rx.recv_async().await.unwrap(), 42);
        assert_eq!(rx.recv_async().await.unwrap(), 43);
    }

    #[tokio::test]
    async fn test_async_send() {
        let (tx, rx) = bounded::<i32>(10);

        tx.send_async(42).await.unwrap();
        assert_eq!(rx.recv_async().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_backpressure() {
        let (tx, rx) = bounded::<i32>(2);

        // Fill the channel
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert!(tx.is_full());

        // Drain one to make space
        assert_eq!(rx.recv_async().await.unwrap(), 1);
        assert!(!tx.is_full());

        // Can send again
        tx.send(3).unwrap();
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
    async fn test_is_closed() {
        let (tx, rx) = bounded::<i32>(10);

        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
    }

    #[tokio::test]
    async fn test_concurrent_send_recv() {
        let (tx, rx) = bounded::<i32>(100);
        let count = 1000;

        let sender = tokio::spawn(async move {
            for i in 0..count {
                tx.send_async(i).await.unwrap();
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

    #[tokio::test]
    async fn test_recv_async_no_busy_loop() {
        // Verify recv_async uses exponential backoff, not busy-looping.
        // With 1s wait and 1ms max backoff, expect ~1000 iterations max.
        // A busy-loop with yield_now() would do millions of iterations.
        let (tx, rx) = bounded::<i32>(10);

        // Spawn sender that waits 1 second before sending
        let sender = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            tx.send_async(42).await.unwrap();
        });

        // Receive (will wait ~1s for the message)
        let result = rx.recv_async().await.unwrap();
        assert_eq!(result, 42);

        sender.await.unwrap();

        // Check poll count - with exponential backoff (1μs -> 1ms max),
        // 1 second of waiting should result in roughly:
        // - First 10 iterations: 1+2+4+8+16+32+64+128+256+512 = ~1ms total
        // - Remaining ~999ms at 1ms each = ~999 iterations
        // Total: ~1009 iterations, let's say < 2000 to be safe
        let polls = rx.poll_count();
        assert!(
            polls < 2000,
            "Too many poll iterations ({polls}), likely busy-looping"
        );
        // Also verify we actually polled (not zero)
        assert!(polls > 0, "Should have polled at least once");
    }
}
