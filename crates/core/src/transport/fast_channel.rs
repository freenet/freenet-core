//! High-performance bounded channel implementation for transport layer.
//!
//! This module provides a hybrid channel that combines:
//! - **crossbeam::channel** for fast, lock-free message passing (~2x faster than tokio::sync::mpsc)
//! - **Exponential backoff** (1µs to 50ms) for async receive polling
//! - **tokio::sync::Notify** for instant sender-disconnect detection
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
    /// Notifies receiver on sender disconnect (fired from Drop)
    recv_notify: Arc<Notify>,
    /// Tracks if the receiver has been dropped
    closed: Arc<AtomicBool>,
}

impl<T> Clone for FastSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            send_notify: self.send_notify.clone(),
            recv_notify: self.recv_notify.clone(),
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
    pub async fn send_async(&self, mut msg: T) -> Result<(), SendError<T>> {
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
    /// Notifies this receiver on sender disconnect
    recv_notify: Arc<Notify>,
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

impl<T> Drop for FastSender<T> {
    fn drop(&mut self) {
        // Wake the receiver so it can detect sender disconnection
        self.recv_notify.notify_one();
    }
}

impl<T> FastReceiver<T> {
    /// Receives a message asynchronously.
    ///
    /// This is the primary receive method for async contexts.
    /// Uses exponential backoff (1µs to 50ms) when the channel is empty,
    /// with instant wakeup via [`Notify`] when all senders disconnect.
    ///
    /// The 50ms cap keeps idle-channel overhead low (~20 polls/sec per
    /// channel) while maintaining sub-millisecond latency for active
    /// connections (backoff resets to 1µs on each successful receive).
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        let mut backoff_us = 1u64;
        const MAX_BACKOFF_US: u64 = 50_000; // 50ms max

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
                    // Wait with exponential backoff, but also listen for
                    // sender disconnection via Notify (fired from Drop).
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep(std::time::Duration::from_micros(backoff_us)) => {
                            backoff_us = (backoff_us * 2).min(MAX_BACKOFF_US);
                        }
                        _ = self.recv_notify.notified() => {
                            // Sender disconnected (or spurious wake) — recheck channel
                        }
                    }
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
    let recv_notify = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));

    let sender = FastSender {
        inner: tx,
        send_notify: send_notify.clone(),
        recv_notify: recv_notify.clone(),
        closed: closed.clone(),
    };

    let receiver = FastReceiver {
        inner: rx,
        send_notify,
        recv_notify,
        closed,
        #[cfg(test)]
        poll_count: Arc::new(AtomicU64::new(0)),
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;

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

        let sender = GlobalExecutor::spawn(async move {
            for i in 0..count {
                tx.send_async(i).await.unwrap();
            }
        });

        let receiver = GlobalExecutor::spawn(async move {
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
        // Verify recv_async uses exponential backoff, not tight-loop polling.
        // With 50ms max backoff and a 1-second wait, we expect ~30 iterations
        // (exponential series 1us..50ms plus steady-state at 50ms).
        let (tx, rx) = bounded::<i32>(10);

        // Spawn sender that waits 1 second before sending
        let sender = GlobalExecutor::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            tx.send_async(42).await.unwrap();
        });

        // Receive (will wait ~1s for the message)
        let result = rx.recv_async().await.unwrap();
        assert_eq!(result, 42);

        sender.await.unwrap();

        // With 50ms max backoff over ~1s, expect ~30-35 iterations:
        // ~17 during exponential ramp-up (1us to 50ms ≈ 100ms total)
        // ~18 at steady-state (900ms / 50ms)
        let polls = rx.poll_count();
        assert!(
            polls < 100,
            "Too many poll iterations ({polls}), backoff may not be working"
        );
        assert!(polls > 0, "Should have polled at least once");
    }

    #[tokio::test]
    async fn test_sender_drop_wakes_blocked_receiver() {
        // Verify that dropping all senders wakes a receiver blocked in recv_async,
        // allowing it to detect disconnection and return RecvError.
        let (tx, rx) = bounded::<i32>(10);

        let receiver = GlobalExecutor::spawn(async move {
            // No messages sent — receiver will block on notified().await
            rx.recv_async().await
        });

        // Give receiver time to park on notified()
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(tx);

        let result = receiver.await.unwrap();
        assert!(
            result.is_err(),
            "Should return RecvError after all senders drop"
        );
    }

    #[tokio::test]
    async fn test_multiple_concurrent_senders() {
        // Verify no messages are lost when multiple cloned senders send concurrently.
        let (tx, rx) = bounded::<i32>(100);
        let n_senders = 5;
        let msgs_per_sender = 200;

        let handles: Vec<_> = (0..n_senders)
            .map(|_| {
                let tx = tx.clone();
                GlobalExecutor::spawn(async move {
                    for i in 0..msgs_per_sender {
                        tx.send_async(i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx); // drop original so channel closes when all clones finish

        let receiver = GlobalExecutor::spawn(async move {
            let mut count = 0;
            while rx.recv_async().await.is_ok() {
                count += 1;
            }
            count
        });

        for h in handles {
            h.await.unwrap();
        }
        let count = receiver.await.unwrap();
        assert_eq!(count, n_senders * msgs_per_sender);
    }

    #[tokio::test]
    async fn stress_high_concurrency_small_channel() {
        // 50 senders, 1000 msgs each, tiny channel (capacity 5).
        // Forces heavy backpressure contention on every send.
        let (tx, rx) = bounded::<u64>(5);
        let n_senders = 50;
        let msgs_per_sender = 1000;

        let handles: Vec<_> = (0..n_senders)
            .map(|sender_id| {
                let tx = tx.clone();
                GlobalExecutor::spawn(async move {
                    for i in 0..msgs_per_sender {
                        tx.send_async(sender_id * msgs_per_sender + i)
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        drop(tx);

        let receiver = GlobalExecutor::spawn(async move {
            let mut count = 0u64;
            while rx.recv_async().await.is_ok() {
                count += 1;
            }
            count
        });

        for h in handles {
            h.await.unwrap();
        }
        let count = receiver.await.unwrap();
        assert_eq!(count, n_senders * msgs_per_sender);
    }

    #[tokio::test]
    async fn stress_rapid_sender_disconnect() {
        // Create and drop senders rapidly while receiver is active.
        // Each sender sends a few messages then disconnects.
        let (tx, rx) = bounded::<u64>(100);
        let total_expected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let handles: Vec<_> = (0..100)
            .map(|wave| {
                let tx = tx.clone();
                let total = total_expected.clone();
                GlobalExecutor::spawn(async move {
                    // Each sender lives briefly — send 10 messages then drop
                    for i in 0..10 {
                        tx.send_async(wave * 10 + i).await.unwrap();
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                    // tx clone dropped here
                })
            })
            .collect();
        drop(tx); // drop original

        let receiver = GlobalExecutor::spawn(async move {
            let mut count = 0u64;
            while rx.recv_async().await.is_ok() {
                count += 1;
            }
            count
        });

        for h in handles {
            h.await.unwrap();
        }
        let count = receiver.await.unwrap();
        let expected = total_expected.load(Ordering::Relaxed);
        assert_eq!(count, expected);
    }

    #[tokio::test]
    async fn stress_many_channels_parallel() {
        // Simulate 200 independent channels (like 200 peer connections).
        // Each has its own sender/receiver pair running concurrently.
        let n_channels = 200;
        let msgs_per_channel = 500;

        let handles: Vec<_> = (0..n_channels)
            .map(|_| {
                GlobalExecutor::spawn(async move {
                    let (tx, rx) = bounded::<u64>(50);

                    let sender = GlobalExecutor::spawn(async move {
                        for i in 0..msgs_per_channel {
                            tx.send_async(i).await.unwrap();
                        }
                    });

                    let receiver = GlobalExecutor::spawn(async move {
                        let mut count = 0u64;
                        while count < msgs_per_channel {
                            rx.recv_async().await.unwrap();
                            count += 1;
                        }
                        count
                    });

                    sender.await.unwrap();
                    let count = receiver.await.unwrap();
                    assert_eq!(count, msgs_per_channel);
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn stress_backpressure_with_slow_receiver() {
        // Capacity-1 channel: every send must wait for the receiver.
        // Receiver adds artificial delay to create sustained backpressure.
        let (tx, rx) = bounded::<u64>(1);
        let n_msgs = 500;

        let sender = GlobalExecutor::spawn(async move {
            for i in 0..n_msgs {
                tx.send_async(i).await.unwrap();
            }
        });

        let receiver = GlobalExecutor::spawn(async move {
            let mut count = 0u64;
            for _ in 0..n_msgs {
                let msg = rx.recv_async().await.unwrap();
                assert_eq!(msg, count);
                count += 1;
                // Slow receiver: yield occasionally to stress backpressure wakeup
                if count % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            count
        });

        sender.await.unwrap();
        let count = receiver.await.unwrap();
        assert_eq!(count, n_msgs);
    }

    #[tokio::test]
    async fn stress_sender_drop_during_backpressure() {
        // Fill channel, start multiple senders blocked on backpressure,
        // then drop receiver — all senders should detect disconnection.
        let (tx, rx) = bounded::<u64>(2);

        // Fill the channel
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        // Spawn senders that will block on backpressure
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let tx = tx.clone();
                GlobalExecutor::spawn(async move {
                    // This will block because channel is full
                    let result = tx.send_async(100 + i).await;
                    result
                })
            })
            .collect();
        drop(tx);

        // Give senders time to park on backpressure
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Drop receiver — senders should get SendError
        drop(rx);

        for h in handles {
            let result = h.await.unwrap();
            assert!(
                result.is_err(),
                "Sender should get error after receiver drops"
            );
        }
    }
}
