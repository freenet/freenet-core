//! Fixed-size thread pool for WASM execution.
//!
//! This module provides a bounded worker pool that avoids the overhead of
//! spawning new threads for each WASM execution. Workers are created once
//! and reused for all WASM calls.

use crossbeam::channel::{bounded, Receiver, Sender};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use wasmer::Store;

/// Global worker pool singleton.
static GLOBAL_WORKER_POOL: OnceLock<WasmWorkerPool> = OnceLock::new();

/// Get or initialize the global worker pool.
///
/// The pool size is determined by available CPU parallelism (minimum 2, maximum 16).
pub(super) fn get_worker_pool() -> &'static WasmWorkerPool {
    GLOBAL_WORKER_POOL.get_or_init(|| {
        let pool_size = std::thread::available_parallelism()
            .map(|n| n.get().clamp(2, 16))
            .unwrap_or(4);
        let pool_size = NonZeroUsize::new(pool_size).unwrap();

        tracing::info!(
            pool_size = pool_size.get(),
            "Initializing global WASM worker pool"
        );

        WasmWorkerPool::new(pool_size)
    })
}

/// Result type for WASM execution.
pub(super) type WasmResult = (Result<i64, wasmer::RuntimeError>, Store);

/// A boxed closure that performs WASM work and returns the result.
type WasmWork = Box<dyn FnOnce() -> WasmResult + Send + 'static>;

/// Message sent to worker threads.
struct WorkItem {
    /// The work to execute.
    work: WasmWork,
    /// Channel to send result back to the caller.
    result_sender: Sender<WasmResult>,
}

/// A handle to wait for WASM execution results.
///
/// Similar to `JoinHandle` but backed by a channel instead of a thread.
pub(super) struct WasmExecutionHandle {
    result_receiver: Receiver<WasmResult>,
}

impl WasmExecutionHandle {
    /// Check if the execution has completed without blocking.
    pub fn is_finished(&self) -> bool {
        !self.result_receiver.is_empty()
    }

    /// Block until the execution completes and return the result.
    ///
    /// Returns `Err` if the worker panicked or was disconnected.
    pub fn join(self) -> Result<WasmResult, WasmExecutionError> {
        self.result_receiver
            .recv()
            .map_err(|_| WasmExecutionError::WorkerPanicked)
    }
}

/// Error returned when WASM execution fails at the worker level.
#[derive(Debug)]
pub(super) enum WasmExecutionError {
    /// The worker thread panicked while executing the work.
    WorkerPanicked,
}

/// A fixed-size thread pool for executing WASM code.
///
/// This pool maintains a set of worker threads that wait for work via channels.
/// Work is distributed to workers in a FIFO manner, and results are returned
/// via dedicated result channels.
///
/// # Thread Safety
///
/// The pool is thread-safe and can be shared across multiple threads. Work
/// submission is lock-free using crossbeam channels.
///
/// # Panics
///
/// If a worker panics while executing WASM code, the worker will be respawned
/// automatically. The caller will receive a `WasmExecutionError::WorkerPanicked`
/// error.
pub(super) struct WasmWorkerPool {
    /// Channel to send work to workers.
    work_sender: Sender<WorkItem>,
    /// Handles to worker threads (for cleanup on drop).
    workers: Vec<JoinHandle<()>>,
    /// Flag to signal workers to shut down.
    shutdown: Arc<AtomicBool>,
    /// Receiver end kept to detect if all workers have died.
    _work_receiver: Receiver<WorkItem>,
}

impl WasmWorkerPool {
    /// Create a new worker pool with the specified number of workers.
    ///
    /// # Arguments
    /// * `size` - Number of worker threads to create.
    ///
    /// # Panics
    /// Panics if `size` is zero.
    pub fn new(size: NonZeroUsize) -> Self {
        let size: usize = size.into();
        // Bounded channel with capacity equal to pool size.
        // This provides backpressure if all workers are busy.
        let (work_sender, work_receiver) = bounded::<WorkItem>(size);
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut workers = Vec::with_capacity(size);
        for i in 0..size {
            let receiver = work_receiver.clone();
            let shutdown = shutdown.clone();
            let handle = thread::Builder::new()
                .name(format!("freenet-wasm-worker-{}", i))
                .spawn(move || {
                    worker_loop(receiver, shutdown);
                })
                .expect("Failed to spawn WASM worker thread");
            workers.push(handle);
        }

        tracing::info!(
            pool_size = size,
            "Created WasmWorkerPool with {} workers",
            size
        );

        Self {
            work_sender,
            workers,
            shutdown,
            _work_receiver: work_receiver,
        }
    }

    /// Execute WASM work on a worker thread.
    ///
    /// This method sends the work to an available worker and returns a handle
    /// that can be used to wait for the result. If all workers are busy, this
    /// will block until a worker becomes available.
    ///
    /// # Arguments
    /// * `work` - A closure that performs the WASM execution and returns the result.
    ///
    /// # Returns
    /// A handle that can be used to poll for completion or wait for the result.
    pub fn execute<F>(&self, work: F) -> WasmExecutionHandle
    where
        F: FnOnce() -> WasmResult + Send + 'static,
    {
        // Create a channel for the result (capacity 1 = oneshot)
        let (result_sender, result_receiver) = bounded::<WasmResult>(1);

        let work_item = WorkItem {
            work: Box::new(work),
            result_sender,
        };

        // Send work to a worker. This blocks if all workers are busy.
        // This is intentional - we want backpressure.
        self.work_sender
            .send(work_item)
            .expect("All workers have died");

        WasmExecutionHandle { result_receiver }
    }

    /// Get the number of workers in the pool.
    #[allow(dead_code)]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for WasmWorkerPool {
    fn drop(&mut self) {
        // Signal workers to shut down
        self.shutdown.store(true, Ordering::SeqCst);

        // Wait for workers to finish (with timeout)
        for handle in self.workers.drain(..) {
            // We don't join because workers might be blocked on recv()
            // The channel will be dropped, causing recv() to return Err
            drop(handle);
        }
    }
}

/// The main loop for worker threads.
fn worker_loop(receiver: Receiver<WorkItem>, shutdown: Arc<AtomicBool>) {
    loop {
        // Check for shutdown
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Wait for work with a timeout so we can check shutdown periodically
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(work_item) => {
                // Execute the work and send result
                // We use catch_unwind to handle panics gracefully
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (work_item.work)()));

                match result {
                    Ok(wasm_result) => {
                        // Send result back (ignore error if receiver dropped)
                        let _ = work_item.result_sender.send(wasm_result);
                    }
                    Err(panic_info) => {
                        // Worker caught a panic - log it
                        let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Unknown panic".to_string()
                        };
                        tracing::error!(
                            panic = %panic_msg,
                            "WASM worker caught panic during execution"
                        );
                        // Result sender will be dropped, causing join() to return Err
                    }
                }
            }
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                // No work available, continue loop to check shutdown
                continue;
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                // Channel closed, exit
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::time::Instant;

    fn make_dummy_store() -> Store {
        use wasmer::sys::EngineBuilder;
        use wasmer_compiler_singlepass::Singlepass;
        let compiler = Singlepass::default();
        let engine = EngineBuilder::new(compiler).engine();
        Store::new(&engine)
    }

    #[test]
    fn test_pool_creation() {
        let pool = WasmWorkerPool::new(NonZeroUsize::new(4).unwrap());
        assert_eq!(pool.worker_count(), 4);
    }

    #[test]
    fn test_execute_single_work() {
        let pool = WasmWorkerPool::new(NonZeroUsize::new(2).unwrap());

        let handle = pool.execute(|| {
            let store = make_dummy_store();
            (Ok(42), store)
        });

        let result = handle.join().unwrap();
        assert_eq!(result.0.unwrap(), 42);
    }

    #[test]
    fn test_execute_multiple_concurrent() {
        let pool = WasmWorkerPool::new(NonZeroUsize::new(4).unwrap());
        let completed = Arc::new(AtomicU32::new(0));

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let completed = completed.clone();
                pool.execute(move || {
                    thread::sleep(Duration::from_millis(10));
                    completed.fetch_add(1, Ordering::SeqCst);
                    let store = make_dummy_store();
                    (Ok(i), store)
                })
            })
            .collect();

        // Wait for all to complete
        for handle in handles {
            let _ = handle.join().unwrap();
        }

        assert_eq!(completed.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn test_is_finished() {
        let pool = WasmWorkerPool::new(NonZeroUsize::new(1).unwrap());

        let handle = pool.execute(|| {
            thread::sleep(Duration::from_millis(50));
            let store = make_dummy_store();
            (Ok(1), store)
        });

        // Should not be finished immediately
        assert!(!handle.is_finished());

        // Wait for completion
        thread::sleep(Duration::from_millis(100));
        assert!(handle.is_finished());

        let _ = handle.join();
    }

    #[test]
    fn test_panic_handling() {
        let pool = WasmWorkerPool::new(NonZeroUsize::new(2).unwrap());

        let handle = pool.execute(|| {
            panic!("intentional panic");
        });

        // Should return error
        let result = handle.join();
        assert!(result.is_err());

        // Pool should still work after panic
        let handle2 = pool.execute(|| {
            let store = make_dummy_store();
            (Ok(42), store)
        });

        let result2 = handle2.join().unwrap();
        assert_eq!(result2.0.unwrap(), 42);
    }

    #[test]
    fn test_no_thread_spawning_per_execution() {
        let pool = WasmWorkerPool::new(NonZeroUsize::new(2).unwrap());
        let start = Instant::now();

        // Execute many times - should be fast because no thread spawning
        for _ in 0..100 {
            let handle = pool.execute(|| {
                let store = make_dummy_store();
                (Ok(1), store)
            });
            let _ = handle.join();
        }

        let elapsed = start.elapsed();
        // 100 executions should complete quickly (< 1 second) without thread spawn overhead
        assert!(
            elapsed < Duration::from_secs(1),
            "100 executions took {:?}",
            elapsed
        );
    }

    #[test]
    fn test_backpressure() {
        // Pool of 2 workers with work that takes 50ms each
        let pool = WasmWorkerPool::new(NonZeroUsize::new(2).unwrap());

        let start = Instant::now();

        // Submit 4 work items
        let handles: Vec<_> = (0..4)
            .map(|i| {
                pool.execute(move || {
                    thread::sleep(Duration::from_millis(50));
                    let store = make_dummy_store();
                    (Ok(i), store)
                })
            })
            .collect();

        // Wait for all
        for h in handles {
            let _ = h.join();
        }

        let elapsed = start.elapsed();
        // With 2 workers and 4 x 50ms work items, should take ~100ms (2 batches)
        assert!(
            elapsed >= Duration::from_millis(80) && elapsed < Duration::from_millis(200),
            "Expected ~100ms, got {:?}",
            elapsed
        );
    }
}
