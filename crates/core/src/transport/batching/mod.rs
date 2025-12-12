//! Syscall batching for improved UDP throughput.
//!
//! On Linux, uses `sendmmsg` to send multiple packets in a single syscall.
//! On other platforms, falls back to sequential sends (no regression, no improvement).
//!
//! Experimental results show batch sizes of 50-100 provide optimal throughput
//! improvement (~1.75x) with diminishing returns beyond that.

/// Default batch size for syscall batching.
/// Based on experimental data showing diminishing returns beyond 100.
#[cfg(target_os = "linux")]
pub const BATCH_SIZE: usize = 100;

#[cfg(target_os = "linux")]
pub mod linux;
