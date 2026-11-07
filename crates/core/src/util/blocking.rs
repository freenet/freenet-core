//! Blocking work that must not stall the async reactor, and must not make a
//! simulation depend on OS scheduling.

/// Runs blocking `f` on the blocking pool in production, and inline on a
/// `current_thread` runtime.
///
/// On a multi-thread runtime (the node in production) `f` goes to
/// `spawn_blocking`, so a long disk walk cannot stall other tasks.
///
/// On a `current_thread` runtime (every turmoil host in a simulation, and most
/// tests) `f` runs inline. A blocking-pool thread finishes in wall-clock time,
/// so the simulation step at which the awaiting task resumes would depend on
/// how the OS schedules that thread, and the same seed would take different
/// paths from one run to the next (#6123). The executor's
/// `run_blocking_offloaded` uses the same gate for the same reason.
///
/// A panic in `f` is caught on both paths and returned as [`BlockingPanicked`],
/// so a caller's loop survives it either way.
pub(crate) async fn offload_blocking<F, R>(f: F) -> Result<R, BlockingPanicked>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::spawn_blocking(f)
                .await
                .map_err(|err| BlockingPanicked(err.to_string()))
        }
        _ => std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|payload| {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            BlockingPanicked(message)
        }),
    }
}

/// The blocking closure given to [`offload_blocking`] panicked (or, on the
/// blocking pool, was cancelled by runtime shutdown).
#[derive(Debug)]
pub(crate) struct BlockingPanicked(String);

impl std::fmt::Display for BlockingPanicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blocking task failed: {}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn runs_inline_on_a_current_thread_runtime() {
        let caller = std::thread::current().id();
        let ran_on = offload_blocking(|| std::thread::current().id())
            .await
            .unwrap();
        assert_eq!(
            ran_on, caller,
            "a simulation host must not hand the work to a wall-clock thread"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uses_the_blocking_pool_on_a_multi_thread_runtime() {
        let caller = std::thread::current().id();
        let ran_on = offload_blocking(|| std::thread::current().id())
            .await
            .unwrap();
        assert_ne!(
            ran_on, caller,
            "production must keep the disk walk off the reactor thread"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_inline_panic_comes_back_as_an_error() {
        let outcome = offload_blocking(|| -> () { panic!("walk failed") }).await;
        let err = outcome.expect_err("the panic must be caught");
        assert!(err.to_string().contains("walk failed"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pool_panic_comes_back_as_an_error() {
        let outcome = offload_blocking(|| -> () { panic!("walk failed") }).await;
        let err = outcome.expect_err("the panic must be caught");
        assert!(err.to_string().contains("walk failed"), "{err}");
    }
}
