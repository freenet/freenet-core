use std::path::PathBuf;

// ── macOS wrapper single-instance lock ──
//
// Problem: launchd's LAL agent fires RunAtLoad while the user's
// open-launched wrapper is still alive, and spawns a second wrapper.
// The backend-level single-instance guard
// (EXIT_CODE_ALREADY_RUNNING = 43) catches the second wrapper's
// DAEMON CHILD but by the time that exit propagates, the second
// wrapper's tao event loop is already running on the main thread and
// owns an NSStatusItem. Two rabbits.
//
// Port-based detection of "another wrapper is running" is fragile:
//   - wrapper A's backend may be mid-crash or mid-backoff, in which
//     case port 7509 is briefly free but wrapper A's tray is still up
//   - `kill_stale_freenet_processes` runs early in wrapper startup
//     and happily pkills the OTHER wrapper's live backend child,
//     widening the race window it was meant to close
//   - a non-Freenet squatter on port 7509 triggers a false positive
//     with no diagnosable UX
//
// Solution: an advisory `flock` on a wrapper-scoped lockfile at
// `~/Library/Caches/Freenet/wrapper.lock`, acquired in `run_wrapper`
// BEFORE `kill_stale_freenet_processes` and BEFORE any user-visible
// state change. Held for the wrapper's lifetime; released on process
// exit by the kernel. If a second wrapper can't acquire, it exits
// silently. Robust to backend state, to pkill interference, and to
// third-party port squatters.

/// Advisory single-instance guard for the macOS wrapper process. Held
/// for the wrapper's lifetime and released automatically by the
/// kernel on process exit.
#[cfg(target_os = "macos")]
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct WrapperSingleInstanceLock {
    // The File keeps the fd alive; dropping it releases the flock.
    _file: std::fs::File,
}

/// Path of the wrapper lockfile if we can determine a cache directory.
#[allow(dead_code)]
pub(super) fn wrapper_lock_path() -> Option<PathBuf> {
    dirs::cache_dir().map(|d| d.join("Freenet").join("wrapper.lock"))
}

/// Outcome of attempting to acquire the wrapper single-instance lock.
/// Extracted as a pure type so the orchestration in `run_wrapper` stays
/// readable and the unit test can exercise every arm.
#[derive(Debug)]
#[allow(dead_code)]
pub(super) enum AcquireWrapperLockOutcome {
    /// We hold the lock; proceed with normal wrapper startup. The
    /// caller must hold the guard for the duration of the wrapper.
    Acquired(WrapperSingleInstanceLock),
    /// Another wrapper already holds the lock. Exit silently.
    AnotherWrapperRunning,
    /// We couldn't even try (no cache dir, can't create parent, etc.).
    /// Treat as "proceed without a lock" rather than silently exiting
    /// the user's only way to run Freenet. On platforms where the
    /// cache dir is always present, this is unreachable.
    UnavailableSoProceed,
}

/// Acquire the wrapper lockfile via `flock(LOCK_EX | LOCK_NB)`.
#[cfg(target_os = "macos")]
#[allow(dead_code)]
pub(super) fn acquire_wrapper_single_instance_lock() -> AcquireWrapperLockOutcome {
    use std::os::unix::io::AsRawFd;
    let Some(lock_path) = wrapper_lock_path() else {
        tracing::warn!("Wrapper lock: cache directory unresolvable; proceeding without lock");
        return AcquireWrapperLockOutcome::UnavailableSoProceed;
    };
    if let Some(parent) = lock_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(
                "Wrapper lock: failed to create {}: {}; proceeding without lock",
                parent.display(),
                e
            );
            return AcquireWrapperLockOutcome::UnavailableSoProceed;
        }
    }
    let file = match std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(
                "Wrapper lock: failed to open {}: {}; proceeding without lock",
                lock_path.display(),
                e
            );
            return AcquireWrapperLockOutcome::UnavailableSoProceed;
        }
    };
    // SAFETY: `file.as_raw_fd()` returns a valid fd owned by `file`,
    // and `libc::flock` only operates on that fd. LOCK_EX | LOCK_NB
    // is an exclusive non-blocking lock that fails fast if held by
    // another process. The lock is released automatically when the
    // fd closes on process exit (kernel-managed); we never need to
    // unlock explicitly.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        AcquireWrapperLockOutcome::AnotherWrapperRunning
    } else {
        AcquireWrapperLockOutcome::Acquired(WrapperSingleInstanceLock { _file: file })
    }
}

/// Non-macOS stub so `run_wrapper` compiles without cfg gates around
/// every mention. On Linux the backend-level port guard is sufficient
/// (no tray); on Windows the install-time wrapper detection is the
/// analogous mechanism.
#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
pub(super) fn acquire_wrapper_single_instance_lock() -> AcquireWrapperLockOutcome {
    AcquireWrapperLockOutcome::UnavailableSoProceed
}

#[cfg(not(target_os = "macos"))]
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct WrapperSingleInstanceLock;

/// At-most-once guard for spawning the first-run dashboard opener within a
/// single wrapper process. Without it, a wrapper that restarts its daemon
/// child several times before the HTTP server binds (e.g. during a crash
/// loop on initial startup) would accumulate one 30-second opener thread
/// per relaunch, each racing to open a browser tab and write the marker.
/// Checking a real wall-clock Instant plus polling a TCP port means we
/// also can't reuse the per-launch marker file itself as a latch.
#[cfg(any(target_os = "windows", target_os = "macos"))]
pub(super) static FIRST_RUN_OPENER_SPAWNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
