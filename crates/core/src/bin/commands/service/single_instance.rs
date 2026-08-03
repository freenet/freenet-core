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

// ── Windows wrapper single-instance lock ──
//
// Problem: unlike macOS (launchd RunAtLoad racing an open-launch) or
// Linux (no tray, so no wrapper-overlap concern), the Windows wrapper had
// NO single-instance guard at all — every relaunch (double-clicking the
// downloaded exe, or launching the installed
// `%localappdata%\Freenet\bin\freenet.exe`, while Freenet was already
// running from autostart or a prior launch) fell straight through to
// `kill_stale_freenet_processes`, which unconditionally `taskkill`s every
// `freenet network` child for the current user — "stale" in name only,
// it has no way to distinguish an orphan from the process a live wrapper
// is actively supervising. The result: every relaunch kills the running
// node and starts a new one, and when the kill races the new wrapper's
// own startup closely enough, two `freenet network` processes can end up
// concurrently reading and (pre-#5132-fix) non-atomically rewriting
// config.toml, which is how a hand-edit could come back as defaults.
// See #5132.
//
// Solution: a named kernel mutex, acquired in `run_wrapper` BEFORE
// `kill_stale_freenet_processes` and before any user-visible state
// change — the direct Windows analogue of the macOS flock guard above.
// `CreateMutexW` on a name already held by another process (even a
// different handle held by the SAME process) returns a valid handle but
// sets the last error to `ERROR_ALREADY_EXISTS`; checking that lets a
// second wrapper detect the first and exit silently instead of killing
// it. The mutex is released automatically by the kernel when every
// handle to it closes, including on ungraceful process exit, so there is
// no stale-lock cleanup to get wrong.
#[cfg(target_os = "windows")]
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct WrapperSingleInstanceLock {
    // The raw handle keeps the mutex alive; closing it (on Drop) releases
    // the mutex. Never used for anything else post-acquisition, so the
    // struct only exists to hold the handle for its lifetime.
    handle: winapi::um::winnt::HANDLE,
}

#[cfg(target_os = "windows")]
impl Drop for WrapperSingleInstanceLock {
    fn drop(&mut self) {
        // SAFETY: `self.handle` is a valid mutex handle returned by a
        // prior successful `CreateMutexW`, not yet closed (this is the
        // only place that closes it, and it runs at most once per value).
        unsafe {
            winapi::um::handleapi::CloseHandle(self.handle);
        }
    }
}

/// Name of the wrapper single-instance mutex. The `Global\` prefix is
/// deliberately omitted: this only needs to be unique per interactive
/// user session (Freenet installs and runs per-user, never as a system
/// service on Windows — see `install_service`), and an unprefixed name
/// is scoped to the caller's session, avoiding any cross-user collision
/// or the extra privilege `Global\` names can require.
#[cfg(target_os = "windows")]
pub(super) const WRAPPER_MUTEX_NAME: &str = "FreenetWrapperSingleInstance";

/// Acquire the wrapper single-instance mutex via `CreateMutexW`.
#[cfg(target_os = "windows")]
#[allow(dead_code)]
pub(super) fn acquire_wrapper_single_instance_lock() -> AcquireWrapperLockOutcome {
    use std::os::windows::ffi::OsStrExt;
    // NUL-terminate explicitly: `CreateMutexW` reads `lpName` as a
    // NUL-terminated wide string, and `encode_wide()` does not append one.
    let name: Vec<u16> = std::ffi::OsStr::new(WRAPPER_MUTEX_NAME)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: `name` is a valid, NUL-terminated UTF-16 buffer that outlives
    // the call (it's a local `Vec` still in scope). Passing null security
    // attributes and `FALSE` for `bInitialOwner` requests default security
    // and does not implicitly acquire the mutex, matching a plain
    // "does this name already exist" probe.
    let handle = unsafe {
        winapi::um::synchapi::CreateMutexW(
            std::ptr::null_mut(),
            winapi::shared::minwindef::FALSE,
            name.as_ptr(),
        )
    };
    if handle.is_null() {
        tracing::warn!(
            "Wrapper lock: CreateMutexW failed (error {}); proceeding without lock",
            unsafe { winapi::um::errhandlingapi::GetLastError() }
        );
        return AcquireWrapperLockOutcome::UnavailableSoProceed;
    }
    // SAFETY: `handle` was just returned non-null by `CreateMutexW` above;
    // `GetLastError` reflects that call's outcome (nothing else has run
    // on this thread since).
    let already_exists = unsafe { winapi::um::errhandlingapi::GetLastError() }
        == winapi::shared::winerror::ERROR_ALREADY_EXISTS;
    if already_exists {
        // SAFETY: `handle` is the valid handle just returned; we're done
        // with it immediately since we're not the owner.
        unsafe {
            winapi::um::handleapi::CloseHandle(handle);
        }
        AcquireWrapperLockOutcome::AnotherWrapperRunning
    } else {
        AcquireWrapperLockOutcome::Acquired(WrapperSingleInstanceLock { handle })
    }
}

/// Non-macOS, non-Windows stub so `run_wrapper` compiles without cfg
/// gates around every mention. Linux has no tray and no wrapper-overlap
/// concern (no autostart mechanism analogous to launchd RunAtLoad or the
/// Windows Run key double-launches this guards against).
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
#[allow(dead_code)]
pub(super) fn acquire_wrapper_single_instance_lock() -> AcquireWrapperLockOutcome {
    AcquireWrapperLockOutcome::UnavailableSoProceed
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
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
