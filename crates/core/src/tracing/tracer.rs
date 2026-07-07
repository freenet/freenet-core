use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::OnceLock;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{Layer, Registry};

/// Number of hours to keep log files (using hourly rotation).
/// At typical gateway log rates (~500KB/hour), 72 hours ≈ 36MB.
const LOG_RETENTION_HOURS: usize = 72;

/// Backstop on the total bytes the freenet log directory may occupy.
/// When log volume spikes above the steady-state assumption baked into
/// `LOG_RETENTION_HOURS` (e.g., the executor-queue overflow in issue
/// #4251 producing thousands of events per second), the time-based
/// retention alone cannot bound disk usage within a single session.
/// This cap deletes oldest-first after the time pass to bring the
/// directory back under the limit.
///
/// Enforcement runs both at tracer init (node start) AND periodically
/// on the background prune loop spawned by `init_tracer` (issue #4699),
/// so a long-uptime node under sustained runaway logging is bounded
/// without needing a restart.
const LOG_DIR_MAX_BYTES: u64 = 512 * 1024 * 1024; // 512 MiB

/// How often the background prune loop re-applies `cleanup_old_logs`.
/// Matches the hourly rotation cadence: a fresh file is sealed every
/// hour, so re-checking the time + size passes hourly keeps the
/// directory bounded between restarts without wasteful churn.
const LOG_PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// Match the rolling-appender naming convention used by
/// `RollingFileAppender::Rotation::HOURLY` for the `freenet` /
/// `freenet.error` prefixes:
///
///   freenet.YYYY-MM-DD-HH.log
///   freenet.error.YYYY-MM-DD-HH.log
///
/// Intentionally does NOT match:
/// - `freenet.log` / `freenet.error.log` — legacy systemd /launchd
///   StandardOutput targets that the OS holds open; deleting them
///   leaks an unlinked-but-open inode (Linux) or errors (Windows)
///   and does not free disk space until restart.
/// - `freenet.error.log.last` — transient per-launch scratch file the
///   macOS wrapper overwrites each iteration.
fn is_rotating_freenet_log(name: &str) -> bool {
    // freenet.error.YYYY-MM-DD-HH.log → after stripping the prefix and
    // suffix, the remainder must be the date-hour stem. We don't parse
    // the stem strictly; cheap shape check: at least one '-' and all
    // remaining characters in [0-9-].
    let stem = if let Some(rest) = name.strip_prefix("freenet.error.") {
        rest
    } else if let Some(rest) = name.strip_prefix("freenet.") {
        rest
    } else {
        return false;
    };
    let Some(date_part) = stem.strip_suffix(".log") else {
        return false;
    };
    !date_part.is_empty()
        && date_part.contains('-')
        && date_part.chars().all(|c| c.is_ascii_digit() || c == '-')
}

/// Guards for non-blocking file appenders - must be kept alive for the lifetime of the program
static LOG_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();

/// Get the default log directory for the current platform.
/// Used by both the tracer (for writing logs) and report command (for reading logs).
pub fn get_log_dir() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        dirs::home_dir().map(|h| h.join(".local/state/freenet"))
    }

    #[cfg(target_os = "macos")]
    {
        dirs::home_dir().map(|h| h.join("Library/Logs/freenet"))
    }

    #[cfg(target_os = "windows")]
    {
        dirs::data_local_dir().map(|d| d.join("freenet").join("logs"))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        None
    }
}

/// Clean up old log files on startup.
///
/// First pass: remove files older than `LOG_RETENTION_HOURS`.
/// Second pass: if the total size of remaining `freenet*.log` files
/// still exceeds `LOG_DIR_MAX_BYTES`, delete oldest-first until under
/// the limit. The size cap is a backstop for runaway log rates that
/// the time-based retention alone can't bound.
fn cleanup_old_logs(log_dir: &std::path::Path) {
    use std::time::{Duration, SystemTime};

    let retention = Duration::from_secs(LOG_RETENTION_HOURS as u64 * 3600);
    let cutoff = SystemTime::now() - retention;

    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return;
    };

    // First pass: time-based deletion, collect survivors for the
    // size-cap pass.
    let mut survivors: Vec<(std::path::PathBuf, SystemTime, u64)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();

        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !is_rotating_freenet_log(name) {
            continue;
        }

        let Ok(metadata) = path.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        if modified < cutoff {
            if let Err(e) = std::fs::remove_file(&path) {
                eprintln!("Failed to remove old log file {}: {}", path.display(), e);
            }
            continue;
        }
        survivors.push((path, modified, metadata.len()));
    }

    enforce_log_dir_size_cap(survivors, LOG_DIR_MAX_BYTES);
}

/// Background loop that re-invokes [`cleanup_old_logs`] on an hourly
/// cadence so the time-based retention and the `LOG_DIR_MAX_BYTES` size
/// cap are enforced for the whole lifetime of a long-uptime node, not
/// just at startup (issue #4699).
///
/// Modeled on the ring subscription sweep (`ring.rs`): a jittered initial
/// delay avoids synchronized prunes across peers, then a `tokio::time::interval`
/// drives the loop with the first immediate tick skipped (startup cleanup
/// already ran).
///
/// Wall-clock (`SystemTime`, via `cleanup_old_logs`) is intentional here:
/// log retention is inherently wall-clock and is not a simulation surface,
/// so `TimeSource` does not apply.
async fn periodic_log_prune(log_dir: PathBuf) {
    // ±25% jitter around a 60s base to desynchronize the first prune across
    // peers without depending on node-level wiring.
    let jitter_secs = crate::config::GlobalRng::random_range(45u64..=75u64);
    tokio::time::sleep(std::time::Duration::from_secs(jitter_secs)).await;

    let mut interval = tokio::time::interval(LOG_PRUNE_INTERVAL);
    interval.tick().await; // Skip the first immediate tick — startup already pruned.

    loop {
        interval.tick().await;
        cleanup_old_logs(&log_dir);
    }
}

/// Delete oldest log files until the total size of the supplied list is
/// at or below `max_bytes`. Mutates the filesystem; the input vector
/// is consumed. Parameterized for test isolation.
///
/// The most-recently-modified file is preserved unconditionally even
/// when it alone exceeds `max_bytes`: it is the file currently being
/// written by `RollingFileAppender`. On Linux, removing it would leave
/// the appender writing to an unlinked inode (disk space not reclaimed
/// until the next rotation); on Windows, `remove_file` would simply
/// fail. Either way the live file should not be a cleanup target.
fn enforce_log_dir_size_cap(
    mut files: Vec<(std::path::PathBuf, std::time::SystemTime, u64)>,
    max_bytes: u64,
) {
    let total: u64 = files.iter().map(|(_, _, size)| *size).sum();
    if total <= max_bytes {
        return;
    }

    // Oldest first; remove from this end and stop before the newest.
    files.sort_by_key(|(_, modified, _)| *modified);
    let live = files.pop(); // newest mtime — never deleted
    let live_size = live.as_ref().map(|(_, _, size)| *size).unwrap_or(0);
    let mut non_live_remaining: u64 = files.iter().map(|(_, _, size)| *size).sum();

    for (path, _, size) in files {
        // Final on-disk size after additional deletions =
        //   live_size + non_live_remaining (decreasing each loop).
        if live_size.saturating_add(non_live_remaining) <= max_bytes {
            break;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                non_live_remaining = non_live_remaining.saturating_sub(size);
            }
            Err(e) => {
                eprintln!(
                    "Failed to enforce log dir size cap on {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }
}

pub fn init_tracer(
    level: Option<LevelFilter>,
    _endpoint: Option<String>,
    log_dir: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    // Initialize console subscriber if enabled
    #[cfg(feature = "console-subscriber")]
    {
        if std::env::var("TOKIO_CONSOLE").is_ok() {
            console_subscriber::init();
            tracing::info!(
                "Tokio console subscriber initialized. Connect with 'tokio-console' command."
            );
            return Ok(());
        }
    }

    let default_filter = if cfg!(any(test, debug_assertions)) {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };
    let default_filter = level.unwrap_or(default_filter);

    use tracing_subscriber::layer::SubscriberExt;

    let disabled_logs = std::env::var("FREENET_DISABLE_LOGS").is_ok();
    if disabled_logs {
        return Ok(());
    }

    let to_stderr = std::env::var("FREENET_LOG_TO_STDERR").is_ok();
    let use_json = std::env::var("FREENET_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    // Determine if we should write to files:
    // - Always write to files when a log directory is available (ensures diagnostic reports work)
    // - Can be disabled with FREENET_LOG_TO_STDERR (uses stderr instead)
    // - The FREENET_DISABLE_LOGS env var disables all logging
    //
    // Note: On Windows especially, logs must go to files because Task Scheduler
    // doesn't capture stdout, making `freenet service report` unable to collect logs.
    let use_file_logging = !to_stderr && log_dir.is_some();

    // Build filter (we'll create separate instances for each layer since filters are consumed)
    fn build_filter(default_filter: LevelFilter) -> tracing_subscriber::EnvFilter {
        tracing_subscriber::EnvFilter::builder()
            .with_default_directive(default_filter.into())
            .from_env_lossy()
            .add_directive("moka=off".parse().expect("infallible"))
            .add_directive("sqlx=error".parse().expect("infallible"))
    }

    let filter_layer = build_filter(default_filter);

    // Also output to console when running interactively (stdout is a terminal)
    // This restores the expected console output while keeping file logging for diagnostic reports
    let also_log_to_console = std::io::stdout().is_terminal();

    // Get rate limit from environment or use default (1000 events/sec)
    let rate_limit: u64 = std::env::var("FREENET_LOG_RATE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(crate::util::rate_limit_layer::DEFAULT_MAX_EVENTS_PER_SECOND);

    // Per-callsite cap (issue #4251 follow-up). Stops a single misbehaving
    // tracing macro from dominating the log even when its rate stays
    // below the global aggregate cap. Configurable via
    // FREENET_LOG_RATE_LIMIT_PER_CALLSITE.
    let per_callsite_limit: u64 = std::env::var("FREENET_LOG_RATE_LIMIT_PER_CALLSITE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(crate::util::rate_limit_layer::DEFAULT_MAX_EVENTS_PER_CALLSITE_PER_SECOND);

    // Rate limiting is disabled in tests and debug builds to avoid masking issues
    let rate_limit_enabled = !cfg!(any(test, debug_assertions))
        && std::env::var("FREENET_DISABLE_LOG_RATE_LIMIT").is_err();

    // Create rate limiters (shared across all layers)
    let rate_limiter = if rate_limit_enabled {
        Some(crate::util::rate_limit_layer::RateLimiter::new(rate_limit))
    } else {
        None
    };
    let per_callsite_limiter = if rate_limit_enabled {
        Some(crate::util::rate_limit_layer::PerCallsiteRateLimiter::new(
            per_callsite_limit,
        ))
    } else {
        None
    };

    if use_file_logging {
        if let Some(log_dir) = log_dir {
            // Create log directory if it doesn't exist
            if let Err(e) = std::fs::create_dir_all(log_dir) {
                eprintln!("Warning: Failed to create log directory: {e}");
                // Fall back to stdout logging
                return init_stdout_tracer(
                    default_filter,
                    to_stderr,
                    use_json,
                    filter_layer,
                    rate_limiter,
                    per_callsite_limiter,
                );
            }

            // Clean up old log files (including legacy daily logs) on startup
            cleanup_old_logs(log_dir);

            // Spawn a background loop that re-applies the same cleanup on an
            // hourly cadence so the size cap is enforced continuously, not only
            // at startup. A long-uptime node under runaway logging would
            // otherwise exceed `LOG_DIR_MAX_BYTES` until its next restart
            // (issue #4699). Tracer-owned: it needs only the log dir path.
            crate::config::GlobalExecutor::spawn(periodic_log_prune(log_dir.to_path_buf()));

            // Create rolling file appender for main log (hourly rotation)
            let main_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .max_log_files(LOG_RETENTION_HOURS)
                .filename_prefix("freenet")
                .filename_suffix("log")
                .build(log_dir)
                .map_err(|e| anyhow::anyhow!("Failed to create log appender: {e}"))?;

            // Create rolling file appender for error log (hourly rotation)
            let error_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .max_log_files(LOG_RETENTION_HOURS)
                .filename_prefix("freenet.error")
                .filename_suffix("log")
                .build(log_dir)
                .map_err(|e| anyhow::anyhow!("Failed to create error log appender: {e}"))?;

            let (main_writer, main_guard) = tracing_appender::non_blocking(main_appender);
            let (error_writer, error_guard) = tracing_appender::non_blocking(error_appender);

            // Store guards to keep writers alive; fail if already initialized
            if LOG_GUARDS.set(vec![main_guard, error_guard]).is_err() {
                return Err(anyhow::anyhow!(
                    "LOG_GUARDS already initialized; tracer cannot be re-initialized"
                ));
            }

            // Apply rate limiting as a global filter if enabled.
            //
            // We MUST use `DynFilterFn` here, NOT `filter_fn`. The latter
            // assumes the closure is callsite-cacheable (no Context arg)
            // and so calls `callsite_enabled` ONCE per callsite, caching
            // the first result as `Interest::always`/`never`. That makes
            // every stateful rate-limit filter a no-op for the second and
            // subsequent events from the same macro — exactly the bug
            // that let issue #4251 spam slip past the pre-existing global
            // `RateLimiter`. `DynFilterFn` defaults to `Interest::sometimes`,
            // so `enabled` is invoked per event. (Caught by codex review on
            // PR #4273 — see the PR thread.)
            if let Some(rate_limiter) = rate_limiter.clone() {
                let per_callsite = per_callsite_limiter.clone();
                let rate_filter = tracing_subscriber::filter::DynFilterFn::new(move |meta, _cx| {
                    per_callsite
                        .as_ref()
                        .map(|pc| pc.should_allow(meta))
                        .unwrap_or(true)
                        && rate_limiter.should_allow()
                });
                let base = Registry::default().with(rate_filter);

                // Create layers for main and error logs (typed against rate-filtered registry)
                let main_layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(main_writer.clone())
                    .with_filter(filter_layer);

                let error_filter = tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(LevelFilter::WARN.into())
                    .from_env_lossy();

                let error_layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(error_writer.clone())
                    .with_filter(error_filter);

                // Add console layer if running interactively
                if also_log_to_console {
                    let console_filter = build_filter(default_filter);
                    let console_layer = tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .pretty()
                        .with_filter(console_filter);

                    let subscriber = base.with(main_layer).with(error_layer).with(console_layer);
                    tracing::subscriber::set_global_default(subscriber)
                        .expect("Error setting subscriber");
                } else {
                    let subscriber = base.with(main_layer).with(error_layer);
                    tracing::subscriber::set_global_default(subscriber)
                        .expect("Error setting subscriber");
                }
            } else {
                // Create layers for main and error logs (typed against plain registry)
                let main_layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(main_writer)
                    .with_filter(filter_layer);

                let error_filter = tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(LevelFilter::WARN.into())
                    .from_env_lossy();

                let error_layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(error_writer)
                    .with_filter(error_filter);

                // Add console layer if running interactively
                if also_log_to_console {
                    let console_filter = build_filter(default_filter);
                    let console_layer = tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .pretty()
                        .with_filter(console_filter);

                    let subscriber = Registry::default()
                        .with(main_layer)
                        .with(error_layer)
                        .with(console_layer);
                    tracing::subscriber::set_global_default(subscriber)
                        .expect("Error setting subscriber");
                } else {
                    let subscriber = Registry::default().with(main_layer).with(error_layer);
                    tracing::subscriber::set_global_default(subscriber)
                        .expect("Error setting subscriber");
                }
            }

            return Ok(());
        }
    }

    // Fall back to stdout/stderr logging
    init_stdout_tracer(
        default_filter,
        to_stderr,
        use_json,
        filter_layer,
        rate_limiter,
        per_callsite_limiter,
    )
}

fn init_stdout_tracer(
    _default_filter: LevelFilter,
    to_stderr: bool,
    use_json: bool,
    filter_layer: tracing_subscriber::EnvFilter,
    rate_limiter: Option<crate::util::rate_limit_layer::RateLimiter>,
    per_callsite_limiter: Option<crate::util::rate_limit_layer::PerCallsiteRateLimiter>,
) -> anyhow::Result<()> {
    use tracing_subscriber::layer::SubscriberExt;

    // Helper to create the format layer
    fn make_layer<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>>(
        to_stderr: bool,
        use_json: bool,
    ) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync> {
        if to_stderr {
            if use_json {
                tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .json()
                    .with_file(cfg!(any(test, debug_assertions)))
                    .with_line_number(cfg!(any(test, debug_assertions)))
                    .with_writer(std::io::stderr)
                    .boxed()
            } else {
                let layer = tracing_subscriber::fmt::layer().with_level(true).pretty();
                let layer = if cfg!(any(test, debug_assertions)) {
                    layer.with_file(true).with_line_number(true)
                } else {
                    layer
                };
                layer.with_writer(std::io::stderr).boxed()
            }
        } else if use_json {
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .json()
                .with_file(cfg!(any(test, debug_assertions)))
                .with_line_number(cfg!(any(test, debug_assertions)))
                .boxed()
        } else {
            let layer = tracing_subscriber::fmt::layer().with_level(true).pretty();
            if cfg!(any(test, debug_assertions)) {
                layer.with_file(true).with_line_number(true).boxed()
            } else {
                layer.boxed()
            }
        }
    }

    // Apply rate limiting as a global filter if enabled.
    // See the equivalent block in `init_tracer` above for why this MUST
    // use `DynFilterFn` rather than `filter_fn`.
    if let Some(rate_limiter) = rate_limiter {
        let per_callsite = per_callsite_limiter.clone();
        let rate_filter = tracing_subscriber::filter::DynFilterFn::new(move |meta, _cx| {
            per_callsite
                .as_ref()
                .map(|pc| pc.should_allow(meta))
                .unwrap_or(true)
                && rate_limiter.should_allow()
        });
        let base = Registry::default().with(rate_filter);
        let layer = make_layer(to_stderr, use_json);
        let subscriber = base.with(layer.with_filter(filter_layer));
        tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
    } else {
        let layer = make_layer(to_stderr, use_json);
        let subscriber = Registry::default().with(layer.with_filter(filter_layer));
        tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
    }
    Ok(())
}

#[cfg(test)]
mod cleanup_tests {
    use super::{
        LOG_DIR_MAX_BYTES, cleanup_old_logs, enforce_log_dir_size_cap, periodic_log_prune,
    };
    use std::fs;
    use std::time::{Duration, SystemTime};

    /// Writes `path` with `size` bytes and sets its mtime to `mtime`.
    fn write_with_mtime(path: &std::path::Path, size: usize, mtime: SystemTime) {
        fs::write(path, vec![b'.'; size]).unwrap();
        let times = std::fs::FileTimes::new().set_modified(mtime);
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_times(times).unwrap();
    }

    /// Regression for issue #4251: when log volume blows past the
    /// time-based retention's implicit assumption (~500 KB/h), the
    /// size cap must delete oldest-first until the directory is
    /// under the supplied limit.
    #[test]
    fn size_cap_deletes_oldest_first_until_under_limit() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        let oldest = dir.path().join("freenet.2026-05-25-12.log");
        let middle = dir.path().join("freenet.2026-05-25-13.log");
        let newest = dir.path().join("freenet.2026-05-25-14.log");

        // 4 KiB each; total 12 KiB. Cap at 8 KiB → oldest must go.
        write_with_mtime(&oldest, 4096, now - Duration::from_secs(3600));
        write_with_mtime(&middle, 4096, now - Duration::from_secs(60));
        write_with_mtime(&newest, 4096, now - Duration::from_secs(30));

        let files = vec![
            (oldest.clone(), now - Duration::from_secs(3600), 4096),
            (middle.clone(), now - Duration::from_secs(60), 4096),
            (newest.clone(), now - Duration::from_secs(30), 4096),
        ];
        enforce_log_dir_size_cap(files, 8192);

        assert!(
            !oldest.exists(),
            "oldest file should be deleted by size cap"
        );
        assert!(middle.exists(), "middle file should survive");
        assert!(newest.exists(), "newest file should survive");
    }

    /// Under-cap directories must not lose any files.
    #[test]
    fn size_cap_is_noop_when_under_limit() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        let small = dir.path().join("freenet.2026-05-25-15.log");
        write_with_mtime(&small, 1024, now);

        let files = vec![(small.clone(), now, 1024)];
        enforce_log_dir_size_cap(files, 1024 * 1024 * 1024);

        assert!(small.exists(), "file under cap must survive");
    }

    /// The time-based pass in `cleanup_old_logs` still removes files
    /// older than the retention window, even when total size is under
    /// the cap.
    #[test]
    fn time_pass_removes_files_older_than_retention() {
        let dir = tempfile::tempdir().unwrap();
        // 100 days old, 1 KiB — well under size cap but past time cap.
        let ancient = dir.path().join("freenet.2026-02-14-00.log");
        write_with_mtime(
            &ancient,
            1024,
            SystemTime::now() - Duration::from_secs(100 * 24 * 3600),
        );

        cleanup_old_logs(dir.path());

        assert!(
            !ancient.exists(),
            "ancient file must be removed by time pass"
        );
    }

    /// Non-`freenet*` files in the same directory must be ignored.
    #[test]
    fn cleanup_ignores_non_freenet_files() {
        let dir = tempfile::tempdir().unwrap();
        let other = dir.path().join("other.log");
        fs::write(&other, b"unrelated").unwrap();

        cleanup_old_logs(dir.path());

        assert!(other.exists(), "non-freenet files must not be touched");
    }

    /// The size cap must NEVER delete the most-recently-modified file
    /// (the live file the rolling appender is currently writing to).
    /// Removing it would leave the appender writing to an unlinked inode
    /// on Linux, or fail on Windows. Regression for review findings on
    /// issue #4251.
    #[test]
    fn size_cap_preserves_most_recently_modified_file() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // Single oversized file — also the newest. Must NOT be deleted.
        let live = dir.path().join("freenet.2026-05-25-18.log");
        write_with_mtime(&live, 16 * 1024, now);

        let files = vec![(live.clone(), now, 16 * 1024)];
        enforce_log_dir_size_cap(files, 1024); // cap below file size

        assert!(
            live.exists(),
            "live file must survive even when alone it exceeds the cap"
        );
    }

    /// Even with the live file preserved, older files must be deleted
    /// to bring the total down. Regression for the live-file fix
    /// composing correctly with the eviction loop.
    #[test]
    fn size_cap_deletes_oldest_but_keeps_live() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // Two oversized files: cap at 5 KiB, live=4 KiB, old=4 KiB,
        // total 8 KiB → old gets deleted, live survives, final = 4 KiB.
        let old = dir.path().join("freenet.2026-05-25-12.log");
        let live = dir.path().join("freenet.2026-05-25-18.log");
        write_with_mtime(&old, 4096, now - Duration::from_secs(3600));
        write_with_mtime(&live, 4096, now);

        let files = vec![
            (old.clone(), now - Duration::from_secs(3600), 4096),
            (live.clone(), now, 4096),
        ];
        enforce_log_dir_size_cap(files, 5120);

        assert!(!old.exists(), "older file must be deleted");
        assert!(live.exists(), "live file must survive");
    }

    /// `cleanup_old_logs` must NOT touch the legacy bare
    /// `freenet.log` / `freenet.error.log` paths — systemd/launchd
    /// hold them open and deletion leaks the inode (Linux) or fails
    /// (Windows). Only the rolling-appender date-suffixed files are
    /// eligible. Regression for review findings on issue #4251.
    #[test]
    fn cleanup_skips_legacy_bare_freenet_log_names() {
        let dir = tempfile::tempdir().unwrap();
        // Make these old so they'd be deleted by the time pass if it
        // applied to them.
        let bare = dir.path().join("freenet.log");
        let bare_err = dir.path().join("freenet.error.log");
        let scratch = dir.path().join("freenet.error.log.last");
        for p in [&bare, &bare_err, &scratch] {
            write_with_mtime(
                p,
                1024,
                SystemTime::now() - Duration::from_secs(30 * 24 * 3600),
            );
        }

        cleanup_old_logs(dir.path());

        assert!(
            bare.exists(),
            "legacy freenet.log must not be deleted (systemd-owned)"
        );
        assert!(
            bare_err.exists(),
            "legacy freenet.error.log must not be deleted (systemd-owned)"
        );
        assert!(
            scratch.exists(),
            "transient freenet.error.log.last must not be deleted (wrapper-owned)"
        );
    }

    /// The size cap must engage exactly at the `LOG_DIR_MAX_BYTES`
    /// boundary (now 512 MiB, lowered from 1 GiB in issue #4699).
    /// Clock-free: exercises the pure size math on a synthetic list with
    /// no filesystem entries, so it asserts the boundary regardless of
    /// how large the const is. `budget + 1` total must trigger a delete;
    /// `budget` exactly must not. The newest file is always preserved.
    #[test]
    fn size_cap_engages_at_512mib_boundary() {
        let cap = LOG_DIR_MAX_BYTES;
        assert_eq!(cap, 512 * 1024 * 1024, "cap must be 512 MiB (#4699)");

        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // Two files. Sized so that live + old == cap + 1 → over by one
        // byte → the old (non-live) file must be deleted to get back to
        // the cap. Split the budget so the live file alone is under cap.
        let live_size = cap / 2;
        let old_size = cap - live_size + 1; // total = cap + 1
        let old = dir.path().join("freenet.2026-05-25-12.log");
        let live = dir.path().join("freenet.2026-05-25-13.log");
        write_with_mtime(&old, old_size as usize, now - Duration::from_secs(3600));
        write_with_mtime(&live, live_size as usize, now);

        let over_by_one = vec![
            (old.clone(), now - Duration::from_secs(3600), old_size),
            (live.clone(), now, live_size),
        ];
        enforce_log_dir_size_cap(over_by_one, cap);
        assert!(!old.exists(), "cap+1 must delete the oldest non-live file");
        assert!(live.exists(), "live file must always survive");

        // Rewrite the old file and feed a list totalling exactly `cap`:
        // no deletion may occur (boundary is inclusive: total <= cap).
        write_with_mtime(
            &old,
            (cap - live_size) as usize,
            now - Duration::from_secs(3600),
        );
        let exactly_cap = vec![
            (
                old.clone(),
                now - Duration::from_secs(3600),
                cap - live_size,
            ),
            (live.clone(), now, live_size),
        ];
        enforce_log_dir_size_cap(exactly_cap, cap);
        assert!(old.exists(), "total == cap must NOT delete anything");
        assert!(live.exists(), "live file must survive at the boundary");
    }

    /// The periodic prune loop (issue #4699) must apply the same
    /// oldest-first, size-capped cleanup as the startup path while
    /// preserving the live (newest) file. Driven with tokio
    /// `start_paused` so the hourly interval advances in virtual time —
    /// no real sleeps. The loop runs forever, so it is spawned and the
    /// test awaits enough virtual time for exactly one prune, then drops
    /// the task.
    #[tokio::test(start_paused = true)]
    async fn periodic_prune_deletes_oldest_first_and_keeps_live() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // 4 KiB each, total 12 KiB. We cannot pass a custom cap into the
        // loop (it uses the const), so instead we rely on the time pass:
        // make the two older files past the 72h retention window and the
        // newest within it. The periodic cleanup must delete the two old
        // ones and keep the live file, proving the loop actually invokes
        // cleanup_old_logs on its tick.
        let old_ts = SystemTime::now() - Duration::from_secs(100 * 24 * 3600);
        let oldest = dir.path().join("freenet.2026-02-14-00.log");
        let middle = dir.path().join("freenet.2026-02-14-01.log");
        let live = dir.path().join("freenet.2026-05-25-14.log");
        write_with_mtime(&oldest, 4096, old_ts);
        write_with_mtime(&middle, 4096, old_ts + Duration::from_secs(3600));
        write_with_mtime(&live, 4096, now);

        let handle = tokio::spawn(periodic_log_prune(dir.path().to_path_buf()));

        // Let the spawned task run up to its first `.await` (the jittered
        // initial sleep) so its timer is registered before we advance.
        tokio::task::yield_now().await;

        // Advance past the jittered initial delay (max 75s). Yield so the
        // task wakes, skips the immediate interval tick, and parks on the
        // hourly interval.
        tokio::time::advance(Duration::from_secs(76)).await;
        tokio::task::yield_now().await;

        // Advance one full hourly interval so the loop reaches its first
        // prune tick, then yield so the tick body runs cleanup_old_logs.
        tokio::time::advance(Duration::from_secs(3601)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        handle.abort();

        assert!(!oldest.exists(), "oldest file must be pruned by the loop");
        assert!(!middle.exists(), "middle file must be pruned by the loop");
        assert!(live.exists(), "live (newest) file must be preserved");
    }
}
