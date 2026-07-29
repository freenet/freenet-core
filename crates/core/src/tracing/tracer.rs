use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::OnceLock;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{Layer, Registry};

/// Default bound on the total bytes the freenet log directory may
/// occupy. Override per-node with [`LOG_DIR_MAX_BYTES_ENV_VAR`].
///
/// Bytes, not hours, are what has to be bounded: rotation is hourly, but
/// an hour of log is a few KiB on an idle peer and ~21 MB on a busy
/// gateway (measured, below), so a fixed hour count buys wildly
/// different amounts of disk from node to node. Which bound actually
/// binds therefore differs by node, and that is intended — a busy node
/// is held by these bytes, a quiet one by `LOG_RETENTION_HOURS`.
///
/// **Sized from the busiest observed node, not the quietest.** The
/// production gateway (nova) was measured at 54 rotating files totalling
/// 518.9 MiB over 25.97 hours — **20.95 MB/hour**. So 512 MiB is the
/// binding constraint there and buys ~25.6 hours; an evening incident is
/// still on disk the next morning. Sizing this from a quiet peer instead
/// would be a serious mistake: at 96 MiB a gateway retains 4.8 hours, so
/// an 18:00 incident is gone by 09:00.
///
/// Note that ~half of that 20.95 MB/h is redundant: `freenet.error.*` is
/// currently a byte-for-byte duplicate of the main log (see issue #5015),
/// because `RUST_LOG` overrides the error layer's WARN default. Fixing
/// that halves the rate outright and would let this default come down to
/// ~256 MiB for the same history. Until it lands, this budget cannot be
/// reduced without costing a gateway real incident history.
///
/// Do NOT set this so low that `live_size` alone can approach it. The
/// size pass never deletes a file an appender has open, so a budget the
/// current-hour files can fill leaves *only* those files — discarding
/// exactly the onset of the incident the logs exist to explain.
///
/// Enforcement runs both at tracer init (node start) AND periodically
/// on the background prune loop spawned by `init_tracer` (issue #4699),
/// so a long-uptime node under sustained runaway logging is bounded
/// without needing a restart.
const LOG_DIR_MAX_BYTES: u64 = 512 * 1024 * 1024; // 512 MiB

/// Operator override for [`LOG_DIR_MAX_BYTES`], as a byte count.
///
/// One default cannot serve both shapes of node this code runs on: at
/// the measured rates, a gateway needs ~500 MiB to hold a day while a
/// background laptop peer reaches its 72-hour horizon in well under 100
/// MiB and would rather have the disk back. The default is sized for the
/// gateway (losing incident history is the worse failure), and this lets
/// the small-node case dial it down.
const LOG_DIR_MAX_BYTES_ENV_VAR: &str = "FREENET_LOG_DIR_MAX_BYTES";

/// Absolute age after which a rotated log file is deleted regardless of
/// how little disk it occupies.
///
/// On a quiet node this is what binds — such a node never approaches the
/// byte budget, so without an age bound it would accumulate rotated
/// files indefinitely. On a busy node `LOG_DIR_MAX_BYTES` binds first
/// (at the measured gateway rate, 72 hours would be ~1.4 GiB).
///
/// Three days covers a Friday-evening fault reported on Monday morning.
const LOG_RETENTION_HOURS: u64 = 72; // 3 days

/// The byte budget in force for this process: the operator override if
/// it is set and usable, otherwise [`LOG_DIR_MAX_BYTES`].
fn log_dir_max_bytes() -> u64 {
    parse_log_dir_max_bytes(std::env::var(LOG_DIR_MAX_BYTES_ENV_VAR).ok().as_deref())
}

/// Pure half of [`log_dir_max_bytes`], split out so it is testable
/// without mutating process-global environment state from tests that run
/// in parallel.
///
/// Degrade-safe: an absent, empty, unparseable, or zero value yields the
/// default. Misconfiguration must not be able to disable log retention
/// (`0` would mean "delete everything the size pass is allowed to") nor
/// stop the node.
fn parse_log_dir_max_bytes(raw: Option<&str>) -> u64 {
    let Some(raw) = raw else {
        return LOG_DIR_MAX_BYTES;
    };
    match raw.trim().parse::<u64>() {
        Ok(bytes) if bytes > 0 => bytes,
        _ => {
            eprintln!(
                "Ignoring invalid {LOG_DIR_MAX_BYTES_ENV_VAR}={raw:?} \
                 (want a positive byte count); using the {LOG_DIR_MAX_BYTES}-byte default"
            );
            LOG_DIR_MAX_BYTES
        }
    }
}

/// How often the background prune loop re-applies `cleanup_old_logs`.
/// Matches the hourly rotation cadence: a fresh file is sealed every
/// hour, so re-checking the time + size passes hourly keeps the
/// directory bounded between restarts without wasteful churn.
const LOG_PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// Which of the two rolling appenders a log file belongs to.
///
/// `init_tracer` builds TWO `RollingFileAppender`s over the SAME
/// directory, so at any moment there are TWO files open for writing,
/// one per family. Anything that deletes files here must know which
/// family a file belongs to — see [`prune_log_files`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFamily {
    /// `freenet.YYYY-MM-DD-HH.log` — the main appender (INFO+).
    Main,
    /// `freenet.error.YYYY-MM-DD-HH.log` — the error appender (WARN+).
    Error,
}

/// Classify a file name against the rolling-appender naming convention
/// used by `RollingFileAppender::Rotation::HOURLY` for the `freenet` /
/// `freenet.error` prefixes:
///
///   freenet.YYYY-MM-DD-HH.log        → [`LogFamily::Main`]
///   freenet.error.YYYY-MM-DD-HH.log  → [`LogFamily::Error`]
///
/// Returns `None` (i.e. "not ours, never delete") for everything else.
/// Intentionally does NOT match:
/// - `freenet.log` / `freenet.error.log` — legacy systemd /launchd
///   StandardOutput targets that the OS holds open; deleting them
///   leaks an unlinked-but-open inode (Linux) or errors (Windows)
///   and does not free disk space until restart.
/// - `freenet.error.log.last` — transient per-launch scratch file the
///   macOS wrapper overwrites each iteration.
/// - `known_good_binary` / `update_probation.json` / `known_bad_version`
///   — on Linux the auto-updater's state dir IS the log dir, and those
///   files are the crash-loop rollback machinery. They do not start
///   with `freenet.`, so they are skipped here; do not loosen this
///   filter into a bare `freenet` prefix match.
fn rotating_log_family(name: &str) -> Option<LogFamily> {
    // Order matters: `freenet.error.` is a superset of `freenet.`, so it
    // must be tested first. Testing the shorter prefix first does NOT
    // merely mis-file error logs as Main — it drops them out of the
    // filter entirely, because the leftover stem `error.<date>` fails the
    // all-digits check below and yields `None`. Nothing that returns
    // `None` is ever pruned, so the error family would grow without
    // bound.
    let (family, stem) = if let Some(rest) = name.strip_prefix("freenet.error.") {
        (LogFamily::Error, rest)
    } else if let Some(rest) = name.strip_prefix("freenet.") {
        (LogFamily::Main, rest)
    } else {
        return None;
    };
    // After stripping the prefix and suffix, the remainder must be the
    // date-hour stem. We don't parse the stem strictly; cheap shape
    // check: at least one '-' and all remaining characters in [0-9-].
    let date_part = stem.strip_suffix(".log")?;
    if !date_part.is_empty()
        && date_part.contains('-')
        && date_part.chars().all(|c| c.is_ascii_digit() || c == '-')
    {
        Some(family)
    } else {
        None
    }
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

/// One rotated log file the pruner may consider deleting.
#[derive(Debug, Clone)]
struct LogFile {
    path: std::path::PathBuf,
    modified: std::time::SystemTime,
    size: u64,
    family: LogFamily,
}

/// Prune the log directory. The single pruning authority for the two
/// rolling appenders (see the `max_log_files` note in `init_tracer`).
///
/// Reads the directory, then hands the rotating log files to
/// [`prune_log_files`], which owns both retention passes.
fn cleanup_old_logs(log_dir: &std::path::Path) {
    use std::time::{Duration, SystemTime};

    let retention = Duration::from_secs(LOG_RETENTION_HOURS * 3600);
    let cutoff = SystemTime::now() - retention;

    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return;
    };

    let mut files: Vec<LogFile> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();

        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        // Anything unclassified is not ours and is never touched.
        let Some(family) = rotating_log_family(name) else {
            continue;
        };

        let Ok(metadata) = path.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        files.push(LogFile {
            path,
            modified,
            size: metadata.len(),
            family,
        });
    }

    prune_log_files(files, cutoff, log_dir_max_bytes());
}

/// The indices in a **mtime-ascending** `files` of the newest file of
/// each family — i.e. the file each appender currently has open.
///
/// Discovered from the data rather than by walking a hand-written list
/// of [`LogFamily`] variants: adding a third appender must not require
/// remembering to extend a list here, because forgetting would silently
/// re-expose that appender's open file to deletion — precisely the bug
/// this function exists to prevent.
fn live_file_indices(files: &[LogFile]) -> Vec<usize> {
    let mut newest: Vec<(LogFamily, usize)> = Vec::new();
    for (idx, file) in files.iter().enumerate() {
        match newest.iter_mut().find(|(family, _)| *family == file.family) {
            // Ascending mtime, so a later index is always newer.
            Some((_, slot)) => *slot = idx,
            None => newest.push((file.family, idx)),
        }
    }
    newest.into_iter().map(|(_, idx)| idx).collect()
}

/// Apply both retention passes to `files`, deleting from the filesystem.
///
/// 1. **Age**: drop files older than `cutoff`, however small.
/// 2. **Bytes**: if what survives still exceeds `max_bytes`, delete
///    oldest-first until it doesn't.
///
/// Which pass binds depends on the node: a busy one is held by bytes, a
/// quiet one by age. See [`LOG_DIR_MAX_BYTES`].
///
/// **The live files are exempt from BOTH passes.** They are the files
/// the two appenders have open; deleting one leaves its appender writing
/// to an unlinked inode, so the space is not reclaimed and everything
/// written for the rest of the hour is invisible to
/// `freenet service report`. The age pass needs this exemption just as
/// much as the size pass does, and for a reason that is easy to miss:
/// rotation is lazy. An appender that is not written to never rotates,
/// so its open file's mtime stays frozen at creation — and a node that
/// goes a full `LOG_RETENTION_HOURS` without a warning therefore has a
/// *live* error log that looks, to a plain `modified < cutoff` test,
/// exactly like an abandoned one.
///
/// # Why this GC exemption has no TTL
///
/// AGENTS.md requires cleanup exemptions to expire via TTL or be
/// overridden by an absolute age threshold, because unbounded exemptions
/// create permanent GC blind spots. This one has neither, deliberately.
/// It is bounded by **rotation** instead of by time, and the thing the
/// rule protects against — a blind spot that grows — cannot happen here:
///
/// * **Bounded in cardinality.** [`live_file_indices`] yields at most one
///   index per distinct [`LogFamily`], so the exempt set is at most two
///   files, structurally. It cannot grow with uptime, file count, or log
///   volume.
/// * **Bounded in bytes.** An exempt file cannot grow once its rotation
///   period passes. `RollingFileAppender::write` calls `should_rollover`
///   and swaps in a fresh file *before* writing the buffer
///   (`tracing-appender-0.2.5/src/rolling.rs:227-236`), so the first
///   write after the boundary lands in a NEW file, never in the stale
///   one. Each exempt file is therefore frozen at whatever its family
///   wrote during a single rotation period — on the measured gateway,
///   ~21 MB/hour across both families, against a 512 MiB budget.
/// * **Self-clearing.** The only event that could make an exempt file
///   grow is a write, and a write is exactly what rotates it away: the
///   exemption transfers to the newly-created file and the superseded one
///   becomes an ordinary candidate that the very next prune sweeps. So
///   the exemption is positional, not sticky — it cannot accumulate.
///
/// The exemption is thus unbounded in *time* but bounded in *bytes* and
/// in *count*, which is what the rule is actually protecting. Pinned by
/// `live_file_exemption_clears_once_the_appender_rotates`.
///
/// An absolute-age override — "delete it anyway past N days" — would be
/// strictly worse, not merely unnecessary. The appender still holds the
/// descriptor, so unlinking reclaims no space until it rotates anyway;
/// the GC gains nothing and the node loses the rest of that hour's logs.
/// It would reintroduce precisely the bug this function exists to fix.
/// Do not add one.
fn prune_log_files(mut files: Vec<LogFile>, cutoff: std::time::SystemTime, max_bytes: u64) {
    files.sort_by_key(|file| file.modified);

    // Computed once, on the full set, and honoured by both passes below.
    let live = live_file_indices(&files);

    // Pass 1 — age.
    let mut retained: Vec<usize> = Vec::with_capacity(files.len());
    let mut retained_bytes: u64 = 0;
    for (idx, file) in files.iter().enumerate() {
        if !live.contains(&idx) && file.modified < cutoff {
            if let Err(e) = std::fs::remove_file(&file.path) {
                eprintln!(
                    "Failed to remove old log file {}: {}",
                    file.path.display(),
                    e
                );
            }
            continue;
        }
        retained.push(idx);
        retained_bytes = retained_bytes.saturating_add(file.size);
    }

    // Pass 2 — bytes. `retained` is still mtime-ascending, so walking it
    // forward deletes oldest-first.
    for idx in retained {
        if retained_bytes <= max_bytes {
            break;
        }
        if live.contains(&idx) {
            continue;
        }
        let file = &files[idx];
        match std::fs::remove_file(&file.path) {
            Ok(()) => retained_bytes = retained_bytes.saturating_sub(file.size),
            Err(e) => eprintln!(
                "Failed to enforce log dir size cap on {}: {}",
                file.path.display(),
                e
            ),
        }
    }
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

            // Create the rolling file appenders (hourly rotation).
            //
            // Deliberately NO max-log-files budget on either appender —
            // `cleanup_old_logs` is the single pruning authority. Do not
            // re-add it (pinned by
            // `appenders_must_not_delegate_pruning_to_max_log_files`):
            // tracing-appender's own `prune_old_logs` selects victims with
            // a bare `filename.starts_with(prefix)` test, and BOTH families
            // plus the legacy bare files start with `freenet`. So the main
            // appender's budget silently counted and deleted the error
            // appender's files (halving each family's real retention), and
            // once the budget was exceeded it deleted oldest-first — which
            // is exactly the systemd/launchd-held `freenet.log` /
            // `freenet.error.log` that `rotating_log_family` refuses to
            // touch, leaking an unlinked-but-open inode.
            //
            // `cleanup_old_logs` gets all of this right: it is family-aware,
            // skips the legacy and rollback-state files, spares both live
            // files, and bounds by bytes rather than file count. It runs at
            // startup and hourly thereafter (`periodic_log_prune`), the same
            // cadence rotation-time pruning had.
            let main_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .filename_prefix("freenet")
                .filename_suffix("log")
                .build(log_dir)
                .map_err(|e| anyhow::anyhow!("Failed to create log appender: {e}"))?;

            let error_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
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
        LOG_DIR_MAX_BYTES, LogFamily, LogFile, cleanup_old_logs, live_file_indices,
        parse_log_dir_max_bytes, periodic_log_prune, prune_log_files, rotating_log_family,
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

    /// Build a [`LogFile`], deriving the family from the file name the
    /// same way production does.
    fn log_file(path: &std::path::Path, modified: SystemTime, size: u64) -> LogFile {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap();
        let family = rotating_log_family(name)
            .unwrap_or_else(|| panic!("{name} is not a rotating log file"));
        LogFile {
            path: path.to_path_buf(),
            modified,
            size,
            family,
        }
    }

    /// Run only the size pass, by giving the age pass a cutoff nothing
    /// can be older than.
    fn size_pass_only(files: Vec<LogFile>, max_bytes: u64) {
        prune_log_files(files, SystemTime::UNIX_EPOCH, max_bytes);
    }

    /// Regression for issue #4251: when log volume blows past the
    /// time-based retention's implicit assumption, the size cap must
    /// delete oldest-first until the directory is under the limit.
    #[test]
    fn size_cap_deletes_oldest_first_until_under_limit() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        let oldest = dir.path().join("freenet.2026-05-25-12.log");
        let middle = dir.path().join("freenet.2026-05-25-13.log");
        let newest = dir.path().join("freenet.2026-05-25-14.log");

        // 4 KiB each; total 12 KiB. Cap at 8 KiB → oldest must go.
        let t_old = now - Duration::from_secs(3600);
        let t_mid = now - Duration::from_secs(60);
        let t_new = now - Duration::from_secs(30);
        write_with_mtime(&oldest, 4096, t_old);
        write_with_mtime(&middle, 4096, t_mid);
        write_with_mtime(&newest, 4096, t_new);

        size_pass_only(
            vec![
                log_file(&oldest, t_old, 4096),
                log_file(&middle, t_mid, 4096),
                log_file(&newest, t_new, 4096),
            ],
            8192,
        );

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

        size_pass_only(vec![log_file(&small, now, 1024)], 1024 * 1024 * 1024);

        assert!(small.exists(), "file under cap must survive");
    }

    /// The age pass still removes files older than the retention window,
    /// even when total size is under the cap.
    #[test]
    fn time_pass_removes_files_older_than_retention() {
        let dir = tempfile::tempdir().unwrap();
        // 100 days old, 1 KiB — well under size cap but past time cap.
        // A second, newer file of the same family keeps it off the live
        // list, which is what makes it eligible at all.
        let ancient = dir.path().join("freenet.2026-02-14-00.log");
        let recent = dir.path().join("freenet.2026-05-25-14.log");
        write_with_mtime(
            &ancient,
            1024,
            SystemTime::now() - Duration::from_secs(100 * 24 * 3600),
        );
        write_with_mtime(&recent, 1024, SystemTime::now());

        cleanup_old_logs(dir.path());

        assert!(
            !ancient.exists(),
            "ancient file must be removed by age pass"
        );
        assert!(recent.exists(), "the live file must survive");
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

    /// The size cap must NEVER delete a file an appender has open, even
    /// when that file alone exceeds the cap. Removing it would leave the
    /// appender writing to an unlinked inode on Linux, or fail on
    /// Windows. Regression for review findings on issue #4251.
    #[test]
    fn size_cap_preserves_most_recently_modified_file() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // Single oversized file — also the newest. Must NOT be deleted.
        let live = dir.path().join("freenet.2026-05-25-18.log");
        write_with_mtime(&live, 16 * 1024, now);

        size_pass_only(vec![log_file(&live, now, 16 * 1024)], 1024);

        assert!(
            live.exists(),
            "live file must survive even when alone it exceeds the cap"
        );
    }

    /// Even with the live file preserved, older files must be deleted to
    /// bring the total down.
    #[test]
    fn size_cap_deletes_oldest_but_keeps_live() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // Cap at 5 KiB, live=4 KiB, old=4 KiB, total 8 KiB → old gets
        // deleted, live survives, final = 4 KiB.
        let old = dir.path().join("freenet.2026-05-25-12.log");
        let live = dir.path().join("freenet.2026-05-25-18.log");
        let t_old = now - Duration::from_secs(3600);
        write_with_mtime(&old, 4096, t_old);
        write_with_mtime(&live, 4096, now);

        size_pass_only(
            vec![log_file(&old, t_old, 4096), log_file(&live, now, 4096)],
            5120,
        );

        assert!(!old.exists(), "older file must be deleted");
        assert!(live.exists(), "live file must survive");
    }

    /// `cleanup_old_logs` must NOT touch the legacy bare `freenet.log` /
    /// `freenet.error.log` paths — systemd/launchd hold them open and
    /// deletion leaks the inode (Linux) or fails (Windows). Only the
    /// rolling-appender date-suffixed files are eligible. Regression for
    /// review findings on issue #4251.
    #[test]
    fn cleanup_skips_legacy_bare_freenet_log_names() {
        let dir = tempfile::tempdir().unwrap();
        // Make these old so they'd be deleted by the age pass if it
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

    /// The default budget must hold a full day of a BUSY node's logs.
    ///
    /// Sizing this from a quiet peer is the mistake this test exists to
    /// prevent. Measured on the production gateway (nova): 54 rotating
    /// files, 518.9 MiB, spanning 25.97 hours = 20.95 MB/hour. At 96 MiB
    /// that node would retain 4.8 hours, so an 18:00 incident would be
    /// gone by 09:00 — the logs would be bounded but useless.
    ///
    /// For contrast, this same budget is not what binds on a quiet peer:
    /// at ~1.4 MB/h a laptop peer reaches `LOG_RETENTION_HOURS` (72h)
    /// having used under 100 MiB, so its retention is decided by age.
    /// Operators on small disks dial the budget down via
    /// `FREENET_LOG_DIR_MAX_BYTES`.
    #[test]
    fn default_budget_holds_a_day_of_a_busy_gateways_logs() {
        // nova, 2026-07: 518.9 MiB over 25.97h.
        const GATEWAY_BYTES_PER_HOUR: u64 = 20_950_000;
        let hours_retained = LOG_DIR_MAX_BYTES / GATEWAY_BYTES_PER_HOUR;
        assert!(
            hours_retained >= 24,
            "default budget of {LOG_DIR_MAX_BYTES} bytes retains only \
             {hours_retained}h at the measured gateway rate of \
             {GATEWAY_BYTES_PER_HOUR} B/h; an overnight incident must still \
             be on disk in the morning"
        );
    }

    /// The size cap must engage exactly at the budget boundary.
    /// Clock-free: exercises the pure size math, so it asserts the
    /// boundary regardless of how large the budget is. `budget + 1` total
    /// must trigger a delete; `budget` exactly must not. The live file is
    /// always preserved.
    #[test]
    fn size_cap_engages_exactly_at_the_budget_boundary() {
        let cap = LOG_DIR_MAX_BYTES;
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        let hour_ago = now - Duration::from_secs(3600);

        // `prune_log_files` takes sizes from the supplied list, never
        // from the filesystem (reading real metadata is
        // `cleanup_old_logs`'s job), so the on-disk files are empty
        // placeholders whose only role is to make `.exists()` meaningful.
        // Materialising `cap` bytes here would write ~512 MiB per run for
        // no additional coverage.
        let old = dir.path().join("freenet.2026-05-25-12.log");
        let live = dir.path().join("freenet.2026-05-25-13.log");

        // Sized so that live + old == cap + 1 → over by one byte → the
        // old (non-live) file must be deleted to get back to the cap.
        // Split the budget so the live file alone is under cap.
        let live_size = cap / 2;
        let old_size = cap - live_size + 1; // total = cap + 1
        write_with_mtime(&old, 0, hour_ago);
        write_with_mtime(&live, 0, now);

        size_pass_only(
            vec![
                log_file(&old, hour_ago, old_size),
                log_file(&live, now, live_size),
            ],
            cap,
        );
        assert!(!old.exists(), "cap+1 must delete the oldest non-live file");
        assert!(live.exists(), "live file must always survive");

        // Recreate the old file and feed a list totalling exactly `cap`:
        // no deletion may occur (boundary is inclusive: total <= cap).
        write_with_mtime(&old, 0, hour_ago);
        size_pass_only(
            vec![
                log_file(&old, hour_ago, cap - live_size),
                log_file(&live, now, live_size),
            ],
            cap,
        );
        assert!(old.exists(), "total == cap must NOT delete anything");
        assert!(live.exists(), "live file must survive at the boundary");
    }

    /// Both rolling appenders hold a file open at once, so the size pass
    /// must spare the newest file of EACH family — not merely the newest
    /// file overall.
    ///
    /// The two appenders' open files do not advance in lockstep, so the
    /// live error log is routinely NOT the newest file in the directory:
    /// whenever the main log has been written more recently, a pruner
    /// that spares one file treats the live error log as an ordinary
    /// deletion candidate. Deleting it makes its appender write to an
    /// unlinked inode — the space is not reclaimed, and everything logged
    /// for the rest of the hour is invisible to `freenet service report`.
    #[test]
    fn size_cap_spares_the_live_file_of_both_appenders() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        let main_old_at = now - Duration::from_secs(3 * 3600);
        let error_live_at = now - Duration::from_secs(2 * 3600);
        let main_live_at = now - Duration::from_secs(3600);

        let main_old = dir.path().join("freenet.2026-05-25-12.log");
        // Newest of the error family, but NOT the newest file overall.
        let error_live = dir.path().join("freenet.error.2026-05-25-13.log");
        let main_live = dir.path().join("freenet.2026-05-25-14.log");

        write_with_mtime(&main_old, 4096, main_old_at);
        write_with_mtime(&error_live, 4096, error_live_at);
        write_with_mtime(&main_live, 4096, main_live_at);

        // 12 KiB total against a 5 KiB cap. Deleting the one evictable
        // file (main_old) still leaves 8 KiB — deliberately over the cap,
        // so a pruner that spares only one file would go on to delete the
        // live error log to chase the budget.
        size_pass_only(
            vec![
                log_file(&main_old, main_old_at, 4096),
                log_file(&error_live, error_live_at, 4096),
                log_file(&main_live, main_live_at, 4096),
            ],
            5120,
        );

        assert!(
            !main_old.exists(),
            "the one evictable (non-live) file must be deleted"
        );
        assert!(
            error_live.exists(),
            "the error appender's open file must survive even though it is not \
             the newest file overall and the directory is still over the cap"
        );
        assert!(
            main_live.exists(),
            "the main appender's open file must survive"
        );
    }

    /// The AGE pass must spare the live files too, not just the size
    /// pass.
    ///
    /// Rotation is lazy: an appender that is not written to never
    /// rotates, so its open file's mtime stays frozen at creation. A node
    /// that goes a full retention window without a warning therefore has
    /// a *live* error log that a plain `modified < cutoff` test cannot
    /// distinguish from an abandoned one — and deleting it is the same
    /// unlinked-inode bug the size pass guards against.
    #[test]
    fn time_pass_spares_a_live_file_whose_mtime_has_aged_out() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // Both error files are past the 72h horizon. The newer of the two
        // is the one the appender still has open.
        let error_abandoned = dir.path().join("freenet.error.2026-05-17-00.log");
        let error_live = dir.path().join("freenet.error.2026-05-21-00.log");
        let main_live = dir.path().join("freenet.2026-05-25-14.log");

        write_with_mtime(
            &error_abandoned,
            1024,
            now - Duration::from_secs(200 * 3600),
        );
        write_with_mtime(&error_live, 1024, now - Duration::from_secs(100 * 3600));
        write_with_mtime(&main_live, 1024, now);

        cleanup_old_logs(dir.path());

        assert!(
            error_live.exists(),
            "the error appender's OPEN file must survive the age pass even \
             though its mtime is older than the retention horizon — it is \
             frozen only because nothing has been logged at WARN+ since"
        );
        assert!(
            !error_abandoned.exists(),
            "a genuinely superseded file of the same family must still be \
             swept, or the age pass would never reclaim anything"
        );
        assert!(main_live.exists(), "the main appender's open file survives");
    }

    /// The live-file exemption must be **positional, not sticky**: once
    /// the appender rotates, the file it used to hold open stops being
    /// exempt and becomes an ordinary collection candidate.
    ///
    /// This is the bound that lets the exemption exist without a TTL (see
    /// [`super::prune_log_files`]). AGENTS.md forbids unbounded GC
    /// exemptions because they become permanent blind spots; this one
    /// cannot accumulate, because the only event that would let an exempt
    /// file grow — a write — is the same event that rotates it away and
    /// moves the exemption to the new file.
    ///
    /// An age ceiling is NOT the alternative: it would unlink a
    /// descriptor the appender still holds, reclaiming nothing and losing
    /// the rest of the hour's logs.
    #[test]
    fn live_file_exemption_clears_once_the_appender_rotates() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        // A near-silent error appender: one file, open, mtime frozen far
        // outside the retention horizon because nothing has hit WARN+.
        let error_a = dir.path().join("freenet.error.2026-05-21-00.log");
        let main_live = dir.path().join("freenet.2026-05-25-14.log");
        write_with_mtime(&error_a, 1024, now - Duration::from_secs(100 * 3600));
        write_with_mtime(&main_live, 1024, now);

        cleanup_old_logs(dir.path());
        assert!(
            error_a.exists(),
            "while it is the newest of its family it is the open file, so it \
             is exempt however old its frozen mtime looks"
        );

        // The appender finally logs a warning. tracing-appender rolls
        // before writing, so the write lands in a NEW file and `error_a`
        // is closed at whatever size it already had.
        let error_b = dir.path().join("freenet.error.2026-05-25-14.log");
        write_with_mtime(&error_b, 1024, now);

        cleanup_old_logs(dir.path());
        assert!(
            !error_a.exists(),
            "once superseded, the previously-exempt file must be collected \
             on the very next prune — the exemption is positional, not \
             sticky, which is what bounds it without a TTL"
        );
        assert!(error_b.exists(), "the new open file inherits the exemption");
        assert!(main_live.exists(), "the main appender's open file survives");
    }

    /// The live set is derived from the files present, not from a
    /// hand-written list of [`LogFamily`] variants — forgetting to extend
    /// such a list when a third appender is added would silently re-expose
    /// that appender's open file to deletion.
    #[test]
    fn live_file_indices_is_the_newest_of_every_family_present() {
        let dir = tempfile::tempdir().unwrap();
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let at = |secs| base + Duration::from_secs(secs);

        // Interleaved and mtime-ascending, as `prune_log_files` sorts it.
        let files = vec![
            log_file(&dir.path().join("freenet.2026-05-25-10.log"), at(0), 1),
            log_file(
                &dir.path().join("freenet.error.2026-05-25-11.log"),
                at(10),
                1,
            ),
            log_file(&dir.path().join("freenet.2026-05-25-12.log"), at(20), 1),
            log_file(
                &dir.path().join("freenet.error.2026-05-25-13.log"),
                at(30),
                1,
            ),
            log_file(&dir.path().join("freenet.2026-05-25-14.log"), at(40), 1),
        ];

        let mut live = live_file_indices(&files);
        live.sort_unstable();
        assert_eq!(
            live,
            vec![3, 4],
            "expected the newest Error (index 3) and the newest Main (index 4)"
        );

        // Every family present must be represented exactly once.
        assert_eq!(live.len(), 2);
        let families: Vec<LogFamily> = live.iter().map(|&i| files[i].family).collect();
        assert!(families.contains(&LogFamily::Main));
        assert!(families.contains(&LogFamily::Error));
    }

    /// `freenet.error.*` must classify as [`LogFamily::Error`], not
    /// [`LogFamily::Main`]. `freenet.error.` is a superset of `freenet.`,
    /// so testing the shorter prefix first drops error logs out of the
    /// filter entirely (their leftover stem fails the all-digits check),
    /// and nothing unclassified is ever pruned — the family would grow
    /// without bound.
    #[test]
    fn error_logs_classify_as_their_own_family() {
        assert_eq!(
            rotating_log_family("freenet.2026-05-25-14.log"),
            Some(LogFamily::Main)
        );
        assert_eq!(
            rotating_log_family("freenet.error.2026-05-25-14.log"),
            Some(LogFamily::Error)
        );

        // Files the pruner must never claim, and so never delete.
        for foreign in [
            "freenet.log",
            "freenet.error.log",
            "freenet.error.log.last",
            "other.log",
            // On Linux the auto-updater's state dir IS the log dir.
            "known_good_binary",
            "update_probation.json",
            "known_bad_version",
        ] {
            assert_eq!(
                rotating_log_family(foreign),
                None,
                "{foreign} must not be claimed by the log pruner"
            );
        }
    }

    /// The operator override must be degrade-safe: anything unusable
    /// falls back to the default rather than disabling retention or
    /// failing the node.
    #[test]
    fn log_dir_max_bytes_override_is_degrade_safe() {
        assert_eq!(
            parse_log_dir_max_bytes(Some("134217728")),
            134217728,
            "a positive byte count must be honoured"
        );
        assert_eq!(
            parse_log_dir_max_bytes(Some("  134217728\n")),
            134217728,
            "surrounding whitespace must be tolerated"
        );

        for bad in [
            None,
            Some(""),
            Some("0"),
            Some("-1"),
            Some("128MiB"),
            Some("nonsense"),
        ] {
            assert_eq!(
                parse_log_dir_max_bytes(bad),
                LOG_DIR_MAX_BYTES,
                "{bad:?} must fall back to the default budget"
            );
        }
    }

    /// Neither appender may delegate pruning back to tracing-appender's
    /// `max_log_files`.
    ///
    /// Its `prune_old_logs` picks victims with a bare
    /// `filename.starts_with(prefix)` test. Both families AND the legacy
    /// bare files start with `freenet`, so the main appender's budget
    /// counts and deletes the error appender's files (halving each
    /// family's real retention) and, once over budget, deletes
    /// oldest-first — reaching the systemd/launchd-held `freenet.log`
    /// that `rotating_log_family` deliberately refuses to touch.
    /// `cleanup_old_logs` is the single pruning authority instead.
    #[test]
    fn appenders_must_not_delegate_pruning_to_max_log_files() {
        // Split so this needle cannot match its own source text.
        let needle = concat!(".max_log", "_files(");
        let source: String = include_str!("tracer.rs")
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            !source.contains(needle),
            "tracer.rs must not call the rolling appender's max-log-files \
             builder method: its prefix match spans both log families and the \
             systemd-owned bare files. Prune via cleanup_old_logs instead."
        );
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
        // loop (it reads the process-wide budget), so instead we rely on
        // the age pass: make the two older files past the
        // LOG_RETENTION_HOURS horizon and the newest within it. The
        // periodic cleanup must delete the two old ones and keep the live
        // file, proving the loop actually invokes cleanup_old_logs on its
        // tick.
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
