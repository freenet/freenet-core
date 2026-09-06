use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::OnceLock;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{Layer, Registry};

/// Bound on the total bytes the freenet log directory may occupy.
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
/// That 20.95 MB/h is a **pre-#5015 measurement**, and ~half of it was
/// redundant: `freenet.error.*` duplicated the main log byte for byte
/// because `RUST_LOG` overrode the error layer's WARN default. #5015
/// has since landed ([`build_error_filter`]), so on nodes that set
/// `RUST_LOG` the error family drops to its WARN+ share (~5 % of its
/// former bytes) and the same gateway now runs at ~10.8 MB/h — 512 MiB
/// buys it roughly two days rather than one.
///
/// Scope that carefully before retuning this: the duplication only ever
/// affected nodes whose operator sets `RUST_LOG`. No install template
/// does, so a stock install was always at the lower rate and sees no
/// change. The halving therefore applies to gateways provisioned by
/// `scripts/init-gateway.sh` and similar, not fleet-wide. This default
/// is deliberately left at 512 MiB here: lowering it is a separate
/// judgement call about how much history a gateway should keep, not a
/// mechanical consequence of #5015.
///
/// Do NOT lower this so far that the current-hour files alone can
/// approach it. The size pass never deletes a file an appender has open,
/// so a budget those files can fill leaves *only* them — discarding
/// exactly the onset of the incident the logs exist to explain.
///
/// Enforcement runs both at tracer init (node start) AND periodically
/// on the background prune loop spawned by `init_tracer` (issue #4699),
/// so a long-uptime node under sustained runaway logging is bounded
/// without needing a restart.
///
/// **This default is overridable at runtime via `FREENET_LOG_DIR_MAX_BYTES`**
/// (issue #5021), because one compiled-in value has to serve both the
/// gateway shape this default is sized for and a quiet background peer
/// that may want to hand back disk, or widen the budget while chasing an
/// intermittent fault, without rebuilding. See [`parse_log_dir_max_bytes`]
/// for the fallback behaviour on a missing or unusable override, and
/// [`MIN_LOG_DIR_MAX_BYTES`] for why an override cannot go arbitrarily low.
const LOG_DIR_MAX_BYTES: u64 = 512 * 1024 * 1024; // 512 MiB

/// The smallest `FREENET_LOG_DIR_MAX_BYTES` override [`parse_log_dir_max_bytes`]
/// will honour; anything below this falls back to [`LOG_DIR_MAX_BYTES`].
///
/// Exists for the same reason the compiled-in default cannot go arbitrarily
/// low (see its doc): the size pass never deletes a file an appender has
/// open, so a budget the two current-hour files (main + error) can fill on
/// their own leaves *only* those files, discarding exactly the onset of
/// whatever incident prompted the investigation (issue #5019, item 3).
///
/// Sized well above every observed hourly rate rather than just above it:
/// the busiest measured gateway ran at ~20.95 MB/hour pre-#5015 and ~10.8
/// MB/hour after (see `default_budget_holds_a_day_of_a_busy_gateways_logs`
/// below for the measurement), combined across both families. 64 MiB is
/// roughly 3x the worse of those two rates, so even a node logging far
/// above anything measured so far keeps more than an hour of history
/// before this floor would start discarding the incident onset — while
/// still letting an operator shrink the 512 MiB default meaningfully for
/// a quiet peer that never approaches it.
const MIN_LOG_DIR_MAX_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

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

/// `FREENET_LOG_DIR_MAX_BYTES`'s raw value, or `None` if unset.
///
/// Split from [`parse_log_dir_max_bytes`] for the same reason as
/// [`error_log_directives`]: keeps the parsing logic a pure function of
/// its input, so it can be unit-tested without mutating process-global
/// environment state (see the "Cross-test interference" entry in
/// `.claude/rules/testing.md` — env vars are exactly that kind of state).
fn log_dir_max_bytes_env() -> Option<String> {
    std::env::var("FREENET_LOG_DIR_MAX_BYTES").ok()
}

/// Resolve the log directory's byte budget from `FREENET_LOG_DIR_MAX_BYTES`'s
/// raw value, falling back to [`LOG_DIR_MAX_BYTES`] whenever the override
/// isn't a usable one (issue #5021).
///
/// Degrade-safe by construction, never panics and never refuses to prune:
/// - absent or empty → default, silently (nothing was configured)
/// - unparseable → default, with a warning (something was configured wrong)
/// - below [`MIN_LOG_DIR_MAX_BYTES`] (including `0`) → default, with a
///   warning — see that constant's doc for why a tiny budget is as
///   destructive as refusing to boot: a value of `0` would mean "delete
///   everything the size pass may delete", and small-but-nonzero values are
///   barely better.
///
/// A misconfigured value must never stop the node from starting or from
/// pruning at all — a node that refuses to boot over a typo'd env var is
/// worse than one that ignores it, which is why this warns and falls back
/// rather than propagating an error.
fn parse_log_dir_max_bytes(raw: Option<&str>) -> u64 {
    match raw {
        None => LOG_DIR_MAX_BYTES,
        Some("") => LOG_DIR_MAX_BYTES,
        Some(raw) => match raw.parse::<u64>() {
            Ok(bytes) if bytes >= MIN_LOG_DIR_MAX_BYTES => bytes,
            Ok(bytes) => {
                eprintln!(
                    "Warning: FREENET_LOG_DIR_MAX_BYTES={bytes} is below the minimum of \
                     {MIN_LOG_DIR_MAX_BYTES} bytes; using the default of {LOG_DIR_MAX_BYTES} \
                     bytes instead. A budget this small can be filled entirely by the current \
                     hour's open log files, which are never pruned, discarding exactly the \
                     incident history logging exists to keep."
                );
                LOG_DIR_MAX_BYTES
            }
            Err(_) => {
                eprintln!(
                    "Warning: FREENET_LOG_DIR_MAX_BYTES={raw:?} is not a valid byte count; \
                     using the default of {LOG_DIR_MAX_BYTES} bytes instead."
                );
                LOG_DIR_MAX_BYTES
            }
        },
    }
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
    let max_bytes = parse_log_dir_max_bytes(log_dir_max_bytes_env().as_deref());

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

    prune_log_files(files, cutoff, max_bytes);
}

/// The indices, in a `files` sorted ascending by `(modified, path)`, of
/// the newest file of each family.
///
/// Discovered from the data rather than by walking a hand-written list
/// of [`LogFamily`] variants: adding a third appender must not require
/// remembering to extend a list here, because forgetting would silently
/// re-expose that appender's open file to deletion — precisely the bug
/// this function exists to prevent.
///
/// **This INFERS which file each appender has open; it does not know.**
/// There is no portable way to ask whether some other part of the
/// process holds a descriptor, so "newest of its family" is a proxy. It
/// is right whenever time moves forward, which is why the caller sorts
/// by `(modified, path)` — see [`prune_log_files`]. A backward clock step
/// mid-hour can defeat it: the appender does not roll (rotation is keyed
/// on the clock too), so its open file's mtime can end up older than a
/// closed sibling's, and the proxy picks the wrong file. The consequence
/// is bounded — it is the same exposure this code removed in the common
/// case, not a new one — and no cheaper signal is available.
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
///
///   **This leg rests on an EXTERNAL property that no test here pins.**
///   Nothing in this crate would fail if a `tracing-appender` bump moved
///   the rollover check after the write; the byte bound would silently
///   become false while all these tests stayed green. It is audited at
///   0.2.5, and `Cargo.toml` carries a pointer back here so a bump is
///   prompted to re-read `rolling.rs`. A local test cannot cover it:
///   forcing a rotation needs control of the appender's clock, which
///   `tracing-appender` exposes only to its own `cfg(test)` builds.
///   If you bump the dependency, re-check that ordering by hand.
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
    // Sorting is load-bearing, not cosmetic: `live_file_indices` reads
    // the live file off the END of each family's run, and the size pass
    // walks this order to delete oldest-first. `cleanup_old_logs` feeds
    // us `read_dir` order, which is arbitrary (hash order on ext4).
    //
    // The path is a tiebreak rather than mtime alone, because mtime is
    // NOT a total order here: on a coarse-granularity filesystem the old
    // file's final write and the new file's creation can land in the same
    // tick, and a stable sort would then fall back to `read_dir` order —
    // leaving the genuinely-open file looking like the older of the two
    // and thus collectable. These file names embed a zero-padded
    // `YYYY-MM-DD-HH` stamp under a fixed per-family prefix, so within a
    // family lexicographic order IS chronological order.
    files.sort_by(|a, b| {
        a.modified
            .cmp(&b.modified)
            .then_with(|| a.path.cmp(&b.path))
    });

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

/// Environment variable that forces the console log layer on even when stdout
/// is not a terminal.
///
/// Named and read like the neighbouring `FREENET_LOG_TO_STDERR`: presence is
/// what counts, so any value (including `0`) enables it.
pub(crate) const LOG_TO_CONSOLE_ENV_VAR: &str = "FREENET_LOG_TO_CONSOLE";

/// Whether to attach the console layer, given whether stdout is a terminal and
/// whether [`LOG_TO_CONSOLE_ENV_VAR`] is set.
///
/// The `is_terminal` probe alone is a proxy for "a human is watching", and it
/// gets containers exactly backwards. A container's stdout is a pipe, so the
/// probe reports "not interactive" for the one deployment where stdout is the
/// ONLY log interface the operator has: `docker logs` shows nothing but the
/// entrypoint banner, and the node looks wedged when it is running fine.
///
/// The obvious workaround, `FREENET_LOG_TO_STDERR`, is wrong here because it
/// turns file logging OFF (see `use_file_logging`), and the log files are what
/// `freenet service report` collects. That would make containerized nodes
/// unsupportable in exchange for being readable. This flag is additive instead:
/// console AND files.
///
/// Split out as a pure function for the same reason as [`error_log_directives`]
/// — it can be tested without mutating process-global environment state, which
/// would race across the test binary's threads.
fn console_logging_enabled(stdout_is_terminal: bool, env_var_set: bool) -> bool {
    stdout_is_terminal || env_var_set
}

/// The `RUST_LOG` directive string exactly as `EnvFilter` itself would
/// read it: the variable's value, or the empty string when unset.
///
/// Split out from [`build_error_filter`] so that the filter construction
/// stays a pure function of its input and can be tested without mutating
/// process-global environment state (which would race across the test
/// binary's threads).
fn error_log_directives() -> String {
    std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).unwrap_or_default()
}

/// Build the filter for the `freenet.error.*` layer: `max(WARN, RUST_LOG)`,
/// evaluated per target.
///
/// The error log is meant to be a WARN-and-above **subset** of the main
/// log, so it can be grepped as a high-signal view of what went wrong.
/// That needs a genuine floor, and `EnvFilter::with_default_directive`
/// is not one — it is a *fallback* that is applied only when the parsed
/// directive string produced no directives at all. See
/// `tracing-subscriber-0.3.23`, `src/filter/env/builder.rs`, the
/// `if !has_dynamics && filter.statics.is_empty()` branch at the end of
/// `Builder::from_directives`.
///
/// So with `RUST_LOG` set — which production sets — the `WARN` default
/// was silently discarded and the error layer inherited `RUST_LOG`'s
/// level: the very same level the main layer uses. The two layers then
/// wrote identical content to two files, doubling log disk on every node
/// (issue #5015; the nova gateway's `freenet.error.*` was 19011 INFO /
/// 776 WARN / 93 ERROR over its first 20000 lines, and every hourly
/// `freenet.*` / `freenet.error.*` pair was byte-for-byte identical).
///
/// AND-ing the env filter with a hard [`LevelFilter::WARN`] makes the
/// level an actual floor, per target:
///
/// - `RUST_LOG` unset → `WARN`+, unchanged from the intended behavior;
/// - `RUST_LOG=info` / `debug` / `trace` → still only `WARN`+;
/// - `RUST_LOG=error` → only `ERROR`. A deliberately *more* restrictive
///   `RUST_LOG` is honored rather than widened — this is `max(WARN, env)`,
///   not "always at least WARN", so the floor never resurrects records the
///   operator explicitly asked to suppress;
/// - per-target directives (`RUST_LOG=freenet::ring=debug`) → that target
///   at `WARN`+ only, so no INFO/DEBUG can leak into the error log through
///   a target-scoped directive either.
///
/// AND-ing (rather than replacing the env filter outright with a bare
/// `LevelFilter::WARN`) is what keeps the operator's `RUST_LOG` in force:
/// a target they silenced stays silent in the error log too.
///
/// This layer deliberately does NOT inherit `build_filter`'s `moka=off` /
/// `sqlx=error` additions. Those exist to suppress third-party INFO/DEBUG
/// chatter from the main log; at WARN+ they are not a volume concern, and
/// adopting them here would remove records the error log captures today.
/// So the error log is a WARN+ subset of the main log for every freenet
/// target, and additionally keeps moka/sqlx WARN+ that the main log
/// suppresses.
///
/// The one place this change removes records from EVERY sink: because the
/// main layer sets `moka=off` / `sqlx=error`, moka's INFO and sqlx's INFO
/// under `RUST_LOG=info` used to reach the error log **and nowhere else**,
/// and now reach neither. That is the intended WARN+ semantics rather than
/// an oversight, and it is narrow — moka is the contract cache, and moka
/// and sqlx WARN/ERROR are still retained here. Anything at WARN or above
/// from any target still lands in this log.
///
/// Only the error layer is affected. The main log's filter (`build_filter`
/// inside [`init_tracer`]) and the console layer are untouched, so nothing
/// is lost from the main log or the journal.
fn build_error_filter<S>(directives: &str) -> impl tracing_subscriber::layer::Filter<S> + 'static
where
    S: 'static,
{
    use tracing_subscriber::filter::FilterExt;

    tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .parse_lossy(directives)
        .and(LevelFilter::WARN)
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

    // Also output to console when running interactively (stdout is a terminal),
    // or when FREENET_LOG_TO_CONSOLE forces it for a container, where stdout is
    // a pipe but is still the operator's only view of the logs. Either way file
    // logging is unaffected, so diagnostic reports keep working. See
    // `console_logging_enabled`.
    let also_log_to_console = console_logging_enabled(
        std::io::stdout().is_terminal(),
        std::env::var(LOG_TO_CONSOLE_ENV_VAR).is_ok(),
    );

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

                let error_layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(error_writer.clone())
                    .with_filter(build_error_filter(&error_log_directives()));

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

                let error_layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(error_writer)
                    .with_filter(build_error_filter(&error_log_directives()));

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

/// Install a stderr-only subscriber for a short-lived CLI subcommand.
///
/// ## Why this exists (#5244)
///
/// `freenet update` ran with **no subscriber at all**: `set_logger` is called
/// only on the node path (`run_node`), so every `tracing::warn!` / `error!` in
/// the installer was a no-op. Combined with the supervisor invoking it as
/// `freenet update --quiet`, sites whose only output was a `warn!` plus a
/// `!quiet`-gated `eprintln!` were completely silent in production — including
/// "installed the update but could not arm crash-loop rollback".
///
/// ## Why not just call [`set_logger`] here
///
/// Two traps, both of which produce a fix that looks done and changes nothing:
///
/// 1. Passing a `log_dir` (the natural copy-paste from `run_node`) sets
///    `use_file_logging`, which routes everything into the rolling log files.
///    systemd captures stdout/stderr, NOT those files — that asymmetry is the
///    whole of #5232. Every `warn!` would start "working" and the journal would
///    stay exactly as blind.
/// 2. With no log dir, [`init_tracer`] still falls through to stdout unless the
///    `FREENET_LOG_TO_STDERR` env var happens to be set.
///
/// So this is explicit rather than configuration-dependent: **stderr, always**.
/// Diagnostics belong there, it leaves the command's human-facing `println!`
/// output on stdout, and systemd records both.
///
/// Deliberately minimal: no file appenders, no rate limiters. This is a
/// process that runs for seconds and exits, and the output it must not lose is
/// the handful of lines saying a safety mechanism did not engage.
///
/// `FREENET_DISABLE_LOGS` is still honoured, and `RUST_LOG` still overrides
/// `level` via `from_env_lossy`.
pub fn init_cli_stderr_tracer(level: LevelFilter) -> anyhow::Result<()> {
    if std::env::var("FREENET_DISABLE_LOGS").is_ok() {
        return Ok(());
    }
    let use_json = std::env::var("FREENET_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    let filter_layer = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy()
        .add_directive("moka=off".parse().expect("infallible"))
        .add_directive("sqlx=error".parse().expect("infallible"));

    // ANSI only for a human at a terminal. journald stores what it is given
    // byte for byte, so colouring unconditionally would write escape sequences
    // into the journal — which is the destination this whole function exists to
    // reach. The file layers in `init_tracer` set `.with_ansi(false)` for the
    // same reason.
    let ansi = std::io::stderr().is_terminal();

    use tracing_subscriber::layer::SubscriberExt;
    let registry = Registry::default().with(filter_layer);
    // `compact` rather than the `pretty` multi-line format `init_stdout_tracer`
    // uses: one journal entry per event is far easier to read (and to grep)
    // than an event split across several.
    let result = if use_json {
        tracing::subscriber::set_global_default(
            registry.with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_writer(std::io::stderr),
            ),
        )
    } else {
        tracing::subscriber::set_global_default(
            registry.with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_ansi(ansi)
                    .with_writer(std::io::stderr),
            ),
        )
    };
    // Returned, not `expect`ed. `init_stdout_tracer` panics here, which for a
    // CLI would turn "a subscriber was already installed" into a failed update
    // — trading a diagnostics problem for an outage.
    result.map_err(|e| anyhow::anyhow!("could not install the CLI subscriber: {e}"))
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

/// Coverage for the console-layer decision, which is what makes a
/// containerized node's logs visible to `docker logs` at all.
#[cfg(test)]
mod console_logging_tests {
    use super::{LOG_TO_CONSOLE_ENV_VAR, console_logging_enabled};

    #[test]
    fn interactive_stdout_still_logs_to_console() {
        assert!(
            console_logging_enabled(true, false),
            "a terminal must keep its console output with no env var set"
        );
    }

    /// The container case, and the reason this flag exists. Without it a
    /// containerized node writes nothing to `docker logs` beyond its
    /// entrypoint banner and looks wedged while running perfectly.
    #[test]
    fn piped_stdout_logs_to_console_when_forced() {
        assert!(
            console_logging_enabled(false, true),
            "FREENET_LOG_TO_CONSOLE must enable console output when stdout is a pipe"
        );
    }

    #[test]
    fn a_terminal_with_the_flag_set_also_logs_to_console() {
        assert!(
            console_logging_enabled(true, true),
            "setting the flag on an interactive terminal must not turn console output off"
        );
    }

    /// The default must not change: a piped stdout with no opt-in stays quiet,
    /// so nothing starts double-logging into a pipeline that did not ask for it.
    #[test]
    fn piped_stdout_stays_quiet_by_default() {
        assert!(
            !console_logging_enabled(false, false),
            "a pipe with no opt-in must not gain console output"
        );
    }

    /// Pinned so the container image and the code cannot drift apart: the
    /// Dockerfile sets this exact name.
    #[test]
    fn env_var_name_is_the_one_the_container_image_sets() {
        assert_eq!(LOG_TO_CONSOLE_ENV_VAR, "FREENET_LOG_TO_CONSOLE");

        let dockerfile = include_str!("../../../../docker/freenet-node/Dockerfile");
        assert!(
            dockerfile.contains(LOG_TO_CONSOLE_ENV_VAR),
            "docker/freenet-node/Dockerfile must set {LOG_TO_CONSOLE_ENV_VAR}, or a \
             containerized node logs nothing to `docker logs`"
        );
    }
}

/// Regression coverage for issue #5015: `freenet.error.*` was a
/// byte-for-byte duplicate of the main log whenever `RUST_LOG` was set.
#[cfg(test)]
mod error_filter_tests {
    use super::{build_error_filter, error_log_directives};
    use std::io;
    use std::sync::{Arc, Mutex};
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::layer::{Filter, SubscriberExt};
    use tracing_subscriber::{Layer, Registry};

    /// The most permissive level the error filter will admit for the
    /// supplied `RUST_LOG` string. `Filter::max_level_hint` on the
    /// `And` combinator is `min(env, WARN)`, so this is a direct,
    /// clock-free, subscriber-free read of the floor.
    fn error_hint(directives: &str) -> Option<LevelFilter> {
        Filter::<Registry>::max_level_hint(&build_error_filter::<Registry>(directives))
    }

    /// Documents the upstream behavior this fix works around, and keeps
    /// the rest of this module honest: if `with_default_directive` ever
    /// became a real floor upstream, this assertion fails and the
    /// workaround can be revisited. It is also the non-vacuity control
    /// for `error_filter_floors_verbose_rust_log_at_warn` — the two
    /// differ only by the `.and(LevelFilter::WARN)` under test.
    #[test]
    fn env_filter_default_directive_is_a_fallback_not_a_floor() {
        let unfloored = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .parse_lossy("info");
        assert_eq!(
            unfloored.max_level_hint(),
            Some(LevelFilter::INFO),
            "with_default_directive(WARN) must be discarded once RUST_LOG parses to \
             any directive — that discard is the #5015 bug"
        );
    }

    /// With `RUST_LOG` unset the error log keeps its pre-existing
    /// WARN+ behavior. This is the one case that was already correct;
    /// the fix must not change it.
    #[test]
    fn error_filter_is_warn_when_rust_log_unset() {
        assert_eq!(error_hint(""), Some(LevelFilter::WARN));
    }

    /// The bug proper: a verbose `RUST_LOG` must not widen the error log.
    #[test]
    fn error_filter_floors_verbose_rust_log_at_warn() {
        for directives in ["info", "debug", "trace", "freenet=info", "freenet=trace"] {
            assert_eq!(
                error_hint(directives),
                Some(LevelFilter::WARN),
                "RUST_LOG={directives} must not admit anything below WARN into the error log"
            );
        }
    }

    /// Per-target directives must not smuggle INFO/DEBUG into the error
    /// log through a narrower target scope.
    #[test]
    fn error_filter_floors_per_target_directives_at_warn() {
        for directives in [
            "freenet::ring=debug",
            "info,freenet::ring=debug",
            "freenet::ring=trace,freenet::transport=debug",
        ] {
            assert_eq!(
                error_hint(directives),
                Some(LevelFilter::WARN),
                "RUST_LOG={directives} must not admit anything below WARN into the error log"
            );
        }
    }

    /// The floor is `max(WARN, RUST_LOG)`, not "always at least WARN":
    /// an operator who deliberately asks for *less* must get less, never
    /// more than they asked for.
    #[test]
    fn error_filter_honors_a_more_restrictive_rust_log() {
        assert_eq!(
            error_hint("error"),
            Some(LevelFilter::ERROR),
            "RUST_LOG=error must leave the error log at ERROR only, not widen it to WARN"
        );
        assert_eq!(
            error_hint("off"),
            Some(LevelFilter::OFF),
            "RUST_LOG=off must silence the error log too"
        );
        assert_eq!(
            error_hint("freenet=error"),
            Some(LevelFilter::ERROR),
            "a more restrictive per-target directive must also be honored"
        );
    }

    /// `error_log_directives` must read exactly the variable `EnvFilter`
    /// itself reads, so swapping `from_env_lossy()` for
    /// `parse_lossy(error_log_directives())` is behavior-preserving on
    /// the env-reading half.
    #[test]
    fn error_log_directives_reads_rust_log() {
        assert_eq!(tracing_subscriber::EnvFilter::DEFAULT_ENV, "RUST_LOG");
        assert_eq!(
            error_log_directives(),
            std::env::var("RUST_LOG").unwrap_or_default()
        );
    }

    #[derive(Clone)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// End-to-end proof that `Filter::enabled` (not just the level hint)
    /// drops sub-WARN records from an error-log-shaped layer.
    ///
    /// Deterministic by construction: the subscriber is installed with
    /// `tracing::subscriber::with_default` (thread-local, so parallel
    /// tests cannot observe or perturb it), the writer is synchronous
    /// and in-memory (no `tracing_appender` worker thread, no flush
    /// race), no environment variable is mutated, and the event messages
    /// are unique to this test.
    #[test]
    fn error_layer_writes_only_warn_and_above_under_verbose_rust_log() {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let layer = tracing_subscriber::fmt::layer()
            .with_level(true)
            .with_ansi(false)
            .with_writer(CaptureWriter(sink.clone()))
            .with_filter(build_error_filter("debug"));

        tracing::subscriber::with_default(Registry::default().with(layer), || {
            tracing::debug!("i5015-debug-must-be-dropped");
            tracing::info!("i5015-info-must-be-dropped");
            tracing::warn!("i5015-warn-must-be-kept");
            tracing::error!("i5015-error-must-be-kept");
        });

        let captured = String::from_utf8(sink.lock().unwrap().clone()).unwrap();
        assert!(
            !captured.contains("i5015-debug-must-be-dropped"),
            "DEBUG leaked into the error log under RUST_LOG=debug: {captured}"
        );
        assert!(
            !captured.contains("i5015-info-must-be-dropped"),
            "INFO leaked into the error log under RUST_LOG=debug: {captured}"
        );
        assert!(
            captured.contains("i5015-warn-must-be-kept"),
            "WARN must still reach the error log: {captured}"
        );
        assert!(
            captured.contains("i5015-error-must-be-kept"),
            "ERROR must still reach the error log: {captured}"
        );
    }

    /// A sibling layer with the main log's (unfloored) filter must be
    /// unaffected by the error layer's WARN floor when both are attached
    /// to the same subscriber.
    ///
    /// This is the hard constraint on this fix: per-layer filtering must
    /// not let the error layer's `Interest::never` for sub-WARN callsites
    /// short-circuit the main layer. Without this test a regression would
    /// silently delete INFO/DEBUG from `freenet.*` — a far worse outcome
    /// than the disk the fix saves.
    ///
    /// `init_tracer` builds TWO subscriber shapes and both ship, so both
    /// are exercised via `with_global_rate_filter`:
    ///
    /// - `false` → `Registry.with(main).with(error)`, taken when rate
    ///   limiting is off, i.e. debug/test builds or
    ///   `FREENET_DISABLE_LOG_RATE_LIMIT`;
    /// - `true` → `Registry.with(<global DynFilterFn>).with(main).with(error)`,
    ///   which is what **release builds actually run**. The global filter
    ///   layer sits beneath both sinks and composes `max_level_hint`
    ///   differently, so testing only the first shape would leave the
    ///   shipping one uncovered.
    fn assert_error_floor_does_not_starve_main_layer(with_global_rate_filter: bool) {
        let main_sink = Arc::new(Mutex::new(Vec::new()));
        let error_sink = Arc::new(Mutex::new(Vec::new()));

        // Built per branch: a layer's `Filter<S>` is parameterized by the
        // subscriber it attaches to, and `S` differs between the shapes.
        macro_rules! layers {
            () => {
                (
                    tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .with_ansi(false)
                        .with_writer(CaptureWriter(main_sink.clone()))
                        .with_filter(tracing_subscriber::EnvFilter::builder().parse_lossy("info")),
                    tracing_subscriber::fmt::layer()
                        .with_level(true)
                        .with_ansi(false)
                        .with_writer(CaptureWriter(error_sink.clone()))
                        .with_filter(build_error_filter("info")),
                )
            };
        }

        let emit = || {
            tracing::info!("i5015-sibling-info");
            tracing::warn!("i5015-sibling-warn");
        };

        if with_global_rate_filter {
            // Always-true so this isolates filter composition rather than
            // the rate limiter's own behavior; the shape is what matters.
            let pass_all = tracing_subscriber::filter::DynFilterFn::new(|_meta, _cx| true);
            let (main_layer, error_layer) = layers!();
            let subscriber = Registry::default()
                .with(pass_all)
                .with(main_layer)
                .with(error_layer);
            tracing::subscriber::with_default(subscriber, emit);
        } else {
            let (main_layer, error_layer) = layers!();
            let subscriber = Registry::default().with(main_layer).with(error_layer);
            tracing::subscriber::with_default(subscriber, emit);
        }

        let shape = if with_global_rate_filter {
            "rate-limited (release) shape"
        } else {
            "plain (debug/test) shape"
        };
        let main = String::from_utf8(main_sink.lock().unwrap().clone()).unwrap();
        let error = String::from_utf8(error_sink.lock().unwrap().clone()).unwrap();

        assert!(
            main.contains("i5015-sibling-info"),
            "[{shape}] the error layer's WARN floor must not suppress INFO on the \
             main layer: {main}"
        );
        assert!(
            main.contains("i5015-sibling-warn"),
            "[{shape}] the main layer must still receive WARN: {main}"
        );
        assert!(
            !error.contains("i5015-sibling-info"),
            "[{shape}] the error layer must still drop INFO: {error}"
        );
        assert!(
            error.contains("i5015-sibling-warn"),
            "[{shape}] the error layer must still receive WARN: {error}"
        );
    }

    #[test]
    fn main_layer_still_receives_info_alongside_the_floored_error_layer() {
        assert_error_floor_does_not_starve_main_layer(false);
    }

    /// Same hard constraint, for the subscriber shape release builds
    /// actually construct (global rate-limit filter beneath both layers).
    #[test]
    fn main_layer_still_receives_info_under_the_release_rate_limited_shape() {
        assert_error_floor_does_not_starve_main_layer(true);
    }

    /// Call-site pin. The filter helper being correct is worthless if
    /// `init_tracer` stops using it, and neither the level-hint tests nor
    /// the capture tests above would notice: they exercise
    /// `build_error_filter` directly, so deleting the wiring leaves them
    /// green. This scrapes the production half of `tracer.rs` and asserts
    /// that every error-log layer is filtered through the helper.
    ///
    /// Needles are matched against whitespace-stripped source so a
    /// `rustfmt` line-wrap of a growing call cannot silently disarm the
    /// pin, and the module marker is assembled with `concat!` so this
    /// test's own source cannot satisfy it.
    #[test]
    fn init_tracer_wires_every_error_layer_through_build_error_filter() {
        let source = include_str!("tracer.rs");

        // Cut at this test module's declaration (the first one in the file)
        // so test source, including this function, can never satisfy a
        // production-code assertion. Deliberately NOT cut at `#[cfg(test)]`:
        // that attribute also appears on individual items, and cutting at the
        // first one can truncate production code and leave the pin vacuous.
        // The anchor is this module's own declaration, spelled with
        // `concat!` so the needle never appears contiguously in source and
        // cannot match itself. An exact anchor also fails CLOSED: rename the
        // module and `find` returns `None`, so the `expect` fires loudly. A
        // looser needle (e.g. just `tests {`) would also match the prose and
        // the `expect` string below, so a rename would silently slide the cut
        // point down and swallow test functions into the "production" slice.
        let cut = source.find(concat!("mod error_filter_", "tests {")).expect(
            "the call-site pin anchors on this module's declaration; \
             if it was renamed, update the anchor deliberately",
        );
        let production: String = source[..cut]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        let count = |needle: &str| production.matches(needle).count();

        let error_layers = count(".with_writer(error_writer");
        assert_eq!(
            error_layers, 2,
            "expected the two error-log layers (rate-limited and plain registry); \
             if this changed, update the pin deliberately"
        );

        // Pin the whole wiring, not just the helper's name: passing anything
        // other than the live `RUST_LOG` would silently stop honoring a
        // deliberately more-restrictive operator setting.
        let floored = count(".with_filter(build_error_filter(&error_log_directives()))");
        assert_eq!(
            floored, error_layers,
            "every freenet.error.* layer must be filtered through \
             build_error_filter(&error_log_directives()); found {error_layers} error \
             layers but {floored} floored filters (#5015)"
        );

        assert_eq!(
            count(".and(LevelFilter::WARN)"),
            1,
            "the WARN floor must live in build_error_filter and nowhere else"
        );

        // `from_env_lossy` belongs to CONSOLE filters only, because RUST_LOG is
        // the operator's console knob. An error layer using it would inherit
        // RUST_LOG's level and lose the WARN floor again (#5015).
        //
        // There are exactly two console filters: `build_filter` (the node's
        // main + console layers) and `init_cli_stderr_tracer` (the CLI's own
        // stderr layer, added for #5244 so `freenet update` is not mute).
        //
        // Counting is not enough on its own — two callers could be the wrong
        // two — so each is pinned to its own function body below, and the total
        // is then pinned so a THIRD caller anywhere fails here rather than
        // sliding in unnoticed.
        let body_of = |sig: &str| -> String {
            let start = source.find(sig).unwrap_or_else(|| {
                panic!("{sig} not found; if it was renamed, update this pin deliberately")
            });
            let rest = &source[start + sig.len()..];
            // Next top-level or nested `fn ` declaration ends the body well
            // enough for a call-site count; an over-long slice would only make
            // this pin STRICTER, never laxer.
            let end = rest.find("\n    fn ").or_else(|| rest.find("\npub fn ")).unwrap_or(rest.len());
            rest[..end].to_string()
        };

        assert_eq!(
            body_of("fn build_filter(").matches(".from_env_lossy()").count(),
            1,
            "build_filter is the node's console filter and must read RUST_LOG"
        );
        assert_eq!(
            body_of("pub fn init_cli_stderr_tracer(")
                .matches(".from_env_lossy()")
                .count(),
            1,
            "init_cli_stderr_tracer is the CLI's console filter and must read RUST_LOG \
             (#5244)"
        );
        assert_eq!(
            count(".from_env_lossy()"),
            2,
            "only the two console filters may use from_env_lossy(); a third caller is \
             an error layer inheriting RUST_LOG's level again (#5015)"
        );
    }
}

#[cfg(test)]
mod cleanup_tests {
    use super::{
        LOG_DIR_MAX_BYTES, LogFamily, LogFile, MIN_LOG_DIR_MAX_BYTES, cleanup_old_logs,
        live_file_indices, parse_log_dir_max_bytes, periodic_log_prune, prune_log_files,
        rotating_log_family,
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

    /// The budget must hold a full day of a BUSY node's logs.
    ///
    /// Sizing it from a quiet peer is the mistake this test exists to
    /// prevent. Measured on the production gateway (nova): 54 rotating
    /// files, 518.9 MiB, spanning 25.97 hours = 20.95 MB/hour. At 96 MiB
    /// that node would retain 4.8 hours, so an 18:00 incident would be
    /// gone by 09:00 — the logs would be bounded but useless.
    ///
    /// For contrast, this same budget is not what binds on a quiet peer:
    /// at ~1.4 MB/h a laptop peer reaches `LOG_RETENTION_HOURS` (72h)
    /// having used under 100 MiB, so its retention is decided by age.
    ///
    /// The rate below is deliberately the **pre-#5015** measurement, taken
    /// while `freenet.error.*` still duplicated the main log. Post-#5015
    /// that gateway runs at ~10.8 MB/h, so keeping the older, higher rate
    /// makes this guard strictly more conservative: it demands the budget
    /// hold a day at the worst rate ever observed. Do not "refresh" it to
    /// the lower figure — that would weaken the assertion, permitting a
    /// budget that no longer holds a day on any node still logging at the
    /// old rate.
    #[test]
    fn default_budget_holds_a_day_of_a_busy_gateways_logs() {
        // nova, 2026-07, pre-#5015: 518.9 MiB over 25.97h.
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

    /// No override configured → the compiled-in default, unchanged.
    #[test]
    fn log_dir_max_bytes_default_when_env_absent() {
        assert_eq!(parse_log_dir_max_bytes(None), LOG_DIR_MAX_BYTES);
    }

    /// An explicitly-empty value is treated the same as absent, not as
    /// garbage — some deploy tooling sets `VAR=` to mean "unset" rather
    /// than omitting the variable.
    #[test]
    fn log_dir_max_bytes_default_when_env_empty() {
        assert_eq!(parse_log_dir_max_bytes(Some("")), LOG_DIR_MAX_BYTES);
    }

    /// A valid override at or above the floor is honoured exactly,
    /// whether it narrows the default (small background peer) or widens
    /// it (chasing an intermittent fault on a busy node).
    #[test]
    fn log_dir_max_bytes_honours_a_valid_override() {
        assert_eq!(
            parse_log_dir_max_bytes(Some(&MIN_LOG_DIR_MAX_BYTES.to_string())),
            MIN_LOG_DIR_MAX_BYTES,
            "the floor itself must be accepted, not rejected as \"below\" it"
        );
        assert_eq!(
            parse_log_dir_max_bytes(Some("1073741824")), // 1 GiB, above the default
            1_073_741_824,
        );
    }

    /// Unparseable garbage falls back to the default rather than
    /// panicking or propagating an error — a typo in the env var must
    /// not stop the node from booting or from pruning.
    #[test]
    fn log_dir_max_bytes_default_when_env_garbage() {
        for garbage in ["not-a-number", "512MiB", "-1", "1.5", "0x200"] {
            assert_eq!(
                parse_log_dir_max_bytes(Some(garbage)),
                LOG_DIR_MAX_BYTES,
                "garbage value {garbage:?} must fall back to the default"
            );
        }
    }

    /// `0` is the sharpest form of the footgun `MIN_LOG_DIR_MAX_BYTES`
    /// guards against — it would mean "delete everything the size pass
    /// may delete" — and must fall back to the default, not be honoured
    /// literally.
    #[test]
    fn log_dir_max_bytes_default_when_env_zero() {
        assert_eq!(parse_log_dir_max_bytes(Some("0")), LOG_DIR_MAX_BYTES);
    }

    /// A small-but-nonzero value below the floor is just as destructive
    /// as zero (issue #5019 item 3: the current-hour files alone can fill
    /// it) and must also fall back to the default.
    #[test]
    fn log_dir_max_bytes_default_when_env_below_floor() {
        assert_eq!(
            parse_log_dir_max_bytes(Some(&(MIN_LOG_DIR_MAX_BYTES - 1).to_string())),
            LOG_DIR_MAX_BYTES
        );
        assert_eq!(parse_log_dir_max_bytes(Some("1")), LOG_DIR_MAX_BYTES);
    }

    /// Bound `include_str!`-scraped source to one free function's body, so a
    /// source-scrape pin can never silently widen to the whole file or match
    /// itself. Copied from `bin/commands/auto_update.rs::fn_body` (see its
    /// doc for the incident history motivating each check below) rather than
    /// shared, since this file has no existing dependency on that module.
    fn fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let at = src
            .find(signature)
            .unwrap_or_else(|| panic!("definition not found: {signature}"));
        // Only valid for FREE functions at column 0 — a method's closing
        // brace is indented, so the first `\n}\n` after it belongs to the
        // enclosing `impl`, silently returning every sibling method too.
        let tests_at = src
            .find("\n#[cfg(test)]\nmod ")
            .map(|i| i + 1)
            .expect("test module not located — this guard cannot verify anything");
        assert!(
            at < tests_at,
            "`{signature}` matched inside the test module — this pin is \
             scraping its own source and would pass vacuously"
        );
        assert!(
            at == 0 || src.as_bytes()[at - 1] == b'\n',
            "fn_body only supports column-0 free functions; `{signature}` is \
             indented (a method?), where the `\\n}}\\n` end-anchor would slice to \
             the end of the enclosing impl instead"
        );
        let after = &src[at + signature.len()..];
        let (body, _) = after
            .split_once("\n}\n")
            .unwrap_or_else(|| panic!("could not locate end of: {signature}"));
        // Vacuity detector: a correctly-bounded function body can never
        // contain the test-module attribute. If it does, the `\n}\n` search
        // ran past the function and this pin is measuring the whole file.
        assert!(
            !body.contains("\n#[cfg(test)]\nmod "),
            "scoped region for `{signature}` escaped into the test module — this \
             pin would pass vacuously"
        );
        body
    }

    /// Pin that `cleanup_old_logs` actually wires the parsed
    /// `FREENET_LOG_DIR_MAX_BYTES` override into `prune_log_files`, rather
    /// than computing it and discarding it in favor of the hardcoded
    /// `LOG_DIR_MAX_BYTES` constant.
    ///
    /// None of the `log_dir_max_bytes_*` tests above can catch this class of
    /// regression: they call `parse_log_dir_max_bytes` directly and stay
    /// green even if nothing in production ever calls it — a well-tested
    /// function wired to nothing, the same shape
    /// `init_tracer_wires_every_error_layer_through_build_error_filter`
    /// (`error_filter_tests`, this file) already guards against for the
    /// error-filter wiring. Verified against a live mutation of the call
    /// site (`let max_bytes = LOG_DIR_MAX_BYTES;`), which this pin catches
    /// and the runtime tests above do not — see the PR description for
    /// issue #5021.
    ///
    /// Source-scrape rather than a runtime assertion because reaching this
    /// call site behaviourally would mean mutating
    /// `FREENET_LOG_DIR_MAX_BYTES` in the process environment from a test,
    /// exactly the cross-test interference `.claude/rules/testing.md` and
    /// `parse_log_dir_max_bytes`'s own doc warn against.
    #[test]
    fn cleanup_old_logs_wires_the_parsed_budget_into_prune_log_files() {
        let body = fn_body(
            include_str!("tracer.rs"),
            "fn cleanup_old_logs(log_dir: &std::path::Path) {",
        );

        // Anchor on the call itself — the API surface that must be
        // reached — not on the local variable it's assigned to, which a
        // harmless rename should not be able to break this pin.
        let calls = body
            .matches("parse_log_dir_max_bytes(log_dir_max_bytes_env().as_deref())")
            .count();
        assert_eq!(
            calls, 1,
            "cleanup_old_logs must call parse_log_dir_max_bytes(log_dir_max_bytes_env()...) \
             exactly once to resolve the byte budget; if the call was renamed or removed, \
             update this pin deliberately"
        );

        // And the parsed value — not a hardcoded default sitting next to
        // it — must be what reaches prune_log_files. LOG_DIR_MAX_BYTES is
        // a stable named constant (itself pinned elsewhere in this file),
        // not a local variable subject to casual renaming, so anchoring
        // the negative check on it doesn't share that fragility.
        assert!(
            !body.contains("prune_log_files(files, cutoff, LOG_DIR_MAX_BYTES)"),
            "prune_log_files must never be called with the hardcoded LOG_DIR_MAX_BYTES \
             constant directly — that would silently disconnect \
             FREENET_LOG_DIR_MAX_BYTES from the pruner"
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

    /// `prune_log_files` must sort before it does anything else.
    ///
    /// `live_file_indices` reads the live file off the END of each
    /// family's run, and the size pass walks the same order to delete
    /// oldest-first — both are wrong on unsorted input. Its one
    /// production caller passes `read_dir` order, which is arbitrary
    /// (hash order on ext4), so the sort is the only thing making either
    /// correct. Nothing else in this suite feeds it out-of-order input:
    /// without this test, deleting the sort would pass or fail depending
    /// on how the filesystem happened to enumerate a temp directory.
    #[test]
    fn prune_log_files_sorts_before_choosing_victims() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();

        let at = |hours_ago: u64| now - Duration::from_secs(hours_ago * 3600);
        let m1 = dir.path().join("freenet.2026-05-25-10.log");
        let e1 = dir.path().join("freenet.error.2026-05-25-11.log");
        let m2 = dir.path().join("freenet.2026-05-25-12.log");
        let e2 = dir.path().join("freenet.error.2026-05-25-13.log");
        let m3 = dir.path().join("freenet.2026-05-25-14.log");
        for (path, hours) in [(&m1, 5), (&e1, 4), (&m2, 3), (&e2, 2), (&m3, 1)] {
            write_with_mtime(path, 1024, at(hours));
        }

        // Deliberately shuffled: newest first, oldest in the middle.
        // A pruner that trusted this order would treat m2 and e2 as the
        // live files and delete the genuinely-open m3.
        size_pass_only(
            vec![
                log_file(&m3, at(1), 1024),
                log_file(&e1, at(4), 1024),
                log_file(&m1, at(5), 1024),
                log_file(&e2, at(2), 1024),
                log_file(&m2, at(3), 1024),
            ],
            2048,
        );

        assert!(
            m3.exists(),
            "the newest Main file is the open one and must survive however \
             the caller ordered the input"
        );
        assert!(
            e2.exists(),
            "the newest Error file is the open one and must survive however \
             the caller ordered the input"
        );
        for (path, name) in [(&m1, "m1"), (&e1, "e1"), (&m2, "m2")] {
            assert!(!path.exists(), "{name} is evictable and must be deleted");
        }
    }

    /// An mtime tie inside one family must break by name, not by input
    /// order.
    ///
    /// `SystemTime` is not a total order over these files: on a
    /// coarse-granularity filesystem the outgoing file's last write and
    /// the incoming file's creation can land in the same tick, exactly at
    /// the rotation instant. A stable sort keyed on mtime alone then
    /// preserves `read_dir` order, so whichever the filesystem happened
    /// to enumerate last is taken for the open file — and the real one
    /// becomes an ordinary deletion candidate.
    #[test]
    fn live_pick_breaks_mtime_ties_by_name() {
        let dir = tempfile::tempdir().unwrap();
        // Identical mtimes, as a same-tick rotation would produce.
        let tied = SystemTime::now();

        let older = dir.path().join("freenet.error.2026-05-25-12.log");
        let newer = dir.path().join("freenet.error.2026-05-25-13.log");
        write_with_mtime(&older, 1024, tied);
        write_with_mtime(&newer, 1024, tied);

        // Ordered so that mtime-only sorting leaves `older` last, i.e.
        // mistaken for the live file.
        size_pass_only(
            vec![log_file(&newer, tied, 1024), log_file(&older, tied, 1024)],
            1024,
        );

        assert!(
            newer.exists(),
            "the later rotation stamp is the open file and must survive the tie"
        );
        assert!(
            !older.exists(),
            "the superseded file must be the one collected"
        );
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
