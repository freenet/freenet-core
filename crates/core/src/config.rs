use std::{
    collections::HashSet,
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, atomic::AtomicBool},
    time::Duration,
};

use anyhow::Context;
use directories::ProjectDirs;
use either::Either;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::{
    dev_tool::PeerId,
    local_node::OperationMode,
    tracing::tracer::get_log_dir,
    transport::{CongestionControlAlgorithm, CongestionControlConfig, TransportKeypair},
};

pub(crate) mod kek;
mod secret;
pub use kek::{
    KEK_SIZE, KekBackend, KekBackendKind, KekError, ensure_kek_loaded, load_from_backend,
    read_backend_marker, replace_backend_marker, resolve_first_start, write_backend_marker,
};
pub use secret::*;

/// Default maximum number of connections for the peer.
pub const DEFAULT_MAX_CONNECTIONS: usize = crate::ring::Ring::DEFAULT_MAX_CONNECTIONS;
/// Default minimum number of connections for the peer.
pub const DEFAULT_MIN_CONNECTIONS: usize = crate::ring::Ring::DEFAULT_MIN_CONNECTIONS;
/// Default threshold for randomizing potential peers for new connections.
///
/// If the hops left for the operation is above or equal to this threshold
/// (of the total DEFAULT_MAX_HOPS_TO_LIVE), then the next potential peer
/// will be selected randomly. Otherwise the optimal peer will be selected
/// by Freenet custom algorithms.
pub const DEFAULT_RANDOM_PEER_CONN_THRESHOLD: usize = 7;
/// Default maximum number of hops to live for any operation
/// (if it applies, e.g. connect requests).
pub const DEFAULT_MAX_HOPS_TO_LIVE: usize = 10;

/// Default UDP port a gateway listens on.
///
/// Used as the fallback when a gateway address in `gateways.toml` specifies a
/// host without an explicit port. This is a fixed, well-known value (NOT a
/// randomly chosen free port like [`default_network_api_port`]): a gateway we
/// are trying to *reach* must be addressed at its real listening port, and a
/// random local port would make the gateway unreachable (issue #1388).
pub const DEFAULT_GATEWAY_PORT: u16 = 31337;

/// How long an operation (GET, PUT, SUBSCRIBE, etc.) can run before timing out.
pub(crate) const OPERATION_TTL: Duration = Duration::from_secs(60);

/// Current version of the crate.
pub(crate) const PCK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Minimum compatible version for range-based version checking.
/// Set at build time via FREENET_MIN_COMPATIBLE_VERSION env var.
/// Defaults to PCK_VERSION (strict match) when not overridden.
pub(crate) const MIN_COMPATIBLE_VERSION: &str = env!("FREENET_MIN_COMPATIBLE_VERSION");

// Initialize the executor once.
static ASYNC_RT: LazyLock<Option<Runtime>> = LazyLock::new(GlobalExecutor::initialize_async_rt);

const DEFAULT_TRANSIENT_BUDGET: usize = 2048;
const DEFAULT_TRANSIENT_TTL_SECS: u64 = 30;
const DEFAULT_EVENT_LOOP_CHANNEL_CAPACITY: usize = 2048;

const QUALIFIER: &str = "";
const ORGANIZATION: &str = "The Freenet Project Inc";
const APPLICATION: &str = "Freenet";

const FREENET_GATEWAYS_INDEX: &str = "https://freenet.org/keys/gateways.toml";

#[derive(clap::Parser, Debug, Clone)]
pub struct ConfigArgs {
    /// Node operation mode. Default is network mode.
    #[arg(value_enum, env = "MODE")]
    pub mode: Option<OperationMode>,

    #[command(flatten)]
    pub ws_api: WebsocketApiArgs,

    #[command(flatten)]
    pub network_api: NetworkArgs,

    #[command(flatten)]
    pub secrets: SecretArgs,

    #[arg(long, env = "LOG_LEVEL")]
    pub log_level: Option<tracing::log::LevelFilter>,

    #[command(flatten)]
    pub config_paths: ConfigPathsArgs,

    /// An arbitrary identifier for the node, mostly for debugging or testing purposes.
    #[arg(long, hide = true)]
    pub id: Option<String>,

    /// Show the version of the application.
    #[arg(long, short)]
    pub version: bool,

    /// Maximum number of threads for blocking operations (WASM execution, etc.).
    /// Default: 2x CPU cores, clamped to 4-32.
    #[arg(long, env = "MAX_BLOCKING_THREADS")]
    pub max_blocking_threads: Option<usize>,

    /// Budget in bytes for hosted contract state. Once it is exceeded,
    /// contracts are evicted (least valuable first) and their on-disk state is
    /// reclaimed. This counts contract state only; WASM code blobs and database
    /// overhead are extra. Default: 1 GiB.
    #[arg(long, env = "MAX_HOSTING_STORAGE")]
    pub max_hosting_storage: Option<u64>,

    /// Fraction (0.0 to 1.0) of the disk space available to Freenet (`used +
    /// free` on the data-dir mount) used to size the disk budget. Hosting
    /// eviction uses whichever budget is smaller, memory or disk. Default: 0.5.
    // Internal (#4683): `effective_budget = min(ram_budget, disk_budget)`.
    #[arg(long, env = "HOSTING_DISK_PCT")]
    pub hosting_disk_pct: Option<f64>,

    /// Upper limit in bytes on the disk budget, so a host with a very large
    /// data disk does not get an unbounded budget. This is the disk equivalent
    /// of `--max-hosting-storage`. Default: 32 GiB.
    // Internal: #4683.
    #[arg(long, env = "MAX_HOSTING_DISK")]
    pub max_hosting_disk: Option<u64>,

    /// Fraction (0.0 to 1.0) of spare host memory (this process's resident size
    /// plus the memory the system reports as available) that the
    /// resident-overhead budget may claim on top of what it already uses. That
    /// budget limits how far an otherwise idle host grows the number of
    /// contracts it hosts, so Freenet does not dominate the process list on a
    /// machine with plenty of free memory. It is a separate axis from
    /// `--max-hosting-storage`, which bounds state bytes. Default: 0.125.
    // Internal (#5333): applies to the resident-overhead (count-derived)
    // eviction budget, and never shrinks it below the host's already-declared
    // static caches. The 1/8 default matches qBittorrent's disk-cache "auto"
    // default and this codebase's own pre-existing `/8` convention.
    #[arg(long, env = "HOSTING_MEM_SHARE")]
    pub hosting_mem_share: Option<f64>,

    /// Per-user secret-storage quota in bytes, for hosted mode. Limits the total
    /// on-disk size of one hosted user's secrets across all delegates, so a
    /// visitor cannot fill the node's disk. Writes past the quota are rejected;
    /// nothing is evicted, since secrets are identity and room keys rather than
    /// a cache. Default: 4 MiB. Use `0` to disable enforcement. Outside hosted
    /// mode the quota is ignored: local single-user secrets are never
    /// quota-checked.
    // Internal (#4561, P5 of #4381): charges both the secret-value blobs and the
    // `.keys` enumeration registry under `users/<user_id>/`, so many or large
    // keys count too. Per-user snapshots are disabled (hosted users are
    // transient), so there is no `.snapshots/` growth to charge. Local
    // single-user secrets keep their snapshots.
    #[arg(long = "per-user-secret-quota", env = "PER_USER_SECRET_QUOTA")]
    pub per_user_secret_quota_bytes: Option<u64>,

    /// Seconds of inactivity after which a hosted user's data is reclaimed by a
    /// background sweep. This keeps a public "try Freenet" node's storage
    /// bounded: a visitor who walks away has their namespace reclaimed. The
    /// clock is real calendar time and survives restarts. Default: 2_592_000
    /// (30 days). Use `0` to disable the sweep. Ignored outside hosted mode.
    // Internal (#4561, P5 of #4381): Local single-user data lives outside the
    // `users/<id>/` tree the sweep walks, so it is never enumerated.
    #[arg(long = "per-user-inactive-ttl", env = "PER_USER_INACTIVE_TTL")]
    pub per_user_inactive_ttl_secs: Option<u64>,

    /// How often, in seconds, the inactive-user reclaim sweep runs. Only used
    /// when hosted mode is on and `--per-user-inactive-ttl` is non-zero.
    /// Default: 3_600 (hourly), which is fine-grained next to the 30-day
    /// default TTL while keeping the sweep's disk walk cheap. A value of `0` is
    /// treated as the default.
    #[arg(
        long = "inactive-user-sweep-interval",
        env = "INACTIVE_USER_SWEEP_INTERVAL"
    )]
    pub inactive_user_sweep_interval_secs: Option<u64>,

    /// Byte budget for the compiled-WASM contract module cache. The delegate
    /// cache gets a quarter of this on top, so the combined ceiling is about
    /// 1.25 times the value you set. When a cache would exceed its budget on
    /// insert, least-recently-used modules are dropped until it fits. When
    /// unset, the default scales with system RAM: total RAM / 8, clamped to
    /// between 64 MiB and 4 GiB.
    // Internal (#4441): the delegate fraction is
    // `DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`, currently 1/4. Bounding by bytes
    // rather than entry count is what stops a node hosting many contracts from
    // thrashing the cache and recompiling on every access.
    #[arg(long, env = "FREENET_MODULE_CACHE_BUDGET_BYTES")]
    pub module_cache_budget_bytes: Option<usize>,

    /// Write the local append-only diagnostic event log (`_EVENT_LOG`).
    ///
    /// On by default in `local` mode, off in `network` mode. Local mode is a
    /// single-node development mode where the log is the whole point; in
    /// network mode it costs real disk for something nothing currently reads.
    ///
    /// The log stays on this machine. It is separate from the telemetry that
    /// feeds telemetry.freenet.org, which this flag does not affect, and
    /// `freenet service report` does not include it. On a live peer, writing it
    /// cost around 61 MiB per hour and accounted for 95% of the process's
    /// fsyncs, so turn it on for nodes you operate and expect to post-mortem.
    // Internal (#4968): `fdev verify-state` consumes `_EVENT_LOG_LOCAL`. The
    // telemetry sink is a separate in-memory `TelemetryReporter` fed off the
    // same event stream. The measurement above was on a live 0.2.111 peer.
    #[arg(
        long = "enable-event-log",
        env = "FREENET_ENABLE_EVENT_LOG",
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub enable_event_log: Option<bool>,

    /// Seconds to wait on shutdown for in-flight client operations
    /// (PUT, GET, UPDATE, SUBSCRIBE) to finish before peer connections are torn
    /// down. Set to 0 to disable. Default: 30.
    // See `Config::shutdown_drain_secs` for the full rationale.
    #[arg(long, env = "SHUTDOWN_DRAIN_SECS")]
    pub shutdown_drain_secs: Option<u64>,

    /// Turn off the node's automatic update check. Off by default, and a normal
    /// release node must not set it: with it set, the node stops picking up the
    /// security and protocol updates Freenet ships frequently.
    ///
    /// This is for deployments built from source that deliberately run ahead of
    /// the latest release, such as try.freenet.org. Without it, such a build
    /// spots the newer published release, exits with code 42 to request an
    /// update, and is then either replaced by the stock release or left
    /// restart-looping. Builds from a dirty working tree already skip the check.
    // Internal (#4690): dirty builds are exempt via `build_info::GIT_DIRTY`;
    // this flag covers the clean-but-unofficial case `GIT_DIRTY` misses.
    //
    // Deliberately a plain boolean flag with no `env` binding: a truthy env
    // value is easy to leave set by accident, and silently disabling
    // auto-update fleet-wide is the exact failure this must avoid. The one
    // bespoke deployment sets it explicitly in its service `ExecStart`.
    #[arg(long = "disable-auto-update")]
    pub disable_auto_update: bool,

    #[command(flatten)]
    pub telemetry: TelemetryArgs,

    #[command(flatten)]
    pub otel: OtelArgs,
}

impl Default for ConfigArgs {
    fn default() -> Self {
        Self {
            mode: Some(OperationMode::Network),
            network_api: NetworkArgs {
                address: Some(default_listening_address()),
                network_port: Some(default_network_api_port()),
                public_address: None,
                public_port: None,
                is_gateway: false,
                skip_load_from_network: true,
                ignore_protocol_checking: false,
                gateways: None,
                gateway: None,
                location: None,
                bandwidth_limit: Some(3_000_000), // 3 MB/s default for streaming transfers only
                total_bandwidth_limit: None,
                min_bandwidth_per_connection: None,
                blocked_addresses: None,
                event_loop_channel_capacity: None,
                transient_budget: Some(DEFAULT_TRANSIENT_BUDGET),
                transient_ttl_secs: Some(DEFAULT_TRANSIENT_TTL_SECS),
                min_connections: None,
                max_connections: None,
                streaming_threshold: None, // Default: 64KB (set in NetworkApiConfig)
                ledbat_min_ssthresh: None, // Uses default from NetworkApiConfig
                congestion_control: None,  // Default: fixedrate (set in NetworkApiConfig)
                bbr_startup_rate: None,    // Uses default from BBR config
            },
            ws_api: WebsocketApiArgs {
                // `None`, NOT an explicit address: this is the "operator said
                // nothing" state, so it must fall through to
                // `resolve_ws_api_address` and land on loopback. Pinning
                // `Some(default_listening_address())` here took the Explicit
                // branch and silently reinstated the wildcard bind for every
                // programmatic composition (GHSA-824h-7x5x-wfmf). Contrast
                // `network_api.address` above, which SHOULD stay `::`: that is
                // the overlay transport, which does have to accept peers.
                address: None,
                ws_api_port: Some(default_ws_api_port()),
                token_ttl_seconds: None,
                token_cleanup_interval_seconds: None,
                allowed_host: None,
                allowed_source_cidrs: None,
                hosted_mode: None,
                per_user_op_rate_limit: None,
                per_user_op_burst: None,
                per_user_export_min_interval_secs: None,
            },
            secrets: Default::default(),
            log_level: Some(tracing::log::LevelFilter::Info),
            config_paths: Default::default(),
            id: None,
            version: false,
            max_blocking_threads: None,
            max_hosting_storage: None,
            hosting_disk_pct: None,
            max_hosting_disk: None,
            hosting_mem_share: None,
            per_user_secret_quota_bytes: None,
            per_user_inactive_ttl_secs: None,
            inactive_user_sweep_interval_secs: None,
            module_cache_budget_bytes: None,
            enable_event_log: None,
            shutdown_drain_secs: None,
            disable_auto_update: false,
            telemetry: Default::default(),
            otel: Default::default(),
        }
    }
}

/// Every `config.toml` key a release has emitted with an underscore, listed
/// with the kebab-case spelling(s) also accepted for it (#5124).
///
/// The FIRST entry of each group is the spelling the node writes; the rest are
/// aliases accepted on read. Declaration order is load-bearing — it is the
/// precedence order [`redundant_key_spellings`] applies, and it is what makes
/// the resolution preserve the value the previous release was using.
///
/// When #5130 flips what is emitted, the first entry must move with it, or that
/// property silently inverts and an upgrade starts changing effective configs.
///
/// Kept beside [`ConfigArgs::read_config`], which needs it, and re-used by the
/// tests so the `#[serde(alias = ...)]` attributes and this list cannot drift
/// apart. `config::tests::key_spelling_groups_match_the_serde_aliases` pins
/// that.
const CONFIG_KEY_SPELLINGS: &[&[&str]] = &[
    &["public_network_address", "public-network-address"],
    &["public_port", "public-network-port", "public-port"],
    &["bandwidth_limit", "bandwidth-limit"],
    &["total_bandwidth_limit", "total-bandwidth-limit"],
    &[
        "min_bandwidth_per_connection",
        "min-bandwidth-per-connection",
    ],
    &["blocked_addresses", "blocked-addresses"],
    &["event_loop_channel_capacity", "event-loop-channel-capacity"],
    &["skip_load_from_network", "skip-load-from-network"],
    &["transport_keypair", "transport-keypair"],
    &["log_level", "log-level"],
    &["contracts_dir", "contracts-dir"],
    &["delegates_dir", "delegates-dir"],
    &["secrets_dir", "secrets-dir"],
    &["db_dir", "db-dir"],
    &["event_log", "event-log"],
    &["data_dir", "data-dir"],
    &["config_dir", "config-dir"],
    &["log_dir", "log-dir"],
    &["wasmtime_cache_dir", "wasmtime-cache-dir"],
    &["is_gateway", "is-gateway"],
    &["max_blocking_threads", "max-blocking-threads"],
];

/// The same, for the `[[gateways]]` entries of `gateways.toml`.
///
/// Separate from [`CONFIG_KEY_SPELLINGS`] because these keys are NESTED — they
/// live inside an array of tables, not at the document root — so the two are
/// applied by different walkers and must not be mixed.
const GATEWAY_KEY_SPELLINGS: &[&[&str]] = &[&["public_key", "public-key"]];

/// The same again, for the nested `[gateways.address]` table.
///
/// `host_address` is the legacy single-string form, and it is what the node
/// EMITS for `Address::HostAddress` — so an operator hyphenating the key their
/// own `gateways.toml` contains hits the identical hard failure `public_key`
/// did. Separate table because it is one level deeper still.
const GATEWAY_ADDRESS_KEY_SPELLINGS: &[&[&str]] = &[&["host_address", "host-address"]];

/// The redundant spellings to drop when a config file gives one setting under
/// more than one of its accepted names. Empty for the overwhelmingly common
/// case of a file that spells each key once.
///
/// Accepting two spellings for a key (#5124) makes a file carrying BOTH
/// ambiguous, and serde resolves that by refusing the file outright with
/// `duplicate field ...`. A config parse failure is fatal, so without this the
/// node would not start — on a file that worked on every earlier release, where
/// the not-yet-recognized spelling was simply ignored as an unknown key.
///
/// That is not a hypothetical file. The operator most likely to have one is
/// precisely the one who hit #5124: tried the hyphenated key, saw nothing
/// happen, added the underscored key, and left the dead line behind. It is
/// easier still to produce after the fix — add the hyphenated key next to the
/// underscored one the node itself wrote.
///
/// The winner is the FIRST spelling present in [`CONFIG_KEY_SPELLINGS`] order,
/// i.e. the one the node emits when it is there. So an upgrade never silently
/// changes a node's effective configuration: the value that wins is the value
/// the previous release was already using. The loser is dropped with a warning
/// naming both, and the next write-back removes it from the file for good.
fn redundant_key_spellings(
    groups: &[&[&'static str]],
    source: &str,
    contains: impl Fn(&str) -> bool,
    // Separate from `contains` so it runs only on the rare ambiguous path.
    // Rendering a value goes through `toml::Value`'s `Display`, which carries
    // an internal `unwrap`, and there is no reason to put that on every boot
    // over operator-controlled data to build a message almost never shown.
    value_of: impl Fn(&str) -> String,
) -> Vec<(&'static str, String)> {
    let mut redundant = Vec::new();
    for group in groups {
        let mut present = group.iter().copied().filter(|key| contains(key));
        let Some(keep) = present.next() else {
            continue;
        };
        for ignored in present {
            // Escaped and capped. A value reaches this message from the
            // REMOTE gateway index too, where it is not operator-controlled:
            // `toml::Value`'s Display renders an embedded newline as a raw
            // newline, so an unescaped value lets a compromised or MITM'd index
            // write attacker-chosen lines — including a forged `warning:`
            // prefix — into the node's stderr and journal.
            let render = |key| {
                let value = value_of(key);
                // `value_of` already yields the TOML rendering — a string comes
                // back quoted, a number bare. Escaping unconditionally would
                // double-quote every path and turn every integer into a quoted
                // string, making the message harder to read than before. Only a
                // value that could forge a log line needs it.
                // `\u{2028}`/`\u{2029}` are belt-and-braces: journald and stderr
                // split on `\n` only, so they cannot forge a line there, but a
                // downstream consumer that treats them as breaks is cheap to
                // rule out. TOML's own Display escapes every other control
                // character already, and renders only `\n` raw.
                let value = if value.contains(['\n', '\r', '\u{2028}', '\u{2029}']) {
                    format!("{value:?}")
                } else {
                    value
                };
                match value.char_indices().nth(200) {
                    Some((cut, _)) => format!("{}… (truncated)", &value[..cut]),
                    None => value,
                }
            };
            let (kept_value, ignored_value) = (render(keep), render(ignored));
            // Name both VALUES, not just both keys. The precedence rule spends
            // the operator's newly-typed setting to buy upgrade safety, and
            // this message is the whole of what they get back for it: without
            // the values it does not say which line to delete to get the
            // outcome they were reaching for. Deliberately does NOT promise the
            // ignored key is removed automatically — that only happens for
            // `config.toml`, and telling an operator it is handled reads as
            // "nothing to do here", which is the confusion this all started as.
            let message = format!(
                "`{ignored} = {ignored_value}` in {source} is ignored: \
                 `{keep} = {kept_value}` is also set and wins. They are two \
                 spellings of one setting (#5124), and the node is using \
                 {kept_value}. To use {ignored_value} instead, delete the \
                 `{keep}` line."
            );
            // Both, deliberately. `read_config` runs inside `ConfigArgs::build`,
            // which the node calls one line BEFORE `set_logger`
            // (`bin/freenet.rs`), so no subscriber is installed yet and the
            // tracing event alone would be swallowed — leaving the operator with
            // a silently-ignored setting, which is the failure this whole change
            // is about. The tracing event is kept for library consumers that do
            // have a subscriber by then. Same reasoning as the `eprintln!`s in
            // `node/p2p_impl.rs`.
            tracing::warn!("{message}");
            eprintln!("warning: {message}");
            // Returned as well as emitted, so a test can pin the wording — the
            // message is the whole compensation for the precedence rule
            // ignoring the operator's newer line.
            redundant.push((ignored, message));
        }
    }
    redundant
}

/// Parse `gateways.toml`, resolving duplicate key spellings the same way
/// [`ConfigArgs::read_config`] does for `config.toml`.
///
/// `public_key` accepts `public-key` (#5124), which makes a file carrying both
/// ambiguous — and `gateways.toml` is hand-edited (pinned peers, isolated
/// networks, test harnesses pre-populating a `--config-dir`), so such a file is
/// exactly as reachable as the `config.toml` case.
///
/// Getting this wrong here is worse than in `config.toml`, because the failure
/// is INTERMITTENT: the local-cache read on the remote-index-success path
/// swallows parse errors, while the fallback path propagates them. A node would
/// run happily for months and then refuse to start the first time freenet.org
/// was unreachable — the exact outage in which the cache is supposed to save
/// it, and long after the edit that caused it.
fn parse_gateways_toml(content: &str, source: &str) -> Result<Gateways, toml::de::Error> {
    let mut table = toml::from_str::<toml::Table>(content)?;
    let mut normalized = false;
    if let Some(toml::Value::Array(entries)) = table.get_mut("gateways") {
        for entry in entries {
            let Some(map) = entry.as_table_mut() else {
                continue;
            };
            let redundant = redundant_key_spellings(
                GATEWAY_KEY_SPELLINGS,
                source,
                |key| map.contains_key(key),
                |key| map.get(key).map(|v| v.to_string()).unwrap_or_default(),
            );
            for (key, _) in redundant {
                map.remove(key);
                normalized = true;
            }
            // And one level deeper: `[gateways.address]` has its own key with
            // the same history.
            if let Some(address) = map.get_mut("address").and_then(|a| a.as_table_mut()) {
                let redundant = redundant_key_spellings(
                    GATEWAY_ADDRESS_KEY_SPELLINGS,
                    source,
                    |key| address.contains_key(key),
                    |key| address.get(key).map(|v| v.to_string()).unwrap_or_default(),
                );
                for (key, _) in redundant {
                    address.remove(key);
                    normalized = true;
                }
            }
        }
    }
    if normalized {
        toml::Value::Table(table).try_into::<Gateways>()
    } else {
        // Unambiguous file: keep the parse whose errors carry line/column.
        toml::from_str::<Gateways>(content)
    }
}

impl ConfigArgs {
    pub fn current_version(&self) -> &str {
        PCK_VERSION
    }

    fn read_config(dir: &PathBuf) -> std::io::Result<Option<Config>> {
        if !dir.exists() {
            return Ok(None);
        }
        let mut read_dir = std::fs::read_dir(dir)?;
        let config_args: Option<(String, String)> = read_dir.find_map(|e| {
            if let Ok(e) = e {
                if e.path().is_dir() {
                    return None;
                }
                let filename = e.file_name().to_string_lossy().into_owned();
                let ext = filename.rsplit('.').next().map(|s| s.to_owned());
                if let Some(ext) = ext {
                    if filename.starts_with("config") {
                        match ext.as_str() {
                            "toml" => {
                                tracing::debug!(filename = %filename, "Found configuration file");
                                return Some((filename, ext));
                            }
                            "json" => {
                                return Some((filename, ext));
                            }
                            _ => {}
                        }
                    }
                }
            }

            None
        });

        match config_args {
            Some((filename, ext)) => {
                let path = dir.join(filename).with_extension(&ext);
                tracing::debug!(path = ?path, "Reading configuration file");
                match ext.as_str() {
                    "toml" => {
                        let mut file = File::open(&path)?;
                        let mut content = String::new();
                        file.read_to_string(&mut content)?;
                        let invalid_data = |e: toml::de::Error| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                        };
                        // Only take the value-level path when the file actually
                        // carries a key twice; the plain string parse is kept
                        // for every other file because its errors carry
                        // line/column spans that a Value-level parse loses.
                        let mut table =
                            toml::from_str::<toml::Table>(&content).map_err(invalid_data)?;
                        let redundant = redundant_key_spellings(
                            CONFIG_KEY_SPELLINGS,
                            &path.display().to_string(),
                            |key| table.contains_key(key),
                            |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
                        );
                        let mut config = if redundant.is_empty() {
                            toml::from_str::<Config>(&content).map_err(invalid_data)?
                        } else {
                            for (key, _) in redundant {
                                table.remove(key);
                            }
                            toml::Value::Table(table)
                                .try_into::<Config>()
                                .map_err(invalid_data)?
                        };
                        let secrets = Self::read_secrets(
                            config.secrets.transport_keypair_path.clone(),
                            config.secrets.nonce_path.clone(),
                            config.secrets.cipher_path.clone(),
                        )?;
                        config.secrets = secrets;
                        Ok(Some(config))
                    }
                    "json" => {
                        let mut file = File::open(&path)?;
                        let mut content = String::new();
                        file.read_to_string(&mut content)?;
                        // Same two-path shape as the TOML branch above, and for
                        // the same reason: the direct parse reports line/column,
                        // the value-level one does not.
                        let mut object = serde_json::from_str::<serde_json::Value>(&content)?;
                        let mut rewritten = false;
                        if let Some(map) = object.as_object_mut() {
                            // A JSON `null` beside another spelling of the same
                            // key has to be REMOVED, not merely ignored: it is
                            // still the field appearing twice as far as serde is
                            // concerned, so leaving it in place fails the parse
                            // even after the other spelling wins. (TOML has no
                            // null, so only this path needs any of it.)
                            //
                            // Scoped to groups that are ACTUALLY ambiguous. A
                            // lone null keeps exactly the meaning it always had
                            // — including staying an error on a key whose type
                            // rejects it — and a document with no duplicate
                            // spelling keeps the direct parse, so it keeps its
                            // line/column spans.
                            let nulls: Vec<&str> = CONFIG_KEY_SPELLINGS
                                .iter()
                                .filter(|group| {
                                    group.iter().filter(|key| map.contains_key(**key)).count() > 1
                                })
                                .flat_map(|group| group.iter().copied())
                                .filter(|key| map.get(*key).is_some_and(|value| value.is_null()))
                                .collect();
                            for key in nulls {
                                map.remove(key);
                                rewritten = true;
                            }
                            let redundant = redundant_key_spellings(
                                CONFIG_KEY_SPELLINGS,
                                &path.display().to_string(),
                                |key| map.contains_key(key),
                                |key| map.get(key).map(|v| v.to_string()).unwrap_or_default(),
                            );
                            for (key, _) in redundant {
                                map.remove(key);
                                rewritten = true;
                            }
                        }
                        let mut config = if rewritten {
                            serde_json::from_value::<Config>(object)?
                        } else {
                            serde_json::from_str::<Config>(&content)?
                        };
                        let secrets = Self::read_secrets(
                            config.secrets.transport_keypair_path.clone(),
                            config.secrets.nonce_path.clone(),
                            config.secrets.cipher_path.clone(),
                        )?;
                        config.secrets = secrets;
                        Ok(Some(config))
                    }
                    ext => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Invalid configuration file extension: {ext}"),
                    )),
                }
            }
            None => Ok(None),
        }
    }

    /// Parse the command line arguments and return the configuration.
    pub async fn build(self) -> anyhow::Result<Config> {
        self.build_with_gateways_index(FREENET_GATEWAYS_INDEX).await
    }

    /// Build the configuration, fetching the remote gateway index from
    /// `gateways_index` when `--skip-load-from-network` is not set.
    ///
    /// The public [`build`](Self::build) wrapper passes the production
    /// [`FREENET_GATEWAYS_INDEX`] constant. Tests inject a local mock-server
    /// URL so the remote-fetch path is exercised deterministically without
    /// reaching out to `freenet.org` (which would be slow and flaky in CI).
    async fn build_with_gateways_index(mut self, gateways_index: &str) -> anyhow::Result<Config> {
        let cfg = if let Some(path) = self.config_paths.config_dir.as_ref() {
            if !path.exists() {
                return Err(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Configuration directory not found",
                )));
            }

            Self::read_config(path)?
        } else {
            // find default application dir to see if there is a config file
            let (config, data, is_temp_dir) = {
                match ConfigPathsArgs::default_dirs(self.id.as_deref())? {
                    Either::Left(defaults) => (
                        defaults.config_local_dir().to_path_buf(),
                        defaults.data_local_dir().to_path_buf(),
                        false,
                    ),
                    Either::Right(dir) => (dir.clone(), dir, true),
                }
            };
            self.config_paths.config_dir = Some(config.clone());
            if self.config_paths.data_dir.is_none() {
                self.config_paths.data_dir = Some(data);
            }
            // Skip reading config from temp directories (test scenarios) - they won't have config files
            // and may have permission issues from previous runs
            if is_temp_dir {
                None
            } else {
                Self::read_config(&config)?.inspect(|_| {
                    tracing::debug!("Found configuration file in default directory");
                })
            }
        };

        // Set when the config.toml merge below discards an auto-derivable
        // `ws-api-address`; reported after resolution, and only if the bind
        // actually narrowed. Also carries the address FAMILY forward, which the
        // re-derivation must not change. See the merge site.
        let mut dropped_persisted_wildcard: Option<IpAddr> = None;

        // Captured BEFORE the merge below, because the merge folds a PERSISTED
        // `allowed-source-cidrs` back into `self` and `build()` then persists
        // whatever it resolves. Reading the merged value would make the
        // auto-widen sticky: one boot with the flag would pin the node to the
        // wildcard bind forever, and REMOVING the flag would never narrow it
        // again — the hardening would permanently miss every node that ever set
        // an allow-list. The address itself already gets exactly this
        // treatment; the grant that widens it has to match.
        let cidrs_granted_this_boot = self.ws_api.allowed_source_cidrs.clone();

        // merge the configuration from the file with the command line arguments
        if let Some(cfg) = cfg {
            self.secrets.merge(cfg.secrets);
            self.mode.get_or_insert(cfg.mode);
            // GHSA-824h-7x5x-wfmf upgrade migration. `build()` persists the
            // RESOLVED config, so every node that has ever booted in network
            // mode already has the old auto-default written into its
            // config.toml as a literal `ws-api-address`. A plain get_or_insert
            // would merge that back and pin the whole existing fleet to the
            // wildcard bind forever — the hardening would reach fresh installs
            // only, which is no hardening at all.
            //
            // The sentinel is "a value this code could have written itself"
            // (`is_auto_derivable_ws_api_address`), which is why it includes
            // the loopback default and not just the wildcards: persisting the
            // post-migration `::1` and then reading it back as an operator
            // choice would permanently disable the auto-widen remedy the
            // release note points people at. Any OTHER persisted value
            // (`127.0.0.1`, a specific interface IP) this code never writes on
            // its own, so it is an explicit choice and is preserved unchanged.
            //
            // CLI/env values are parsed into `self` BEFORE this merge, so
            // `--ws-api-address ::` still wins on every boot. Accepted,
            // release-note-worthy edge: an operator who hand-edited a wildcard
            // into config.toml with no CLI flag and no allow-list is
            // indistinguishable from the old auto-default and gets re-derived
            // to loopback. The remedy is one flag.
            if !is_auto_derivable_ws_api_address(cfg.ws_api.address) {
                self.ws_api.address.get_or_insert(cfg.ws_api.address);
            } else if self.ws_api.address.is_none() {
                // Only note it if the re-derivation actually CHANGES the bind.
                // A node with an overlay/proxy grant drops the persisted
                // wildcard here and auto-widens straight back to it, and
                // announcing "ignoring your address" on every boot of a node
                // whose bind never moved is how a log line gets tuned out.
                dropped_persisted_wildcard = Some(cfg.ws_api.address);
            }
            self.ws_api.ws_api_port.get_or_insert(cfg.ws_api.port);
            self.ws_api
                .token_ttl_seconds
                .get_or_insert(cfg.ws_api.token_ttl_seconds);
            self.ws_api
                .token_cleanup_interval_seconds
                .get_or_insert(cfg.ws_api.token_cleanup_interval_seconds);
            if !cfg.ws_api.allowed_hosts.is_empty() {
                self.ws_api
                    .allowed_host
                    .get_or_insert(cfg.ws_api.allowed_hosts);
            }
            if !cfg.ws_api.allowed_source_cidrs.is_empty() {
                self.ws_api.allowed_source_cidrs.get_or_insert(
                    cfg.ws_api
                        .allowed_source_cidrs
                        .iter()
                        .map(|net| net.to_string())
                        .collect(),
                );
            }
            self.ws_api
                .hosted_mode
                .get_or_insert(cfg.ws_api.hosted_mode);
            self.ws_api
                .per_user_op_rate_limit
                .get_or_insert(cfg.ws_api.per_user_op_rate_limit);
            self.ws_api
                .per_user_op_burst
                .get_or_insert(cfg.ws_api.per_user_op_burst);
            self.ws_api
                .per_user_export_min_interval_secs
                .get_or_insert(cfg.ws_api.per_user_export_min_interval_secs);
            self.network_api
                .address
                .get_or_insert(cfg.network_api.address);
            self.network_api
                .network_port
                .get_or_insert(cfg.network_api.port);
            if let Some(addr) = cfg.network_api.public_address {
                self.network_api.public_address.get_or_insert(addr);
            }
            if let Some(port) = cfg.network_api.public_port {
                self.network_api.public_port.get_or_insert(port);
            }
            if let Some(limit) = cfg.network_api.bandwidth_limit {
                self.network_api.bandwidth_limit.get_or_insert(limit);
            }
            if let Some(addrs) = cfg.network_api.blocked_addresses {
                self.network_api
                    .blocked_addresses
                    .get_or_insert_with(|| addrs.into_iter().collect());
            }
            self.network_api
                .transient_budget
                .get_or_insert(cfg.network_api.transient_budget);
            self.network_api
                .transient_ttl_secs
                .get_or_insert(cfg.network_api.transient_ttl_secs);
            self.network_api
                .min_connections
                .get_or_insert(cfg.network_api.min_connections);
            self.network_api
                .max_connections
                .get_or_insert(cfg.network_api.max_connections);
            if cfg.network_api.streaming_threshold != default_streaming_threshold() {
                self.network_api
                    .streaming_threshold
                    .get_or_insert(cfg.network_api.streaming_threshold);
            }
            // Merge LEDBAT min_ssthresh: CLI args override config file, config file overrides default
            if self.network_api.ledbat_min_ssthresh.is_none() {
                self.network_api.ledbat_min_ssthresh = cfg.network_api.ledbat_min_ssthresh;
            }
            // Merge congestion control: CLI args override config file
            if self.network_api.congestion_control.is_none()
                && cfg.network_api.congestion_control != default_congestion_control()
            {
                self.network_api
                    .congestion_control
                    .get_or_insert(cfg.network_api.congestion_control);
            }
            if self.network_api.bbr_startup_rate.is_none() {
                self.network_api.bbr_startup_rate = cfg.network_api.bbr_startup_rate;
            }
            if let Some(limit) = cfg.network_api.total_bandwidth_limit {
                self.network_api.total_bandwidth_limit.get_or_insert(limit);
            }
            if let Some(min_bw) = cfg.network_api.min_bandwidth_per_connection {
                self.network_api
                    .min_bandwidth_per_connection
                    .get_or_insert(min_bw);
            }
            self.network_api
                .event_loop_channel_capacity
                .get_or_insert(cfg.network_api.event_loop_channel_capacity);
            // `--is-gateway` is a plain on/off flag: when absent we can't tell
            // "not a gateway" from "flag not passed", so only let the file turn
            // it ON. A saved gateway then stays a gateway on a bare restart (the
            // telemetry flags below have the same limitation).
            if cfg.is_gateway {
                self.network_api.is_gateway = true;
            }
            // Same on/off-flag limitation: only let the file turn this ON, so a
            // node set up to run isolated stays isolated on a bare restart
            // instead of going back to fetching the public gateway list.
            if cfg.network_api.skip_load_from_network {
                self.network_api.skip_load_from_network = true;
            }
            if let Some(loc) = cfg.location {
                self.network_api.location.get_or_insert(loc);
            }
            self.log_level.get_or_insert(cfg.log_level);
            // #4565 upgrade migration: a pre-A2 release auto-persisted the OLD
            // flat 1 GiB default as `max-hosting-storage = 1073741824`. Treat
            // that exact historical sentinel as auto-derived (NOT an explicit
            // operator choice) so it RE-DERIVES from live RAM on upgrade —
            // otherwise a small box that upgraded would keep the 1 GiB budget and
            // stay on the #4565 OOM path. `skip_serializing_if` alone only stops
            // NEW configs from pinning; it can't unpin the historical value.
            // CLI/env explicit values are parsed into `self` BEFORE this file
            // merge, so `--max-hosting-storage 1073741824` / the env var still
            // wins; only a FILE value equal to the sentinel is re-derived. On a
            // >=8 GiB box the re-derivation yields 1 GiB anyway; on a smaller box
            // reducing it is the whole point.
            if cfg.max_hosting_storage != crate::ring::LEGACY_FLAT_HOSTING_BUDGET_BYTES {
                self.max_hosting_storage
                    .get_or_insert(cfg.max_hosting_storage);
            }
            // #4683 disk-budget sizing knobs — persisted, so merge them back from
            // the file (the #3890/#4275 silent-revert class). No legacy sentinel:
            // these are new fields, so a plain get_or_insert is correct.
            self.hosting_disk_pct.get_or_insert(cfg.hosting_disk_pct);
            self.max_hosting_disk.get_or_insert(cfg.max_hosting_disk);
            self.hosting_mem_share.get_or_insert(cfg.hosting_mem_share);
            // #4968. `cfg.enable_event_log` is itself an Option, so an older
            // config.toml with no such key merges as `None` and leaves the
            // mode-dependent default intact rather than pinning `false`.
            if let Some(persisted) = cfg.enable_event_log {
                self.enable_event_log.get_or_insert(persisted);
            }
            self.per_user_secret_quota_bytes
                .get_or_insert(cfg.per_user_secret_quota_bytes);
            self.per_user_inactive_ttl_secs
                .get_or_insert(cfg.per_user_inactive_ttl_secs);
            self.inactive_user_sweep_interval_secs
                .get_or_insert(cfg.inactive_user_sweep_interval_secs);
            // #4864 upgrade migration: an existing node whose config.toml was
            // auto-written on a >12 GiB box carries the OLD auto-derived clamp
            // `module-cache-budget-bytes = 1610612736` (1.5 GiB, the previous
            // MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES, which only ever appeared as
            // an auto-derived value on boxes with >12 GiB RAM). A plain
            // get_or_insert would merge that stale 1.5 GiB back and pin the node
            // to it forever, so the exact large gateways this change targets
            // would NEVER see the new 4 GiB default. That exact value is the only
            // distinguishable "this was auto-derived, not operator-chosen" signal
            // we have, so treat it as auto: skip the merge, leave self None, and
            // let build() RE-DERIVE it via default_module_cache_budget_bytes()
            // below (yielding 4 GiB on a large box). CLI/env explicit values are
            // parsed into `self` BEFORE this file merge, so they still win. Any
            // OTHER persisted value is an explicit operator choice and is
            // preserved unchanged. Accepted, release-notes-worthy edge: an
            // operator who EXPLICITLY set exactly 1.5 GiB is indistinguishable
            // from the old auto-default and will be re-derived.
            const OLD_AUTO_MODULE_CACHE_BUDGET_SENTINEL: usize = 1_610_612_736;
            if cfg.module_cache_budget_bytes != OLD_AUTO_MODULE_CACHE_BUDGET_SENTINEL {
                self.module_cache_budget_bytes
                    .get_or_insert(cfg.module_cache_budget_bytes);
            }
            self.shutdown_drain_secs
                .get_or_insert(cfg.shutdown_drain_secs);
            self.max_blocking_threads
                .get_or_insert(cfg.max_blocking_threads);
            self.config_paths.merge(cfg.config_paths.as_ref().clone());
            // Merge telemetry config - CLI args override file config
            // Note: enabled defaults to true via clap, so we only override
            // if the config file explicitly sets it to false
            if !cfg.telemetry.enabled {
                self.telemetry.enabled = false;
            }
            if self.telemetry.endpoint.is_none() {
                self.telemetry
                    .endpoint
                    .get_or_insert(cfg.telemetry.endpoint);
            }
            self.telemetry
                .transport_snapshot_interval_secs
                .get_or_insert(cfg.telemetry.transport_snapshot_interval_secs);
            // reference-ping-enabled defaults to false via clap; override
            // if the config file sets it to true. The inverse direction
            // doesn't need handling — the clap default is already false.
            if cfg.telemetry.reference_ping_enabled {
                self.telemetry.reference_ping_enabled = true;
            }
            // iface-tx-enabled: same one-directional override as
            // reference-ping (clap default is already false).
            if cfg.telemetry.iface_tx_enabled {
                self.telemetry.iface_tx_enabled = true;
            }
            // otel-telemetry-enabled defaults to false via clap, so only the
            // file-says-true direction needs handling — same one-directional
            // override as reference-ping/iface-tx above. Kept separate from the
            // telemetry merge on purpose: the two features are independent.
            if cfg.otel.enabled {
                self.otel.enabled = true;
            }
            if let Some(endpoint) = cfg.otel.endpoint {
                self.otel.endpoint.get_or_insert(endpoint);
            }
        }

        // Validate the effective config (CLI + values merged from config.toml).
        // After the merge so a gateway role restored from the file is still
        // checked for its public address/port, not silently armed (#4275).
        self.network_api.validate()?;

        let mode = self.mode.unwrap_or(OperationMode::Network);
        let config_paths = self.config_paths.build(self.id.as_deref())?;

        let secrets = self.secrets.build(Some(&config_paths.secrets_dir(mode)))?;

        let peer_id = self
            .network_api
            .public_address
            .zip(self.network_api.public_port)
            .map(|(addr, port)| {
                PeerId::new(
                    secrets.transport_keypair.public().clone(),
                    (addr, port).into(),
                )
            });
        let gateways_file = config_paths.config_dir.join("gateways.toml");

        // In Local mode, skip all gateway loading since we don't connect to external peers
        let remotely_loaded_gateways = if mode == OperationMode::Local {
            Gateways::default()
        } else if !self.network_api.skip_load_from_network {
            load_gateways_from_index(gateways_index, &config_paths.secrets_dir)
                .await
                .inspect_err(|error| {
                    tracing::error!(
                        error = %error,
                        index = gateways_index,
                        "Failed to load gateways from index"
                    );
                })
                .unwrap_or_default()
        } else if let Some(gateways) = self.network_api.gateways {
            let gateways = gateways
                .into_iter()
                .map(|cfg| {
                    let cfg = serde_json::from_str::<InlineGwConfig>(&cfg)?;
                    Ok::<_, anyhow::Error>(GatewayConfig {
                        address: Address::HostAddress(cfg.address),
                        public_key_path: cfg.public_key_path,
                        location: cfg.location,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Gateways { gateways }
        } else {
            Gateways::default()
        };

        // Pre-compute whether --gateway entries are available. This is checked in
        // the file-load error path below to avoid failing with "no gateways" when
        // CLI entries will be merged after the main gateway resolution block.
        let has_cli_gateways = self
            .network_api
            .gateway
            .as_ref()
            .is_some_and(|v| !v.is_empty());

        // Decide which gateways to use based on whether we fetched from network
        let gateways = if mode == OperationMode::Local {
            // In Local mode, start with empty gateways — no external connections.
            // Note: --gateway entries are intentionally merged after this block
            // (unlike the hidden --gateways flag which is discarded here) so that
            // test harnesses can inject specific gateway addresses in Local mode.
            Gateways { gateways: vec![] }
        } else if !self.network_api.skip_load_from_network
            && !remotely_loaded_gateways.gateways.is_empty()
        {
            // When we successfully fetch gateways from the network, replace local ones entirely
            // This ensures users always use the current active gateways
            // TODO: This behavior will likely change once we release a stable version

            // #4275: warn about locally-cached gateways the remote index no
            // longer lists (e.g. a peer pinned via --gateway) before discarding
            // them. The remote index still wins; --skip-load-from-network keeps
            // a custom peer set.
            if let Ok(content) = fs::read_to_string(&gateways_file) {
                if let Ok(local_cache) = parse_gateways_toml(&content, "gateways.toml") {
                    let dropped = gateways_dropped_by_remote_replace(
                        &local_cache.gateways,
                        &remotely_loaded_gateways.gateways,
                    );
                    if !dropped.is_empty() {
                        tracing::warn!(
                            dropped = ?dropped,
                            file = ?gateways_file,
                            "Remote gateway index does not list {} locally-cached \
                             gateway(s); they will be discarded. If you pinned them \
                             manually, run with --skip-load-from-network to keep a \
                             custom peer set.",
                            dropped.len()
                        );
                    }
                }
            }

            tracing::info!(
                gateway_count = remotely_loaded_gateways.gateways.len(),
                "Replacing local gateways with gateways from remote index"
            );

            // Save the updated gateways to the local file for next time
            if let Err(e) = remotely_loaded_gateways.save_to_file(&gateways_file) {
                tracing::warn!(
                    error = %e,
                    file = ?gateways_file,
                    "Failed to save updated gateways to file"
                );
            }

            remotely_loaded_gateways
        } else if self.network_api.skip_load_from_network && self.network_api.is_gateway {
            // When skip_load_from_network is set for a gateway, run fully isolated.
            // Don't connect to any other gateways - this enables isolated test networks
            // where the test gateway doesn't mesh with production.
            if remotely_loaded_gateways.gateways.is_empty() {
                tracing::info!(
                    "Gateway running in isolated mode (skip_load_from_network), not connecting to other gateways"
                );
                Gateways { gateways: vec![] }
            } else {
                // Inline gateways were provided via --gateways flag, use those
                remotely_loaded_gateways
            }
        } else if self.network_api.skip_load_from_network && has_cli_gateways {
            // #3980: Strict additive --gateway semantics under
            // skip_load_from_network. When the user explicitly passes
            // --gateway, treat the CLI entries (plus any --gateways inline
            // JSON entries resolved into `remotely_loaded_gateways` above)
            // as the complete bootstrap set: do NOT merge in the on-disk
            // gateways.toml cache (which on a default install lists public
            // production peers like nova/vega). The explicit --gateway
            // entries are merged below.
            //
            // When --gateway is NOT supplied under skip_load_from_network,
            // the on-disk gateways.toml is still read (next branch). This
            // preserves the contract used by isolated test harnesses
            // (e.g. freenet-test-network's Docker NAT setup) that
            // pre-populate gateways.toml in a custom --config-dir.
            tracing::info!(
                "skip_load_from_network with --gateway entries: \
                 ignoring on-disk gateways.toml; using only CLI-supplied gateways"
            );
            // Returning `remotely_loaded_gateways` (empty or populated from
            // --gateways JSON) preserves the precedence contract documented
            // below at the --gateway merge step.
            remotely_loaded_gateways
        } else {
            // Either skip_load_from_network is set (use local file only), or the
            // remote fetch failed and we need to fall back to the local cache.
            let remote_fetch_failed = !self.network_api.skip_load_from_network
                && remotely_loaded_gateways.gateways.is_empty();

            if remote_fetch_failed {
                tracing::warn!(
                    file = ?gateways_file,
                    "Remote gateway fetch failed, falling back to local cache"
                );
            }

            let mut gateways = match File::open(&*gateways_file) {
                Ok(mut file) => {
                    let mut content = String::new();
                    file.read_to_string(&mut content)?;
                    parse_gateways_toml(&content, "gateways.toml").map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?
                }
                Err(err) => {
                    // A gateway is allowed to start with an empty bootstrap
                    // list (an isolated gateway is a valid configuration), so
                    // exempt `is_gateway` nodes from this guard. A gateway
                    // started with `--is-gateway --public-network-address X
                    // --network-port Y` (and no `--public-network-port`) has
                    // `peer_id == None`, because `peer_id` is derived from
                    // `public_address.zip(public_port)` above. Keying the guard
                    // on `peer_id.is_none()` alone would therefore wrongly
                    // reject such a gateway on first boot when the remote index
                    // is unreachable, no on-disk gateways.toml exists, and no
                    // `--gateway`/`--gateways` is supplied. See issue #4268.
                    //
                    // The original `peer_id.is_none()` condition is preserved:
                    // a non-gateway peer that DOES have a public identity
                    // (`--public-network-address` + `--public-network-port`, so
                    // `peer_id == Some`) is still allowed to initialize as a
                    // disjoint bootstrap node with no gateways (see the
                    // "initializing disjoint gateway" warning below). Only a
                    // non-gateway with no public identity and no gateways is
                    // rejected, as before.
                    if peer_id.is_none()
                        && !self.network_api.is_gateway
                        && mode == OperationMode::Network
                        && remotely_loaded_gateways.gateways.is_empty()
                        && !has_cli_gateways
                    {
                        let hint = if remote_fetch_failed {
                            "Cannot initialize node without gateways. \
                             The remote gateway index could not be reached and no \
                             local cache exists yet. Check your network connection \
                             and firewall settings, then try again."
                        } else {
                            "Cannot initialize node without gateways"
                        };
                        tracing::error!(
                            file = ?gateways_file,
                            error = %err,
                            remote_fetch_failed,
                            "{hint}"
                        );

                        return Err(anyhow::Error::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            hint,
                        )));
                    }
                    if remotely_loaded_gateways.gateways.is_empty() {
                        tracing::warn!("No gateways file found, initializing disjoint gateway");
                    }
                    Gateways { gateways: vec![] }
                }
            };

            if !remotely_loaded_gateways.gateways.is_empty() {
                gateways.merge_and_deduplicate(remotely_loaded_gateways);
            }

            gateways
        };

        // Merge any --gateway entries into the gateway list (runs in all modes,
        // including Local, so test harnesses can inject specific gateways).
        // User-specified gateways take precedence: they are inserted first,
        // so file-loaded duplicates (by address) are skipped.
        //
        // Precedence when both --gateways (hidden JSON) and --gateway are set:
        // --gateways entries are resolved above and become `gateways`; --gateway
        // entries are prepended here, so on address collision --gateway wins.
        let mut gateways = gateways;
        if let Some(cli_entries) = self.network_api.gateway {
            let secrets_dir = config_paths.secrets_dir(mode);

            // Clean up stale key files from previous runs
            if let Ok(entries) = fs::read_dir(&secrets_dir) {
                for entry in entries.flatten() {
                    if entry
                        .file_name()
                        .to_str()
                        .is_some_and(|n| n.starts_with("cli_gw_") && n.ends_with(".pub"))
                    {
                        if let Err(e) = fs::remove_file(entry.path()) {
                            tracing::debug!(
                                error = %e,
                                file = ?entry.path(),
                                "Failed to remove stale CLI gateway key file"
                            );
                        }
                    }
                }
            }

            let mut cli_gateways = Gateways { gateways: vec![] };
            let mut seen_addrs = HashSet::new();
            for entry in &cli_entries {
                match parse_gateway(entry, &secrets_dir) {
                    Ok(gw) => {
                        if !seen_addrs.insert(gw.address.clone()) {
                            tracing::warn!(
                                address = ?gw.address,
                                "Skipping duplicate --gateway address"
                            );
                            continue;
                        }
                        tracing::info!(
                            address = ?gw.address,
                            "Adding user-specified gateway via --gateway"
                        );
                        cli_gateways.gateways.push(gw);
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "Failed to parse --gateway \"{entry}\": {e}"
                        ));
                    }
                }
            }
            // CLI-specified gateways go first so they win deduplication
            cli_gateways.merge_and_deduplicate(gateways);
            gateways = cli_gateways;
        }

        // --- client (HTTP/WebSocket) API exposure ---------------------------
        // Resolved before the `Config` literal so the bind address, the host
        // allowlist and the hosted-mode flag are all in scope together: the
        // exposure warning below needs all three, and the auto-widen decision
        // needs to be logged, which a struct-literal field initializer cannot
        // do cleanly.
        let ws_api_allowed_hosts = self.ws_api.allowed_host.clone().unwrap_or_default();
        let ws_api_hosted_mode = self.ws_api.hosted_mode.unwrap_or(false);
        let (ws_api_address, ws_api_address_source) = resolve_ws_api_address(
            mode,
            self.ws_api.address,
            cidrs_granted_this_boot.as_deref(),
            dropped_persisted_wildcard,
        );
        // Recorded, NOT logged: `build()` runs before `set_logger`, so anything
        // emitted here has no subscriber. `Config::log_client_api_exposure()`
        // reports it after the subscriber exists. See `WsApiExposure`.
        let ws_api_exposure = WsApiExposure {
            source: ws_api_address_source,
            // Recorded when the re-derivation actually MOVED the bind, in
            // either direction; `log_client_api_exposure` picks the message
            // from which way it went. A value that re-derives to itself (the
            // steady state from boot 2 onward) is not reported at all, because
            // re-announcing an unchanged bind every boot is how a log line gets
            // tuned out.
            dropped_persisted_address: dropped_persisted_wildcard
                .filter(|persisted| *persisted != ws_api_address),
        };
        let this = Config {
            mode,
            peer_id,
            network_api: NetworkApiConfig {
                address: self.network_api.address.unwrap_or_else(|| match mode {
                    OperationMode::Local => default_local_address(),
                    OperationMode::Network => default_listening_address(),
                }),
                port: self
                    .network_api
                    .network_port
                    .unwrap_or_else(default_network_api_port),
                public_address: self.network_api.public_address,
                public_port: self.network_api.public_port,
                ignore_protocol_version: self.network_api.ignore_protocol_checking,
                bandwidth_limit: self.network_api.bandwidth_limit,
                total_bandwidth_limit: self.network_api.total_bandwidth_limit,
                min_bandwidth_per_connection: self.network_api.min_bandwidth_per_connection,
                blocked_addresses: self
                    .network_api
                    .blocked_addresses
                    .map(|addrs| addrs.into_iter().collect()),
                event_loop_channel_capacity: self
                    .network_api
                    .event_loop_channel_capacity
                    .unwrap_or_else(default_event_loop_channel_capacity),
                transient_budget: self
                    .network_api
                    .transient_budget
                    .unwrap_or(DEFAULT_TRANSIENT_BUDGET),
                transient_ttl_secs: self
                    .network_api
                    .transient_ttl_secs
                    .unwrap_or(DEFAULT_TRANSIENT_TTL_SECS),
                min_connections: self
                    .network_api
                    .min_connections
                    .unwrap_or(DEFAULT_MIN_CONNECTIONS),
                max_connections: self
                    .network_api
                    .max_connections
                    .unwrap_or(DEFAULT_MAX_CONNECTIONS),
                streaming_threshold: self
                    .network_api
                    .streaming_threshold
                    .unwrap_or_else(default_streaming_threshold),
                ledbat_min_ssthresh: self
                    .network_api
                    .ledbat_min_ssthresh
                    .or_else(default_ledbat_min_ssthresh),
                congestion_control: self
                    .network_api
                    .congestion_control
                    .clone()
                    .unwrap_or_else(default_congestion_control),
                bbr_startup_rate: self.network_api.bbr_startup_rate,
                skip_load_from_network: self.network_api.skip_load_from_network,
            },
            ws_api: WebsocketApiConfig {
                address: ws_api_address,
                port: self.ws_api.ws_api_port.unwrap_or(default_ws_api_port()),
                token_ttl_seconds: self
                    .ws_api
                    .token_ttl_seconds
                    .unwrap_or(default_token_ttl_seconds()),
                token_cleanup_interval_seconds: self
                    .ws_api
                    .token_cleanup_interval_seconds
                    .unwrap_or(default_token_cleanup_interval_seconds()),
                allowed_hosts: ws_api_allowed_hosts,
                allowed_source_cidrs: self
                    .ws_api
                    .allowed_source_cidrs
                    .as_ref()
                    .map(|cidrs| {
                        cidrs
                            .iter()
                            .map(|s| {
                                let net = s.parse::<ipnet::IpNet>().map_err(|e| {
                                    anyhow::anyhow!(
                                        "invalid CIDR `{s}` in allowed-source-cidrs: {e}"
                                    )
                                })?;
                                crate::server::validate_source_cidr(&net).map_err(|msg| {
                                    anyhow::anyhow!("allowed-source-cidrs: {msg}")
                                })?;
                                Ok::<_, anyhow::Error>(net)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default(),
                hosted_mode: ws_api_hosted_mode,
                per_user_op_rate_limit: self
                    .ws_api
                    .per_user_op_rate_limit
                    .unwrap_or_else(default_per_user_op_rate_limit),
                per_user_op_burst: self
                    .ws_api
                    .per_user_op_burst
                    .unwrap_or_else(default_per_user_op_burst),
                per_user_export_min_interval_secs: self
                    .ws_api
                    .per_user_export_min_interval_secs
                    .unwrap_or_else(default_per_user_export_min_interval_secs),
                // Runtime-only: resolve the secrets dir for this mode so the WS
                // serve layer can stamp per-user activity markers (#4561).
                secrets_dir: config_paths.secrets_dir(mode),
                // Runtime-only: resolve the unpacked-webapp cache so the HTTP
                // layer knows which directory its LRU sweep may delete from.
                webapp_cache_dir: default_webapp_cache_dir(),
                // Runtime-only: this boot's exposure decision, replayed by
                // `Config::log_client_api_exposure()` once logging exists.
                exposure: ws_api_exposure,
            },
            secrets,
            log_level: self.log_level.unwrap_or(tracing::log::LevelFilter::Info),
            config_paths: Arc::new(config_paths),
            gateways: gateways.gateways.clone(),
            is_gateway: self.network_api.is_gateway,
            location: self.network_api.location,
            max_blocking_threads: self
                .max_blocking_threads
                .unwrap_or_else(default_max_blocking_threads),
            // Passed through un-resolved on purpose: `None` must stay `None` so
            // `Config::event_log_enabled` can apply the mode-dependent default
            // (ON in Local, OFF in Network) and an upgrading local node whose
            // config.toml predates this key keeps its log.
            enable_event_log: self.enable_event_log,
            max_hosting_storage: self
                .max_hosting_storage
                .unwrap_or_else(crate::ring::default_hosting_budget_bytes),
            hosting_disk_pct: self
                .hosting_disk_pct
                .unwrap_or(crate::ring::DEFAULT_HOSTING_DISK_PCT),
            max_hosting_disk: self
                .max_hosting_disk
                .unwrap_or(crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES),
            hosting_mem_share: self
                .hosting_mem_share
                .unwrap_or(crate::ring::DEFAULT_RESIDENT_OVERHEAD_MEM_SHARE),
            per_user_secret_quota_bytes: self
                .per_user_secret_quota_bytes
                .unwrap_or(crate::wasm_runtime::DEFAULT_PER_USER_SECRET_QUOTA_BYTES as u64),
            per_user_inactive_ttl_secs: self
                .per_user_inactive_ttl_secs
                .unwrap_or(default_per_user_inactive_ttl_secs()),
            inactive_user_sweep_interval_secs: {
                // `0` means "use the default" (an interval of 0 is meaningless —
                // the sweep would otherwise floor it to 1s and hammer the disk).
                // Remap here so the resolved value always reflects the documented
                // semantics, rather than relying on a downstream `.max(1)`.
                let v = self
                    .inactive_user_sweep_interval_secs
                    .unwrap_or(default_inactive_user_sweep_interval_secs());
                if v == 0 {
                    default_inactive_user_sweep_interval_secs()
                } else {
                    v
                }
            },
            module_cache_budget_bytes: self
                .module_cache_budget_bytes
                .unwrap_or_else(crate::wasm_runtime::default_module_cache_budget_bytes),
            shutdown_drain_secs: self
                .shutdown_drain_secs
                .unwrap_or_else(default_shutdown_drain_secs),
            disable_auto_update: self.disable_auto_update,
            telemetry: TelemetryConfig {
                enabled: self.telemetry.enabled,
                endpoint: self
                    .telemetry
                    .endpoint
                    .unwrap_or_else(|| DEFAULT_TELEMETRY_ENDPOINT.to_string()),
                transport_snapshot_interval_secs: self
                    .telemetry
                    .transport_snapshot_interval_secs
                    .unwrap_or_else(default_transport_snapshot_interval_secs),
                // Test environments are identified by the --id flag, which is used for
                // simulated networks and integration tests. We disable telemetry in these
                // environments to avoid flooding the collector with test data.
                is_test_environment: self.id.is_some(),
                reference_ping_enabled: self.telemetry.reference_ping_enabled,
                iface_tx_enabled: self.telemetry.iface_tx_enabled,
            },
            otel: OtelConfig {
                enabled: self.otel.enabled,
                endpoint: self.otel.endpoint,
                // Same --id rule as telemetry: simulated networks and
                // integration tests must not ship data to a collector.
                is_test_environment: self.id.is_some(),
            },
        };

        fs::create_dir_all(this.config_dir())?;
        // Only persist gateways when they were fetched from the remote index.
        // When skip_load_from_network is set (local test networks), the gateways.toml
        // is managed externally and should not be overwritten.
        if !self.network_api.skip_load_from_network {
            gateways.save_to_file(&gateways_file)?;
        }

        // Persist on first run (no file yet) or when the effective config
        // changed — e.g. the operator passed a new CLI flag — so config.toml
        // stays the source of truth (#4275). Comparing against the file's
        // current contents (written by the same serializer) keeps an unchanged
        // restart a no-op, so operator hand-edits survive.
        let config_path = this.config_dir().join("config.toml");
        let new_config_toml = toml::to_string(&this)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let current = std::fs::read_to_string(&config_path).ok();
        if current.as_deref() != Some(new_config_toml.as_str()) {
            tracing::info!(path = ?config_path, "Persisting configuration");
            // Write to a per-process temp file and rename into place rather
            // than truncating config.toml directly. `File::create` +
            // `write_all` leaves a window where a concurrent reader (e.g.
            // another `freenet` process racing this one) can observe a
            // truncated or partially-written file; `rename` replaces the
            // destination atomically, so a reader always sees either the
            // old or the new complete content.
            //
            // The temp filename embeds this process's PID so two writers
            // racing `build()` concurrently (the wrapper's single-instance
            // guard only prevents two *wrapper* processes from launching a
            // node each — it doesn't stop e.g. a manually-run `freenet
            // network` from racing an already-supervised one) never share
            // the same temp file: each writes and fsyncs its own complete
            // copy before renaming, so the destination only ever receives
            // one writer's complete content, never an interleaved mix of
            // two. A shared temp filename would defeat this: both writers'
            // `File::create` + `write_all` calls could interleave on the
            // same underlying file before either renames.
            //
            // The extension ("toml.tmp", not "toml") keeps `read_config`'s
            // directory scan from ever picking a leftover up as a candidate
            // config file if a rename is interrupted before cleanup.
            let tmp_path = config_path.with_extension(format!("{}.toml.tmp", std::process::id()));
            let mut file = File::create(&tmp_path)?;
            file.write_all(new_config_toml.as_bytes())?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&tmp_path, &config_path)?;
        }

        Ok(this)
    }
}

mod serde_log_level_filter {
    use serde::{Deserialize, Deserializer, Serializer};
    use tracing::log::LevelFilter;

    pub fn parse_log_level_str<'a, D>(level: &str) -> Result<LevelFilter, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        Ok(match level.trim() {
            "off" | "Off" | "OFF" => LevelFilter::Off,
            "error" | "Error" | "ERROR" => LevelFilter::Error,
            "warn" | "Warn" | "WARN" => LevelFilter::Warn,
            "info" | "Info" | "INFO" => LevelFilter::Info,
            "debug" | "Debug" | "DEBUG" => LevelFilter::Debug,
            "trace" | "Trace" | "TRACE" => LevelFilter::Trace,
            s => return Err(serde::de::Error::custom(format!("unknown log level: {s}"))),
        })
    }

    pub fn serialize<S>(level: &LevelFilter, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let level = match level {
            LevelFilter::Off => "off",
            LevelFilter::Error => "error",
            LevelFilter::Warn => "warn",
            LevelFilter::Info => "info",
            LevelFilter::Debug => "debug",
            LevelFilter::Trace => "trace",
        };
        serializer.serialize_str(level)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<LevelFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        let level = String::deserialize(deserializer)?;
        parse_log_level_str::<D>(level.as_str())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    /// Node operation mode.
    pub mode: OperationMode,
    #[serde(flatten)]
    pub network_api: NetworkApiConfig,
    #[serde(flatten)]
    pub ws_api: WebsocketApiConfig,
    #[serde(flatten)]
    pub secrets: Secrets,
    #[serde(with = "serde_log_level_filter", alias = "log-level")]
    pub log_level: tracing::log::LevelFilter,
    #[serde(flatten)]
    config_paths: Arc<ConfigPaths>,
    #[serde(skip)]
    pub(crate) peer_id: Option<PeerId>,
    #[serde(skip)]
    pub(crate) gateways: Vec<GatewayConfig>,
    #[serde(alias = "is-gateway")]
    pub(crate) is_gateway: bool,
    pub(crate) location: Option<f64>,
    /// Maximum number of threads for blocking operations (WASM execution, etc.).
    #[serde(
        default = "default_max_blocking_threads",
        alias = "max-blocking-threads"
    )]
    pub max_blocking_threads: usize,
    /// Budget in bytes for hosted contract *state*. Once exceeded, contracts
    /// are evicted (least-valuable-first) and their on-disk state reclaimed.
    /// This bounds tracked contract state only — WASM code blobs and ReDb/
    /// SQLite database overhead are additional and not counted against it.
    ///
    /// The default is capability-relative (RAM-scaled): `clamp(total_ram / 8,
    /// 128 MiB, 1 GiB)`, so a memory-constrained host gets a proportionally
    /// smaller budget instead of the old flat 1 GiB (#4642 A2 / #4565). Set an
    /// explicit value to override the RAM-scaled default.
    ///
    /// `skip_serializing_if` drops this field from `config.toml` when it holds
    /// the auto-derived default, so the budget RE-DERIVES from live RAM on every
    /// boot instead of being pinned at first boot. Without this, a node that
    /// first-booted on a large box would bake the large budget into `config.toml`
    /// and keep it after moving to a smaller box / tighter cgroup — defeating the
    /// #4565 OOM protection. An explicit operator value differs from the derived
    /// default, so it is persisted and survives restarts. See
    /// [`is_default_hosting_budget`].
    #[serde(
        default = "default_max_hosting_storage",
        rename = "max-hosting-storage",
        skip_serializing_if = "is_default_hosting_budget"
    )]
    pub max_hosting_storage: u64,
    /// Fraction (0.0–1.0) of the data-dir mount's Freenet-reachable capacity
    /// (`used + free`) used to size the aggregate disk budget (#4683). The disk
    /// budget is the second floor on hosting eviction
    /// (`effective = min(ram_budget, disk_budget)`). Default 0.5. Persisted so an
    /// operator override survives a flag-less restart.
    #[serde(default = "default_hosting_disk_pct", rename = "hosting-disk-pct")]
    pub hosting_disk_pct: f64,
    /// Hard upper clamp in bytes for the aggregate disk budget (#4683), the disk
    /// analogue of `max-hosting-storage`. The disk budget never exceeds this even
    /// on a host with a very large data disk. Default 32 GiB. Persisted so an
    /// operator override survives a flag-less restart.
    #[serde(default = "default_max_hosting_disk", rename = "max-hosting-disk")]
    pub max_hosting_disk: u64,
    /// Fraction (0.0-1.0) of LIVE host-wide surplus memory the resident-
    /// overhead (count-derived) eviction budget may claim on top of its own
    /// RSS (#5333). Default 0.125 (1/8). Persisted so an operator override
    /// survives a flag-less restart.
    #[serde(default = "default_hosting_mem_share", rename = "hosting-mem-share")]
    pub hosting_mem_share: f64,
    /// Per-user secret-storage quota in bytes for hosted mode (#4561, P5 of
    /// #4381). Bounds a single hosted user's TOTAL on-disk footprint (active
    /// secret-value blobs + the `.keys` enumeration registry) under their
    /// `users/<user_id>/` tree, summed across delegates. Per-user value
    /// snapshots are disabled, so there is no `.snapshots/` growth to charge.
    /// REJECT-on-full (never evict). Default 4 MiB; `0` disables. No effect
    /// outside hosted mode (local single-user secrets are never quota-checked).
    #[serde(
        default = "default_per_user_secret_quota_bytes",
        rename = "per-user-secret-quota"
    )]
    pub per_user_secret_quota_bytes: u64,
    /// Inactivity TTL in seconds after which a HOSTED user's entire per-user
    /// data is reclaimed by a background sweep (#4561, P5 of #4381). Durable,
    /// real-calendar time (survives restarts). Default 2_592_000 (30 days);
    /// `0` disables the sweep. No effect outside hosted mode — Local
    /// single-user data is never enumerated or reclaimed.
    #[serde(
        default = "default_per_user_inactive_ttl_secs",
        rename = "per-user-inactive-ttl"
    )]
    pub per_user_inactive_ttl_secs: u64,
    /// How often (seconds) the inactive-user reclaim sweep runs (#4561). Only
    /// relevant in hosted mode with a non-zero TTL. Default 3_600 (hourly).
    #[serde(
        default = "default_inactive_user_sweep_interval_secs",
        rename = "inactive-user-sweep-interval"
    )]
    pub inactive_user_sweep_interval_secs: u64,
    /// Byte budget for the compiled-WASM **contract** module cache. The
    /// delegate cache gets a fraction of this
    /// (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`), so the combined ceiling is
    /// ~1.25× this value. Bounds the cache by total compiled bytes rather than
    /// entry count, so a node hosting many contracts doesn't thrash (issue
    /// #4441). When unset, the default scales with system RAM
    /// (`clamp(total_ram / 8, 64 MiB, 4 GiB)`) so a small VPS doesn't OOM and
    /// a big gateway still caches a large working set.
    #[serde(
        default = "default_module_cache_budget_bytes",
        rename = "module-cache-budget-bytes"
    )]
    pub module_cache_budget_bytes: usize,

    /// Whether to write the local append-only diagnostic event log
    /// (`_EVENT_LOG`). Resolved in [`ConfigArgs::build`], where the operation
    /// mode is known: defaults ON in `local` mode and OFF in `network` mode,
    /// with an explicit `--enable-event-log` / `FREENET_ENABLE_EVENT_LOG` /
    /// `enable-event-log` setting always winning.
    ///
    /// Deliberately `Option<bool>` rather than `bool`: a config.toml written by
    /// an older release has NO `enable-event-log` key, and a plain
    /// `#[serde(default)] bool` would deserialize that absence to `false`,
    /// indistinguishable from an operator's explicit `false`. The merge in
    /// [`ConfigArgs::build`] would then pin that `false` forever and silently
    /// strip the event log from upgrading `local`-mode nodes (breaking
    /// `fdev verify-state`) — the #3890/#4275 silent-revert class. Keeping the
    /// absence as `None` lets [`Config::event_log_enabled`] re-derive the
    /// mode-dependent default.
    ///
    /// NOT related to the telemetry that feeds telemetry.freenet.org; see the
    /// `ConfigArgs::enable_event_log` docs (#4968).
    #[serde(
        default,
        rename = "enable-event-log",
        skip_serializing_if = "Option::is_none"
    )]
    pub enable_event_log: Option<bool>,

    /// Telemetry configuration
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,

    /// OpenTelemetry SDK metrics exporter settings. Strictly isolated from
    /// `telemetry` above — see `docs/design/otel-metrics-exporter.md`.
    #[serde(flatten)]
    pub otel: OtelConfig,

    /// Maximum seconds to wait on graceful shutdown for in-flight
    /// client-originated operations (PUT/UPDATE/GET/SUBSCRIBE) to
    /// finish before tearing down peer connections.
    ///
    /// Set to `0` to disable the drain entirely (legacy behaviour:
    /// disconnect immediately on SIGTERM). Default is 30s, which
    /// covers a typical `freenet-git` mirror push (~3 MiB pack split
    /// into 4 chunks) plus headroom. systemd's `TimeoutStopSec` is
    /// set to 45s in this PR (30s drain + 15s peer-teardown
    /// headroom) — raise both in lockstep if you raise this value;
    /// `TimeoutStopSec` is the hard ceiling at which systemd
    /// SIGKILLs the process.
    ///
    /// Motivation: release-driven auto-update was killing in-flight
    /// `freenet-git` mirror PUTs on the nova gateway, producing
    /// repeated `Mirror to Freenet` failure alerts to
    /// `#freenet-dev:matrix.org`.
    #[serde(
        default = "default_shutdown_drain_secs",
        rename = "shutdown-drain-secs"
    )]
    pub shutdown_drain_secs: u64,

    /// Operator opt-out of the automatic self-update check. RUNTIME-ONLY, NOT
    /// persisted (`#[serde(skip)]`, like `secrets_dir` /
    /// `TelemetryConfig::is_test_environment`): it is set from the
    /// `--disable-auto-update` CLI flag in `build()`. Default `false`, so a
    /// release node auto-updates exactly as before. See the flag's rustdoc on
    /// `ConfigArgs::disable_auto_update` for why a from-source deployment
    /// (try.freenet.org) needs it (#4690).
    #[serde(skip)]
    pub disable_auto_update: bool,
}

/// Default graceful-shutdown drain window.
fn default_shutdown_drain_secs() -> u64 {
    30
}

/// Number of `Executor<Runtime>` workers the `RuntimePool` runs.
///
/// Reserve one logical core for the Tokio event loop and OS scheduling. WASM
/// execution is CPU-bound, so the pool naturally can't exceed useful
/// parallelism. Capped at 16 to stay well within the max_blocking_threads limit
/// (see [`default_max_blocking_threads`]), preventing the executor pool from
/// exhausting the blocking pool. `FREENET_RUNTIME_POOL_SIZE` overrides it
/// (useful for tests).
///
/// This is CPU-derived, and `MemoryMax` does not constrain CPU count — a 20-core
/// laptop inside a 2 GiB cgroup gets 16 workers. Anything sized PER WORKER must
/// therefore compose its ceiling against the memory limit rather than assume the
/// product is affordable (#5268 defect 3), which is why this lives here as one
/// shared source: `RuntimePool::new` sizes the pool from it and the per-worker
/// cache budgets divide by it.
pub(crate) fn runtime_pool_size() -> NonZeroUsize {
    let cores = std::thread::available_parallelism().map(|n| n.get()).ok();
    let override_value = std::env::var(RUNTIME_POOL_SIZE_ENV)
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    resolve_pool_size(cores, override_value)
}

/// Env var overriding [`runtime_pool_size`] (useful for tests).
const RUNTIME_POOL_SIZE_ENV: &str = "FREENET_RUNTIME_POOL_SIZE";

/// Upper bound on the executor pool, keeping it well within
/// [`default_max_blocking_threads`].
const MAX_RUNTIME_POOL_SIZE: usize = 16;

/// Pure clamp math behind [`runtime_pool_size`], split out so its boundaries are
/// unit-testable without mutating the process-global environment (which would
/// race every other test in the binary) or depending on the test host's core
/// count.
///
/// `cores` is `None` when the OS cannot report parallelism; `override_value` is
/// the parsed env var when set.
fn resolve_pool_size(cores: Option<usize>, override_value: Option<usize>) -> NonZeroUsize {
    let from_cores = cores
        .unwrap_or(4)
        .saturating_sub(1)
        .clamp(1, MAX_RUNTIME_POOL_SIZE);
    let resolved = override_value
        .map(|n| n.clamp(1, MAX_RUNTIME_POOL_SIZE))
        .unwrap_or(from_cores);
    NonZeroUsize::new(resolved).expect("clamped to at least 1")
}

/// Default max blocking threads: 2x CPU cores, clamped to 4-32.
fn default_max_blocking_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() * 2).clamp(4, 32))
        .unwrap_or(8)
}

/// Default operator-facing budget for hosted contract state (RAM-scaled).
///
/// Resolves to [`crate::ring::default_hosting_budget_bytes`], the single source
/// of truth for this value — the RAM-scaled in-code default used by the hosting
/// cache and its tests. This indirection keeps the operator-facing default and
/// the in-code default from ever drifting apart. The default is capability-
/// relative (`clamp(total_ram / 8, 128 MiB, 1 GiB)`, #4642 A2) rather than a
/// flat constant, so a memory-constrained host gets a proportionally smaller
/// budget (addresses #4565); an explicit value always overrides it.
fn default_max_hosting_storage() -> u64 {
    crate::ring::default_hosting_budget_bytes()
}

/// Default fraction of Freenet-reachable disk capacity for the aggregate disk
/// budget (#4683): resolves to [`crate::ring::DEFAULT_HOSTING_DISK_PCT`] (0.5),
/// the single source of truth shared with the sizing math.
fn default_hosting_disk_pct() -> f64 {
    crate::ring::DEFAULT_HOSTING_DISK_PCT
}

/// Default hard cap for the aggregate disk budget (#4683): resolves to
/// [`crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES`] (32 GiB).
fn default_max_hosting_disk() -> u64 {
    crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES
}

/// Default fraction of live host-wide surplus memory the resident-overhead
/// eviction budget may claim (#5333): resolves to
/// [`crate::ring::DEFAULT_RESIDENT_OVERHEAD_MEM_SHARE`] (0.125), the single
/// source of truth shared with the sizing math.
fn default_hosting_mem_share() -> f64 {
    crate::ring::DEFAULT_RESIDENT_OVERHEAD_MEM_SHARE
}

/// `skip_serializing_if` predicate for [`Config::max_hosting_storage`]: true
/// when the resolved value equals the auto-derived RAM-scaled default, so it is
/// omitted from `config.toml` and re-derived from live RAM on the next boot
/// (#4565 first-boot-pinning fix — see the field docs).
///
/// Because the derived default is always clamped to `[128 MiB, 1 GiB]`, an
/// explicit operator value OUTSIDE that range can never match and is always
/// persisted. The one ambiguous case — an explicit value that happens to equal
/// the current derived default — re-derives on a RAM change, which is the safe
/// direction (toward the smaller, capability-relative budget) anyway.
fn is_default_hosting_budget(v: &u64) -> bool {
    *v == default_max_hosting_storage()
}

/// Default per-user secret-storage quota (4 MiB). Resolves to
/// [`crate::wasm_runtime::DEFAULT_PER_USER_SECRET_QUOTA_BYTES`], the single
/// source of truth for the in-code default, so the operator-facing default and
/// the store's fallback never drift apart.
fn default_per_user_secret_quota_bytes() -> u64 {
    crate::wasm_runtime::DEFAULT_PER_USER_SECRET_QUOTA_BYTES as u64
}

/// Default inactive-user TTL (30 days). Resolves to
/// [`crate::wasm_runtime::DEFAULT_PER_USER_INACTIVE_TTL_SECS`], the single
/// source of truth, so the operator-facing default and the sweep's fallback
/// never drift.
const fn default_per_user_inactive_ttl_secs() -> u64 {
    crate::wasm_runtime::DEFAULT_PER_USER_INACTIVE_TTL_SECS
}

/// Default inactive-user sweep interval (1 hour). Far finer than the 30-day
/// TTL, so reclamation lag is negligible while the periodic disk walk stays
/// cheap.
const fn default_inactive_user_sweep_interval_secs() -> u64 {
    3_600
}

/// Default contract-module cache byte budget, scaled to system RAM
/// (`clamp(total_ram / 8, 64 MiB, 4 GiB)`).
///
/// Resolves to [`crate::wasm_runtime::default_module_cache_budget_bytes`], the
/// single source of truth, so the operator-facing default and the in-code
/// default never drift.
fn default_module_cache_budget_bytes() -> usize {
    crate::wasm_runtime::default_module_cache_budget_bytes()
}

impl Config {
    pub fn transport_keypair(&self) -> &TransportKeypair {
        self.secrets.transport_keypair()
    }

    pub fn paths(&self) -> Arc<ConfigPaths> {
        self.config_paths.clone()
    }

    /// Report how the client (HTTP/WebSocket) API's exposure was decided.
    ///
    /// **Call this AFTER [`set_logger`].** These messages cannot live in
    /// `ConfigArgs::build()`, which runs before the global tracing subscriber
    /// is installed: emitted there they would be silently dropped, and the
    /// operator whose LAN clients just stopped connecting would get no
    /// explanation at all. `build()` records the decision on
    /// [`WebsocketApiConfig::exposure`] and this replays it.
    ///
    /// Three independent things get reported, in the order an operator needs
    /// them: what changed for this node, why the bind is wide if it is, and
    /// whether the resulting exposure is dangerous.
    pub fn log_client_api_exposure(&self) {
        let address = self.ws_api.address;

        // These three were UNNAMESPACED (`WS_API_ADDRESS`, `ALLOWED_HOST`,
        // `ALLOWED_SOURCE_CIDRS`) until GHSA-824h-7x5x-wfmf. That was survivable
        // while a network-mode node bound `::` anyway and they only shaped which
        // Host headers it accepted; it is not now that `--ws-api-address`
        // decides the socket directly, at the highest precedence, and
        // `--allowed-source-cidrs` decides whether it is wide. A stray value in
        // a shared container, CI runner or systemd environment could open the
        // API. They are namespaced, and a leftover old-style variable is
        // REPORTED rather than ignored — otherwise a working deployment goes
        // loopback-only with no explanation.
        for (legacy, replacement) in [
            ("WS_API_ADDRESS", "FREENET_WS_API_ADDRESS"),
            ("ALLOWED_HOST", "FREENET_ALLOWED_HOST"),
            ("ALLOWED_SOURCE_CIDRS", "FREENET_ALLOWED_SOURCE_CIDRS"),
        ] {
            if std::env::var_os(legacy).is_some() && std::env::var_os(replacement).is_none() {
                tracing::warn!(
                    legacy,
                    replacement,
                    "The environment variable `{legacy}` is no longer read (it was \
                     unnamespaced, and it can now decide whether the client API listens \
                     beyond this machine). Rename it to `{replacement}`."
                );
            }
        }
        // These two branches report the SAME event — a persisted address was
        // dropped and re-derived — in the two directions it can go. They are
        // deliberately separate messages rather than one message plus a filter,
        // and the history is worth keeping.
        //
        // An earlier cut emitted only the narrowing text and fired it whenever
        // anything was dropped, so a node that dropped a loopback value and then
        // auto-widened printed "clients on other machines can no longer reach
        // this node" directly above "bound to all interfaces". The fix applied to
        // that contradiction was to SUPPRESS the first line for the widening
        // case — which silenced the only notice that a node someone had pinned
        // to loopback was now listening on every interface, and that widening
        // then went unnoticed through two further rounds of review.
        //
        // The lesson, because the next person to hit a noisy contradiction will
        // reach for the same fix: a contradictory pair of log lines is evidence
        // that the code does two DIFFERENT things on one path. Make each message
        // say which one happened. Never silence one of them — the branch you
        // suppress is the one nobody will hear about again.
        if let Some(persisted) = self.ws_api.exposure.dropped_persisted_address {
            if address.is_loopback() {
                // warn!, not info!: this fires on ONE boot and never again (from
                // boot 2 the persisted value re-derives to itself and is not
                // reported). For a remote gateway whose operator finds out days
                // later when a client fails, this line is the whole
                // explanation, so it must not rank below the exposure warning
                // that fires for the comparatively safe case.
                tracing::warn!(
                    %persisted,
                    resolved = %address,
                    "The client API now defaults to loopback, so the `ws-api-address` \
                     previously auto-written to config.toml has been re-derived. Clients \
                     on OTHER machines can no longer reach this node's API. If they \
                     should, add --ws-api-address :: to the node's invocation and KEEP it \
                     there — a value only persisted in config.toml is not enough, because \
                     this code cannot tell its own past output from your choice. On a \
                     systemd install add it with `systemctl edit` as a drop-in; do not \
                     hand-edit the generated unit, which marks it user-modified and opts \
                     this node out of future unit updates."
                );
            } else {
                // The opposite direction, and the one a hardening change owes an
                // operator loudly: their config named a LOOPBACK address, and we
                // dropped it (this code writes that value itself, so it cannot be
                // told from an operator's choice) and then widened on the CIDR
                // grant. The socket is more exposed than the config it replaced.
                tracing::warn!(
                    %persisted,
                    resolved = %address,
                    "The loopback `ws-api-address` in config.toml was re-derived — this \
                     code writes that value itself, so it cannot be distinguished from a \
                     deliberate pin — and --allowed-source-cidrs then widened the bind. \
                     The client API is now reachable from OTHER machines, which the \
                     config file alone said it should not be. Pass --ws-api-address \
                     explicitly to pin it either way."
                );
            }
        }
        if self.ws_api.exposure.source == WsApiAddressSource::AutoWidened {
            tracing::info!(
                %address,
                "Client API bound to all interfaces because --allowed-source-cidrs is \
                 set, preserving an invocation that relied on the old network-mode \
                 default (that flag is inert on a loopback socket). The default is now \
                 loopback; pass --ws-api-address explicitly to override this either \
                 way, and keep the flag in the invocation — a grant left only in \
                 config.toml does not widen a later boot."
            );
        }
        if let Some(reason) = ws_api_shares_one_namespace_with_remote_clients(
            self.ws_api.hosted_mode,
            address,
            &self.ws_api.allowed_hosts,
        ) {
            tracing::warn!(
                %address,
                allowed_hosts = ?self.ws_api.allowed_hosts,
                hosted_mode = self.ws_api.hosted_mode,
                "Client API exposure: {reason}. A connection that presents no per-user \
                 token drives this node's SHARED namespace and can read and modify its \
                 contract state, identities and keys. Bind loopback \
                 (--ws-api-address ::1) unless you intend this."
            );
        }
    }
}

#[derive(clap::Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetworkArgs {
    /// Address to bind to for the network event listener, default is :: (dual-stack)
    #[arg(
        name = "network_address",
        long = "network-address",
        env = "NETWORK_ADDRESS"
    )]
    #[serde(rename = "network-address", skip_serializing_if = "Option::is_none")]
    pub address: Option<IpAddr>,

    /// Port to bind for the network event listener, default is 31337
    #[arg(long, env = "NETWORK_PORT")]
    #[serde(rename = "network-port", skip_serializing_if = "Option::is_none")]
    pub network_port: Option<u16>,

    /// Public address for the network. Required for gateways.
    #[arg(long = "public-network-address", env = "PUBLIC_NETWORK_ADDRESS")]
    #[serde(
        rename = "public-network-address",
        skip_serializing_if = "Option::is_none"
    )]
    pub public_address: Option<IpAddr>,

    /// Public port for the network. Required for gateways.
    #[arg(long = "public-network-port", env = "PUBLIC_NETWORK_PORT")]
    #[serde(
        rename = "public-network-port",
        skip_serializing_if = "Option::is_none"
    )]
    pub public_port: Option<u16>,

    /// Whether the node is a gateway or not.
    /// If the node is a gateway, it will be able to accept connections from other nodes.
    #[arg(long)]
    pub is_gateway: bool,

    /// Skip fetching the remote gateway index.
    ///
    /// The on-disk gateways.toml cache is also skipped in two cases: when the
    /// node is a gateway (`--is-gateway`), which always runs isolated under this
    /// flag, and when an explicit `--gateway` entry is supplied, in which case
    /// the command-line entries replace the cache. A non-gateway peer with no
    /// `--gateway` entry still reads gateways.toml.
    // Any hidden `--gateways` JSON entries are honored in all three cases, and
    // merged with the cache in the last one. That last case preserves the
    // contract test harnesses rely on (e.g. freenet-test-network), which
    // pre-populate gateways.toml via `--config-dir`.
    #[arg(long)]
    pub skip_load_from_network: bool,

    /// Optional list of gateways to connect to in network mode. Used for testing purposes.
    #[arg(long, hide = true)]
    pub gateways: Option<Vec<String>>,

    /// Gateway peers to connect to, specified as "ip:port,hex-pubkey".
    /// The hex-pubkey is a 64-character hex-encoded X25519 public key (32 bytes).
    /// Can be repeated: --gateway "1.2.3.4:31337,abcd..." --gateway "5.6.7.8:31337,ef01..."
    #[arg(long)]
    #[serde(rename = "gateway", skip_serializing_if = "Option::is_none")]
    pub gateway: Option<Vec<String>>,

    /// Optional location of the node, this is to be able to deterministically set locations for gateways for testing purposes.
    #[arg(long, hide = true, env = "LOCATION")]
    pub location: Option<f64>,

    /// Ignores protocol version failures, continuing to run the node if there is a mismatch with the gateway.
    #[arg(long)]
    pub ignore_protocol_checking: bool,

    /// Bandwidth limit for large streaming data transfers, in bytes per second.
    /// Applies only to the streaming path used for large transfers; the general
    /// packet rate limiter is currently disabled for reliability reasons.
    /// Default: 3 MB/s (3,000,000 bytes/second).
    #[arg(long)]
    pub bandwidth_limit: Option<usize>,

    /// Total bandwidth limit across all connections, in bytes per second. Each
    /// connection is allowed total / active_connections. Overrides the
    /// per-connection `--bandwidth-limit`.
    #[arg(long)]
    #[serde(
        rename = "total-bandwidth-limit",
        skip_serializing_if = "Option::is_none"
    )]
    pub total_bandwidth_limit: Option<usize>,

    /// Minimum bandwidth per connection when using total_bandwidth_limit (bytes/sec).
    /// Prevents connection starvation when many connections are active.
    /// Default: 1 MB/s (1,000,000 bytes/second)
    #[arg(long)]
    #[serde(
        rename = "min-bandwidth-per-connection",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_bandwidth_per_connection: Option<usize>,

    /// List of IP:port addresses to refuse connections to/from.
    #[arg(long, num_args = 0..)]
    pub blocked_addresses: Option<Vec<SocketAddr>>,

    /// Capacity for the event loop notification and op execution channels.
    /// Default: 2048. Increase under sustained multi-client load to reduce
    /// channel saturation and associated context-switch spikes.
    #[arg(long, env = "EVENT_LOOP_CHANNEL_CAPACITY")]
    #[serde(
        rename = "event-loop-channel-capacity",
        skip_serializing_if = "Option::is_none"
    )]
    pub event_loop_channel_capacity: Option<usize>,

    /// Maximum number of concurrent transient connections accepted by a gateway.
    #[arg(long, env = "TRANSIENT_BUDGET")]
    #[serde(rename = "transient-budget", skip_serializing_if = "Option::is_none")]
    pub transient_budget: Option<usize>,

    /// Time (in seconds) before an unpromoted transient connection is dropped.
    #[arg(long, env = "TRANSIENT_TTL_SECS")]
    #[serde(rename = "transient-ttl-secs", skip_serializing_if = "Option::is_none")]
    pub transient_ttl_secs: Option<u64>,

    /// Minimum desired connections for the ring topology. Defaults to 10.
    #[arg(long = "min-number-of-connections", env = "MIN_NUMBER_OF_CONNECTIONS")]
    #[serde(
        rename = "min-number-of-connections",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_connections: Option<usize>,

    /// Maximum allowed connections for the ring topology. Defaults to 20.
    #[arg(long = "max-number-of-connections", env = "MAX_NUMBER_OF_CONNECTIONS")]
    #[serde(
        rename = "max-number-of-connections",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_connections: Option<usize>,

    /// Threshold in bytes above which streaming transport is used.
    /// Default: 65536 (64KB)
    #[arg(long, env = "STREAMING_THRESHOLD")]
    #[serde(
        rename = "streaming-threshold",
        skip_serializing_if = "Option::is_none"
    )]
    pub streaming_threshold: Option<usize>,

    /// Minimum ssthresh floor for LEDBAT timeout recovery (bytes).
    ///
    /// On high-latency paths (>100ms RTT), repeated timeouts can cause ssthresh
    /// to collapse to ~5KB, severely limiting throughput recovery.
    /// Setting a higher floor prevents this "ssthresh death spiral".
    ///
    /// Recommended values by network type:
    /// - LAN (<10ms RTT): None (use default)
    /// - Regional (10-50ms): None (use default)
    /// - Continental (50-100ms): 51200 (50KB)
    /// - Intercontinental (100-200ms): 102400-512000 (100KB-500KB)
    /// - Satellite (500ms+): 524288-2097152 (500KB-2MB)
    ///
    /// Default: None (uses spec-compliant 2*min_cwnd ≈ 5.7KB floor)
    #[arg(long, env = "LEDBAT_MIN_SSTHRESH")]
    #[serde(
        rename = "ledbat-min-ssthresh",
        skip_serializing_if = "Option::is_none"
    )]
    pub ledbat_min_ssthresh: Option<usize>,

    /// Congestion control algorithm for transport connections.
    ///
    /// Available algorithms:
    /// - `fixedrate` (default): Fixed-rate transmission at 10 Mbps per connection, ignores network feedback
    /// - `bbr`: BBR (Bottleneck Bandwidth and RTT) - model-based, tolerates packet loss
    /// - `ledbat`: LEDBAT++ - delay-based, yields to foreground traffic
    ///
    /// Default: `fixedrate` (most stable for production)
    #[arg(long, env = "FREENET_CONGESTION_CONTROL")]
    #[serde(rename = "congestion-control", skip_serializing_if = "Option::is_none")]
    pub congestion_control: Option<String>,

    /// BBR startup minimum pacing rate (bytes/sec).
    ///
    /// Only used when congestion_control is set to "bbr".
    /// Lower values are safer for virtualized/constrained network environments (like CI).
    ///
    /// Default: 25 MB/s (25_000_000 bytes/sec)
    #[arg(long, env = "FREENET_BBR_STARTUP_RATE")]
    #[serde(rename = "bbr-startup-rate", skip_serializing_if = "Option::is_none")]
    pub bbr_startup_rate: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InlineGwConfig {
    /// Address of the gateway.
    pub address: SocketAddr,

    /// Path to the public key of the gateway (hex-encoded X25519 key).
    ///
    /// Deliberately NO `public-key` alias, unlike [`GatewayConfig`]: this is
    /// the hidden `--gateways` JSON flag, emitted only by test harnesses, so a
    /// second spelling buys nothing — while accepting one would re-create the
    /// fatal `duplicate field` case in the one place `redundant_key_spellings`
    /// does not reach.
    #[serde(rename = "public_key")]
    pub public_key_path: PathBuf,

    /// Optional location of the gateway. Necessary for deterministic testing.
    pub location: Option<f64>,
}

/// Parse a `--gateway` value in the format "ip:port,hex-pubkey".
///
/// Validates the socket address and the 32-byte X25519 public key (64 hex chars),
/// writes the key to a file in `secrets_dir`, and returns a `GatewayConfig`.
fn parse_gateway(input: &str, secrets_dir: &Path) -> anyhow::Result<GatewayConfig> {
    let (addr_str, key_hex) = input.split_once(',').ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid --gateway format: expected \"ip:port,hex-pubkey\", got \"{input}\""
        )
    })?;

    let addr: SocketAddr = addr_str
        .trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid socket address \"{addr_str}\" in --gateway: {e}"))?;

    let key_bytes = hex::decode(key_hex.trim())
        .map_err(|e| anyhow::anyhow!("Invalid hex public key in --gateway: {e}"))?;

    if key_bytes.len() != 32 {
        anyhow::bail!(
            "Invalid public key length {} in --gateway (expected 32 bytes / 64 hex chars)",
            key_bytes.len()
        );
    }

    // Write the hex-encoded key to secrets_dir so NodeConfig::new can load it
    // (NodeConfig reads the file and calls hex::decode on the contents).
    fs::create_dir_all(secrets_dir)?;
    // Use hex-encoded address for the filename to avoid IPv6 bracket/colon issues
    let key_filename = format!("cli_gw_{}.pub", hex::encode(addr.to_string()));
    let key_path = secrets_dir.join(&key_filename);

    // Write with restricted permissions from the start to avoid a TOCTOU window
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&key_path)?;
        file.write_all(key_hex.trim().as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        fs::write(&key_path, key_hex.trim())?;
    }

    Ok(GatewayConfig {
        address: Address::HostAddress(addr),
        public_key_path: key_path,
        location: None,
    })
}

impl NetworkArgs {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.is_gateway {
            // A gateway advertises its own identity (peer_id) from its public
            // address + port and, unlike a NAT'd peer, can never learn or
            // correct it later. Require both explicitly; otherwise peer_id is
            // None and the gateway boots with no ring location. See #4324.
            if self.public_address.is_none() {
                return Err(anyhow::anyhow!(
                    "Gateway nodes must specify a public network address (--public-network-address)"
                ));
            }
            if self.public_port.is_none() {
                return Err(anyhow::anyhow!(
                    "Gateway nodes must specify a public network port (--public-network-port)"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkApiConfig {
    /// Address to listen to locally
    #[serde(default = "default_listening_address", rename = "network-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[serde(default = "default_network_api_port", rename = "network-port")]
    pub port: u16,

    /// Public external address for the network, mandatory for gateways.
    #[serde(
        rename = "public_network_address",
        alias = "public-network-address",
        skip_serializing_if = "Option::is_none"
    )]
    pub public_address: Option<IpAddr>,

    /// Public external port for the network, mandatory for gateways.
    /// Both kebab spellings are accepted: `public-network-port` (which matches
    /// the `--public-network-port` flag and is the key this will be WRITTEN as
    /// once #5130 lands) and the direct `public-port`.
    #[serde(
        rename = "public_port",
        alias = "public-network-port",
        alias = "public-port",
        skip_serializing_if = "Option::is_none"
    )]
    pub public_port: Option<u16>,

    /// Whether to ignore protocol version compatibility routine while initiating connections.
    #[serde(skip)]
    pub ignore_protocol_version: bool,

    /// Bandwidth limit per connection for data transfers (in bytes per second).
    /// NOTE: This applies to each connection independently - N connections may use N * bandwidth_limit total.
    /// Each connection uses LEDBAT congestion control to yield to foreground traffic.
    /// Default: 10 MB/s (10,000,000 bytes/second)
    ///
    /// If `total_bandwidth_limit` is set, this field is ignored and per-connection rates
    /// are derived from: `total_bandwidth_limit / active_connections`.
    #[serde(alias = "bandwidth-limit", skip_serializing_if = "Option::is_none")]
    pub bandwidth_limit: Option<usize>,

    /// Total bandwidth limit across ALL connections (in bytes per second).
    /// When set, individual connection rates are computed as: `total / active_connections`.
    /// This overrides the per-connection `bandwidth_limit`.
    ///
    /// Example: With 50 MB/s total and 5 connections, each gets 10 MB/s.
    /// Default: None (use per-connection `bandwidth_limit` instead)
    #[serde(
        alias = "total-bandwidth-limit",
        skip_serializing_if = "Option::is_none"
    )]
    pub total_bandwidth_limit: Option<usize>,

    /// Minimum bandwidth per connection when using `total_bandwidth_limit` (bytes/sec).
    /// Prevents connection starvation when many connections are active.
    ///
    /// If `total / N < min`, each connection gets `min` (exceeding total is possible).
    /// Default: 1 MB/s (1,000,000 bytes/second)
    #[serde(
        alias = "min-bandwidth-per-connection",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_bandwidth_per_connection: Option<usize>,

    /// List of IP:port addresses to refuse connections to/from.
    #[serde(alias = "blocked-addresses", skip_serializing_if = "Option::is_none")]
    pub blocked_addresses: Option<HashSet<SocketAddr>>,

    /// Capacity for the event loop notification and op execution channels.
    /// Default: 2048. Increase under sustained multi-client load to reduce
    /// channel saturation and associated context-switch spikes.
    #[serde(
        default = "default_event_loop_channel_capacity",
        alias = "event-loop-channel-capacity"
    )]
    pub event_loop_channel_capacity: usize,

    /// Maximum number of concurrent transient connections accepted by a gateway.
    #[serde(default = "default_transient_budget", rename = "transient-budget")]
    pub transient_budget: usize,

    /// Time (in seconds) before an unpromoted transient connection is dropped.
    #[serde(default = "default_transient_ttl_secs", rename = "transient-ttl-secs")]
    pub transient_ttl_secs: u64,

    /// Minimum desired connections for the ring topology.
    #[serde(
        default = "default_min_connections",
        rename = "min-number-of-connections"
    )]
    pub min_connections: usize,

    /// Maximum allowed connections for the ring topology.
    #[serde(
        default = "default_max_connections",
        rename = "max-number-of-connections"
    )]
    pub max_connections: usize,

    /// Threshold in bytes above which streaming transport is used.
    /// Default: 65536 (64KB)
    #[serde(
        default = "default_streaming_threshold",
        rename = "streaming-threshold"
    )]
    pub streaming_threshold: usize,

    /// Minimum ssthresh floor for LEDBAT timeout recovery (bytes).
    ///
    /// On high-latency paths (>100ms RTT), repeated timeouts can cause ssthresh
    /// to collapse to ~5KB, severely limiting throughput recovery.
    /// Setting a higher floor prevents this "ssthresh death spiral".
    ///
    /// Default: 102400 (100KB) - suitable for intercontinental connections.
    /// Set to None for LAN-only deployments.
    #[serde(
        default = "default_ledbat_min_ssthresh",
        rename = "ledbat-min-ssthresh",
        skip_serializing_if = "Option::is_none"
    )]
    pub ledbat_min_ssthresh: Option<usize>,

    /// Congestion control algorithm for transport connections.
    ///
    /// Available algorithms:
    /// - `fixedrate` (default): Fixed-rate transmission at 10 Mbps per connection
    /// - `bbr`: BBR (Bottleneck Bandwidth and RTT)
    /// - `ledbat`: LEDBAT++ (Low Extra Delay Background Transport)
    #[serde(default = "default_congestion_control", rename = "congestion-control")]
    pub congestion_control: String,

    /// BBR startup minimum pacing rate (bytes/sec).
    ///
    /// Only used when congestion_control is "bbr".
    #[serde(
        default = "default_bbr_startup_rate",
        rename = "bbr-startup-rate",
        skip_serializing_if = "Option::is_none"
    )]
    pub bbr_startup_rate: Option<u64>,

    /// When true, this node is part of a local/test network and does not load
    /// gateways from the public remote index. Used to disable the relay-ready
    /// gate and other production-only features. The on-disk gateways.toml is
    /// also skipped in two cases: when `is_gateway` is true (isolated
    /// gateway), and when an explicit `--gateway` CLI entry is supplied. With
    /// neither, the on-disk gateways.toml is still read — the test-harness
    /// contract preserved for callers like freenet-test-network's Docker NAT
    /// path that pre-populate the file in a custom `--config-dir`.
    #[serde(default, alias = "skip-load-from-network")]
    pub skip_load_from_network: bool,
}

impl NetworkApiConfig {
    /// Build a `CongestionControlConfig` from the current network API configuration.
    ///
    /// This parses the `congestion_control` string to determine the algorithm
    /// and applies any algorithm-specific settings like `bbr_startup_rate`.
    pub fn build_congestion_config(&self) -> CongestionControlConfig {
        let algo = match self.congestion_control.to_lowercase().as_str() {
            "bbr" => CongestionControlAlgorithm::Bbr,
            "ledbat" => CongestionControlAlgorithm::Ledbat,
            _ => CongestionControlAlgorithm::FixedRate, // Default for production
        };

        let mut config = CongestionControlConfig::new(algo);

        // Apply BBR-specific settings
        if algo == CongestionControlAlgorithm::Bbr {
            if let Some(rate) = self.bbr_startup_rate {
                tracing::debug!("Using custom BBR startup pacing rate: {} bytes/sec", rate);
                config = config.with_startup_min_pacing_rate(rate);
            }
        }

        config
    }
}

mod port_allocation;
use port_allocation::find_available_port;

pub fn default_network_api_port() -> u16 {
    find_available_port().unwrap_or(31337) // Fallback to 31337 if we can't find a random port
}

pub(crate) fn default_event_loop_channel_capacity() -> usize {
    DEFAULT_EVENT_LOOP_CHANNEL_CAPACITY
}

fn default_transient_budget() -> usize {
    DEFAULT_TRANSIENT_BUDGET
}

fn default_transient_ttl_secs() -> u64 {
    DEFAULT_TRANSIENT_TTL_SECS
}

fn default_min_connections() -> usize {
    DEFAULT_MIN_CONNECTIONS
}

fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

/// Default streaming threshold: 64KB
fn default_streaming_threshold() -> usize {
    64 * 1024
}

/// Default minimum ssthresh for LEDBAT timeout recovery.
///
/// Returns `Some(100KB)` - suitable for intercontinental connections where
/// repeated timeouts could otherwise cause ssthresh to collapse to ~5KB.
///
/// See: docs/architecture/transport/configuration/bandwidth-configuration.md
fn default_ledbat_min_ssthresh() -> Option<usize> {
    Some(100 * 1024) // 100KB floor
}

/// Default congestion control algorithm.
///
/// Returns "fixedrate" - the most stable option for production.
fn default_congestion_control() -> String {
    "fixedrate".to_string()
}

/// Default BBR startup pacing rate.
///
/// Returns None to use the BBR default (25 MB/s).
fn default_bbr_startup_rate() -> Option<u64> {
    None
}

#[derive(clap::Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct WebsocketApiArgs {
    /// Address to bind for the local HTTP/WebSocket client API.
    ///
    /// Defaults to loopback (`::1`, plus a `127.0.0.1` companion bind) in both
    /// operation modes, since running as a network peer says nothing about
    /// wanting this node's fully privileged control API reachable from other
    /// machines. Pass `::`, or a specific interface address, to serve clients on
    /// other hosts, and keep the flag in the node's invocation: a value left
    /// only in config.toml is re-derived on the next boot.
    ///
    /// In network mode `--allowed-source-cidrs` widens this bind on its own,
    /// because it is inert on a loopback socket and so can only have been set by
    /// someone expecting non-local clients. `--allowed-host` does not widen it:
    /// that is a Host-header allowlist, and it works on loopback, where a
    /// same-host reverse proxy lives. A reverse proxy on a different host needs
    /// this flag too.
    ///
    /// Security: anything that can reach this address and port can read and
    /// modify your contract state, identities and keys.
    #[arg(
        name = "ws_api_address",
        long = "ws-api-address",
        env = "FREENET_WS_API_ADDRESS"
    )]
    #[serde(rename = "ws-api-address", skip_serializing_if = "Option::is_none")]
    pub address: Option<IpAddr>,

    /// Port to expose the websocket on, default is 7509
    #[arg(long, env = "WS_API_PORT")]
    #[serde(rename = "ws-api-port", skip_serializing_if = "Option::is_none")]
    pub ws_api_port: Option<u16>,

    /// Token time-to-live in seconds (default is 86400 = 24 hours)
    #[arg(long, env = "TOKEN_TTL_SECONDS")]
    #[serde(rename = "token-ttl-seconds", skip_serializing_if = "Option::is_none")]
    pub token_ttl_seconds: Option<u64>,

    /// Token cleanup interval in seconds (default is 300 = 5 minutes)
    #[arg(long, env = "TOKEN_CLEANUP_INTERVAL_SECONDS")]
    #[serde(
        rename = "token-cleanup-interval-seconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub token_cleanup_interval_seconds: Option<u64>,

    /// Additional hostname(s) to accept in the Host header for the local
    /// HTTP/WebSocket API (including the delegate permission-prompt
    /// endpoints `/permission/pending`, `/permission/events`,
    /// `/permission/events/ws`, and
    /// `/permission/{nonce}/respond`).
    /// Use when accessing the node via a custom domain (e.g., through a reverse proxy).
    /// Can be specified multiple times. If omitted, only the machine's hostname and
    /// bound IP are accepted.
    #[arg(long, env = "FREENET_ALLOWED_HOST")]
    #[serde(rename = "allowed-host", skip_serializing_if = "Option::is_none")]
    pub allowed_host: Option<Vec<String>>,

    /// Additional source IP ranges, in CIDR notation, allowed to reach the
    /// local HTTP/WebSocket API.
    ///
    /// This flag does two things, and the first is easy to miss:
    ///
    /// 1. Without `--ws-api-address`, in network mode, it binds the API to all
    ///    interfaces. The source filter it relaxes never runs on a loopback
    ///    socket, so the flag would otherwise do nothing.
    /// 2. It then accepts the ranges named here on top of loopback and all of
    ///    RFC1918 and IPv6 ULA, which are always accepted. It never narrows
    ///    access: this is not an "only these sources" allowlist.
    ///
    /// So `--allowed-source-cidrs 100.64.0.0/10` on its own means: listen on
    /// every interface, accept your entire local network, and accept that range
    /// as well. Pass `--ws-api-address` too to keep the bind under your control.
    ///
    /// Security: only add ranges you fully control. CGNAT space such as
    /// `100.64.0.0/10` is shared between subscribers of some ISPs (Starlink,
    /// T-Mobile, many cable carriers) and is safe only on an overlay network
    /// such as Tailscale or WireGuard. Anything that can reach the API port can
    /// access your contract state, keys, and client API.
    #[arg(
        long = "allowed-source-cidrs",
        env = "FREENET_ALLOWED_SOURCE_CIDRS",
        value_delimiter = ','
    )]
    #[serde(
        rename = "allowed-source-cidrs",
        skip_serializing_if = "Option::is_none"
    )]
    pub allowed_source_cidrs: Option<Vec<String>>,

    /// Opt in to hosted mode, off by default: honor the durable `userToken`
    /// query parameter on the WebSocket upgrade and give each token its own
    /// delegate-secret namespace. Turn it on only for a node you intend to
    /// operate as a shared public proxy for untrusted users.
    ///
    /// While it is off, `userToken` is ignored and every connection is
    /// single-user.
    ///
    /// Even with hosted mode on, a `userToken` is honored only on a loopback
    /// connection carrying `X-Forwarded-Proto: https`, which means a
    /// TLS-terminating reverse proxy on the same host. The loopback source shows
    /// the proxy-to-node hop is local, and the `https` header is the TLS
    /// terminator's evidence that the browser-to-proxy hop used TLS. Two cases
    /// are refused with a `403`: any non-loopback source, whatever headers it
    /// sends, and a loopback source without `X-Forwarded-Proto: https`, so a
    /// plaintext loopback connection is refused too.
    ///
    /// The `Host` header plays no part in that decision, so `--allowed-host`
    /// cannot make a token acceptable. It still governs which origins the node
    /// accepts requests from, so it remains relevant to a hosted node's attack
    /// surface.
    ///
    /// Required proxy configuration: run a TLS-terminating reverse proxy on the
    /// same host, connecting to the node over loopback. The proxy has to set
    /// `X-Forwarded-Proto` itself to the real browser-facing scheme, and strip
    /// any `X-Forwarded-*` headers the client sent, so that a client cannot
    /// forge the TLS attestation. Caddy does both by default. nginx forwards
    /// unknown client headers through by default, so it needs
    /// `proxy_set_header X-Forwarded-Proto $scheme;`, which both sets the header
    /// and stops the client's own copy being passed through. A literal `https`
    /// works there too if the server block is HTTPS-only.
    ///
    /// Known limitation: the node cannot tell an `X-Forwarded-Proto` the proxy
    /// set from one it passed through. A proxy misconfigured to forward a
    /// client-supplied `X-Forwarded-Proto: https` over a plaintext listener
    /// would let a client spoof it and use a token over cleartext. Configuring
    /// the proxy correctly is the operator's responsibility.
    ///
    /// Testing hosted mode locally needs the same setup, or the header sent by
    /// hand (`curl -H 'X-Forwarded-Proto: https'` from loopback). A TLS
    /// terminator on a different host, such as a remote load balancer, is not
    /// supported, because its source address is not loopback.
    ///
    /// Works as a bare flag: `--hosted-mode` turns it on, `--hosted-mode=false`
    /// turns it off, and leaving it out keeps whatever the config file or
    /// environment set.
    // Internal (P2 of #4381, refuse-plaintext-token): `Host` is not consulted
    // because a proxy can rewrite it (nginx's default rewrites it to the
    // upstream `127.0.0.1:7509`), so it cannot grant trust; only the
    // `X-Forwarded-Proto` header can.
    //
    // Kept as `Option<bool>` rather than a `bool` with `default_value` so
    // config-file and env layering can leave it unset (`None`) and the CLI only
    // overrides when actually present. `None` resolves to `false` in `build`.
    #[arg(
        long = "hosted-mode",
        env = "FREENET_HOSTED_MODE",
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    #[serde(rename = "hosted-mode", skip_serializing_if = "Option::is_none")]
    pub hosted_mode: Option<bool>,

    /// Sustained per-user operation rate limit, in requests per second, for
    /// hosted mode. Limits how fast one hosted user (one `userToken`) can issue
    /// contract operations (GET, PUT, UPDATE, SUBSCRIBE), so a single visitor
    /// cannot flood the node's executor and network. Requests over the rate are
    /// refused at the WebSocket boundary and the client retries. Default: 10.
    /// Use `0` to disable. Ignored outside hosted mode.
    // Internal: #4561, P5 of #4381.
    #[arg(long = "per-user-op-rate-limit", env = "PER_USER_OP_RATE_LIMIT")]
    pub per_user_op_rate_limit: Option<u64>,

    /// Per-user operation burst capacity for hosted mode: how many operations an
    /// idle user can issue back to back before being throttled to
    /// `--per-user-op-rate-limit`. Default: 100. Only meaningful when operation
    /// rate limiting is on.
    // Internal: #4561.
    #[arg(long = "per-user-op-burst", env = "PER_USER_OP_BURST")]
    pub per_user_op_burst: Option<u64>,

    /// Minimum seconds between hosted-export downloads, per user. The export
    /// endpoint enumerates and re-encrypts every secret in the user's scope, so
    /// it is far more expensive than a single operation and gets its own,
    /// tighter limit. A request inside this window returns HTTP 429.
    /// Default: 10. Use `0` to disable. Hosted mode only.
    // Internal: #4561.
    #[arg(
        long = "per-user-export-min-interval-secs",
        env = "PER_USER_EXPORT_MIN_INTERVAL_SECS"
    )]
    pub per_user_export_min_interval_secs: Option<u64>,
}

/// Default telemetry endpoint (nova.locut.us OTLP collector).
/// Using domain name for resilience to IP changes.
pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "http://nova.locut.us:4318";

#[derive(clap::Parser, Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryArgs {
    /// Send telemetry to help improve Freenet. On by default during alpha.
    /// It covers operation timing and network topology. Contract content is
    /// never included.
    #[arg(
        long = "telemetry-enabled",
        env = "FREENET_TELEMETRY_ENABLED",
        default_value = "true"
    )]
    #[serde(rename = "telemetry-enabled", default = "default_telemetry_enabled")]
    pub enabled: bool,

    /// Telemetry endpoint URL (OTLP/HTTP format)
    #[arg(long = "telemetry-endpoint", env = "FREENET_TELEMETRY_ENDPOINT")]
    #[serde(rename = "telemetry-endpoint", skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Interval in seconds for emitting transport layer metric snapshots.
    /// Set to 0 to disable transport snapshots. Default: 30 seconds.
    #[arg(
        long = "transport-snapshot-interval-secs",
        env = "FREENET_TRANSPORT_SNAPSHOT_INTERVAL_SECS"
    )]
    #[serde(
        rename = "transport-snapshot-interval-secs",
        skip_serializing_if = "Option::is_none"
    )]
    pub transport_snapshot_interval_secs: Option<u64>,

    /// Send a reference ping once a second: a UDP DNS query to a fixed external
    /// target (1.1.1.1:53 by default) whose round-trip time is recorded next to
    /// the per-peer overlay RTT, so overlay queueing can be told apart from
    /// local uplink contention. Off by default; production gateways turn it on.
    // Internal (Phase 1.5 of #4074): stays off on developer machines and in
    // integration tests so CI does not fire DNS traffic.
    #[arg(
        long = "reference-ping-enabled",
        env = "FREENET_REFERENCE_PING_ENABLED",
        default_value = "false"
    )]
    #[serde(
        rename = "reference-ping-enabled",
        default = "default_reference_ping_enabled"
    )]
    pub reference_ping_enabled: bool,

    /// Report interface transmit totals once a second by reading
    /// `/proc/net/dev` on Linux, along with how much of that traffic is not
    /// Freenet's own, so uplink saturation can be attributed to Freenet or to
    /// the operator's other traffic. Best-effort, and off by default;
    /// production gateways turn it on.
    // Internal (Phase 1.6 of #4074): emits aggregate tx bytes and the derived
    // `op = total - freenet_own`. Like reference-ping, it stays off on
    // developer machines and in tests.
    #[arg(
        long = "iface-tx-enabled",
        env = "FREENET_IFACE_TX_ENABLED",
        default_value = "false"
    )]
    #[serde(rename = "iface-tx-enabled", default = "default_iface_tx_enabled")]
    pub iface_tx_enabled: bool,
}

impl Default for TelemetryArgs {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: None,
            transport_snapshot_interval_secs: None,
            reference_ping_enabled: false,
            iface_tx_enabled: false,
        }
    }
}

fn default_telemetry_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Whether telemetry reporting is enabled
    #[serde(default = "default_telemetry_enabled", rename = "telemetry-enabled")]
    pub enabled: bool,

    /// Telemetry endpoint URL
    #[serde(default = "default_telemetry_endpoint", rename = "telemetry-endpoint")]
    pub endpoint: String,

    /// Interval in seconds for emitting transport layer metric snapshots.
    /// Set to 0 to disable transport snapshots.
    /// Default: 30 seconds.
    #[serde(
        default = "default_transport_snapshot_interval_secs",
        rename = "transport-snapshot-interval-secs"
    )]
    pub transport_snapshot_interval_secs: u64,

    /// Whether this is a test environment (detected via --id flag).
    /// When true, telemetry is disabled to avoid flooding the collector with test data.
    #[serde(skip)]
    pub is_test_environment: bool,

    /// Enable the Phase 1.5 reference-ping shadow probe (#4074).
    /// Opt-in: defaults to false; production gateway configs set
    /// this to true. See `TelemetryArgs::reference_ping_enabled`.
    #[serde(
        default = "default_reference_ping_enabled",
        rename = "reference-ping-enabled"
    )]
    pub reference_ping_enabled: bool,

    /// Enable the Phase 1.6 OS-interface-tx shadow probe (#4074).
    /// Opt-in: defaults to false; production gateway configs set this to
    /// true. See `TelemetryArgs::iface_tx_enabled`.
    #[serde(default = "default_iface_tx_enabled", rename = "iface-tx-enabled")]
    pub iface_tx_enabled: bool,
}

fn default_transport_snapshot_interval_secs() -> u64 {
    30
}

fn default_telemetry_endpoint() -> String {
    DEFAULT_TELEMETRY_ENDPOINT.to_string()
}

fn default_reference_ping_enabled() -> bool {
    false
}

fn default_iface_tx_enabled() -> bool {
    false
}

/// Default OTLP/HTTP endpoint for the SDK metrics pipeline, used when neither
/// the standard `OTEL_EXPORTER_OTLP_*` env vars nor `otel-endpoint` are set.
///
/// Deliberately NOT `DEFAULT_TELEMETRY_ENDPOINT`: `otel-telemetry-enabled` and
/// `telemetry-enabled` are strictly isolated features that are not expected to
/// share a backend. Pointing this pipeline at the central dashboard collector
/// must always be an explicit operator choice.
pub const DEFAULT_OTEL_ENDPOINT: &str = "http://localhost:4318";

/// CLI/file args for the OpenTelemetry SDK metrics exporter.
///
/// Strictly independent of [`TelemetryArgs`]: no shared field, no shared
/// default, no fallback in either direction.
#[derive(clap::Parser, Debug, Clone, Default, Serialize, Deserialize)]
pub struct OtelArgs {
    /// Enable the OpenTelemetry SDK metrics exporter. Independent of
    /// `telemetry-enabled`; enabling or disabling one has no effect on the
    /// other.
    ///
    /// `num_args`/`default_missing_value` rather than a bare flag: with an
    /// `env` binding, clap's `SetTrue` action treats ANY value of the variable
    /// as true, so `FREENET_OTEL_TELEMETRY_ENABLED=false` would silently turn
    /// the exporter ON. This form accepts `--otel-telemetry-enabled`,
    /// `--otel-telemetry-enabled=false`, and a properly parsed env value.
    #[arg(
        id = "otel_telemetry_enabled",
        long = "otel-telemetry-enabled",
        env = "FREENET_OTEL_TELEMETRY_ENABLED",
        num_args = 0..=1,
        default_value = "false",
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    #[serde(rename = "otel-telemetry-enabled", default)]
    pub enabled: bool,

    /// OTLP/HTTP collector base URL (e.g. `http://collector:4318`).
    ///
    /// No clap `env =` binding on purpose. The standard
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
    /// variables must take priority over this file-level value, and binding
    /// them here would merge them into the config layer and invert that
    /// precedence. They are resolved in `tracing::otel` instead.
    #[arg(id = "otel_endpoint", long = "otel-endpoint")]
    #[serde(rename = "otel-endpoint", skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

/// Resolved configuration for the OpenTelemetry SDK metrics exporter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OtelConfig {
    /// Whether the SDK metrics exporter is enabled.
    #[serde(default, rename = "otel-telemetry-enabled")]
    pub enabled: bool,

    /// Operator-configured OTLP/HTTP collector base URL, if any. `None` means
    /// "let the SDK resolve it" — see `tracing::otel::resolve_metrics_endpoint`.
    #[serde(
        default,
        rename = "otel-endpoint",
        skip_serializing_if = "Option::is_none"
    )]
    pub endpoint: Option<String>,

    /// Whether this is a test environment (detected via `--id`). Mirrors
    /// [`TelemetryConfig::is_test_environment`]; suppresses export so test
    /// networks can't ship data to a collector.
    #[serde(skip)]
    pub is_test_environment: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: DEFAULT_TELEMETRY_ENDPOINT.to_string(),
            transport_snapshot_interval_secs: default_transport_snapshot_interval_secs(),
            is_test_environment: false,
            reference_ping_enabled: false,
            iface_tx_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketApiConfig {
    /// Address to bind to
    /// The serde fallback is LOOPBACK, matching `resolve_ws_api_address`: a
    /// `WebsocketApiConfig` deserialized outside `ConfigArgs::build()` must not
    /// land on a wildcard listener just because the key was absent
    /// (GHSA-824h-7x5x-wfmf). Inside `build()` either value is re-derived
    /// anyway — both are `is_auto_derivable_ws_api_address`.
    #[serde(default = "default_local_address", rename = "ws-api-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[serde(default = "default_ws_api_port", rename = "ws-api-port")]
    pub port: u16,

    /// Token time-to-live in seconds
    #[serde(default = "default_token_ttl_seconds", rename = "token-ttl-seconds")]
    pub token_ttl_seconds: u64,

    /// Token cleanup interval in seconds
    #[serde(
        default = "default_token_cleanup_interval_seconds",
        rename = "token-cleanup-interval-seconds"
    )]
    pub token_cleanup_interval_seconds: u64,

    /// Additional hostnames allowed in the Host header for WebSocket connections.
    /// Empty means only auto-detected hostnames (machine hostname + bound IP) are allowed.
    #[serde(default, rename = "allowed-host")]
    pub allowed_hosts: Vec<String>,

    /// Additional source IP ranges (CIDR) permitted to reach the API.
    /// Stored as parsed `IpNet` so config errors surface at startup.
    /// Empty means only loopback + RFC1918 / IPv6 ULA are accepted.
    #[serde(default, rename = "allowed-source-cidrs")]
    pub allowed_source_cidrs: Vec<ipnet::IpNet>,

    /// Opt-in hosted mode (P2 of #4381). When `true`, a WebSocket connection
    /// that presents a durable per-user token (the `userToken` query parameter)
    /// gets a per-user delegate-secret namespace derived from that token; when
    /// `false` (the default), the `userToken` parameter is ignored entirely and
    /// every connection is single-user — byte-for-byte the pre-#4381 behavior.
    ///
    /// This flag ONLY governs whether the WS boundary derives a per-user
    /// context; everything downstream is driven by whether a context was
    /// derived, so with the flag off the entire feature is inert.
    #[serde(default, rename = "hosted-mode")]
    pub hosted_mode: bool,

    /// Sustained per-user operation rate limit (requests/second) for hosted
    /// mode (#4561, P5 of #4381). Bounds how fast a single hosted user can
    /// issue contract operations so one visitor cannot flood the node. `0`
    /// disables op rate limiting. No effect outside hosted mode. Default 10.
    #[serde(
        default = "default_per_user_op_rate_limit",
        rename = "per-user-op-rate-limit"
    )]
    pub per_user_op_rate_limit: u64,

    /// Per-user operation burst capacity for hosted mode (#4561). Max ops a
    /// previously-idle user may issue back-to-back before being throttled to
    /// the sustained rate. Default 100.
    #[serde(default = "default_per_user_op_burst", rename = "per-user-op-burst")]
    pub per_user_op_burst: u64,

    /// Minimum seconds between hosted-export downloads per user (#4561). Export
    /// is expensive, so it gets a separate tighter limit; a request inside this
    /// window returns HTTP 429. `0` disables export rate limiting. Default 10.
    #[serde(
        default = "default_per_user_export_min_interval_secs",
        rename = "per-user-export-min-interval-secs"
    )]
    pub per_user_export_min_interval_secs: u64,

    /// Resolved secrets directory for this node. RUNTIME-ONLY, NOT persisted
    /// (`#[serde(skip)]`, like `TelemetryConfig::is_test_environment`): it is
    /// derived from the full `Config` in `build()` (`config.secrets_dir()`), so
    /// serializing/round-tripping a `WebsocketApiConfig` standalone leaves it
    /// empty and `build()` repopulates it.
    ///
    /// The WS serve layer injects it as an `Extension` so the per-user
    /// last-activity marker (#4561, P5 of #4381, inactive-user TTL) can be
    /// stamped at the same `<base>/users/<user_id>/.last_seen` location the
    /// reclaim sweep reads. Empty (the default on the standalone test paths)
    /// disables stamping, which is correct for non-hosted/test composition that
    /// has no secrets tree to mark.
    #[serde(skip)]
    pub secrets_dir: std::path::PathBuf,

    /// How this boot decided the client API's exposure. RUNTIME-ONLY
    /// (`#[serde(skip)]`, same reason as `secrets_dir` above): it describes a
    /// resolution, not operator-authored TOML.
    ///
    /// Populated by `ConfigArgs::build()` and reported by
    /// [`Config::log_client_api_exposure`]. It is carried rather than logged
    /// in place because `build()` runs before the tracing subscriber exists.
    #[serde(skip)]
    pub exposure: WsApiExposure,

    /// Directory holding unpacked web-contract bundles, derived in `build()`
    /// (see [`default_webapp_cache_dir`]). Runtime-only for the same reason as
    /// `secrets_dir` above: it is a resolved path, not operator-authored TOML.
    ///
    /// The HTTP layer builds its `WebappCache` from this and threads it to the
    /// web handlers. It is a config value rather than a process global on
    /// purpose — the cache is size-bounded by LRU EVICTION, so whoever owns the
    /// path owns a directory something will delete from. Two consequences:
    /// a node pointed at a temp data dir (every `#[freenet_test]` node) gets an
    /// isolated cache instead of sweeping the developer's real one, and two
    /// nodes run by the same user can be given separate caches instead of
    /// silently sharing one.
    ///
    /// # What isolation this actually guarantees
    ///
    /// Precisely: **every node built through [`ConfigArgs::build`] serves from
    /// the directory its config names, and every `#[freenet_test]` node has that
    /// config pointed at its own temp dir** (the harness assigns it; pinned by
    /// `every_node_isolates_its_webapp_cache` in `freenet-macros`). That covers
    /// production and every integration test that goes through the harness,
    /// including `tests/playwright_shell.rs`, the one test that actually fetches
    /// `/v1/contract/web/` and therefore the one that actually sweeps.
    ///
    /// It is NOT "no test can ever reach the real cache". Two standalone
    /// composition paths deliberately resolve [`default_webapp_cache_dir`]
    /// themselves, because both are real user-facing modes rather than test
    /// scaffolding:
    ///
    /// - `HttpClientApi::as_router`, the direct router-composition entry point.
    ///   Its signature is public API and takes no cache root.
    /// - `WebsocketApiConfig::default()` / `From<SocketAddr>`, the fallback for
    ///   any serving config not produced by `build()`.
    ///
    /// Leaving those resolved is the deliberate choice (see
    /// `standalone_websocket_api_config_resolves_the_real_webapp_cache_dir`):
    /// the alternative, an empty `PathBuf` matching `secrets_dir`, is benign
    /// only because *that* field's consumer reads empty as "stamping disabled".
    /// This field has no such consumer semantics, so an empty root would instead
    /// write cache entries under the process's working directory and skip the
    /// size sweep entirely (`read_dir("")` fails), i.e. trade a shared but
    /// bounded cache for an unbounded one somewhere unexpected. The residual
    /// risk is made audible instead: `WebappCache::with_root` logs the resolved
    /// root once at startup, so a composition that lands on the real user cache
    /// says so.
    ///
    /// So: a NEW test that composes a server or router directly and fetches a
    /// web contract must set this field (or use `#[freenet_test]`). Nothing
    /// stops it from not doing so, and it would then sweep the developer's real
    /// cache.
    #[serde(skip)]
    pub webapp_cache_dir: std::path::PathBuf,
}

/// Default directory for unpacked web-contract bundles.
///
/// The XDG cache dir (`~/.cache/freenet/webapp_cache` on Linux), which is where
/// this cache has always lived — deliberately unchanged, because relocating it
/// would strand every existing installation's directory with nothing left to
/// sweep it, which is the opposite of what the size bound is for.
///
/// `FREENET_WEBAPP_CACHE_DIR` overrides it. That exists for operators running
/// several nodes as one user: the cache is per-user by default but its eviction
/// guards are per-process, so pointing each node at its own directory is the
/// clean way to keep one node's sweep away from another's entries. Set but
/// EMPTY reads as unset (see `resolve_webapp_cache_dir`).
pub fn default_webapp_cache_dir() -> std::path::PathBuf {
    resolve_webapp_cache_dir(std::env::var_os(WEBAPP_CACHE_DIR_ENV))
}

/// Operator override for [`default_webapp_cache_dir`].
const WEBAPP_CACHE_DIR_ENV: &str = "FREENET_WEBAPP_CACHE_DIR";

/// [`default_webapp_cache_dir`] with the environment read hoisted into a
/// parameter, so the empty-value case is testable without mutating
/// process-global state.
///
/// An override that is set but EMPTY is treated as unset, deliberately.
/// `var_os` reports `FREENET_WEBAPP_CACHE_DIR=` as `Some("")` (an empty string
/// is a legitimate environment value, not an absent one), and taking that at
/// face value silently disables the very bound this directory now has: every
/// entry path derived from an empty root is RELATIVE, so the cache is written
/// under whatever directory the node happened to be started in, and the sweep's
/// `read_dir("")` fails with `ENOENT` so it evicts nothing and the cache grows
/// without limit again. Nothing is destroyed and nothing errors, so an operator
/// who exports the variable without a value (a stray `=`, an unset shell
/// variable expanded into it) gets an unbounded cache in an unexpected place
/// and no indication that anything is wrong. Falling back to the default and
/// saying so is the only outcome that is either correct or visible.
fn resolve_webapp_cache_dir(override_dir: Option<std::ffi::OsString>) -> std::path::PathBuf {
    match override_dir {
        Some(dir) if !dir.is_empty() => return std::path::PathBuf::from(dir),
        Some(_) => tracing::warn!(
            env = WEBAPP_CACHE_DIR_ENV,
            "webapp cache: override is set but empty; ignoring it and using the \
             default cache directory. An empty root would place the cache under \
             the node's working directory and disable its size bound entirely."
        ),
        None => {}
    }
    directories::ProjectDirs::from("", "The Freenet Project Inc", "freenet")
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir().join("freenet"))
        .join("webapp_cache")
}

#[inline]
const fn default_token_ttl_seconds() -> u64 {
    86400 // 24 hours
}

#[inline]
const fn default_token_cleanup_interval_seconds() -> u64 {
    300 // 5 minutes
}

/// Default sustained per-user op rate (req/sec) in hosted mode. Resolves to the
/// single source of truth in `client_events::user_op_rate_limit` so the
/// operator-facing default and the limiter's in-code default never drift.
#[inline]
const fn default_per_user_op_rate_limit() -> u64 {
    crate::client_events::user_op_rate_limit::DEFAULT_PER_USER_OP_RATE_LIMIT
}

/// Default per-user op burst capacity in hosted mode.
#[inline]
const fn default_per_user_op_burst() -> u64 {
    crate::client_events::user_op_rate_limit::DEFAULT_PER_USER_OP_BURST
}

/// Default minimum seconds between hosted exports per user.
#[inline]
const fn default_per_user_export_min_interval_secs() -> u64 {
    crate::client_events::user_op_rate_limit::DEFAULT_PER_USER_EXPORT_MIN_INTERVAL_SECS
}

impl From<SocketAddr> for WebsocketApiConfig {
    fn from(addr: SocketAddr) -> Self {
        Self {
            address: addr.ip(),
            port: addr.port(),
            token_ttl_seconds: default_token_ttl_seconds(),
            token_cleanup_interval_seconds: default_token_cleanup_interval_seconds(),
            allowed_hosts: Vec::new(),
            allowed_source_cidrs: Vec::new(),
            hosted_mode: false,
            per_user_op_rate_limit: default_per_user_op_rate_limit(),
            per_user_op_burst: default_per_user_op_burst(),
            per_user_export_min_interval_secs: default_per_user_export_min_interval_secs(),
            secrets_dir: std::path::PathBuf::new(),
            webapp_cache_dir: default_webapp_cache_dir(),
            exposure: WsApiExposure::default(),
        }
    }
}

impl Default for WebsocketApiConfig {
    #[inline]
    fn default() -> Self {
        Self {
            // Loopback, matching `resolve_ws_api_address`'s default: a caller
            // that composes this config without going through
            // `ConfigArgs::build()` gets the safe bind rather than silently
            // inheriting a wildcard listener (GHSA-824h-7x5x-wfmf).
            address: default_local_address(),
            port: default_ws_api_port(),
            token_ttl_seconds: default_token_ttl_seconds(),
            token_cleanup_interval_seconds: default_token_cleanup_interval_seconds(),
            allowed_hosts: Vec::new(),
            allowed_source_cidrs: Vec::new(),
            hosted_mode: false,
            per_user_op_rate_limit: default_per_user_op_rate_limit(),
            per_user_op_burst: default_per_user_op_burst(),
            per_user_export_min_interval_secs: default_per_user_export_min_interval_secs(),
            secrets_dir: std::path::PathBuf::new(),
            webapp_cache_dir: default_webapp_cache_dir(),
            exposure: WsApiExposure::default(),
        }
    }
}

/// Default listening address: `::` (IPv6 dual-stack, accepts IPv4 via mapped addresses).
#[inline]
const fn default_listening_address() -> IpAddr {
    IpAddr::V6(Ipv6Addr::UNSPECIFIED)
}

#[inline]
const fn default_local_address() -> IpAddr {
    IpAddr::V6(Ipv6Addr::LOCALHOST)
}

/// How [`resolve_ws_api_address`] arrived at the client-API bind address.
///
/// Carried on the resolved [`WebsocketApiConfig`] rather than logged inline,
/// because `build()` runs BEFORE the global tracing subscriber is installed.
/// See [`WsApiExposure`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WsApiAddressSource {
    /// The operator named an address (`--ws-api-address`, `FREENET_WS_API_ADDRESS`, or
    /// a `ws-api-address` key in `config.toml` that this code could not itself
    /// have written). Used verbatim.
    ///
    /// The default for a `WebsocketApiConfig` composed directly (tests, the
    /// standalone server paths): such a caller always names its own address.
    #[default]
    Explicit,
    /// No address given and nothing configured that only makes sense for a
    /// non-local listener: loopback (`::1`, plus its `127.0.0.1` companion).
    DefaultLoopback,
    /// No address given, but in NETWORK mode with `--allowed-source-cidrs`
    /// and/or `--allowed-host` set. Widened to the wildcard `::` so
    /// invocations that were relying on the old network-mode default keep
    /// working untouched.
    AutoWidened,
}

/// How this boot decided the client API's exposure, for reporting once logging
/// exists. RUNTIME-ONLY (`#[serde(skip)]`, like `WebsocketApiConfig::secrets_dir`):
/// it describes this boot's resolution, not operator-authored TOML.
///
/// This exists because `ConfigArgs::build()` runs BEFORE
/// [`set_logger`] installs the global subscriber (see `bin/freenet.rs`), so a
/// `tracing::warn!` inside `build()` is emitted with no subscriber and goes
/// nowhere. [`Config::log_client_api_exposure`] replays the decision after the
/// subscriber exists. Do NOT move these messages back into `build()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WsApiExposure {
    /// How the bind address was chosen.
    pub source: WsApiAddressSource,
    /// A `ws-api-address` this code could itself have auto-written, discarded
    /// from `config.toml` during the merge so it could be re-derived.
    ///
    /// `Some` only when the operator supplied no address by flag or env AND the
    /// re-derivation actually MOVED the bind. A value that re-derives to itself
    /// — the steady state from the second boot onward — is not recorded, because
    /// re-announcing an unchanged bind every boot is how a log line gets tuned
    /// out. Which direction it moved selects the message; see
    /// `Config::log_client_api_exposure`.
    pub dropped_persisted_address: Option<IpAddr>,
}

/// True for any address [`resolve_ws_api_address`] could itself have produced,
/// in this release or an earlier one: the two wildcards and the two loopbacks.
///
/// This is the migration sentinel. `build()` persists the RESOLVED config, so a
/// `ws-api-address` in `config.toml` is just as likely to be this code's own
/// past output as an operator's choice — and the two are indistinguishable by
/// value. Treating the auto-derivable values as "not an operator choice" is
/// what lets the resolution re-run each boot:
///
/// - `::` (the network-mode auto-default since #3648) and `0.0.0.0` (before it)
///   must be re-derived, or the hardening would reach fresh installs only;
/// - the loopbacks must be re-derived too, or the FIRST post-upgrade boot would
///   persist loopback and thereby make it look explicit, permanently disabling
///   the auto-widen remedy the release note and the startup log point at.
///
/// Enumerated exactly rather than tested with `is_loopback()`, so an operator
/// who deliberately pinned some other loopback address (`127.0.0.5`, a
/// per-service loopback alias) keeps it: this code never writes that, so it is
/// a choice.
fn is_auto_derivable_ws_api_address(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => v4 == Ipv4Addr::UNSPECIFIED || v4 == Ipv4Addr::LOCALHOST,
        IpAddr::V6(v6) => v6 == Ipv6Addr::UNSPECIFIED || v6 == Ipv6Addr::LOCALHOST,
    }
}

/// The loopback / wildcard address in the same family as `family_hint`.
///
/// Re-derivation must never cross address families. The primary bind is FATAL
/// while only the companion is best-effort (`server::serve_dual_stack`), so
/// handing `::1` to a host with IPv6 disabled fails with EAFNOSUPPORT and the
/// node does not start. That is unrecoverable rather than merely broken:
/// `build()` rewrites `config.toml` BEFORE the bind is attempted, and
/// `commands::rollback` does not snapshot config, so the crash-loop rollback
/// restores the previous binary onto a config that now names an unbindable
/// family and it dies identically — and rollback does not fire twice.
///
/// A node that persisted `0.0.0.0` (the pre-#3648 auto-default) has been
/// binding IPv4 successfully for its whole life; that is the only evidence
/// available about which families this host actually supports, so honour it.
/// With no hint (a fresh install) the IPv6 default stands, which is the same
/// exposure `main` already had — network mode defaulted to `::`.
fn ws_api_default_for_family(family_hint: Option<IpAddr>, wildcard: bool) -> IpAddr {
    match (family_hint, wildcard) {
        (Some(IpAddr::V4(_)), false) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        (Some(IpAddr::V4(_)), true) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        (_, false) => default_local_address(),
        (_, true) => default_listening_address(),
    }
}

/// Whether a `--allowed-source-cidrs` list grants anything.
///
/// An empty list grants nothing, and neither does a list of blank strings:
/// `FREENET_ALLOWED_SOURCE_CIDRS=` declared with no value in a docker-compose
/// `.env`, a k8s ConfigMap, or a systemd `Environment=` line parses as
/// `Some(vec![""])`, and reading that as "the operator wants non-local clients"
/// would widen the bind on a node whose operator granted nothing.
fn grants_anything(list: Option<&[String]>) -> bool {
    list.is_some_and(|entries| entries.iter().any(|e| !e.trim().is_empty()))
}

/// Resolve the address the client (HTTP/WebSocket) API binds to.
///
/// The default is **loopback in both operation modes**. `OperationMode` still
/// governs ring participation and `secrets_dir(mode)`; it deliberately does NOT
/// govern who may drive the client API. Running as a network peer is a
/// statement about the overlay, not consent to expose a fully-privileged
/// control API (contract state, delegate secrets, key material) to every host
/// that can route to this machine.
///
/// Auto-widening is a BACKWARD-COMPATIBILITY measure for one flag only.
/// `--allowed-source-cidrs` is genuinely inert on a loopback socket — a
/// non-private source can never reach `::1`, so the filter it relaxes never
/// runs — which means an operator who set it and nothing else was relying on
/// the old wide network-mode default. Widening preserves that with no action.
///
/// `--allowed-host` is deliberately NOT a trigger, though an earlier cut of
/// this change made it one. It is a Host-header allowlist that is fully
/// functional on loopback, and the same-host reverse proxy is its primary
/// documented use — indeed the ONLY shape in which hosted mode's `userToken` is
/// honoured at all, since `decide_user_token` requires a loopback source. So
/// widening for it would bind every interface, for no functional gain, in the
/// commonest proxy deployment there is. A proxy on a DIFFERENT host is a
/// deliberate multi-machine choice whose operator passes `--ws-api-address`;
/// the startup log says so.
///
/// `cidrs_granted_this_boot` must come from the CLI/env values captured BEFORE
/// the `config.toml` merge. Reading the merged value would make the widen
/// sticky: one boot with the flag would pin the node to the wildcard forever,
/// and removing the flag would never narrow it back.
///
/// Widening is scoped to `OperationMode::Network` because local mode has always
/// bound loopback, so there is no wide default to preserve and widening there
/// would mean this hardening OPENED a socket that used to be closed. An
/// explicit `--ws-api-address` always wins in either mode.
fn resolve_ws_api_address(
    mode: OperationMode,
    explicit: Option<IpAddr>,
    cidrs_granted_this_boot: Option<&[String]>,
    family_hint: Option<IpAddr>,
) -> (IpAddr, WsApiAddressSource) {
    if let Some(addr) = explicit {
        return (addr, WsApiAddressSource::Explicit);
    }
    let compat_widen =
        matches!(mode, OperationMode::Network) && grants_anything(cidrs_granted_this_boot);
    if compat_widen {
        (
            ws_api_default_for_family(family_hint, true),
            WsApiAddressSource::AutoWidened,
        )
    } else {
        (
            ws_api_default_for_family(family_hint, false),
            WsApiAddressSource::DefaultLoopback,
        )
    }
}

/// Whether startup should warn that the client API is reachable beyond this
/// machine while connections can land in ONE shared secret namespace, and if
/// so, why. `None` means stay quiet.
///
/// The shared namespace is the danger. `decide_user_token` hands a connection
/// the node's single-user context whenever hosted mode is off **or the
/// connection simply omits `userToken`** — so hosted mode ADDS a per-user
/// namespace for well-behaved clients, it does not remove the shared one.
///
/// Two triggers:
/// - a **non-loopback bind** warns regardless of hosted mode, because any host
///   that can route to the address can omit a token and land in the shared
///   namespace;
/// - **`--allowed-host` on a loopback bind with hosted mode OFF** warns. That
///   flag names a reverse proxy, and a proxy terminates the connection itself,
///   so every visitor arrives wearing the proxy's source address and the node's
///   own source-IP filters cannot tell them apart.
///
/// **Loopback + hosted mode stays quiet, and that is a known gap, not an
/// assertion of safety.** A connection that omits `userToken` reads the shared
/// namespace there too, and behind a public proxy that is any visitor;
/// containment on the flagship deployment today is that the shared namespace is
/// empty (measured: zero files outside `*/users/*`), which is a fact about
/// current state rather than a structural guarantee. Making this branch fire
/// only when the shared namespace actually holds something needs a probe of the
/// secrets tree, which is a separate change with its own failure modes — it is
/// tracked in advisory GHSA-824h-7x5x-wfmf §8, with the measured tree shape and
/// the requirements, rather than bolted onto a default-hardening PR, because a
/// probe that is wrong in either direction is worse than no warning: too eager
/// and it fires on every boot of the flagship and trains operators to ignore the
/// one signal there is.
fn ws_api_shares_one_namespace_with_remote_clients(
    hosted_mode: bool,
    address: IpAddr,
    allowed_hosts: &[String],
) -> Option<&'static str> {
    if !address.is_loopback() {
        return Some(if hosted_mode {
            "the client API is bound to a non-loopback address, and hosted mode does \
             not close that: a connection which simply omits `userToken` still lands \
             in this node's shared single-user namespace"
        } else {
            "the client API is bound to a non-loopback address, so any host that can \
             route to it can drive this node"
        });
    }
    if hosted_mode {
        return None;
    }
    if !allowed_hosts.is_empty() {
        return Some(
            "--allowed-host names a reverse proxy in front of this node, and a proxy \
             terminates the connection itself, so every visitor arrives looking local \
             and the source-IP filters cannot tell them apart",
        );
    }
    None
}

#[inline]
const fn default_ws_api_port() -> u16 {
    7509
}

#[derive(clap::Parser, Default, Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPathsArgs {
    /// The configuration directory.
    #[arg(long, default_value = None, env = "CONFIG_DIR")]
    pub config_dir: Option<PathBuf>,
    /// The data directory.
    #[arg(long, default_value = None, env = "DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    /// The log directory.
    #[arg(long, default_value = None, env = "LOG_DIR")]
    pub log_dir: Option<PathBuf>,
}

impl ConfigPathsArgs {
    fn merge(&mut self, other: ConfigPaths) {
        self.config_dir.get_or_insert(other.config_dir);
        self.data_dir.get_or_insert(other.data_dir);
        self.log_dir = self.log_dir.take().or(other.log_dir);
    }

    fn default_dirs(id: Option<&str>) -> std::io::Result<Either<ProjectDirs, PathBuf>> {
        // if id is set, most likely we are running tests or in simulated mode
        let default_dir: Either<_, _> = if cfg!(any(test, debug_assertions)) || id.is_some() {
            let base_name = if let Some(id) = id {
                format!("freenet-{id}")
            } else {
                "freenet".into()
            };
            let temp_path = std::env::temp_dir().join(&base_name);

            // Clean up stale temp directories from previous test runs that may have
            // different permissions (common on shared CI runners). If we can't remove
            // the stale directory (permission denied, in use, etc.), use a unique
            // fallback path with process ID to avoid conflicts.
            if temp_path.exists() && fs::remove_dir_all(&temp_path).is_err() {
                let unique_path =
                    std::env::temp_dir().join(format!("{}-{}", base_name, std::process::id()));
                // Clean up any stale unique path too (unlikely but possible)
                let _cleanup = fs::remove_dir_all(&unique_path);
                return Ok(Either::Right(unique_path));
            }
            Either::Right(temp_path)
        } else {
            Either::Left(
                ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
                    .ok_or(std::io::ErrorKind::NotFound)?,
            )
        };
        Ok(default_dir)
    }

    pub fn build(self, id: Option<&str>) -> std::io::Result<ConfigPaths> {
        // Used by the Windows migration block below; suppress warning on other platforms.
        #[allow(unused_variables)]
        let has_custom_data_dir = self.data_dir.is_some();
        let app_data_dir = self
            .data_dir
            .map(Ok::<_, std::io::Error>)
            .unwrap_or_else(|| {
                let default_dirs = Self::default_dirs(id)?;
                let Either::Left(defaults) = default_dirs else {
                    unreachable!("default_dirs should return Left if data_dir is None and id is not set for temp dir")
                };
                // Use data_local_dir (Local AppData on Windows) instead of
                // data_dir (Roaming AppData). Roaming syncs across domain-joined
                // machines and is not appropriate for node data (contracts, DB).
                // See #3739.
                Ok(defaults.data_local_dir().to_path_buf())
            })?;
        // Migrate data from old Roaming path to new Local path on Windows.
        // Before #3739, data was stored in %APPDATA% (Roaming) by mistake.
        // If the old path has data and the new path doesn't, move it.
        #[cfg(target_os = "windows")]
        if !has_custom_data_dir && id.is_none() {
            if let Ok(Either::Left(ref proj)) = Self::default_dirs(None) {
                let old_roaming = proj.data_dir().to_path_buf();
                if old_roaming != app_data_dir
                    && old_roaming.join("contracts").exists()
                    && !app_data_dir.join("contracts").exists()
                {
                    tracing::info!(
                        old = ?old_roaming,
                        new = ?app_data_dir,
                        "Migrating data from Roaming to Local AppData"
                    );
                    // Ensure the parent directory exists before rename.
                    // On a fresh Local AppData install, the intermediate dirs
                    // (e.g., "The Freenet Project Inc/Freenet") won't exist yet.
                    if let Some(parent) = app_data_dir.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    if let Err(e) = fs::rename(&old_roaming, &app_data_dir) {
                        tracing::warn!(
                            error = %e,
                            "Failed to migrate data directory; starting fresh"
                        );
                        // rename can fail across drives; a fresh start is fine
                        // since the node will re-fetch contracts from the network.
                    }
                }
            }
        }

        let contracts_dir = app_data_dir.join("contracts");
        let delegates_dir = app_data_dir.join("delegates");
        let secrets_dir = app_data_dir.join("secrets");
        let db_dir = app_data_dir.join("db");
        // Wasmtime's compile cache is relocated onto the data-dir mount (#4683)
        // so it (a) shares the mount whose free space sizes the disk budget and
        // (b) is measurable as freenet's own on-disk usage. Its default OS-cache
        // location is neither. A sibling of the data dirs, created below.
        let wasmtime_cache_dir = app_data_dir.join("wasmtime-cache");

        if !contracts_dir.exists() {
            fs::create_dir_all(&contracts_dir)?;
            fs::create_dir_all(contracts_dir.join("local"))?;
        }

        if !delegates_dir.exists() {
            fs::create_dir_all(&delegates_dir)?;
            fs::create_dir_all(delegates_dir.join("local"))?;
        }

        if !secrets_dir.exists() {
            fs::create_dir_all(&secrets_dir)?;
            fs::create_dir_all(secrets_dir.join("local"))?;
        }

        if !db_dir.exists() {
            fs::create_dir_all(&db_dir)?;
            fs::create_dir_all(db_dir.join("local"))?;
        }

        if !wasmtime_cache_dir.exists() {
            fs::create_dir_all(&wasmtime_cache_dir)?;
        }

        let event_log = app_data_dir.join("_EVENT_LOG");
        if !event_log.exists() {
            fs::write(&event_log, [])?;
            let mut local_file = event_log.clone();
            local_file.set_file_name("_EVENT_LOG_LOCAL");
            fs::write(local_file, [])?;
        }

        let config_dir = self
            .config_dir
            .map(Ok::<_, std::io::Error>)
            .unwrap_or_else(|| {
                let default_dirs = Self::default_dirs(id)?;
                let Either::Left(defaults) = default_dirs else {
                    unreachable!("default_dirs should return Left if config_dir is None and id is not set for temp dir")
                };
                Ok(defaults.config_dir().to_path_buf())
            })?;

        let log_dir = self.log_dir.or_else(get_log_dir);

        Ok(ConfigPaths {
            config_dir,
            data_dir: app_data_dir,
            contracts_dir,
            delegates_dir,
            secrets_dir,
            db_dir,
            wasmtime_cache_dir,
            event_log,
            log_dir,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPaths {
    #[serde(alias = "contracts-dir")]
    contracts_dir: PathBuf,
    #[serde(alias = "delegates-dir")]
    delegates_dir: PathBuf,
    #[serde(alias = "secrets-dir")]
    secrets_dir: PathBuf,
    #[serde(alias = "db-dir")]
    db_dir: PathBuf,
    #[serde(alias = "event-log")]
    event_log: PathBuf,
    #[serde(alias = "data-dir")]
    data_dir: PathBuf,
    #[serde(alias = "config-dir")]
    config_dir: PathBuf,
    #[serde(default = "get_log_dir", alias = "log-dir")]
    log_dir: Option<PathBuf>,
    /// Relocated wasmtime compile-cache directory (#4683). `#[serde(default)]`
    /// so a `config.toml` persisted before this field existed deserializes with
    /// an empty path; `build()` always re-derives the real one from the data
    /// dir, so the persisted value is never load-bearing.
    #[serde(default, alias = "wasmtime-cache-dir")]
    wasmtime_cache_dir: PathBuf,
}

impl ConfigPaths {
    pub fn db_dir(&self, mode: OperationMode) -> PathBuf {
        match mode {
            OperationMode::Local => self.db_dir.join("local"),
            OperationMode::Network => self.db_dir.to_owned(),
        }
    }

    pub fn with_db_dir(mut self, db_dir: PathBuf) -> Self {
        self.db_dir = db_dir;
        self
    }

    pub fn contracts_dir(&self, mode: OperationMode) -> PathBuf {
        match mode {
            OperationMode::Local => self.contracts_dir.join("local"),
            OperationMode::Network => self.contracts_dir.to_owned(),
        }
    }

    pub fn with_contract_dir(mut self, contracts_dir: PathBuf) -> Self {
        self.contracts_dir = contracts_dir;
        self
    }

    pub fn delegates_dir(&self, mode: OperationMode) -> PathBuf {
        match mode {
            OperationMode::Local => self.delegates_dir.join("local"),
            OperationMode::Network => self.delegates_dir.to_owned(),
        }
    }

    pub fn with_delegates_dir(mut self, delegates_dir: PathBuf) -> Self {
        self.delegates_dir = delegates_dir;
        self
    }

    pub fn config_dir(&self) -> PathBuf {
        self.config_dir.clone()
    }

    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.clone()
    }

    /// Relocated wasmtime compile-cache directory (#4683). Not mode-split: the
    /// compile cache is keyed by (engine config + WASM bytes) and shared across
    /// local/network runtimes on the same node.
    pub fn wasmtime_cache_dir(&self) -> PathBuf {
        self.wasmtime_cache_dir.clone()
    }

    pub fn secrets_dir(&self, mode: OperationMode) -> PathBuf {
        match mode {
            OperationMode::Local => self.secrets_dir.join("local"),
            OperationMode::Network => self.secrets_dir.to_owned(),
        }
    }

    pub fn with_secrets_dir(mut self, secrets_dir: PathBuf) -> Self {
        self.secrets_dir = secrets_dir;
        self
    }

    pub fn event_log(&self, mode: OperationMode) -> PathBuf {
        match mode {
            OperationMode::Local => {
                let mut local_file = self.event_log.clone();
                local_file.set_file_name("_EVENT_LOG_LOCAL");
                local_file
            }
            OperationMode::Network => self.event_log.to_owned(),
        }
    }

    pub fn log_dir(&self) -> Option<&Path> {
        self.log_dir.as_deref()
    }

    pub fn with_event_log(mut self, event_log: PathBuf) -> Self {
        self.event_log = event_log;
        self
    }

    pub fn iter(&self) -> ConfigPathsIter<'_> {
        ConfigPathsIter {
            curr: 0,
            config_paths: self,
        }
    }

    fn path_by_index(&self, index: usize) -> (bool, &PathBuf) {
        match index {
            0 => (true, &self.contracts_dir),
            1 => (true, &self.delegates_dir),
            2 => (true, &self.secrets_dir),
            3 => (true, &self.db_dir),
            4 => (true, &self.data_dir),
            5 => (false, &self.event_log),
            6 => (true, &self.config_dir),
            7 => (true, &self.wasmtime_cache_dir),
            _ => panic!("invalid path index"),
        }
    }

    const MAX_PATH_INDEX: usize = 7;
}

pub struct ConfigPathsIter<'a> {
    curr: usize,
    config_paths: &'a ConfigPaths,
}

impl<'a> Iterator for ConfigPathsIter<'a> {
    /// The first is whether this path is a directory or a file.
    type Item = (bool, &'a PathBuf);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr > ConfigPaths::MAX_PATH_INDEX {
            None
        } else {
            let path = self.config_paths.path_by_index(self.curr);
            self.curr += 1;
            Some(path)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(ConfigPaths::MAX_PATH_INDEX))
    }
}

impl core::iter::FusedIterator for ConfigPathsIter<'_> {}

impl Config {
    pub fn db_dir(&self) -> PathBuf {
        self.config_paths.db_dir(self.mode)
    }

    pub fn contracts_dir(&self) -> PathBuf {
        self.config_paths.contracts_dir(self.mode)
    }

    /// Relocated wasmtime compile-cache directory (#4683). Not mode-split.
    pub fn wasmtime_cache_dir(&self) -> PathBuf {
        self.config_paths.wasmtime_cache_dir()
    }

    pub fn delegates_dir(&self) -> PathBuf {
        self.config_paths.delegates_dir(self.mode)
    }

    pub fn secrets_dir(&self) -> PathBuf {
        self.config_paths.secrets_dir(self.mode)
    }

    pub fn event_log(&self) -> PathBuf {
        self.config_paths.event_log(self.mode)
    }

    /// Whether this node should write the local append-only diagnostic event
    /// log at [`Self::event_log`].
    ///
    /// Resolves the mode-dependent default: ON in `local` mode (a single-node
    /// dev mode where the log is the point, and where `fdev verify-state`
    /// consumes `_EVENT_LOG_LOCAL`), OFF in `network` mode (what end users
    /// run). An explicit `--enable-event-log` flag, `FREENET_ENABLE_EVENT_LOG`
    /// env var, or `enable-event-log` config key always wins.
    ///
    /// This does NOT gate the telemetry that feeds telemetry.freenet.org —
    /// that is a separate `TelemetryReporter` sink fed in-memory off the same
    /// event stream (#4968).
    pub fn event_log_enabled(&self) -> bool {
        self.enable_event_log
            .unwrap_or(matches!(self.mode, OperationMode::Local))
    }

    pub fn config_dir(&self) -> PathBuf {
        self.config_paths.config_dir()
    }

    pub fn data_dir(&self) -> PathBuf {
        self.config_paths.data_dir()
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Gateways {
    pub gateways: Vec<GatewayConfig>,
}

impl Gateways {
    /// Merges `other` into `self`, deduplicating by address. On collision, `self`'s
    /// entry takes precedence. Preserves insertion order (`self` entries first).
    pub fn merge_and_deduplicate(&mut self, other: Gateways) {
        let mut seen: HashSet<Address> = HashSet::new();
        let mut merged = Vec::with_capacity(self.gateways.len() + other.gateways.len());
        for gw in self.gateways.drain(..).chain(other.gateways) {
            if seen.insert(gw.address.clone()) {
                merged.push(gw);
            }
        }
        self.gateways = merged;
    }

    pub fn save_to_file(&self, path: &Path) -> anyhow::Result<()> {
        // Ensure parent directory exists (fixes Windows first-run where config dir may not exist)
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = toml::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

/// Gateway addresses in `local` (the on-disk `gateways.toml` cache) absent
/// from `remote` (the freshly fetched index) — the entries the remote-index
/// replacement is about to drop. Surfaced as a warning so an operator-pinned
/// `--gateway` peer is never discarded silently (#4275).
fn gateways_dropped_by_remote_replace(
    local: &[GatewayConfig],
    remote: &[GatewayConfig],
) -> Vec<Address> {
    let remote_addrs: HashSet<&Address> = remote.iter().map(|g| &g.address).collect();
    local
        .iter()
        .filter(|g| !remote_addrs.contains(&g.address))
        .map(|g| g.address.clone())
        .collect()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GatewayConfig {
    /// Address of the gateway. It can be either a hostname or an IP address and port.
    pub address: Address,

    /// Path to the public key of the gateway (hex-encoded X25519 key).
    #[serde(rename = "public_key", alias = "public-key")]
    pub public_key_path: PathBuf,

    /// Optional location of the gateway.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<f64>,
}

impl PartialEq for GatewayConfig {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl Eq for GatewayConfig {}

impl std::hash::Hash for GatewayConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

/// A gateway address as it appears in `gateways.toml`.
///
/// # On-disk formats (all accepted on deserialize, see [`Address`]'s
/// `Deserialize` impl)
///
/// New, preferred form — host and port as separate fields, port optional and
/// defaulting to [`DEFAULT_GATEWAY_PORT`]:
///
/// ```toml
/// [gateways.address]
/// host = "vega.locut.us"
/// port = 31337            # optional; defaults to 31337 when omitted
/// ```
///
/// Legacy forms (still parsed so existing deployments keep working):
///
/// ```toml
/// [gateways.address]
/// hostname = "vega.locut.us:31337"   # host[:port] packed into one string
/// ```
///
/// ```toml
/// [gateways.address]
/// host_address = "203.0.113.1:31337" # a fully-resolved socket address
/// ```
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Address {
    /// Separate host and port. This is the canonical form emitted on serialize.
    ///
    /// `port` is always populated (defaulted to [`DEFAULT_GATEWAY_PORT`] when
    /// omitted on the wire) so the serialized form is unambiguous and
    /// round-trips.
    Host { host: String, port: u16 },
    /// Legacy: host with an optional `:port` suffix packed into one string.
    Hostname(String),
    /// Legacy: a fully-resolved socket address.
    HostAddress(SocketAddr),
}

// Custom `Serialize` emits each variant as a *flat* table so the on-disk form
// is symmetric with `Deserialize` (below) and matches the legacy wire format
// exactly (e.g. `hostname = "..."`). The derived enum `Serialize` would instead
// nest the struct variant under its own key (`[address.host]`), which neither
// the deserializer nor old binaries expect.
impl Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            Address::Host { host, port } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("host", host)?;
                map.serialize_entry("port", port)?;
                map.end()
            }
            Address::Hostname(hostname) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("hostname", hostname)?;
                map.end()
            }
            Address::HostAddress(addr) => {
                let mut map = serializer.serialize_map(Some(1))?;
                // SocketAddr serializes as its string form ("ip:port") here,
                // matching the legacy `host_address = "..."` representation.
                map.serialize_entry("host_address", &addr.to_string())?;
                map.end()
            }
        }
    }
}

// Custom `Deserialize` so a single `Address` table can be one of three shapes:
//   { host = "...", port = N? }  (new)
//   { hostname = "host[:port]" } (legacy)
//   { host_address = "ip:port" } (legacy)
//
// We deserialize into an intermediate that captures whichever key is present,
// then validate that exactly one address form was supplied. A hand-written
// impl (rather than `#[serde(untagged)]`) keeps the error messages precise and
// lets `port` default to `DEFAULT_GATEWAY_PORT` for the new `host` form.
impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct AddressRepr {
            host: Option<String>,
            port: Option<u16>,
            hostname: Option<String>,
            #[serde(alias = "host-address")]
            host_address: Option<SocketAddr>,
        }

        let repr = AddressRepr::deserialize(deserializer)?;

        // `port` is only meaningful alongside `host`.
        if repr.port.is_some() && repr.host.is_none() {
            return Err(serde::de::Error::custom(
                "gateway address `port` is only valid together with `host`; \
                 for the legacy single-string form put the port inside `hostname` \
                 (e.g. hostname = \"example.com:31337\")",
            ));
        }

        match (repr.host, repr.hostname, repr.host_address) {
            (Some(host), None, None) => Ok(Address::Host {
                host,
                port: repr.port.unwrap_or(DEFAULT_GATEWAY_PORT),
            }),
            (None, Some(hostname), None) => Ok(Address::Hostname(hostname)),
            (None, None, Some(addr)) => Ok(Address::HostAddress(addr)),
            (None, None, None) => Err(serde::de::Error::custom(
                "gateway address must specify one of `host`, `hostname`, or `host_address`",
            )),
            _ => Err(serde::de::Error::custom(
                "gateway address must specify exactly one of `host`, `hostname`, or `host_address`",
            )),
        }
    }
}

/// Global async executor abstraction for spawning tasks.
///
/// This abstraction allows swapping the underlying executor for deterministic
/// simulation testing. In production, it delegates to tokio. For deterministic
/// simulation, use Turmoil which provides deterministic task scheduling.
///
/// # Usage
/// ```ignore
/// use freenet::config::GlobalExecutor;
/// GlobalExecutor::spawn(async { /* task */ });
/// ```
pub struct GlobalExecutor;

impl GlobalExecutor {
    /// Returns the runtime handle if it was initialized or none if it was already
    /// running on the background.
    pub(crate) fn initialize_async_rt() -> Option<Runtime> {
        if tokio::runtime::Handle::try_current().is_ok() {
            tracing::debug!(target: "freenet::diagnostics::thread_explosion", "GlobalExecutor: runtime exists");
            None
        } else {
            tracing::warn!(target: "freenet::diagnostics::thread_explosion", "GlobalExecutor: Creating fallback runtime");
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all().thread_name("freenet-node");
            if cfg!(debug_assertions) {
                builder.worker_threads(2).max_blocking_threads(2);
            }
            Some(builder.build().expect("failed to build tokio runtime"))
        }
    }

    #[inline]
    pub fn spawn<R: Send + 'static>(
        f: impl Future<Output = R> + Send + 'static,
    ) -> tokio::task::JoinHandle<R> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(f)
        } else if let Some(rt) = &*ASYNC_RT {
            tracing::warn!(target: "freenet::diagnostics::thread_explosion", "GlobalExecutor::spawn using fallback");
            rt.spawn(f)
        } else {
            unreachable!("ASYNC_RT should be initialized if Handle::try_current fails")
        }
    }
}

// =============================================================================
// GlobalRng - Deterministic RNG abstraction for simulation testing
// =============================================================================

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};

static THREAD_INDEX_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

std::thread_local! {
    static THREAD_RNG: std::cell::RefCell<Option<SmallRng>> = const { std::cell::RefCell::new(None) };
    static THREAD_INDEX: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static THREAD_SEED: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Global RNG abstraction for deterministic simulation testing.
///
/// In production mode (no seed set), this delegates to the system RNG.
/// In simulation mode (seed set via `set_seed`), this uses a deterministic
/// seeded RNG that produces reproducible results.
///
/// # Test Isolation
///
/// For test isolation, prefer `scoped_seed()` or `SeedGuard` over `set_seed()`:
///
/// ```ignore
/// use freenet::config::GlobalRng;
///
/// // Option 1: Scoped seed (recommended for tests)
/// // Automatically clears seed when closure returns
/// GlobalRng::scoped_seed(0xDEADBEEF, || {
///     let value = GlobalRng::random_range(0..100); // Deterministic
/// });
/// // Seed automatically cleared here
///
/// // Option 2: RAII guard (for complex control flow)
/// {
///     let _guard = GlobalRng::seed_guard(0xDEADBEEF);
///     let value = GlobalRng::random_range(0..100); // Deterministic
/// } // Seed automatically cleared when guard drops
///
/// // Option 3: Manual set/clear (use with caution)
/// GlobalRng::set_seed(0xDEADBEEF);
/// // ... operations ...
/// GlobalRng::clear_seed(); // Don't forget this!
/// ```
pub struct GlobalRng;

/// RAII guard that clears the GlobalRng seed when dropped.
///
/// This ensures test isolation by automatically restoring the RNG to
/// production mode (system randomness) when the guard goes out of scope,
/// even if the test panics.
///
/// # Example
/// ```ignore
/// use freenet::config::GlobalRng;
///
/// #[test]
/// fn my_deterministic_test() {
///     let _guard = GlobalRng::seed_guard(12345);
///     // All RNG operations are now deterministic
///     assert_eq!(GlobalRng::random_range(0..100), 42); // Always same value
/// } // Guard drops here, seed is cleared
/// ```
pub struct SeedGuard {
    // Private field prevents external construction
    _private: (),
}

impl Drop for SeedGuard {
    fn drop(&mut self) {
        GlobalRng::clear_seed();
    }
}

impl GlobalRng {
    /// Sets the thread-local seed for deterministic RNG.
    ///
    /// **Warning:** For test isolation, prefer `scoped_seed()` or `seed_guard()`
    /// which automatically clean up the seed state.
    ///
    /// Call this at test/simulation startup for reproducibility.
    /// Must call `clear_seed()` when done to avoid affecting other tests.
    ///
    /// This is purely thread-local — parallel tests on different threads are fully isolated.
    pub fn set_seed(seed: u64) {
        THREAD_SEED.with(|s| s.set(Some(seed)));
        THREAD_RNG.with(|rng| {
            *rng.borrow_mut() = None;
        });
        // Pin thread index to 0 so the derived RNG seed is deterministic
        // regardless of which OS thread runs this test (see #2733).
        THREAD_INDEX.with(|idx| idx.set(Some(0)));
    }

    /// Clears the simulation seed, reverting to system RNG.
    pub fn clear_seed() {
        THREAD_SEED.with(|s| s.set(None));
        THREAD_RNG.with(|rng| {
            *rng.borrow_mut() = None;
        });
        THREAD_INDEX.with(|idx| idx.set(None));
    }

    /// Returns the deterministic thread index for the current thread.
    ///
    /// Each thread gets a unique index from the global `THREAD_INDEX_COUNTER`.
    /// This is used by thread-local ID counters to compute non-overlapping offset blocks.
    pub fn thread_index() -> u64 {
        THREAD_INDEX.with(|c| match c.get() {
            Some(idx) => idx,
            None => {
                let idx = THREAD_INDEX_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                c.set(Some(idx));
                idx
            }
        })
    }

    /// Returns true if a simulation seed is set for the current thread.
    pub fn is_seeded() -> bool {
        THREAD_SEED.with(|s| s.get()).is_some()
    }

    /// Creates a RAII guard that sets the seed and clears it on drop.
    ///
    /// This is the recommended way to use deterministic RNG in tests,
    /// as it guarantees cleanup even if the test panics.
    ///
    /// # Example
    /// ```ignore
    /// let _guard = GlobalRng::seed_guard(12345);
    /// // All operations here use seeded RNG
    /// let x = GlobalRng::random_range(0..100);
    /// // Guard drops at end of scope, seed cleared automatically
    /// ```
    pub fn seed_guard(seed: u64) -> SeedGuard {
        Self::set_seed(seed);
        SeedGuard { _private: () }
    }

    /// Executes a closure with a seeded RNG, then clears the seed.
    ///
    /// This is the safest way to use deterministic RNG in tests:
    /// - The seed is automatically cleared when the closure returns
    /// - Works correctly even if the closure panics (uses catch_unwind internally)
    ///
    /// # Example
    /// ```ignore
    /// let result = GlobalRng::scoped_seed(12345, || {
    ///     // Deterministic operations
    ///     GlobalRng::random_range(0..100)
    /// });
    /// // Seed is cleared here, regardless of success or panic
    /// ```
    pub fn scoped_seed<F, R>(seed: u64, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = Self::seed_guard(seed);
        f()
    }

    /// Executes a closure with access to the RNG.
    /// Uses seeded RNG if set via `set_seed()`, otherwise system RNG.
    #[inline]
    pub fn with_rng<F, R>(f: F) -> R
    where
        F: FnOnce(&mut dyn RngCore) -> R,
    {
        // Thread-local seed only — no global fallback. This ensures parallel tests
        // on different threads are fully isolated.
        let seed = THREAD_SEED.with(|s| s.get());

        if let Some(seed) = seed {
            // Simulation mode: use thread-local seeded RNG
            THREAD_RNG.with(|rng_cell| {
                let mut rng_ref = rng_cell.borrow_mut();
                if rng_ref.is_none() {
                    let thread_seed =
                        seed.wrapping_add(Self::thread_index().wrapping_mul(0x9E3779B97F4A7C15));
                    *rng_ref = Some(SmallRng::seed_from_u64(thread_seed));
                }
                f(rng_ref.as_mut().unwrap())
            })
        } else {
            // Production mode: use system RNG
            f(&mut rand::rng())
        }
    }

    /// Generate a random value in the given range.
    #[inline]
    pub fn random_range<T, R>(range: R) -> T
    where
        T: rand::distr::uniform::SampleUniform,
        R: rand::distr::uniform::SampleRange<T>,
    {
        Self::with_rng(|rng| rng.random_range(range))
    }

    /// Generate a random boolean with the given probability of being true.
    #[inline]
    pub fn random_bool(probability: f64) -> bool {
        Self::with_rng(|rng| rng.random_bool(probability))
    }

    /// Choose a random element from a slice.
    #[inline]
    pub fn choose<T>(slice: &[T]) -> Option<&T> {
        if slice.is_empty() {
            None
        } else {
            let idx = Self::random_range(0..slice.len());
            Some(&slice[idx])
        }
    }

    /// Shuffle a slice in place.
    #[inline]
    pub fn shuffle<T>(slice: &mut [T]) {
        Self::with_rng(|rng| {
            use rand::seq::SliceRandom;
            slice.shuffle(rng);
        })
    }

    /// Fill a byte slice with random data.
    #[inline]
    pub fn fill_bytes(dest: &mut [u8]) {
        Self::with_rng(|rng| rng.fill_bytes(dest))
    }

    /// Generate a random u64.
    #[inline]
    pub fn random_u64() -> u64 {
        Self::with_rng(|rng| rng.random())
    }

    /// Generate a random u32.
    #[inline]
    pub fn random_u32() -> u32 {
        Self::with_rng(|rng| rng.random())
    }
}

// =============================================================================
// Global Simulation Time
// =============================================================================

// Thread-local simulation time: allows parallel simulation tests without interference.
std::thread_local! {
    static SIMULATION_TIME_MS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static SIMULATION_TIME_COUNTER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Global simulation time configuration for deterministic testing.
///
/// In production mode (no simulation time set), ULID generation uses real system time.
/// In simulation mode, a configurable base time is used, ensuring reproducible transaction IDs.
///
/// # Usage
///
/// ```ignore
/// use freenet::config::GlobalSimulationTime;
///
/// // Set simulation time to a known epoch
/// GlobalSimulationTime::set_time_ms(1704067200000); // 2024-01-01 00:00:00 UTC
///
/// // All ULIDs generated after this use simulation time
/// let tx = Transaction::new::<SomeOp>();
///
/// // Clear when done
/// GlobalSimulationTime::clear_time();
/// ```
pub struct GlobalSimulationTime;

impl GlobalSimulationTime {
    /// Sets the simulation time base in milliseconds since Unix epoch (thread-local).
    ///
    /// All subsequent ULID generations on this thread will use this time (with auto-increment).
    pub fn set_time_ms(time_ms: u64) {
        SIMULATION_TIME_MS.with(|t| t.set(Some(time_ms)));
        SIMULATION_TIME_COUNTER.with(|c| c.set(0));
    }

    /// Clears the simulation time, reverting to system time (thread-local).
    pub fn clear_time() {
        SIMULATION_TIME_MS.with(|t| t.set(None));
        SIMULATION_TIME_COUNTER.with(|c| c.set(0));
    }

    /// Returns the current time in milliseconds for ULID generation.
    ///
    /// If simulation time is set, returns simulation time + counter increment.
    /// Otherwise, returns real system time.
    pub fn current_time_ms() -> u64 {
        SIMULATION_TIME_MS.with(|t| {
            if let Some(base_time) = t.get() {
                let counter = SIMULATION_TIME_COUNTER.with(|c| {
                    let val = c.get();
                    c.set(val + 1);
                    val
                });
                base_time.saturating_add(counter)
            } else {
                use std::time::{SystemTime, UNIX_EPOCH};
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time before unix epoch")
                    .as_millis() as u64
            }
        })
    }

    /// Returns the current time in milliseconds WITHOUT incrementing the counter.
    ///
    /// Use this for read-only time checks like elapsed time calculations.
    /// For ULID generation, use `current_time_ms()` which ensures uniqueness.
    pub fn read_time_ms() -> u64 {
        SIMULATION_TIME_MS.with(|t| {
            if let Some(base_time) = t.get() {
                let counter = SIMULATION_TIME_COUNTER.with(|c| c.get());
                base_time.saturating_add(counter)
            } else {
                use std::time::{SystemTime, UNIX_EPOCH};
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time before unix epoch")
                    .as_millis() as u64
            }
        })
    }

    /// Returns true if simulation time is set (thread-local).
    pub fn is_simulation_time() -> bool {
        SIMULATION_TIME_MS.with(|t| t.get().is_some())
    }

    /// Generates a deterministic ULID using GlobalRng and simulation time.
    ///
    /// When both GlobalRng and GlobalSimulationTime are configured:
    /// - Timestamp: Uses simulation time base + monotonic counter
    /// - Random: Uses seeded RNG from GlobalRng
    ///
    /// When not in simulation mode, uses regular `Ulid::generate()`.
    pub fn new_ulid() -> ulid::Ulid {
        use ulid::Ulid;

        if GlobalRng::is_seeded() || Self::is_simulation_time() {
            // Deterministic mode: construct ULID manually
            let timestamp_ms = Self::current_time_ms();

            // Generate 80 bits of random data using GlobalRng
            let mut random_bytes = [0u8; 10];
            GlobalRng::fill_bytes(&mut random_bytes);

            // Construct ULID: 48-bit timestamp (ms) + 80-bit random
            // ULID format: TTTTTTTTTTRRRRRRRRRRRRRRRRRRRRR (T=timestamp, R=random)
            let ts = (timestamp_ms as u128) << 80;
            let rand_high = (random_bytes[0] as u128) << 72;
            let rand_mid = u64::from_be_bytes([
                random_bytes[1],
                random_bytes[2],
                random_bytes[3],
                random_bytes[4],
                random_bytes[5],
                random_bytes[6],
                random_bytes[7],
                random_bytes[8],
            ]) as u128;
            let rand_low = (random_bytes[9] as u128) << 56;
            let ulid_value = ts | rand_high | (rand_mid << 8) | rand_low;

            Ulid(ulid_value)
        } else {
            // Production mode: use standard ULID generation
            Ulid::generate()
        }
    }
}

// =============================================================================
// Simulation Transport Optimization
// =============================================================================

std::thread_local! {
    static SIMULATION_TRANSPORT_OPT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static SIMULATION_IDLE_TIMEOUT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Opt-in transport timer optimization for large-scale simulations.
///
/// When enabled, the transport layer uses relaxed timer intervals (5x slower ACK,
/// resend, and rate-update checks) and disables keepalive pings. This dramatically
/// reduces tokio scheduler overhead for 100+ node simulations where ~15K connections
/// would otherwise create ~900K timer firings per second of virtual time.
///
/// This is a separate flag from `GlobalSimulationTime` because some simulation tests
/// need realistic keepalive behavior (e.g., connection timeout tests). Only
/// large-scale simulations that prioritize throughput should enable this.
///
/// # Safety
///
/// Only affects code paths in `PeerConnection::recv()` and `RealTime::supports_keepalive()`.
/// Production code never sets this flag — it is only called from `run_simulation_direct()`
/// which is gated behind `#[cfg(any(test, feature = "testing"))]`.
pub struct SimulationTransportOpt;

impl SimulationTransportOpt {
    /// Enable relaxed transport timers for the current thread.
    pub fn enable() {
        SIMULATION_TRANSPORT_OPT.with(|f| f.set(true));
    }

    /// Disable relaxed transport timers (restore production behavior).
    pub fn disable() {
        SIMULATION_TRANSPORT_OPT.with(|f| f.set(false));
    }

    /// Returns `true` if relaxed transport timers are enabled on this thread.
    pub fn is_enabled() -> bool {
        SIMULATION_TRANSPORT_OPT.with(|f| f.get())
    }
}

/// Extended idle timeout for simulation connections.
///
/// In `start_paused(true)` simulations, virtual time can jump past the default
/// 120s idle timeout when tasks await `spawn_blocking` (WASM execution). This
/// causes spurious connection drops even with keepalive enabled, because tokio
/// auto-advances time while the blocking thread pool runs.
///
/// This flag is separate from `SimulationTransportOpt` because ALL simulation
/// sizes need the extended timeout, whereas only large simulations (50+ nodes)
/// benefit from relaxed ACK intervals and disabled keepalive.
pub struct SimulationIdleTimeout;

impl SimulationIdleTimeout {
    /// Enable extended idle timeout for the current thread.
    pub fn enable() {
        SIMULATION_IDLE_TIMEOUT.with(|f| f.set(true));
    }

    /// Disable extended idle timeout (restore production behavior).
    pub fn disable() {
        SIMULATION_IDLE_TIMEOUT.with(|f| f.set(false));
    }

    /// Returns `true` if extended idle timeout is enabled on this thread.
    pub fn is_enabled() -> bool {
        SIMULATION_IDLE_TIMEOUT.with(|f| f.get())
    }
}

// =============================================================================
// Global Test Metrics (for simulation testing)
// =============================================================================

// Thread-local test metrics: allows parallel simulation tests without interference.
std::thread_local! {
    static GLOBAL_RESYNC_REQUESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// ResyncRequests emitted specifically because a DELTA FAILED TO APPLY —
    /// the #2763 summary-caching signal, counted at the decision that makes it
    /// rather than inferred from the total (#5510).
    static GLOBAL_DELTA_FAILURE_RESYNCS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_DELTA_SENDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Fan-out legs skipped because the peer's cached summary already matched
    /// ours (the pre-existing mechanism, counted for #5147 diagnosis).
    static GLOBAL_FANOUT_SUMMARY_SKIPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Fan-out targets dropped because the originator named them (#5147).
    static GLOBAL_BROADCAST_TARGETS_SUPPRESSED: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
    /// Fan-out legs dropped because the target was the delivering peer (#5147).
    static GLOBAL_BROADCAST_SENDER_SKIPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Inbound broadcast payloads that reached a terminal classification
    /// (#5147). Denominator for the duplicate-delivery ratio.
    static GLOBAL_BROADCAST_DELIVERIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The subset of those that carried nothing new — a deduped duplicate, or
    /// a merge that moved no state (#5147).
    static GLOBAL_REDUNDANT_BROADCAST_DELIVERIES: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
    static GLOBAL_FULL_STATE_SENDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_INSERTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_REMOVES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_HWM: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_SKIPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_NEIGHBOR_HOSTING_UPDATES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Recipients dropped from a proactive summary notification because they
    /// are advertised co-hosts the broadcast already covered (#4965).
    static GLOBAL_NOTIFICATION_COHOSTS_SKIPPED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Recipients a proactive summary notification actually SENT to — the cost
    /// side of the pair above, which only ever counted the saving.
    static GLOBAL_NOTIFICATION_TARGETS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // Hosting advertisement retractions emitted on stop-hosting (eviction).
    // Advertisement-layer reliability + retraction, #4642 spec step 1.
    static GLOBAL_NEIGHBOR_HOSTING_RETRACTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_ANTI_STARVATION_TRIGGERS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // Terminal advertisement consult (hosting redesign piece C, invariant 5).
    static GLOBAL_TERMINAL_CONSULT_ATTEMPTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_TERMINAL_CONSULT_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_TERMINAL_CONSULT_RESOLVED_FOUND: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_TERMINAL_CONSULT_STILL_NOT_FOUND: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // Summary-first PUT (#4642 step 3-bis) — PUT-bytes-by-case falsifier.
    // Count + bytes for the new-contract case (no holder found; full state
    // ships hop-by-hop via the existing `PutMsg::Request` path).
    static GLOBAL_PUT_PROBE_NEW_CONTRACT_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PUT_PROBE_NEW_CONTRACT_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // Count + bytes for the existing-mesh case (holder found; only a
    // `StateDelta` ships via `ProbeReconcile`).
    static GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // UPDATE broadcast merges skipped by the per-contract merge-failure backoff
    // (poison-contract quarantine, #4861).
    static GLOBAL_MERGES_SUPPRESSED_BY_BACKOFF: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // ResyncRequest emissions / ResyncResponse sends suppressed by the resync
    // rate limiters (#4861). Response suppression is split by which limiter
    // fired: the per-(peer, contract) limit vs the global per-contract cap
    // (#4864 review — indistinguishable in telemetry when shared).
    static GLOBAL_RESYNC_REQUESTS_SUPPRESSED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_RESYNC_RESPONSES_SUPPRESSED_PER_PEER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_RESYNC_RESPONSES_SUPPRESSED_GLOBAL: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_RESYNC_RESPONSES_UNSOLICITED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // Hash-first summary exchange falsifiers (#4965). See the `record_*`
    // rustdoc on `GlobalTestMetrics` for what each one proves.
    static GLOBAL_SUMMARY_DIGEST_MSGS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_FULL_MSGS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_FULL_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // #4965 agreement rate, split by MESSAGE SHAPE rather than by emitter.
    //
    // The emitter tag (#5052) is `#[serde(skip)]`, so an INBOUND SummaryDigests
    // always decodes as `Other` — the receiver, which is the only side that can
    // judge agreement, cannot know which send site produced it. Entry count is
    // the best available proxy and needs no wire change: the notification and
    // rejection emitters are single-entry BY CONSTRUCTION, and only
    // `InterestsReply` — the ~5-min heartbeat — is genuinely multi-entry (see
    // `outbound_message_mix::SummariesDetail`).
    //
    // Known contamination, stated so the number is not over-read, and it is
    // larger than a heartbeat edge case: `ChangeInterestsReply` is single-entry
    // 100% of the time (measured mean exactly 1.000, `max_entries` 1, over
    // 418,476 messages on 1,284 peers), because `broadcast_change_interests`
    // gossips one contract per message. Corrected 2026-08-12 (#5153 review F1) —
    // this said "both reply emitters are multi-entry", which is what made the
    // proxy look clean. A narrow heartbeat (a peer pair sharing exactly ONE
    // contract) contaminates too, but is the smaller term. So the single bucket
    // is "state-change-driven sites PLUS interest-churn replies PLUS narrow
    // heartbeats": directional evidence, not attribution, and the send-side
    // per-emitter census in `outbound_message_mix` is what makes the
    // contamination subtractable rather than assumed.
    /// Peak size of the digest arm's per-hash local-summary cache (#4965).
    /// The observable for the RETENTION bound: the cache holds owned summary
    /// clones, so its peak entry count is what decides whether a hostile
    /// message can accumulate hundreds of MB.
    static GLOBAL_SUMMARY_CACHE_PEAK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_AGREE_SINGLE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_AGREE_MULTI: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_MISMATCH_SINGLE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_MISMATCH_MULTI: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_SUMMARY_BYTE_REQUESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Global test metrics for tracking events across the simulation network.
///
/// These counters are incremented by production code and read by tests to verify
/// correct behavior. They should only be used in testing scenarios.
///
/// # Usage in Tests
///
/// ```ignore
/// use freenet::config::GlobalTestMetrics;
///
/// // Reset at test start
/// GlobalTestMetrics::reset();
///
/// // Run simulation...
///
/// // Check results
/// assert_eq!(GlobalTestMetrics::resync_requests(), 0,
///     "No resyncs should be needed with correct summary caching");
/// ```
pub struct GlobalTestMetrics;

impl GlobalTestMetrics {
    /// Resets all test metrics to zero (thread-local). Call at the start of each test.
    pub fn reset() {
        GLOBAL_RESYNC_REQUESTS.with(|c| c.set(0));
        GLOBAL_DELTA_FAILURE_RESYNCS.with(|c| c.set(0));
        GLOBAL_DELTA_SENDS.with(|c| c.set(0));
        GLOBAL_FANOUT_SUMMARY_SKIPS.with(|c| c.set(0));
        GLOBAL_BROADCAST_TARGETS_SUPPRESSED.with(|c| c.set(0));
        GLOBAL_BROADCAST_SENDER_SKIPS.with(|c| c.set(0));
        GLOBAL_BROADCAST_DELIVERIES.with(|c| c.set(0));
        GLOBAL_REDUNDANT_BROADCAST_DELIVERIES.with(|c| c.set(0));
        GLOBAL_FULL_STATE_SENDS.with(|c| c.set(0));
        GLOBAL_PENDING_OP_INSERTS.with(|c| c.set(0));
        GLOBAL_PENDING_OP_SKIPS.with(|c| c.set(0));
        GLOBAL_PENDING_OP_REMOVES.with(|c| c.set(0));
        GLOBAL_PENDING_OP_HWM.with(|c| c.set(0));
        GLOBAL_NEIGHBOR_HOSTING_UPDATES.with(|c| c.set(0));
        GLOBAL_NOTIFICATION_COHOSTS_SKIPPED.with(|c| c.set(0));
        GLOBAL_NOTIFICATION_TARGETS.with(|c| c.set(0));
        GLOBAL_NEIGHBOR_HOSTING_RETRACTIONS.with(|c| c.set(0));
        GLOBAL_ANTI_STARVATION_TRIGGERS.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_ATTEMPTS.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_HITS.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_RESOLVED_FOUND.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_STILL_NOT_FOUND.with(|c| c.set(0));
        GLOBAL_PUT_PROBE_NEW_CONTRACT_COUNT.with(|c| c.set(0));
        GLOBAL_PUT_PROBE_NEW_CONTRACT_BYTES.with(|c| c.set(0));
        GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_COUNT.with(|c| c.set(0));
        GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_BYTES.with(|c| c.set(0));
        GLOBAL_MERGES_SUPPRESSED_BY_BACKOFF.with(|c| c.set(0));
        GLOBAL_RESYNC_REQUESTS_SUPPRESSED.with(|c| c.set(0));
        GLOBAL_RESYNC_RESPONSES_SUPPRESSED_PER_PEER.with(|c| c.set(0));
        GLOBAL_RESYNC_RESPONSES_SUPPRESSED_GLOBAL.with(|c| c.set(0));
        GLOBAL_RESYNC_RESPONSES_UNSOLICITED.with(|c| c.set(0));
        GLOBAL_SUMMARY_DIGEST_MSGS.with(|c| c.set(0));
        GLOBAL_SUMMARY_FULL_MSGS.with(|c| c.set(0));
        GLOBAL_SUMMARY_FULL_BYTES.with(|c| c.set(0));
        GLOBAL_SUMMARY_CACHE_PEAK.with(|c| c.set(0));
        GLOBAL_SUMMARY_AGREE_SINGLE.with(|c| c.set(0));
        GLOBAL_SUMMARY_AGREE_MULTI.with(|c| c.set(0));
        GLOBAL_SUMMARY_MISMATCH_SINGLE.with(|c| c.set(0));
        GLOBAL_SUMMARY_MISMATCH_MULTI.with(|c| c.set(0));
        GLOBAL_SUMMARY_BYTE_REQUESTS.with(|c| c.set(0));
    }

    /// Records that a ResyncRequest was received.
    /// Called from production code when handling ResyncRequest messages.
    pub fn record_resync_request() {
        GLOBAL_RESYNC_REQUESTS.with(|c| c.set(c.get() + 1));
    }

    /// Returns the total number of ResyncRequests received since last reset.
    ///
    /// This is the TOTAL across every cause. Since #5510 there are several — a
    /// delta that failed to apply, a queue-full broadcast drop, and a
    /// rate-limited broadcast drop (with a fourth, the trailing coalesced
    /// repair, once #5525 lands) — so a test that means "no delta failed" must
    /// use [`Self::delta_failure_resyncs`] instead.
    /// Asserting zero on this total makes any new, legitimate resync source
    /// look like the #2763 regression.
    pub fn resync_requests() -> u64 {
        GLOBAL_RESYNC_REQUESTS.with(|c| c.get())
    }

    /// Records a ResyncRequest emitted because a DELTA FAILED TO APPLY.
    ///
    /// Recorded at the branch that makes that decision (the `is_delta &&
    /// !queue_full` arm of the broadcast driver), never derived by subtracting
    /// other causes from the total — the shape
    /// `.claude/rules/bug-prevention-patterns.md` warns about, where a
    /// subtraction silently absorbs every other cause and keeps reporting a
    /// plausible number after the thing it claims to measure is gone.
    pub fn record_delta_failure_resync() {
        GLOBAL_DELTA_FAILURE_RESYNCS.with(|c| c.set(c.get() + 1));
    }

    /// ResyncRequests emitted because a delta failed to apply — the precise
    /// #2763 summary-caching signal.
    pub fn delta_failure_resyncs() -> u64 {
        GLOBAL_DELTA_FAILURE_RESYNCS.with(|c| c.get())
    }

    /// Records that an UPDATE broadcast merge was skipped by the per-contract
    /// merge-failure backoff (poison-contract quarantine, #4861).
    pub fn record_merge_suppressed_by_backoff() {
        GLOBAL_MERGES_SUPPRESSED_BY_BACKOFF.with(|c| c.set(c.get() + 1));
    }

    /// Returns the number of merges skipped by the merge-failure backoff since
    /// last reset.
    pub fn merges_suppressed_by_backoff() -> u64 {
        GLOBAL_MERGES_SUPPRESSED_BY_BACKOFF.with(|c| c.get())
    }

    /// Records that a `ResyncRequest` emission was suppressed by the per-contract
    /// resync emit rate limiter (#4861).
    pub fn record_resync_request_suppressed() {
        GLOBAL_RESYNC_REQUESTS_SUPPRESSED.with(|c| c.set(c.get() + 1));
    }

    /// Returns the number of ResyncRequest emissions suppressed since last reset.
    pub fn resync_requests_suppressed() -> u64 {
        GLOBAL_RESYNC_REQUESTS_SUPPRESSED.with(|c| c.get())
    }

    /// Records a `ResyncResponse` send suppressed by the per-(peer, contract)
    /// responder rate limiter (#4861).
    pub fn record_resync_response_suppressed_per_peer() {
        GLOBAL_RESYNC_RESPONSES_SUPPRESSED_PER_PEER.with(|c| c.set(c.get() + 1));
    }

    /// Records a `ResyncResponse` send suppressed by the GLOBAL per-contract
    /// responder cap (#4861 / #4864 review — separate counter so the two
    /// limiters are distinguishable in telemetry).
    pub fn record_resync_response_suppressed_global() {
        GLOBAL_RESYNC_RESPONSES_SUPPRESSED_GLOBAL.with(|c| c.set(c.get() + 1));
    }

    /// Returns ResyncResponse sends suppressed by the per-(peer, contract)
    /// limiter since last reset.
    pub fn resync_responses_suppressed_per_peer() -> u64 {
        GLOBAL_RESYNC_RESPONSES_SUPPRESSED_PER_PEER.with(|c| c.get())
    }

    /// Returns ResyncResponse sends suppressed by the global per-contract cap
    /// since last reset.
    pub fn resync_responses_suppressed_global() -> u64 {
        GLOBAL_RESYNC_RESPONSES_SUPPRESSED_GLOBAL.with(|c| c.get())
    }

    /// Records a received `ResyncResponse` DROPPED because it had no matching
    /// outstanding `ResyncRequest` — unsolicited or replayed, or TTL-expired
    /// (#4864 round-8, Codex P1). The apply is refused before any WASM runs.
    pub fn record_resync_response_unsolicited() {
        GLOBAL_RESYNC_RESPONSES_UNSOLICITED.with(|c| c.set(c.get() + 1));
    }

    /// Returns the number of unsolicited/replayed ResyncResponses dropped since
    /// last reset.
    pub fn resync_responses_unsolicited() -> u64 {
        GLOBAL_RESYNC_RESPONSES_UNSOLICITED.with(|c| c.get())
    }

    /// Returns total ResyncResponse sends suppressed (per-peer + global) since
    /// last reset.
    pub fn resync_responses_suppressed() -> u64 {
        Self::resync_responses_suppressed_per_peer() + Self::resync_responses_suppressed_global()
    }

    /// Records a fan-out leg skipped by the summary-match gate.
    pub fn record_fanout_summary_skip() {
        GLOBAL_FANOUT_SUMMARY_SKIPS.with(|c| c.set(c.get() + 1));
    }

    /// Fan-out legs skipped by the summary-match gate since reset.
    pub fn fanout_summary_skips() -> u64 {
        GLOBAL_FANOUT_SUMMARY_SKIPS.with(|c| c.get())
    }

    /// Records one fan-out target dropped because the originator's list named
    /// it (#5147).
    ///
    /// Incremented BY the filter in `get_broadcast_targets_update`, alongside
    /// its own `skipped_covered` field, never re-derived from the difference
    /// between the resolved co-host set and the final target set — that
    /// subtraction also absorbs the sender, self, and resolve-failure filters,
    /// so it would keep reporting a plausible number after this filter is
    /// deleted. The simulation's discriminator rests on this counter going to
    /// exactly 0 when the feature is off, which a derived count would not do.
    pub fn record_broadcast_target_suppressed() {
        GLOBAL_BROADCAST_TARGETS_SUPPRESSED.with(|c| c.set(c.get() + 1));
    }

    /// Records that the peer which delivered this update was dropped from our
    /// own fan-out (#5147 sender exclusion).
    ///
    /// A FOURTH terminal outcome for an offered leg, alongside sent /
    /// summary-skipped / list-suppressed. It needs its own counter for the same
    /// reason the others do — it is incremented by the filter that makes the
    /// decision, not derived at a call site — and because without it the
    /// simulation's leg-accounting identity is short by exactly the number of
    /// sender exclusions, which reads as the two arms having done different
    /// amounts of work when they did not.
    pub fn record_broadcast_sender_skipped() {
        GLOBAL_BROADCAST_SENDER_SKIPS.with(|c| c.set(c.get() + 1));
    }

    /// Fan-out legs dropped because the target was the delivering peer.
    pub fn broadcast_sender_skips() -> u64 {
        GLOBAL_BROADCAST_SENDER_SKIPS.with(|c| c.get())
    }

    /// Fan-out targets suppressed by the originator target list since reset.
    pub fn broadcast_targets_suppressed() -> u64 {
        GLOBAL_BROADCAST_TARGETS_SUPPRESSED.with(|c| c.get())
    }

    /// Records one inbound broadcast payload reaching a terminal outcome, and
    /// whether it carried anything new (#5147).
    ///
    /// Called from `PayloadMix::record_receiver_terminal` — the ONE place that
    /// classifies an inbound broadcast — so the redundancy count is produced by
    /// the code making the decision rather than re-derived from set sizes
    /// elsewhere. See `.claude/rules/bug-prevention-patterns.md`, "Metric
    /// describing a filtering decision, re-derived at the call site": a
    /// subtraction across two collections silently absorbs every other filter
    /// between them and keeps reporting a plausible number after the filter it
    /// claims to measure is gone.
    ///
    /// A `Failed` outcome is counted as a delivery but NOT as redundant: the
    /// payload may well have carried something new and the merge simply broke.
    /// Calling a failure redundant would flatter any change that increased
    /// merge failures.
    pub fn record_broadcast_delivery(redundant: bool) {
        GLOBAL_BROADCAST_DELIVERIES.with(|c| c.set(c.get() + 1));
        if redundant {
            GLOBAL_REDUNDANT_BROADCAST_DELIVERIES.with(|c| c.set(c.get() + 1));
        }
    }

    /// Inbound broadcast payloads that reached a terminal outcome since reset.
    pub fn broadcast_deliveries() -> u64 {
        GLOBAL_BROADCAST_DELIVERIES.with(|c| c.get())
    }

    /// Those of [`Self::broadcast_deliveries`] that changed nothing.
    pub fn redundant_broadcast_deliveries() -> u64 {
        GLOBAL_REDUNDANT_BROADCAST_DELIVERIES.with(|c| c.get())
    }

    /// Records that a delta was sent in a state change broadcast.
    /// Called from p2p_protoc.rs when sent_delta = true.
    pub fn record_delta_send() {
        GLOBAL_DELTA_SENDS.with(|c| c.set(c.get() + 1));
    }

    /// Returns the total number of delta sends since last reset.
    pub fn delta_sends() -> u64 {
        GLOBAL_DELTA_SENDS.with(|c| c.get())
    }

    /// Records that full state was sent in a state change broadcast.
    /// Called from p2p_protoc.rs when sent_delta = false.
    pub fn record_full_state_send() {
        GLOBAL_FULL_STATE_SENDS.with(|c| c.set(c.get() + 1));
    }

    /// Returns the total number of full state sends since last reset.
    pub fn full_state_sends() -> u64 {
        GLOBAL_FULL_STATE_SENDS.with(|c| c.get())
    }

    pub fn record_pending_op_insert() {
        GLOBAL_PENDING_OP_INSERTS.with(|c| c.set(c.get() + 1));
    }

    pub fn pending_op_inserts() -> u64 {
        GLOBAL_PENDING_OP_INSERTS.with(|c| c.get())
    }

    pub fn record_pending_op_remove() {
        GLOBAL_PENDING_OP_REMOVES.with(|c| c.set(c.get() + 1));
    }

    pub fn pending_op_removes() -> u64 {
        GLOBAL_PENDING_OP_REMOVES.with(|c| c.get())
    }

    /// A waiter install was refused because a LIVE incumbent already held the tx.
    ///
    /// Expected to stay at zero: the invariant is one live waiter per tx per
    /// node, so a non-zero count means either a genuine collision or that the
    /// guard is refusing a legitimate waiter — the latter otherwise surfaces
    /// only as a driver retry with nothing pointing back here.
    pub fn record_pending_op_skip() {
        GLOBAL_PENDING_OP_SKIPS.with(|c| c.set(c.get() + 1));
    }

    pub fn pending_op_skips() -> u64 {
        GLOBAL_PENDING_OP_SKIPS.with(|c| c.get())
    }

    /// Track high-water mark for pending_op_results size.
    pub fn record_pending_op_size(len: u64) {
        GLOBAL_PENDING_OP_HWM.with(|c| c.set(c.get().max(len)));
    }

    pub fn pending_op_high_water_mark() -> u64 {
        GLOBAL_PENDING_OP_HWM.with(|c| c.get())
    }

    pub fn record_neighbor_hosting_update() {
        GLOBAL_NEIGHBOR_HOSTING_UPDATES.with(|c| c.set(c.get() + 1));
    }

    pub fn neighbor_hosting_updates() -> u64 {
        GLOBAL_NEIGHBOR_HOSTING_UPDATES.with(|c| c.get())
    }

    /// A proactive summary notification skipped `n` advertised co-hosts,
    /// because the broadcast to them already carried the summary in
    /// `sender_summary_bytes` (#4965).
    ///
    /// Exists because the simulation-level effect of that exclusion is NOT
    /// visible in `delta_sends` / `full_state_sends`: measured on the co-host
    /// mesh scenario, reverting the exclusion left both metrics bit-identical
    /// (140 / 6), so a test asserting on them alone would pass whether or not
    /// the change was present. This counter is the one signal that actually
    /// moves, which is what makes
    /// `test_cohost_mesh_update_fanout_stays_delta_dominated` a test OF the
    /// change rather than merely a test that runs alongside it.
    pub fn record_notification_cohosts_skipped(n: u64) {
        GLOBAL_NOTIFICATION_COHOSTS_SKIPPED.with(|c| c.set(c.get() + n));
    }

    pub fn notification_cohosts_skipped() -> u64 {
        GLOBAL_NOTIFICATION_COHOSTS_SKIPPED.with(|c| c.get())
    }

    /// A proactive summary notification ATTEMPTED to `n` recipients.
    ///
    /// Attempted, not delivered: `n` is the resolved recipient set, counted
    /// once per notification round, before the per-peer enqueue can fail. That
    /// matches what the cost question asks (how many messages this mechanism
    /// puts on the wire) and matches its sibling `notification_cohosts_skipped`,
    /// which is likewise an intent count. Do not read it as an ack.
    ///
    /// The cost half of the pair. `notification_cohosts_skipped` counts only
    /// what the #4965 exclusion SAVED, so for as long as it stood alone the
    /// simulation could see this mechanism getting cheaper and could not see it
    /// getting more expensive.
    ///
    /// That asymmetry is not academic: #5190's fix restores notifications to
    /// every peer the #5147 target list suppresses, and the A/B rig that exists
    /// specifically to judge #5147 measured the 13 sends it saved while being
    /// structurally blind to the ~429 messages it added. The trade could only
    /// be argued, not measured. **A counter for a mechanism's saving needs its
    /// twin for the mechanism's cost, or the rig can only ever return good
    /// news.**
    pub fn record_notification_targets(n: u64) {
        GLOBAL_NOTIFICATION_TARGETS.with(|c| c.set(c.get() + n));
    }

    pub fn notification_targets() -> u64 {
        GLOBAL_NOTIFICATION_TARGETS.with(|c| c.get())
    }

    /// A hosting advertisement retraction was emitted because this node stopped
    /// hosting a contract (eviction). Advertisement-layer reliability +
    /// retraction, #4642 spec step 1 — see `NeighborHostingManager::on_contract_unhosted`.
    pub fn record_neighbor_hosting_retraction() {
        GLOBAL_NEIGHBOR_HOSTING_RETRACTIONS.with(|c| c.set(c.get() + 1));
    }

    pub fn neighbor_hosting_retractions() -> u64 {
        GLOBAL_NEIGHBOR_HOSTING_RETRACTIONS.with(|c| c.get())
    }

    pub fn record_anti_starvation_trigger() {
        GLOBAL_ANTI_STARVATION_TRIGGERS.with(|c| c.set(c.get() + 1));
    }

    pub fn anti_starvation_triggers() -> u64 {
        GLOBAL_ANTI_STARVATION_TRIGGERS.with(|c| c.get())
    }

    // --- Terminal advertisement consult (hosting redesign piece C) ---
    //
    // Aggregate scalars measuring whether the terminal consult actually
    // closes findability dead-ends (invariant 5). Thread-local, so under
    // the single-threaded simulation runner they aggregate across all sim
    // nodes and a test can assert the consult path fired. Production
    // per-node scalars live in `node::network_status` (RwLock global).

    /// A routing terminus consulted its neighbor host-advertisements for
    /// the target key before giving up (one increment per terminus, not
    /// per advertised host tried).
    pub fn record_terminal_consult_attempt() {
        GLOBAL_TERMINAL_CONSULT_ATTEMPTS.with(|c| c.set(c.get() + 1));
    }

    pub fn terminal_consult_attempts() -> u64 {
        GLOBAL_TERMINAL_CONSULT_ATTEMPTS.with(|c| c.get())
    }

    /// The consult found at least one advertised host to forward to
    /// (a candidate off the direct routing path).
    pub fn record_terminal_consult_hit() {
        GLOBAL_TERMINAL_CONSULT_HITS.with(|c| c.set(c.get() + 1));
    }

    pub fn terminal_consult_hits() -> u64 {
        GLOBAL_TERMINAL_CONSULT_HITS.with(|c| c.get())
    }

    /// A consult forward resolved the request to Found/Subscribed —
    /// a dead-end that the consult actually closed.
    pub fn record_terminal_consult_resolved_found() {
        GLOBAL_TERMINAL_CONSULT_RESOLVED_FOUND.with(|c| c.set(c.get() + 1));
    }

    pub fn terminal_consult_resolved_found() -> u64 {
        GLOBAL_TERMINAL_CONSULT_RESOLVED_FOUND.with(|c| c.get())
    }

    /// A consult ran but the request still ended NotFound (no advertised
    /// host, or every advertised host also failed).
    pub fn record_terminal_consult_still_not_found() {
        GLOBAL_TERMINAL_CONSULT_STILL_NOT_FOUND.with(|c| c.set(c.get() + 1));
    }

    pub fn terminal_consult_still_not_found() -> u64 {
        GLOBAL_TERMINAL_CONSULT_STILL_NOT_FOUND.with(|c| c.get())
    }

    // --- Summary-first PUT (#4642 step 3-bis): PUT-bytes-by-case falsifier ---
    //
    // Proves (or disproves) the byte-savings claim: how many originator PUTs
    // took the new-contract path (full state shipped, the pre-existing
    // behavior) versus the existing-mesh path (only a delta shipped via
    // `ProbeReconcile`), and how many bytes each path actually moved. Fed
    // from a single call site (`operations::put::op_ctx_task::
    // record_put_probe_outcome`) so this thread-local counter and the
    // per-node `node::network_status` dashboard counter never drift
    // relative to each other.

    /// Record a summary-first PUT probe that found no holder (genuinely new
    /// contract): `bytes` is the full-state payload size about to ship
    /// hop-by-hop via the existing `PutMsg::Request` path.
    pub fn record_put_probe_new_contract(bytes: u64) {
        GLOBAL_PUT_PROBE_NEW_CONTRACT_COUNT.with(|c| c.set(c.get() + 1));
        GLOBAL_PUT_PROBE_NEW_CONTRACT_BYTES.with(|c| c.set(c.get() + bytes));
    }

    pub fn put_probe_new_contract_sends() -> u64 {
        GLOBAL_PUT_PROBE_NEW_CONTRACT_COUNT.with(|c| c.get())
    }

    pub fn put_probe_new_contract_bytes() -> u64 {
        GLOBAL_PUT_PROBE_NEW_CONTRACT_BYTES.with(|c| c.get())
    }

    /// Record a summary-first PUT probe that found an existing holder:
    /// `bytes` is the `StateDelta` size shipped via `ProbeReconcile` (0 if
    /// the delta was empty — the holder's state was already logically
    /// equivalent to ours, so nothing was sent).
    pub fn record_put_probe_existing_mesh_delta(bytes: u64) {
        GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_COUNT.with(|c| c.set(c.get() + 1));
        GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_BYTES.with(|c| c.set(c.get() + bytes));
    }

    pub fn put_probe_existing_mesh_delta_sends() -> u64 {
        GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_COUNT.with(|c| c.get())
    }

    pub fn put_probe_existing_mesh_delta_bytes() -> u64 {
        GLOBAL_PUT_PROBE_EXISTING_MESH_DELTA_BYTES.with(|c| c.get())
    }

    // === Hash-first summary exchange falsifiers (#4965) ===
    //
    // The claim under test is "the common case stops shipping summary bytes".
    // These counters are fed from the constructor functions that can build
    // these messages — `node::summaries_reply_in_form` (which applies the
    // chosen encoding, and which `node::summaries_reply_for_peer` delegates
    // to) and `node::full_summaries_message` (which every full-bytes path
    // routes through, including the `operations::update` helpers, which are
    // CALLERS rather than construction sites). So
    // `summary_full_bytes() == 0` means no summary byte was put on the wire by
    // any path, not merely by the one a test happened to exercise.
    // `no_uninstrumented_full_summaries_construction` pins that no production
    // site builds an `InterestMessage::Summaries` outside the constructor.

    /// A hash-first `SummaryDigests` message was emitted, advertising
    /// contracts without their summaries.
    pub fn record_summary_digest_msg() {
        GLOBAL_SUMMARY_DIGEST_MSGS.with(|c| c.set(c.get() + 1));
    }

    /// A full-bytes `Summaries` message was emitted: either the pre-floor
    /// fallback, or the answer to a `SummaryRequest`. `bytes` is the total
    /// summary payload it carries — the quantity hash-first exists to avoid.
    pub fn record_summary_full_msg(bytes: u64) {
        GLOBAL_SUMMARY_FULL_MSGS.with(|c| c.set(c.get() + 1));
        GLOBAL_SUMMARY_FULL_BYTES.with(|c| c.set(c.get() + bytes));
    }

    /// Observe the digest arm's local-summary cache size, keeping the peak.
    ///
    /// Records the RETENTION bound directly rather than through a proxy: the
    /// cache holds owned summary clones, so a peak proportional to the number
    /// of hashes a peer named — rather than to ONE hash's contract set — is
    /// the accumulation this is here to catch.
    pub fn note_summary_cache_size(len: usize) {
        GLOBAL_SUMMARY_CACHE_PEAK.with(|c| c.set(c.get().max(len as u64)));
    }

    pub fn summary_cache_peak() -> u64 {
        GLOBAL_SUMMARY_CACHE_PEAK.with(|c| c.get())
    }

    /// One advertised digest matched our own summary, settling that contract
    /// with zero summary bytes exchanged.
    ///
    /// `single_entry` splits by the SHAPE of the message the entry arrived in
    /// — see the module note on why that is the best available proxy for the
    /// send site.
    pub fn record_summary_digest_agreement(single_entry: bool) {
        if single_entry {
            GLOBAL_SUMMARY_AGREE_SINGLE.with(|c| c.set(c.get() + 1));
        } else {
            GLOBAL_SUMMARY_AGREE_MULTI.with(|c| c.set(c.get() + 1));
        }
    }

    /// A digest could NOT settle a contract, so its bytes must be requested.
    ///
    /// The denominator half of the agreement rate: without it, a low agreement
    /// COUNT and a low exchange VOLUME look identical, and the whole question
    /// (does the state-change-driven site agree less often than the heartbeat
    /// one?) is about a RATE.
    pub fn record_summary_digest_mismatch(single_entry: bool) {
        if single_entry {
            GLOBAL_SUMMARY_MISMATCH_SINGLE.with(|c| c.set(c.get() + 1));
        } else {
            GLOBAL_SUMMARY_MISMATCH_MULTI.with(|c| c.set(c.get() + 1));
        }
    }

    /// Agreements observed in SINGLE-entry `SummaryDigests` messages.
    ///
    /// Proxy for the state-change-driven send sites (proactive notification,
    /// rejection summary-back), which are single-entry by construction. This
    /// is the population #4861 makes us care about: the proactive site fires
    /// immediately after WE change state, so the receiver may not have applied
    /// the update yet and could disagree far more often than the fleet-wide
    /// 98.1% suggests — and every disagreement costs two extra messages on the
    /// axis that caused the storm.
    pub fn summary_digest_agreements_single() -> u64 {
        GLOBAL_SUMMARY_AGREE_SINGLE.with(|c| c.get())
    }

    /// Agreements observed in MULTI-entry `SummaryDigests` messages — the
    /// heartbeat / interest-churn replies.
    pub fn summary_digest_agreements_multi() -> u64 {
        GLOBAL_SUMMARY_AGREE_MULTI.with(|c| c.get())
    }

    pub fn summary_digest_mismatches_single() -> u64 {
        GLOBAL_SUMMARY_MISMATCH_SINGLE.with(|c| c.get())
    }

    pub fn summary_digest_mismatches_multi() -> u64 {
        GLOBAL_SUMMARY_MISMATCH_MULTI.with(|c| c.get())
    }

    /// Total agreements, both shapes.
    pub fn summary_digest_agreements() -> u64 {
        Self::summary_digest_agreements_single() + Self::summary_digest_agreements_multi()
    }

    /// A digest could not settle some contracts, so their bytes were
    /// requested. Non-zero means the mismatch path ran.
    pub fn record_summary_byte_request() {
        GLOBAL_SUMMARY_BYTE_REQUESTS.with(|c| c.set(c.get() + 1));
    }

    pub fn summary_digest_msgs() -> u64 {
        GLOBAL_SUMMARY_DIGEST_MSGS.with(|c| c.get())
    }

    pub fn summary_full_msgs() -> u64 {
        GLOBAL_SUMMARY_FULL_MSGS.with(|c| c.get())
    }

    pub fn summary_full_bytes() -> u64 {
        GLOBAL_SUMMARY_FULL_BYTES.with(|c| c.get())
    }

    pub fn summary_byte_requests() -> u64 {
        GLOBAL_SUMMARY_BYTE_REQUESTS.with(|c| c.get())
    }
}

pub fn set_logger(
    level: Option<tracing::level_filters::LevelFilter>,
    endpoint: Option<String>,
    log_dir: Option<&Path>,
) {
    #[cfg(feature = "trace")]
    {
        static LOGGER_SET: AtomicBool = AtomicBool::new(false);
        if LOGGER_SET
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }

        crate::tracing::tracer::init_tracer(level, endpoint, log_dir)
            .expect("failed tracing initialization")
    }
}

async fn load_gateways_from_index(url: &str, pub_keys_dir: &Path) -> anyhow::Result<Gateways> {
    // Use an explicit timeout so the node doesn't hang indefinitely when the
    // network is unavailable (e.g., immediately after a Windows restart before
    // the network stack is ready). See #3716, #3717.
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let response = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    // Name the remote index, not a local file: a duplicate spelling published
    // there would otherwise have every node on the network tell its operator to
    // go edit an innocent gateways.toml, on every boot.
    let mut gateways: Gateways = parse_gateways_toml(&response, url)?;
    let mut base_url = reqwest::Url::parse(url)?;
    base_url.set_path("");
    let mut valid_gateways = Vec::new();

    for gateway in &mut gateways.gateways {
        gateway.location = None; // always ignore any location from files if set, it should be derived from IP
        let public_key_url = base_url.join(&gateway.public_key_path.to_string_lossy())?;
        let public_key_response = client
            .get(public_key_url)
            .send()
            .await?
            .error_for_status()?;
        let file_name = gateway
            .public_key_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Invalid public key path"))?;
        let local_path = pub_keys_dir.join(file_name);
        let mut public_key_file = File::create(&local_path)?;
        let content = public_key_response.bytes().await?;
        std::io::copy(&mut content.as_ref(), &mut public_key_file)?;

        // Validate the public key (hex-encoded X25519 public key, 32 bytes = 64 hex chars)
        // Also accept legacy RSA PEM keys temporarily for backwards compatibility
        let mut key_file = File::open(&local_path).with_context(|| {
            format!(
                "failed loading gateway pubkey from {:?}",
                gateway.public_key_path
            )
        })?;
        let mut buf = String::new();
        key_file.read_to_string(&mut buf)?;
        let buf = buf.trim();

        // Check if it's a legacy RSA PEM public key
        if buf.starts_with("-----BEGIN") {
            tracing::warn!(
                public_key_path = ?gateway.public_key_path,
                "Gateway uses legacy RSA PEM public key format. \
                 Gateway needs to be updated to X25519 format. Skipping."
            );
            continue;
        }

        if let Ok(key_bytes) = hex::decode(buf) {
            if key_bytes.len() == 32 {
                gateway.public_key_path = local_path;
                valid_gateways.push(gateway.clone());
            } else {
                tracing::warn!(
                    public_key_path = ?gateway.public_key_path,
                    "Invalid public key length {} (expected 32), ignoring",
                    key_bytes.len()
                );
            }
        } else {
            tracing::warn!(
                public_key_path = ?gateway.public_key_path,
                "Invalid public key hex encoding in remote gateway file, ignoring"
            );
        }
    }

    gateways.gateways = valid_gateways;
    Ok(gateways)
}

/// Test-only: build a `ConfigArgs` rooted at `dir` in the given mode, ready to
/// `build()` into a real `Config` whose data dir is `dir`.
///
/// Lives at module level rather than inside `mod tests` so the event-log tests
/// in `tracing::aof` and `node` can share one definition of "a config that
/// builds" with the `#[cfg(test)]` config tests here — the three modules must
/// agree on the shape or they stop testing the same thing.
#[cfg(test)]
pub(crate) fn event_log_test_args(dir: &std::path::Path, mode: OperationMode) -> ConfigArgs {
    ConfigArgs {
        mode: Some(mode),
        // A non-gateway network node with no gateways is rejected by
        // `build()`, so the network-mode cases build as a gateway. That is
        // the realistic shape anyway: a gateway IS a network-mode node, and
        // it is exactly the kind of node we operate and want the log on.
        network_api: {
            let is_network = matches!(mode, OperationMode::Network);
            NetworkArgs {
                is_gateway: is_network,
                // A gateway must declare a public address.
                public_address: is_network.then(|| "203.0.113.1".parse().unwrap()),
                public_port: is_network.then_some(31337),
                skip_load_from_network: true,
                ..Default::default()
            }
        },
        config_paths: ConfigPathsArgs {
            config_dir: Some(dir.to_path_buf()),
            data_dir: Some(dir.to_path_buf()),
            log_dir: Some(dir.to_path_buf()),
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use httptest::{Expectation, Server, matchers::*, responders::*};

    use std::collections::BTreeSet;

    use crate::node::NodeConfig;
    use crate::transport::TransportKeypair;

    use super::*;

    /// The pool size is the multiplier in every per-worker memory budget
    /// (#5268 defect 3), so its clamp boundaries are load-bearing, not cosmetic:
    /// the divisor the budgets use and the count the pool actually creates come
    /// from this one function, and a mismatch between them WAS the defect.
    ///
    /// Exercised through the pure `resolve_pool_size` rather than the env-reading
    /// wrapper: `FREENET_RUNTIME_POOL_SIZE` is process-global, so a test that set
    /// it would race every other test in the binary.
    #[test]
    fn runtime_pool_size_clamps_cores_and_override() {
        let size = |cores, over| resolve_pool_size(cores, over).get();

        // One core is reserved for the event loop and OS scheduling.
        assert_eq!(size(Some(20), None), 16, "capped at MAX");
        assert_eq!(size(Some(17), None), 16, "exactly at MAX after the reserve");
        assert_eq!(size(Some(8), None), 7);
        assert_eq!(size(Some(2), None), 1, "vega's shape: 2 cores -> 1 worker");
        // Never zero, however few cores are reported.
        assert_eq!(size(Some(1), None), 1);
        assert_eq!(size(Some(0), None), 1);
        // Unknown parallelism falls back to a 4-core assumption.
        assert_eq!(size(None, None), 3);

        // The override wins, and is clamped the same way — a hostile or fat-
        // fingered value must not multiply the per-worker budgets past MAX.
        assert_eq!(size(Some(20), Some(1)), 1);
        assert_eq!(size(Some(2), Some(16)), 16);
        assert_eq!(size(Some(2), Some(9_999)), 16);
        assert_eq!(size(Some(20), Some(0)), 1, "zero clamps up, never panics");
    }

    // ---------------------------------------------------------------------
    // #5124 — `config.toml` key convention.
    //
    // Keys in `config.toml` mix hyphens and underscores with no pattern
    // distinguishing them (`total-bandwidth-limit` and `bandwidth_limit` sit
    // adjacent in the same file), so a setting nobody can spell is a setting
    // nobody can use. Step 1 makes every key in `config.toml` accept its
    // kebab-case spelling, so either guess works — plus the two keys in
    // `gateways.toml` with the same history, `public_key` and the nested
    // `host_address`.
    // Emitting one consistent spelling is step 2 (#5130) — see
    // `emitted_config_toml_keys_keep_their_released_spelling` for why the two
    // cannot land together.
    //
    // A wholly unknown key is still ignored in silence; that is #5131.
    // ---------------------------------------------------------------------

    /// A `config.toml` in the exact spelling releases write, with every
    /// optional key set to a value distinct from its default — so a key that
    /// fails to bind shows up as a wrong value rather than a coincidental
    /// match against a default.
    const RELEASED_CONFIG_TOML: &str = r#"
mode = "network"
network-address = "0.0.0.0"
network-port = 31338
public_network_address = "203.0.113.7"
public_port = 31339
bandwidth_limit = 3000001
total_bandwidth_limit = 2000002
min_bandwidth_per_connection = 250003
blocked_addresses = ["198.51.100.9:1234"]
event_loop_channel_capacity = 4096
transient-budget = 1024
transient-ttl-secs = 45
min-number-of-connections = 25
max-number-of-connections = 200
streaming-threshold = 65537
ledbat-min-ssthresh = 102401
congestion-control = "bbr"
bbr-startup-rate = 12345678
skip_load_from_network = true
ws-api-address = "127.0.0.1"
ws-api-port = 7510
token-ttl-seconds = 86401
token-cleanup-interval-seconds = 301
allowed-host = ["example.invalid"]
allowed-source-cidrs = ["100.64.0.0/10"]
hosted-mode = true
per-user-op-rate-limit = 11
per-user-op-burst = 101
per-user-export-min-interval-secs = 12
transport_keypair = "/tmp/freenet-5124/secrets/transport_keypair"
nonce = "/tmp/freenet-5124/secrets/nonce"
cipher = "/tmp/freenet-5124/secrets/delegate_cipher"
log_level = "debug"
contracts_dir = "/tmp/freenet-5124/contracts"
delegates_dir = "/tmp/freenet-5124/delegates"
secrets_dir = "/tmp/freenet-5124/secrets"
db_dir = "/tmp/freenet-5124/db"
event_log = "/tmp/freenet-5124/_EVENT_LOG"
data_dir = "/tmp/freenet-5124"
config_dir = "/tmp/freenet-5124/config"
log_dir = "/tmp/freenet-5124/logs"
wasmtime_cache_dir = "/tmp/freenet-5124/wasmtime-cache"
is_gateway = true
location = 0.25
max_blocking_threads = 17
max-hosting-storage = 12345678
hosting-disk-pct = 0.25
max-hosting-disk = 23456789
per-user-secret-quota = 4194305
per-user-inactive-ttl = 2592001
inactive-user-sweep-interval = 3601
module-cache-budget-bytes = 4294967296
enable-event-log = true
telemetry-enabled = true
telemetry-endpoint = "http://127.0.0.1:14318"
transport-snapshot-interval-secs = 31
reference-ping-enabled = true
iface-tx-enabled = true
shutdown-drain-secs = 42
"#;

    /// [`RELEASED_CONFIG_TOML`] with every underscored key rewritten to its
    /// kebab-case spelling, taken from [`CONFIG_KEY_SPELLINGS`].
    ///
    /// Derived from the production table rather than a second copy of it: three
    /// hand-maintained encodings of one fact (the serde attributes, this, and
    /// the table) is one too many, and review proved a key dropped from the
    /// test-side copy was caught by nothing. Asserts each rewrite fires, so a
    /// typo cannot quietly reduce this to a copy of the released document.
    fn kebab_config_toml() -> String {
        let mut doc = RELEASED_CONFIG_TOML.to_string();
        for (underscored, kebab) in CONFIG_KEY_SPELLINGS.iter().map(|g| (g[0], g[1])) {
            let from = format!("\n{underscored} = ");
            let to = format!("\n{kebab} = ");
            assert!(
                doc.contains(&from),
                "RELEASED_CONFIG_TOML is missing the `{underscored}` key that \
                 CONFIG_KEY_SPELLINGS pairs with `{kebab}`"
            );
            assert_ne!(from, to, "group `{underscored}` rewrites to itself");
            doc = doc.replace(&from, &to);
        }
        assert_ne!(
            doc, RELEASED_CONFIG_TOML,
            "the kebab fixture is a byte copy of the released one, so every \
             test built on it is vacuous"
        );
        doc
    }

    /// The value every renamed key carries, asserted
    /// against a parsed `Config`.
    ///
    /// Shared by the released-spelling and kebab-spelling tests so both are
    /// held to the same explicit expectation. Comparing the two parses against
    /// each OTHER is not enough on its own: if a key were ignored under BOTH
    /// spellings, both would fall back to the same default and the comparison
    /// would pass while the setting did nothing.
    fn assert_seeded_values_bound(cfg: &Config) {
        assert_seeded_values_bound_except_secrets(cfg);
        assert_eq!(
            cfg.secrets.transport_keypair_path.as_deref(),
            Some(Path::new("/tmp/freenet-5124/secrets/transport_keypair"))
        );
        // Single-word keys, so this change cannot move them — but they are the
        // operator's key material, and nothing else in these tests asserts they
        // still bind.
        assert_eq!(
            cfg.secrets.nonce_path.as_deref(),
            Some(Path::new("/tmp/freenet-5124/secrets/nonce"))
        );
        assert_eq!(
            cfg.secrets.cipher_path.as_deref(),
            Some(Path::new("/tmp/freenet-5124/secrets/delegate_cipher"))
        );
    }

    /// The same, minus the three secret paths — for tests that go through
    /// `read_config`, which resolves those off disk and so cannot name files
    /// that do not exist.
    fn assert_seeded_values_bound_except_secrets(cfg: &Config) {
        assert_eq!(
            cfg.network_api.public_address,
            Some("203.0.113.7".parse::<IpAddr>().unwrap())
        );
        assert_eq!(cfg.network_api.public_port, Some(31339));
        assert_eq!(cfg.network_api.bandwidth_limit, Some(3_000_001));
        assert_eq!(cfg.network_api.total_bandwidth_limit, Some(2_000_002));
        assert_eq!(cfg.network_api.min_bandwidth_per_connection, Some(250_003));
        assert_eq!(
            cfg.network_api.blocked_addresses,
            Some(HashSet::from(["198.51.100.9:1234".parse().unwrap()]))
        );
        assert_eq!(cfg.network_api.event_loop_channel_capacity, 4096);
        assert!(cfg.network_api.skip_load_from_network);
        assert_eq!(cfg.log_level, tracing::log::LevelFilter::Debug);
        assert_eq!(
            cfg.config_paths.contracts_dir,
            PathBuf::from("/tmp/freenet-5124/contracts")
        );
        assert_eq!(
            cfg.config_paths.delegates_dir,
            PathBuf::from("/tmp/freenet-5124/delegates")
        );
        assert_eq!(
            cfg.config_paths.secrets_dir,
            PathBuf::from("/tmp/freenet-5124/secrets")
        );
        assert_eq!(
            cfg.config_paths.db_dir,
            PathBuf::from("/tmp/freenet-5124/db")
        );
        assert_eq!(
            cfg.config_paths.event_log,
            PathBuf::from("/tmp/freenet-5124/_EVENT_LOG")
        );
        assert_eq!(
            cfg.config_paths.data_dir,
            PathBuf::from("/tmp/freenet-5124")
        );
        assert_eq!(
            cfg.config_paths.config_dir,
            PathBuf::from("/tmp/freenet-5124/config")
        );
        assert_eq!(
            cfg.config_paths.log_dir,
            Some(PathBuf::from("/tmp/freenet-5124/logs"))
        );
        assert_eq!(
            cfg.config_paths.wasmtime_cache_dir,
            PathBuf::from("/tmp/freenet-5124/wasmtime-cache")
        );
        assert!(cfg.is_gateway);
        assert_eq!(cfg.max_blocking_threads, 17);
        // Keys that were always kebab-case, spot-checked so the documents are
        // exercised beyond the renamed set.
        assert_eq!(cfg.network_api.congestion_control, "bbr");
        assert_eq!(cfg.ws_api.per_user_op_burst, 101);
        assert_eq!(cfg.shutdown_drain_secs, 42);
    }

    /// THE BUG (#5124): a `config.toml` key could not be spelled the way the
    /// rest of the file demonstrates. Following the hyphenated convention got
    /// you a silently-ignored setting (for keys with a default) or a refusal to
    /// start (for `log_level` / `is_gateway` / the `ConfigPaths` keys, which
    /// have none). Every key must now accept its kebab-case spelling.
    #[test]
    fn kebab_case_config_toml_keys_are_accepted() {
        let cfg: Config = toml::from_str(&kebab_config_toml())
            .expect("every config.toml key must be accepted in kebab-case");
        assert_seeded_values_bound(&cfg);
    }

    /// BACK-COMPAT: accepting the hyphenated spelling must not cost the
    /// underscored one. Every `config.toml` any release has ever written keeps
    /// working, unchanged and indefinitely.
    #[test]
    fn released_underscored_config_toml_keys_still_bind() {
        let cfg: Config = toml::from_str(RELEASED_CONFIG_TOML)
            .expect("a config.toml written by any release must still parse");
        assert_seeded_values_bound(&cfg);
    }

    /// The two spellings must be genuinely interchangeable, not merely both
    /// parseable. (Read with `assert_seeded_values_bound`, which is what stops
    /// this from passing on two identically-defaulted configs.)
    #[test]
    fn released_and_kebab_config_toml_are_equivalent() {
        let released: Config = toml::from_str(RELEASED_CONFIG_TOML).unwrap();
        let kebab: Config = toml::from_str(&kebab_config_toml()).unwrap();
        assert_eq!(
            toml::to_string(&released).unwrap(),
            toml::to_string(&kebab).unwrap(),
        );
    }

    /// `public-port` is accepted alongside `public-network-port`: the file's
    /// released key is `public_port`, so that is the spelling an operator
    /// hyphenating what they see will reach for, while the flag they read in
    /// `--help` is `--public-network-port`. Both work.
    #[test]
    fn both_kebab_spellings_of_public_port_are_accepted() {
        for key in ["public-network-port", "public-port"] {
            let doc = RELEASED_CONFIG_TOML.replace("\npublic_port = ", &format!("\n{key} = "));
            assert_ne!(
                doc, RELEASED_CONFIG_TOML,
                "the substitution did not fire, so `{key}` is not actually \
                 being exercised"
            );
            let cfg: Config =
                toml::from_str(&doc).unwrap_or_else(|e| panic!("`{key}` must be accepted: {e}"));
            assert_eq!(cfg.network_api.public_port, Some(31339), "{key}");
        }
    }

    /// STRUCTURAL GUARD (#5124): every key the node WRITES is also accepted
    /// hyphenated. Derived from the serialized output rather than from a
    /// hand-written fixture, so a field added later is covered without anyone
    /// remembering to extend a document — which is the exact discipline that
    /// failed and produced this bug.
    ///
    /// Limit worth knowing: this compares re-serialized bytes, so it can only
    /// see a lost binding whose value DIFFERS from what the field falls back
    /// to. A field seeded to its own default round-trips identically and slips
    /// past — see [`config_with_every_field_seeded`]'s seeding contract, and
    /// the set-equality check in
    /// `emitted_config_toml_keys_keep_their_released_spelling`, which catches
    /// that case for any always-emitted key regardless of its value.
    #[tokio::test]
    async fn every_emitted_config_key_is_also_accepted_in_kebab_case() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base = clap_bare_args(temp_dir.path()).build().await.unwrap();
        let seeded = config_with_every_field_seeded(&base);

        let emitted = toml::to_string(&seeded).unwrap();
        let table: toml::Table = toml::from_str(&emitted).unwrap();
        let mut kebabbed = toml::Table::new();
        for (key, value) in &table {
            // Not `.collect()`: that would silently swallow a future pair of
            // keys that kebab to the same string, dropping one from the check.
            assert!(
                kebabbed
                    .insert(key.replace('_', "-"), value.clone())
                    .is_none(),
                "two emitted keys collide when hyphenated, at `{key}`"
            );
        }

        let renamed = table.keys().filter(|k| k.contains('_')).count();
        assert!(
            renamed >= CONFIG_KEY_SPELLINGS.len(),
            "expected at least {} underscored keys to rewrite, saw {renamed} — \
             if emitted keys became kebab-case, this guard and #5130's rollout \
             both need revisiting",
            CONFIG_KEY_SPELLINGS.len()
        );

        let reparsed: Config = toml::from_str(&toml::to_string(&kebabbed).unwrap())
            .expect("the kebab-case spelling of every emitted key must be accepted");
        assert_eq!(
            toml::to_string(&reparsed).unwrap(),
            emitted,
            "a key lost its value when spelled in kebab-case — it is missing a \
             #[serde(alias = \"...\")] for the hyphenated form"
        );
    }

    /// The exact set of `config.toml` keys #5127 shipped emitting with an
    /// underscore, written out INDEPENDENTLY of [`CONFIG_KEY_SPELLINGS`].
    ///
    /// Independent on purpose: a table cannot guard itself. Review demonstrated
    /// a green mutation that REORDERED a group so its kebab spelling came first
    /// and moved the `#[serde(rename)]` to match — self-consistent, no row
    /// deleted, and it is the edit a #5130 author following the table's own
    /// "first entry is what the node writes" instruction would naturally make.
    /// Pinning the count instead of the spellings could not see it.
    ///
    /// Removing an entry here means the node now writes that key under a
    /// spelling shipped releases cannot read. Do it only as part of #5130's
    /// rollout, having confirmed the release rollback would restore already
    /// accepts the new spelling.
    const KEYS_EMITTED_WITH_AN_UNDERSCORE: &[&str] = &[
        "public_network_address",
        "public_port",
        "bandwidth_limit",
        "total_bandwidth_limit",
        "min_bandwidth_per_connection",
        "blocked_addresses",
        "event_loop_channel_capacity",
        "skip_load_from_network",
        "transport_keypair",
        "log_level",
        "contracts_dir",
        "delegates_dir",
        "secrets_dir",
        "db_dir",
        "event_log",
        "data_dir",
        "config_dir",
        "log_dir",
        "wasmtime_cache_dir",
        "is_gateway",
        "max_blocking_threads",
    ];

    /// ROLLBACK SAFETY (#5124 / #5130): the node must keep WRITING the key
    /// spellings older releases can read.
    ///
    /// Crash-loop auto-rollback (#4073, `bin/commands/rollback.rs`) reinstalls
    /// the immediately-previous binary when a freshly-updated node crashes
    /// during probation. `config.toml` is rewritten on the first boot after an
    /// update, so if that rewrite used keys the previous release cannot parse,
    /// the rolled-back binary exits 1 on `missing field ...` — and rollback
    /// does not fire twice, so the node stays down until an operator edits the
    /// file by hand. That turns the brick-safety mechanism into the brick.
    ///
    /// So the emitted spelling may only change once EVERY release that
    /// rollback could restore already accepts the new one. This release makes
    /// the hyphenated spellings accepted; #5130 flips what is emitted, one
    /// release later. Until then this guard fails if the emitted format moves.
    #[tokio::test]
    async fn emitted_config_toml_keys_keep_their_released_spelling() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base = clap_bare_args(temp_dir.path()).build().await.unwrap();
        // Everything in the table must actually be emitted for this guard to
        // see it; `transport_keypair` is the one key sourced from the real
        // build rather than the seed, so assert the premise instead of
        // skipping it — a filter would fail open if `build()` ever stopped
        // setting it.
        assert!(
            base.secrets.transport_keypair_path.is_some(),
            "the seeded config must carry a transport_keypair path, or this \
             guard silently stops covering that key"
        );
        let seeded = config_with_every_field_seeded(&base);
        let table: toml::Table = toml::from_str(&toml::to_string(&seeded).unwrap()).unwrap();

        let emitted_underscored: BTreeSet<&str> = table
            .keys()
            .filter(|key| key.contains('_'))
            .map(String::as_str)
            .collect();
        let tabled: BTreeSet<&str> = CONFIG_KEY_SPELLINGS
            .iter()
            .map(|group| group[0])
            .filter(|key| key.contains('_'))
            .collect();
        let pinned: BTreeSet<&str> = KEYS_EMITTED_WITH_AN_UNDERSCORE.iter().copied().collect();

        // Against the INDEPENDENT list first: this is the assertion that
        // survives the table being edited self-consistently.
        assert_eq!(
            emitted_underscored, pinned,
            "the keys WRITTEN with an underscore no longer match the set this \
             release shipped, which breaks crash-loop rollback (#4073) — read \
             KEYS_EMITTED_WITH_AN_UNDERSCORE's rustdoc and #5130 before \
             changing it"
        );

        // Set equality, not "every tabled key is emitted": the reverse
        // direction is what catches a NEW field landing with an underscored
        // key, and it catches it whatever value it was seeded with — unlike a
        // round-trip comparison, which cannot see a field whose lost binding
        // reproduces the same bytes.
        assert_eq!(
            emitted_underscored, tabled,
            "the set of keys WRITTEN with an underscore has changed. A key \
             here but not in CONFIG_KEY_SPELLINGS is a new field that should \
             have been named kebab-case from the start; a key there but not \
             here means the emitted spelling MOVED, which breaks crash-loop \
             rollback (#4073) — see this test's rustdoc and #5130."
        );
        assert_eq!(
            CONFIG_KEY_SPELLINGS.len(),
            KEYS_EMITTED_WITH_AN_UNDERSCORE.len(),
            "CONFIG_KEY_SPELLINGS gained or lost a group without the pinned \
             list moving with it"
        );
    }

    /// ROLLBACK SAFETY, the operator-edit direction: a config hand-written in
    /// kebab-case is normalized back to the emitted spelling on the first boot.
    ///
    /// That is what keeps an operator's edit from creating a file only new
    /// binaries can read — the same brick #4073 would otherwise hit, arrived at
    /// from the other side. It is a consequence of writing `Config` back out
    /// rather than an explicit mechanism, so it is easy to remove by accident;
    /// pinned so #5130 has to do it deliberately.
    #[tokio::test]
    async fn a_kebab_written_config_is_normalized_to_the_emitted_spelling() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();
        let kebab = kebab_config_toml()
            .lines()
            .filter(|line| {
                !["transport-keypair = ", "nonce = ", "cipher = "]
                    .iter()
                    .any(|key| line.starts_with(key))
            })
            // Point the paths at the temp dir so the build does not touch the
            // developer's real data directories.
            .map(|line| line.replace("/tmp/freenet-5124", &dir.display().to_string()))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(dir.join("config.toml"), kebab).unwrap();

        clap_bare_args(dir).build().await.unwrap();

        let rewritten: toml::Table =
            toml::from_str(&std::fs::read_to_string(dir.join("config.toml")).unwrap()).unwrap();
        for group in CONFIG_KEY_SPELLINGS {
            let emitted = group[0];
            // Every alias, not just the first: `public_port` has two, and the
            // second would otherwise never be checked here.
            for alias in &group[1..] {
                assert!(
                    !rewritten.contains_key(*alias) || *alias == emitted,
                    "`{alias}` survived the write-back; a released binary \
                     cannot read it, so a rollback onto one would not start"
                );
            }
        }
        assert!(
            rewritten.contains_key("log_level"),
            "the rewritten file must use the emitted spelling"
        );
    }

    /// [`RELEASED_CONFIG_TOML`] minus the three secret-path keys.
    ///
    /// `read_config` resolves those paths off disk, so a test that goes through
    /// it (rather than deserializing directly) must not name files that do not
    /// exist. Their aliases are covered by the direct-deserialization tests.
    fn released_config_toml_without_secret_paths() -> String {
        RELEASED_CONFIG_TOML
            .lines()
            .filter(|line| {
                !["transport_keypair = ", "nonce = ", "cipher = "]
                    .iter()
                    .any(|key| line.starts_with(key))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Raw serde REFUSES a file that spells one key two ways — `duplicate
    /// field ...`. Pinned because it is the reason [`redundant_key_spellings`]
    /// exists: a config parse failure is fatal, so without that normalization
    /// accepting a second spelling would turn a file that booted on every
    /// earlier release (where the unrecognized spelling was ignored) into a
    /// node that will not start.
    #[test]
    fn raw_deserialization_rejects_a_key_spelled_two_ways() {
        let doc = format!("{RELEASED_CONFIG_TOML}bandwidth-limit = 999\n");
        let err = toml::from_str::<Config>(&doc).expect_err(
            "raw serde must reject the ambiguous file — read_config normalizes it first",
        );
        assert!(
            err.to_string().contains("duplicate"),
            "expected a duplicate-field error, got: {err}"
        );
    }

    /// REGRESSION (found in review of this PR): a config that spells one key
    /// both ways must still boot, and must keep the value it had before the
    /// upgrade.
    ///
    /// The operator most likely to have such a file is exactly the one who hit
    /// #5124 — tried `bandwidth-limit`, saw nothing happen, added
    /// `bandwidth_limit`, left the dead line. That file works on every shipped
    /// release. Accepting both spellings without this normalization turns it
    /// into `Error: TOML parse error at line 1, column 1 / duplicate field`,
    /// exit 1 — which `bin/commands/rollback.rs` counts as a crash.
    #[test]
    fn a_key_spelled_two_ways_keeps_the_value_the_previous_release_used() {
        let temp_dir = tempfile::tempdir().unwrap();
        // The hyphenated line is the operator's failed attempt; the underscored
        // one is what the node wrote and what the previous release honored.
        let doc = format!(
            "{}\nbandwidth-limit = 999\n",
            released_config_toml_without_secret_paths()
        );
        std::fs::write(temp_dir.path().join("config.toml"), doc).unwrap();

        let cfg = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect("a config spelling one key two ways must still load")
            .expect("config.toml is present");
        assert_eq!(
            cfg.network_api.bandwidth_limit,
            Some(3_000_001),
            "the spelling the node emits must win, so an upgrade never silently \
             changes a node's effective configuration"
        );
        // Every other key must survive the normalized parse too: resolving the
        // ambiguity routes the whole document through a different deserializer
        // (`toml::Value::try_into` rather than `from_str`), so assert the lot
        // rather than the one key the test is named for.
        assert_seeded_values_bound_except_secrets(&cfg);
    }

    /// Same normalization, but between two ALIASES of one key, where neither is
    /// the emitted spelling. Declaration order in [`CONFIG_KEY_SPELLINGS`]
    /// decides, so the outcome is deterministic rather than dependent on the
    /// order the keys happen to appear in the file.
    #[test]
    fn two_aliases_of_one_key_resolve_deterministically() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base = released_config_toml_without_secret_paths();
        // Assert on the SUBSTITUTION, before appending: `doc != base` would
        // hold either way because of the appended line, so that check could
        // never fail. (Which is the defect it was added to prevent — caught in
        // review of the commit that added it.)
        let substituted = base.replace("\npublic_port = ", "\npublic-network-port = ");
        let doc = substituted.clone() + "\npublic-port = 31999\n";
        assert!(
            substituted != base,
            "the substitution did not fire, so this would silently collapse \
             into a duplicate of the emitted-vs-alias test and stop covering \
             alias-vs-alias precedence"
        );
        std::fs::write(temp_dir.path().join("config.toml"), doc).unwrap();

        let cfg = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect("two aliases of one key must not stop the node booting")
            .expect("config.toml is present");
        assert_eq!(cfg.network_api.public_port, Some(31339));
    }

    /// The normalization must not fire on a well-formed file: a file spelling
    /// each key once takes the plain string-parse path, whose errors carry the
    /// line/column spans a value-level parse would lose.
    #[test]
    fn a_well_formed_config_has_no_redundant_spellings() {
        for doc in [RELEASED_CONFIG_TOML.to_string(), kebab_config_toml()] {
            let table: toml::Table = toml::from_str(&doc).unwrap();
            assert!(
                redundant_key_spellings(
                    CONFIG_KEY_SPELLINGS,
                    "config.toml",
                    |key| table.contains_key(key),
                    |key| table.get(key).map(|v| v.to_string()).unwrap_or_default()
                )
                .is_empty(),
                "no key is spelled twice here, so nothing may be dropped"
            );
        }
    }

    /// [`CONFIG_KEY_SPELLINGS`] drives the duplicate-spelling normalization,
    /// and the `#[serde(alias = ...)]` attributes drive what deserializes.
    /// They are two hand-maintained lists of the same fact, so pin that every
    /// spelling in the table is genuinely accepted — a stale entry would
    /// silently drop a key the file legitimately uses.
    #[test]
    fn key_spelling_groups_match_the_serde_aliases() {
        let baseline =
            toml::to_string(&toml::from_str::<Config>(RELEASED_CONFIG_TOML).unwrap()).unwrap();
        for group in CONFIG_KEY_SPELLINGS {
            let emitted = group[0];
            for spelling in *group {
                let doc = RELEASED_CONFIG_TOML
                    .replace(&format!("\n{emitted} = "), &format!("\n{spelling} = "));
                assert!(
                    doc != RELEASED_CONFIG_TOML || *spelling == emitted,
                    "RELEASED_CONFIG_TOML does not carry `{emitted}`, so this \
                     group is untested"
                );
                let parsed = toml::from_str::<Config>(&doc).unwrap_or_else(|e| {
                    panic!("`{spelling}` is in CONFIG_KEY_SPELLINGS but is not accepted: {e}")
                });
                // `is_ok()` alone would prove nothing: `Config` flattens and
                // sets no `deny_unknown_fields`, so an unknown key parses
                // happily and is discarded. Compare against the baseline
                // instead — that is what proves the spelling bound to THIS
                // field with the same value, rather than being ignored.
                assert_eq!(
                    toml::to_string(&parsed).unwrap(),
                    baseline,
                    "`{spelling}` parsed but did not bind to the same field \
                     and value as `{emitted}` — it is being ignored as an \
                     unknown key"
                );
            }
        }
    }

    /// The `config.json` branch of `read_config` got the same two-path rewrite
    /// as the TOML one and had no coverage at all — every other test here is
    /// TOML. Covers both halves: the hyphenated spelling is accepted, and an
    /// ambiguous file resolves to the spelling the node emits.
    #[tokio::test]
    async fn config_json_accepts_both_spellings_and_resolves_duplicates() {
        let as_json = || {
            let table = toml::from_str::<toml::Table>(&released_config_toml_without_secret_paths())
                .unwrap();
            serde_json::to_value(table).unwrap()
        };

        // (a) hyphenated only — the key must bind.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut doc = as_json();
        let object = doc.as_object_mut().unwrap();
        let value = object.remove("bandwidth_limit").unwrap();
        object.insert("bandwidth-limit".to_string(), value);
        std::fs::write(
            temp_dir.path().join("config.json"),
            serde_json::to_string_pretty(&doc).unwrap(),
        )
        .unwrap();
        let cfg = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect("a config.json using the hyphenated key must load")
            .expect("config.json is present");
        assert_eq!(cfg.network_api.bandwidth_limit, Some(3_000_001));

        // (b) both spellings — the emitted one wins, and the file still loads.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut doc = as_json();
        doc.as_object_mut()
            .unwrap()
            .insert("bandwidth-limit".to_string(), 999.into());
        std::fs::write(
            temp_dir.path().join("config.json"),
            serde_json::to_string_pretty(&doc).unwrap(),
        )
        .unwrap();
        let cfg = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect("a config.json naming one key twice must still load")
            .expect("config.json is present");
        assert_eq!(
            cfg.network_api.bandwidth_limit,
            Some(3_000_001),
            "the spelling the node emits must win, as it does for TOML"
        );
    }

    /// REGRESSION (introduced and caught within this PR): a JSON `null` under
    /// one spelling, beside a real value under the other.
    ///
    /// Treating the `null` as merely absent left it in the document, so serde
    /// still saw the field twice and `read_config` failed with `duplicate
    /// field` — the node would not start, on a file that booted before. The
    /// null has to be REMOVED for the surviving spelling to bind.
    #[tokio::test]
    async fn config_json_null_does_not_shadow_or_break_the_other_spelling() {
        for (name, null_key, value_key) in [
            (
                "null under the emitted spelling",
                "bandwidth_limit",
                "bandwidth-limit",
            ),
            ("null under the alias", "bandwidth-limit", "bandwidth_limit"),
        ] {
            let temp_dir = tempfile::tempdir().unwrap();
            let table = toml::from_str::<toml::Table>(&released_config_toml_without_secret_paths())
                .unwrap();
            let mut doc = serde_json::to_value(table).unwrap();
            let object = doc.as_object_mut().unwrap();
            object.insert(null_key.to_string(), serde_json::Value::Null);
            object.insert(value_key.to_string(), 999.into());
            std::fs::write(
                temp_dir.path().join("config.json"),
                serde_json::to_string(&doc).unwrap(),
            )
            .unwrap();

            let cfg = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
                .unwrap_or_else(|e| panic!("{name}: the node must still start: {e}"))
                .unwrap_or_else(|| panic!("{name}: config.json is present"));
            assert_eq!(
                cfg.network_api.bandwidth_limit,
                Some(999),
                "{name}: the real value must win over a null"
            );
        }
    }

    /// A lone `null` still means unset, as it always did.
    #[tokio::test]
    async fn config_json_lone_null_leaves_the_setting_unset() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table =
            toml::from_str::<toml::Table>(&released_config_toml_without_secret_paths()).unwrap();
        let mut doc = serde_json::to_value(table).unwrap();
        doc.as_object_mut()
            .unwrap()
            .insert("bandwidth_limit".to_string(), serde_json::Value::Null);
        std::fs::write(
            temp_dir.path().join("config.json"),
            serde_json::to_string(&doc).unwrap(),
        )
        .unwrap();
        let cfg = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect("a lone null must not be an error")
            .expect("config.json is present");
        assert_eq!(cfg.network_api.bandwidth_limit, None);
    }

    /// The null handling is scoped to groups that are actually ambiguous, so a
    /// lone null keeps exactly the meaning it had before this change — and a
    /// document with no duplicate spelling keeps the direct parse, and with it
    /// its line/column spans.
    ///
    /// Sweeping every null unconditionally, as the first version did, silently
    /// widened two things: a null on a defaulted key started falling back to
    /// the default instead of erroring, and any document containing one lost
    /// its spans.
    #[tokio::test]
    async fn a_lone_json_null_neither_widens_nor_costs_the_spans() {
        let write =
            |dir: &Path, mutate: &dyn Fn(&mut serde_json::Map<String, serde_json::Value>)| {
                let table =
                    toml::from_str::<toml::Table>(&released_config_toml_without_secret_paths())
                        .unwrap();
                let mut doc = serde_json::to_value(table).unwrap();
                mutate(doc.as_object_mut().unwrap());
                std::fs::write(
                    dir.join("config.json"),
                    serde_json::to_string(&doc).unwrap(),
                )
                .unwrap();
            };

        // A lone null on a key whose type rejects it is still an error.
        let temp_dir = tempfile::tempdir().unwrap();
        write(temp_dir.path(), &|object| {
            object.insert("max_blocking_threads".to_string(), serde_json::Value::Null);
        });
        ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect_err("a lone null on a non-Option key must stay an error");

        // And an unrelated type error in a document containing a lone null
        // still reports where it is.
        let temp_dir = tempfile::tempdir().unwrap();
        write(temp_dir.path(), &|object| {
            object.insert("bandwidth_limit".to_string(), serde_json::Value::Null);
            object.insert("ws-api-port".to_string(), "not a port".into());
        });
        let err = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect_err("a type error must still be an error")
            .to_string();
        assert!(
            err.contains("line") && err.contains("column"),
            "a document with no duplicate spelling must keep the \
             span-preserving parse; got: {err}"
        );
    }

    /// REGRESSION (found in review): the same duplicate-spelling case as
    /// `config.toml`, but nested inside `[[gateways]]` — and nastier, because
    /// the failure is INTERMITTENT. The local-cache read on the
    /// remote-index-success path swallows parse errors while the fallback path
    /// propagates them, so a node with such a file runs for months and then
    /// refuses to start the first time freenet.org is unreachable.
    #[test]
    fn gateways_toml_public_key_spelled_two_ways_still_loads() {
        let doc = "[[gateways]]\n\
                   public_key = \"/tmp/freenet-5124/vega.pub\"\n\
                   public-key = \"/tmp/freenet-5124/stale.pub\"\n\
                   location = 0.25\n\
                   \n\
                   [gateways.address]\n\
                   host = \"vega.locut.us\"\n\
                   port = 31337\n";
        assert!(
            toml::from_str::<Gateways>(doc).is_err(),
            "precondition: raw serde rejects the ambiguous file"
        );
        let gateways = parse_gateways_toml(doc, "gateways.toml")
            .expect("a gateways.toml naming one key twice must still load");
        assert_eq!(
            gateways.gateways[0].public_key_path,
            PathBuf::from("/tmp/freenet-5124/vega.pub"),
            "the spelling the node writes must win, as it does for config.toml"
        );
        // The rest of the entry must survive the normalized parse too — it goes
        // through a different deserializer than the direct path.
        assert_eq!(gateways.gateways[0].location, Some(0.25));
    }

    /// The warning must name the source it was given, not a hardcoded file.
    /// `parse_gateways_toml` also parses the REMOTE index, where a duplicate
    /// spelling would otherwise send every operator on the network to edit an
    /// innocent local file — so the label being a parameter is the point, and
    /// nothing else pins it.
    #[test]
    fn the_gateways_warning_names_the_source_it_was_given() {
        let table: toml::Table =
            toml::from_str("public_key = \"/tmp/a.pub\"\npublic-key = \"/tmp/b.pub\"\n").unwrap();
        for source in ["gateways.toml", "https://freenet.org/keys/gateways.toml"] {
            let reported = redundant_key_spellings(
                GATEWAY_KEY_SPELLINGS,
                source,
                |key| table.contains_key(key),
                |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
            );
            let [(_, message)] = reported.as_slice() else {
                panic!("expected one redundant spelling, got {reported:?}");
            };
            assert!(
                message.contains(source),
                "the warning must name `{source}`; got: {message}"
            );
        }
    }

    /// Both emissions must survive.
    ///
    /// The `eprintln!` is the load-bearing one: `read_config` runs inside
    /// `ConfigArgs::build`, one line before `set_logger`, so no subscriber
    /// exists yet and the tracing event alone goes nowhere. Deleting it returns
    /// the operator to silence about a setting the node just ignored — which is
    /// the #5124 failure itself, arriving through the fix for it. Deleting
    /// either was otherwise green.
    ///
    /// Source-scraped, and honest about being so: capturing stderr in-process
    /// is awkward, and a scrape that says why beats no guard at all.
    #[test]
    fn the_duplicate_spelling_warning_is_still_emitted_both_ways() {
        let source = include_str!("config.rs");
        let body = source
            .split("fn redundant_key_spellings(")
            .nth(1)
            .expect("redundant_key_spellings must exist")
            .split("\nfn ")
            .next()
            .expect("its body must be delimited by the next item");
        // Needles assembled at runtime and matched on the CALL, not the macro
        // name: written literally they would match this test's own text, and
        // the bare macro name also matches the prose comment beside the
        // emissions. Both traps were hit writing this.
        for emission in [
            format!("tracing::warn!(\"{{{}}}\")", "message"),
            format!("eprintln!(\"warning: {{{}}}\")", "message"),
        ] {
            assert!(
                body.contains(&emission),
                "`redundant_key_spellings` must still emit `{emission}` — see \
                 this test's rustdoc for why both are needed"
            );
        }
    }

    /// ...and the remote-index call site must actually PASS its URL.
    ///
    /// The message-formatting test above pins what `redundant_key_spellings`
    /// produces; this pins the one line that decides which source it is told
    /// about. Reverting that argument to a hardcoded `"gateways.toml"` is
    /// otherwise invisible — it was, and that is the whole of what the commit
    /// threading `source` through changed. Source-scraped because exercising it
    /// for real would mean serving a duplicate-spelling index over HTTP and
    /// capturing stderr.
    #[test]
    fn the_remote_gateway_index_is_parsed_under_its_own_url() {
        let source = include_str!("config.rs");
        // Assembled at runtime: spelled out as a literal, the needle would
        // appear in this test's own text and match itself. (It did.)
        let needle = format!("parse_gateways_toml(&response, {})", "url");
        assert!(
            source.contains(&needle),
            "load_gateways_from_index must pass the index URL as the source \
             label, or a duplicate spelling published upstream tells every \
             operator on the network to edit their own innocent gateways.toml"
        );
    }

    /// The gateways two-path parse has the same justification as the config
    /// one, and the same risk of being read as dead weight later.
    #[test]
    fn an_unambiguous_broken_gateways_toml_keeps_its_line_and_column() {
        let doc = "[[gateways]]\npublic_key = 42\n\n[gateways.address]\nhost = \"a\"\n";
        let err = parse_gateways_toml(doc, "gateways.toml")
            .expect_err("a type error must still be an error")
            .to_string();
        assert!(
            err.contains("line") && err.contains("column"),
            "an unambiguous file must take the span-preserving parse; got: {err}"
        );
    }

    /// `host_address` is `gateways.toml`'s OTHER key with this history — the
    /// legacy single-string address form, and what the node emits for
    /// `Address::HostAddress`. Hyphenating the key their own file contains gave
    /// operators `gateway address must specify one of ...` naming the key they
    /// had just specified.
    #[test]
    fn gateways_toml_host_address_is_accepted_in_both_spellings() {
        for key in ["host_address", "host-address"] {
            let doc = format!(
                "[[gateways]]\npublic_key = \"/tmp/freenet-5124/vega.pub\"\n\
                 \n[gateways.address]\n{key} = \"203.0.113.1:31337\"\n"
            );
            let gateways = parse_gateways_toml(&doc, "gateways.toml")
                .unwrap_or_else(|e| panic!("`{key}` must be accepted: {e}"));
            assert_eq!(
                gateways.gateways[0].address,
                Address::HostAddress("203.0.113.1:31337".parse().unwrap()),
                "{key}"
            );
        }
    }

    /// ... and naming it both ways must not stop the node booting, the same as
    /// one level up.
    #[test]
    fn gateways_toml_host_address_spelled_two_ways_still_loads() {
        let doc = "[[gateways]]\n\
                   public_key = \"/tmp/freenet-5124/vega.pub\"\n\
                   \n\
                   [gateways.address]\n\
                   host_address = \"203.0.113.1:31337\"\n\
                   host-address = \"198.51.100.9:1234\"\n";
        assert!(
            toml::from_str::<Gateways>(doc).is_err(),
            "precondition: raw serde rejects the ambiguous nested table"
        );
        let gateways = parse_gateways_toml(doc, "gateways.toml")
            .expect("a nested address naming one key twice must still load");
        assert_eq!(
            gateways.gateways[0].address,
            Address::HostAddress("203.0.113.1:31337".parse().unwrap()),
            "the spelling the node writes must win"
        );
    }

    /// The two-path parse exists so an ordinary broken file keeps its
    /// line/column span, which a value-level parse loses. Pinned, because once
    /// #5130 makes normalization common someone will read the direct branch as
    /// dead weight.
    #[test]
    fn an_unambiguous_broken_config_keeps_its_line_and_column() {
        let temp_dir = tempfile::tempdir().unwrap();
        let doc = released_config_toml_without_secret_paths().replace(
            "max_blocking_threads = 17",
            "max_blocking_threads = \"not a number\"",
        );
        std::fs::write(temp_dir.path().join("config.toml"), doc).unwrap();
        let err = ConfigArgs::read_config(&temp_dir.path().to_path_buf())
            .expect_err("a type error must still be an error")
            .to_string();
        assert!(
            err.contains("line") && err.contains("column"),
            "an unambiguous file must take the span-preserving parse; got: {err}"
        );
    }

    /// The warning is the whole compensation for the precedence rule ignoring
    /// the operator's newer line, so pin what it says: both keys, both values,
    /// which one is in effect, and which line to delete.
    #[test]
    fn the_duplicate_spelling_warning_names_both_values_and_the_remedy() {
        let table: toml::Table =
            toml::from_str("bandwidth_limit = 10000000\nbandwidth-limit = 50000000\n").unwrap();
        let reported = redundant_key_spellings(
            CONFIG_KEY_SPELLINGS,
            "config.toml",
            |key| table.contains_key(key),
            |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
        );
        let [(ignored, message)] = reported.as_slice() else {
            panic!("expected exactly one redundant spelling, got {reported:?}");
        };
        assert_eq!(*ignored, "bandwidth-limit");
        for expected in [
            "bandwidth-limit",                   // the key being ignored
            "= 50000000",                        // ... its value, bare not re-quoted
            "bandwidth_limit",                   // the key that wins
            "= 10000000",                        // ... and the value now in effect
            "delete the `bandwidth_limit` line", // the remedy
            "config.toml",                       // where to do it
        ] {
            assert!(
                message.contains(expected),
                "the warning must mention `{expected}`; got: {message}"
            );
        }

        // A string value keeps exactly ONE level of quoting. Escaping the TOML
        // rendering a second time turned every path into `"\"/srv\""` and every
        // integer into a quoted string — making the message worse than it was
        // before it was made safe.
        let table: toml::Table =
            toml::from_str("data_dir = \"/var/lib/freenet\"\ndata-dir = \"/srv/freenet\"\n")
                .unwrap();
        let reported = redundant_key_spellings(
            CONFIG_KEY_SPELLINGS,
            "config.toml",
            |key| table.contains_key(key),
            |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
        );
        let [(_, message)] = reported.as_slice() else {
            panic!("expected one redundant spelling");
        };
        assert!(
            message.contains("= \"/srv/freenet\""),
            "a string value must be quoted once, not escaped twice; got: {message}"
        );

        // ...but a value that could forge a log line is escaped onto one line.
        let table: toml::Table = toml::from_str(
            "data_dir = \"/var/lib/freenet\"\ndata-dir = \"\"\"\nwarning: forged\n/srv\"\"\"\n",
        )
        .unwrap();
        let reported = redundant_key_spellings(
            CONFIG_KEY_SPELLINGS,
            "config.toml",
            |key| table.contains_key(key),
            |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
        );
        let [(_, message)] = reported.as_slice() else {
            panic!("expected one redundant spelling");
        };
        assert!(
            !message.contains('\n'),
            "a newline-bearing value must not break the message onto a second \
             line, or a remote index could forge log entries; got: {message}"
        );

        // The same for the Unicode line separators. They cannot forge a line in
        // journald or stderr, which split on `\n` — this is for a downstream
        // consumer that treats them as breaks, and it is here so the belt-and-
        // braces cannot be dropped silently.
        let table: toml::Table =
            toml::from_str("data_dir = \"/var/lib/freenet\"\ndata-dir = \"a\\u2028b\\u2029c\"\n")
                .unwrap();
        let reported = redundant_key_spellings(
            CONFIG_KEY_SPELLINGS,
            "config.toml",
            |key| table.contains_key(key),
            |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
        );
        let [(_, message)] = reported.as_slice() else {
            panic!("expected one redundant spelling");
        };
        assert!(
            !message.contains(['\u{2028}', '\u{2029}']),
            "Unicode line separators must be escaped out of the message; got: \
             {message}"
        );

        // ...and a very long one is capped. The third leg of the same
        // hardening: without it a hostile index can flood the journal with a
        // single enormous value rather than with extra lines.
        let long = "x".repeat(400);
        let table: toml::Table =
            toml::from_str(&format!("data_dir = \"/var\"\ndata-dir = \"{long}\"\n")).unwrap();
        let reported = redundant_key_spellings(
            CONFIG_KEY_SPELLINGS,
            "config.toml",
            |key| table.contains_key(key),
            |key| table.get(key).map(|v| v.to_string()).unwrap_or_default(),
        );
        let [(_, message)] = reported.as_slice() else {
            panic!("expected one redundant spelling");
        };
        assert!(
            message.contains("(truncated)") && !message.contains(&long),
            "a very long value must be capped; got {} chars",
            message.len()
        );
    }

    /// Every `#[serde(alias = "...")]` in the config types must appear in
    /// [`CONFIG_KEY_SPELLINGS`] or [`GATEWAY_KEY_SPELLINGS`].
    ///
    /// `key_spelling_groups_match_the_serde_aliases` checks the safe direction
    /// — that everything listed is accepted. This checks the DANGEROUS one: an
    /// alias added without a table row means a file naming that key both ways
    /// is never normalized, so it hard-fails the node with `duplicate field`.
    /// That is the fatal case this whole change exists to prevent, and it would
    /// arrive with every other test green.
    ///
    /// Scraped from source because serde aliases are not reflectable at
    /// runtime. #5130 will add and move many of these, which is exactly when a
    /// row is easiest to forget.
    #[test]
    fn every_serde_alias_is_listed_in_a_spelling_table() {
        let listed: BTreeSet<&str> = CONFIG_KEY_SPELLINGS
            .iter()
            .chain(GATEWAY_KEY_SPELLINGS.iter())
            .chain(GATEWAY_ADDRESS_KEY_SPELLINGS.iter())
            .flat_map(|group| group.iter().copied())
            .collect();

        let mut found = 0usize;
        for (file, source) in [
            ("config.rs", include_str!("config.rs")),
            ("config/secret.rs", include_str!("config/secret.rs")),
        ] {
            for (line_no, line) in source.lines().enumerate() {
                // Anywhere in the line: aliases share an attribute with
                // `default` / `rename` / `skip_serializing_if` as often as not.
                for occurrence in line.split("alias = \"").skip(1) {
                    let Some(spelling) = occurrence.split('"').next() else {
                        continue;
                    };
                    // Skip the illustrative `alias = "..."` in prose.
                    // `_` included deliberately: every alias #5130 adds is
                    // the UNDERSCORED spelling of a key it flips to kebab, so
                    // excluding them would blind this guard in exactly the
                    // scenario its rustdoc names. The illustrative
                    // `alias = "..."` in prose is still skipped, by the `.`.
                    if spelling.is_empty()
                        || !spelling.chars().all(|c| {
                            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_'
                        })
                    {
                        continue;
                    }
                    found += 1;
                    assert!(
                        listed.contains(spelling),
                        "{file}:{} declares `alias = \"{spelling}\"` but no \
                         spelling table lists it, so a config naming that key \
                         both ways would refuse to start instead of being \
                         resolved",
                        line_no + 1
                    );
                }
            }
        }
        // A floor, so a scraper that silently stops matching cannot pass by
        // finding nothing. Every group contributes at least one alias.
        let expected = CONFIG_KEY_SPELLINGS.len()
            + GATEWAY_KEY_SPELLINGS.len()
            + GATEWAY_ADDRESS_KEY_SPELLINGS.len();
        assert!(
            found >= expected,
            "scraped only {found} aliases but {expected} spelling groups exist \
             — the scraper has probably stopped matching the attribute format"
        );
    }

    /// `gateways.toml` is hand-edited by operators too, and its `public_key`
    /// has no serde default — spelling it hyphenated gave `missing field
    /// public_key` and a node that would not start. The key is still WRITTEN
    /// underscored: the same format is served by the remote gateway index, so
    /// renaming it is a wire change (unlike accepting a second spelling).
    #[test]
    fn gateways_toml_public_key_is_accepted_in_both_spellings() {
        for key in ["public_key", "public-key"] {
            let doc = format!(
                "[[gateways]]\naddress = {{ host = \"vega.locut.us\", port = 31337 }}\n\
                 {key} = \"/tmp/freenet-5124/vega.pub\"\n"
            );
            let gateways: Gateways = toml::from_str(&doc)
                .unwrap_or_else(|e| panic!("`{key}` must be accepted in gateways.toml: {e}"));
            assert_eq!(
                gateways.gateways[0].public_key_path,
                PathBuf::from("/tmp/freenet-5124/vega.pub"),
                "{key}"
            );
        }
    }

    #[test]
    fn otel_args_default_is_off_and_endpointless() {
        // The new pipeline exports nothing yet, so shipping it on would be a
        // behavior change. Operators opt in explicitly.
        let args = OtelArgs::default();
        assert!(
            !args.enabled,
            "otel-telemetry-enabled must default to false"
        );
        assert_eq!(args.endpoint, None, "no implicit collector");
    }

    #[test]
    fn otel_flag_parses_from_cli() {
        use clap::Parser;
        let none = ConfigArgs::try_parse_from(["freenet"]).expect("bare parse");
        assert!(!none.otel.enabled, "no flag -> off");
        let set = ConfigArgs::try_parse_from(["freenet", "--otel-telemetry-enabled"])
            .expect("flag parse");
        assert!(set.otel.enabled, "--otel-telemetry-enabled -> on");
        // Explicit `=false` must parse and mean false. Without this form the flag
        // would be a bare ArgAction::SetTrue, and clap turns ANY value of the bound
        // env var — including "false" — into true.
        let off = ConfigArgs::try_parse_from(["freenet", "--otel-telemetry-enabled=false"])
            .expect("explicit false parse");
        assert!(!off.otel.enabled, "--otel-telemetry-enabled=false -> off");
        let with_ep = ConfigArgs::try_parse_from([
            "freenet",
            "--otel-endpoint",
            "http://collector.example:4318",
        ])
        .expect("endpoint parse");
        assert_eq!(
            with_ep.otel.endpoint.as_deref(),
            Some("http://collector.example:4318")
        );
    }

    /// C1 regression: the round-trip guard test above only round-trips the
    /// serializer's OWN output, so a key-shape mismatch (nested `[otel]`
    /// table vs. the flat keys the design spec and AGENTS.md document) is
    /// invisible to it. Write the literal documented `config.toml` text and
    /// confirm the flat keys actually parse into `Config::otel`.
    #[tokio::test]
    async fn otel_flat_config_toml_keys_are_honored() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Base build to create the on-disk secrets + a valid config.toml for
        // every OTHER field (all of them are `#[serde(flatten)]`d scalars, so
        // this baseline has no `[table]` headers at all).
        clap_bare_args(temp_dir.path()).build().await.unwrap();
        let base = tokio::fs::read_to_string(temp_dir.path().join("config.toml"))
            .await
            .unwrap();

        // Strip whatever otel shape build() just wrote (pre-fix: a nested
        // `[otel]` header + its two keys; post-fix: the two flat keys) so the
        // literal lines appended below are unambiguous root-level keys.
        let base: String = base
            .lines()
            .filter(|line| {
                *line != "[otel]"
                    && !line.starts_with("otel-telemetry-enabled")
                    && !line.starts_with("otel-endpoint")
            })
            .map(|line| format!("{line}\n"))
            .collect();

        // The literal config.toml the design spec (Configuration table) and
        // AGENTS.md document: flat keys at the file root, no `[otel]` table.
        let literal = format!(
            "{base}otel-telemetry-enabled = true\notel-endpoint = \"http://collector.example:4318\"\n"
        );
        std::fs::write(temp_dir.path().join("config.toml"), literal).unwrap();

        let rebuilt = clap_bare_args(temp_dir.path()).build().await.unwrap();
        assert!(
            rebuilt.otel.enabled,
            "documented flat `otel-telemetry-enabled` key must be honored"
        );
        assert_eq!(
            rebuilt.otel.endpoint.as_deref(),
            Some("http://collector.example:4318"),
            "documented flat `otel-endpoint` key must be honored"
        );
    }

    #[test]
    fn otel_endpoint_never_defaults_to_the_dashboard_collector() {
        // Hard isolation requirement: the two pipelines share no backend.
        assert_ne!(
            DEFAULT_OTEL_ENDPOINT, DEFAULT_TELEMETRY_ENDPOINT,
            "otel must not default to the central dashboard collector"
        );
        assert_eq!(DEFAULT_OTEL_ENDPOINT, "http://localhost:4318");
    }

    #[tokio::test]
    async fn test_serde_config_args() {
        // Use tempfile for a guaranteed-writable directory (avoids CI permission issues on /tmp)
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        let serialized = toml::to_string(&cfg).unwrap();
        let _: Config = toml::from_str(&serialized).unwrap();
    }

    #[tokio::test]
    async fn max_hosting_storage_defaults_to_ram_scaled_clamped() {
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        // The default is now capability-relative (RAM-scaled, #4642 A2), not a
        // flat 1 GiB. It must resolve to the hosting cache's single-source-of-
        // truth default and land within the documented clamp range on any host.
        // Reference the constants rather than hardcoded byte values so this test
        // never drifts from the clamp.
        let min = crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES;
        let max = crate::ring::MAX_DEFAULT_HOSTING_BUDGET_BYTES;
        assert_eq!(
            cfg.max_hosting_storage,
            crate::ring::default_hosting_budget_bytes(),
            "default max_hosting_storage should resolve to the hosting cache's \
             single-source-of-truth RAM-scaled default budget"
        );
        assert!(
            (min..=max).contains(&cfg.max_hosting_storage),
            "default budget {} must be within the [{min}, {max}] clamp",
            cfg.max_hosting_storage
        );
    }

    /// Hosted mode (P2 of #4381) is OFF unless explicitly enabled. This is the
    /// inert-by-default guarantee: a node built with no hosted-mode flag never
    /// honors a user token and stays single-user.
    #[tokio::test]
    async fn hosted_mode_defaults_to_off() {
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        assert!(
            !cfg.ws_api.hosted_mode,
            "hosted_mode must default to false (inert unless explicitly enabled)"
        );
    }

    /// Build a `ConfigArgs` rooted at `dir` in the given mode. Shared by the
    /// #4968 event-log default tests so each case differs only in what it sets.
    fn event_log_args(dir: &std::path::Path, mode: OperationMode) -> ConfigArgs {
        super::event_log_test_args(dir, mode)
    }

    /// #4968: a network-mode node (what end users run) must NOT write the local
    /// diagnostic event log by default. On a live 0.2.111 peer that log was
    /// ~61 MiB/hour of appends and 95% of every fsync the process issued.
    #[tokio::test]
    async fn event_log_defaults_off_in_network_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cfg = event_log_args(temp_dir.path(), OperationMode::Network)
            .build()
            .await
            .unwrap();
        assert!(
            !cfg.event_log_enabled(),
            "network mode must default to event log OFF"
        );
    }

    /// #4968: local mode is a single-node dev mode where the log is the point,
    /// and `fdev verify-state` consumes `_EVENT_LOG_LOCAL`. It stays ON.
    #[tokio::test]
    async fn event_log_defaults_on_in_local_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cfg = event_log_args(temp_dir.path(), OperationMode::Local)
            .build()
            .await
            .unwrap();
        assert!(
            cfg.event_log_enabled(),
            "local mode must default to event log ON so fdev verify-state keeps working"
        );
    }

    /// An explicit setting overrides the mode-dependent default in BOTH
    /// directions — on for a network node we operate, off for a local one.
    #[tokio::test]
    async fn event_log_explicit_setting_overrides_mode_default() {
        let on_dir = tempfile::tempdir().unwrap();
        let mut on = event_log_args(on_dir.path(), OperationMode::Network);
        on.enable_event_log = Some(true);
        assert!(
            on.build().await.unwrap().event_log_enabled(),
            "explicit true must enable the log on a network node"
        );

        let off_dir = tempfile::tempdir().unwrap();
        let mut off = event_log_args(off_dir.path(), OperationMode::Local);
        off.enable_event_log = Some(false);
        assert!(
            !off.build().await.unwrap().event_log_enabled(),
            "explicit false must disable the log even in local mode"
        );
    }

    /// Regression (#4968, the #3890/#4275 silent-revert class): building twice
    /// against the SAME config dir must not flip local mode's default off.
    ///
    /// The first build persists a `config.toml`. Because `enable_event_log` is
    /// `None` it is `skip_serializing_if`-omitted, so that file is byte-identical
    /// in this respect to one written by a pre-#4968 release. If the merge step
    /// treated the absent key as an explicit `false`, the second build would
    /// silently strip the event log from every upgrading local-mode node and
    /// break `fdev verify-state`. It must still resolve to ON.
    #[tokio::test]
    async fn event_log_absent_config_key_does_not_pin_local_mode_off() {
        let temp_dir = tempfile::tempdir().unwrap();

        let first = event_log_args(temp_dir.path(), OperationMode::Local)
            .build()
            .await
            .unwrap();
        assert!(first.event_log_enabled(), "precondition: first build is ON");

        let persisted = tokio::fs::read_to_string(temp_dir.path().join("config.toml"))
            .await
            .expect("first build must persist a config.toml");
        assert!(
            !persisted.contains("enable-event-log"),
            "precondition: an unset event-log flag must be omitted from config.toml, \
             otherwise this test is not exercising the pre-#4968 upgrade shape. Got:\n{persisted}"
        );

        let second = event_log_args(temp_dir.path(), OperationMode::Local)
            .build()
            .await
            .unwrap();
        assert!(
            second.event_log_enabled(),
            "a config.toml with no enable-event-log key must NOT pin local mode to OFF"
        );
    }

    /// The opposite direction of the merge: an operator who sets
    /// `enable-event-log = true` in config.toml (rather than passing the CLI
    /// flag every start) must have it honored on the next boot.
    #[tokio::test]
    async fn event_log_persisted_true_is_honored_on_reboot() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut args = event_log_args(temp_dir.path(), OperationMode::Network);
        args.enable_event_log = Some(true);
        assert!(args.build().await.unwrap().event_log_enabled());

        let persisted = tokio::fs::read_to_string(temp_dir.path().join("config.toml"))
            .await
            .unwrap();
        assert!(
            persisted.contains("enable-event-log"),
            "an explicit setting must be written to config.toml. Got:\n{persisted}"
        );

        // Reboot with NO CLI flag: the persisted value must survive.
        let rebooted = event_log_args(temp_dir.path(), OperationMode::Network)
            .build()
            .await
            .unwrap();
        assert!(
            rebooted.event_log_enabled(),
            "enable-event-log = true in config.toml must survive a reboot without the CLI flag"
        );
    }

    /// `--enable-event-log` uses clap's `num_args = 0..=1` +
    /// `default_missing_value` form, which is easy to get subtly wrong (a bare
    /// flag silently parsing as `None`, or `=false` being rejected). The
    /// identical `--hosted-mode` pattern carries the same test for that reason.
    ///
    /// Without this, every other event-log test would still pass while the
    /// operator-facing flag did nothing — the tests set the field directly.
    #[test]
    fn enable_event_log_cli_accepts_bare_flag_and_explicit_value() {
        use clap::Parser;

        // The arg also reads FREENET_ENABLE_EVENT_LOG via clap's `env`. Clear it
        // for the duration of this test so the runner's environment can't mask
        // the CLI-form assertions, then restore it.
        let saved = std::env::var_os("FREENET_ENABLE_EVENT_LOG");
        // SAFETY: this is the only test that touches FREENET_ENABLE_EVENT_LOG,
        // and it restores the prior value below; nextest per-process isolation
        // means no other thread observes the transient unset.
        unsafe {
            std::env::remove_var("FREENET_ENABLE_EVENT_LOG");
        }

        // Absent => None, so the mode-dependent default applies.
        let absent = ConfigArgs::try_parse_from(["freenet"]).expect("bare argv should parse");
        assert_eq!(
            absent.enable_event_log, None,
            "an absent flag must stay None so the mode default can apply"
        );

        // Bare `--enable-event-log` => Some(true) via default_missing_value.
        let bare = ConfigArgs::try_parse_from(["freenet", "--enable-event-log"])
            .expect("bare --enable-event-log should parse");
        assert_eq!(
            bare.enable_event_log,
            Some(true),
            "bare --enable-event-log must mean Some(true)"
        );

        // `--enable-event-log=false` => Some(false), the explicit opt-out.
        let explicit_false = ConfigArgs::try_parse_from(["freenet", "--enable-event-log=false"])
            .expect("--enable-event-log=false should parse");
        assert_eq!(
            explicit_false.enable_event_log,
            Some(false),
            "--enable-event-log=false must mean Some(false)"
        );

        // Space-separated value form.
        let spaced = ConfigArgs::try_parse_from(["freenet", "--enable-event-log", "true"])
            .expect("--enable-event-log true should parse");
        assert_eq!(
            spaced.enable_event_log,
            Some(true),
            "--enable-event-log true must mean Some(true)"
        );

        // SAFETY: restoring the value captured above; same rationale as the
        // remove_var at the top of this test.
        unsafe {
            if let Some(v) = saved {
                std::env::set_var("FREENET_ENABLE_EVENT_LOG", v);
            }
        }
    }

    /// When explicitly enabled, hosted mode resolves to `true` and survives a
    /// TOML round-trip (so it works from a config file, not just the CLI flag).
    #[tokio::test]
    async fn hosted_mode_explicit_true_round_trips() {
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ws_api: WebsocketApiArgs {
                hosted_mode: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        assert!(
            cfg.ws_api.hosted_mode,
            "explicit --hosted-mode should resolve to true"
        );

        let serialized = toml::to_string(&cfg).unwrap();
        let reparsed: Config = toml::from_str(&serialized).unwrap();
        assert!(
            reparsed.ws_api.hosted_mode,
            "hosted_mode=true must survive a TOML serialize/deserialize round-trip"
        );
    }

    /// `--hosted-mode` is the operator switch for the feature, so it MUST work as
    /// a BARE flag (clap optional-value form), while staying `Option<bool>` so
    /// config-file/env layering can leave it unset. Asserts the three forms:
    ///   bare `--hosted-mode`        => Some(true)
    ///   `--hosted-mode=false`       => Some(false)
    ///   absent                      => None
    #[test]
    fn hosted_mode_cli_accepts_bare_flag_and_explicit_value() {
        use clap::Parser;

        // The arg also reads FREENET_HOSTED_MODE via clap's `env`. Clear it for
        // the duration of this test so the env of the test runner can't mask the
        // CLI-form assertions, then restore it.
        let saved = std::env::var_os("FREENET_HOSTED_MODE");
        // SAFETY: this is the only test that touches FREENET_HOSTED_MODE, and it
        // restores the prior value below; nextest per-process isolation means no
        // other thread observes the transient unset.
        unsafe {
            std::env::remove_var("FREENET_HOSTED_MODE");
        }

        // Bare `--hosted-mode` => Some(true) (default_missing_value).
        let bare = ConfigArgs::try_parse_from(["freenet", "--hosted-mode"])
            .expect("bare --hosted-mode should parse");
        assert_eq!(
            bare.ws_api.hosted_mode,
            Some(true),
            "bare --hosted-mode must mean Some(true)"
        );

        // `--hosted-mode=false` => Some(false) (explicit override off).
        let explicit_false = ConfigArgs::try_parse_from(["freenet", "--hosted-mode=false"])
            .expect("--hosted-mode=false should parse");
        assert_eq!(
            explicit_false.ws_api.hosted_mode,
            Some(false),
            "--hosted-mode=false must mean Some(false)"
        );

        // `--hosted-mode true` (space-separated value) => Some(true).
        let explicit_true = ConfigArgs::try_parse_from(["freenet", "--hosted-mode", "true"])
            .expect("--hosted-mode true should parse");
        assert_eq!(
            explicit_true.ws_api.hosted_mode,
            Some(true),
            "--hosted-mode true must mean Some(true)"
        );

        // Absent => None (so config-file/env can still supply the value, and
        // `build()` resolves None to false).
        let absent =
            ConfigArgs::try_parse_from(["freenet"]).expect("no hosted-mode flag should parse");
        assert_eq!(
            absent.ws_api.hosted_mode, None,
            "absent --hosted-mode must leave it None for config/env layering"
        );

        // Restore the env var for any other test in this process.
        // SAFETY: restores the value saved above; same single-test /
        // nextest-isolation rationale as the unset.
        unsafe {
            if let Some(v) = saved {
                std::env::set_var("FREENET_HOSTED_MODE", v);
            }
        }
    }

    #[tokio::test]
    async fn max_hosting_storage_explicit_value_round_trips() {
        let temp_dir = tempfile::tempdir().unwrap();
        // 2 GiB is deliberately ABOVE the auto-derived clamp ceiling (1 GiB), so
        // an explicit operator value can never coincide with the RAM-scaled
        // default and is therefore always persisted / honored across restarts —
        // regardless of the CI host's real RAM (a 2 GiB runner's derived default
        // is 256 MiB, which a 256 MiB test value would collide with once
        // `skip_serializing_if` drops the auto-derived value; #4565 fix).
        let custom = 2 * 1024 * 1024 * 1024_u64;
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            max_hosting_storage: Some(custom),
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        assert_eq!(cfg.max_hosting_storage, custom);

        // An explicit override is persisted (NOT skipped) and round-trips.
        let serialized = toml::to_string(&cfg).unwrap();
        assert!(
            serialized.contains("max-hosting-storage"),
            "an explicit operator value must be persisted, got:\n{serialized}"
        );
        let deserialized: Config = toml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.max_hosting_storage, custom);
    }

    /// First-boot-pinning fix (#4565): a node that first-boots with the
    /// auto-derived (RAM-scaled) budget must NOT bake it into `config.toml`, so a
    /// later boot re-derives from live RAM. Concretely: (1) the auto-derived
    /// value is omitted from the serialized config, and (2) a `config.toml`
    /// without the key rebuilds to the live-RAM default rather than a pinned
    /// value. Combined with the RAM-scaling proof in `cache.rs`
    /// (`budget_for_ram_scales_and_clamps`), this means a node that first-boots on
    /// a large box and restarts on a smaller box / tighter cgroup gets the
    /// SMALLER budget, not the pinned old one.
    #[tokio::test]
    async fn auto_derived_hosting_budget_is_not_persisted_and_re_derives() {
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            // No explicit max_hosting_storage -> resolves to the auto-derived
            // RAM-scaled default.
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        assert_eq!(
            cfg.max_hosting_storage,
            crate::ring::default_hosting_budget_bytes(),
            "with no operator value, the budget must be the auto-derived default"
        );

        // (1) The auto-derived value must be OMITTED from config.toml, so nothing
        // is pinned for the next boot to inherit.
        let serialized = toml::to_string(&cfg).unwrap();
        assert!(
            !serialized.contains("max-hosting-storage"),
            "the auto-derived budget must not be pinned into config.toml, got:\n{serialized}"
        );

        // (2) A config.toml WITHOUT the key rebuilds to the live-RAM default
        // (serde `default = default_max_hosting_storage`), i.e. it re-derives
        // rather than reverting to a stale pinned value. On a smaller box the
        // rebuilt value would be smaller; here (same host) it equals the current
        // derived default.
        let rebuilt: Config = toml::from_str(&serialized).unwrap();
        assert_eq!(
            rebuilt.max_hosting_storage,
            crate::ring::default_hosting_budget_bytes(),
            "a config.toml without the key must re-derive the budget from live RAM"
        );
    }

    /// #4565 upgrade migration: an existing `config.toml` from a pre-A2 release
    /// pinned the OLD flat 1 GiB default as `max-hosting-storage = 1073741824`.
    /// `skip_serializing_if` alone only stops NEW configs from pinning, so on
    /// upgrade that historical value must be treated as auto-derived and
    /// re-derived from live RAM — otherwise a small box that upgraded keeps the
    /// 1 GiB budget and stays on the #4565 OOM path.
    ///
    /// `default_hosting_budget_bytes()` reads the test host's real RAM, so on a
    /// >= 8 GiB host the re-derived value coincidentally equals 1 GiB. The
    /// control case (b) — an explicit non-legacy value must survive — is what
    /// makes this test meaningful regardless of host RAM; on a small /
    /// cgroup-limited host, case (a) additionally fails without the migration.
    #[tokio::test]
    async fn legacy_pinned_hosting_budget_re_derives_but_explicit_survives() {
        // (a) A legacy config.toml pinning the exact 1 GiB sentinel. Generate a
        // valid config first (the auto value is skip-serialized), then inject the
        // historical line to mimic the pre-A2 on-disk state, then rebuild as an
        // "upgrade boot".
        let legacy_dir = tempfile::tempdir().unwrap();
        clap_bare_args(legacy_dir.path()).build().await.unwrap();
        let cfg_path = legacy_dir.path().join("config.toml");
        let existing = std::fs::read_to_string(&cfg_path).unwrap();
        assert!(
            !existing.contains("max-hosting-storage"),
            "fresh build must not persist the auto-derived value, got:\n{existing}"
        );
        // Top-level key prepended before any table header (valid TOML).
        std::fs::write(
            &cfg_path,
            format!("max-hosting-storage = 1073741824\n{existing}"),
        )
        .unwrap();
        let upgraded = clap_bare_args(legacy_dir.path()).build().await.unwrap();
        assert_eq!(
            upgraded.max_hosting_storage,
            crate::ring::default_hosting_budget_bytes(),
            "a legacy auto-persisted 1 GiB default must re-derive from live RAM \
             on upgrade, not stay pinned"
        );

        // (b) Control: an explicit NON-legacy persisted value (2 GiB, above the
        // auto ceiling) is a real operator choice and MUST survive the upgrade.
        // This proves the migration is value-specific (targets only the old
        // default sentinel), not a blanket re-derivation — meaningful on any RAM.
        let explicit_dir = tempfile::tempdir().unwrap();
        clap_bare_args(explicit_dir.path()).build().await.unwrap();
        let cfg_path = explicit_dir.path().join("config.toml");
        let existing = std::fs::read_to_string(&cfg_path).unwrap();
        std::fs::write(
            &cfg_path,
            format!("max-hosting-storage = 2147483648\n{existing}"),
        )
        .unwrap();
        let upgraded = clap_bare_args(explicit_dir.path()).build().await.unwrap();
        assert_eq!(
            upgraded.max_hosting_storage,
            2 * 1024 * 1024 * 1024_u64,
            "an explicit non-legacy persisted value must survive upgrade"
        );
    }

    #[tokio::test]
    async fn module_cache_budget_defaults_to_ram_scaled_clamped() {
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        // The default is RAM-scaled and clamped to [MIN, MAX]. It must resolve to
        // the wasm_runtime single-source-of-truth default and land within the
        // documented clamp range on any host. Reference the constants rather than
        // hardcoded byte values so this test never drifts from the clamp.
        let min = crate::wasm_runtime::MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES;
        let max = crate::wasm_runtime::MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES;
        assert_eq!(
            cfg.module_cache_budget_bytes,
            crate::wasm_runtime::default_module_cache_budget_bytes(),
            "default module cache budget should resolve to the wasm_runtime \
             single-source-of-truth default"
        );
        assert!(
            (min..=max).contains(&cfg.module_cache_budget_bytes),
            "default budget {} must be within the [{min}, {max}] clamp",
            cfg.module_cache_budget_bytes
        );
    }

    #[tokio::test]
    async fn module_cache_budget_explicit_value_round_trips() {
        let temp_dir = tempfile::tempdir().unwrap();
        let custom = 768 * 1024 * 1024_usize;
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            module_cache_budget_bytes: Some(custom),
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        assert_eq!(cfg.module_cache_budget_bytes, custom);

        // Round-trips through TOML serialization.
        let serialized = toml::to_string(&cfg).unwrap();
        assert!(serialized.contains("module-cache-budget-bytes"));
        let deserialized: Config = toml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.module_cache_budget_bytes, custom);
    }

    /// Build a minimal local-mode ConfigArgs with the given CIDR list and
    /// return the result of `build().await`. The allowed_source_cidrs path
    /// is the only interesting variation; everything else is defaulted.
    async fn build_with_cidrs(cidrs: Option<Vec<String>>) -> anyhow::Result<Config> {
        let temp_dir = tempfile::tempdir().unwrap();
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ws_api: WebsocketApiArgs {
                allowed_source_cidrs: cidrs,
                ..Default::default()
            },
            ..Default::default()
        };
        args.build().await
    }

    #[tokio::test]
    async fn allowed_source_cidrs_round_trip_through_build() {
        let cfg = build_with_cidrs(Some(vec![
            "100.64.0.0/10".to_string(),
            "fd7a:115c:a1e0::/48".to_string(),
        ]))
        .await
        .unwrap();
        assert_eq!(cfg.ws_api.allowed_source_cidrs.len(), 2);
        assert_eq!(
            cfg.ws_api.allowed_source_cidrs[0],
            "100.64.0.0/10".parse::<ipnet::IpNet>().unwrap()
        );
        assert_eq!(
            cfg.ws_api.allowed_source_cidrs[1],
            "fd7a:115c:a1e0::/48".parse::<ipnet::IpNet>().unwrap()
        );
    }

    #[tokio::test]
    async fn allowed_source_cidrs_default_is_empty() {
        // Regression guard: if the user configures nothing, the built
        // config must carry an empty vec so the server-side filter falls
        // back to private-only behavior.
        let cfg = build_with_cidrs(None).await.unwrap();
        assert!(cfg.ws_api.allowed_source_cidrs.is_empty());
    }

    #[tokio::test]
    async fn allowed_source_cidrs_rejects_malformed() {
        let err = build_with_cidrs(Some(vec!["not-a-cidr".to_string()]))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("allowed-source-cidrs") && msg.contains("not-a-cidr"),
            "error should name the field and the offending value: {msg}"
        );
    }

    #[tokio::test]
    async fn allowed_source_cidrs_rejects_whole_internet_catchall() {
        // 0.0.0.0/0 parses fine as IpNet but the validator must reject
        // it — this is the footgun the middleware can't defend against
        // once the vec is populated.
        let err = build_with_cidrs(Some(vec!["0.0.0.0/0".to_string()]))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("0.0.0.0/0") && msg.contains("/8"),
            "error should explain why and name the minimum: {msg}"
        );
    }

    #[tokio::test]
    async fn allowed_source_cidrs_rejects_ipv6_catchall() {
        let err = build_with_cidrs(Some(vec!["::/0".to_string()]))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("::/0") && msg.contains("/16"));
    }

    /// Write a config.toml to `dir` by serializing a default local-mode
    /// Config and patching ws-api fields into it.
    async fn write_config_toml_with_ws_api(dir: &Path, ws_api_patch: &WebsocketApiConfig) {
        // Build a valid base config we can serialize
        let base_args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(dir.to_path_buf()),
                data_dir: Some(dir.to_path_buf()),
                log_dir: Some(dir.to_path_buf()),
            },
            ..Default::default()
        };
        let mut base_cfg = base_args.build().await.unwrap();
        base_cfg.ws_api = ws_api_patch.clone();
        let toml_str = toml::to_string(&base_cfg).unwrap();
        std::fs::write(dir.join("config.toml"), toml_str).unwrap();
    }

    #[tokio::test]
    async fn file_config_cidrs_merged_into_build() {
        // Regression test: allowed-source-cidrs and allowed-host set in
        // config.toml were silently dropped because the merge block in
        // build() didn't copy them from the file config into ConfigArgs.
        let temp_dir = tempfile::tempdir().unwrap();
        write_config_toml_with_ws_api(
            temp_dir.path(),
            &WebsocketApiConfig {
                allowed_source_cidrs: vec![
                    "100.64.0.0/10".parse().unwrap(),
                    "fd7a:115c:a1e0::/48".parse().unwrap(),
                ],
                allowed_hosts: vec!["my-tailscale-host".to_string()],
                ..Default::default()
            },
        )
        .await;

        // Build again from the config file (no CLI overrides for these fields)
        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();

        assert_eq!(
            cfg.ws_api.allowed_source_cidrs.len(),
            2,
            "CIDRs from config.toml must be present in built config"
        );
        assert_eq!(
            cfg.ws_api.allowed_source_cidrs[0],
            "100.64.0.0/10".parse::<ipnet::IpNet>().unwrap()
        );
        assert_eq!(
            cfg.ws_api.allowed_source_cidrs[1],
            "fd7a:115c:a1e0::/48".parse::<ipnet::IpNet>().unwrap()
        );
        assert_eq!(
            cfg.ws_api.allowed_hosts,
            vec!["my-tailscale-host".to_string()],
            "allowed-host from config.toml must be present in built config"
        );
    }

    /// A local-mode `ConfigArgs` pointing every path at `dir`. Used to seed a
    /// `config.toml` (first `build()` persists it) and to read it back on a
    /// later bare build — the real persistence round-trip.
    fn local_args(dir: &Path) -> ConfigArgs {
        ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(dir.to_path_buf()),
                data_dir: Some(dir.to_path_buf()),
                log_dir: Some(dir.to_path_buf()),
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn gateway_role_and_total_bandwidth_round_trip_through_build() {
        // Regression for #4275: is_gateway/location/total_bandwidth_limit are
        // written to config.toml but build()'s merge never read them back, so a
        // bare `freenet network` demoted the gateway and dropped its bandwidth
        // cap. Exercises the real round-trip: first build persists, bare build
        // reads back.
        let temp_dir = tempfile::tempdir().unwrap();

        let mut first = local_args(temp_dir.path());
        first.network_api.is_gateway = true;
        first.network_api.public_address = Some("1.2.3.4".parse().unwrap());
        first.network_api.public_port = Some(31337);
        first.network_api.location = Some(0.5);
        first.network_api.total_bandwidth_limit = Some(100_000_000);
        first.network_api.max_connections = Some(2000);
        first.build().await.unwrap();
        assert!(
            temp_dir.path().join("config.toml").exists(),
            "first build with flags must persist config.toml"
        );

        let cfg = local_args(temp_dir.path()).build().await.unwrap();

        assert!(
            cfg.is_gateway,
            "is_gateway from config.toml must survive a bare build (node must stay a gateway)"
        );
        assert_eq!(
            cfg.location,
            Some(0.5),
            "location from config.toml must survive a bare build"
        );
        assert_eq!(
            cfg.network_api.total_bandwidth_limit,
            Some(100_000_000),
            "total_bandwidth_limit from config.toml must survive a bare build"
        );
        assert_eq!(
            cfg.network_api.max_connections, 2000,
            "max_connections from config.toml must survive a bare build"
        );
        assert!(
            cfg.peer_id.is_some(),
            "peer_id must be reconstructed from the restored public address/port"
        );
    }

    #[tokio::test]
    async fn gateway_in_config_without_public_address_fails_validation() {
        // A config.toml claiming is_gateway=true with no public address must be
        // rejected, not silently armed — which only holds if validate() runs
        // after the merge. The normal flow can't produce such a file (validate
        // rejects it up front), so hand-craft it: seed a valid non-gateway
        // config.toml, then flip is_gateway on with no public address.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut seeded = local_args(temp_dir.path()).build().await.unwrap();
        seeded.is_gateway = true;
        seeded.network_api.public_address = None;
        seeded.network_api.public_port = None;
        std::fs::write(
            temp_dir.path().join("config.toml"),
            toml::to_string(&seeded).unwrap(),
        )
        .unwrap();

        let err = local_args(temp_dir.path()).build().await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("public network address"),
            "an is_gateway=true config without a public address must fail validation: {msg}"
        );
    }

    #[tokio::test]
    async fn cli_total_bandwidth_limit_overrides_file_config() {
        // CLI args still take precedence over the file value: the new merge
        // must use get_or_insert (fill-if-empty), not a blind overwrite.
        let temp_dir = tempfile::tempdir().unwrap();

        let mut first = local_args(temp_dir.path());
        first.network_api.total_bandwidth_limit = Some(100_000_000);
        first.build().await.unwrap();

        let mut second = local_args(temp_dir.path());
        second.network_api.total_bandwidth_limit = Some(50_000_000);
        let cfg = second.build().await.unwrap();

        assert_eq!(
            cfg.network_api.total_bandwidth_limit,
            Some(50_000_000),
            "CLI --total-bandwidth-limit must override the config.toml value"
        );
    }

    #[tokio::test]
    async fn cli_flag_set_on_a_later_run_is_persisted_to_config() {
        // #4275 (B2): config.toml was only written on the first run, so a value
        // passed on the CLI when the file already existed applied for that run
        // but was lost on the next bare restart. It must be persisted instead.
        let temp_dir = tempfile::tempdir().unwrap();

        // First run creates config.toml (no total_bandwidth_limit).
        clap_bare_args(temp_dir.path()).build().await.unwrap();

        // Later run passes a new value on the CLI.
        let mut args = clap_bare_args(temp_dir.path());
        args.network_api.total_bandwidth_limit = Some(50_000_000);
        args.build().await.unwrap();

        // A subsequent bare run must see the persisted value.
        let cfg = clap_bare_args(temp_dir.path()).build().await.unwrap();
        assert_eq!(
            cfg.network_api.total_bandwidth_limit,
            Some(50_000_000),
            "a CLI flag set on a later run must be written back to config.toml"
        );
    }

    #[tokio::test]
    async fn bare_restart_does_not_rewrite_unchanged_config() {
        // The re-persist must only fire when something changed: a no-op restart
        // must leave config.toml byte-identical, so operator hand-edits survive.
        // Uses clap_bare_args (all-None) so the rebuild reads every value back
        // from the file; local_args would re-pick a random network port each
        // build (ConfigArgs::default) and look like a spurious change.
        let temp_dir = tempfile::tempdir().unwrap();
        clap_bare_args(temp_dir.path()).build().await.unwrap();

        let path = temp_dir.path().join("config.toml");
        let before = std::fs::read_to_string(&path).unwrap();
        clap_bare_args(temp_dir.path()).build().await.unwrap();
        let after = std::fs::read_to_string(&path).unwrap();

        assert_eq!(
            before, after,
            "a no-op restart must not rewrite config.toml"
        );
    }

    #[tokio::test]
    async fn persisting_config_never_truncates_existing_file_on_temp_write_failure() {
        // The config.toml write path must go through a temp file + rename
        // rather than truncating config.toml in place, so a reader (e.g. a
        // second `freenet` process racing this one — see #5132) can never
        // observe an empty or partially-written file. Force a failure in the
        // temp-file write step (by pre-occupying its path with a directory)
        // and verify the destination file survives byte-for-byte untouched.
        let temp_dir = tempfile::tempdir().unwrap();

        // First run creates a real config.toml.
        clap_bare_args(temp_dir.path()).build().await.unwrap();
        let config_path = temp_dir.path().join("config.toml");
        let original = std::fs::read_to_string(&config_path).unwrap();
        assert!(!original.is_empty());

        // Occupy this process's temp path with a directory so the next
        // persist's `File::create` on it fails instead of succeeding. The
        // temp filename embeds the current PID (see the persist code), so
        // it's reproduced here rather than hardcoded.
        std::fs::create_dir(config_path.with_extension(format!("{}.toml.tmp", std::process::id())))
            .unwrap();

        // Change a value so the persist path actually attempts a write.
        let mut args = clap_bare_args(temp_dir.path());
        args.network_api.total_bandwidth_limit = Some(123_456_789);
        let result = args.build().await;

        assert!(
            result.is_err(),
            "build() must surface the forced temp-file write failure, not silently succeed"
        );

        let after = std::fs::read_to_string(&config_path).unwrap();
        assert_eq!(
            original, after,
            "a failed persist must leave the existing config.toml completely untouched, \
             never truncated or partially overwritten"
        );
    }

    #[test]
    fn warns_only_about_cached_gateways_absent_from_remote_index() {
        // #4275 (A2): the remote-index replacement must surface — but only —
        // the locally-cached gateways that the index no longer lists, so a
        // manually pinned peer is not dropped silently.
        fn gw(host: &str) -> GatewayConfig {
            GatewayConfig {
                address: Address::Host {
                    host: host.to_string(),
                    port: 31337,
                },
                public_key_path: PathBuf::from("/dev/null"),
                location: None,
            }
        }

        let local = vec![gw("a"), gw("b"), gw("c")];
        let remote = vec![gw("b"), gw("c"), gw("d")];

        // Only "a" is in the local cache but missing from the remote index.
        assert_eq!(
            gateways_dropped_by_remote_replace(&local, &remote),
            vec![Address::Host {
                host: "a".to_string(),
                port: 31337
            }],
        );

        // Remote is a superset / identical → nothing is dropped → no warning.
        assert!(gateways_dropped_by_remote_replace(&local, &local).is_empty());
        assert!(gateways_dropped_by_remote_replace(&[], &remote).is_empty());
    }

    /// A `ConfigArgs` mirroring a real bare `freenet network` parse: every
    /// optional field unset (None), pointed at `dir` in Local mode. Avoids
    /// `ConfigArgs::default()`, which pre-fills some fields with `Some(..)` that
    /// would MASK the file value on merge and give the guard below a false pass.
    fn clap_bare_args(dir: &Path) -> ConfigArgs {
        ConfigArgs {
            mode: Some(OperationMode::Local),
            network_api: NetworkArgs::default(),
            ws_api: WebsocketApiArgs::default(),
            secrets: Default::default(),
            log_level: None,
            config_paths: ConfigPathsArgs {
                config_dir: Some(dir.to_path_buf()),
                data_dir: Some(dir.to_path_buf()),
                log_dir: Some(dir.to_path_buf()),
            },
            id: None,
            version: false,
            max_blocking_threads: None,
            max_hosting_storage: None,
            hosting_disk_pct: None,
            max_hosting_disk: None,
            hosting_mem_share: None,
            per_user_secret_quota_bytes: None,
            per_user_inactive_ttl_secs: None,
            inactive_user_sweep_interval_secs: None,
            module_cache_budget_bytes: None,
            enable_event_log: None,
            shutdown_drain_secs: None,
            disable_auto_update: false,
            telemetry: Default::default(),
            otel: Default::default(),
        }
    }

    #[tokio::test]
    async fn disable_auto_update_flag_wires_through_build() {
        // The default (no flag) MUST leave auto-update ENABLED (disable=false).
        // This is the load-bearing default (#4690): a release node must keep
        // updating automatically. Paired with the freenet.rs bin test on the
        // gate helper, this covers the clap-arg -> Config plumbing end.
        let temp = tempfile::tempdir().unwrap();
        let default_cfg = clap_bare_args(temp.path()).build().await.unwrap();
        assert!(
            !default_cfg.disable_auto_update,
            "default must keep auto-update ON"
        );

        // With the flag set on ConfigArgs, it reaches the built Config.
        let temp2 = tempfile::tempdir().unwrap();
        let mut args = clap_bare_args(temp2.path());
        args.disable_auto_update = true;
        let cfg = args.build().await.unwrap();
        assert!(
            cfg.disable_auto_update,
            "--disable-auto-update must reach Config"
        );
    }

    #[test]
    fn disable_auto_update_flag_parses_from_cli() {
        use clap::Parser;
        // Absent → false (auto-update ON — the load-bearing default, #4690).
        let none = ConfigArgs::try_parse_from(["freenet"]).expect("bare parse");
        assert!(!none.disable_auto_update, "no flag → auto-update ON");
        // Present → true (bespoke from-source deployment opt-out).
        let set =
            ConfigArgs::try_parse_from(["freenet", "--disable-auto-update"]).expect("flag parse");
        assert!(set.disable_auto_update, "--disable-auto-update → OFF");
    }

    /// A [`Config`] with EVERY persisted field seeded to a non-default value,
    /// on top of a real `build()` result for the fields that must be genuine
    /// (`secrets`, `config_paths`).
    ///
    /// This is a struct literal with NO `..`, so a new field on `Config` or on
    /// any struct flattened into it fails to COMPILE here until the author
    /// gives it a value. That compile-time gate is what makes all three callers
    /// exhaustive rather than dependent on someone remembering to extend a
    /// hand-written fixture:
    ///
    ///  - [`all_persisted_config_fields_round_trip_through_build`] — does the
    ///    value survive the `config.toml` merge? (#4275)
    ///  - `every_emitted_config_key_is_also_accepted_in_kebab_case` — is the
    ///    key it is written under also accepted hyphenated? (#5124)
    ///  - `emitted_config_toml_keys_keep_their_released_spelling` — is the key
    ///    it is WRITTEN under still the one older releases can read? (#5124)
    ///
    /// # Seed every field to a NON-DEFAULT value, and every `Option` to `Some`
    ///
    /// The compile gate forces you to write *a* value; it cannot force a
    /// *distinct* one, and that is the difference between the guards working
    /// and passing vacuously:
    ///
    ///  - An `Option` carrying `skip_serializing_if` that is seeded `None`
    ///    emits no key at all, so no guard can inspect it. This is the one
    ///    residual hole — nothing but this paragraph stops it.
    ///  - A field seeded to the value it would fall back to anyway round-trips
    ///    byte-identically, so the round-trip guard cannot see a lost binding.
    ///    (The set-equality check in
    ///    `emitted_config_toml_keys_keep_their_released_spelling` does catch
    ///    this for any field that is always emitted, whatever it was seeded
    ///    with.)
    ///
    /// `secrets` is the known exception: it is taken from the real build rather
    /// than seeded, so `nonce` is absent whenever the build left it unset.
    fn config_with_every_field_seeded(base: &Config) -> Config {
        Config {
            mode: OperationMode::Local,
            network_api: NetworkApiConfig {
                address: "10.1.2.3".parse().unwrap(),
                port: 40001,
                public_address: Some("1.2.3.4".parse().unwrap()),
                public_port: Some(40002),
                ignore_protocol_version: false, // #[serde(skip)] — not persisted
                bandwidth_limit: Some(7_000_000),
                total_bandwidth_limit: Some(123_000_000),
                min_bandwidth_per_connection: Some(2_000_000),
                blocked_addresses: Some(
                    std::iter::once("9.9.9.9:1234".parse::<SocketAddr>().unwrap()).collect(),
                ),
                event_loop_channel_capacity: 4096,
                transient_budget: 4097,
                transient_ttl_secs: 61,
                min_connections: 11,
                max_connections: 222,
                streaming_threshold: 131_072,
                ledbat_min_ssthresh: Some(200_000),
                congestion_control: "bbr".to_string(),
                bbr_startup_rate: Some(5_000),
                skip_load_from_network: true,
            },
            ws_api: WebsocketApiConfig {
                // Deliberately NOT an auto-derivable address: this guard is
                // about persisted-field round-tripping, so it must not also
                // exercise the migration. Its green therefore says nothing
                // about the sentinel — that is
                // `auto_derivable_addresses_are_exactly_the_ones_this_code_writes`
                // and `re_derivation_preserves_the_address_family`.
                address: "10.1.2.4".parse().unwrap(),
                port: 8123,
                token_ttl_seconds: 4321,
                token_cleanup_interval_seconds: 321,
                allowed_hosts: vec!["my-host".to_string()],
                allowed_source_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
                hosted_mode: true,
                per_user_op_rate_limit: 33,
                per_user_op_burst: 77,
                per_user_export_min_interval_secs: 17,
                // serde-skip runtime field; repopulated by build() and not
                // asserted in the round-trip (bound to `_` in the destructure).
                secrets_dir: std::path::PathBuf::new(),
                // Runtime-only (`#[serde(skip)]`): describes THIS boot's
                // resolution, so it is deliberately not round-tripped. Seeded
                // non-default anyway so the guard below is not comparing two
                // defaults.
                exposure: WsApiExposure {
                    source: WsApiAddressSource::AutoWidened,
                    dropped_persisted_address: Some(default_listening_address()),
                },
                webapp_cache_dir: default_webapp_cache_dir(),
            },
            secrets: base.secrets.clone(),
            log_level: tracing::log::LevelFilter::Debug,
            config_paths: base.config_paths.clone(),
            peer_id: None,
            gateways: vec![],
            is_gateway: true,
            location: Some(0.5),
            max_blocking_threads: 7,
            max_hosting_storage: 123_456_789,
            hosting_disk_pct: 0.37,
            max_hosting_disk: 9_876_543_210,
            hosting_mem_share: 0.21,
            per_user_secret_quota_bytes: 7_654_321,
            per_user_inactive_ttl_secs: 1_234_567,
            inactive_user_sweep_interval_secs: 7_200,
            module_cache_budget_bytes: 987_654_321,
            // Non-default on purpose: the seed is Local mode, where the #4968
            // default is ON, so `Some(false)` fails this test if the merge
            // drops the field (it would come back as `None`).
            enable_event_log: Some(false),
            telemetry: TelemetryConfig {
                enabled: false,
                endpoint: "http://example.invalid:4318".to_string(),
                transport_snapshot_interval_secs: 45,
                is_test_environment: false, // #[serde(skip)] — derived from --id
                reference_ping_enabled: true,
                iface_tx_enabled: true,
            },
            otel: OtelConfig {
                enabled: true,
                endpoint: Some("http://example.invalid:4319".to_string()),
                is_test_environment: false, // #[serde(skip)] — derived from --id
            },
            shutdown_drain_secs: 77,
            disable_auto_update: true, // #[serde(skip)] — see destructure below
        }
    }

    #[tokio::test]
    async fn all_persisted_config_fields_round_trip_through_build() {
        // #4275 guard against the recurring bug class (#3890, #4275): build()'s
        // field-by-field merge silently drops any persisted field it doesn't
        // list. Seeds a non-default value for EVERY persisted field, writes it,
        // rebuilds from a clap-bare ConfigArgs, and asserts each one survives.
        //
        // The destructuring below has NO `..`: adding a field to any of these
        // structs fails to COMPILE until the author classifies it (round-trips
        // -> merge + assert; skip-by-design -> bind to `_`). Keeps it honest.
        let temp_dir = tempfile::tempdir().unwrap();

        // Valid base build: creates the on-disk secret files (and gives us real
        // secrets + resolved paths) that the rebuild will read back.
        let base = clap_bare_args(temp_dir.path()).build().await.unwrap();

        let seed = config_with_every_field_seeded(&base);

        std::fs::write(
            temp_dir.path().join("config.toml"),
            toml::to_string(&seed).unwrap(),
        )
        .unwrap();

        let rebuilt = clap_bare_args(temp_dir.path()).build().await.unwrap();

        // Exhaustive destructure — NO `..`. A new Config field must be handled here.
        let Config {
            mode,
            network_api,
            ws_api,
            secrets: _, // key material, not config
            log_level,
            config_paths: _, // re-resolved per process (temp dir)
            peer_id: _,      // derived from public addr/port
            gateways: _,     // lives in gateways.toml
            is_gateway,
            location,
            max_blocking_threads,
            max_hosting_storage,
            hosting_disk_pct,
            max_hosting_disk,
            hosting_mem_share,
            per_user_secret_quota_bytes,
            per_user_inactive_ttl_secs,
            inactive_user_sweep_interval_secs,
            module_cache_budget_bytes,
            enable_event_log,
            telemetry,
            otel,
            shutdown_drain_secs,
            // #[serde(skip)] runtime CLI/env flag — set from --disable-auto-update
            // at build() time, intentionally not persisted, so it does not
            // round-trip through config.toml (#4690).
            disable_auto_update: _,
        } = rebuilt;

        assert_eq!(mode, seed.mode, "mode");
        assert_eq!(log_level, seed.log_level, "log_level");
        assert_eq!(is_gateway, seed.is_gateway, "is_gateway");
        assert_eq!(location, seed.location, "location");
        assert_eq!(
            max_blocking_threads, seed.max_blocking_threads,
            "max_blocking_threads"
        );
        assert_eq!(
            max_hosting_storage, seed.max_hosting_storage,
            "max_hosting_storage"
        );
        assert_eq!(hosting_disk_pct, seed.hosting_disk_pct, "hosting_disk_pct");
        assert_eq!(max_hosting_disk, seed.max_hosting_disk, "max_hosting_disk");
        assert_eq!(
            hosting_mem_share, seed.hosting_mem_share,
            "hosting_mem_share"
        );
        assert_eq!(
            per_user_secret_quota_bytes, seed.per_user_secret_quota_bytes,
            "per_user_secret_quota_bytes"
        );
        assert_eq!(
            per_user_inactive_ttl_secs, seed.per_user_inactive_ttl_secs,
            "per_user_inactive_ttl_secs"
        );
        assert_eq!(
            inactive_user_sweep_interval_secs, seed.inactive_user_sweep_interval_secs,
            "inactive_user_sweep_interval_secs"
        );
        assert_eq!(
            module_cache_budget_bytes, seed.module_cache_budget_bytes,
            "module_cache_budget_bytes"
        );
        assert_eq!(
            enable_event_log, seed.enable_event_log,
            "enable_event_log (#4968) — an explicit setting must survive the \
             config.toml merge, or an operator's opt-in is silently reverted"
        );
        assert_eq!(
            shutdown_drain_secs, seed.shutdown_drain_secs,
            "shutdown_drain_secs"
        );
        assert_eq!(otel.enabled, seed.otel.enabled, "otel.enabled");
        assert_eq!(
            otel.endpoint, seed.otel.endpoint,
            "otel.endpoint — an operator's collector URL must survive the \
             config.toml merge"
        );

        let NetworkApiConfig {
            address,
            port,
            public_address,
            public_port,
            ignore_protocol_version: _, // serde-skip
            bandwidth_limit,
            total_bandwidth_limit,
            min_bandwidth_per_connection,
            blocked_addresses,
            event_loop_channel_capacity,
            transient_budget,
            transient_ttl_secs,
            min_connections,
            max_connections,
            streaming_threshold,
            ledbat_min_ssthresh,
            congestion_control,
            bbr_startup_rate,
            skip_load_from_network,
        } = network_api;
        assert_eq!(address, seed.network_api.address, "network_api.address");
        assert_eq!(port, seed.network_api.port, "network_api.port");
        assert_eq!(
            public_address, seed.network_api.public_address,
            "public_address"
        );
        assert_eq!(public_port, seed.network_api.public_port, "public_port");
        assert_eq!(
            bandwidth_limit, seed.network_api.bandwidth_limit,
            "bandwidth_limit"
        );
        assert_eq!(
            total_bandwidth_limit, seed.network_api.total_bandwidth_limit,
            "total_bandwidth_limit"
        );
        assert_eq!(
            min_bandwidth_per_connection, seed.network_api.min_bandwidth_per_connection,
            "min_bandwidth_per_connection"
        );
        assert_eq!(
            blocked_addresses, seed.network_api.blocked_addresses,
            "blocked_addresses"
        );
        assert_eq!(
            event_loop_channel_capacity, seed.network_api.event_loop_channel_capacity,
            "event_loop_channel_capacity"
        );
        assert_eq!(
            transient_budget, seed.network_api.transient_budget,
            "transient_budget"
        );
        assert_eq!(
            transient_ttl_secs, seed.network_api.transient_ttl_secs,
            "transient_ttl_secs"
        );
        assert_eq!(
            min_connections, seed.network_api.min_connections,
            "min_connections"
        );
        assert_eq!(
            max_connections, seed.network_api.max_connections,
            "max_connections"
        );
        assert_eq!(
            streaming_threshold, seed.network_api.streaming_threshold,
            "streaming_threshold"
        );
        assert_eq!(
            ledbat_min_ssthresh, seed.network_api.ledbat_min_ssthresh,
            "ledbat_min_ssthresh"
        );
        assert_eq!(
            congestion_control, seed.network_api.congestion_control,
            "congestion_control"
        );
        assert_eq!(
            bbr_startup_rate, seed.network_api.bbr_startup_rate,
            "bbr_startup_rate"
        );
        assert_eq!(
            skip_load_from_network, seed.network_api.skip_load_from_network,
            "skip_load_from_network"
        );

        let WebsocketApiConfig {
            address: ws_address,
            port: ws_port,
            token_ttl_seconds,
            token_cleanup_interval_seconds,
            allowed_hosts,
            allowed_source_cidrs,
            hosted_mode,
            per_user_op_rate_limit,
            per_user_op_burst,
            per_user_export_min_interval_secs,
            secrets_dir: _, // serde-skip runtime field, repopulated by build()
            // serde-skip runtime field, repopulated by build() from
            // `default_webapp_cache_dir()` (env-overridable).
            webapp_cache_dir: _,
            // serde-skip runtime field: how THIS boot resolved the client-API
            // bind. Not config — it is an output of `build()`, re-derived every
            // boot — so it deliberately does not round-trip. Its own coverage
            // is `build_records_the_exposure_decision_for_later_reporting`.
            exposure: _,
        } = ws_api;
        assert_eq!(ws_address, seed.ws_api.address, "ws_api.address");
        assert_eq!(ws_port, seed.ws_api.port, "ws_api.port");
        assert_eq!(
            token_ttl_seconds, seed.ws_api.token_ttl_seconds,
            "token_ttl_seconds"
        );
        assert_eq!(
            token_cleanup_interval_seconds, seed.ws_api.token_cleanup_interval_seconds,
            "token_cleanup_interval_seconds"
        );
        assert_eq!(allowed_hosts, seed.ws_api.allowed_hosts, "allowed_hosts");
        assert_eq!(
            allowed_source_cidrs, seed.ws_api.allowed_source_cidrs,
            "allowed_source_cidrs"
        );
        assert_eq!(hosted_mode, seed.ws_api.hosted_mode, "ws_api.hosted_mode");
        assert_eq!(
            per_user_op_rate_limit, seed.ws_api.per_user_op_rate_limit,
            "ws_api.per_user_op_rate_limit"
        );
        assert_eq!(
            per_user_op_burst, seed.ws_api.per_user_op_burst,
            "ws_api.per_user_op_burst"
        );
        assert_eq!(
            per_user_export_min_interval_secs, seed.ws_api.per_user_export_min_interval_secs,
            "ws_api.per_user_export_min_interval_secs"
        );

        let TelemetryConfig {
            enabled,
            endpoint,
            transport_snapshot_interval_secs,
            is_test_environment: _, // serde-skip, derived from --id
            reference_ping_enabled,
            iface_tx_enabled,
        } = telemetry;
        assert_eq!(enabled, seed.telemetry.enabled, "telemetry.enabled");
        assert_eq!(endpoint, seed.telemetry.endpoint, "telemetry.endpoint");
        assert_eq!(
            transport_snapshot_interval_secs, seed.telemetry.transport_snapshot_interval_secs,
            "transport_snapshot_interval_secs"
        );
        assert_eq!(
            reference_ping_enabled, seed.telemetry.reference_ping_enabled,
            "reference_ping_enabled"
        );
        assert_eq!(
            iface_tx_enabled, seed.telemetry.iface_tx_enabled,
            "iface_tx_enabled"
        );
    }

    #[tokio::test]
    async fn module_cache_budget_old_auto_sentinel_re_derives_but_explicit_values_persist() {
        // #4864: an existing node whose config.toml was auto-written on a >12 GiB
        // box carries the OLD auto-derived 1.5 GiB clamp
        // (module-cache-budget-bytes = 1_610_612_736, the previous
        // MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES). build()'s merge must treat that
        // exact sentinel as "auto" and RE-DERIVE the current default (so large
        // gateways pick up the new 4 GiB clamp instead of staying pinned to
        // 1.5 GiB forever), while PRESERVING any other persisted value (a genuine
        // operator choice) and DERIVING when the field is absent.

        // Resolve the freshly-derived default the SAME way the production default
        // fn does, so the assertions are host-independent (no hard-coded number).
        // On a box where the derived default happens to equal the sentinel (exactly
        // 12 GiB RAM) case 1 still holds: re-derivation is a no-op there.
        let derived_default = crate::wasm_runtime::default_module_cache_budget_bytes();

        // Seed config.toml with `module-cache-budget-bytes = seed` (or omit the key
        // when `seed` is None), rebuild from clap-bare args, and return the resolved
        // value. Mirrors all_persisted_config_fields_round_trip_through_build: a base
        // build creates the on-disk secret files + a valid full Config to serialize.
        async fn rebuilt_budget(seed: Option<usize>) -> usize {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut base = clap_bare_args(temp_dir.path()).build().await.unwrap();
            let toml_str = match seed {
                Some(v) => {
                    base.module_cache_budget_bytes = v;
                    toml::to_string(&base).unwrap()
                }
                None => {
                    // Drop the field so build() sees it as absent and the serde
                    // default fills it — exercises the "absent -> derive" path.
                    // The key is a top-level scalar, so removing its single line
                    // keeps the TOML valid.
                    toml::to_string(&base)
                        .unwrap()
                        .lines()
                        .filter(|l| !l.trim_start().starts_with("module-cache-budget-bytes"))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            };
            std::fs::write(temp_dir.path().join("config.toml"), toml_str).unwrap();
            clap_bare_args(temp_dir.path())
                .build()
                .await
                .unwrap()
                .module_cache_budget_bytes
        }

        // 1. Exact old-auto sentinel -> RE-DERIVED to the fresh default (NOT the
        //    stale 1.5 GiB value).
        let from_sentinel = rebuilt_budget(Some(1_610_612_736)).await;
        assert_eq!(
            from_sentinel, derived_default,
            "the old auto-derived 1.5 GiB sentinel must re-derive to the fresh \
             default, not stay pinned at 1_610_612_736"
        );

        // 2. Some other explicit persisted value -> preserved exactly.
        let explicit = 777_000_000usize;
        let from_explicit = rebuilt_budget(Some(explicit)).await;
        assert_eq!(
            from_explicit, explicit,
            "an explicit non-sentinel operator value must round-trip unchanged"
        );

        // 3. Field absent from config.toml -> resolves to the derived default.
        let from_absent = rebuilt_budget(None).await;
        assert_eq!(
            from_absent, derived_default,
            "an absent field must resolve to the derived default"
        );
    }

    #[tokio::test]
    async fn cli_cidrs_override_file_config() {
        // CLI args take precedence over config file values.
        let temp_dir = tempfile::tempdir().unwrap();
        write_config_toml_with_ws_api(
            temp_dir.path(),
            &WebsocketApiConfig {
                allowed_source_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
                allowed_hosts: vec!["file-host".to_string()],
                ..Default::default()
            },
        )
        .await;

        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            ws_api: WebsocketApiArgs {
                allowed_source_cidrs: Some(vec!["172.16.0.0/12".to_string()]),
                allowed_host: Some(vec!["cli-host".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();

        assert_eq!(cfg.ws_api.allowed_source_cidrs.len(), 1);
        assert_eq!(
            cfg.ws_api.allowed_source_cidrs[0],
            "172.16.0.0/12".parse::<ipnet::IpNet>().unwrap(),
            "CLI value must win over file config"
        );
        assert_eq!(
            cfg.ws_api.allowed_hosts,
            vec!["cli-host".to_string()],
            "CLI value must win over file config"
        );
    }

    #[tokio::test]
    async fn test_load_gateways_from_index() {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(request::method("GET"), request::path("/gateways")))
                .respond_with(status_code(200).body(
                    r#"
                    [[gateways]]
                    address = { hostname = "example.com" }
                    public_key = "/path/to/public_key.pem"
                    "#,
                )),
        );

        let url = server.url_str("/gateways");

        // Generate a valid X25519 public key in hex format
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        server.expect(
            Expectation::matching(request::path("/path/to/public_key.pem"))
                .respond_with(status_code(200).body(key_hex)),
        );

        let pub_keys_dir = tempfile::tempdir().unwrap();
        let gateways = load_gateways_from_index(&url, pub_keys_dir.path())
            .await
            .unwrap();

        assert_eq!(gateways.gateways.len(), 1);
        assert_eq!(
            gateways.gateways[0].address,
            Address::Hostname("example.com".to_string())
        );
        assert_eq!(
            gateways.gateways[0].public_key_path,
            pub_keys_dir.path().join("public_key.pem")
        );
        assert!(pub_keys_dir.path().join("public_key.pem").exists());
    }

    #[test]
    fn test_gateways() {
        let gateways = Gateways {
            gateways: vec![
                GatewayConfig {
                    address: Address::HostAddress(
                        ([127, 0, 0, 1], default_network_api_port()).into(),
                    ),
                    public_key_path: PathBuf::from("path/to/key"),
                    location: None,
                },
                GatewayConfig {
                    address: Address::Hostname("technic.locut.us".to_string()),
                    public_key_path: PathBuf::from("path/to/key"),
                    location: None,
                },
            ],
        };

        let serialized = toml::to_string(&gateways).unwrap();
        let _: Gateways = toml::from_str(&serialized).unwrap();
    }

    // ---- Address deserialization: backward compat + new host/port form (#1388) ----

    /// Legacy single-string form, exactly as it appears in the deployed
    /// `https://freenet.org/keys/gateways.toml` today. MUST keep parsing.
    #[test]
    fn test_address_deser_legacy_hostname_string() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/public.vega.gw.pem"
            [gateways.address]
            hostname = "vega.locut.us:31337"
        "#;
        let gateways: Gateways = toml::from_str(toml_str).unwrap();
        assert_eq!(gateways.gateways.len(), 1);
        assert_eq!(
            gateways.gateways[0].address,
            Address::Hostname("vega.locut.us:31337".to_string())
        );
    }

    /// Legacy single-string form without a port still parses (port is resolved
    /// later by `parse_socket_addr`, which now defaults to 31337).
    #[test]
    fn test_address_deser_legacy_hostname_string_no_port() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/public.vega.gw.pem"
            [gateways.address]
            hostname = "vega.locut.us"
        "#;
        let gateways: Gateways = toml::from_str(toml_str).unwrap();
        assert_eq!(
            gateways.gateways[0].address,
            Address::Hostname("vega.locut.us".to_string())
        );
    }

    /// Legacy fully-resolved socket-address form. MUST keep parsing.
    #[test]
    fn test_address_deser_legacy_host_address() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/k.pem"
            [gateways.address]
            host_address = "203.0.113.1:31337"
        "#;
        let gateways: Gateways = toml::from_str(toml_str).unwrap();
        assert_eq!(
            gateways.gateways[0].address,
            Address::HostAddress("203.0.113.1:31337".parse().unwrap())
        );
    }

    /// New form with explicit host and port.
    #[test]
    fn test_address_deser_new_host_port() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/public.vega.gw.pem"
            [gateways.address]
            host = "vega.locut.us"
            port = 31337
        "#;
        let gateways: Gateways = toml::from_str(toml_str).unwrap();
        assert_eq!(
            gateways.gateways[0].address,
            Address::Host {
                host: "vega.locut.us".to_string(),
                port: 31337
            }
        );
    }

    /// New form with host and a non-default explicit port.
    #[test]
    fn test_address_deser_new_host_explicit_nondefault_port() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/k.pem"
            [gateways.address]
            host = "example.com"
            port = 12345
        "#;
        let gateways: Gateways = toml::from_str(toml_str).unwrap();
        assert_eq!(
            gateways.gateways[0].address,
            Address::Host {
                host: "example.com".to_string(),
                port: 12345
            }
        );
    }

    /// New form with host but NO port: must default to 31337, not a random port.
    #[test]
    fn test_address_deser_new_host_default_port() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/public.vega.gw.pem"
            [gateways.address]
            host = "vega.locut.us"
        "#;
        let gateways: Gateways = toml::from_str(toml_str).unwrap();
        assert_eq!(
            gateways.gateways[0].address,
            Address::Host {
                host: "vega.locut.us".to_string(),
                port: DEFAULT_GATEWAY_PORT
            }
        );
        assert_eq!(DEFAULT_GATEWAY_PORT, 31337);
    }

    /// `port` without `host` is rejected (it would silently be lost otherwise).
    #[test]
    fn test_address_deser_port_without_host_is_error() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/k.pem"
            [gateways.address]
            hostname = "example.com:80"
            port = 31337
        "#;
        assert!(toml::from_str::<Gateways>(toml_str).is_err());
    }

    /// An address table with none of the recognized keys is rejected.
    #[test]
    fn test_address_deser_empty_is_error() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/k.pem"
            [gateways.address]
        "#;
        assert!(toml::from_str::<Gateways>(toml_str).is_err());
    }

    /// Specifying more than one address form at once is rejected.
    #[test]
    fn test_address_deser_conflicting_forms_is_error() {
        let toml_str = r#"
            [[gateways]]
            public_key = "keys/k.pem"
            [gateways.address]
            host = "example.com"
            hostname = "example.com:31337"
        "#;
        assert!(toml::from_str::<Gateways>(toml_str).is_err());
    }

    /// The new `Host` variant round-trips through serialize -> deserialize.
    #[test]
    fn test_address_host_variant_roundtrip() {
        let gateways = Gateways {
            gateways: vec![GatewayConfig {
                address: Address::Host {
                    host: "vega.locut.us".to_string(),
                    port: 31337,
                },
                public_key_path: PathBuf::from("keys/k.pem"),
                location: None,
            }],
        };
        let serialized = toml::to_string(&gateways).unwrap();
        // The `Host` variant must serialize as a FLAT table (host/port as
        // sibling keys), matching the new wire form in the issue — not nested
        // under a `[gateways.address.host]` sub-table (the derived enum form).
        assert!(
            serialized.contains("host = \"vega.locut.us\"") && serialized.contains("port = 31337"),
            "unexpected serialized form:\n{serialized}"
        );
        assert!(
            !serialized.contains("[gateways.address.host]"),
            "Host variant must not nest under its own sub-table:\n{serialized}"
        );
        let deserialized: Gateways = toml::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized.gateways[0].address,
            gateways.gateways[0].address
        );
    }

    /// Pin the legacy serialized wire forms so a future refactor can't silently
    /// change what we write to `gateways.toml` (old binaries must keep reading
    /// files this build writes).
    #[test]
    fn test_address_legacy_variants_serialize_unchanged() {
        let hostname = Gateways {
            gateways: vec![GatewayConfig {
                address: Address::Hostname("vega.locut.us:31337".to_string()),
                public_key_path: PathBuf::from("keys/k.pem"),
                location: None,
            }],
        };
        let s = toml::to_string(&hostname).unwrap();
        assert!(
            s.contains("hostname = \"vega.locut.us:31337\""),
            "legacy hostname form changed:\n{s}"
        );

        let host_addr = Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress("203.0.113.1:31337".parse().unwrap()),
                public_key_path: PathBuf::from("keys/k.pem"),
                location: None,
            }],
        };
        let s = toml::to_string(&host_addr).unwrap();
        assert!(
            s.contains("host_address = \"203.0.113.1:31337\""),
            "legacy host_address form changed:\n{s}"
        );
    }

    #[tokio::test]
    #[ignore = "Requires gateway keys to be updated to X25519 format (issue #2531)"]
    async fn test_remote_freenet_gateways() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let gateways = load_gateways_from_index(FREENET_GATEWAYS_INDEX, tmp_dir.path())
            .await
            .unwrap();
        assert!(!gateways.gateways.is_empty());

        for gw in gateways.gateways {
            assert!(gw.public_key_path.exists());
            // Validate the public key is in hex format (32 bytes = 64 hex chars)
            let key_contents = std::fs::read_to_string(&gw.public_key_path).unwrap();
            let key_bytes =
                hex::decode(key_contents.trim()).expect("Gateway public key should be valid hex");
            assert_eq!(
                key_bytes.len(),
                32,
                "Gateway public key should be 32 bytes (X25519)"
            );
            let socket = NodeConfig::parse_socket_addr(&gw.address).await.unwrap();
            // Don't test for specific port since it's randomly assigned
            assert!(socket.port() > 1024); // Ensure we're using unprivileged ports
        }
    }

    #[test]
    fn test_streaming_config_defaults_via_serde() {
        let minimal_config = r#"
            network-address = "127.0.0.1"
            network-port = 8080
        "#;
        let network_api: NetworkApiConfig = toml::from_str(minimal_config).unwrap();
        assert_eq!(
            network_api.streaming_threshold,
            64 * 1024,
            "Default streaming threshold should be 64KB"
        );
    }

    #[test]
    fn test_streaming_config_serde() {
        let config_str = r#"
            network-address = "127.0.0.1"
            network-port = 8080
            streaming-threshold = 131072
        "#;

        let config: NetworkApiConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.streaming_threshold, 128 * 1024);

        let serialized = toml::to_string(&config).unwrap();
        assert!(serialized.contains("streaming-threshold = 131072"));
    }

    #[test]
    fn test_network_args_streaming_defaults() {
        let args = NetworkArgs::default();
        assert!(
            args.streaming_threshold.is_none(),
            "NetworkArgs.streaming_threshold should be None by default"
        );
    }

    #[test]
    fn test_congestion_control_config_defaults() {
        // Verify default congestion control is fixedrate
        let config_str = r#"
            network-address = "127.0.0.1"
            network-port = 8080
        "#;
        let network_api: NetworkApiConfig = toml::from_str(config_str).unwrap();
        assert_eq!(
            network_api.congestion_control, "fixedrate",
            "Default congestion control should be fixedrate"
        );
        assert!(
            network_api.bbr_startup_rate.is_none(),
            "Default BBR startup rate should be None"
        );

        // Build the congestion config and verify the algorithm
        let cc_config = network_api.build_congestion_config();
        assert_eq!(cc_config.algorithm, CongestionControlAlgorithm::FixedRate);
    }

    #[test]
    fn test_congestion_control_config_bbr() {
        // Test BBR configuration with custom startup rate
        let config_str = r#"
            network-address = "127.0.0.1"
            network-port = 8080
            congestion-control = "bbr"
            bbr-startup-rate = 10000000
        "#;

        let config: NetworkApiConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.congestion_control, "bbr");
        assert_eq!(config.bbr_startup_rate, Some(10_000_000));

        // Build the congestion config and verify BBR with custom startup rate
        let cc_config = config.build_congestion_config();
        assert_eq!(cc_config.algorithm, CongestionControlAlgorithm::Bbr);
    }

    #[test]
    fn test_congestion_control_config_ledbat() {
        // Test LEDBAT configuration
        let config_str = r#"
            network-address = "127.0.0.1"
            network-port = 8080
            congestion-control = "ledbat"
        "#;

        let config: NetworkApiConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.congestion_control, "ledbat");

        let cc_config = config.build_congestion_config();
        assert_eq!(cc_config.algorithm, CongestionControlAlgorithm::Ledbat);
    }

    #[test]
    fn test_congestion_control_config_serde_roundtrip() {
        // Test serialization/deserialization of congestion control config
        let config_str = r#"
            network-address = "127.0.0.1"
            network-port = 8080
            congestion-control = "bbr"
            bbr-startup-rate = 5000000
        "#;

        let config: NetworkApiConfig = toml::from_str(config_str).unwrap();

        // Round-trip test
        let serialized = toml::to_string(&config).unwrap();
        assert!(serialized.contains("congestion-control = \"bbr\""));
        assert!(serialized.contains("bbr-startup-rate = 5000000"));

        // Deserialize again and verify
        let config2: NetworkApiConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(config2.congestion_control, "bbr");
        assert_eq!(config2.bbr_startup_rate, Some(5_000_000));
    }

    #[test]
    fn test_set_seed_pins_thread_index_to_zero() {
        GlobalRng::clear_seed();

        GlobalRng::set_seed(0xDEAD_BEEF);
        assert_eq!(GlobalRng::thread_index(), 0);

        // Same seed produces same RNG output
        let val1 = GlobalRng::random_u64();
        GlobalRng::set_seed(0xDEAD_BEEF);
        let val2 = GlobalRng::random_u64();
        assert_eq!(val1, val2);

        GlobalRng::clear_seed();
    }

    #[tokio::test]
    async fn test_config_build_with_gateway_flag() {
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let temp_dir = tempfile::tempdir().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![format!("192.168.1.1:31337,{key_hex}")]),
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        // Local mode skips gateway loading, but --gateway should still be added
        assert_eq!(cfg.gateways.len(), 1);
        assert_eq!(
            cfg.gateways[0].address,
            Address::HostAddress("192.168.1.1:31337".parse().unwrap())
        );
    }

    #[test]
    fn test_parse_gateway_valid() {
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let input = format!("192.168.1.1:31337,{key_hex}");
        let tmp_dir = tempfile::tempdir().unwrap();

        let gw = parse_gateway(&input, tmp_dir.path()).unwrap();

        assert_eq!(
            gw.address,
            Address::HostAddress("192.168.1.1:31337".parse().unwrap())
        );
        assert!(gw.public_key_path.exists());
        let saved_key = std::fs::read_to_string(&gw.public_key_path).unwrap();
        assert_eq!(saved_key, key_hex);
        assert_eq!(gw.location, None);
    }

    #[test]
    fn test_parse_gateway_invalid_format() {
        let tmp_dir = tempfile::tempdir().unwrap();

        // Missing comma
        assert!(parse_gateway("192.168.1.1:31337", tmp_dir.path()).is_err());

        // Invalid hex
        assert!(parse_gateway("192.168.1.1:31337,not_hex_at_all!", tmp_dir.path()).is_err());

        // Wrong key length (16 bytes instead of 32)
        let short_hex = "ab".repeat(16);
        assert!(parse_gateway(&format!("192.168.1.1:31337,{short_hex}"), tmp_dir.path()).is_err());

        // Invalid socket addr
        let key_hex = "ab".repeat(32);
        assert!(parse_gateway(&format!("not_an_addr,{key_hex}"), tmp_dir.path()).is_err());
    }

    /// Tests `merge_and_deduplicate` using the production call order from `build()`:
    /// CLI gateways are `self`, file-loaded are `other`. On address collision, CLI wins.
    #[test]
    fn test_gateway_deduplication() {
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let tmp_dir = tempfile::tempdir().unwrap();

        let addr: SocketAddr = "10.0.0.1:31337".parse().unwrap();

        // File-loaded gateway with same address (stale key)
        let file_loaded = Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(addr),
                public_key_path: PathBuf::from("old/key/path"),
                location: None,
            }],
        };

        // CLI gateway with same address (fresh key)
        let gw = parse_gateway(&format!("{addr},{key_hex}"), tmp_dir.path()).unwrap();
        let cli_key_path = gw.public_key_path.clone();
        let mut cli = Gateways { gateways: vec![gw] };

        // Production order: cli_gateways.merge_and_deduplicate(file_loaded)
        cli.merge_and_deduplicate(file_loaded);
        // Should deduplicate by address — only one entry
        assert_eq!(cli.gateways.len(), 1);
        // CLI entry wins (self takes precedence)
        assert_eq!(cli.gateways[0].public_key_path, cli_key_path);
    }

    #[tokio::test]
    async fn test_config_build_network_mode_gateway_only() {
        // Simulates the censorship/CGNAT scenario: no gateways file, no remote index,
        // only --gateway. This must not fail with "Cannot initialize node
        // without gateways".
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let temp_dir = tempfile::tempdir().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![format!("203.0.113.1:31337,{key_hex}")]),
                skip_load_from_network: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        assert_eq!(cfg.gateways.len(), 1);
        assert_eq!(
            cfg.gateways[0].address,
            Address::HostAddress("203.0.113.1:31337".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn test_config_build_multiple_gateways() {
        let kp1 = TransportKeypair::new();
        let kp2 = TransportKeypair::new();
        let kp3 = TransportKeypair::new();
        let hex1 = hex::encode(kp1.public().as_bytes());
        let hex2 = hex::encode(kp2.public().as_bytes());
        let hex3 = hex::encode(kp3.public().as_bytes());
        let temp_dir = tempfile::tempdir().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![
                    format!("10.0.0.1:31337,{hex1}"),
                    format!("10.0.0.2:31337,{hex2}"),
                    format!("10.0.0.3:31337,{hex3}"),
                ]),
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        assert_eq!(cfg.gateways.len(), 3);

        let addrs: Vec<_> = cfg.gateways.iter().map(|g| g.address.clone()).collect();
        assert!(addrs.contains(&Address::HostAddress("10.0.0.1:31337".parse().unwrap())));
        assert!(addrs.contains(&Address::HostAddress("10.0.0.2:31337".parse().unwrap())));
        assert!(addrs.contains(&Address::HostAddress("10.0.0.3:31337".parse().unwrap())));
    }

    /// Mirrors the production call order in `build()`: CLI gateways are `self`, file-loaded
    /// gateways are `other`. This ensures CLI-provided keys win over stale file entries.
    #[tokio::test]
    async fn test_gateway_overrides_file_loaded() {
        // When a user explicitly provides --gateway for an address that
        // also exists in the file-loaded gateways, the CLI entry should win.
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let tmp_dir = tempfile::tempdir().unwrap();

        let addr: SocketAddr = "10.0.0.1:31337".parse().unwrap();

        // Simulate: file-loaded gateways have this address with old key
        let mut file_gateways = Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(addr),
                public_key_path: PathBuf::from("old/stale/key.pub"),
                location: None,
            }],
        };

        // User provides fresh key via CLI
        let gw = parse_gateway(&format!("{addr},{key_hex}"), tmp_dir.path()).unwrap();
        let cli_key_path = gw.public_key_path.clone();
        let mut cli_gateways = Gateways { gateways: vec![gw] };

        // CLI gateways go first so they win deduplication
        cli_gateways.merge_and_deduplicate(file_gateways);
        file_gateways = cli_gateways;

        assert_eq!(file_gateways.gateways.len(), 1);
        // The CLI-provided key path should win, not the stale file one
        assert_eq!(file_gateways.gateways[0].public_key_path, cli_key_path);
    }

    #[tokio::test]
    async fn test_config_build_network_mode_empty_gateway() {
        // An empty vec in --gateway should NOT bypass the "no gateways" error.
        let temp_dir = tempfile::tempdir().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![]),
                skip_load_from_network: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let err = args.build().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot initialize node without gateways"),
            "Expected 'Cannot initialize node without gateways', got: {err}"
        );
    }

    /// Serve an empty gateway index from a local mock server. Used to drive the
    /// remote-fetch path (i.e. `--skip-load-from-network` is NOT set) into the
    /// file-load fallback branch deterministically, without reaching out to the
    /// real `freenet.org` index (which would be slow and flaky in CI).
    fn empty_gateways_index_server() -> (Server, String) {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method("GET"))
                .times(..)
                .respond_with(status_code(200).body("")),
        );
        let url = server.url_str("/gateways.toml");
        (server, url)
    }

    /// Regression test for #4268: an isolated gateway — remote index
    /// unreachable/empty, no on-disk `gateways.toml`, and no
    /// `--gateway`/`--gateways` — must still be allowed to start with an empty
    /// bootstrap list. Before the #4268 fix, the file-load fallback branch
    /// guarded on `peer_id.is_none()` and wrongly rejected such a gateway with
    /// "Cannot initialize node without gateways". This is the file-load
    /// analogue of the `--skip-load-from-network` guard fixed in PR #4264.
    ///
    /// The gateway is configured with both `--public-network-address` and
    /// `--public-network-port` (required for gateways since #4324), so it has a
    /// valid `peer_id`. This test pins the *bootstrap* behavior, independent of
    /// identity derivation.
    ///
    /// Note: `skip_load_from_network` is intentionally NOT set here — that flag
    /// would route an `is_gateway` node through the earlier
    /// `skip_load && is_gateway` branch and never reach the file-load guard
    /// this test exercises.
    #[tokio::test]
    async fn test_file_load_branch_isolated_gateway_succeeds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();
        // No gateways.toml is written: the config_dir is empty, so File::open
        // fails and we hit the guard under test.
        assert!(!config_dir.join("gateways.toml").exists());

        let (_server, index_url) = empty_gateways_index_server();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: true,
                public_address: Some("203.0.113.10".parse().unwrap()),
                public_port: Some(31337),
                network_port: Some(31337),
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args
            .build_with_gateways_index(&index_url)
            .await
            .expect("isolated gateway with no bootstrap gateways must be allowed to start");

        assert!(cfg.is_gateway);
        assert!(
            cfg.gateways.is_empty(),
            "isolated gateway should start with no bootstrap gateways, got {:?}",
            cfg.gateways
        );
        assert!(
            cfg.peer_id.is_some(),
            "expected peer_id to be derived from public address + port"
        );
    }

    /// Regression test for #4324: a gateway started with
    /// `--is-gateway --public-network-address X --network-port Y` but NO
    /// `--public-network-port` must be rejected at config-build time.
    ///
    /// Such a gateway would otherwise derive `peer_id == None` (peer_id =
    /// `public_address.zip(public_port)`) → `own_addr == None` → no ring
    /// location. Unlike a NAT'd peer, a gateway has no upstream to learn or
    /// correct its address later, so it would stay degraded permanently. The
    /// agreed fix (maintainer consensus on the issue) is to fail fast and
    /// require the public port explicitly, rather than silently falling back
    /// to the local bind port.
    #[tokio::test]
    async fn test_gateway_without_public_port_is_rejected() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();

        let (_server, index_url) = empty_gateways_index_server();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: true,
                // network_port set but no public_port: it no longer substitutes
                // for the public port, so this must be rejected.
                public_address: Some("203.0.113.10".parse().unwrap()),
                network_port: Some(31337),
                public_port: None,
                ..Default::default()
            },
            ..Default::default()
        };

        let err = args
            .build_with_gateways_index(&index_url)
            .await
            .expect_err("gateway without --public-network-port must be rejected");
        assert!(
            err.to_string().contains("public network port"),
            "expected error to mention the missing public network port, got: {err}"
        );
    }

    /// Companion to #4268: the widened guard must NOT let a *non-gateway* peer
    /// start with no bootstrap gateways. A regular peer with `peer_id == None`,
    /// an empty config_dir, an empty remote index, and no `--gateway` entries
    /// has nothing to connect to and must still be rejected with "Cannot
    /// initialize node without gateways". This pins that the fix only relaxes
    /// the guard for gateways, not for peers.
    #[tokio::test]
    async fn test_file_load_branch_non_gateway_without_gateways_is_rejected() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();
        assert!(!config_dir.join("gateways.toml").exists());

        let (_server, index_url) = empty_gateways_index_server();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: false,
                // No public_port -> peer_id == None, mirroring the gateway case,
                // but as a non-gateway this peer genuinely cannot bootstrap.
                ..Default::default()
            },
            ..Default::default()
        };

        let err = args
            .build_with_gateways_index(&index_url)
            .await
            .expect_err("non-gateway peer without any gateways must be rejected");
        assert!(
            err.to_string()
                .contains("Cannot initialize node without gateways"),
            "Expected 'Cannot initialize node without gateways', got: {err}"
        );
    }

    /// Pin for #4268: the guard must keep its original `peer_id.is_none()`
    /// condition so a non-gateway peer that DOES have a public identity
    /// (`--public-network-address` + `--public-network-port`, hence
    /// `peer_id == Some`) is still allowed to initialize as a disjoint
    /// bootstrap node when no gateways are available. The first draft of the
    /// #4268 fix gated solely on `!is_gateway`, which would have wrongly
    /// rejected this previously-supported startup path; this test locks it in.
    #[tokio::test]
    async fn test_file_load_branch_public_non_gateway_bootstraps_disjoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();
        assert!(!config_dir.join("gateways.toml").exists());

        let (_server, index_url) = empty_gateways_index_server();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: false,
                // Public identity set -> peer_id == Some, so the node may start
                // disjoint even though no gateways are available.
                public_address: Some("198.51.100.7".parse().unwrap()),
                public_port: Some(31337),
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args
            .build_with_gateways_index(&index_url)
            .await
            .expect("public non-gateway peer must be allowed to bootstrap disjoint");
        assert!(!cfg.is_gateway);
        assert!(
            cfg.gateways.is_empty(),
            "disjoint peer should start with no gateways, got {:?}",
            cfg.gateways
        );
    }

    /// Regression test for #3980: when `--skip-load-from-network` is combined
    /// with an explicit `--gateway` entry, the on-disk `gateways.toml` must
    /// NOT be merged into the result. Without this guarantee, a default-install
    /// machine whose `gateways.toml` lists public peers (e.g. nova/vega) would
    /// have those public peers dialed by an "isolated" test node — exactly
    /// the leak #3980 reported.
    ///
    /// Note: when --gateway is NOT supplied under skip_load_from_network, the
    /// on-disk gateways.toml IS still read — that path is the contract used
    /// by isolated test harnesses (e.g. freenet-test-network's Docker NAT)
    /// that pre-populate gateways.toml in a custom --config-dir.
    #[tokio::test]
    async fn test_skip_load_from_network_with_cli_gateway_ignores_on_disk_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();
        let gateways_file = config_dir.join("gateways.toml");

        // Pre-populate gateways.toml with a "production" gateway entry that
        // the test must NOT leak to the final config.
        let public_gateway_addr: SocketAddr = "203.0.113.99:31337".parse().unwrap();
        let preexisting = toml::to_string(&Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(public_gateway_addr),
                public_key_path: PathBuf::from("public_gateway.pub"),
                location: None,
            }],
        })
        .unwrap();
        fs::write(&gateways_file, preexisting).unwrap();

        let isolated_keypair = TransportKeypair::new();
        let isolated_key_hex = hex::encode(isolated_keypair.public().as_bytes());
        let isolated_addr: SocketAddr = "127.0.0.1:31338".parse().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![format!("{isolated_addr},{isolated_key_hex}")]),
                skip_load_from_network: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();

        // The CLI-provided isolated gateway must be the ONLY entry. The public
        // gateway from the on-disk file must NOT leak into the final list.
        assert_eq!(cfg.gateways.len(), 1, "gateways={:?}", cfg.gateways);
        assert_eq!(
            cfg.gateways[0].address,
            Address::HostAddress(isolated_addr),
            "isolated gateway should be selected"
        );
        assert!(
            !cfg.gateways
                .iter()
                .any(|gw| gw.address == Address::HostAddress(public_gateway_addr)),
            "on-disk gateways.toml leaked into final config despite skip_load_from_network"
        );
    }

    /// Companion to #3980: under `--skip-load-from-network` with NO `--gateway`
    /// CLI entries, the on-disk `gateways.toml` MUST still be read. This is
    /// the contract used by isolated test harnesses (notably
    /// freenet-test-network's Docker NAT path) that pre-populate
    /// `gateways.toml` in a custom `--config-dir`. Regressed once during
    /// review of #4264 when the skip path was widened too aggressively;
    /// keep this test to pin the contract.
    #[tokio::test]
    async fn test_skip_load_from_network_without_cli_gateways_reads_on_disk_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();
        let gateways_file = config_dir.join("gateways.toml");

        let test_gateway_addr: SocketAddr = "10.20.30.40:31337".parse().unwrap();
        let preexisting = toml::to_string(&Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(test_gateway_addr),
                public_key_path: PathBuf::from("test_gateway.pub"),
                location: None,
            }],
        })
        .unwrap();
        fs::write(&gateways_file, preexisting).unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: false,
                // No --gateway / --gateways supplied; harness pre-populated
                // gateways.toml is the only bootstrap source.
                public_address: Some("198.51.100.1".parse().unwrap()),
                public_port: Some(31338),
                skip_load_from_network: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        assert_eq!(cfg.gateways.len(), 1, "gateways={:?}", cfg.gateways);
        assert_eq!(
            cfg.gateways[0].address,
            Address::HostAddress(test_gateway_addr),
            "on-disk gateways.toml must be honored when --gateway is not supplied"
        );
    }

    /// Pin: under `--skip-load-from-network --is-gateway`, the on-disk
    /// gateways.toml is NOT read regardless of whether `--gateway` is set.
    /// An isolated gateway runs without any bootstrap peers (unless inline
    /// `--gateways` JSON entries are supplied). This is the pre-existing
    /// behavior of the `skip_load && is_gateway` branch and matches the
    /// docstring contract.
    #[tokio::test]
    async fn test_skip_load_from_network_gateway_mode_ignores_on_disk_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();
        let gateways_file = config_dir.join("gateways.toml");

        // Pre-populate gateways.toml — must NOT leak into an isolated gateway.
        let leaked_addr: SocketAddr = "192.0.2.50:31337".parse().unwrap();
        let preexisting = toml::to_string(&Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(leaked_addr),
                public_key_path: PathBuf::from("leaked.pub"),
                location: None,
            }],
        })
        .unwrap();
        fs::write(&gateways_file, preexisting).unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: true,
                public_address: Some("198.51.100.1".parse().unwrap()),
                public_port: Some(31337),
                skip_load_from_network: true,
                // No --gateway supplied.
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        assert!(cfg.is_gateway);
        assert!(
            cfg.gateways.is_empty(),
            "isolated gateway must NOT read gateways.toml; got {:?}",
            cfg.gateways
        );
    }

    /// Pin: under `--skip-load-from-network --gateway X --gateways JSON_Y`,
    /// both the CLI entry and the JSON entry reach the final config — the
    /// new "strict additive --gateway" branch must preserve any inline
    /// --gateways JSON entries rather than silently dropping them.
    #[tokio::test]
    async fn test_skip_load_from_network_preserves_inline_gateways_with_cli_gateway() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();

        // Pre-populate gateways.toml with an entry that MUST NOT leak.
        let leaked_addr: SocketAddr = "192.0.2.99:31337".parse().unwrap();
        let preexisting = toml::to_string(&Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(leaked_addr),
                public_key_path: PathBuf::from("leaked.pub"),
                location: None,
            }],
        })
        .unwrap();
        fs::write(config_dir.join("gateways.toml"), preexisting).unwrap();

        // Inline --gateways JSON: a test gateway address.
        let json_keypair = TransportKeypair::new();
        let json_key_path = config_dir.join("json_gw.pub");
        fs::write(
            &json_key_path,
            hex::encode(json_keypair.public().as_bytes()),
        )
        .unwrap();
        let json_addr: SocketAddr = "10.10.10.10:31337".parse().unwrap();
        let json_inline = serde_json::to_string(&InlineGwConfig {
            address: json_addr,
            public_key_path: json_key_path,
            location: None,
        })
        .unwrap();

        // CLI --gateway: another distinct test gateway address.
        let cli_keypair = TransportKeypair::new();
        let cli_key_hex = hex::encode(cli_keypair.public().as_bytes());
        let cli_addr: SocketAddr = "10.20.20.20:31337".parse().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![format!("{cli_addr},{cli_key_hex}")]),
                gateways: Some(vec![json_inline]),
                skip_load_from_network: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        let addrs: Vec<_> = cfg.gateways.iter().map(|gw| gw.address.clone()).collect();
        assert!(
            addrs.contains(&Address::HostAddress(cli_addr)),
            "CLI --gateway must be in final list: {addrs:?}"
        );
        assert!(
            addrs.contains(&Address::HostAddress(json_addr)),
            "Inline --gateways JSON entry must be in final list: {addrs:?}"
        );
        assert!(
            !addrs.contains(&Address::HostAddress(leaked_addr)),
            "On-disk gateways.toml must NOT leak when --gateway is supplied: {addrs:?}"
        );
        assert_eq!(
            addrs.len(),
            2,
            "expected exactly cli + json entries: {addrs:?}"
        );
    }

    /// Pin: `skip_load_from_network + is_gateway + --gateway X` with a public
    /// gateway in the on-disk gateways.toml. The pre-existing
    /// `skip_load && is_gateway` branch fires first and ignores the file;
    /// the post-block merge then prepends the CLI --gateway entry. Final
    /// gateways list must be exactly [X] with no on-disk leak. Covers the
    /// four-way combination flagged in re-review for #4264.
    #[tokio::test]
    async fn test_skip_load_from_network_isolated_gateway_with_cli_gateway_ignores_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path();

        let leaked_addr: SocketAddr = "192.0.2.77:31337".parse().unwrap();
        let preexisting = toml::to_string(&Gateways {
            gateways: vec![GatewayConfig {
                address: Address::HostAddress(leaked_addr),
                public_key_path: PathBuf::from("leaked.pub"),
                location: None,
            }],
        })
        .unwrap();
        fs::write(config_dir.join("gateways.toml"), preexisting).unwrap();

        let cli_keypair = TransportKeypair::new();
        let cli_key_hex = hex::encode(cli_keypair.public().as_bytes());
        let cli_addr: SocketAddr = "10.30.30.30:31337".parse().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Network),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                is_gateway: true,
                public_address: Some("198.51.100.7".parse().unwrap()),
                public_port: Some(31337),
                gateway: Some(vec![format!("{cli_addr},{cli_key_hex}")]),
                skip_load_from_network: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        let addrs: Vec<_> = cfg.gateways.iter().map(|gw| gw.address.clone()).collect();
        assert!(cfg.is_gateway);
        assert_eq!(addrs.len(), 1, "expected only the CLI gateway: {addrs:?}");
        assert_eq!(addrs[0], Address::HostAddress(cli_addr));
        assert!(
            !addrs.contains(&Address::HostAddress(leaked_addr)),
            "isolated gateway leaked file content: {addrs:?}"
        );
    }

    #[tokio::test]
    async fn test_config_build_invalid_gateway_error() {
        // An unparseable --gateway value should propagate a clear error.
        let temp_dir = tempfile::tempdir().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec!["not-valid".into()]),
                ..Default::default()
            },
            ..Default::default()
        };

        let err = args.build().await.unwrap_err();
        assert!(
            err.to_string().contains("Failed to parse --gateway"),
            "Expected 'Failed to parse --gateway', got: {err}"
        );
    }

    #[tokio::test]
    async fn test_config_build_duplicate_gateway_entries() {
        // Two identical --gateway entries should be deduplicated to one.
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let entry = format!("10.0.0.1:31337,{key_hex}");
        let temp_dir = tempfile::tempdir().unwrap();

        let args = ConfigArgs {
            mode: Some(OperationMode::Local),
            config_paths: ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
                log_dir: Some(temp_dir.path().to_path_buf()),
            },
            network_api: NetworkArgs {
                gateway: Some(vec![entry.clone(), entry]),
                ..Default::default()
            },
            ..Default::default()
        };

        let cfg = args.build().await.unwrap();
        assert_eq!(cfg.gateways.len(), 1);
    }

    #[test]
    fn test_parse_gateway_key_file_permissions() {
        let keypair = TransportKeypair::new();
        let key_hex = hex::encode(keypair.public().as_bytes());
        let tmp_dir = tempfile::tempdir().unwrap();

        let gw = parse_gateway(&format!("192.168.1.1:31337,{key_hex}"), tmp_dir.path()).unwrap();

        assert!(gw.public_key_path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&gw.public_key_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "Key file should have 0600 permissions");
        }
    }

    // =========================================================================
    // Webapp cache root resolution
    // =========================================================================

    /// A non-empty override wins; an EMPTY one reads as unset.
    ///
    /// `FREENET_WEBAPP_CACHE_DIR=` yields `Some("")` from `var_os`, and taking
    /// that literally roots the cache at `PathBuf::new()`: every derived entry
    /// path becomes relative (written under the node's working directory) and
    /// the sweep's `read_dir("")` fails, so the size bound never runs. Silent,
    /// non-destructive, and exactly the unbounded cache this whole change
    /// exists to remove.
    ///
    /// Driven through `resolve_webapp_cache_dir` rather than by setting the
    /// variable: `set_var` is `unsafe` in edition 2024 precisely because tests
    /// share one process environment, and a racing writer here would be a
    /// flake, not a finding.
    #[test]
    fn an_empty_webapp_cache_dir_override_reads_as_unset() {
        let default = resolve_webapp_cache_dir(None);
        assert!(
            default.is_absolute() && default.ends_with("webapp_cache"),
            "premise: with no override the resolved root is the absolute XDG \
             default; got {}",
            default.display()
        );

        let explicit = std::path::PathBuf::from("/tmp/freenet-webapp-cache-test");
        assert_eq!(
            resolve_webapp_cache_dir(Some(explicit.clone().into_os_string())),
            explicit,
            "a non-empty override must be honoured verbatim"
        );

        assert_eq!(
            resolve_webapp_cache_dir(Some(std::ffi::OsString::new())),
            default,
            "an override that is set but EMPTY must fall back to the default \
             root. Honouring it roots the cache at \"\", which writes entries \
             under the process's working directory and disables the size sweep \
             entirely (read_dir(\"\") fails), with nothing logged or returned to \
             say so."
        );
    }

    /// `default_webapp_cache_dir` must consult the environment THROUGH the
    /// resolver above, not read it raw and not skip it.
    ///
    /// The test above proves the resolver's rule; this proves the rule is the
    /// one production runs. Deleting the `var_os` read (the operator override
    /// stops working) or bypassing `resolve_webapp_cache_dir` (the empty case
    /// comes back) both leave every other test in this file green.
    #[test]
    fn default_webapp_cache_dir_resolves_the_env_override() {
        let src = production_source();
        let body = extract_fn_body(src, "pub fn default_webapp_cache_dir()");
        let collapsed: String = body.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            collapsed.contains(concat!(
                "resolve_webapp_cache_dir(std::env::var_os(",
                "WEBAPP_CACHE_DIR_ENV))"
            )),
            "default_webapp_cache_dir must feed the environment read straight \
             into the resolver; anything else means the operator override or \
             the empty-value rule is no longer what production applies. Body: \
             `{collapsed}`"
        );
    }

    /// The standalone `WebsocketApiConfig` constructors resolve the REAL cache
    /// directory, deliberately, and this pins that as a decision rather than an
    /// accident.
    ///
    /// The obvious-looking alternative is to mirror `secrets_dir`, whose
    /// `Default` is an empty `PathBuf`, so that a test composing a server from
    /// `WebsocketApiConfig::default()` could not reach the developer's cache.
    /// Rejected, for three reasons:
    ///
    /// 1. Empty is not benign here. It is benign for `secrets_dir` only because
    ///    that field's consumer reads empty as "stamping disabled". This field's
    ///    consumer has no such rule: an empty root writes cache entries under
    ///    the process's working directory and skips the sweep (see
    ///    `an_empty_webapp_cache_dir_override_reads_as_unset`). Copying the
    ///    value without the consumer semantics copies the look of the
    ///    precedent, not its safety.
    /// 2. It would not close the door it is aimed at. `HttpClientApi::as_router`
    ///    is the direct router-composition entry a test would reach for, and it
    ///    resolves `default_webapp_cache_dir()` itself. Its signature is public
    ///    API and carries no cache root.
    /// 3. These constructors are the fallback for any future serving path that
    ///    is not `ConfigArgs::build()`, where an unbounded cache under an
    ///    arbitrary working directory would be strictly worse than a shared but
    ///    bounded one in the canonical location.
    ///
    /// The residual risk is documented on the field and made audible by
    /// `WebappCache::with_root`, which logs the resolved root once at startup.
    #[test]
    fn standalone_websocket_api_config_resolves_the_real_webapp_cache_dir() {
        let expected = default_webapp_cache_dir();
        assert_eq!(
            WebsocketApiConfig::default().webapp_cache_dir,
            expected,
            "Default must resolve the real cache root; see this test's rustdoc \
             before changing it to an empty path"
        );
        assert_eq!(
            WebsocketApiConfig::from(SocketAddr::from(([127, 0, 0, 1], 50509))).webapp_cache_dir,
            expected,
            "From<SocketAddr> must agree with Default; a split between them is \
             how one composition path silently gets a different cache"
        );
    }

    /// Production half of this file, for the source pins above.
    ///
    /// Cut at `mod tests {`, NOT at `#[cfg(test)]`: the latter also sits on
    /// individual test-only items elsewhere in the tree, so a future one landing
    /// above this module would truncate the slice and quietly disarm every pin
    /// that reads it. The needle is split so it cannot match its own source.
    fn production_source() -> &'static str {
        const FULL: &str = include_str!("config.rs");
        let cutoff = FULL
            .find(concat!("mod ", "tests {"))
            .expect("config.rs must have a test module");
        &FULL[..cutoff]
    }

    /// Body of the named function within `source`, brace-balanced.
    fn extract_fn_body<'a>(source: &'a str, signature_prefix: &str) -> &'a str {
        let start = source
            .find(signature_prefix)
            .unwrap_or_else(|| panic!("could not find {signature_prefix}"));
        let brace = source[start..].find('{').expect("fn signature has a body");
        let body_start = start + brace + 1;
        let bytes = source.as_bytes();
        let mut depth: i32 = 1;
        let mut i = body_start;
        while i < bytes.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &source[body_start..i];
                    }
                }
                _ => {}
            }
            i += 1;
        }
        panic!("unterminated body for {signature_prefix}");
    }

    // ------------------------------------------------------------------
    // GHSA-824h-7x5x-wfmf — the client API defaults to loopback in BOTH
    // operation modes.
    //
    // Before this change `OperationMode::Network` defaulted the client API to
    // the wildcard `::`, so every host on the LAN could drive a fully
    // privileged control API (contract state, delegate secrets, key material)
    // on any node whose operator had done nothing but run `freenet network`.
    // `OperationMode` still governs ring participation and `secrets_dir(mode)`;
    // it no longer governs who may drive the client API.
    // ------------------------------------------------------------------

    /// Build a `ConfigArgs` in `mode` with everything unrelated to the client
    /// API pinned to something inert, so a test can vary only the ws-api knobs.
    fn ws_api_test_args(mode: OperationMode, config_dir: &std::path::Path) -> ConfigArgs {
        ConfigArgs {
            mode: Some(mode),
            config_paths: ConfigPathsArgs {
                config_dir: Some(config_dir.to_path_buf()),
                data_dir: Some(config_dir.to_path_buf()),
                log_dir: Some(config_dir.to_path_buf()),
            },
            network_api: NetworkArgs {
                // A public identity means the peer may bootstrap disjoint, so
                // an empty gateway index is not a build error and the test
                // stays focused on the ws-api resolution.
                public_address: Some("198.51.100.7".parse().unwrap()),
                public_port: Some(31337),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn ws_api_defaults_to_loopback_in_network_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let cfg = ws_api_test_args(OperationMode::Network, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("build should succeed");

        assert_eq!(
            cfg.ws_api.address,
            default_local_address(),
            "a plain `freenet network` node must NOT expose its client API to the LAN"
        );
        assert!(cfg.ws_api.address.is_loopback());
    }

    #[tokio::test]
    async fn ws_api_defaults_to_loopback_in_local_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let cfg = ws_api_test_args(OperationMode::Local, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("build should succeed");

        assert_eq!(cfg.ws_api.address, default_local_address());
    }

    #[tokio::test]
    async fn ws_api_auto_widens_when_allowed_source_cidrs_is_set() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut args = ws_api_test_args(OperationMode::Network, temp_dir.path());
        // The canonical Tailscale invocation, which worked before this change
        // only because the default happened to be `::`.
        args.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);

        let cfg = args
            .build_with_gateways_index(&index_url)
            .await
            .expect("build should succeed");

        assert_eq!(
            cfg.ws_api.address,
            default_listening_address(),
            "--allowed-source-cidrs is a no-op on a loopback listener, so it must widen the bind"
        );
    }

    /// `--allowed-host` must NOT widen the bind. It is a Host-header allowlist
    /// that works perfectly on loopback, and the same-host reverse proxy is its
    /// primary documented use — the only shape in which hosted mode honours
    /// `userToken` at all, since `decide_user_token` requires a loopback source.
    /// Widening for it would bind every interface in the commonest proxy
    /// deployment for no functional gain.
    #[tokio::test]
    async fn allowed_host_alone_does_not_widen_the_bind() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut args = ws_api_test_args(OperationMode::Network, temp_dir.path());
        args.ws_api.allowed_host = Some(vec!["node.example.org".to_string()]);

        let cfg = args
            .build_with_gateways_index(&index_url)
            .await
            .expect("build should succeed");

        assert_eq!(cfg.ws_api.address, default_local_address());
        assert_eq!(
            cfg.ws_api.allowed_hosts.len(),
            1,
            "the allowlist still applies"
        );
    }

    #[tokio::test]
    async fn explicit_ws_api_address_is_never_auto_widened() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        // This is try.freenet.org's exact shape: an explicit loopback bind
        // behind a reverse proxy that is named with --allowed-host. The
        // explicit flag must win, or the hardening would silently un-harden
        // the one deployment that already did the right thing.
        let mut args = ws_api_test_args(OperationMode::Network, temp_dir.path());
        args.ws_api.address = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
        args.ws_api.allowed_host = Some(vec!["try.freenet.org".to_string()]);
        args.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);

        let cfg = args
            .build_with_gateways_index(&index_url)
            .await
            .expect("build should succeed");

        assert_eq!(cfg.ws_api.address, IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    /// An empty allow-list grants nothing, so it must not be read as intent to
    /// widen. This is not hypothetical: the config-file merge hands `build()`
    /// the persisted `allowed-host = []` / `allowed-source-cidrs = []` that
    /// every already-booted node has in its `config.toml`.
    #[test]
    fn empty_allow_lists_do_not_widen() {
        for mode in [OperationMode::Network, OperationMode::Local] {
            assert_eq!(
                resolve_ws_api_address(mode, None, Some(&[]), None),
                (default_local_address(), WsApiAddressSource::DefaultLoopback)
            );
            assert_eq!(
                resolve_ws_api_address(mode, None, None, None),
                (default_local_address(), WsApiAddressSource::DefaultLoopback)
            );
        }
    }

    /// `FREENET_ALLOWED_HOST=` declared with no value — routine in a docker-compose
    /// `.env`, a k8s ConfigMap, or a systemd `Environment=` line — parses as
    /// `Some(vec![""])`. Reading that as intent would widen the bind on a node
    /// whose operator granted nothing.
    #[test]
    fn blank_allow_list_entries_do_not_widen() {
        assert_eq!(
            resolve_ws_api_address(
                OperationMode::Network,
                None,
                Some(&[String::new(), "   ".to_string()]),
                None,
            ),
            (default_local_address(), WsApiAddressSource::DefaultLoopback)
        );
    }

    /// Local mode has ALWAYS bound loopback, so there is no wide default to
    /// preserve there. Widening for it would mean this hardening OPENED a
    /// socket that used to be closed — the exact thing it exists to prevent.
    #[test]
    fn local_mode_never_auto_widens() {
        for hint in [None, Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED))] {
            let (addr, source) = resolve_ws_api_address(
                OperationMode::Local,
                None,
                Some(&["100.64.0.0/10".to_string()]),
                hint,
            );
            assert_eq!(source, WsApiAddressSource::DefaultLoopback);
            assert!(
                addr.is_loopback(),
                "local mode must stay loopback, got {addr}"
            );
        }
    }

    #[test]
    fn resolve_ws_api_address_reports_how_it_decided() {
        let explicit = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50));
        for mode in [OperationMode::Network, OperationMode::Local] {
            assert_eq!(
                resolve_ws_api_address(mode, Some(explicit), None, None),
                (explicit, WsApiAddressSource::Explicit)
            );
        }
        assert_eq!(
            resolve_ws_api_address(
                OperationMode::Network,
                None,
                Some(&["10.0.0.0/8".to_string()]),
                None
            ),
            (default_listening_address(), WsApiAddressSource::AutoWidened)
        );
        // `--allowed-host` is deliberately NOT a widening trigger: it is fully
        // functional on a loopback socket, where the same-host reverse proxy —
        // the only shape in which hosted mode honours `userToken` — lives.
        assert_eq!(
            resolve_ws_api_address(OperationMode::Network, None, None, None),
            (default_local_address(), WsApiAddressSource::DefaultLoopback)
        );
    }

    /// The migration sentinel is "a value this code could have written", which
    /// must include the loopback default — see
    /// `remedy_still_works_after_the_upgrade_persisted_loopback`.
    #[test]
    fn auto_derivable_addresses_are_exactly_the_ones_this_code_writes() {
        for auto in [
            default_listening_address(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            default_local_address(),
        ] {
            assert!(is_auto_derivable_ws_api_address(auto), "{auto}");
        }
        // `127.0.0.1` is auto-derivable too: it is what an IPv4-family node
        // re-derives to, so treating it as a choice would strand exactly the
        // hosts B1 exists to protect.
        assert!(is_auto_derivable_ws_api_address(IpAddr::V4(
            Ipv4Addr::LOCALHOST
        )));
        // A deliberately-pinned loopback ALIAS is a choice — this code never
        // writes one, which is why the sentinel enumerates instead of calling
        // `is_loopback()`.
        for chosen in [
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 5)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)),
        ] {
            assert!(!is_auto_derivable_ws_api_address(chosen), "{chosen}");
        }
    }

    /// The exposure warning fires on a non-loopback bind REGARDLESS of hosted
    /// mode (an untokened connection reaches the shared namespace either way),
    /// and additionally on (non-loopback bind OR
    /// an `--allowed-host` reverse-proxy grant). Both triggers matter: a proxy
    /// terminates the connection itself, so every visitor looks local to the
    /// node's own source-IP filters even when the bind is `127.0.0.1`.
    #[test]
    fn exposure_warning_fires_only_when_one_namespace_is_reachable_remotely() {
        let loopback = default_local_address();
        let wildcard = default_listening_address();
        let proxy = vec!["try.freenet.org".to_string()];

        // Quiet: the default single-user desktop node.
        assert_eq!(
            ws_api_shares_one_namespace_with_remote_clients(false, loopback, &[]),
            None
        );
        // Quiet: try.freenet.org's shape. A KNOWN GAP rather than a safety
        // claim — an untokened connection reads the shared namespace here too.
        // Containment today is that the namespace is empty; making the warning
        // conditional on that needs a probe of the secrets tree, tracked
        // separately so a probe that is wrong in either direction cannot ride
        // in on a default-hardening change.
        assert_eq!(
            ws_api_shares_one_namespace_with_remote_clients(true, loopback, &proxy),
            None
        );

        // Fires: reachable from other machines, one shared namespace. The
        // reason names the bind, which is the thing to change.
        let bind_reason = ws_api_shares_one_namespace_with_remote_clients(false, wildcard, &[])
            .expect("a wildcard bind with hosted mode off must warn");
        assert!(bind_reason.contains("non-loopback"), "{bind_reason}");

        // Fires: hosted mode does NOT make a wide bind safe. `decide_user_token`
        // returns the shared context whenever a connection omits `userToken`.
        let hosted_wide = ws_api_shares_one_namespace_with_remote_clients(true, wildcard, &proxy)
            .expect("a wildcard bind must warn even in hosted mode");
        assert!(hosted_wide.contains("hosted mode does"), "{hosted_wide}");

        // Fires: proxied with hosted mode off, the shape with no isolation.
        let proxy_reason = ws_api_shares_one_namespace_with_remote_clients(false, loopback, &proxy)
            .expect("a proxied node with hosted mode off must warn");
        assert!(proxy_reason.contains("--allowed-host"), "{proxy_reason}");
    }

    /// `build()` persists the RESOLVED config, so every node that has already
    /// booted in network mode carries the old auto-default
    /// `ws-api-address = "::"` in its `config.toml`. Merging that back would
    /// pin the entire existing fleet to the wildcard bind and leave the
    /// hardening reaching fresh installs only.
    #[tokio::test]
    async fn persisted_legacy_wildcard_address_is_re_derived_to_loopback() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        // First boot on the OLD code: resolve to `::` and persist it.
        let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
        first.ws_api.address = Some(default_listening_address());
        let first_cfg = first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");
        assert_eq!(first_cfg.ws_api.address, default_listening_address());
        let persisted =
            std::fs::read_to_string(temp_dir.path().join("config.toml")).expect("config persisted");
        assert!(
            persisted.contains(r#"ws-api-address = "::""#),
            "test premise: the wildcard must actually be written to config.toml, got:\n{persisted}"
        );

        // Second boot on the NEW code, flag-less: the persisted `::` is
        // recognised as the old auto-default and re-derived.
        let cfg = ws_api_test_args(OperationMode::Network, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("second build should succeed");
        assert_eq!(
            cfg.ws_api.address,
            default_local_address(),
            "an upgrading node must stop exposing its client API to the LAN"
        );
    }

    /// Re-derivation must PRESERVE THE ADDRESS FAMILY, because the primary bind
    /// is fatal while only the companion is best-effort (`server.rs`
    /// `serve_dual_stack`).
    ///
    /// A node installed before #3648 persisted `0.0.0.0`. Re-deriving that to
    /// the IPv6 `::1` on a host with IPv6 disabled makes the primary bind fail
    /// with EAFNOSUPPORT — and `build()` has already rewritten `config.toml`
    /// before the bind is attempted, so the crash-loop rollback restores the
    /// previous binary onto a config that now says `::1` and it dies the same
    /// way. Rollback does not fire twice: the node is down for good. The
    /// brick-safety mechanism becomes the brick, reached through a VALUE change
    /// rather than the key change `code-style.md` already guards.
    #[tokio::test]
    async fn re_derivation_preserves_the_address_family() {
        for (persisted, expected) in [
            (
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ),
            (
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ),
        ] {
            let temp_dir = tempfile::tempdir().unwrap();
            let (_server, index_url) = empty_gateways_index_server();

            let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
            first.ws_api.address = Some(persisted);
            first
                .build_with_gateways_index(&index_url)
                .await
                .expect("first build should succeed");

            let cfg = ws_api_test_args(OperationMode::Network, temp_dir.path())
                .build_with_gateways_index(&index_url)
                .await
                .expect("second build should succeed");
            assert_eq!(
                cfg.ws_api.address, expected,
                "persisted {persisted} must re-derive within its own family; \
                 crossing families is a fatal bind on a single-stack host"
            );
        }
    }

    /// Same requirement for the auto-widen branch: an IPv4-family node that
    /// applies the documented remedy must widen to `0.0.0.0`, not `::`.
    #[tokio::test]
    async fn auto_widen_preserves_the_address_family() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
        first.ws_api.address = Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");

        let mut remedied = ws_api_test_args(OperationMode::Network, temp_dir.path());
        remedied.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        let cfg = remedied
            .build_with_gateways_index(&index_url)
            .await
            .expect("second build should succeed");
        assert_eq!(cfg.ws_api.address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    /// The migration keys on "a value this code could have written" and nothing
    /// else: a specific interface address is never auto-written, so it is an
    /// operator choice and must survive an upgrade untouched.
    ///
    /// Note `127.0.0.1` is deliberately NOT in this test any more. It became
    /// auto-derivable when re-derivation started preserving the address family
    /// — it is what an IPv4 node re-derives TO — so it round-trips to itself
    /// rather than being preserved. Same observable value, different mechanism;
    /// asserting it here would pass for the wrong reason. Its real coverage is
    /// `auto_derivable_addresses_are_exactly_the_ones_this_code_writes`.
    #[tokio::test]
    async fn persisted_explicit_address_survives_the_migration() {
        for chosen in [
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 5)),
        ] {
            let temp_dir = tempfile::tempdir().unwrap();
            let (_server, index_url) = empty_gateways_index_server();

            let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
            first.ws_api.address = Some(chosen);
            first
                .build_with_gateways_index(&index_url)
                .await
                .expect("first build should succeed");

            let cfg = ws_api_test_args(OperationMode::Network, temp_dir.path())
                .build_with_gateways_index(&index_url)
                .await
                .expect("second build should succeed");
            assert_eq!(cfg.ws_api.address, chosen);
        }
    }

    /// The upgrade path for a Tailscale-style node whose systemd unit passes
    /// `--allowed-source-cidrs` on every boot: the migration drops the
    /// persisted `::` and auto-widening puts it straight back. Zero operator
    /// action, same bind as before the upgrade.
    #[tokio::test]
    async fn upgrading_node_with_cidr_grant_keeps_its_wildcard_bind() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
        first.ws_api.address = Some(default_listening_address());
        first.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");

        // Restart with the flag still in the invocation, as a service does.
        let mut second = ws_api_test_args(OperationMode::Network, temp_dir.path());
        second.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        let cfg = second
            .build_with_gateways_index(&index_url)
            .await
            .expect("second build should succeed");
        assert_eq!(cfg.ws_api.address, default_listening_address());
        assert_eq!(cfg.ws_api.allowed_source_cidrs.len(), 1);
    }

    /// The auto-widen must NOT be sticky. A grant that only survives in
    /// `config.toml` cannot widen a later boot, or one run with the flag would
    /// pin the node to the wildcard forever and REMOVING the flag would never
    /// narrow it again — the hardening would permanently miss every node that
    /// ever set an allow-list.
    #[tokio::test]
    async fn a_persisted_cidr_grant_does_not_widen_a_later_flagless_boot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
        first.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        let widened = first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");
        assert_eq!(widened.ws_api.address, default_listening_address());

        // Flag removed from the invocation; only config.toml still names it.
        let cfg = ws_api_test_args(OperationMode::Network, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("second build should succeed");
        assert_eq!(
            cfg.ws_api.address,
            default_local_address(),
            "a grant the operator no longer passes must not keep the socket wide"
        );
        // The list itself still applies to the source filter; only the BIND
        // decision is re-taken from this boot's flags.
        assert_eq!(cfg.ws_api.allowed_source_cidrs.len(), 1);
    }

    /// The exposure decision must survive onto the resolved `Config`, because
    /// that is the only way it reaches a log: `build()` runs before the tracing
    /// subscriber is installed, so anything it emits is dropped.
    #[tokio::test]
    async fn build_records_the_exposure_decision_for_later_reporting() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut args = ws_api_test_args(OperationMode::Network, temp_dir.path());
        args.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        let widened = args
            .build_with_gateways_index(&index_url)
            .await
            .expect("build should succeed");
        assert_eq!(
            widened.ws_api.exposure.source,
            WsApiAddressSource::AutoWidened
        );
        assert_eq!(widened.ws_api.exposure.dropped_persisted_address, None);

        // A fresh dir, so the migration branch is the one under test.
        let upgrade_dir = tempfile::tempdir().unwrap();
        let mut first = ws_api_test_args(OperationMode::Network, upgrade_dir.path());
        first.ws_api.address = Some(default_listening_address());
        first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");
        let migrated = ws_api_test_args(OperationMode::Network, upgrade_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("second build should succeed");
        assert_eq!(
            migrated.ws_api.exposure.dropped_persisted_address,
            Some(default_listening_address()),
            "the operator must be told their LAN clients just lost access"
        );
        assert_eq!(
            migrated.ws_api.exposure.source,
            WsApiAddressSource::DefaultLoopback
        );

        // Boot 3 in the same dir: the operator applies the documented remedy on
        // an ALREADY-MIGRATED node — config.toml holds the post-migration
        // loopback, and the CIDR grant arrives now. This is the sequence the
        // startup message actually points operators at, and it only works
        // because the sentinel treats loopback as auto-derivable: read back as
        // an operator choice it would take the Explicit short-circuit and the
        // remedy would silently do nothing.
        let mut widened_later = ws_api_test_args(OperationMode::Network, upgrade_dir.path());
        widened_later.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        let cfg = widened_later
            .build_with_gateways_index(&index_url)
            .await
            .expect("third build should succeed");
        assert_eq!(cfg.ws_api.address, default_listening_address());
        assert_eq!(cfg.ws_api.exposure.source, WsApiAddressSource::AutoWidened);
        // Recorded, because the bind MOVED — and moved toward exposure. A
        // hardening change that widens a node whose config named loopback owes
        // the operator a loud line about it, which is why the notice is
        // directional rather than suppressed here.
        assert_eq!(
            cfg.ws_api.exposure.dropped_persisted_address,
            Some(default_local_address()),
            "widening past a persisted loopback address must be reported, not silent"
        );
    }

    /// D4 regression: the same remedy sequence, asserted on the BIND rather
    /// than on the exposure record, and starting from a node that is already
    /// migrated. `auto_widen_preserves_the_address_family` starts from a
    /// persisted wildcard, which is the migration boot, not the remedy.
    #[tokio::test]
    async fn remedy_still_works_after_the_upgrade_persisted_loopback() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        // Boot 1: pre-upgrade node with the old wildcard auto-default.
        let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
        first.ws_api.address = Some(default_listening_address());
        first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");

        // Boot 2: upgraded, flag-less. Re-derived to loopback and persisted.
        let migrated = ws_api_test_args(OperationMode::Network, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("second build should succeed");
        assert_eq!(migrated.ws_api.address, default_local_address());
        let persisted =
            std::fs::read_to_string(temp_dir.path().join("config.toml")).expect("config persisted");
        assert!(
            persisted.contains(r#"ws-api-address = "::1""#),
            "test premise: the loopback address must be persisted, got:\n{persisted}"
        );

        // Boot 3: the operator applies the documented remedy.
        let mut remedied = ws_api_test_args(OperationMode::Network, temp_dir.path());
        remedied.ws_api.allowed_source_cidrs = Some(vec!["100.64.0.0/10".to_string()]);
        let cfg = remedied
            .build_with_gateways_index(&index_url)
            .await
            .expect("third build should succeed");
        assert_eq!(
            cfg.ws_api.address,
            default_listening_address(),
            "the auto-widen remedy must still work once the migration has run"
        );
    }

    /// The notice must fire ONCE. Its filter has two clauses, and only one of
    /// them was covered: dropping `!persisted.is_loopback()` while keeping the
    /// other passes every other test, yet makes the notice reappear on every
    /// flagless boot forever — the tuned-out-log failure the migration comment
    /// exists to avoid. Boot three times and assert the third is silent.
    #[tokio::test]
    async fn the_loss_of_access_notice_does_not_repeat_after_the_migration_boot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_server, index_url) = empty_gateways_index_server();

        let mut first = ws_api_test_args(OperationMode::Network, temp_dir.path());
        first.ws_api.address = Some(default_listening_address());
        first
            .build_with_gateways_index(&index_url)
            .await
            .expect("first build should succeed");

        let migrated = ws_api_test_args(OperationMode::Network, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("migration boot should succeed");
        assert_eq!(
            migrated.ws_api.exposure.dropped_persisted_address,
            Some(default_listening_address()),
            "the migration boot must report the loss of access exactly once"
        );

        let steady = ws_api_test_args(OperationMode::Network, temp_dir.path())
            .build_with_gateways_index(&index_url)
            .await
            .expect("third build should succeed");
        assert_eq!(steady.ws_api.address, default_local_address());
        assert_eq!(
            steady.ws_api.exposure.dropped_persisted_address, None,
            "re-announcing an unchanged bind on every boot is how a log line \
             gets tuned out"
        );
    }

    /// Source pin: the resolver and the reporting path must actually be wired
    /// in. A unit-tested pure function that nothing calls is a verification
    /// that cannot fail.
    ///
    /// Both halves matter and they live in different functions: `build()` must
    /// RESOLVE through the helper, and `Config::log_client_api_exposure` must
    /// be the thing that reports it (it cannot be reported from `build()`,
    /// which has no tracing subscriber yet).
    #[test]
    fn the_client_api_exposure_path_stays_wired_end_to_end() {
        let src = production_source();

        let build = extract_fn_body(
            src,
            "async fn build_with_gateways_index(mut self, gateways_index: &str)",
        );
        assert!(
            build.contains("resolve_ws_api_address("),
            "build_with_gateways_index no longer resolves the client-API bind through \
             the shared helper"
        );
        assert!(
            build.contains("exposure: ws_api_exposure"),
            "build_with_gateways_index no longer records the exposure decision, so \
             nothing downstream can report it"
        );
        // `build()` legitimately warns about other things (gateway dedup, source
        // CIDRs), so pin the specific property: it must not CONSULT the exposure
        // predicate, because anything it decided to say about exposure would be
        // emitted before `set_logger` and silently dropped.
        assert!(
            !build.contains("ws_api_shares_one_namespace_with_remote_clients("),
            "the exposure warning must NOT be decided in build(): it runs before \
             set_logger, so the message would be silently dropped"
        );

        let report = extract_fn_body(src, "pub fn log_client_api_exposure(&self)");
        assert!(
            report.contains("ws_api_shares_one_namespace_with_remote_clients("),
            "log_client_api_exposure no longer consults the exposure predicate"
        );
        assert!(
            report.contains("tracing::warn!"),
            "log_client_api_exposure no longer warns about a shared-namespace exposure"
        );

        // A re-derivation that MOVED the bind is reported in whichever direction
        // it moved, and both branches must survive. The record-site filter is
        // pinned above, so re-applying the old "suppress the contradiction" fix
        // there fails — but deleting the widening `else` here achieves the same
        // silence and is invisible to every behavioural test, because nothing
        // asserts on emitted logs. That is the cannot-fail shape this function's
        // own comment block warns about, so pin the selection by its message
        // stems.
        for (stem, direction) in [
            ("can no longer reach this node's API", "narrowing"),
            ("widened the bind", "widening"),
        ] {
            assert!(
                report.contains(stem),
                "log_client_api_exposure no longer reports the {direction} \
                 re-derivation. Both directions must speak: silencing one is how \
                 a node that got MORE exposed stopped saying so."
            );
        }

        // Each renamed environment variable must keep its deprecation notice.
        // Dropping a pair from the array is silent: the operator's old-style
        // variable is then ignored with no explanation, which is exactly the
        // failure the loop exists to prevent.
        for legacy in ["WS_API_ADDRESS", "ALLOWED_HOST", "ALLOWED_SOURCE_CIDRS"] {
            assert!(
                report.contains(&format!("(\"{legacy}\", \"FREENET_{legacy}\")")),
                "log_client_api_exposure no longer reports a leftover `{legacy}`; a \
                 deployment relying on it goes loopback-only with no explanation"
            );
        }

        // The mode-keyed DEFAULT must not come back. Pin it on the resolver's
        // signature rather than on a text search of `build()`: `mode` is a
        // legitimate input (it scopes the compat auto-widen), so what must stay
        // true is that the no-flags branch is mode-independent — which the
        // behavioural test `empty_allow_lists_do_not_widen` asserts for both
        // modes. Here we only pin that the resolver is the single decision site.
        let resolver = extract_fn_body(src, "fn resolve_ws_api_address(");
        assert!(
            resolver.contains("WsApiAddressSource::DefaultLoopback"),
            "resolve_ws_api_address no longer has a loopback default branch"
        );

        // Cross-file: both helpers are called from exactly ONE place each, in a
        // different file, so deleting a call site leaves every test in this
        // module green while silently removing the only operator-facing signal
        // (and, for the merge, the flags that decide the bind). Scraping the
        // binary's source from HERE also avoids the self-match trap that an
        // `include_str!("freenet.rs")` inside that file's own test module would
        // hit, the same reason `production_source()` exists.
        let main_src = include_str!("bin/freenet.rs");
        let main_body = extract_fn_body(main_src, "fn freenet_main() -> anyhow::Result<()>");
        for (needle, why) in [
            (
                "config.log_client_api_exposure()",
                "nothing would report the client API's exposure — build() cannot, it \
                 runs before set_logger",
            ),
            (
                "merge_pre_subcommand_ws_api_args(",
                "flags placed before the subcommand would be silently discarded, \
                 leaving the node loopback-only against the operator's intent",
            ),
        ] {
            assert!(
                main_src.contains(needle),
                "bin/freenet.rs no longer calls `{needle}`: {why}"
            );
        }
        // The merge must run for BOTH subcommands, not just whichever one a
        // refactor happened to keep.
        assert_eq!(
            main_body
                .matches("merge_pre_subcommand_ws_api_args(")
                .count(),
            2,
            "merge_pre_subcommand_ws_api_args must be called from the Network arm \
             AND the Local arm"
        );
    }
}
