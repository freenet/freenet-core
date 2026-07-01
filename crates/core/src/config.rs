use std::{
    collections::HashSet,
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    net::{IpAddr, Ipv6Addr, SocketAddr},
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

    /// Budget in bytes for hosted contract *state*. Once exceeded, contracts
    /// are evicted (least-valuable-first) and their on-disk state reclaimed.
    /// This bounds tracked contract state only — WASM code blobs and ReDb/
    /// SQLite database overhead are additional and not counted against it.
    /// Default: 1 GiB.
    #[arg(long, env = "MAX_HOSTING_STORAGE")]
    pub max_hosting_storage: Option<u64>,

    /// Per-user secret-storage quota in bytes for HOSTED mode (#4561, P5 of
    /// #4381). Bounds a single hosted user's (one `userToken`) TOTAL on-disk
    /// footprint under their `users/<user_id>/` tree, summed across every
    /// delegate — both the active secret-value blobs AND the `.keys`
    /// enumeration registry (so many/large keys are charged too) — so a visitor
    /// cannot fill the node's disk. Per-user secret-value snapshots are disabled
    /// (hosted users are transient and don't need overwrite history), so there
    /// is no `.snapshots/` growth to charge. REJECT-on-full (never evict —
    /// secrets are authoritative identity/room keys, not a cache). Default:
    /// 4 MiB. `0` disables enforcement. Has NO effect outside hosted mode —
    /// local single-user secrets are never quota-checked (and keep snapshots).
    #[arg(long = "per-user-secret-quota", env = "PER_USER_SECRET_QUOTA")]
    pub per_user_secret_quota_bytes: Option<u64>,

    /// Inactivity TTL, in seconds, after which a HOSTED user's entire
    /// per-user data is reclaimed by a background sweep (#4561, P5 of #4381).
    /// Keeps a public "try Freenet" node a transient demo with bounded storage:
    /// a visitor who walks away has their namespace reclaimed after this many
    /// real-calendar seconds of inactivity (durable across restarts). Default:
    /// 2_592_000 (30 days). `0` disables the sweep entirely. Has NO effect
    /// outside hosted mode — Local single-user data is never enumerated or
    /// reclaimed (it lives outside the `users/<id>/` tree the sweep touches).
    #[arg(long = "per-user-inactive-ttl", env = "PER_USER_INACTIVE_TTL")]
    pub per_user_inactive_ttl_secs: Option<u64>,

    /// How often, in seconds, the inactive-user reclaim sweep runs (#4561).
    /// Only relevant when hosted mode is on and `per-user-inactive-ttl` is
    /// non-zero. Default: 3_600 (hourly) — far finer than the 30-day TTL, so
    /// reclamation lag is negligible while keeping the sweep's disk-walk cost
    /// trivial. Must be > 0; `0` is treated as the default.
    #[arg(
        long = "inactive-user-sweep-interval",
        env = "INACTIVE_USER_SWEEP_INTERVAL"
    )]
    pub inactive_user_sweep_interval_secs: Option<u64>,

    /// Byte budget for the compiled-WASM **contract** module cache. The
    /// **delegate** cache gets a fraction of this value
    /// (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`, currently 1/4), so the combined
    /// ceiling is ~1.25× this. When a cache's tracked compiled-byte total would
    /// exceed its budget on insert, least-recently-used modules are evicted
    /// until it fits. Bounding by bytes (not entry count) stops a node hosting
    /// many contracts from thrashing the cache and recompiling on every access
    /// (issue #4441). When unset, the default scales with system RAM
    /// (`clamp(total_ram / 8, 64 MiB, 1.5 GiB)`); set this to override.
    #[arg(long, env = "FREENET_MODULE_CACHE_BUDGET_BYTES")]
    pub module_cache_budget_bytes: Option<usize>,

    /// Seconds to wait on graceful shutdown for in-flight client
    /// PUT/GET/UPDATE/SUBSCRIBE operations to finish before tearing
    /// down peer connections. Set to 0 to disable. Default: 30s. See
    /// `Config::shutdown_drain_secs` for the full rationale.
    #[arg(long, env = "SHUTDOWN_DRAIN_SECS")]
    pub shutdown_drain_secs: Option<u64>,

    #[command(flatten)]
    pub telemetry: TelemetryArgs,
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
                address: Some(default_listening_address()),
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
            per_user_secret_quota_bytes: None,
            per_user_inactive_ttl_secs: None,
            inactive_user_sweep_interval_secs: None,
            module_cache_budget_bytes: None,
            shutdown_drain_secs: None,
            telemetry: Default::default(),
        }
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
                        let mut config = toml::from_str::<Config>(&content).map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                        })?;
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
                        let mut config = serde_json::from_reader::<_, Config>(&mut file)?;
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

        // merge the configuration from the file with the command line arguments
        if let Some(cfg) = cfg {
            self.secrets.merge(cfg.secrets);
            self.mode.get_or_insert(cfg.mode);
            self.ws_api.address.get_or_insert(cfg.ws_api.address);
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
            self.per_user_secret_quota_bytes
                .get_or_insert(cfg.per_user_secret_quota_bytes);
            self.per_user_inactive_ttl_secs
                .get_or_insert(cfg.per_user_inactive_ttl_secs);
            self.inactive_user_sweep_interval_secs
                .get_or_insert(cfg.inactive_user_sweep_interval_secs);
            self.module_cache_budget_bytes
                .get_or_insert(cfg.module_cache_budget_bytes);
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
                if let Ok(local_cache) = toml::from_str::<Gateways>(&content) {
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
                    toml::from_str::<Gateways>(&content).map_err(|e| {
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
                address: {
                    self.ws_api.address.unwrap_or_else(|| match mode {
                        OperationMode::Local => default_local_address(),
                        OperationMode::Network => default_listening_address(),
                    })
                },
                port: self.ws_api.ws_api_port.unwrap_or(default_ws_api_port()),
                token_ttl_seconds: self
                    .ws_api
                    .token_ttl_seconds
                    .unwrap_or(default_token_ttl_seconds()),
                token_cleanup_interval_seconds: self
                    .ws_api
                    .token_cleanup_interval_seconds
                    .unwrap_or(default_token_cleanup_interval_seconds()),
                allowed_hosts: self.ws_api.allowed_host.unwrap_or_default(),
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
                hosted_mode: self.ws_api.hosted_mode.unwrap_or(false),
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
            max_hosting_storage: self
                .max_hosting_storage
                .unwrap_or_else(crate::ring::default_hosting_budget_bytes),
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
            let mut file = File::create(&config_path)?;
            file.write_all(new_config_toml.as_bytes())?;
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
    #[serde(with = "serde_log_level_filter")]
    pub log_level: tracing::log::LevelFilter,
    #[serde(flatten)]
    config_paths: Arc<ConfigPaths>,
    #[serde(skip)]
    pub(crate) peer_id: Option<PeerId>,
    #[serde(skip)]
    pub(crate) gateways: Vec<GatewayConfig>,
    pub(crate) is_gateway: bool,
    pub(crate) location: Option<f64>,
    /// Maximum number of threads for blocking operations (WASM execution, etc.).
    #[serde(default = "default_max_blocking_threads")]
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
    /// (`clamp(total_ram / 8, 64 MiB, 1.5 GiB)`) so a small VPS doesn't OOM and
    /// a big gateway still caches a large working set.
    #[serde(
        default = "default_module_cache_budget_bytes",
        rename = "module-cache-budget-bytes"
    )]
    pub module_cache_budget_bytes: usize,
    /// Telemetry configuration
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,
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
}

/// Default graceful-shutdown drain window.
fn default_shutdown_drain_secs() -> u64 {
    30
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
/// (`clamp(total_ram / 8, 64 MiB, 1.5 GiB)`).
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

    /// Skip fetching the remote gateway index. The on-disk gateways.toml
    /// cache is also skipped in two cases: (1) the node is a gateway
    /// (--is-gateway), which always runs isolated under this flag (any
    /// --gateways JSON entries are still honored); (2) an explicit
    /// --gateway CLI entry is supplied, in which case the CLI entries
    /// (plus any --gateways JSON entries) REPLACE the on-disk cache.
    /// Otherwise — non-gateway peer with no --gateway CLI entry — the
    /// on-disk gateways.toml is still read (and merged with any
    /// --gateways JSON), preserving the contract used by test harnesses
    /// (e.g. freenet-test-network) that pre-populate it via --config-dir.
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

    /// Bandwidth limit for large streaming data transfers (in bytes per second).
    /// NOTE: This only applies to the send_stream mechanism for large data transfers.
    /// The general packet rate limiter is currently disabled due to reliability issues.
    /// Default: 3 MB/s (3,000,000 bytes/second)
    #[arg(long)]
    pub bandwidth_limit: Option<usize>,

    /// Total bandwidth limit across ALL connections (in bytes per second).
    /// When set, individual connection rates are computed as: total / active_connections.
    /// This overrides the per-connection bandwidth_limit.
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
        skip_serializing_if = "Option::is_none"
    )]
    pub public_address: Option<IpAddr>,

    /// Public external port for the network, mandatory for gateways.
    #[serde(rename = "public_port", skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_limit: Option<usize>,

    /// Total bandwidth limit across ALL connections (in bytes per second).
    /// When set, individual connection rates are computed as: `total / active_connections`.
    /// This overrides the per-connection `bandwidth_limit`.
    ///
    /// Example: With 50 MB/s total and 5 connections, each gets 10 MB/s.
    /// Default: None (use per-connection `bandwidth_limit` instead)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bandwidth_limit: Option<usize>,

    /// Minimum bandwidth per connection when using `total_bandwidth_limit` (bytes/sec).
    /// Prevents connection starvation when many connections are active.
    ///
    /// If `total / N < min`, each connection gets `min` (exceeding total is possible).
    /// Default: 1 MB/s (1,000,000 bytes/second)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_bandwidth_per_connection: Option<usize>,

    /// List of IP:port addresses to refuse connections to/from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_addresses: Option<HashSet<SocketAddr>>,

    /// Capacity for the event loop notification and op execution channels.
    /// Default: 2048. Increase under sustained multi-client load to reduce
    /// channel saturation and associated context-switch spikes.
    #[serde(default = "default_event_loop_channel_capacity")]
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
    #[serde(default)]
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
    /// Address to bind to for the websocket API, default is :: (dual-stack)
    #[arg(
        name = "ws_api_address",
        long = "ws-api-address",
        env = "WS_API_ADDRESS"
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
    /// endpoints `/permission/pending`, `/permission/events`, and
    /// `/permission/{nonce}/respond`).
    /// Use when accessing the node via a custom domain (e.g., through a reverse proxy).
    /// Can be specified multiple times. If omitted, only the machine's hostname and
    /// bound IP are accepted.
    #[arg(long, env = "ALLOWED_HOST")]
    #[serde(rename = "allowed-host", skip_serializing_if = "Option::is_none")]
    pub allowed_host: Option<Vec<String>>,

    /// Additional source IP ranges (CIDR notation) permitted to reach the
    /// local HTTP/WebSocket API.
    ///
    /// By default, only loopback and RFC1918 / IPv6 ULA ranges are accepted.
    /// Use this to grant access from VPN overlays you control (e.g. Tailscale:
    /// `--allowed-source-cidrs 100.64.0.0/10`). Can be specified multiple times.
    ///
    /// SECURITY: Only add ranges you fully control. CGNAT space like
    /// `100.64.0.0/10` is shared between subscribers of some ISPs (Starlink,
    /// T-Mobile, many cable carriers) and is only safe on an overlay network
    /// such as Tailscale or WireGuard. Anything that can reach the API port
    /// can access your contract state, keys, and client API.
    #[arg(
        long = "allowed-source-cidrs",
        env = "ALLOWED_SOURCE_CIDRS",
        value_delimiter = ','
    )]
    #[serde(
        rename = "allowed-source-cidrs",
        skip_serializing_if = "Option::is_none"
    )]
    pub allowed_source_cidrs: Option<Vec<String>>,

    /// Opt-in hosted mode (P2 of #4381): honor a per-connection durable user
    /// token (the `userToken` query parameter on the WebSocket upgrade) and
    /// give that connection its own per-user delegate-secret namespace.
    ///
    /// OFF by default. When off, `userToken` is ignored and every connection is
    /// single-user, byte-for-byte today's behavior. Enable only on a node you
    /// intend to operate as a shared public proxy for untrusted users.
    ///
    /// SECURE-CONNECTION REQUIREMENT (refuse-plaintext-token, #4381): even with
    /// hosted mode on, the durable `userToken` is honored ONLY over a **loopback**
    /// connection carrying `X-Forwarded-Proto: https` — i.e. behind a
    /// TLS-terminating reverse proxy colocated on the same host. The loopback
    /// source proves the proxy→node hop is local; the `https` XFP is positive
    /// evidence (set by the TLS terminator) that the browser→proxy hop used TLS.
    ///
    /// Everything else is **rejected** with `403` (fail-closed): a non-loopback
    /// source, OR a loopback source without `X-Forwarded-Proto: https` (header
    /// missing or `http`). A direct plaintext connection — even loopback — is
    /// refused. `Host` is deliberately NOT consulted: it is proxy-rewritable
    /// (nginx's default rewrites it to the upstream `127.0.0.1:7509`), so it
    /// cannot grant trust; only the `https` XFP can.
    ///
    /// OPERATOR NOTE (REQUIRED proxy config): front the node with a
    /// TLS-terminating reverse proxy on the SAME host that connects over
    /// loopback. The proxy MUST (a) SET / OVERWRITE `X-Forwarded-Proto` itself to
    /// the real browser→proxy scheme, AND (b) STRIP any client-supplied
    /// `X-Forwarded-*` headers, so a client cannot forge the TLS attestation.
    /// Caddy does both by default. nginx requires
    /// `proxy_set_header X-Forwarded-Proto $scheme;` (a literal `https` is fine
    /// for an HTTPS-only server block) and must NOT pass through a client-supplied
    /// `X-Forwarded-Proto` — nginx forwards unknown client headers by default, so
    /// the explicit `proxy_set_header` overwrite is what stops pass-through.
    ///
    /// SECURITY NOTE (known limitation): the node trusts `X-Forwarded-Proto` from
    /// a loopback source and cannot tell a header the proxy SET from one it merely
    /// PASSED THROUGH from the client. If the proxy is misconfigured to forward a
    /// client-supplied `X-Forwarded-Proto: https` over a plaintext listener, a
    /// client could spoof it and the token would be honored over cleartext. The
    /// node cannot detect this pass-through misconfiguration; correct proxy
    /// configuration is the operator's responsibility.
    ///
    /// A developer testing hosted mode locally must likewise front it with a TLS
    /// proxy or send the header (`curl -H 'X-Forwarded-Proto: https'` from
    /// loopback) — a plain plaintext loopback request is refused. A TLS terminator
    /// on a **different** host (remote load balancer) is not supported today (its
    /// source is not loopback) and would need future explicit trusted-proxy-IP
    /// config.
    ///
    /// `--hosted-mode` is THE operator switch, so it works as a BARE flag:
    /// `--hosted-mode` => `Some(true)`; `--hosted-mode=false` (or
    /// `--hosted-mode false`) => `Some(false)`; absent => `None`. Kept as
    /// `Option<bool>` (not a plain `bool` with `default_value`) so config-file /
    /// env layering can still leave it unset (`None`) and the CLI only overrides
    /// when actually present — `None` is then resolved to `false` in `build`.
    #[arg(
        long = "hosted-mode",
        env = "FREENET_HOSTED_MODE",
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    #[serde(rename = "hosted-mode", skip_serializing_if = "Option::is_none")]
    pub hosted_mode: Option<bool>,

    /// Sustained per-user operation rate limit (requests/second) for HOSTED
    /// mode (#4561, P5 of #4381). Bounds how fast a single hosted user (one
    /// `userToken`) can issue contract operations (GET/PUT/UPDATE/SUBSCRIBE) so
    /// one visitor cannot flood the node's executor and network. Over-rate
    /// requests are REJECTED at the WebSocket boundary (the client retries).
    /// Default: 10 req/sec. `0` disables operation rate limiting. Has NO effect
    /// outside hosted mode — local single-user requests are never rate-limited.
    #[arg(long = "per-user-op-rate-limit", env = "PER_USER_OP_RATE_LIMIT")]
    pub per_user_op_rate_limit: Option<u64>,

    /// Per-user operation burst capacity for HOSTED mode (#4561). The maximum
    /// number of operations a user who has been idle can issue back-to-back
    /// before being throttled to the sustained `--per-user-op-rate-limit`.
    /// Default: 100. Paired with the rate limit above; only meaningful when
    /// op rate limiting is enabled.
    #[arg(long = "per-user-op-burst", env = "PER_USER_OP_BURST")]
    pub per_user_op_burst: Option<u64>,

    /// Minimum seconds between hosted-export downloads PER USER (#4561). The
    /// export endpoint enumerates and re-encrypts every secret in the user's
    /// scope, so it is far more expensive than a single op and gets a separate,
    /// tighter limit. A request inside this window returns HTTP 429. Default:
    /// 10s. `0` disables export rate limiting. Hosted-mode only.
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
    /// Enable telemetry reporting to help improve Freenet (default: true during alpha).
    /// Telemetry includes operation timing and network topology data, but never contract content.
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

    /// Enable the Phase 1.5 reference-ping shadow probe (#4074): a 1Hz
    /// UDP DNS query to a fixed external target (default 1.1.1.1:53)
    /// whose RTT is recorded alongside the per-peer overlay RTT so the
    /// collector can disentangle overlay queueing from local uplink
    /// contention. Opt-in: defaults to false. Production gateway
    /// configs set this to true; developer machines and integration
    /// tests leave it off so they don't fire DNS traffic from CI.
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

    /// Enable the Phase 1.6 OS-interface-tx shadow probe (#4074): a 1Hz
    /// read of `/proc/net/dev` (Linux) that emits aggregate interface tx
    /// bytes and the derived `op = total - freenet_own` so the floor
    /// analysis can attribute uplink saturation to Freenet vs the
    /// operator's other traffic. Best-effort and opt-in: defaults to
    /// false; production gateway configs set this to true. Like
    /// reference-ping, it stays off on developer machines and in tests.
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
    #[serde(default = "default_listening_address", rename = "ws-api-address")]
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
        }
    }
}

impl Default for WebsocketApiConfig {
    #[inline]
    fn default() -> Self {
        Self {
            address: default_listening_address(),
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
            event_log,
            log_dir,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPaths {
    contracts_dir: PathBuf,
    delegates_dir: PathBuf,
    secrets_dir: PathBuf,
    db_dir: PathBuf,
    event_log: PathBuf,
    data_dir: PathBuf,
    config_dir: PathBuf,
    #[serde(default = "get_log_dir")]
    log_dir: Option<PathBuf>,
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
            _ => panic!("invalid path index"),
        }
    }

    const MAX_PATH_INDEX: usize = 6;
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

    pub fn delegates_dir(&self) -> PathBuf {
        self.config_paths.delegates_dir(self.mode)
    }

    pub fn secrets_dir(&self) -> PathBuf {
        self.config_paths.secrets_dir(self.mode)
    }

    pub fn event_log(&self) -> PathBuf {
        self.config_paths.event_log(self.mode)
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
    #[serde(rename = "public_key")]
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
    /// When not in simulation mode, uses regular `Ulid::new()`.
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
            Ulid::new()
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
    static GLOBAL_DELTA_SENDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_FULL_STATE_SENDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_INSERTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_REMOVES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_PENDING_OP_HWM: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_NEIGHBOR_HOSTING_UPDATES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_ANTI_STARVATION_TRIGGERS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // Terminal advertisement consult (hosting redesign piece C, invariant 5).
    static GLOBAL_TERMINAL_CONSULT_ATTEMPTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_TERMINAL_CONSULT_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_TERMINAL_CONSULT_RESOLVED_FOUND: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GLOBAL_TERMINAL_CONSULT_STILL_NOT_FOUND: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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
        GLOBAL_DELTA_SENDS.with(|c| c.set(0));
        GLOBAL_FULL_STATE_SENDS.with(|c| c.set(0));
        GLOBAL_PENDING_OP_INSERTS.with(|c| c.set(0));
        GLOBAL_PENDING_OP_REMOVES.with(|c| c.set(0));
        GLOBAL_PENDING_OP_HWM.with(|c| c.set(0));
        GLOBAL_NEIGHBOR_HOSTING_UPDATES.with(|c| c.set(0));
        GLOBAL_ANTI_STARVATION_TRIGGERS.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_ATTEMPTS.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_HITS.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_RESOLVED_FOUND.with(|c| c.set(0));
        GLOBAL_TERMINAL_CONSULT_STILL_NOT_FOUND.with(|c| c.set(0));
    }

    /// Records that a ResyncRequest was received.
    /// Called from production code when handling ResyncRequest messages.
    pub fn record_resync_request() {
        GLOBAL_RESYNC_REQUESTS.with(|c| c.set(c.get() + 1));
    }

    /// Returns the total number of ResyncRequests received since last reset.
    pub fn resync_requests() -> u64 {
        GLOBAL_RESYNC_REQUESTS.with(|c| c.get())
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
    let mut gateways: Gateways = toml::from_str(&response)?;
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

#[cfg(test)]
mod tests {
    use httptest::{Expectation, Server, matchers::*, responders::*};

    use crate::node::NodeConfig;
    use crate::transport::TransportKeypair;

    use super::*;

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
            per_user_secret_quota_bytes: None,
            per_user_inactive_ttl_secs: None,
            inactive_user_sweep_interval_secs: None,
            module_cache_budget_bytes: None,
            shutdown_drain_secs: None,
            telemetry: Default::default(),
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

        let seed = Config {
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
            per_user_secret_quota_bytes: 7_654_321,
            per_user_inactive_ttl_secs: 1_234_567,
            inactive_user_sweep_interval_secs: 7_200,
            module_cache_budget_bytes: 987_654_321,
            telemetry: TelemetryConfig {
                enabled: false,
                endpoint: "http://example.invalid:4318".to_string(),
                transport_snapshot_interval_secs: 45,
                is_test_environment: false, // #[serde(skip)] — derived from --id
                reference_ping_enabled: true,
                iface_tx_enabled: true,
            },
            shutdown_drain_secs: 77,
        };

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
            per_user_secret_quota_bytes,
            per_user_inactive_ttl_secs,
            inactive_user_sweep_interval_secs,
            module_cache_budget_bytes,
            telemetry,
            shutdown_drain_secs,
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
            shutdown_drain_secs, seed.shutdown_drain_secs,
            "shutdown_drain_secs"
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
}
