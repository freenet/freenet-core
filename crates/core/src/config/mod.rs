use std::{
    collections::HashSet,
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, LazyLock,
    },
    time::Duration,
};

use anyhow::Context;
use directories::ProjectDirs;
use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::{
    dev_tool::PeerId,
    local_node::OperationMode,
    transport::{CongestionControlAlgorithm, CongestionControlConfig, TransportKeypair},
};

mod secret;
pub use secret::*;

/// Default maximum number of connections for the peer.
pub const DEFAULT_MAX_CONNECTIONS: usize = 20;
/// Default minimum number of connections for the peer.
pub const DEFAULT_MIN_CONNECTIONS: usize = 10;
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

pub(crate) const OPERATION_TTL: Duration = Duration::from_secs(60);

/// Current version of the crate.
pub(crate) const PCK_VERSION: &str = env!("CARGO_PKG_VERSION");

// Initialize the executor once.
static ASYNC_RT: LazyLock<Option<Runtime>> = LazyLock::new(GlobalExecutor::initialize_async_rt);

const DEFAULT_TRANSIENT_BUDGET: usize = 2048;
const DEFAULT_TRANSIENT_TTL_SECS: u64 = 30;

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
                location: None,
                bandwidth_limit: Some(3_000_000), // 3 MB/s default for streaming transfers only
                total_bandwidth_limit: None,
                min_bandwidth_per_connection: None,
                blocked_addresses: None,
                transient_budget: Some(DEFAULT_TRANSIENT_BUDGET),
                transient_ttl_secs: Some(DEFAULT_TRANSIENT_TTL_SECS),
                min_connections: None,
                max_connections: None,
                streaming_enabled: None,   // Default: disabled
                streaming_threshold: None, // Default: 64KB (set in NetworkApiConfig)
                ledbat_min_ssthresh: None, // Uses default from NetworkApiConfig
                congestion_control: None,  // Default: fixedrate (set in NetworkApiConfig)
                bbr_startup_rate: None,    // Uses default from BBR config
            },
            ws_api: WebsocketApiArgs {
                address: Some(default_listening_address()),
                ws_api_port: Some(default_http_gateway_port()),
                token_ttl_seconds: None,
                token_cleanup_interval_seconds: None,
            },
            secrets: Default::default(),
            log_level: Some(tracing::log::LevelFilter::Info),
            config_paths: Default::default(),
            id: None,
            version: false,
            max_blocking_threads: None,
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
                            config.secrets.transport_keypair_path,
                            config.secrets.nonce_path,
                            config.secrets.cipher_path,
                        )?;
                        config.secrets = secrets;
                        Ok(Some(config))
                    }
                    "json" => {
                        let mut file = File::open(&path)?;
                        let mut config = serde_json::from_reader::<_, Config>(&mut file)?;
                        let secrets = Self::read_secrets(
                            config.secrets.transport_keypair_path,
                            config.secrets.nonce_path,
                            config.secrets.cipher_path,
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
    pub async fn build(mut self) -> anyhow::Result<Config> {
        // Validate gateway configuration
        self.network_api.validate()?;

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

        let should_persist = cfg.is_none();

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
            if cfg.network_api.streaming_enabled {
                self.network_api.streaming_enabled.get_or_insert(true);
            }
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
            self.log_level.get_or_insert(cfg.log_level);
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
        }

        let mode = self.mode.unwrap_or(OperationMode::Network);
        let config_paths = self.config_paths.build(self.id.as_deref())?;

        let secrets = self.secrets.build()?;

        let peer_id = self
            .network_api
            .public_address
            .zip(self.network_api.public_port)
            .map(|(addr, port)| {
                PeerId::new(
                    (addr, port).into(),
                    secrets.transport_keypair.public().clone(),
                )
            });
        let gateways_file = config_paths.config_dir.join("gateways.toml");

        // In Local mode, skip all gateway loading since we don't connect to external peers
        let remotely_loaded_gateways = if mode == OperationMode::Local {
            Gateways::default()
        } else if !self.network_api.skip_load_from_network {
            load_gateways_from_index(FREENET_GATEWAYS_INDEX, &config_paths.secrets_dir)
                .await
                .inspect_err(|error| {
                    tracing::error!(
                        error = %error,
                        index = FREENET_GATEWAYS_INDEX,
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
                .try_collect()?;
            Gateways { gateways }
        } else {
            Gateways::default()
        };

        // Decide which gateways to use based on whether we fetched from network
        let gateways = if mode == OperationMode::Local {
            // In Local mode, use empty gateways - no external connections
            Gateways { gateways: vec![] }
        } else if !self.network_api.skip_load_from_network
            && !remotely_loaded_gateways.gateways.is_empty()
        {
            // When we successfully fetch gateways from the network, replace local ones entirely
            // This ensures users always use the current active gateways
            // TODO: This behavior will likely change once we release a stable version
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
        } else {
            // When skip_load_from_network is set for a regular peer, use local gateways file
            let mut gateways = match File::open(&*gateways_file) {
                Ok(mut file) => {
                    let mut content = String::new();
                    file.read_to_string(&mut content)?;
                    toml::from_str::<Gateways>(&content).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?
                }
                Err(err) => {
                    if peer_id.is_none()
                        && mode == OperationMode::Network
                        && remotely_loaded_gateways.gateways.is_empty()
                    {
                        tracing::error!(
                            file = ?gateways_file,
                            error = %err,
                            "Failed to read gateways file"
                        );

                        return Err(anyhow::Error::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Cannot initialize node without gateways",
                        )));
                    }
                    if remotely_loaded_gateways.gateways.is_empty() {
                        tracing::warn!("No gateways file found, initializing disjoint gateway");
                    }
                    Gateways { gateways: vec![] }
                }
            };

            // If we have remotely loaded gateways but skip_load_from_network was not set,
            // it means the remote fetch failed but we got default/inline gateways
            if !remotely_loaded_gateways.gateways.is_empty() {
                gateways.merge_and_deduplicate(remotely_loaded_gateways);
            }

            gateways
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
                streaming_enabled: self.network_api.streaming_enabled.unwrap_or(false),
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
            },
            ws_api: WebsocketApiConfig {
                // the websocket API is always local
                address: self.ws_api.address.unwrap_or_else(default_local_address),
                port: self
                    .ws_api
                    .ws_api_port
                    .unwrap_or(default_http_gateway_port()),
                token_ttl_seconds: self
                    .ws_api
                    .token_ttl_seconds
                    .unwrap_or(default_token_ttl_seconds()),
                token_cleanup_interval_seconds: self
                    .ws_api
                    .token_cleanup_interval_seconds
                    .unwrap_or(default_token_cleanup_interval_seconds()),
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
            },
        };

        fs::create_dir_all(this.config_dir())?;
        gateways.save_to_file(&gateways_file)?;

        if should_persist {
            let path = this.config_dir().join("config.toml");
            tracing::info!(path = ?path, "Persisting configuration");
            let mut file = File::create(path)?;
            file.write_all(
                toml::to_string(&this)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                    .as_bytes(),
            )?;
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
    /// Telemetry configuration
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,
}

/// Default max blocking threads: 2x CPU cores, clamped to 4-32.
fn default_max_blocking_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() * 2).clamp(4, 32))
        .unwrap_or(8)
}

impl Config {
    pub fn transport_keypair(&self) -> &TransportKeypair {
        self.secrets.transport_keypair()
    }

    pub(crate) fn paths(&self) -> Arc<ConfigPaths> {
        self.config_paths.clone()
    }
}

#[derive(clap::Parser, Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetworkArgs {
    /// Address to bind to for the network event listener, default is 0.0.0.0
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

    /// Skips loading gateway configurations from the network and merging it with existing one.
    #[arg(long)]
    pub skip_load_from_network: bool,

    /// Optional list of gateways to connect to in network mode. Used for testing purposes.
    #[arg(long, hide = true)]
    pub gateways: Option<Vec<String>>,

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

    /// Enable streaming transport for large transfers (experimental).
    /// When enabled, transfers larger than streaming_threshold use streaming
    /// instead of atomic messages. Default: false
    #[arg(long, env = "STREAMING_ENABLED")]
    #[serde(rename = "streaming-enabled", skip_serializing_if = "Option::is_none")]
    pub streaming_enabled: Option<bool>,

    /// Threshold in bytes above which streaming transport is used.
    /// Only applies when streaming_enabled is true.
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
    /// - `fixedrate` (default): Fixed-rate transmission at 100 Mbps, ignores network feedback
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

    /// Path to the public key of the gateway in PEM format.
    #[serde(rename = "public_key")]
    pub public_key_path: PathBuf,

    /// Optional location of the gateway. Necessary for deterministic testing.
    pub location: Option<f64>,
}

impl NetworkArgs {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.is_gateway {
            // For gateways, require both public address and port
            if self.public_address.is_none() {
                return Err(anyhow::anyhow!(
                    "Gateway nodes must specify a public network address"
                ));
            }
            if self.public_port.is_none() && self.network_port.is_none() {
                return Err(anyhow::anyhow!("Gateway nodes must specify a network port"));
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

    /// Enable streaming transport for large transfers (experimental).
    /// When enabled, transfers larger than `streaming_threshold` use streaming
    /// instead of atomic messages. Default: false
    #[serde(default, rename = "streaming-enabled")]
    pub streaming_enabled: bool,

    /// Threshold in bytes above which streaming transport is used.
    /// Only applies when `streaming_enabled` is true.
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
    /// - `fixedrate` (default): Fixed-rate transmission at 100 Mbps
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
/// Transfers larger than this will use streaming when `streaming_enabled` is true.
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

#[derive(clap::Parser, Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct WebsocketApiArgs {
    /// Address to bind to for the websocket API, default is 0.0.0.0
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
}

impl Default for TelemetryArgs {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: None,
            transport_snapshot_interval_secs: None,
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
}

fn default_transport_snapshot_interval_secs() -> u64 {
    30
}

fn default_telemetry_endpoint() -> String {
    DEFAULT_TELEMETRY_ENDPOINT.to_string()
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: DEFAULT_TELEMETRY_ENDPOINT.to_string(),
            transport_snapshot_interval_secs: default_transport_snapshot_interval_secs(),
            is_test_environment: false,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct WebsocketApiConfig {
    /// Address to bind to
    #[serde(default = "default_listening_address", rename = "ws-api-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[serde(default = "default_http_gateway_port", rename = "ws-api-port")]
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
}

#[inline]
const fn default_token_ttl_seconds() -> u64 {
    86400 // 24 hours
}

#[inline]
const fn default_token_cleanup_interval_seconds() -> u64 {
    300 // 5 minutes
}

impl From<SocketAddr> for WebsocketApiConfig {
    fn from(addr: SocketAddr) -> Self {
        Self {
            address: addr.ip(),
            port: addr.port(),
            token_ttl_seconds: default_token_ttl_seconds(),
            token_cleanup_interval_seconds: default_token_cleanup_interval_seconds(),
        }
    }
}

impl Default for WebsocketApiConfig {
    #[inline]
    fn default() -> Self {
        Self {
            address: default_listening_address(),
            port: default_http_gateway_port(),
            token_ttl_seconds: default_token_ttl_seconds(),
            token_cleanup_interval_seconds: default_token_cleanup_interval_seconds(),
        }
    }
}

#[inline]
const fn default_listening_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

#[inline]
const fn default_local_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

#[inline]
const fn default_http_gateway_port() -> u16 {
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
}

impl ConfigPathsArgs {
    fn merge(&mut self, other: ConfigPaths) {
        self.config_dir.get_or_insert(other.config_dir);
        self.data_dir.get_or_insert(other.data_dir);
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
                let _ = fs::remove_dir_all(&unique_path);
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
        let app_data_dir = self
            .data_dir
            .map(Ok::<_, std::io::Error>)
            .unwrap_or_else(|| {
                let default_dirs = Self::default_dirs(id)?;
                let Either::Left(defaults) = default_dirs else {
                    unreachable!("default_dirs should return Left if data_dir is None and id is not set for temp dir")
                };
                Ok(defaults.data_dir().to_path_buf())
            })?;
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

        Ok(ConfigPaths {
            config_dir,
            data_dir: app_data_dir,
            contracts_dir,
            delegates_dir,
            secrets_dir,
            db_dir,
            event_log,
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
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Gateways {
    pub gateways: Vec<GatewayConfig>,
}

impl Gateways {
    pub fn merge_and_deduplicate(&mut self, other: Gateways) {
        let mut existing_gateways: HashSet<_> = self.gateways.drain(..).collect();
        for gateway in other.gateways {
            existing_gateways.insert(gateway);
        }
        self.gateways = existing_gateways.into_iter().collect();
    }

    pub fn save_to_file(&self, path: &Path) -> anyhow::Result<()> {
        let content = toml::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GatewayConfig {
    /// Address of the gateway. It can be either a hostname or an IP address and port.
    pub address: Address,

    /// Path to the public key of the gateway in PEM format.
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub enum Address {
    #[serde(rename = "hostname")]
    Hostname(String),
    #[serde(rename = "host_address")]
    HostAddress(SocketAddr),
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

use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};

/// Global seed for deterministic simulation.
/// Set this before any RNG operations to ensure reproducibility.
static SIMULATION_SEED: LazyLock<Mutex<Option<u64>>> = LazyLock::new(|| Mutex::new(None));

/// Counter for deterministic thread indexing.
/// Each thread gets a unique, deterministic index for RNG seeding.
static THREAD_INDEX_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// Thread-local seeded RNG for deterministic operations.
// Each thread gets its own RNG seeded from the global seed + deterministic thread index.
std::thread_local! {
    static THREAD_RNG: std::cell::RefCell<Option<SmallRng>> = const { std::cell::RefCell::new(None) };
    // Deterministic thread index assigned at first RNG access
    static THREAD_INDEX: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
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
    /// Sets the global seed for deterministic RNG.
    ///
    /// **Warning:** For test isolation, prefer `scoped_seed()` or `seed_guard()`
    /// which automatically clean up the seed state.
    ///
    /// Call this at test/simulation startup for reproducibility.
    /// Must call `clear_seed()` when done to avoid affecting other tests.
    pub fn set_seed(seed: u64) {
        // Reset thread index counter first so threads get fresh indices
        Self::reset_thread_index_counter();
        *SIMULATION_SEED.lock() = Some(seed);
        // Clear thread-local RNG and index so it gets re-seeded with fresh index
        THREAD_RNG.with(|rng| {
            *rng.borrow_mut() = None;
        });
        THREAD_INDEX.with(|idx| {
            idx.set(None);
        });
    }

    /// Clears the simulation seed, reverting to system RNG.
    pub fn clear_seed() {
        *SIMULATION_SEED.lock() = None;
        THREAD_RNG.with(|rng| {
            *rng.borrow_mut() = None;
        });
        THREAD_INDEX.with(|idx| {
            idx.set(None);
        });
        // Reset thread index counter for next simulation
        Self::reset_thread_index_counter();
    }

    /// Resets the thread index counter for deterministic simulation.
    /// This should be called between simulation runs to ensure reproducibility.
    pub fn reset_thread_index_counter() {
        THREAD_INDEX_COUNTER.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    /// Returns true if a simulation seed is set.
    pub fn is_seeded() -> bool {
        SIMULATION_SEED.lock().is_some()
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

    /// Executes a closure with access to the global RNG.
    /// Uses seeded RNG if set, otherwise system RNG.
    #[inline]
    pub fn with_rng<F, R>(f: F) -> R
    where
        F: FnOnce(&mut dyn RngCore) -> R,
    {
        if let Some(seed) = *SIMULATION_SEED.lock() {
            // Simulation mode: use thread-local seeded RNG
            THREAD_RNG.with(|rng_cell| {
                let mut rng_ref = rng_cell.borrow_mut();
                if rng_ref.is_none() {
                    // Use deterministic thread index for reproducibility
                    // Thread IDs are not stable across runs, so we use a counter
                    let thread_index = THREAD_INDEX.with(|idx| {
                        if let Some(i) = idx.get() {
                            i
                        } else {
                            let new_idx = THREAD_INDEX_COUNTER
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            idx.set(Some(new_idx));
                            new_idx
                        }
                    });
                    let thread_seed =
                        seed.wrapping_add(thread_index.wrapping_mul(0x9E3779B97F4A7C15));
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

/// Global simulation time base in milliseconds since Unix epoch.
/// When set, ULID generation uses this instead of system time.
/// Combined with GlobalRng seeding, this allows fully deterministic transaction IDs.
static SIMULATION_TIME_MS: LazyLock<Mutex<Option<u64>>> = LazyLock::new(|| Mutex::new(None));

/// Counter for deterministic time progression within a single millisecond.
/// Increments with each ULID generation to ensure uniqueness.
static SIMULATION_TIME_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    /// Sets the global simulation time base in milliseconds since Unix epoch.
    ///
    /// All subsequent ULID generations will use this time (with auto-increment).
    pub fn set_time_ms(time_ms: u64) {
        *SIMULATION_TIME_MS.lock() = Some(time_ms);
        SIMULATION_TIME_COUNTER.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    /// Clears the simulation time, reverting to system time.
    pub fn clear_time() {
        *SIMULATION_TIME_MS.lock() = None;
        SIMULATION_TIME_COUNTER.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    /// Returns the current time in milliseconds for ULID generation.
    ///
    /// If simulation time is set, returns simulation time + counter increment.
    /// Otherwise, returns real system time.
    pub fn current_time_ms() -> u64 {
        if let Some(base_time) = *SIMULATION_TIME_MS.lock() {
            // Deterministic time: base + counter for uniqueness
            let counter = SIMULATION_TIME_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            base_time.saturating_add(counter)
        } else {
            // Production: use real system time
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_millis() as u64
        }
    }

    /// Returns the current time in milliseconds WITHOUT incrementing the counter.
    ///
    /// Use this for read-only time checks like elapsed time calculations.
    /// For ULID generation, use `current_time_ms()` which ensures uniqueness.
    pub fn read_time_ms() -> u64 {
        if let Some(base_time) = *SIMULATION_TIME_MS.lock() {
            // Return base time + current counter (without incrementing)
            let counter = SIMULATION_TIME_COUNTER.load(std::sync::atomic::Ordering::SeqCst);
            base_time.saturating_add(counter)
        } else {
            // Production: use real system time
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_millis() as u64
        }
    }

    /// Returns true if simulation time is set.
    pub fn is_simulation_time() -> bool {
        SIMULATION_TIME_MS.lock().is_some()
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

pub fn set_logger(level: Option<tracing::level_filters::LevelFilter>, endpoint: Option<String>) {
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

        crate::tracing::tracer::init_tracer(level, endpoint).expect("failed tracing initialization")
    }
}

async fn load_gateways_from_index(url: &str, pub_keys_dir: &Path) -> anyhow::Result<Gateways> {
    let response = reqwest::get(url).await?.error_for_status()?.text().await?;
    let mut gateways: Gateways = toml::from_str(&response)?;
    let mut base_url = reqwest::Url::parse(url)?;
    base_url.set_path("");
    let mut valid_gateways = Vec::new();

    for gateway in &mut gateways.gateways {
        gateway.location = None; // always ignore any location from files if set, it should be derived from IP
        let public_key_url = base_url.join(&gateway.public_key_path.to_string_lossy())?;
        let public_key_response = reqwest::get(public_key_url).await?.error_for_status()?;
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
    use httptest::{matchers::*, responders::*, Expectation, Server};

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
            },
            ..Default::default()
        };
        let cfg = args.build().await.unwrap();
        let serialized = toml::to_string(&cfg).unwrap();
        let _: Config = toml::from_str(&serialized).unwrap();
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
        // Verify streaming is disabled by default when deserializing empty config
        // This tests the serde defaults which mirror the runtime defaults
        let minimal_config = r#"
            network-address = "127.0.0.1"
            network-port = 8080
        "#;
        let network_api: NetworkApiConfig = toml::from_str(minimal_config).unwrap();
        assert!(
            !network_api.streaming_enabled,
            "Streaming should be disabled by default"
        );
        assert_eq!(
            network_api.streaming_threshold,
            64 * 1024,
            "Default streaming threshold should be 64KB"
        );
    }

    #[test]
    fn test_streaming_config_serde() {
        // Test serialization/deserialization of streaming config with explicit values
        let config_str = r#"
            network-address = "127.0.0.1"
            network-port = 8080
            streaming-enabled = true
            streaming-threshold = 131072
        "#;

        let config: NetworkApiConfig = toml::from_str(config_str).unwrap();
        assert!(config.streaming_enabled);
        assert_eq!(config.streaming_threshold, 128 * 1024);

        // Round-trip test
        let serialized = toml::to_string(&config).unwrap();
        assert!(serialized.contains("streaming-enabled = true"));
        assert!(serialized.contains("streaming-threshold = 131072"));
    }

    #[test]
    fn test_network_args_streaming_defaults() {
        // Verify NetworkArgs streaming fields are None by default (disabled)
        let args = NetworkArgs::default();
        assert!(
            args.streaming_enabled.is_none(),
            "NetworkArgs.streaming_enabled should be None by default"
        );
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
}
