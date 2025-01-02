use std::{
    collections::HashSet,
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use anyhow::Context;
use directories::ProjectDirs;
use either::Either;
use once_cell::sync::Lazy;
use pkcs8::DecodePublicKey;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::{dev_tool::PeerId, local_node::OperationMode, transport::TransportKeypair};

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

// Initialize the executor once.
static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(GlobalExecutor::initialize_async_rt);

const QUALIFIER: &str = "";
const ORGANIZATION: &str = "The Freenet Project Inc";
const APPLICATION: &str = "Freenet";

const FREENET_GATEWAYS_INDEX: &str = "https://freenet.org/keys/gateways.toml";

#[derive(clap::Parser, Debug)]
pub struct ConfigArgs {
    /// Node operation mode. Default is network mode.
    #[clap(value_enum, env = "MODE")]
    pub mode: Option<OperationMode>,

    #[clap(flatten)]
    pub ws_api: WebsocketApiArgs,

    #[clap(flatten)]
    pub network_api: NetworkArgs,

    #[clap(flatten)]
    pub secrets: SecretArgs,

    #[clap(long, env = "LOG_LEVEL")]
    pub log_level: Option<tracing::log::LevelFilter>,

    #[clap(flatten)]
    config_paths: ConfigPathsArgs,

    /// An arbitrary identifier for the node, mostly for debugging or testing purposes.
    #[clap(long)]
    pub id: Option<String>,
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
            },
            ws_api: WebsocketApiArgs {
                address: Some(default_listening_address()),
                ws_api_port: Some(default_http_gateway_port()),
            },
            secrets: Default::default(),
            log_level: Some(tracing::log::LevelFilter::Info),
            config_paths: Default::default(),
            id: None,
        }
    }
}

impl ConfigArgs {
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
                                tracing::info!("Found configuration file: {filename}.{ext}");
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
                tracing::info!("Reading configuration file: {path:?}",);
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
                        format!("Invalid configuration file extension: {}", ext),
                    )),
                }
            }
            None => Ok(None),
        }
    }

    /// Parse the command line arguments and return the configuration.
    pub async fn build(mut self) -> anyhow::Result<Config> {
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
            let (config, data) = {
                match ConfigPathsArgs::default_dirs(self.id.as_deref())? {
                    Either::Left(defaults) => (
                        defaults.config_local_dir().to_path_buf(),
                        defaults.data_local_dir().to_path_buf(),
                    ),
                    Either::Right(dir) => (dir.clone(), dir),
                }
            };
            self.config_paths.config_dir = Some(config.clone());
            if self.config_paths.data_dir.is_none() {
                self.config_paths.data_dir = Some(data);
            }
            Self::read_config(&config)?.inspect(|_| {
                tracing::info!("Found configuration file in default directory");
            })
        };

        let should_persist = cfg.is_none();

        // merge the configuration from the file with the command line arguments
        if let Some(cfg) = cfg {
            self.secrets.merge(cfg.secrets);
            self.mode.get_or_insert(cfg.mode);
            self.ws_api.address.get_or_insert(cfg.ws_api.address);
            self.ws_api.ws_api_port.get_or_insert(cfg.ws_api.port);
            self.log_level.get_or_insert(cfg.log_level);
            self.config_paths.merge(cfg.config_paths.as_ref().clone());
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

        let remotely_loaded_gateways = if !self.network_api.skip_load_from_network {
            load_gateways_from_index(FREENET_GATEWAYS_INDEX, &config_paths.secrets_dir)
                .await
                .inspect_err(|error| {
                    tracing::error!(
                        "Failed to load gateways from index (at {FREENET_GATEWAYS_INDEX}): {error}"
                    );
                })
                .unwrap_or_default()
        } else {
            Gateways::default()
        };
        let mut gateways = match File::open(&*gateways_file) {
            Ok(mut file) => {
                let mut content = String::new();
                file.read_to_string(&mut content)?;
                toml::from_str::<Gateways>(&content).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?
            }
            Err(err) => {
                // TODO: remove local-simulation feature and use runtime flags
                #[cfg(all(not(any(test, debug_assertions)), not(feature = "local-simulation")))]
                {
                    if peer_id.is_none()
                        && mode == OperationMode::Network
                        && remotely_loaded_gateways.gateways.is_empty()
                    {
                        tracing::error!(file = ?gateways_file, "Failed to read gateways file: {err}");

                        return Err(anyhow::Error::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Cannot initialize node without gateways",
                        )));
                    }
                }
                let _ = err;
                if remotely_loaded_gateways.gateways.is_empty() {
                    tracing::warn!("No gateways file found, initializing disjoint gateway.");
                }
                Gateways { gateways: vec![] }
            }
        };
        gateways.merge_and_deduplicate(remotely_loaded_gateways);

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
                    .unwrap_or(default_network_api_port()),
                public_address: self.network_api.public_address,
                public_port: self.network_api.public_port,
            },
            ws_api: WebsocketApiConfig {
                address: self.ws_api.address.unwrap_or_else(|| match mode {
                    OperationMode::Local => default_local_address(),
                    OperationMode::Network => default_listening_address(),
                }),
                port: self
                    .ws_api
                    .ws_api_port
                    .unwrap_or(default_http_gateway_port()),
            },
            secrets,
            log_level: self.log_level.unwrap_or(tracing::log::LevelFilter::Info),
            config_paths: Arc::new(config_paths),
            gateways: gateways.gateways.clone(),
            is_gateway: self.network_api.is_gateway,
        };

        fs::create_dir_all(this.config_dir())?;
        gateways.save_to_file(&gateways_file)?;

        if should_persist {
            let path = this.config_dir().join("config.toml");
            tracing::info!("Persisting configuration to {:?}", path);
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

#[derive(Debug, Serialize, Deserialize)]
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
}

impl Config {
    pub fn transport_keypair(&self) -> &TransportKeypair {
        self.secrets.transport_keypair()
    }

    pub(crate) fn paths(&self) -> Arc<ConfigPaths> {
        self.config_paths.clone()
    }
}

#[derive(clap::Parser, Debug, Default, Copy, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
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
}

#[inline]
pub const fn default_network_api_port() -> u16 {
    31337
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

    /// Port to expose the websocket on, default is 50509
    #[arg(long, env = "WS_API_PORT")]
    #[serde(rename = "ws-api-port", skip_serializing_if = "Option::is_none")]
    pub ws_api_port: Option<u16>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct WebsocketApiConfig {
    /// Address to bind to
    #[serde(default = "default_listening_address", rename = "ws-api-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[serde(default = "default_http_gateway_port", rename = "ws-api-port")]
    pub port: u16,
}

impl From<SocketAddr> for WebsocketApiConfig {
    fn from(addr: SocketAddr) -> Self {
        Self {
            address: addr.ip(),
            port: addr.port(),
        }
    }
}

impl Default for WebsocketApiConfig {
    #[inline]
    fn default() -> Self {
        Self {
            address: default_listening_address(),
            port: default_http_gateway_port(),
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
    50509
}

#[derive(clap::Parser, Default, Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPathsArgs {
    /// The configuration directory.
    #[arg(long, default_value = None, env = "CONFIG_DIR")]
    config_dir: Option<PathBuf>,
    /// The contracts directory.
    #[arg(long, default_value = None, env = "CONTRACTS_DIR")]
    contracts_dir: Option<PathBuf>,
    /// The delegates directory.
    #[arg(long, default_value = None, env = "DELEGATES_DIR")]
    delegates_dir: Option<PathBuf>,
    /// The secrets directory.
    #[arg(long, default_value = None, env = "SECRECTS_DIR")]
    secrets_dir: Option<PathBuf>,
    /// The database directory.
    #[arg(long, default_value = None, env = "DB_DIR")]
    db_dir: Option<PathBuf>,
    /// The event log file.
    #[arg(long, default_value = None, env = "EVENT_LOG")]
    event_log: Option<PathBuf>,
    /// The data directory.
    #[arg(long, default_value = None, env = "DATA_DIR")]
    data_dir: Option<PathBuf>,
}

impl ConfigPathsArgs {
    fn merge(&mut self, other: ConfigPaths) {
        self.config_dir.get_or_insert(other.config_dir);
        self.contracts_dir.get_or_insert(other.contracts_dir);
        self.delegates_dir.get_or_insert(other.delegates_dir);
        self.secrets_dir.get_or_insert(other.secrets_dir);
        self.db_dir.get_or_insert(other.db_dir);
        self.event_log.get_or_insert(other.event_log);
        self.data_dir.get_or_insert(other.data_dir);
    }

    fn default_dirs(id: Option<&str>) -> std::io::Result<Either<ProjectDirs, PathBuf>> {
        // if id is set, most likely we are running tests or in simulated mode
        let default_dir: Either<_, _> = if cfg!(any(test, debug_assertions)) || id.is_some() {
            Either::Right(std::env::temp_dir().join(if let Some(id) = id {
                format!("freenet-{id}")
            } else {
                "freenet".into()
            }))
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
                    unreachable!()
                };
                Ok(defaults.data_dir().to_path_buf())
            })?;
        let contracts_dir = self
            .contracts_dir
            .unwrap_or_else(|| app_data_dir.join("contracts"));
        let delegates_dir = self
            .delegates_dir
            .unwrap_or_else(|| app_data_dir.join("delegates"));
        let secrets_dir = self
            .secrets_dir
            .unwrap_or_else(|| app_data_dir.join("secrets"));
        let db_dir = self.db_dir.unwrap_or_else(|| app_data_dir.join("db"));

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
                    unreachable!()
                };
                Ok(defaults.config_dir().to_path_buf())
            })?;

        Ok(ConfigPaths {
            contracts_dir,
            delegates_dir,
            secrets_dir,
            db_dir,
            data_dir: app_data_dir,
            event_log,
            config_dir,
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

    pub fn iter(&self) -> ConfigPathsIter {
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct GatewayConfig {
    /// Address of the gateway. It can be either a hostname or an IP address and port.
    pub address: Address,

    /// Path to the public key of the gateway in PEM format.
    #[serde(rename = "public_key")]
    pub public_key_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub enum Address {
    #[serde(rename = "hostname")]
    Hostname(String),
    #[serde(rename = "host_address")]
    HostAddress(SocketAddr),
}

pub(crate) struct GlobalExecutor;

impl GlobalExecutor {
    /// Returns the runtime handle if it was initialized or none if it was already
    /// running on the background.
    pub(crate) fn initialize_async_rt() -> Option<Runtime> {
        if tokio::runtime::Handle::try_current().is_ok() {
            None
        } else {
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
            rt.spawn(f)
        } else {
            unreachable!("the executor must have been initialized")
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

        // Validate the public key
        let mut key_file = File::open(&local_path).with_context(|| {
            format!(
                "failed loading gateway pubkey from {:?}",
                gateway.public_key_path
            )
        })?;
        let mut buf = String::new();
        key_file.read_to_string(&mut buf)?;
        if rsa::RsaPublicKey::from_public_key_pem(&buf).is_ok() {
            gateway.public_key_path = local_path;
            valid_gateways.push(gateway.clone());
        } else {
            tracing::warn!(
                "Invalid public key found in remote gateway file: {:?}, ignoring",
                gateway.public_key_path
            );
        }
    }

    gateways.gateways = valid_gateways;
    Ok(gateways)
}

#[cfg(test)]
mod tests {
    use httptest::{matchers::*, responders::*, Expectation, Server};

    use pkcs8::EncodePublicKey;
    use rsa::RsaPublicKey;

    use crate::node::NodeConfig;

    use super::*;

    #[tokio::test]
    async fn test_serde_config_args() {
        let args = ConfigArgs::default();
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

        let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 256).unwrap();
        let key = key
            .to_public_key()
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .unwrap();
        server.expect(
            Expectation::matching(request::path("/path/to/public_key.pem"))
                .respond_with(status_code(200).body(key.as_str().to_string())),
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
                },
                GatewayConfig {
                    address: Address::Hostname("technic.locut.us".to_string()),
                    public_key_path: PathBuf::from("path/to/key"),
                },
            ],
        };

        let serialized = toml::to_string(&gateways).unwrap();
        let _: Gateways = toml::from_str(&serialized).unwrap();
    }

    #[tokio::test]
    async fn test_remote_freenet_gateways() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let gateways = load_gateways_from_index(FREENET_GATEWAYS_INDEX, tmp_dir.path())
            .await
            .unwrap();
        assert!(!gateways.gateways.is_empty());

        for gw in gateways.gateways {
            assert!(gw.public_key_path.exists());
            assert!(RsaPublicKey::from_public_key_pem(
                &std::fs::read_to_string(gw.public_key_path).unwrap(),
            )
            .is_ok());
            let socket = NodeConfig::parse_socket_addr(&gw.address).await.unwrap();
            assert_eq!(socket.port(), default_network_api_port());
        }
    }
}
