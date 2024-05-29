use std::{
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use directories::ProjectDirs;
use once_cell::sync::Lazy;
use pkcs1::DecodeRsaPrivateKey;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::{dev_tool::PeerId, local_node::OperationMode, transport::TransportKeypair};

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

#[derive(clap::Parser, Debug, Serialize, Deserialize)]
pub struct ConfigArgs {
    /// Node operation mode. Default is network mode.
    #[clap(value_enum, env = "MODE")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<OperationMode>,

    #[clap(flatten)]
    #[serde(flatten)]
    pub ws_api: WebsocketApiArgs,

    #[clap(flatten)]
    #[serde(flatten)]
    pub network_listener: NetworkArgs,

    #[clap(value_parser, env = "TRANSPORT_KEYPAIR")]
    pub transport_keypair: Option<PathBuf>,

    #[serde(
        with = "serde_option_log_level_filter",
        skip_serializing_if = "Option::is_none"
    )]
    #[clap(long, env = "LOG_LEVEL")]
    pub log_level: Option<tracing::log::LevelFilter>,

    #[clap(flatten)]
    #[serde(flatten)]
    config_paths: ConfigPathsArgs,

    /// An arbitrary identifier for the node, mostly for debugging or testing purposes.
    #[clap(long)]
    pub id: Option<String>,
}

impl Default for ConfigArgs {
    fn default() -> Self {
        Self {
            mode: Some(OperationMode::Network),
            network_listener: NetworkArgs {
                address: Some(default_address()),
                port: Some(default_gateway_port()),
                public_address: None,
                public_port: None,
            },
            ws_api: WebsocketApiArgs {
                address: Some(default_address()),
                port: Some(default_gateway_port()),
            },
            transport_keypair: None,
            log_level: Some(tracing::log::LevelFilter::Info),
            config_paths: Default::default(),
            id: None,
        }
    }
}

impl ConfigArgs {
    fn read_config(dir: &PathBuf) -> std::io::Result<Option<Config>> {
        if dir.exists() {
            let mut dir = std::fs::read_dir(dir)?;
            let config_args = dir.find_map(|f| {
                if let Ok(f) = f {
                    let filename = f.file_name().to_string_lossy().into_owned();
                    let ext = filename.rsplit('.').next().map(|s| s.to_owned());

                    if let Some(ext) = ext {
                        if filename.starts_with("config") {
                            match ext.as_str() {
                                "toml" => {
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
                Some((filename, ext)) => match ext.as_str() {
                    "toml" => {
                        let mut file = File::open(&*filename)?;
                        let mut content = String::new();
                        file.read_to_string(&mut content)?;
                        let mut config = toml::from_str::<Config>(&content).map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                        })?;
                        let (_, transport_keypair) =
                            Self::read_transport_keypair(config.secrets_dir())?;
                        config.transport_keypair = transport_keypair;
                        Ok(Some(config))
                    }
                    "json" => {
                        let mut file = File::open(&*filename)?;
                        let mut config = serde_json::from_reader::<_, Config>(&mut file)?;
                        let (_, transport_keypair) =
                            Self::read_transport_keypair(config.secrets_dir())?;
                        config.transport_keypair = transport_keypair;
                        Ok(Some(config))
                    }
                    ext => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Invalid configuration file extension: {}", ext),
                    )),
                },
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn read_transport_keypair(
        path_to_key: PathBuf,
    ) -> std::io::Result<(PathBuf, TransportKeypair)> {
        let mut key_file = File::open(&path_to_key).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("Failed to open key file {}: {e}", path_to_key.display()),
            )
        })?;
        let mut buf = String::new();
        key_file.read_to_string(&mut buf).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("Failed to read key file {}: {e}", path_to_key.display()),
            )
        })?;

        let pk = rsa::RsaPrivateKey::from_pkcs1_pem(&buf).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to read key file {}: {e}", path_to_key.display()),
            )
        })?;

        Ok::<_, std::io::Error>((path_to_key, TransportKeypair::from_private_key(pk)))
    }

    /// Parse the command line arguments and return the configuration.
    pub fn build(mut self) -> std::io::Result<Config> {
        let cfg = if let Some(path) = self.config_paths.config_dir.as_ref() {
            if !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Configuration directory not found",
                ));
            }

            Self::read_config(path)?
        } else {
            // find default application dir to see if there is a config file
            let dir = ConfigPathsArgs::config_dir()?;
            Self::read_config(&dir).ok().flatten()
        };

        let should_persist = cfg.is_none();

        // merge the configuration from the file with the command line arguments
        if let Some(cfg) = cfg {
            self.transport_keypair
                .get_or_insert(cfg.transport_keypair_path);
            self.mode.get_or_insert(cfg.mode);
            self.ws_api.address.get_or_insert(cfg.ws_api.address);
            self.ws_api.port.get_or_insert(cfg.ws_api.port);
            self.log_level.get_or_insert(cfg.log_level);
            self.config_paths.merge(cfg.config_paths.as_ref().clone());
        }

        let mode = self.mode.unwrap_or(OperationMode::Network);
        let config_paths = self.config_paths.build(self.id.as_deref())?;

        let transport_key = self
            .transport_keypair
            .map(Self::read_transport_keypair)
            .transpose()?;
        let (transport_keypair_path, transport_keypair) =
            if let Some((transport_key_path, transport_key)) = transport_key {
                (transport_key_path, transport_key)
            } else {
                let transport_key = TransportKeypair::new();
                let transport_key_path =
                    config_paths.secrets_dir(mode).join("transport_keypair.pem");

                // if the transport key file exists, then read it
                if transport_key_path.exists() {
                    Self::read_transport_keypair(transport_key_path)?
                } else {
                    let mut file = File::create(&transport_key_path)?;
                    file.write_all(&transport_key.secret().to_bytes().map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("failed to write transport key: {e}"),
                        )
                    })?)?;
                    (transport_key_path, transport_key)
                }
            };

        let this = Config {
            mode,
            peer_id: self
                .network_listener
                .public_address
                .zip(self.network_listener.public_port)
                .map(|(addr, port)| {
                    PeerId::new((addr, port).into(), transport_keypair.public().clone())
                }),
            network_api: NetworkApiConfig {
                address: self.ws_api.address.unwrap_or_else(|| match mode {
                    OperationMode::Local => default_local_address(),
                    OperationMode::Network => default_address(),
                }),
                port: self.ws_api.port.unwrap_or(default_gateway_port()),
                public_address: self.network_listener.public_address,
                public_port: self.network_listener.public_port,
            },
            ws_api: WebsocketApiConfig {
                address: self.ws_api.address.unwrap_or_else(|| match mode {
                    OperationMode::Local => default_local_address(),
                    OperationMode::Network => default_address(),
                }),
                port: self.ws_api.port.unwrap_or(default_gateway_port()),
            },
            transport_keypair,
            transport_keypair_path,
            log_level: self.log_level.unwrap_or(tracing::log::LevelFilter::Info),
            config_paths: Arc::new(config_paths),
        };

        fs::create_dir_all(&this.config_paths.config_dir)?;
        if should_persist {
            let mut file = File::create(this.config_paths.config_dir.join("config.toml"))?;
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
        let level = <&str>::deserialize(deserializer)?;
        parse_log_level_str::<D>(level)
    }
}

mod serde_option_log_level_filter {
    use serde::{Deserialize, Deserializer, Serializer};
    use tracing::log::LevelFilter;

    pub fn serialize<S>(level: &Option<LevelFilter>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(level) = level {
            super::serde_log_level_filter::serialize(level, serializer)
        } else {
            serializer.serialize_none()
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<LevelFilter>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let level = <Option<&str>>::deserialize(deserializer)?;

        match level {
            Some(level) => Ok(Some(
                super::serde_log_level_filter::parse_log_level_str::<D>(level)?,
            )),
            None => Ok(None),
        }
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
    #[serde(skip)]
    pub transport_keypair: TransportKeypair,
    #[serde(rename = "transport_keypair")]
    pub transport_keypair_path: PathBuf,
    #[serde(with = "serde_log_level_filter")]
    pub log_level: tracing::log::LevelFilter,
    #[serde(flatten)]
    config_paths: Arc<ConfigPaths>,
    #[serde(skip)]
    pub(crate) peer_id: Option<PeerId>,
}

impl Config {
    pub fn transport_keypair(&self) -> &TransportKeypair {
        &self.transport_keypair
    }

    pub(crate) fn paths(&self) -> Arc<ConfigPaths> {
        self.config_paths.clone()
    }
}

#[derive(clap::Parser, Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct NetworkArgs {
    /// Address to bind to for the network event listener, default is 0.0.0.0
    #[arg(long = "network-address", env = "NETWORK_ADDRESS")]
    #[serde(rename = "network-address", skip_serializing_if = "Option::is_none")]
    pub address: Option<IpAddr>,

    /// Port to bind for the network event listener, default is 31337
    #[arg(long = "network-port", env = "NETWORK_PORT")]
    #[serde(rename = "network-port", skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

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
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct NetworkApiConfig {
    /// Address to bind to
    #[serde(default = "default_address", rename = "network-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[serde(default = "default_network_port", rename = "network-port")]
    pub port: u16,

    #[serde(
        rename = "public_network_address",
        skip_serializing_if = "Option::is_none"
    )]
    pub public_address: Option<IpAddr>,

    #[serde(rename = "public_port", skip_serializing_if = "Option::is_none")]
    pub public_port: Option<u16>,
}

#[inline]
const fn default_network_port() -> u16 {
    31337
}

#[derive(clap::Parser, Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct WebsocketApiArgs {
    /// Address to bind to for the websocket API, default is 0.0.0.0
    #[arg(long = "ws-api-address", env = "WS_API_ADDRESS")]
    #[serde(rename = "ws-api-address", skip_serializing_if = "Option::is_none")]
    pub address: Option<IpAddr>,

    /// Port to expose the websocket on, default is 50509
    #[arg(long = "ws-api-port", env = "WS_API_PORT")]
    #[serde(rename = "ws-api-port", skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct WebsocketApiConfig {
    /// Address to bind to
    #[serde(default = "default_address", rename = "ws-api-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[serde(default = "default_gateway_port", rename = "ws-api-port")]
    pub port: u16,
}

impl Default for WebsocketApiConfig {
    #[inline]
    fn default() -> Self {
        Self {
            address: default_address(),
            port: default_gateway_port(),
        }
    }
}

#[inline]
const fn default_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

#[inline]
const fn default_local_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

#[inline]
const fn default_gateway_port() -> u16 {
    50509
}

#[derive(clap::Parser, Default, Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPathsArgs {
    config_dir: Option<PathBuf>,
    contracts_dir: Option<PathBuf>,
    delegates_dir: Option<PathBuf>,
    secrets_dir: Option<PathBuf>,
    db_dir: Option<PathBuf>,
    event_log: Option<PathBuf>,
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

    pub fn app_data_dir(id: Option<&str>) -> std::io::Result<PathBuf> {
        let project_dir = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
            .ok_or(std::io::ErrorKind::NotFound)?;
        let app_data_dir: PathBuf = if cfg!(any(test, debug_assertions)) {
            std::env::temp_dir().join(if let Some(id) = id {
                format!("freenet-{id}")
            } else {
                "freenet".into()
            })
        } else {
            project_dir.data_dir().into()
        };
        Ok(app_data_dir)
    }

    pub fn config_dir() -> std::io::Result<PathBuf> {
        let project_dir = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
            .ok_or(std::io::ErrorKind::NotFound)?;
        let config_data_dir: PathBuf = if cfg!(any(test, debug_assertions)) {
            std::env::temp_dir().join("freenet").join("config")
        } else {
            project_dir.config_dir().into()
        };
        Ok(config_data_dir)
    }

    pub fn build(self, id: Option<&str>) -> std::io::Result<ConfigPaths> {
        let app_data_dir = self
            .data_dir
            .map(Ok)
            .unwrap_or_else(|| Self::app_data_dir(id))?;
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

        Ok(ConfigPaths {
            contracts_dir,
            delegates_dir,
            secrets_dir,
            db_dir,
            data_dir: app_data_dir,
            event_log,
            config_dir: match self.config_dir {
                Some(dir) => dir,
                None => Self::config_dir()?,
            },
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

impl<'a> core::iter::FusedIterator for ConfigPathsIter<'a> {}

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

pub fn set_logger(level: Option<tracing::level_filters::LevelFilter>) {
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

        crate::tracing::tracer::init_tracer(level).expect("failed tracing initialization")
    }
}
