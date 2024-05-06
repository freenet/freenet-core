use std::{
    fs::{self, File},
    future::Future,
    io::Read,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::atomic::AtomicBool,
    time::Duration,
};

use directories::ProjectDirs;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::{local_node::OperationMode, transport::TransportKeypair};

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
    /// Node operation mode.
    #[clap(value_enum, env = "MODE")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<OperationMode>,

    /// Path to the configuration file.
    #[clap(short, long)]
    #[serde(skip)]
    pub config: Option<PathBuf>,

    #[clap(flatten)]
    #[serde(flatten)]
    pub gateway: GatewayArgs,
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
}

impl Default for ConfigArgs {
    fn default() -> Self {
        Self {
            mode: Some(OperationMode::Local),
            config: None,
            gateway: GatewayArgs {
                address: Some(default_gateway_address()),
                port: Some(default_gateway_port()),
            },
            transport_keypair: None,
            log_level: Some(tracing::log::LevelFilter::Info),
            config_paths: Default::default(),
        }
    }
}

impl ConfigArgs {
    /// Parse the command line arguments and return the configuration.
    pub fn build(mut self) -> std::io::Result<Config> {
        let cfg = if let Some(path) = self.config.as_ref() {
            if !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Configuration file not found",
                ));
            }
            match path.extension() {
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid configuration file",
                    ))
                }
                Some(ext) if ext == "toml" => {
                    let mut file = File::open(path)?;
                    let mut content = String::new();
                    file.read_to_string(&mut content)?;
                    Some(toml::from_str::<Self>(&content).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?)
                }
                Some(ext) if ext == "json" => {
                    let mut file = File::open(path)?;
                    Some(serde_json::from_reader::<_, Self>(&mut file)?)
                }
                Some(ext) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Invalid configuration file extension: {}",
                            ext.to_string_lossy()
                        ),
                    ))
                }
            }
        } else {
            None
        };

        // merge the configuration from the file with the command line arguments
        if let Some(cfg) = cfg {
            if self.transport_keypair.is_none() {
                self.transport_keypair = cfg.transport_keypair;
            }

            if self.mode.is_none() {
                self.mode = cfg.mode;
            }

            if self.gateway.address.is_none() {
                self.gateway.address = cfg.gateway.address;
            }

            if self.gateway.port.is_none() {
                self.gateway.port = cfg.gateway.port;
            }

            if self.log_level.is_none() {
                self.log_level = cfg.log_level;
            }

            self.config_paths.merge(cfg.config_paths);
        }

        let mode = self.mode.unwrap_or(OperationMode::Local);
        Ok(Config {
            mode,
            gateway: GatewayConfig {
                address: self.gateway.address.unwrap_or_else(|| match mode {
                    OperationMode::Local => default_local_gateway_address(),
                    OperationMode::Network => default_gateway_address(),
                }),
                port: self.gateway.port.unwrap_or(default_gateway_port()),
            },
            transport_keypair: match self.transport_keypair {
                Some(path_to_key) => {
                    let mut key_file = File::open(&path_to_key).map_err(|e| {
                        std::io::Error::new(
                            e.kind(),
                            format!("Failed to open key file {}: {e}", path_to_key.display()),
                        )
                    })?;
                    let mut buf = Vec::new();
                    key_file.read_to_end(&mut buf).map_err(|e| {
                        std::io::Error::new(
                            e.kind(),
                            format!("Failed to read key file {}: {e}", path_to_key.display()),
                        )
                    })?;
                    todo!("get an rsa private key from the file and create a TransportKeypair")
                }
                None => TransportKeypair::new(),
            },
            log_level: self.log_level.unwrap_or(tracing::log::LevelFilter::Info),
            config_paths: self.config_paths.build()?,
        })
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
    pub gateway: GatewayConfig,
    pub transport_keypair: TransportKeypair,
    #[serde(with = "serde_log_level_filter")]
    pub log_level: tracing::log::LevelFilter,
    #[serde(flatten)]
    config_paths: ConfigPaths,
}

impl Config {
    pub fn transport_keypair(&self) -> &TransportKeypair {
        &self.transport_keypair
    }
}

#[derive(clap::Parser, Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct GatewayArgs {
    /// Address to bind to, default is 0.0.0.0
    #[arg(long = "gateway-address", env = "GATEWAY_ADDRESS")]
    #[serde(rename = "gateway-address", skip_serializing_if = "Option::is_none")]
    pub address: Option<IpAddr>,

    /// Port to expose api on, default is 50509
    #[arg(long = "gateway-port", env = "GATEWAY_PORT")]
    #[serde(rename = "gateway-port", skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(clap::Parser, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Address to bind to
    #[arg(long = "gateway-address", default_value_t = default_gateway_address(), env = "GATEWAY_ADDRESS")]
    #[serde(default = "default_gateway_address", rename = "gateway-address")]
    pub address: IpAddr,

    /// Port to expose api on
    #[arg(long = "gateway-port", default_value_t = default_gateway_port(), env = "GATEWAY_PORT")]
    #[serde(default = "default_gateway_port", rename = "gateway-port")]
    pub port: u16,
}

impl Default for GatewayConfig {
    #[inline]
    fn default() -> Self {
        Self {
            address: default_gateway_address(),
            port: default_gateway_port(),
        }
    }
}

#[inline]
const fn default_gateway_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

#[inline]
const fn default_local_gateway_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

#[inline]
const fn default_gateway_port() -> u16 {
    50509
}

#[derive(clap::Parser, Default, Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPathsArgs {
    contracts_dir: Option<PathBuf>,
    delegates_dir: Option<PathBuf>,
    secrets_dir: Option<PathBuf>,
    db_dir: Option<PathBuf>,
    event_log: Option<PathBuf>,
    data_dir: Option<PathBuf>,
}

impl ConfigPathsArgs {
    fn merge(&mut self, other: Self) {
        if self.contracts_dir.is_none() {
            self.contracts_dir = other.contracts_dir;
        }

        if self.delegates_dir.is_none() {
            self.delegates_dir = other.delegates_dir;
        }

        if self.secrets_dir.is_none() {
            self.secrets_dir = other.secrets_dir;
        }

        if self.db_dir.is_none() {
            self.db_dir = other.db_dir;
        }

        if self.event_log.is_none() {
            self.event_log = other.event_log;
        }

        if self.data_dir.is_none() {
            self.data_dir = other.data_dir;
        }
    }

    pub fn app_data_dir() -> std::io::Result<PathBuf> {
        let project_dir = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
            .ok_or(std::io::ErrorKind::NotFound)?;
        let app_data_dir: PathBuf = if cfg!(any(test, debug_assertions)) {
            std::env::temp_dir().join("freenet")
        } else {
            project_dir.data_dir().into()
        };
        Ok(app_data_dir)
    }

    pub fn build(self) -> std::io::Result<ConfigPaths> {
        let app_data_dir = self.data_dir.map(Ok).unwrap_or_else(Self::app_data_dir)?;
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
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigPaths {
    contracts_dir: PathBuf,
    delegates_dir: PathBuf,
    secrets_dir: PathBuf,
    db_dir: PathBuf,
    event_log: PathBuf,
    data_dir: PathBuf,
}

impl ConfigPaths {
    pub fn db_dir(&self, mode: OperationMode) -> PathBuf {
        match mode {
            OperationMode::Local => self.db_dir.join("local").join("freenet"),
            OperationMode::Network => self.db_dir.to_owned().join("freenet"),
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
}

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

    // fn load_conf() -> anyhow::Result<Config> {
    //     let settings: config::Config = config::Config::builder()
    //         .add_source(config::Environment::with_prefix("FREENET"))
    //         .build()
    //         .unwrap();

    //     let transport_keypair: Option<TransportKeypair> = if let Ok(path_to_key) = settings
    //         .get_string("local_peer_key_file")
    //         .map(PathBuf::from)
    //     {
    //         let mut key_file = File::open(&path_to_key).unwrap_or_else(|_| {
    //             panic!(
    //                 "Failed to open key file: {}",
    //                 &path_to_key.to_str().unwrap()
    //             )
    //         });
    //         let mut buf = Vec::new();
    //         key_file.read_to_end(&mut buf).unwrap();
    //         todo!("get an rsa private key from the file and create a TransportKeypair")
    //     } else {
    //         None
    //     };

    //     let log_level = settings
    //         .get_string("log")
    //         .map(|lvl| lvl.parse().ok())
    //         .ok()
    //         .flatten()
    //         .unwrap_or(tracing::log::LevelFilter::Info);

    //     let data_dir = settings.get_string("data_dir").ok().map(PathBuf::from);
    //     let config_paths = ConfigPaths::new(data_dir)?;

    //     let local_mode = settings.get_string("network_mode").is_err();

    //     Ok(Config {
    //         transport_keypair: transport_keypair.unwrap_or_else(|| TransportKeypair::new()),
    //         log_level,
    //         config_paths,
    //         local_mode: AtomicBool::new(local_mode),
    //         #[cfg(feature = "websocket")]
    //         ws: WebSocketApiConfig::from_config(&settings),
    //     })
    // }
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
