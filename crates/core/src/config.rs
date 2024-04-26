use std::{
    borrow::Cow,
    fs::{self, File},
    future::Future,
    io::Read,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    pin::Pin,
    sync::atomic::AtomicBool,
    time::Duration,
};

use directories::ProjectDirs;
use libp2p::identity;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::local_node::OperationMode;

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

pub(crate) const PEER_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const OPERATION_TTL: Duration = Duration::from_secs(60);

// Initialize the executor once.
static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(GlobalExecutor::initialize_async_rt);

const QUALIFIER: &str = "";
const ORGANIZATION: &str = "The Freenet Project Inc";
const APPLICATION: &str = "Freenet";

#[derive(clap::Parser, Debug, Serialize, Deserialize)]
pub struct ConfigArgs {
    /// Node operation mode.
    #[clap(value_enum, default_value_t = OperationMode::Local, env = "MODE")]
    pub mode: OperationMode,

    /// Overrides the default data directory where Freenet contract files are stored.
    #[clap(long, env = "NODE_DATA_DIR")]
    pub node_data_dir: Option<PathBuf>,

    #[clap(flatten)]
    #[serde(flatten)]
    pub gateway: GatewayConfig,
    #[clap(value_parser, env = "LOCAL_PEER_KEYPAIR")]
    pub local_peer_keypair: Option<PathBuf>,
    #[serde(with = "serde_log_level_filter")]
    #[clap(long, default_value = "info", env = "INFO")]
    pub log_level: tracing::log::LevelFilter,
    #[clap(flatten)]
    #[serde(flatten)]
    config_paths: ConfigPathsArgs,
}

impl Default for ConfigArgs {
    fn default() -> Self {
        Self {
            mode: OperationMode::Local,
            node_data_dir: None,
            gateway: Default::default(),
            local_peer_keypair: None,
            log_level: tracing::log::LevelFilter::Info,
            config_paths: Default::default(),
        }
    }
}

impl ConfigArgs {
    /// Parse the command line arguments and return the configuration.
    pub fn build(self) -> std::io::Result<Config> {
        Ok(Config {
            mode: self.mode,
            gateway: self.gateway,
            local_peer_keypair: match self.local_peer_keypair {
                Some(path) => parse_keypair(path.to_string_lossy().trim())
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
                None => identity::Keypair::generate_ed25519(),
            },
            log_level: self.log_level,
            config_paths: self.config_paths.build()?,
        })
    }
}

fn parse_keypair(s: &str) -> Result<identity::Keypair, Cow<'static, str>> {
    // first check if the string is a path to a file
    let path = std::path::Path::new(s);
    if path.exists() {
        let mut key_file =
            File::open(path).map_err(|e| format!("Failed to open key file {s}: {e}"))?;
        let mut buf = Vec::new();
        key_file
            .read_to_end(&mut buf)
            .map_err(|e| format!("Failed to read key file {s}: {e}"))?;
        let keypair = identity::Keypair::from_protobuf_encoding(&buf)
            .map_err(|e| format!("Failed to parse key file {s}: {e}"))?;
        return Ok(keypair);
    }

    // otherwise, try to parse the string as a base64 encoded keypair
    let keypair =
        identity::Keypair::from_protobuf_encoding(s.as_bytes()).map_err(|e| format!("{e}"))?;
    Ok(keypair)
}

fn deserialize_keypair<'de, D>(deserializer: D) -> Result<identity::Keypair, D::Error>
where
    D: serde::Deserializer<'de>,
{
    parse_keypair(<&str>::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

mod serde_log_level_filter {
    use serde::{Deserialize, Deserializer, Serializer};
    use tracing::log::LevelFilter;

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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// Node operation mode.
    pub mode: OperationMode,

    #[serde(flatten)]
    pub gateway: GatewayConfig,
    // FIXME: how to serialize this?
    /// Path to the local peer keypair file.
    #[serde(skip_serializing, deserialize_with = "deserialize_keypair")]
    pub local_peer_keypair: identity::Keypair,
    #[serde(with = "serde_log_level_filter")]
    pub log_level: tracing::log::LevelFilter,
    #[serde(flatten)]
    config_paths: ConfigPaths,
}

impl Config {
    pub fn local_peer_keypair(&self) -> &identity::Keypair {
        &self.local_peer_keypair
    }
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

    // pub fn conf() -> &'static Config {
    //     // CONFIG.get_or_init(|| match Config::load_conf() {
    //     //     Ok(config) => config,
    //     //     Err(err) => {
    //     //         tracing::error!("failed while loading configuration: {err}");
    //     //         panic!("Failed while loading configuration")
    //     //     }
    //     // })
    //     todo!()
    // }

    // fn load_conf() -> std::io::Result<Config> {
    //     let settings: config::Config = config::Config::builder()
    //         .add_source(config::Environment::with_prefix("FREENET"))
    //         .build()
    //         .unwrap();
    //     let local_peer_keypair = if let Ok(path_to_key) = settings
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
    //         Some(
    //             identity::Keypair::from_protobuf_encoding(&buf)
    //                 .map_err(|_| std::io::ErrorKind::InvalidData)?,
    //         )
    //     } else {
    //         None
    //     };
    //     let log_level = settings
    //         .get_string("log")
    //         .map(|lvl| lvl.parse().ok())
    //         .ok()
    //         .flatten()
    //         .unwrap_or(tracing::log::LevelFilter::Info);
    //     let (bootstrap_ip, bootstrap_port, bootstrap_id) = Config::get_bootstrap_host(&settings)?;

    //     let data_dir = settings.get_string("data_dir").ok().map(PathBuf::from);
    //     let config_paths = ConfigPaths::new(data_dir)?;

    //     let local_mode = settings.get_string("network_mode").is_err();

    //     Ok(Config {
    //         bootstrap_ip,
    //         bootstrap_port,
    //         bootstrap_id,
    //         local_peer_keypair: local_peer_keypair
    //             .unwrap_or_else(identity::Keypair::generate_ed25519),
    //         log_level,
    //         config_paths,
    //         local_mode: AtomicBool::new(local_mode),
    //         #[cfg(feature = "websocket")]
    //         ws: WebSocketApiConfig::from_config(&settings),
    //     })
    // }

    // fn get_bootstrap_host(
    //     settings: &config::Config,
    // ) -> std::io::Result<(IpAddr, u16, Option<PeerId>)> {
    //     let bootstrap_ip = IpAddr::from_str(
    //         &settings
    //             .get_string("bootstrap_host")
    //             .unwrap_or_else(|_| format!("{}", Ipv4Addr::LOCALHOST)),
    //     )
    //     .map_err(|_err| std::io::ErrorKind::InvalidInput)?;

    //     let bootstrap_port = settings
    //         .get_int("bootstrap_port")
    //         .ok()
    //         .map(u16::try_from)
    //         .unwrap_or(Ok(DEFAULT_BOOTSTRAP_PORT))
    //         .map_err(|_err| std::io::ErrorKind::InvalidInput)?;

    //     let id_str = if let Some(id_str) = settings
    //         .get_string("bootstrap_id")
    //         .ok()
    //         .map(|id| id.parse().map_err(|_err| std::io::ErrorKind::InvalidInput))
    //     {
    //         Some(id_str?)
    //     } else {
    //         None
    //     };

    //     Ok((bootstrap_ip, bootstrap_port, id_str))
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

impl libp2p::swarm::Executor for GlobalExecutor {
    fn exec(&self, future: Pin<Box<dyn Future<Output = ()> + 'static + Send>>) {
        GlobalExecutor::spawn(future);
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
