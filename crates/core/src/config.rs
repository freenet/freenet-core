#![allow(unused)] // FIXME: remove unused
use std::{
    convert::TryFrom,
    fs::{self, File},
    future::Future,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    time::Duration,
};

use directories::ProjectDirs;
use libp2p::{identity, PeerId};
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

use crate::DynError;

const DEFAULT_BOOTSTRAP_PORT: u16 = 7800;
const DEFAULT_WEBSOCKET_API_PORT: u16 = 55008;

static CONFIG: std::sync::OnceLock<Config> = std::sync::OnceLock::new();
pub(crate) const PEER_TIMEOUT: Duration = Duration::from_secs(60);

// Initialize the executor once.
static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(GlobalExecutor::initialize_async_rt);

const QUALIFIER: &str = "";
const ORGANIZATION: &str = "The Freenet Project Inc";
const APPLICATION: &str = "Freenet";

pub struct Config {
    pub bootstrap_ip: IpAddr,
    pub bootstrap_port: u16,
    pub bootstrap_id: Option<PeerId>,
    pub local_peer_keypair: Option<identity::Keypair>,
    pub log_level: tracing::log::LevelFilter,
    pub config_paths: ConfigPaths,

    #[cfg(feature = "websocket")]
    pub(crate) ws: WebSocketApiConfig,
}

#[cfg(feature = "websocket")]
#[derive(Debug, Copy, Clone)]
pub(crate) struct WebSocketApiConfig {
    ip: IpAddr,
    port: u16,
}

impl From<WebSocketApiConfig> for SocketAddr {
    fn from(val: WebSocketApiConfig) -> Self {
        (val.ip, val.port).into()
    }
}

#[cfg(feature = "websocket")]
impl WebSocketApiConfig {
    fn from_config(config: &config::Config) -> Self {
        WebSocketApiConfig {
            ip: IpAddr::from_str(
                &config
                    .get_string("websocket_api_ip")
                    .unwrap_or_else(|_| format!("{}", Ipv4Addr::LOCALHOST)),
            )
            .map_err(|_err| std::io::ErrorKind::InvalidInput)
            .unwrap(),
            port: config
                .get_int("websocket_api_port")
                .map(u16::try_from)
                .unwrap_or(Ok(DEFAULT_WEBSOCKET_API_PORT))
                .map_err(|_err| std::io::ErrorKind::InvalidInput)
                .unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct ConfigPaths {
    pub(crate) contracts_dir: PathBuf,
    pub(crate) delegates_dir: PathBuf,
    pub(crate) secrets_dir: PathBuf,
    pub(crate) db_dir: PathBuf,
    app_data_dir: PathBuf,
}

impl ConfigPaths {
    fn new() -> std::io::Result<ConfigPaths> {
        let project_dir = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
            .ok_or(std::io::ErrorKind::NotFound)?;
        let app_data_dir: PathBuf = if cfg!(any(test, debug_assertions)) {
            std::env::temp_dir().join("freenet")
        } else {
            project_dir.data_dir().into()
        };
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
        }

        Ok(Self {
            contracts_dir,
            delegates_dir,
            secrets_dir,
            db_dir,
            app_data_dir,
        })
    }

    pub fn local_contracts_dir(&self) -> PathBuf {
        self.contracts_dir.join("local")
    }

    pub fn contracts_dir(&self) -> &Path {
        &self.contracts_dir
    }

    pub fn local_delegates_dir(&self) -> PathBuf {
        self.delegates_dir.join("local")
    }

    pub fn delegates_dir(&self) -> &Path {
        &self.delegates_dir
    }

    pub fn local_secrets_dir(&self) -> PathBuf {
        self.secrets_dir.join("local")
    }

    pub fn secrets_dir(&self) -> &Path {
        &self.secrets_dir
    }
}

impl Config {
    pub fn set_from_cli() -> Result<(), DynError> {
        todo!()
    }

    pub fn get_static_conf() -> &'static Config {
        CONFIG.get_or_init(|| Config::load_conf().expect("Failed to load configuration"))
    }

    fn load_conf() -> std::io::Result<Config> {
        let settings = config::Config::builder()
            .add_source(config::Environment::with_prefix("FREENET"))
            .build()
            .unwrap();
        let local_peer_keypair = if let Ok(path_to_key) = settings
            .get_string("local_peer_key_file")
            .map(PathBuf::from)
        {
            let mut key_file = File::open(&path_to_key).unwrap_or_else(|_| {
                panic!(
                    "Failed to open key file: {}",
                    &path_to_key.to_str().unwrap()
                )
            });
            let mut buf = Vec::new();
            key_file.read_to_end(&mut buf).unwrap();
            Some(
                identity::Keypair::from_protobuf_encoding(&buf)
                    .map_err(|_| std::io::ErrorKind::InvalidData)?,
            )
        } else {
            None
        };
        let log_level = settings
            .get_string("log")
            .map(|lvl| lvl.parse().ok())
            .ok()
            .flatten()
            .unwrap_or(tracing::log::LevelFilter::Info);
        let (bootstrap_ip, bootstrap_port, bootstrap_id) = Config::get_bootstrap_host(&settings)?;
        let config_paths = ConfigPaths::new()?;

        Ok(Config {
            bootstrap_ip,
            bootstrap_port,
            bootstrap_id,
            local_peer_keypair,
            log_level,
            config_paths,
            #[cfg(feature = "websocket")]
            ws: WebSocketApiConfig::from_config(&settings),
        })
    }

    fn get_bootstrap_host(
        settings: &config::Config,
    ) -> std::io::Result<(IpAddr, u16, Option<PeerId>)> {
        let bootstrap_ip = IpAddr::from_str(
            &settings
                .get_string("bootstrap_host")
                .unwrap_or_else(|_| format!("{}", Ipv4Addr::LOCALHOST)),
        )
        .map_err(|_err| std::io::ErrorKind::InvalidInput)?;

        let bootstrap_port = settings
            .get_int("bootstrap_port")
            .ok()
            .map(u16::try_from)
            .unwrap_or(Ok(DEFAULT_BOOTSTRAP_PORT))
            .map_err(|_err| std::io::ErrorKind::InvalidInput)?;

        let id_str = if let Some(id_str) = settings
            .get_string("bootstrap_id")
            .ok()
            .map(|id| id.parse().map_err(|_err| std::io::ErrorKind::InvalidInput))
        {
            Some(id_str?)
        } else {
            None
        };

        Ok((bootstrap_ip, bootstrap_port, id_str))
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

impl libp2p::swarm::Executor for GlobalExecutor {
    fn exec(&self, future: Pin<Box<dyn Future<Output = ()> + 'static + Send>>) {
        GlobalExecutor::spawn(future);
    }
}

pub(super) mod tracer {
    use super::*;

    #[cfg(feature = "trace")]
    pub fn init_tracer() -> Result<(), opentelemetry::trace::TraceError> {
        use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::Registry;

        let tracer = opentelemetry_jaeger::new_agent_pipeline().install_simple()?;
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = Registry::default().with(telemetry);
        global::set_text_map_propagator(TraceContextPropagator::new());
        tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
        Ok(())
    }
}
