use std::{path::PathBuf, time::Duration};

use anyhow::Error;
use freenet::dev_tool::SimNetwork;

pub(crate) mod network;
mod single_process;

use crate::network_metrics_server::{start_server, ServerConfig};

/// Testing framework for running Freenet network simulations.
#[derive(clap::Parser, Clone)]
pub struct TestConfig {
    /// Test name. If not provided, a random name will be generated.
    #[arg(long)]
    name: Option<String>,
    /// Seed to use when generating random data. If not provided, a random seed will be used.
    #[arg(long)]
    seed: Option<u64>,
    /// Number of total gateways for this test.
    #[arg(long, default_value_t = 2)]
    gateways: usize,
    /// Number of total regular peer nodes for this test.
    #[arg(long, default_value_t = 10)]
    nodes: usize,
    /// Max hops to live for operations (if it applies).
    #[arg(long, default_value_t = freenet::config::DEFAULT_MAX_HOPS_TO_LIVE)]
    ring_max_htl: usize,
    /// Default threshold for randomizing potential peers for new connections.
    #[arg(long, default_value_t = freenet::config::DEFAULT_RANDOM_PEER_CONN_THRESHOLD)]
    rnd_if_htl_above: usize,
    /// Maximum number of connections per peer.
    #[arg(long, default_value_t = freenet::config::DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
    /// Minimum number of connections per peer.
    #[arg(long, default_value_t = freenet::config::DEFAULT_MIN_CONNECTIONS)]
    min_connections: usize,
    /// Maximum number of contracts live in the network.
    #[arg(long)]
    max_contract_number: Option<usize>,
    /// Number of events that will be executed in this simulation.
    /// Events are simulated get, puts and other operations.
    #[arg(long, default_value_t = u32::MAX)]
    events: u32,
    /// Time in milliseconds to wait for the next event to be executed.
    #[arg(long)]
    event_wait_ms: Option<u64>,
    /// Time in milliseconds to wait for the network to be sufficiently connected to start sending events.
    /// (20% of the expected connections to be processed per gateway)
    #[arg(long)]
    connection_wait_ms: Option<u64>,
    /// Time in milliseconds to wait for the next peer in the simulation to be started.
    #[arg(long)]
    peer_start_backoff_ms: Option<u64>,
    /// If provided, the execution data will be saved in this directory.
    #[arg(long)]
    execution_data: Option<PathBuf>,
    /// Don't start the metrics server for this test run.
    #[arg(long)]
    disable_metrics: bool,
    #[clap(subcommand)]
    /// Execution mode for the test.
    pub command: TestMode,
}

impl TestConfig {
    fn get_connection_check_params(&self) -> (Duration, f64) {
        let conns_per_gw = (self.nodes / self.gateways) as f64;
        let conn_percent = (conns_per_gw / self.nodes as f64).min(0.99);
        let connectivity_timeout =
            Duration::from_millis(self.connection_wait_ms.unwrap_or_else(|| {
                // expect a peer to take max 200ms to connect, this should happen in parallel
                // but err on the side of safety
                (conns_per_gw
                    * self
                        .peer_start_backoff_ms
                        .map(|ms| ms as f64)
                        .unwrap_or(200.0))
                .ceil() as u64
            }));
        (connectivity_timeout, conn_percent)
    }

    fn seed(&self) -> u64 {
        use rand::RngCore;
        self.seed.unwrap_or_else(|| rand::rngs::OsRng.next_u64())
    }
}

fn randomize_test_name() -> String {
    const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz";
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();
    let mut name = String::with_capacity(16);
    for _ in 0..16 {
        name.push(ALPHABET.chars().choose(&mut rng).expect("non empty"));
    }
    name
}

/// Under which mode will the test run execute.
#[derive(clap::Parser, Clone)]
pub enum TestMode {
    /// Runs multiple simulated nodes in a single process.
    SingleProcess,
    /// Runs multiple simulated nodes in multiple processes and multiple machines.
    Network(network::NetworkProcessConfig),
}

pub(crate) async fn test_framework(base_config: TestConfig) -> anyhow::Result<(), Error> {
    let (server, changes_recorder) = if !base_config.disable_metrics {
        let (s, r) = start_server(&ServerConfig {
            log_directory: base_config.execution_data.clone(),
        })
        .await;
        (Some(s), r)
    } else {
        (None, None)
    };
    let res = match &base_config.command {
        TestMode::SingleProcess => single_process::run(&base_config).await,
        TestMode::Network(config) => network::run(&base_config, config).await,
    };
    if let Some(server) = server {
        server.abort();
    }
    if let Some(changes) = changes_recorder {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            r = changes => {
                r?;
            }
        }
    }
    res
}

async fn config_sim_network(base_config: &TestConfig) -> anyhow::Result<SimNetwork, Error> {
    if base_config.gateways == 0 {
        anyhow::bail!("Gateways should be higher than 0");
    }
    if base_config.nodes == 0 {
        anyhow::bail!("Nodes should be higher than 0");
    }
    let name = &base_config
        .name
        .as_ref()
        .cloned()
        .unwrap_or_else(randomize_test_name);
    let mut sim = SimNetwork::new(
        name,
        base_config.gateways,
        base_config.nodes,
        base_config.ring_max_htl,
        base_config.rnd_if_htl_above,
        base_config.max_connections,
        base_config.min_connections,
    )
    .await;
    if let Some(backoff) = base_config.peer_start_backoff_ms {
        sim.with_start_backoff(Duration::from_millis(backoff));
    }
    Ok(sim)
}
