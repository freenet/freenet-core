use std::{path::PathBuf, time::Duration};

use anyhow::Error;
use freenet::dev_tool::SimNetwork;
use freenet::simulation::FaultConfig;

pub(crate) mod network;
mod single_process;

use crate::network_metrics_server::{start_server, ServerConfig};

/// Parse a seed value that can be decimal or hex (0x prefix).
fn parse_seed(s: &str) -> Result<u64, String> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16).map_err(|e| format!("invalid hex seed: {}", e))
    } else {
        s.parse().map_err(|e| format!("invalid seed: {}", e))
    }
}

/// Testing framework for running Freenet network simulations.
#[derive(clap::Parser, Clone)]
pub struct TestConfig {
    /// Test name. If not provided, a random name will be generated.
    #[arg(long)]
    name: Option<String>,
    /// Seed to use when generating random data (supports hex with 0x prefix).
    /// If not provided, a random seed will be used.
    /// Example: --seed 12345 or --seed 0xDEADBEEF
    #[arg(long, value_parser = parse_seed)]
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

    // =========================================================================
    // Fault Injection Options
    // =========================================================================
    /// Enable fault injection with the specified message loss rate (0.0 to 1.0).
    /// Example: --message-loss 0.05 for 5% message loss
    #[arg(long, value_name = "RATE")]
    message_loss: Option<f64>,

    /// Minimum latency to add to messages in milliseconds.
    /// Used together with --latency-max to define a range.
    #[arg(long, value_name = "MS")]
    latency_min: Option<u64>,

    /// Maximum latency to add to messages in milliseconds.
    /// Used together with --latency-min to define a range.
    #[arg(long, value_name = "MS")]
    latency_max: Option<u64>,

    // =========================================================================
    // Verification Options (enabled by default for eventual consistency)
    // =========================================================================
    /// Check convergence after test completion with the specified timeout in seconds.
    /// Default: 60 seconds. Set to 0 to disable.
    /// Example: --check-convergence 30
    #[arg(long, value_name = "TIMEOUT_SECS", default_value = "60")]
    check_convergence: u64,

    /// Minimum success rate for operations (0.0 to 1.0).
    /// Test will fail if success rate is below this threshold.
    /// Default: 0.95 (95%). Set to 0.0 to disable.
    /// Example: --min-success-rate 0.95 for 95% success rate
    #[arg(long, value_name = "RATE", default_value = "0.95")]
    min_success_rate: f64,

    /// Print operation summary after test completion.
    #[arg(long)]
    print_summary: bool,

    /// Print network statistics after test completion (requires fault injection).
    #[arg(long)]
    print_network_stats: bool,

    #[clap(subcommand)]
    /// Execution mode for the test.
    pub command: TestMode,
}

impl TestConfig {
    pub(crate) fn get_connection_check_params(&self) -> (Duration, f64) {
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

    pub(crate) fn seed(&self) -> u64 {
        use rand::RngCore;
        self.seed.unwrap_or_else(|| rand::rng().next_u64())
    }

    /// Returns true if any fault injection options are configured.
    fn has_fault_injection(&self) -> bool {
        self.message_loss.is_some() || self.latency_min.is_some() || self.latency_max.is_some()
    }

    /// Builds a FaultConfig from the CLI options.
    fn build_fault_config(&self) -> Option<FaultConfig> {
        if !self.has_fault_injection() {
            return None;
        }

        let mut builder = FaultConfig::builder();

        if let Some(loss_rate) = self.message_loss {
            builder = builder.message_loss_rate(loss_rate);
        }

        // Handle latency range
        match (self.latency_min, self.latency_max) {
            (Some(min), Some(max)) => {
                builder =
                    builder.latency_range(Duration::from_millis(min)..Duration::from_millis(max));
            }
            (Some(min), None) => {
                // Fixed latency if only min specified
                builder = builder.fixed_latency(Duration::from_millis(min));
            }
            (None, Some(max)) => {
                // Range from 0 to max
                builder = builder.latency_range(Duration::ZERO..Duration::from_millis(max));
            }
            (None, None) => {}
        }

        Some(builder.build())
    }
}

fn randomize_test_name() -> String {
    const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz";
    use rand::seq::IteratorRandom;
    let mut rng = rand::rng();
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

pub(crate) fn test_framework(base_config: TestConfig) -> anyhow::Result<(), Error> {
    // Both SingleProcess and Network modes are handled here.
    // SingleProcess uses Turmoil (deterministic) - runs without tokio runtime.
    // Network mode uses real networking - needs tokio runtime.

    match &base_config.command {
        TestMode::SingleProcess => {
            // Single process mode uses Turmoil, no metrics server needed
            single_process::run(&base_config)
        }
        TestMode::Network(config) => {
            // Network mode needs tokio runtime for async operations
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            rt.block_on(async {
                let disable_metrics =
                    base_config.disable_metrics || matches!(config.mode, network::Process::Peer);

                let (server, changes_recorder) = if !disable_metrics {
                    let (s, r) = start_server(&ServerConfig {
                        log_directory: base_config.execution_data.clone(),
                    })
                    .await;
                    (Some(s), r)
                } else {
                    (None, None)
                };

                let res = network::run(&base_config, config).await;

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
            })
        }
    }
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
    let seed = base_config.seed.unwrap_or(SimNetwork::DEFAULT_SEED);
    let mut sim = SimNetwork::new(
        name,
        base_config.gateways,
        base_config.nodes,
        base_config.ring_max_htl,
        base_config.rnd_if_htl_above,
        base_config.max_connections,
        base_config.min_connections,
        seed,
    )
    .await;
    if let Some(backoff) = base_config.peer_start_backoff_ms {
        sim.with_start_backoff(Duration::from_millis(backoff));
    }

    // Apply fault injection if configured
    if let Some(fault_config) = base_config.build_fault_config() {
        tracing::info!(
            "Enabling fault injection: loss={:?}, latency={:?}",
            base_config.message_loss,
            base_config.latency_min.zip(base_config.latency_max)
        );
        sim.with_fault_injection(fault_config);
    }

    Ok(sim)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config() {
        let mut nw = config_sim_network(&TestConfig {
            name: Some("test".to_string()),
            seed: None,
            gateways: 1,
            nodes: 1,
            ring_max_htl: 1,
            rnd_if_htl_above: 1,
            max_connections: 1,
            min_connections: 1,
            max_contract_number: None,
            events: 1,
            event_wait_ms: None,
            connection_wait_ms: None,
            peer_start_backoff_ms: None,
            execution_data: None,
            disable_metrics: true,
            // Fault injection options
            message_loss: None,
            latency_min: None,
            latency_max: None,
            // Verification options (defaults: convergence=60s, success_rate=95%)
            check_convergence: 60,
            min_success_rate: 0.95,
            print_summary: false,
            print_network_stats: false,
            command: TestMode::SingleProcess,
        })
        .await
        .unwrap();
        let peers = nw.build_peers();
        let keys = peers
            .iter()
            .map(|(lb, c)| (lb, format!("{}", c.key_pair.public())))
            .collect::<Vec<_>>();
        dbg!(keys);
    }

    #[test]
    fn test_build_fault_config() {
        // No fault config if nothing set
        let config = TestConfig {
            name: None,
            seed: None,
            gateways: 1,
            nodes: 1,
            ring_max_htl: 10,
            rnd_if_htl_above: 7,
            max_connections: 10,
            min_connections: 5,
            max_contract_number: None,
            events: 100,
            event_wait_ms: None,
            connection_wait_ms: None,
            peer_start_backoff_ms: None,
            execution_data: None,
            disable_metrics: true,
            message_loss: None,
            latency_min: None,
            latency_max: None,
            check_convergence: 60,
            min_success_rate: 0.95,
            print_summary: false,
            print_network_stats: false,
            command: TestMode::SingleProcess,
        };
        assert!(config.build_fault_config().is_none());
        assert!(!config.has_fault_injection());

        // With message loss
        let config_with_loss = TestConfig {
            message_loss: Some(0.1),
            ..config.clone()
        };
        assert!(config_with_loss.has_fault_injection());
        let fault_config = config_with_loss.build_fault_config().unwrap();
        assert!(fault_config.message_loss_rate > 0.0);

        // With latency range
        let config_with_latency = TestConfig {
            latency_min: Some(10),
            latency_max: Some(50),
            ..config
        };
        assert!(config_with_latency.has_fault_injection());
        assert!(config_with_latency.build_fault_config().is_some());
    }
}
