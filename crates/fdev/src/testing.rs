use anyhow::Error;

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
    #[arg(long, default_value_t = usize::MAX)]
    events: usize,
    #[clap(subcommand)]
    /// Execution mode for the test.
    pub command: TestMode,
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
    /// Runs multiple simulated nodes in multiple processes.
    MultiProcess,
    /// Runs multiple simulated nodes in multiple processes and multiple machines.
    Network,
}

pub(crate) async fn test_framework(base_config: TestConfig) -> Result<(), Error> {
    match &base_config.command {
        TestMode::SingleProcess => {
            single_process::run(&base_config).await?;
        }
        TestMode::MultiProcess => {
            todo!()
        }
        TestMode::Network => {
            todo!()
        }
    }
    Ok(())
}

mod single_process {
    use freenet::network_sim::SimNetwork;
    use futures::StreamExt;
    use rand::RngCore;
    use tokio::signal;

    pub(super) async fn run(base_config: &super::TestConfig) -> Result<(), super::Error> {
        let name = &base_config
            .name
            .as_ref()
            .cloned()
            .unwrap_or_else(super::randomize_test_name);
        let mut simulated_network = SimNetwork::new(
            name,
            base_config.gateways,
            base_config.nodes,
            base_config.ring_max_htl,
            base_config.rnd_if_htl_above,
            base_config.max_connections,
            base_config.min_connections,
        )
        .await;

        let join_handles = simulated_network
            .start_with_rand_gen::<fastrand::Rng>(
                base_config
                    .seed
                    .unwrap_or_else(|| rand::rngs::OsRng.next_u64()),
                base_config
                    .max_contract_number
                    .unwrap_or(base_config.nodes * 10),
                base_config.events,
            )
            .await;

        let join_tasks = async move {
            let mut futs = futures::stream::FuturesUnordered::from_iter(join_handles);
            while let Some(join_handle) = futs.next().await {
                join_handle??;
            }
            Ok::<_, super::Error>(())
        };

        // Wait for a signal to gracefully shut down the program
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = signal::ctrl_c() /* SIGINT handling */ => {}
                _ = sigterm.recv() => {}
                finalized = join_tasks => {
                    match finalized {
                        Ok(_) => println!("Test finalized successfully"),
                        Err(e) => {
                            println!("Test finalized with error: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            unimplemented!("Windows support is not implemented");
        }

        println!("Shutting down test...");

        Ok(())
    }
}
