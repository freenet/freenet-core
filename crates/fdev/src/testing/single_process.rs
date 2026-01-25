use std::time::Duration;

pub(super) fn run(config: &super::TestConfig) -> anyhow::Result<(), super::Error> {
    // Create a tokio runtime for the async setup phase
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Set up the SimNetwork asynchronously
    let simulated_network = rt.block_on(super::config_sim_network(config))?;

    // Now drop the tokio runtime before calling Turmoil
    drop(rt);

    // Run the simulation using Turmoil for deterministic execution
    // This MUST be called outside of any async/tokio context
    simulated_network
        .run_fdev_test::<rand::rngs::SmallRng>(
            config.seed(),
            config.max_contract_number.unwrap_or(config.nodes * 10),
            config.events as usize,
            Duration::from_secs(300), // 5 minute simulation timeout
        )
        .map_err(super::Error::from)
}
