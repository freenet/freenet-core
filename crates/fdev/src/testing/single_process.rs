use std::time::Duration;

pub(super) fn run(config: &super::TestConfig) -> anyhow::Result<(), super::Error> {
    // Create runtime to build SimNetwork
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let simulated_network = rt.block_on(super::config_sim_network(config))?;

    // Drop runtime before run_simulation_direct creates its own
    drop(rt);

    let seed = config.seed();
    let max_contract_num = config.max_contract_number.unwrap_or(config.nodes * 10);
    let iterations = config.events as usize;

    // Use --event-wait-ms if provided, otherwise default to 200ms
    let event_wait = Duration::from_millis(config.event_wait_ms.unwrap_or(200));

    simulated_network.run_simulation_direct::<rand::rngs::SmallRng>(
        seed,
        max_contract_num,
        iterations,
        event_wait,
    )?;

    Ok(())
}
