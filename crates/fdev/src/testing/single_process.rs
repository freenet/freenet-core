use std::time::Duration;

pub(super) fn run(config: &super::TestConfig) -> anyhow::Result<(), super::Error> {
    // Create runtime to build SimNetwork
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let simulated_network = rt.block_on(super::config_sim_network(config))?;

    // Drop runtime before Turmoil execution
    drop(rt);

    // Spawn a NEW thread with no tokio context for Turmoil execution.
    // This is critical because:
    // 1. Turmoil creates its own tokio runtime internally
    // 2. Tokio panics if you try to create a runtime when thread-local context exists
    // 3. Even after dropping a runtime, the thread-local context persists
    // 4. Solution: spawn a fresh thread with no tokio history
    let seed = config.seed();
    let max_contract_num = config.max_contract_number.unwrap_or(config.nodes * 10);
    let iterations = config.events as usize;
    let event_wait = Duration::from_millis(config.event_wait_ms.unwrap_or(200));

    // Calculate simulation duration from event timing:
    // total virtual time = startup(2s) + events * wait + propagation(2s) + convergence(10s) + 20% buffer
    let event_time_secs = (iterations as u64 * event_wait.as_millis() as u64) / 1000;
    let base_duration_secs = 2 + event_time_secs + 2 + 10;
    let simulation_duration = Duration::from_secs(base_duration_secs + base_duration_secs / 5);

    std::thread::spawn(move || {
        simulated_network.run_fdev_test::<rand::rngs::SmallRng>(
            seed,
            max_contract_num,
            iterations,
            simulation_duration,
            event_wait,
        )
    })
    .join()
    .map_err(|e| anyhow::anyhow!("Turmoil thread panicked: {:?}", e))??;

    Ok(())
}
