use std::time::Duration;

use freenet::dev_tool::ChurnConfig;

pub(super) fn run(config: &super::TestConfig) -> anyhow::Result<(), super::Error> {
    // Create runtime to build SimNetwork
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let mut simulated_network = rt.block_on(super::config_sim_network(config))?;

    // Configure churn if --churn-rate is set
    if let Some(crash_probability) = config.churn_rate {
        tracing::info!(
            crash_probability,
            recovery_ms = config.churn_recovery_delay_ms,
            permanent_rate = config.churn_permanent_rate,
            tick_ms = config.churn_tick_ms,
            "Enabling node churn"
        );
        simulated_network.with_churn(ChurnConfig {
            crash_probability,
            tick_interval: Duration::from_millis(config.churn_tick_ms),
            recovery_delay: Duration::from_millis(config.churn_recovery_delay_ms),
            max_simultaneous_crashes: None,
            permanent_crash_rate: config.churn_permanent_rate,
            warmup_delay: Duration::from_secs(5),
        });
    }

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
