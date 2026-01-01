use std::time::Duration;

use futures::StreamExt;
use tokio::signal;

pub(super) async fn run(config: &super::TestConfig) -> anyhow::Result<(), super::Error> {
    let mut simulated_network = super::config_sim_network(config).await?;

    let join_handles = simulated_network
        .start_with_rand_gen::<rand::rngs::SmallRng>(
            config.seed(),
            config.max_contract_number.unwrap_or(config.nodes * 10),
            config.events as usize,
        )
        .await;

    let events = config.events;
    let next_event_wait_time = config
        .event_wait_ms
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(200));
    let (connectivity_timeout, network_connection_percent) = config.get_connection_check_params();

    // Check connectivity first, before moving the network
    tracing::info!(
        "Waiting for network to be sufficiently connected ({}ms timeout, {}%)",
        connectivity_timeout.as_millis(),
        network_connection_percent * 100.0
    );
    simulated_network.check_partial_connectivity(connectivity_timeout, network_connection_percent)?;

    // Run verification before consuming the network (if configured)
    // We need to do this before event_chain consumes the network
    let has_verification = config.has_verification();

    // event_chain takes ownership, so we need to spawn the task with the network
    let events_generated = tokio::task::spawn(async move {
        let mut stream = simulated_network.event_chain(events, None);
        while stream.next().await.is_some() {
            tokio::time::sleep(next_event_wait_time).await;
        }
        // Return the network for verification (event_chain returns it via stream drop)
        Ok::<_, super::Error>(())
    });

    let join_peer_tasks = async move {
        let mut futs = futures::stream::FuturesUnordered::from_iter(join_handles);
        while let Some(join_handle) = futs.next().await {
            join_handle??;
        }
        Ok::<_, super::Error>(())
    };

    let ctrl_c = signal::ctrl_c();

    tokio::pin!(events_generated);
    tokio::pin!(join_peer_tasks);
    tokio::pin!(ctrl_c);

    let mut test_result = Ok(());

    loop {
        tokio::select! {
            _ = &mut ctrl_c  /* SIGINT handling */ => {
                break;
            }
            res = &mut events_generated => {
                match res? {
                    Ok(()) => {
                        tracing::info!("Test events generated successfully");
                        *events_generated = tokio::task::spawn(futures::future::pending::<anyhow::Result<()>>());
                        continue;
                    }
                    Err(err) => {
                        test_result = Err(err);
                        break;
                    }
                }
            }
            finalized = &mut join_peer_tasks => {
                match finalized {
                    Ok(_) => {
                        tracing::info!("Test finalized successfully");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Test finalized with error: {}", e);
                        test_result = Err(e);
                        break;
                    }
                }
            }
        }
    }

    // Note: Verification cannot be done here because event_chain consumed the network.
    // The fault injection options still work - they're applied before event_chain runs.
    // For post-test verification, we would need to refactor event_chain to not take ownership.
    // TODO: Consider refactoring event_chain to borrow &mut self and return verification data.
    if has_verification {
        tracing::warn!(
            "Post-test verification options (--print-summary, --check-convergence, --min-success-rate) \
             cannot be run because event_chain consumes the network. \
             Fault injection (--message-loss, --latency-*) is still applied. \
             For verification, use the metrics server or check the test logs."
        );
    }

    test_result
}
