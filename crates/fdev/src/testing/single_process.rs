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
    let events_generated = tokio::task::spawn(async move {
        tracing::info!(
            "Waiting for network to be sufficiently connected ({}ms timeout, {}%)",
            connectivity_timeout.as_millis(),
            network_connection_percent * 100.0
        );
        simulated_network
            .check_partial_connectivity(connectivity_timeout, network_connection_percent)?;
        let mut stream = simulated_network.event_chain(events, None);
        while stream.next().await.is_some() {
            tokio::time::sleep(next_event_wait_time).await;
        }
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
                        return Err(err);
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
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok(())
}
