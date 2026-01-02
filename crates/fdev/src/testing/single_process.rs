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

    // Check connectivity first
    tracing::info!(
        "Waiting for network to be sufficiently connected ({}ms timeout, {}%)",
        connectivity_timeout.as_millis(),
        network_connection_percent * 100.0
    );
    simulated_network
        .check_partial_connectivity(connectivity_timeout, network_connection_percent)?;

    // event_chain now borrows &mut self, so we can still access simulated_network after
    let mut stream = simulated_network.event_chain(events, None);

    // Track whether events completed normally
    let mut events_completed = false;

    let join_peer_tasks = async {
        let mut futs = futures::stream::FuturesUnordered::from_iter(join_handles);
        while let Some(join_handle) = futs.next().await {
            join_handle??;
        }
        Ok::<_, super::Error>(())
    };

    let ctrl_c = signal::ctrl_c();

    tokio::pin!(join_peer_tasks);
    tokio::pin!(ctrl_c);

    let mut test_result = Ok(());

    loop {
        tokio::select! {
            _ = &mut ctrl_c  /* SIGINT handling */ => {
                tracing::info!("Received Ctrl+C, shutting down...");
                break;
            }
            event = stream.next() => {
                match event {
                    Some(_event_id) => {
                        tokio::time::sleep(next_event_wait_time).await;
                    }
                    None => {
                        tracing::info!("All {} events generated successfully", events);
                        events_completed = true;
                        // Continue to wait for peer tasks or ctrl+c
                    }
                }
            }
            finalized = &mut join_peer_tasks => {
                match finalized {
                    Ok(_) => {
                        tracing::info!("All peer tasks finalized successfully");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Peer tasks finalized with error: {}", e);
                        test_result = Err(e);
                        break;
                    }
                }
            }
        }
    }

    // Drop the stream to release the borrow on simulated_network
    drop(stream);

    // Now we can perform verification since we still have access to simulated_network
    if test_result.is_ok() && events_completed {
        test_result = run_verification(config, &simulated_network).await;
    }

    test_result
}

/// Run post-test verification based on config options.
async fn run_verification(
    config: &super::TestConfig,
    network: &freenet::dev_tool::SimNetwork,
) -> Result<(), super::Error> {
    // Print operation summary if requested
    if config.print_summary {
        let summary = network.get_operation_summary().await;
        tracing::info!("=== Operation Summary ===");
        tracing::info!(
            "Put: {}/{} succeeded ({:.1}% success rate)",
            summary.put.succeeded,
            summary.put.completed(),
            summary.put.success_rate() * 100.0
        );
        tracing::info!(
            "Get: {}/{} succeeded ({:.1}% success rate)",
            summary.get.succeeded,
            summary.get.completed(),
            summary.get.success_rate() * 100.0
        );
        tracing::info!(
            "Subscribe: {}/{} succeeded ({:.1}% success rate)",
            summary.subscribe.succeeded,
            summary.subscribe.completed(),
            summary.subscribe.success_rate() * 100.0
        );
        tracing::info!(
            "Update: {}/{} succeeded ({:.1}% success rate)",
            summary.update.succeeded,
            summary.update.completed(),
            summary.update.success_rate() * 100.0
        );
        tracing::info!(
            "Overall: {}/{} succeeded ({:.1}% success rate), {} timeouts",
            summary.total_succeeded(),
            summary.total_completed(),
            summary.overall_success_rate() * 100.0,
            summary.timeouts
        );
    }

    // Print network stats if requested (requires fault injection)
    if config.print_network_stats {
        if let Some(stats) = network.get_network_stats() {
            tracing::info!("=== Network Statistics ===");
            tracing::info!(
                "Messages: {} sent, {} delivered, {} dropped ({:.1}% loss)",
                stats.messages_sent,
                stats.messages_delivered,
                stats.total_dropped(),
                stats.loss_ratio() * 100.0
            );
            if stats.total_dropped() > 0 {
                tracing::info!(
                    "Drop reasons: {} loss, {} partition, {} crash",
                    stats.messages_dropped_loss,
                    stats.messages_dropped_partition,
                    stats.messages_dropped_crash
                );
            }
            if stats.messages_delayed_delivered > 0 {
                tracing::info!(
                    "Latency: {} delayed messages, avg {:?}",
                    stats.messages_delayed_delivered,
                    stats.average_latency()
                );
            }
        } else {
            tracing::warn!("Network stats not available (fault injection not configured)");
        }
    }

    // Check minimum success rate
    if let Some(min_rate) = config.min_success_rate {
        let summary = network.get_operation_summary().await;
        let actual_rate = summary.overall_success_rate();
        if actual_rate < min_rate {
            let msg = format!(
                "Success rate {:.1}% is below minimum threshold {:.1}%",
                actual_rate * 100.0,
                min_rate * 100.0
            );
            tracing::error!("{}", msg);
            return Err(anyhow::anyhow!(msg));
        }
        tracing::info!(
            "Success rate check passed: {:.1}% >= {:.1}%",
            actual_rate * 100.0,
            min_rate * 100.0
        );
    }

    // Check convergence
    if let Some(timeout_secs) = config.check_convergence {
        let timeout = Duration::from_secs(timeout_secs);
        let poll_interval = Duration::from_millis(500);

        tracing::info!("Checking convergence (timeout: {}s)...", timeout_secs);

        match network.await_convergence(timeout, poll_interval, 1).await {
            Ok(result) => {
                tracing::info!(
                    "Convergence check passed: {} contracts converged, {} diverged",
                    result.converged.len(),
                    result.diverged.len()
                );
            }
            Err(result) => {
                let msg = format!(
                    "Convergence check failed: {} contracts converged, {} still diverged",
                    result.converged.len(),
                    result.diverged.len()
                );
                tracing::error!("{}", msg);

                // Log details of diverged contracts
                for diverged in &result.diverged {
                    tracing::error!(
                        "  Contract {} has {} different states across {} peers",
                        diverged.contract_key,
                        diverged.unique_state_count(),
                        diverged.peer_states.len()
                    );
                }

                return Err(anyhow::anyhow!(msg));
            }
        }
    }

    Ok(())
}
