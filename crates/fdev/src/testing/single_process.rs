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
    // Virtual time to advance per event (for message delivery simulation)
    // This is independent of real-time pacing
    let virtual_time_per_event = config
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
        .check_partial_connectivity(connectivity_timeout, network_connection_percent)
        .await?;

    // Post-connectivity stabilization: advance VirtualTime to let all nodes fully
    // establish their ring positions before generating events. The connectivity check
    // only ensures a percentage of nodes have at least one connection, but nodes need
    // additional time to build their full connection set for reliable routing.
    //
    // This is especially important when connectivity_percent < 1.0, as the remaining
    // nodes may still be joining. Without this stabilization, operations targeting
    // nodes that haven't fully joined will fail.
    let stabilization_time = Duration::from_secs(60);
    tracing::info!(
        "Network connectivity check passed, stabilizing for {}s virtual time",
        stabilization_time.as_secs()
    );
    // Advance 60s of virtual time in chunks, with minimal real-time delays
    // to allow tokio tasks to process messages
    for _ in 0..600 {
        simulated_network.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
        // Minimal real-time sleep just for scheduler - 1ms instead of 10ms
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // event_chain now borrows &mut self, so we can still access simulated_network after
    // Use Option so we can drop the stream when events complete, signaling peers to disconnect
    let mut stream = Some(simulated_network.event_chain(events, None));

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
            event = async {
                match stream.as_mut() {
                    Some(s) => s.next().await,
                    None => std::future::pending().await, // Never resolves once stream is dropped
                }
            } => {
                match event {
                    Some(_event_id) => {
                        // Advance VirtualTime to allow message delivery in the simulation
                        // This replaces real-time sleep with simulated time progression
                        simulated_network.advance_time(virtual_time_per_event);
                        // Yield to let tokio tasks process delivered messages
                        tokio::task::yield_now().await;
                        // Minimal real-time sleep (1ms) to prevent busy-looping
                        // and allow the scheduler to run other tasks
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    None => {
                        tracing::info!("All {} events generated successfully", events);
                        events_completed = true;
                        // Drop the stream to release the watch::Sender, which signals
                        // peers to disconnect. Without this, peers wait forever for
                        // more events and never exit their event loops.
                        stream = None;
                        // Continue to wait for peer tasks to finalize
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

    // Drop the stream to release the borrow on simulated_network (if not already dropped)
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
            summary.total_completed() + summary.timeouts,
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

    // Check minimum success rate (enabled by default, set to 0.0 to disable)
    let min_rate = config.min_success_rate;
    if min_rate > 0.0 {
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

    // Check convergence (enabled by default, set to 0 to disable)
    let timeout_secs = config.check_convergence;
    if timeout_secs > 0 {
        let timeout = Duration::from_secs(timeout_secs);
        // Reduced from 500ms to 100ms for faster simulation completion
        let poll_interval = Duration::from_millis(100);

        // We only require at least 1 contract to be replicated to verify convergence.
        // The number of contracts created depends on the random event generation,
        // not an arbitrary formula. The convergence check verifies that whatever
        // contracts exist have converged, not that a specific number were created.
        let min_expected_contracts = 1;

        tracing::info!(
            "Checking convergence (timeout: {}s, requiring at least {} replicated contract)...",
            timeout_secs,
            min_expected_contracts
        );

        match network
            .await_convergence(timeout, poll_interval, min_expected_contracts)
            .await
        {
            Ok(result) => {
                // Additional validation: check minimum replica count
                // For a proper convergence test, contracts should be replicated to multiple peers
                const MIN_REPLICA_COUNT: usize = 2;
                let low_replica_contracts: Vec<_> = result
                    .converged
                    .iter()
                    .filter(|c| c.replica_count < MIN_REPLICA_COUNT)
                    .collect();

                if !low_replica_contracts.is_empty() {
                    tracing::warn!(
                        "{} contracts have fewer than {} replicas (may indicate replication issues)",
                        low_replica_contracts.len(),
                        MIN_REPLICA_COUNT
                    );
                }

                // Log replica distribution for visibility
                let avg_replicas: f64 = if result.converged.is_empty() {
                    0.0
                } else {
                    result
                        .converged
                        .iter()
                        .map(|c| c.replica_count)
                        .sum::<usize>() as f64
                        / result.converged.len() as f64
                };

                tracing::info!(
                    "Convergence check passed: {} contracts converged (avg {:.1} replicas/contract), {} diverged",
                    result.converged.len(),
                    avg_replicas,
                    result.diverged.len()
                );
            }
            Err(result) => {
                let msg = format!(
                    "Convergence check failed: {} contracts converged, {} still diverged. \
                     Eventual consistency requires 100% convergence.",
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
