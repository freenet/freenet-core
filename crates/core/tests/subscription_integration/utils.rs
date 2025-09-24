use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use freenet_stdlib::prelude::ContractKey;

use super::MultiNodeTestHarness;

#[derive(Debug, Clone)]
pub enum FailureType {
    NodeCrash(String),
    NetworkPartition(Vec<String>),
    SlowNetwork(Duration),
    MessageDrop(f64), // Probability of dropping messages
}

pub async fn wait_for_propagation(
    harness: &MultiNodeTestHarness,
    contract: ContractKey,
    expected_nodes: usize,
    timeout: Duration,
) -> Result<()> {
    harness.wait_for_propagation(contract, expected_nodes, timeout).await
}

pub async fn inject_network_failure(
    harness: &mut MultiNodeTestHarness,
    failure_type: FailureType,
) -> Result<()> {
    match failure_type {
        FailureType::NodeCrash(peer_id) => {
            // Find and kill the node process
            for node in &mut harness.nodes {
                if node.id == peer_id {
                    if let Some(mut process) = node.process.take() {
                        process.kill()?;
                        tracing::info!("Killed node {}", peer_id);
                    }
                    break;
                }
            }
        }

        FailureType::NetworkPartition(peers) => {
            // Block network connections between specified peers
            // This would require iptables or similar network control
            tracing::warn!("Network partition injection not yet implemented");
        }

        FailureType::SlowNetwork(latency) => {
            // Add network latency
            // This would require tc (traffic control) or similar
            tracing::warn!("Network latency injection not yet implemented");
        }

        FailureType::MessageDrop(probability) => {
            // Configure message dropping
            tracing::warn!("Message drop injection not yet implemented");
        }
    }

    Ok(())
}

pub fn riverctl_available() -> bool {
    Command::new("riverctl")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

pub async fn wait_for_condition<F>(
    mut condition: F,
    timeout: Duration,
    check_interval: Duration,
) -> Result<()>
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if condition() {
            return Ok(());
        }
        tokio::time::sleep(check_interval).await;
    }

    Err(anyhow::anyhow!("Condition not met within timeout"))
}

pub fn create_test_contract() -> (freenet_stdlib::prelude::ContractCode<'static>, freenet_stdlib::prelude::Parameters<'static>, freenet_stdlib::prelude::WrappedState) {
    // Create a simple test contract
    let code = vec![0u8; 100]; // Placeholder WASM code
    let contract_code = freenet_stdlib::prelude::ContractCode::from(code);

    let params = freenet_stdlib::prelude::Parameters::from(vec![]);

    let initial_state = freenet_stdlib::prelude::WrappedState::new(
        serde_json::to_vec(&serde_json::json!({
            "counter": 0,
            "subscribers": []
        })).unwrap()
    );

    (contract_code, params, initial_state)
}

pub struct TestAssertions;

impl TestAssertions {
    pub async fn assert_subscription_received(
        harness: &MultiNodeTestHarness,
        node_index: usize,
        transaction_id: freenet_stdlib::prelude::Transaction,
    ) -> Result<()> {
        // Check logs for subscription confirmation
        let collector = harness.log_collector.lock().await;
        let trace = collector.trace_transaction(transaction_id).await;

        if trace.events.is_empty() {
            return Err(anyhow::anyhow!("No events found for transaction"));
        }

        // Check if the target node received the subscription response
        let node_id = &harness.nodes[node_index].id;
        let received = trace.events.iter().any(|e| {
            e.node_id == *node_id && e.message.contains("subscription confirmed")
        });

        if !received {
            return Err(anyhow::anyhow!("Node {} did not receive subscription response", node_id));
        }

        Ok(())
    }

    pub async fn assert_subscription_active(
        harness: &MultiNodeTestHarness,
        node_index: usize,
        contract_key: ContractKey,
    ) -> Result<()> {
        // Check that the node is actively subscribed to the contract
        let monitor = harness.network_monitor.lock().await;

        let node_id = &harness.nodes[node_index].id;
        let subscribed = monitor.contract_locations
            .get(&contract_key)
            .map(|locs| locs.contains(node_id))
            .unwrap_or(false);

        if !subscribed {
            return Err(anyhow::anyhow!("Node {} is not subscribed to contract", node_id));
        }

        Ok(())
    }

    pub async fn assert_peer_sequence_attempted(
        harness: &MultiNodeTestHarness,
        transaction_id: freenet_stdlib::prelude::Transaction,
        expected_peers: &[String],
    ) -> Result<()> {
        let collector = harness.log_collector.lock().await;
        let trace = collector.trace_transaction(transaction_id).await;

        let mut attempted_peers = Vec::new();
        for event in &trace.events {
            if event.message.contains("attempting connection to") {
                // Parse peer from message
                // This is simplified - real implementation would parse properly
                attempted_peers.push(event.node_id.clone());
            }
        }

        if attempted_peers != expected_peers {
            return Err(anyhow::anyhow!(
                "Expected peer sequence {:?}, but got {:?}",
                expected_peers,
                attempted_peers
            ));
        }

        Ok(())
    }

    pub async fn verify_transaction_correlation(
        harness: &MultiNodeTestHarness,
        transaction_id: freenet_stdlib::prelude::Transaction,
    ) -> Result<()> {
        let collector = harness.log_collector.lock().await;
        let trace = collector.trace_transaction(transaction_id).await;

        // Verify that the transaction flows through the network correctly
        if trace.nodes_involved.len() < 2 {
            return Err(anyhow::anyhow!(
                "Transaction should involve at least 2 nodes, but only found {}",
                trace.nodes_involved.len()
            ));
        }

        // Check for proper request/response pattern
        let has_request = trace.events.iter().any(|e| e.message.contains("request"));
        let has_response = trace.events.iter().any(|e| e.message.contains("response"));

        if !has_request || !has_response {
            return Err(anyhow::anyhow!("Transaction missing request/response pattern"));
        }

        Ok(())
    }
}

pub async fn generate_test_report(harness: &MultiNodeTestHarness) -> String {
    let mut report = String::new();

    report.push_str("=== Test Execution Report ===\n\n");

    // Network statistics
    let monitor = harness.network_monitor.lock().await;
    let stats = monitor.get_network_stats();
    report.push_str(&format!("{}\n", stats));

    // Log analysis
    let collector = harness.log_collector.lock().await;
    let log_report = collector.generate_report().await;
    report.push_str(&log_report);

    // Network topology
    report.push_str("\n");
    report.push_str(&monitor.visualize_topology());

    // Contract distribution
    report.push_str("\n");
    report.push_str(&monitor.get_contract_distribution());

    report
}