use freenet_stdlib::client_api::{HostResponse, NodeDiagnosticsConfig, NodeQuery, QueryResponse};
use prettytable::{Cell, Row, Table};

use crate::{
    commands::{execute_command, start_api_client},
    config::BaseConfig,
};

pub async fn diagnostics(base_cfg: BaseConfig, contract_keys: Vec<String>) -> anyhow::Result<()> {
    let mut client = start_api_client(base_cfg).await?;

    tracing::info!("Querying node diagnostics");

    // Parse contract keys
    let parsed_keys: Result<Vec<freenet_stdlib::prelude::ContractKey>, anyhow::Error> =
        contract_keys
            .iter()
            .map(|key| {
                let contract_id =
                    freenet_stdlib::prelude::ContractInstanceId::try_from(key.clone())
                        .map_err(|e| anyhow::anyhow!("Invalid contract key '{}': {}", key, e))?;
                Ok(freenet_stdlib::prelude::ContractKey::from(contract_id))
            })
            .collect();

    let contract_keys = parsed_keys?;

    // Full diagnostics configuration - get everything
    let config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: true,
        include_subscriptions: true,
        contract_keys,
        include_system_metrics: true,
        include_detailed_peer_info: true,
        include_subscriber_peer_ids: true,
    };

    execute_command(
        freenet_stdlib::client_api::ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
            config,
        }),
        &mut client,
    )
    .await?;

    let HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(response)) =
        client.recv().await?
    else {
        anyhow::bail!("Unexpected response from the host");
    };

    // Display the results
    println!("=== NODE DIAGNOSTICS ===\n");

    // Node information
    if let Some(node_info) = &response.node_info {
        println!("📍 Node Information:");
        println!("  Peer ID: {}", node_info.peer_id);
        println!(
            "  Type: {}",
            if node_info.is_gateway {
                "gateway"
            } else {
                "regular"
            }
        );
        if let Some(listening_address) = &node_info.listening_address {
            println!("  Listening Address: {}", listening_address);
        }
        if let Some(location) = &node_info.location {
            println!("  Location: {}", location);
        }
        println!();
    }

    // Network information
    if let Some(network_info) = &response.network_info {
        println!("🔗 Network Information:");
        println!("  Active connections: {}", network_info.active_connections);

        if !network_info.connected_peers.is_empty() {
            let mut table = Table::new();
            table.add_row(Row::new(vec![Cell::new("Peer ID"), Cell::new("Address")]));

            for (peer_id, address) in &network_info.connected_peers {
                table.add_row(Row::new(vec![Cell::new(peer_id), Cell::new(address)]));
            }

            println!("  Connected peers:");
            table.printstd();
        } else {
            println!("  Connected peers: None");
        }
        println!();
    }

    // Subscriptions
    if !response.subscriptions.is_empty() {
        println!("📋 Subscriptions:");
        let mut table = Table::new();
        table.add_row(Row::new(vec![
            Cell::new("Contract Key"),
            Cell::new("Client ID"),
        ]));

        for sub in &response.subscriptions {
            table.add_row(Row::new(vec![
                Cell::new(&sub.contract_key.to_string()),
                Cell::new(&sub.client_id.to_string()),
            ]));
        }

        table.printstd();
        println!();
    }

    // Contract states
    if !response.contract_states.is_empty() {
        println!("📄 Contract States:");
        let mut table = Table::new();
        table.add_row(Row::new(vec![
            Cell::new("Contract Key"),
            Cell::new("Subscribers"),
            Cell::new("Subscriber Peer IDs"),
        ]));

        for (key, state) in &response.contract_states {
            let subscriber_ids = if state.subscriber_peer_ids.is_empty() {
                "None".to_string()
            } else {
                state.subscriber_peer_ids.join(", ")
            };

            table.add_row(Row::new(vec![
                Cell::new(&key.to_string()),
                Cell::new(&state.subscribers.to_string()),
                Cell::new(&subscriber_ids),
            ]));
        }

        table.printstd();
        println!();
    }

    // System metrics
    if let Some(metrics) = &response.system_metrics {
        println!("📊 System Metrics:");
        println!("  Active connections: {}", metrics.active_connections);
        println!("  Seeding contracts: {}", metrics.seeding_contracts);
        println!();
    }

    Ok(())
}
