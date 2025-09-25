use freenet_stdlib::client_api::{HostResponse, NodeQuery, QueryResponse};
use prettytable::{Cell, Row, Table};

use crate::{
    commands::{execute_command, start_api_client},
    config::BaseConfig,
};

pub async fn query(base_cfg: BaseConfig) -> anyhow::Result<()> {
    let mut client = start_api_client(base_cfg).await?;

    // Query for connected peers
    tracing::info!("Querying for connected peers");
    execute_command(
        freenet_stdlib::client_api::ClientRequest::NodeQueries(NodeQuery::ConnectedPeers),
        &mut client,
    )
    .await?;
    let HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }) =
        client.recv().await?
    else {
        anyhow::bail!("Unexpected response from the host");
    };

    let mut table = Table::new();

    table.add_row(Row::new(vec![
        Cell::new("Identifier"),
        Cell::new("SocketAddress"),
    ]));

    for (identifier, socketaddress) in peers {
        table.add_row(Row::new(vec![
            Cell::new(&identifier.to_string()),
            Cell::new(&socketaddress.to_string()),
        ]));
    }

    println!("\n=== Connected Peers ===");
    table.printstd();

    // Query for subscription info
    tracing::info!("Querying for subscription info");
    execute_command(
        freenet_stdlib::client_api::ClientRequest::NodeQueries(NodeQuery::SubscriptionInfo),
        &mut client,
    )
    .await?;

    let HostResponse::QueryResponse(QueryResponse::NetworkDebug(info)) = client.recv().await?
    else {
        anyhow::bail!("Unexpected response from the host");
    };

    // Display application subscription info
    println!("\n=== Application Subscriptions (WebSocket Clients) ===");
    if !info.subscriptions.is_empty() {
        let mut sub_table = Table::new();
        sub_table.add_row(Row::new(vec![
            Cell::new("Contract Key"),
            Cell::new("Client ID"),
        ]));

        for sub in info.subscriptions {
            sub_table.add_row(Row::new(vec![
                Cell::new(&format!("{:.8}...", sub.contract_key)),
                Cell::new(&sub.client_id.to_string()),
            ]));
        }
        sub_table.printstd();
    } else {
        println!("No application subscriptions");
    }

    // Query for proximity cache info
    tracing::info!("Querying for proximity cache info");
    execute_command(
        freenet_stdlib::client_api::ClientRequest::NodeQueries(NodeQuery::ProximityCacheInfo),
        &mut client,
    )
    .await?;

    let HostResponse::QueryResponse(QueryResponse::ProximityCache(proximity_info)) =
        client.recv().await?
    else {
        anyhow::bail!("Unexpected response from the host");
    };

    // Display proximity cache information
    println!("\n=== Proximity Cache Information ===");

    if !proximity_info.my_cache.is_empty() {
        println!("\nContracts cached locally:");
        let mut cache_table = Table::new();
        cache_table.add_row(Row::new(vec![
            Cell::new("Contract Key"),
            Cell::new("Cache Hash"),
            Cell::new("Cached Since"),
        ]));

        for entry in proximity_info.my_cache {
            cache_table.add_row(Row::new(vec![
                Cell::new(&format!("{:.16}...", entry.contract_key)),
                Cell::new(&format!("{:08x}", entry.cache_hash)),
                Cell::new(&format!("{}", entry.cached_since)),
            ]));
        }
        cache_table.printstd();
    } else {
        println!("No contracts cached locally");
    }

    if !proximity_info.neighbor_caches.is_empty() {
        println!("\nNeighbor cache knowledge:");
        for neighbor in proximity_info.neighbor_caches {
            println!(
                "  {} caches {} contracts (last update: {})",
                neighbor.peer_id,
                neighbor.known_contracts.len(),
                neighbor.last_update
            );
        }
    } else {
        println!("No neighbor cache information available");
    }

    println!("\nProximity propagation statistics:");
    println!(
        "  Cache announces sent:       {}",
        proximity_info.stats.cache_announces_sent
    );
    println!(
        "  Cache announces received:   {}",
        proximity_info.stats.cache_announces_received
    );
    println!(
        "  Updates via proximity:      {}",
        proximity_info.stats.updates_via_proximity
    );
    println!(
        "  Updates via subscription:   {}",
        proximity_info.stats.updates_via_subscription
    );
    println!(
        "  False positive forwards:    {}",
        proximity_info.stats.false_positive_forwards
    );
    println!(
        "  Avg neighbor cache size:    {:.2}",
        proximity_info.stats.avg_neighbor_cache_size
    );

    Ok(())
}
