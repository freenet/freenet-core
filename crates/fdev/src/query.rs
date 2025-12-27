use std::time::Duration;

use freenet_stdlib::client_api::{HostResponse, NodeQuery, QueryResponse};
use prettytable::{Cell, Row, Table};

use crate::{
    commands::{close_api_client, execute_command, start_api_client},
    config::BaseConfig,
};

/// Timeout for waiting for query responses.
/// Queries should be fast, but we use a generous timeout to handle slow networks.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn query(base_cfg: BaseConfig) -> anyhow::Result<()> {
    let mut client = start_api_client(base_cfg).await?;

    let result = query_inner(&mut client).await;

    // Always gracefully close the WebSocket connection, even on timeout/error
    close_api_client(&mut client).await;

    result
}

async fn query_inner(client: &mut freenet_stdlib::client_api::WebApi) -> anyhow::Result<()> {
    // Query for connected peers
    tracing::info!("Querying for connected peers");
    execute_command(
        freenet_stdlib::client_api::ClientRequest::NodeQueries(NodeQuery::ConnectedPeers),
        client,
    )
    .await?;

    let response = tokio::time::timeout(QUERY_TIMEOUT, client.recv())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Timeout waiting for query response after {} seconds",
                QUERY_TIMEOUT.as_secs()
            )
        })?
        .map_err(|e| anyhow::anyhow!("Failed to receive response: {e}"))?;

    let HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }) = response else {
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
        client,
    )
    .await?;

    let response = tokio::time::timeout(QUERY_TIMEOUT, client.recv())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Timeout waiting for query response after {} seconds",
                QUERY_TIMEOUT.as_secs()
            )
        })?
        .map_err(|e| anyhow::anyhow!("Failed to receive response: {e}"))?;

    let HostResponse::QueryResponse(QueryResponse::NetworkDebug(info)) = response else {
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

    Ok(())
}
