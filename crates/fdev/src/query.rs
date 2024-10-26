use freenet_stdlib::client_api::{ConnectedPeers, HostResponse, QueryResponse};
use prettytable::{Cell, Row, Table};

use crate::{
    commands::{execute_command, start_api_client},
    config::BaseConfig,
};

pub async fn query(base_cfg: BaseConfig) -> anyhow::Result<()> {
    let mut client = start_api_client(base_cfg).await?;
    tracing::info!("Querying for connected peers");
    execute_command(
        freenet_stdlib::client_api::ClientRequest::NodeQueries(ConnectedPeers {}),
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

    table.printstd();

    Ok(())
}
