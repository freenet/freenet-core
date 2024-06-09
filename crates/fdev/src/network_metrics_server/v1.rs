use super::*;

pub(super) async fn run_server(
    barrier: Arc<tokio::sync::Barrier>,
    changes: tokio::sync::broadcast::Sender<Change>,
) -> anyhow::Result<()> {
    const DEFAULT_PORT: u16 = 55010;

    let port = std::env::var("FDEV_NETWORK_METRICS_SERVER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let router = Router::new()
        .route("/v1", get(home))
        .route("/v1/push-stats/", get(push_stats))
        .route("/v1/pull-stats/peer-changes/", get(pull_peer_changes))
        .with_state(Arc::new(ServerState {
            changes,
            peer_data: DashMap::new(),
            transactions_data: DashMap::new(),
        }));

    tracing::info!("Starting metrics server on port {port}");
    barrier.wait().await;
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn home() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("Location", "/v1/pull-stats/")
        .body(Body::empty())
        .expect("should be valid response")
        .into_response()
}
