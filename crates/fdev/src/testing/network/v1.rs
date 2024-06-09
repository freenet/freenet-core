use super::*;

pub async fn start_server_v1(supervisor: Arc<Supervisor>) -> Result<(), NetworkSimulationError> {
    let (startup_sender, startup_receiver) = oneshot::channel();
    let peers_config = supervisor.peers_config.clone();

    let cloned_supervisor = supervisor.clone();

    let router = Router::new()
        .route("/v1/ws", get(|ws| ws_handler(ws, cloned_supervisor)))
        .route(
            "/v1/config/:peer_id",
            get(|path: Path<String>| config_handler(peers_config, path)),
        );

    let socket = SocketAddr::from(([0, 0, 0, 0], 3000));

    tokio::spawn(async move {
        tracing::info!("Supervisor running on {}", socket);
        let listener = tokio::net::TcpListener::bind(socket).await.map_err(|_| {
            NetworkSimulationError::ServerStartFailure("Failed to bind TCP listener".into())
        })?;

        if startup_sender.send(()).is_err() {
            tracing::error!("Failed to send startup signal");
            return Err(NetworkSimulationError::ServerStartFailure(
                "Failed to send startup signal".into(),
            ));
        }

        axum::serve(listener, router)
            .await
            .map_err(|e| NetworkSimulationError::ServerStartFailure(format!("Server error: {}", e)))
    });

    startup_receiver
        .await
        .map_err(|_| NetworkSimulationError::ServerStartFailure("Server startup failed".into()))?;

    tracing::info!("Server started successfully");
    Ok(())
}
