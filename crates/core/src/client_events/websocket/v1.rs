use super::*;

impl WebSocketProxy {
    pub fn as_router_v1(server_routing: Router) -> (Self, Router) {
        let (proxy_request_sender, request_to_server) = mpsc::channel(PARALLELISM);
        let attested_contracts = Arc::new(RwLock::new(HashMap::new()));

        let router = server_routing
            .route("/v1/contract/command", get(websocket_commands))
            .layer(Extension(WebSocketRequest(proxy_request_sender)))
            .layer(Extension(AttestedContracts(attested_contracts.clone())))
            .layer(axum::middleware::from_fn(connection_info));
            
        (
            Self {
                proxy_server_request: request_to_server,
                response_channels: HashMap::new(),
            },
            router,
        )
    }
}
