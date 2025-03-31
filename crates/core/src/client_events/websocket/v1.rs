use super::*;

impl WebSocketProxy {
    pub fn as_router_v1(server_routing: Router) -> (Self, Router) {
        let (proxy_request_sender, proxy_server_request) = mpsc::channel(PARALLELISM);

        let attested_contracts = Arc::new(RwLock::new(HashMap::new()));
        let router = server_routing
            .with_state(attested_contracts)
            .route("/v1/contract/command", get(websocket_commands))
            .layer(Extension(WebSocketRequest(proxy_request_sender)))
            .layer(axum::middleware::from_fn(connection_info));
        
        (
            WebSocketProxy {
                proxy_server_request,
                response_channels: HashMap::new(),
            },
            router,
        )
    }
}
