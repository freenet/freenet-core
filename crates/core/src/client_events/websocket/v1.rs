use super::*;

impl WebSocketProxy {
    pub fn as_router_v1(server_routing: Router) -> (Self, Router) {
        let attested_contracts = Arc::new(RwLock::new(HashMap::<AuthToken, (ContractInstanceId, ClientId)>::new()));
        Self::as_router_v1_with_attested_contracts(server_routing, attested_contracts)
    }
    
    pub fn as_router_v1_with_attested_contracts(
        server_routing: Router,
        attested_contracts: Arc<RwLock<HashMap<AuthToken, (ContractInstanceId, ClientId)>>>,
    ) -> (Self, Router) {
        let (proxy_request_sender, proxy_server_request) = mpsc::channel(PARALLELISM);

        // Using Extension instead of with_state to avoid changing the Router's type parameter
        let router = server_routing
            .route("/v1/contract/command", get(websocket_commands))
            .layer(Extension(attested_contracts))
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
