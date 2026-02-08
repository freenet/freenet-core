use super::*;

impl HttpGateway {
    /// Returns the uninitialized axum router with a provided attested_contracts map.
    pub fn create_router_v1_with_attested_contracts(
        socket: &SocketAddr,
        attested_contracts: AttestedContractMap,
    ) -> (Self, Router) {
        let localhost = match socket.ip() {
            IpAddr::V4(ip) if ip.is_loopback() || ip.is_unspecified() => true,
            IpAddr::V6(ip) if ip.is_loopback() || ip.is_unspecified() => true,
            _ => false,
        };
        let contract_web_path = std::env::temp_dir().join("freenet").join("webs");
        std::fs::create_dir_all(contract_web_path).unwrap();

        let (proxy_request_sender, request_to_server) = mpsc::channel(1);

        let config = Config { localhost };

        let router = Router::new()
            .route("/v1", get(home))
            .route("/v1/contract/web/{key}/", get(web_home_v1))
            .route("/v2", get(home))
            .route("/v2/contract/web/{key}/", get(web_home_v2))
            .with_state(config)
            .route("/v1/contract/web/{key}/{*path}", get(web_subpages_v1))
            .route("/v2/contract/web/{key}/{*path}", get(web_subpages_v2))
            .layer(Extension(attested_contracts.clone()))
            .layer(Extension(HttpGatewayRequest(proxy_request_sender)));

        (
            Self {
                proxy_server_request: request_to_server,
                attested_contracts: attested_contracts.clone(),
                response_channels: HashMap::new(),
            },
            router,
        )
    }
}

async fn web_home_v1(
    key: Path<String>,
    rs: Extension<HttpGatewayRequest>,
    config: axum::extract::State<Config>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(key, rs, config, "v1").await
}

async fn web_home_v2(
    key: Path<String>,
    rs: Extension<HttpGatewayRequest>,
    config: axum::extract::State<Config>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(key, rs, config, "v2").await
}

async fn web_home(
    Path(key): Path<String>,
    Extension(rs): Extension<HttpGatewayRequest>,
    axum::extract::State(config): axum::extract::State<Config>,
    version_prefix: &str,
) -> Result<axum::response::Response, WebSocketApiError> {
    use headers::{Header, HeaderMapExt};

    let domain = config
        .localhost
        .then_some("localhost")
        .expect("non-local connections not supported yet");
    let token = AuthToken::generate();

    let auth_header = headers::Authorization::<headers::authorization::Bearer>::name().to_string();
    let cookie = cookie::Cookie::build((auth_header, format!("Bearer {}", token.as_str())))
        .domain(domain)
        .path(format!("/{version_prefix}/contract/web/{key}"))
        .same_site(cookie::SameSite::Strict)
        .max_age(cookie::time::Duration::days(1))
        .secure(!config.localhost)
        .http_only(false)
        .build();

    let token_header = headers::Authorization::bearer(token.as_str()).unwrap();
    let contract_response =
        path_handlers::contract_home(key, rs, token.clone(), version_prefix).await?;

    // FIXME: We may be able to store the token in attested_contracts here if we can get the ContractInstanceId
    // from the `key` but leaving it for now based on "if it ain't broke, don't fix it" principle.

    let mut response = contract_response.into_response();
    response.headers_mut().typed_insert(token_header);
    response.headers_mut().insert(
        headers::SetCookie::name(),
        headers::HeaderValue::from_str(&cookie.to_string()).unwrap(),
    );

    Ok(response)
}

async fn web_subpages_v1(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(key, last_path, "v1").await
}

async fn web_subpages_v2(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(key, last_path, "v2").await
}

async fn web_subpages(
    key: String,
    last_path: String,
    version_prefix: &str,
) -> Result<axum::response::Response, WebSocketApiError> {
    let full_path: String = format!("/{version_prefix}/contract/web/{key}/{last_path}");
    path_handlers::variable_content(key, full_path, version_prefix)
        .await
        .map_err(|e| *e)
        .map(|r| r.into_response())
}
