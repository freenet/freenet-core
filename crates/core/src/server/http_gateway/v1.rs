use super::*;

// Please add detailed tracing logging to this code, particularly to the `web_home` function. AI!

impl HttpGateway {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router_v1(socket: &SocketAddr) -> (Self, Router) {
        let localhost = match socket.ip() {
            IpAddr::V4(ip) if ip.is_loopback() => true,
            IpAddr::V6(ip) if ip.is_loopback() => true,
            _ => false,
        };
        let contract_web_path = std::env::temp_dir().join("freenet").join("webs");
        std::fs::create_dir_all(contract_web_path).unwrap();

        let (proxy_request_sender, request_to_server) = mpsc::channel(1);

        let config = Config { localhost };

        let router = Router::new()
            .route("/v1", get(home))
            .route("/v1/contract/web/:key/", get(web_home))
            .with_state(config)
            .route("/v1/contract/web/:key/*path", get(web_subpages))
            .layer(Extension(HttpGatewayRequest(proxy_request_sender)));

        (
            Self {
                proxy_server_request: request_to_server,
                attested_contracts: HashMap::new(),
                response_channels: HashMap::new(),
            },
            router,
        )
    }
}

async fn web_home(
    Path(key): Path<String>,
    Extension(rs): Extension<HttpGatewayRequest>,
    axum::extract::State(config): axum::extract::State<Config>,
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
        .path(format!("/v1/contract/web/{key}"))
        .same_site(cookie::SameSite::Strict)
        .max_age(cookie::time::Duration::days(1))
        .secure(!config.localhost)
        .http_only(false)
        .build();

    let token_header = headers::Authorization::bearer(token.as_str()).unwrap();
    let contract_idx = path_handlers::contract_home(key, rs, token).await?;
    let mut response = contract_idx.into_response();
    response.headers_mut().typed_insert(token_header);
    response.headers_mut().insert(
        headers::SetCookie::name(),
        headers::HeaderValue::from_str(&cookie.to_string()).unwrap(),
    );

    Ok(response)
}

async fn web_subpages(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let full_path: String = format!("/v1/contract/web/{}/{}", key, last_path);
    path_handlers::variable_content(key, full_path)
        .await
        .map_err(|e| *e)
        .map(|r| r.into_response())
}
