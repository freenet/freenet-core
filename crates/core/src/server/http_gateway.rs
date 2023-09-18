use std::net::{IpAddr, SocketAddr};

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use tokio::sync::mpsc;

use super::{errors::WebSocketApiError, path_handlers, AuthToken, ClientConnection};

/// A gateway to access and interact with contracts through an HTTP interface.
///
/// Contracts initially accessed through the gateway have to be compliant with the container contract
/// [specification](https://docs.freenet.org/glossary.html#container-contract) for Locutus.
///
/// Check the Locutus book for [more information](https://docs.freenet.org/dev-guide.html).
pub(crate) struct HttpGateway;

#[derive(Clone)]
struct Config {
    localhost: bool,
}

impl HttpGateway {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router(socket: &SocketAddr) -> Router {
        let localhost = match socket.ip() {
            IpAddr::V4(ip) if ip.is_loopback() => true,
            IpAddr::V6(ip) if ip.is_loopback() => true,
            _ => false,
        };
        let contract_web_path = std::env::temp_dir().join("locutus").join("webs");
        std::fs::create_dir_all(contract_web_path).unwrap();

        let config = Config { localhost };

        Router::new()
            .route("/", get(home))
            .route("/contract/web/:key/", get(web_home))
            .with_state(config)
            .route("/contract/web/:key/*path", get(web_subpages))
    }
}

async fn home() -> axum::response::Response {
    axum::response::Response::default()
}

async fn web_home(
    Path(key): Path<String>,
    Extension(rs): Extension<mpsc::Sender<ClientConnection>>,
    axum::extract::State(config): axum::extract::State<Config>,
) -> Result<axum::response::Response, WebSocketApiError> {
    use axum::headers::{Header, HeaderMapExt};

    let domain = config
        .localhost
        .then_some("localhost")
        .expect("non-local connections not supported yet");
    let token = AuthToken::generate();

    let auth_header =
        axum::headers::Authorization::<axum::headers::authorization::Bearer>::name().to_string();
    let cookie = cookie::Cookie::build(auth_header, format!("Bearer {}", token.as_str()))
        .domain(domain)
        .path(format!("/contract/web/{key}"))
        .same_site(cookie::SameSite::Strict)
        .max_age(cookie::time::Duration::days(1))
        .secure(!config.localhost)
        .http_only(false)
        .finish();

    let token_header = axum::headers::Authorization::bearer(token.as_str()).unwrap();
    let contract_idx = path_handlers::contract_home(key, rs, token).await?;
    let mut response = contract_idx.into_response();
    response.headers_mut().typed_insert(token_header);
    response.headers_mut().insert(
        axum::headers::SetCookie::name(),
        axum::headers::HeaderValue::from_str(&cookie.to_string()).unwrap(),
    );

    Ok(response)
}

async fn web_subpages(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let full_path: String = format!("/contract/web/{}/{}", key, last_path);
    path_handlers::variable_content(key, full_path)
        .await
        .map_err(|e| *e)
        .map(|r| r.into_response())
}
