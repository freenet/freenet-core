//! Handle the `web` part of the bundles.

use std::path::{Path, PathBuf};

use axum::response::{Html, IntoResponse};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse},
    prelude::*,
};
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};

use crate::client_events::AuthToken;

use super::{
    app_packaging::{WebApp, WebContractError},
    errors::WebSocketApiError,
    http_gateway::HttpGatewayRequest,
    ClientConnection, HostCallbackResult,
};
use tracing::{debug, instrument};

mod v1;

#[instrument(level = "debug", skip(request_sender))]
pub(super) async fn contract_home(
    key: String,
    request_sender: HttpGatewayRequest,
    assigned_token: AuthToken,
) -> Result<impl IntoResponse, WebSocketApiError> {
    debug!(
        "contract_home: Converting string key to ContractInstanceId: {}",
        key
    );
    let instance_id = ContractInstanceId::from_bytes(&key).map_err(|err| {
        debug!("contract_home: Failed to parse contract key: {}", err);
        WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        }
    })?;
    debug!("contract_home: Successfully parsed contract instance id");
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    debug!("contract_home: Sending NewConnection request");
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token: Some((assigned_token.clone(), instance_id)),
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let client_id = if let Some(HostCallbackResult::NewId { id }) = response_recv.recv().await {
        id
    } else {
        return Err(WebSocketApiError::NodeError {
            error_cause: "Couldn't register new client in the node".into(),
        });
    };
    debug!("contract_home: Sending GET request for contract");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(
                ContractRequest::Get {
                    key: instance_id,
                    return_contract_code: true,
                    subscribe: false,
                    blocking_subscribe: false,
                }
                .into(),
            ),
            auth_token: None,
            attested_contract: None,
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    debug!("contract_home: Waiting for GET response");
    let response = match response_recv.recv().await {
        Some(HostCallbackResult::Result {
            result:
                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    contract,
                    state,
                    ..
                })),
            ..
        }) => match contract {
            Some(contract) => {
                let contract_key = contract.key();
                let path = contract_web_path(contract_key.id());
                let state_bytes = state.as_ref();
                let current_hash = hash_state(state_bytes);
                let hash_path = state_hash_path(contract_key.id());

                let needs_update = match tokio::fs::read(&hash_path).await {
                    Ok(stored_hash_bytes) if stored_hash_bytes.len() == 8 => {
                        let stored_hash = u64::from_be_bytes(stored_hash_bytes.try_into().unwrap());
                        stored_hash != current_hash
                    }
                    _ => true,
                };

                if needs_update {
                    debug!("State changed or not cached, unpacking webapp");
                    let state = State::from(state_bytes);

                    fn err(
                        err: WebContractError,
                        contract: &ContractContainer,
                    ) -> WebSocketApiError {
                        let key = contract.key();
                        tracing::error!("{err}");
                        WebSocketApiError::InvalidParam {
                            error_cause: format!("failed unpacking contract: {key}"),
                        }
                    }

                    // Clear existing cache if any
                    let _ = tokio::fs::remove_dir_all(&path).await;
                    tokio::fs::create_dir_all(&path).await.map_err(|e| {
                        WebSocketApiError::NodeError {
                            error_cause: format!("Failed to create cache dir: {e}"),
                        }
                    })?;

                    let mut web =
                        WebApp::try_from(state.as_ref()).map_err(|e| err(e, &contract))?;
                    web.unpack(&path).map_err(|e| err(e, &contract))?;

                    // Store new hash
                    tokio::fs::write(&hash_path, current_hash.to_be_bytes())
                        .await
                        .map_err(|e| WebSocketApiError::NodeError {
                            error_cause: format!("Failed to write state hash: {e}"),
                        })?;
                }

                match get_web_body(&path, &assigned_token, &key).await {
                    Ok(b) => b.into_response(),
                    Err(err) => {
                        tracing::error!("Failed to read webapp after unpacking: {err}");
                        return Err(WebSocketApiError::NodeError {
                            error_cause: format!("Failed to read webapp: {err}"),
                        });
                    }
                }
            }
            None => {
                return Err(WebSocketApiError::MissingContract { instance_id });
            }
        },
        Some(HostCallbackResult::Result {
            result: Err(err), ..
        }) => {
            tracing::error!("error getting contract `{key}`: {err}");
            return Err(WebSocketApiError::AxumError {
                error: err.kind().clone(),
            });
        }
        None => {
            return Err(WebSocketApiError::NodeError {
                error_cause: format!("Contract not found: {key}"),
            });
        }
        other => {
            tracing::error!("Unexpected node response: {other:?}");
            return Err(WebSocketApiError::NodeError {
                error_cause: format!("Unexpected response from node: {other:?}"),
            });
        }
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(ClientRequest::Disconnect { cause: None }),
            auth_token: None,
            attested_contract: None,
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    Ok(response)
}

#[instrument(level = "debug")]
pub(super) async fn variable_content(
    key: String,
    req_path: String,
) -> Result<impl IntoResponse, Box<WebSocketApiError>> {
    debug!(
        "variable_content: Processing request for key: {}, path: {}",
        key, req_path
    );
    // compose the correct absolute path
    let instance_id =
        ContractInstanceId::from_bytes(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;
    let base_path = contract_web_path(&instance_id);
    debug!("variable_content: Base path resolved to: {:?}", base_path);

    // Parse the full request path URI to extract the relative path using the v1 helper.
    let req_uri =
        req_path
            .parse::<axum::http::Uri>()
            .map_err(|err| WebSocketApiError::InvalidParam {
                error_cause: format!("Failed to parse request path as URI: {err}"),
            })?;
    debug!("variable_content: Parsed request URI: {:?}", req_uri);

    let relative_path = v1::get_file_path(req_uri)?;
    debug!(
        "variable_content: Extracted relative path: {}",
        relative_path
    );

    let file_path = base_path.join(relative_path);
    debug!("variable_content: Full file path to serve: {:?}", file_path);
    debug!(
        "variable_content: Checking if file exists: {}",
        file_path.exists()
    );

    // serve the file
    let mut serve_file = tower_http::services::fs::ServeFile::new(&file_path);
    let fake_req = axum::http::Request::new(axum::body::Body::empty());
    serve_file
        .try_call(fake_req)
        .await
        .map_err(|err| {
            WebSocketApiError::NodeError {
                error_cause: format!("{err}"),
            }
            .into()
        })
        .map(|r| r.into_response())
}

#[instrument(level = "debug", skip(auth_token))]
async fn get_web_body(
    path: &Path,
    auth_token: &AuthToken,
    contract_key: &str,
) -> Result<impl IntoResponse, WebSocketApiError> {
    debug!(
        "get_web_body: Attempting to read index.html from path: {:?}",
        path
    );
    let web_path = path.join("index.html");
    debug!("get_web_body: Full web path: {:?}", web_path);
    let mut key_file = File::open(&web_path)
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let mut buf = vec![];
    key_file
        .read_to_end(&mut buf)
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let mut body = String::from_utf8(buf).map_err(|err| WebSocketApiError::NodeError {
        error_cause: format!("{err}"),
    })?;

    // Rewrite root-relative asset paths so they resolve under the contract's web prefix.
    // Dioxus generates paths like /./assets/app.js which browsers normalize to /assets/app.js
    // (root-relative). These bypass the /v1/contract/web/{key}/ prefix and 404.
    // Safety: contract_key is a base58-encoded contract ID (alphanumeric only).
    let prefix = format!("/v1/contract/web/{contract_key}/");
    body = body.replace("\"/./", &format!("\"{prefix}"));
    body = body.replace("'/./", &format!("'{prefix}"));

    // Inject auth token into HTML so web apps can access it without re-fetching.
    // Safety: AuthToken uses base58 encoding which only produces alphanumeric characters
    // (0-9, A-Z, a-z excluding 0, O, I, l), so no escaping is needed for the JavaScript string.
    let token_script = format!(
        r#"<script>window.__FREENET_AUTH_TOKEN__ = "{}";</script>"#,
        auth_token.as_str()
    );
    if let Some(pos) = body.find("</head>") {
        body.insert_str(pos, &token_script);
    } else if let Some(pos) = body.find("<body") {
        // Fallback: insert before <body> if no </head>
        body.insert_str(pos, &token_script);
    } else {
        // Last resort: prepend to document
        body = format!("{token_script}{body}");
    }

    Ok(Html(body))
}

/// Returns the base directory for webapp cache.
/// Uses XDG cache directory (~/.cache/freenet on Linux) to avoid permission
/// conflicts when multiple users run freenet on the same machine.
fn webapp_cache_dir() -> PathBuf {
    directories::ProjectDirs::from("", "The Freenet Project Inc", "freenet")
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir().join("freenet"))
        .join("webapp_cache")
}

fn contract_web_path(instance_id: &ContractInstanceId) -> PathBuf {
    webapp_cache_dir().join(instance_id.encode())
}

fn hash_state(state: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(state);
    hasher.finish()
}

fn state_hash_path(instance_id: &ContractInstanceId) -> PathBuf {
    webapp_cache_dir().join(format!("{}.hash", instance_id.encode()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn root_relative_asset_paths_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";
        let html = r#"<!DOCTYPE html>
<html>
    <head>
        <title>Test</title>
    <link rel="preload" as="script" href="/./assets/app.js" crossorigin></head>
    <body><div id="main"></div>
    <script type="module" async src="/./assets/app.js"></script>
    </body>
</html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let token = AuthToken::generate();
        let response = get_web_body(dir.path(), &token, key).await.unwrap();
        let body = response.into_response();
        let bytes = axum::body::to_bytes(body.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result = String::from_utf8(bytes.to_vec()).unwrap();

        let expected_href = format!("href=\"/v1/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected_href),
            "href not rewritten.\nGot: {result}"
        );

        let expected_src = format!("src=\"/v1/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected_src),
            "src not rewritten.\nGot: {result}"
        );

        // Original root-relative paths should be gone
        assert!(
            !result.contains("\"/./assets/"),
            "original /./assets/ paths still present"
        );

        // Auth token script should also be present
        assert!(
            result.contains("__FREENET_AUTH_TOKEN__"),
            "auth token not injected"
        );
    }

    #[tokio::test]
    async fn single_quoted_paths_also_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html = "<head><script src='/./assets/app.js'></script></head>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let token = AuthToken::generate();
        let response = get_web_body(dir.path(), &token, key).await.unwrap();
        let body = response.into_response();
        let bytes = axum::body::to_bytes(body.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result = String::from_utf8(bytes.to_vec()).unwrap();

        let expected = format!("'/v1/contract/web/{key}/assets/app.js'");
        assert!(
            result.contains(&expected),
            "single-quoted path not rewritten.\nGot: {result}"
        );
    }

    #[tokio::test]
    async fn paths_without_dot_slash_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Paths like "/assets/app.js" (without /.) should NOT be rewritten,
        // only the Dioxus-specific "/./assets/" pattern is targeted.
        let html = r#"<head><link href="/assets/app.css"></head><body></body>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let token = AuthToken::generate();
        let response = get_web_body(dir.path(), &token, key).await.unwrap();
        let body = response.into_response();
        let bytes = axum::body::to_bytes(body.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result = String::from_utf8(bytes.to_vec()).unwrap();

        // The /assets/ path should remain unchanged (no /. prefix)
        assert!(
            result.contains("\"/assets/app.css\""),
            "path without /. was incorrectly rewritten.\nGot: {result}"
        );
    }
}
