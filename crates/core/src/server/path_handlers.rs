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
        "contract_home: Converting string key to ContractKey: {}",
        key
    );
    let key = ContractKey::from_id(key).map_err(|err| {
        debug!("contract_home: Failed to parse contract key: {}", err);
        WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        }
    })?;
    debug!("contract_home: Successfully parsed contract key");
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    debug!("contract_home: Sending NewConnection request");
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token: Some((assigned_token, key.into())),
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
                    key,
                    return_contract_code: true,
                    subscribe: false,
                }
                .into(),
            ),
            auth_token: None,
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
                let key = contract.key();
                let path = contract_web_path(&key);
                let state_bytes = state.as_ref();
                let current_hash = hash_state(state_bytes);
                let hash_path = state_hash_path(&key);

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

                match get_web_body(&path).await {
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
                return Err(WebSocketApiError::MissingContract { key });
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
    let key = ContractKey::from_id(key).map_err(|err| WebSocketApiError::InvalidParam {
        error_cause: format!("{err}"),
    })?;
    let base_path = contract_web_path(&key);
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

#[instrument(level = "debug")]
async fn get_web_body(path: &Path) -> Result<impl IntoResponse, WebSocketApiError> {
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
    let body = String::from_utf8(buf).map_err(|err| WebSocketApiError::NodeError {
        error_cause: format!("{err}"),
    })?;
    Ok(Html(body))
}

fn contract_web_path(key: &ContractKey) -> PathBuf {
    std::env::temp_dir()
        .join("freenet")
        .join("webapp_cache")
        .join(key.encoded_contract_id())
}

fn hash_state(state: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(state);
    hasher.finish()
}

fn state_hash_path(key: &ContractKey) -> PathBuf {
    std::env::temp_dir()
        .join("freenet")
        .join("webapp_cache")
        .join(format!("{}.hash", key.encoded_contract_id()))
}
