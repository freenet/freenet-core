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

const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub(super) async fn contract_home(
    key: String,
    request_sender: HttpGatewayRequest,
    assigned_token: AuthToken,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let key = ContractKey::from_id(key)
        .map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })
        .unwrap();
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token: Some((assigned_token, key.clone().into())),
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })
        .unwrap();
    let client_id = if let Some(HostCallbackResult::NewId { id }) = response_recv.recv().await {
        id
    } else {
        return Err(WebSocketApiError::NodeError {
            error_cause: "Couldn't register new client in the node".into(),
        });
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(
                ContractRequest::Get {
                    key: key.clone(),
                    fetch_contract: true,
                }
                .into(),
            ),
            auth_token: None,
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })
        .unwrap();
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
                let web_body = match get_web_body(&path).await {
                    Ok(b) => b.into_response(),
                    Err(err) => match err {
                        WebSocketApiError::NodeError {
                            error_cause: _cause,
                        } => {
                            let state = State::from(state.as_ref());

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

                            let mut web = WebApp::try_from(state.as_ref())
                                .map_err(|e| err(e, &contract))
                                .unwrap();
                            web.unpack(path).map_err(|e| err(e, &contract)).unwrap();
                            let index = web
                                .get_file("index.html")
                                .map_err(|e| err(e, &contract))
                                .unwrap();
                            let index_body = String::from_utf8(index).map_err(|err| {
                                WebSocketApiError::NodeError {
                                    error_cause: format!("{err}"),
                                }
                            })?;
                            Html(index_body).into_response()
                        }
                        other => {
                            tracing::error!("{other}");
                            return Err(other);
                        }
                    },
                };
                web_body
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
        other => unreachable!("received unexpected node response: {other:?}"),
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
        })
        .unwrap();
    Ok(response)
}

pub(super) async fn variable_content(
    key: String,
    req_path: String,
) -> Result<impl IntoResponse, Box<WebSocketApiError>> {
    // compose the correct absolute path
    let key = ContractKey::from_id(key).map_err(|err| WebSocketApiError::InvalidParam {
        error_cause: format!("{err}"),
    })?;
    let base_path = contract_web_path(&key);
    let req_uri = req_path
        .parse()
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let file_path = base_path.join(get_file_path(req_uri)?);

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

async fn get_web_body(path: &Path) -> Result<impl IntoResponse, WebSocketApiError> {
    let web_path = path.join("web").join("index.html");
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
        .join("webs")
        .join(key.encoded_contract_id())
        .join("web")
}

#[inline]
fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let p = uri.path().strip_prefix("/contract/").ok_or_else(|| {
        Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!("{uri} not valid"),
        })
    })?;
    let path = p
        .chars()
        .skip_while(|c| ALPHABET.contains(*c))
        .skip_while(|c| c == &'/')
        .skip_while(|c| ALPHABET.contains(*c))
        .skip_while(|c| c == &'/')
        .collect::<String>();
    Ok(path)
}

#[test]
fn get_path() {
    let req_path = "/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/state.html";
    let base_dir =
        PathBuf::from("/tmp/freenet/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/");
    let uri: axum::http::Uri = req_path.parse().unwrap();
    let parsed = get_file_path(uri).unwrap();
    let result = base_dir.join(parsed);
    assert_eq!(
        std::path::PathBuf::from(
            "/tmp/freenet/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/state.html"
        ),
        result
    );
}
