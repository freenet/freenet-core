//! Handle the `web` part of the bundles.

use axum::http::StatusCode;
use axum::response::{Html, Response};
use std::path::{Path, PathBuf};

use locutus_runtime::{
    locutus_stdlib::web::{WebApp, WebContractError},
    ContractContainer, ContractKey, State,
};

use locutus_core::{
    locutus_runtime::locutus_stdlib::client_api::{
        ClientRequest, ContractRequest, ContractResponse, HostResponse,
    },
    *,
};
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};

use crate::errors::Error;
use crate::{ClientConnection, HostCallbackResult};

const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub(crate) async fn contract_home(
    key: String,
    request_sender: mpsc::Sender<ClientConnection>,
) -> Result<Html<String>, Error> {
    let key = ContractKey::from_id(key)
        .map_err(|err| Error::InvalidParam(format!("{err}")))
        .unwrap();
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection(response_sender))
        .await
        .map_err(|_| Error::NodeError)
        .unwrap();
    let client_id = if let Some(HostCallbackResult::NewId(id)) = response_recv.recv().await {
        id
    } else {
        todo!("this is an error");
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: ContractRequest::Get {
                key: key.clone(),
                fetch_contract: true,
            }
            .into(),
        })
        .await
        .map_err(|_| Error::NodeError)
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
                    Ok(b) => b,
                    Err(err) => match err {
                        Error::NodeError => {
                            let state = State::from(state.as_ref());

                            fn err(err: WebContractError, contract: &ContractContainer) -> Error {
                                let key = contract.key();
                                tracing::error!("{err}");
                                Error::InvalidParam(format!("failed unpacking contract: {key}"))
                            }

                            let mut web = WebApp::try_from(state.as_ref())
                                .map_err(|e| err(e, &contract))
                                .unwrap();
                            web.unpack(path).map_err(|e| err(e, &contract)).unwrap();
                            let index = web
                                .get_file("index.html")
                                .map_err(|e| err(e, &contract))
                                .unwrap();
                            let index_body =
                                String::from_utf8(index).map_err(|_| Error::NodeError)?;
                            Html(index_body)
                        }
                        other => {
                            tracing::error!("{other}");
                            return Err(Error::HttpError(StatusCode::INTERNAL_SERVER_ERROR));
                        }
                    },
                };
                web_body
            }
            None => {
                todo!("error indicating the contract is not present");
            }
        },
        Some(HostCallbackResult::Result {
            result: Err(err), ..
        }) => {
            tracing::error!("error getting contract `{key}`: {err}");
            return Err(Error::AxumError(err.kind()));
        }
        None => {
            return Err(Error::NodeError);
        }
        other => unreachable!("received unexpected node response: {other:?}"),
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: ClientRequest::Disconnect { cause: None },
        })
        .await
        .map_err(|_| Error::NodeError)
        .unwrap();
    Ok(response)
}

pub async fn variable_content(key: String, req_path: String) -> Result<Html<String>, Error> {
    let key = ContractKey::from_id(key)
        .map_err(|err| Error::InvalidParam(format!("{err}")))
        .unwrap();
    let base_path = contract_web_path(&key);
    let req_uri = req_path.parse().unwrap();
    let file_path = base_path.join(get_file_path(req_uri).unwrap());
    let mut buf = vec![];
    File::open(file_path)
        .await
        .map_err(|_| Error::NodeError)?
        .read_to_end(&mut buf)
        .await
        .map_err(|_| Error::NodeError)?;
    let body = String::from_utf8(buf).map_err(|_| Error::NodeError)?;
    Ok(Html(body))
}

async fn get_web_body(path: &Path) -> Result<Html<String>, Error> {
    let web_path = path.join("web").join("index.html");
    let mut key_file = File::open(&web_path).await.map_err(|_| Error::NodeError)?;
    let mut buf = vec![];
    key_file
        .read_to_end(&mut buf)
        .await
        .map_err(|_| Error::NodeError)?;
    let body = String::from_utf8(buf).map_err(|_| Error::NodeError)?;
    Ok(Html(body))
}

fn contract_web_path(key: &ContractKey) -> PathBuf {
    std::env::temp_dir()
        .join("locutus")
        .join("webs")
        .join(key.encoded_contract_id())
        .join("web")
}

#[inline]
fn get_file_path(uri: axum::http::Uri) -> Result<String, Response> {
    let p = uri
        .path()
        .strip_prefix("/contract/")
        .ok_or_else(|| Error::InvalidParam(format!("invalid uri: {uri}")))
        .unwrap();
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
        PathBuf::from("/tmp/locutus/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/");
    let uri: axum::http::Uri = req_path.parse().unwrap();
    let parsed = get_file_path(uri).unwrap();
    let result = base_dir.join(parsed);
    assert_eq!(
        std::path::PathBuf::from(
            "/tmp/locutus/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/state.html"
        ),
        result
    );
}
