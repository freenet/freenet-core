//! Handle the `web` part of the bundles.

use std::path::{Path, PathBuf};

use locutus_runtime::{
    locutus_stdlib::web::{WebApp, WebContractError},
    ContractContainer, ContractKey, State,
};

use locutus_core::*;
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};
use warp::{reject, reply, Rejection, Reply};

use crate::{
    errors::{self, InvalidParam, NodeError},
    ClientConnection, HostCallbackResult,
};

const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub(crate) async fn contract_home(
    key: String,
    request_sender: mpsc::Sender<ClientConnection>,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_id(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection(response_sender))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
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
        .map_err(|_| reject::custom(errors::NodeError))?;
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
                let key = contract.get_key();
                let path = contract_web_path(&key);
                let web_body = match get_web_body(&path).await {
                    Ok(b) => b,
                    Err(err) => {
                        let err: std::io::ErrorKind = err.kind();
                        match err {
                            std::io::ErrorKind::NotFound => {
                                let state = State::from(state.as_ref());

                                fn err(
                                    err: WebContractError,
                                    contract: &ContractContainer,
                                ) -> InvalidParam {
                                    let key = contract.get_key();
                                    log::error!("{err}");
                                    InvalidParam(format!("failed unpacking contract: {}", key))
                                }

                                let mut web = WebApp::try_from(state.as_ref())
                                    .map_err(|e| err(e, &contract))?;
                                web.unpack(path).map_err(|e| err(e, &contract))?;
                                let index =
                                    web.get_file("index.html").map_err(|e| err(e, &contract))?;
                                warp::hyper::Body::from(index)
                            }
                            other => {
                                log::error!("{other}");
                                return Err(errors::HttpError(
                                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                                )
                                .into());
                            }
                        }
                    }
                };
                Ok(reply::html(web_body))
            }
            None => {
                todo!("error indicating the contract is not present");
            }
        },
        Some(HostCallbackResult::Result {
            result: Err(err), ..
        }) => {
            log::error!("error getting contract `{key}`: {err}");
            return Err(err.kind().into());
        }
        None => {
            return Err(NodeError.into());
        }
        other => unreachable!("received unexpected node response: {other:?}"),
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: ClientRequest::Disconnect { cause: None },
        })
        .await
        .map_err(|_| NodeError)?;
    response
}

pub async fn variable_content(
    key: String,
    req_path: warp::path::FullPath,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_id(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let base_path = contract_web_path(&key);
    let req_uri = req_path.as_str().parse().unwrap();
    let file_path = base_path.join(get_file_path(req_uri)?);
    let mut buf = vec![];
    File::open(file_path)
        .await
        .map_err(|_| NodeError)?
        .read_to_end(&mut buf)
        .await
        .map_err(|_| NodeError)?;
    Ok(reply::html(warp::hyper::Body::from(buf)))
}

async fn get_web_body(path: &Path) -> std::io::Result<warp::hyper::Body> {
    let web_path = path.join("web").join("index.html");
    let mut key_file = File::open(&web_path).await?;
    let mut buf = vec![];
    key_file.read_to_end(&mut buf).await?;
    Ok(warp::hyper::Body::from(buf))
}

fn contract_web_path(key: &ContractKey) -> PathBuf {
    std::env::temp_dir()
        .join("locutus")
        .join("webs")
        .join(key.encoded_contract_id())
        .join("web")
}

#[inline]
fn get_file_path(uri: warp::http::Uri) -> Result<String, Rejection> {
    let p = uri
        .path()
        .strip_prefix("/contract/")
        .ok_or_else(|| reject::custom(errors::InvalidParam(format!("invalid uri: {uri}"))))?;
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
    let uri: warp::http::Uri = req_path.parse().unwrap();
    let parsed = get_file_path(uri).unwrap();
    let result = base_dir.join(parsed);
    assert_eq!(
        std::path::PathBuf::from(
            "/tmp/locutus/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/state.html"
        ),
        result
    );
}
