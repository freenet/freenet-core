use locutus_node::either::Either;
use locutus_runtime::ContractKey;
use std::path::PathBuf;

use locutus_node::*;
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};
use warp::{reject, reply, Rejection, Reply};

use crate::{
    errors::{self, NodeError},
    ClientHandlingMessage, HttpGateway,
};

pub async fn contract_home(
    key: String,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send((
            ClientRequest::Get {
                key,
                fetch_contract: true,
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let (id, response) = match response_recv.recv().await {
        Some((id, Ok(HostResponse::GetResponse { contract, .. }))) => match contract {
            Some(contract) => {
                let path = HttpGateway::contract_web_path(contract.key());
                let web_body = get_web_body(path).await?;
                (id, Ok(reply::html(web_body)))
            }
            None => {
                todo!("error indicating the contract is not present");
            }
        },
        Some((_id, Err(err))) => {
            log::error!("error getting contract `{key}`: {err}");
            return Err(err.kind().into());
        }
        None => {
            return Err(NodeError.into());
        }
        other => unreachable!("received unexpected node response: {other:?}"),
    };
    request_sender
        .send((ClientRequest::Disconnect { cause: None }, Either::Right(id)))
        .await
        .map_err(|_| NodeError)?;
    response
}

async fn get_web_body(path: PathBuf) -> Result<warp::hyper::Body, Rejection> {
    let web_path = path.join("web/index.html");
    let mut key_file = File::open(&web_path)
        .await
        .unwrap_or_else(|_| panic!("Failed to open key file: {}", &web_path.to_str().unwrap()));
    let mut buf = vec![];
    key_file.read_to_end(&mut buf).await.unwrap();
    Ok(warp::hyper::Body::from(buf))
}

pub async fn variable_content(
    key: String,
    // referer: warp::http::Uri,
    req_path: warp::path::FullPath,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let base_path = HttpGateway::contract_web_path(&key).join("web/");
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

const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

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
        .collect::<String>();
    Ok(path)
}

#[test]
fn get_path() {
    let req_path = "/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
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
