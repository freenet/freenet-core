use locutus_node::either::Either;
use locutus_runtime::ContractKey;
use std::fs::File;
use std::path::PathBuf;

use locutus_node::*;
use std::io::Read;
use tokio::sync::mpsc;
use warp::{reject, reply, Rejection, Reply};

use crate::{
    errors::{self, NodeError},
    ClientHandlingMessage, DynError, HttpGateway,
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
                let web_body = get_web_body(path).unwrap();
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

fn get_web_body(path: PathBuf) -> Result<warp::hyper::Body, DynError> {
    let mut index = vec![];
    let web_path = path.join("web/index.html");
    let mut key_file = File::open(&web_path)
        .unwrap_or_else(|_| panic!("Failed to open key file: {}", &web_path.to_str().unwrap()));
    key_file.read_to_end(&mut index).unwrap();
    Ok(warp::hyper::Body::from(index))
}
