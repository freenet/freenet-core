//! Handles the `state data` part of the bundles.
use locutus_runtime::{ContractKey, StateDelta};

use futures::future::ready;
use futures::{SinkExt, StreamExt};
use locutus_node::*;
use tokio::sync::mpsc;
use warp::ws::{Message, WebSocket};
use warp::{reject, reply, Rejection, Reply};

use crate::{errors, ClientConnection, HostCallbackResult};

use self::errors::NodeError;

pub(crate) async fn get_state(
    client_id: ClientId,
    key: String,
    request_sender: &mpsc::Sender<ClientConnection>,
    response_recv: &mut mpsc::UnboundedReceiver<HostCallbackResult>,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: ClientRequest::Get {
                key,
                fetch_contract: false,
            },
        })
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let response = match response_recv.recv().await {
        Some(HostCallbackResult::Result {
            result: Ok(HostResponse::GetResponse { state, .. }),
            ..
        }) => {
            let bytes = bytes::Bytes::copy_from_slice(state.as_ref());
            let response = reply::Response::new(warp::hyper::Body::from(bytes));
            Ok(response)
        }
        None => {
            return Err(NodeError.into());
        }
        _ => unreachable!(),
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

pub(crate) async fn update_state(
    client_id: ClientId,
    key: String,
    delta: StateDelta<'static>,
    request_sender: &mpsc::Sender<ClientConnection>,
    response_recv: &mut mpsc::UnboundedReceiver<HostCallbackResult>,
) -> Result<impl Reply, Rejection> {
    let contract_key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: ClientRequest::Update {
                key: contract_key,
                delta,
            },
        })
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let response = match response_recv.recv().await {
        Some(HostCallbackResult::Result {
            result: Ok(HostResponse::UpdateResponse { key, .. }),
            ..
        }) => {
            assert_eq!(key, contract_key);
            Ok(reply::json(&serde_json::json!({"result": "ok"})))
        }
        Some(HostCallbackResult::Result {
            result: Err(err), ..
        }) => Ok(reply::json(
            &serde_json::json!({"result": "error", "err": format!("{err}")}),
        )),
        None => {
            return Err(NodeError.into());
        }
        err => unreachable!("{err:?}"),
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

pub(crate) async fn state_changes_notification(
    client_id: ClientId,
    key: String,
    ws: WebSocket,
    request_sender: &mpsc::Sender<ClientConnection>,
    _response_recv: &mut mpsc::UnboundedReceiver<HostCallbackResult>,
) {
    let contract_key = ContractKey::from_spec(key).unwrap();
    let (updates, mut updates_recv) = mpsc::unbounded_channel();

    let (mut ws_sender, mut _ws_receiver) = {
        let (tx, rx) = ws.split();

        let str_sender = tx.with(|msg: Vec<u8>| {
            let res: Result<Message, warp::Error> = Ok(Message::binary(msg));
            ready(res)
        });

        (str_sender, rx)
    };

    request_sender
        .send(ClientConnection::UpdateSubChannel {
            client_id,
            callback: updates,
            key: contract_key,
        })
        .await
        .map_err(|_| reject::custom(errors::NodeError))
        .unwrap();

    // FIXME: at this point must give back control back to the main websocket stream (probably through a `select`)
    //        in here, otherwise will block communication until an update is received
    // todo: await for some sort of confirmation through "response_recv"
    while let Some(response) = updates_recv.recv().await {
        if let Ok(HostResponse::UpdateNotification { key, update }) = response {
            assert_eq!(key, contract_key);
            let s = update.as_ref();
            // fixme: state delta off by one err when reading from buf
            let _ = ws_sender.send(s[..s.len() - 1].to_vec()).await;
        } else {
            break;
        }
    }
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: ClientRequest::Disconnect { cause: None },
        })
        .await
        .map_err(|_| NodeError)
        .unwrap();
}
