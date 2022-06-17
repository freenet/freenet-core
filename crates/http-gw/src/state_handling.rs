use locutus_node::either::Either;
use locutus_runtime::{ContractKey, StateDelta};

use futures::future::ready;
use futures::{SinkExt, StreamExt};
use locutus_node::*;
use tokio::sync::mpsc;
use warp::ws::{Message, WebSocket};
use warp::{reject, reply, Rejection, Reply};

use crate::{errors, ClientHandlingMessage};

use self::errors::NodeError;

pub async fn get_state(
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
                fetch_contract: false,
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let (id, response) = match response_recv.recv().await {
        Some((id, Ok(HostResponse::GetResponse { state, .. }))) => {
            // todo: we assume that this is json, but it could be anything
            //       in reality we must send this back as bytes and let the client handle it
            //       but in order to test things out we send a json
            let json_str: serde_json::Value = serde_json::from_slice(state.as_ref()).unwrap();
            (id, Ok(reply::json(&json_str)))
        }
        None => {
            return Err(NodeError.into());
        }
        _ => unreachable!(),
    };
    request_sender
        .send((ClientRequest::Disconnect { cause: None }, Either::Right(id)))
        .await
        .map_err(|_| NodeError)?;
    response
}

// todo: assuming json but could be anything really, they have to send as bytes
pub async fn update_state(
    key: String,
    update_value: serde_json::Value,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
) -> Result<impl Reply, Rejection> {
    let contract_key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let delta = serde_json::to_vec(&update_value).unwrap();
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send((
            ClientRequest::Update {
                key: contract_key,
                delta: StateDelta::from(delta),
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let (id, response) = match response_recv.recv().await {
        Some((id, Ok(HostResponse::UpdateResponse { key, .. }))) => {
            assert_eq!(key, contract_key);
            (id, Ok(reply::json(&serde_json::json!({"result": "ok"}))))
        }
        Some((id, Err(err))) => (
            id,
            Ok(reply::json(
                &serde_json::json!({"result": "error", "err": format!("{err}")}),
            )),
        ),
        None => {
            return Err(NodeError.into());
        }
        err => unreachable!("{err:?}"),
    };
    request_sender
        .send((ClientRequest::Disconnect { cause: None }, Either::Right(id)))
        .await
        .map_err(|_| NodeError)?;
    response
}

pub async fn state_updates_notification(
    key: String,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
    ws: WebSocket,
) {
    let contract_key = ContractKey::from_spec(key).unwrap();
    let (updates, mut updates_recv) = mpsc::unbounded_channel();
    let (response_sender, _response_recv) = mpsc::unbounded_channel();

    let (mut ws_sender, mut _ws_receiver) = {
        let (tx, rx) = ws.split();

        let str_sender = tx.with(|msg: serde_json::Value| {
            let res: Result<Message, warp::Error> = Ok(Message::text(
                serde_json::to_string(&msg).expect("Converting message to JSON"),
            ));
            ready(res)
        });

        (str_sender, rx)
    };

    let peer = PeerKey::random(); // FIXME: this must be obtained somehow from the node
    request_sender
        .send((
            ClientRequest::Subscribe {
                key: contract_key,
                updates,
                peer,
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))
        .unwrap();
    // todo: await for some sort of confirmation through "response_recv"
    while let Some(response) = updates_recv.recv().await {
        if let HostResponse::UpdateNotification { key, update } = response {
            // todo: we assume that this is json, but it could be anything
            //       in reality we must send this back as bytes and let the client handle it
            //       but in order to test things out we send a json
            assert_eq!(key, contract_key);
            let s = update.as_ref();
            // fixme: state delta off by one err when reading from buf
            let json_str: serde_json::Value =
                serde_json::from_slice(&s[..s.len() - 1]).expect("deserialization err");
            let _ = ws_sender.send(json_str).await;
        } else {
            break;
        }
    }
    request_sender
        .send((
            ClientRequest::Disconnect { cause: None },
            Either::Right(ClientId::new(0)),
        ))
        .await
        .map_err(|_| NodeError)
        .unwrap();
}
