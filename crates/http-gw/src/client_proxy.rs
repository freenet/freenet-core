use std::{collections::HashMap, net::SocketAddr};

use locutus_node::*;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use warp::{
    hyper::StatusCode,
    reject::{self, Reject},
    reply, Filter, Rejection, Reply,
};

type HostResult = Result<HostResponse, ClientError>;
type NewResponseSender = Sender<Result<HostResponse, ClientError>>;

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

pub struct HttpGateway {
    server_request: Receiver<(ClientId, ClientRequest)>,
    server_response: Sender<(ClientId, HostResult)>,
}

impl HttpGateway {
    pub fn as_filter(socket: impl Into<SocketAddr>) -> (Self, impl warp::Filter) {
        let (request_sender, server_request) = channel(PARALLELISM);
        let (server_response, response_receiver) = channel(PARALLELISM);
        let (new_client_up, new_clients) = channel(PARALLELISM);
        let filter = warp::path::end()
            .map(move || (request_sender.clone(), new_client_up.clone()))
            .and(warp::path::param())
            .and_then(|(rs, nw), key: String| async move { handle_contract(key).await })
            .recover(handle_error);
        tokio::spawn(responses(new_clients, response_receiver));
        (
            Self {
                server_request,
                server_response,
            },
            filter,
        )
    }
}

#[derive(Debug)]
struct InvalidParam(String);

impl Reject for InvalidParam {}

async fn handle_contract(key: String) -> Result<impl Reply, Rejection> {
    let key = key.to_lowercase();
    let key_as_bytes =
        hex::decode(&key).map_err(|err| reject::custom(InvalidParam(format!("{err}"))))?;
    Ok(reply::reply())
}

async fn handle_error(err: Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if let Some(e) = err.find::<InvalidParam>() {
        return Ok(reply::with_status(e.0.to_owned(), StatusCode::BAD_REQUEST));
    }
    Ok(reply::with_status(
        "INTERNAL SERVER ERROR".to_owned(),
        StatusCode::INTERNAL_SERVER_ERROR,
    ))
}

enum ClientHandling {
    NewClient(ClientId, NewResponseSender),
    ClientDisconnected(ClientId),
}

async fn responses(
    mut client_handler: Receiver<ClientHandling>,
    mut response_receiver: Receiver<(ClientId, HostResult)>,
) {
    let mut clients = HashMap::new();
    loop {
        tokio::select! {
        new_client = client_handler.recv() => {
            match new_client {
            Some(ClientHandling::NewClient(client_id, responses)) => {
                clients.insert(client_id, responses);
            }
            Some(ClientHandling::ClientDisconnected(client_id)) => {
                clients.remove(&client_id);
            }
            None => return,
            }
        }
        host_result = response_receiver.recv() => {
            match host_result {
            Some((client_id, response)) => {
                if let Some(ch) = clients.get_mut(&client_id) {
                if Sender::send(ch, response).await.is_err() {
                    log::error!("Tried to send an a response to an unregistered client");
                    return;
                }
                } else {
                   return;
                }
            }
            None => return,
            }
        }
        }
    }
}

#[async_trait::async_trait]
impl ClientEventsProxy for HttpGateway {
    async fn recv(&mut self) -> Result<(ClientId, ClientRequest), ClientError> {
        todo!()
    }

    async fn send(
        &mut self,
        client: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Result<(), ClientError> {
        Ok(())
    }

    fn cloned(&self) -> BoxedClient {
        todo!()
    }
}
