use locutus_node::either::Either;

use locutus_node::*;
use locutus_runtime::{ContractKey, StateDelta};
use std::{
    collections::HashMap,
    future::Future,
    io::Read,
    path::PathBuf,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
use warp::hyper::body::Bytes;
use warp::ws::WebSocket;
use warp::{filters::BoxedFilter, reply, Filter, Rejection, Reply};

use crate::{
    errors,
    state_handling::{get_state, state_updates_notification, update_state},
    ClientHandlingMessage, HostResult, UnpackedState,
};

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

pub struct HttpGateway {
    server_request: mpsc::Receiver<ClientHandlingMessage>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized warp filter to compose with other routing handling or websockets.
    pub fn as_filter<F>(update_hook: F) -> (Self, BoxedFilter<(impl Reply + 'static,)>)
    where
        F: Fn(Vec<u8>) -> Result<StateDelta<'static>, Rejection> + Copy + Send + Sync + 'static,
    {
        let contract_web_path = std::env::temp_dir().join("locutus").join("webs");
        std::fs::create_dir_all(&contract_web_path).unwrap();

        let (request_sender, server_request) = mpsc::channel(PARALLELISM);
        let gateway = Self {
            server_request,
            response_channels: HashMap::new(),
        };

        let get_home = warp::path::end().and_then(home);

        let rs = request_sender.clone();
        let contract_home = warp::path::path("contract")
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path::end())
            .and_then(|rs, key: String| async move {
                crate::contract_web_handling::contract_home(key, rs).await
            });

        let contract_subpages = warp::path::path("contract")
            .and(warp::path::param())
            .and(warp::filters::path::full())
            .and_then(|key: String, path| async move {
                crate::contract_web_handling::variable_content(key, path).await
            });

        let rs = request_sender.clone();
        let state_update_notifications = warp::path::path("contract")
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path!("state" / "updates"))
            .and(warp::ws())
            .map(|rs, key: String, ws: warp::ws::Ws| {
                ws.on_upgrade(move |websocket: WebSocket| {
                    state_updates_notification(key, rs, websocket)
                })
            });

        let rs = request_sender.clone();
        let update_contract_state = warp::path::path("contract")
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path!("state" / "update"))
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .and_then(move |rs, key: String, update_val: Bytes| async move {
                let r = update_hook(update_val.to_vec())?;
                update_state(key, r, rs).await
            });

        let get_contract_state = warp::path::path("contract")
            .map(move || request_sender.clone())
            .and(warp::path::param())
            .and(warp::path!("state" / "get"))
            .and_then(|rs, key: String| async move { get_state(key, rs).await });

        let filters = get_home
            .or(contract_home)
            .or(get_contract_state)
            .or(state_update_notifications)
            .or(update_contract_state)
            .or(contract_subpages)
            .recover(errors::handle_error)
            .with(warp::trace::request());

        (gateway, filters.boxed())
    }

    pub fn store_web(
        state: &mut UnpackedState<impl Read>,
        key: &ContractKey,
    ) -> Result<(), ClientError> {
        let UnpackedState { web, .. } = state;
        let contract_web_path = Self::contract_web_path(key);
        web.unpack(contract_web_path)
            .map_err(|_| ErrorKind::IncorrectState(*key))?;
        Ok(())
    }

    pub fn contract_web_path(key: &ContractKey) -> PathBuf {
        std::env::temp_dir()
            .join("locutus")
            .join("webs")
            .join(key.encode())
    }
}

async fn home() -> Result<impl Reply, Rejection> {
    Ok(reply::reply())
}

#[derive(Debug)]
enum ExtractError {
    Io(std::io::Error),
    StripPrefixError(std::path::StripPrefixError),
}

impl From<std::io::Error> for ExtractError {
    fn from(error: std::io::Error) -> Self {
        ExtractError::Io(error)
    }
}

impl From<std::path::StripPrefixError> for ExtractError {
    fn from(error: std::path::StripPrefixError) -> Self {
        ExtractError::StripPrefixError(error)
    }
}

/// Each request is unique so we don't keep track of a client session of any sort.
static ID: AtomicUsize = AtomicUsize::new(0);

#[allow(clippy::needless_lifetimes)]
impl ClientEventsProxy for HttpGateway {
    fn recv<'a>(
        &'a mut self,
    ) -> Pin<
        Box<dyn Future<Output = Result<(ClientId, ClientRequest), ClientError>> + Send + Sync + '_>,
    > {
        Box::pin(async move {
            if let Some((req, ch_or_id)) = self.server_request.recv().await {
                match ch_or_id {
                    Either::Left(new_client_ch) => {
                        // is a new client, assign an id and open a channel to communicate responses from the node
                        tracing::debug!("received request: {req}");
                        let cli_id = ClientId::new(ID.fetch_add(1, Ordering::SeqCst));
                        self.response_channels.insert(cli_id, new_client_ch);
                        Ok((cli_id, req))
                    }
                    Either::Right(existing_client) => {
                        // just forward the request to the node
                        Ok((existing_client, req))
                    }
                }
            } else {
                todo!()
            }
        })
    }

    fn send<'a>(
        &'a mut self,
        client: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + '_>> {
        Box::pin(async move {
            if let Some(ch) = self.response_channels.remove(&client) {
                let should_rm = response
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);
                if ch.send((client, response)).is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(client, ch);
                }
            } else {
                log::warn!("client: {client} not found");
            }
            Ok(())
        })
    }

    fn cloned(&self) -> BoxedClient {
        unimplemented!()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{fs::File, io::Read, path::PathBuf};

    use super::*;

    fn _test_state() -> Result<WrappedState, std::io::Error> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let path = PathBuf::from(CRATE_DIR).join("tests/encoded_state");
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(WrappedState::new(bytes))
    }
}
