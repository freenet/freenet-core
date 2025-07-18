use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use freenet_stdlib::client_api::{ClientError, ErrorKind, HostResponse};
use freenet_stdlib::prelude::ContractInstanceId;
use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::client_events::{ClientEventsProxy, ClientId, OpenRequest};
use crate::server::HostCallbackResult;

use super::{errors::WebSocketApiError, path_handlers, AuthToken, ClientConnection};

mod v1;

#[derive(Clone)]
pub(super) struct HttpGatewayRequest(mpsc::Sender<ClientConnection>);

impl std::ops::Deref for HttpGatewayRequest {
    type Target = mpsc::Sender<ClientConnection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type AttestedContractMap = Arc<RwLock<HashMap<AuthToken, (ContractInstanceId, ClientId)>>>;

/// A gateway to access and interact with contracts through an HTTP interface.
pub(crate) struct HttpGateway {
    pub attested_contracts: AttestedContractMap,
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router(socket: &SocketAddr) -> (Self, Router) {
        let attested_contracts = Arc::new(RwLock::new(HashMap::new()));
        Self::as_router_with_attested_contracts(socket, attested_contracts)
    }

    /// Returns the uninitialized axum router with a provided attested_contracts map.
    pub fn as_router_with_attested_contracts(
        socket: &SocketAddr,
        attested_contracts: AttestedContractMap,
    ) -> (Self, Router) {
        Self::create_router_v1_with_attested_contracts(socket, attested_contracts)
    }
}

#[derive(Clone, Debug)]
struct Config {
    localhost: bool,
}

#[instrument(level = "debug")]
async fn home() -> axum::response::Response {
    axum::response::Response::default()
}

impl ClientEventsProxy for HttpGateway {
    #[instrument(level = "debug", skip(self))]
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async move {
            while let Some(msg) = self.proxy_server_request.recv().await {
                match msg {
                    ClientConnection::NewConnection {
                        callbacks,
                        assigned_token,
                    } => {
                        let cli_id = ClientId::next();
                        callbacks
                            .send(HostCallbackResult::NewId { id: cli_id })
                            .map_err(|_e| ErrorKind::NodeUnavailable)?;
                        if let Some((assigned_token, contract)) = assigned_token {
                            self.attested_contracts
                                .write()
                                .map_err(|_| ErrorKind::FailedOperation)?
                                .insert(assigned_token.clone(), (contract, cli_id));
                            tracing::debug!(
                                ?assigned_token,
                                ?contract,
                                ?cli_id,
                                "Stored assigned token in attested_contracts map"
                            );
                        }
                        self.response_channels.insert(cli_id, callbacks);
                        continue;
                    }
                    ClientConnection::Request {
                        client_id,
                        req,
                        auth_token,
                        attested_contract,
                    } => {
                        return Ok(OpenRequest::new(client_id, req)
                            .with_token(auth_token)
                            .with_attested_contract(attested_contract))
                    }
                }
            }
            tracing::warn!("Shutting down http gateway receiver");
            Err(ErrorKind::Disconnect.into())
        }
        .boxed()
    }

    #[instrument(level = "debug", skip(self))]
    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        async move {
            if let Some(ch) = self.response_channels.remove(&id) {
                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);
                if ch.send(HostCallbackResult::Result { id, result }).is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(id, ch);
                } else {
                    tracing::info!("dropped connection to client #{id}");
                }
            } else {
                tracing::warn!("client: {id} not found");
            }
            Ok(())
        }
        .boxed()
    }
}
