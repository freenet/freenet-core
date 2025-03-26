use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use freenet_stdlib::client_api::{ClientError, ErrorKind, HostResponse};
use freenet_stdlib::prelude::ContractInstanceId;
use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};

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

/// A gateway to access and interact with contracts through an HTTP interface.
///
/// Contracts initially accessed through the gateway have to be compliant with the container contract
/// [specification](https://docs.freenet.org/glossary.html#container-contract) for Locutus.
///
/// Check the Locutus book for [more information](https://docs.freenet.org/dev-guide.html).
pub(crate) struct HttpGateway {
    pub attested_contracts: HashMap<AuthToken, (ContractInstanceId, ClientId)>,
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router(socket: &SocketAddr) -> (Self, Router) {
        Self::as_router_v1(socket)
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
    fn recv(&mut self) -> BoxFuture<Result<OpenRequest<'static>, ClientError>> {
        async move {
            while let Some(msg) = self.proxy_server_request.recv().await {
                match msg {
                    ClientConnection::NewConnection {
                        callbacks,
                        assigned_token,
                    } => {
                        debug!("Received new connection request");
                        trace!("New connection assigned_token: {:?}", assigned_token);
                        
                        let cli_id = ClientId::next();
                        debug!("Generated client ID: {}", cli_id);
                        
                        callbacks
                            .send(HostCallbackResult::NewId { id: cli_id })
                            .map_err(|e| {
                                error!("Failed to send new ID to client: {:?}", e);
                                ErrorKind::NodeUnavailable
                            })?;
                        
                        if let Some((assigned_token, contract)) = assigned_token {
                            debug!(
                                "Registering attested contract: token={:?}, contract={:?}, client={}",
                                assigned_token, contract, cli_id
                            );
                            
                            self.attested_contracts
                                .insert(assigned_token, (contract, cli_id));
                            
                            debug!(
                                "Updated attested_contracts map: {} entries",
                                self.attested_contracts.len()
                            );
                            trace!("Full attested_contracts map: {:?}", self.attested_contracts);
                        }
                        
                        debug!("Storing response channel for client {}", cli_id);
                        self.response_channels.insert(cli_id, callbacks);
                        continue;
                    }
                    ClientConnection::Request {
                        client_id,
                        req,
                        auth_token,
                    } => {
                        debug!(
                            "Processing request from client {}, auth_token: {:?}",
                            client_id, auth_token
                        );
                        return Ok(OpenRequest::new(client_id, req).with_token(auth_token));
                    }
                }
            }
            warn!("Shutting down http gateway receiver");
            Err(ErrorKind::Disconnect.into())
        }
        .boxed()
    }

    #[instrument(level = "debug", skip(self, result))]
    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>> {
        async move {
            debug!("Sending response to client {}", id);
            
            if let Some(ch) = self.response_channels.remove(&id) {
                let should_rm = result
                    .as_ref()
                    .map_err(|err| {
                        let is_disconnect = matches!(err.kind(), ErrorKind::Disconnect);
                        if is_disconnect {
                            debug!("Client {} disconnected", id);
                        }
                        is_disconnect
                    })
                    .err()
                    .unwrap_or(false);
                
                // Check if we need to update attested_contracts on disconnect
                if should_rm {
                    debug!("Checking for attested contracts to clean up for client {}", id);
                    let tokens_to_remove: Vec<AuthToken> = self
                        .attested_contracts
                        .iter()
                        .filter_map(|(token, (_, client_id))| {
                            if *client_id == id {
                                Some(token.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    
                    for token in tokens_to_remove {
                        if let Some((contract, _)) = self.attested_contracts.remove(&token) {
                            debug!(
                                "Removed attested contract mapping: token={:?}, contract={:?}, client={}",
                                token, contract, id
                            );
                        }
                    }
                }
                
                if ch.send(HostCallbackResult::Result { id, result }).is_ok() && !should_rm {
                    // still alive connection, keep it
                    debug!("Connection to client {} still alive, keeping channel", id);
                    self.response_channels.insert(id, ch);
                } else {
                    info!("Dropped connection to client #{}", id);
                }
            } else {
                warn!("Client {} not found in response_channels", id);
            }
            
            Ok(())
        }
        .boxed()
    }
}
