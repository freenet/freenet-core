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

#[instrument(level = "trace", name = "http_gateway_home")]
async fn home() -> axum::response::Response {
    trace!("Serving home endpoint");
    axum::response::Response::default()
}

impl ClientEventsProxy for HttpGateway {
    #[instrument(
        level = "debug",
        name = "http_gateway_recv",
        skip(self),
        fields(gateway_connections = %self.response_channels.len())
    )]
    fn recv(&mut self) -> BoxFuture<Result<OpenRequest<'static>, ClientError>> {
        async move {
            trace!("Waiting for incoming client connection");
            while let Some(msg) = self.proxy_server_request.recv().await {
                match msg {
                    ClientConnection::NewConnection {
                        callbacks,
                        assigned_token,
                    } => {
                        let cli_id = ClientId::next();
                        info!(client_id = %cli_id, "New client connection established");
                        
                        if let Some(ref token) = assigned_token {
                            trace!(client_id = %cli_id, token = ?token.0, "Connection has assigned token");
                        }
                        
                        match callbacks.send(HostCallbackResult::NewId { id: cli_id }) {
                            Ok(_) => debug!(client_id = %cli_id, "Sent client ID to new connection"),
                            Err(e) => {
                                error!(client_id = %cli_id, error = %e, "Failed to send client ID");
                                return Err(ErrorKind::NodeUnavailable.into());
                            }
                        }
                        
                        if let Some((assigned_token, contract)) = assigned_token {
                            info!(
                                client_id = %cli_id,
                                token = ?assigned_token,
                                contract = %contract,
                                "Registering attested contract"
                            );
                            
                            self.attested_contracts
                                .insert(assigned_token, (contract, cli_id));
                            
                            trace!(
                                client_id = %cli_id,
                                contract_count = self.attested_contracts.len(),
                                "Updated attested contracts map"
                            );
                        }
                        
                        debug!(client_id = %cli_id, "Storing response channel");
                        self.response_channels.insert(cli_id, callbacks);
                        continue;
                    }
                    ClientConnection::Request {
                        client_id,
                        req,
                        auth_token,
                    } => {
                        debug!(
                            client_id = %client_id,
                            auth_token = ?auth_token,
                            request_type = ?req.kind(),
                            "Processing client request"
                        );
                        return Ok(OpenRequest::new(client_id, req).with_token(auth_token));
                    }
                }
            }
            warn!("HTTP gateway receiver channel closed");
            Err(ErrorKind::Disconnect.into())
        }
        .boxed()
    }

    #[instrument(
        level = "debug",
        name = "http_gateway_send",
        skip(self, result),
        fields(client_id = %id, result_type = ?result.as_ref().map(|_| "success").unwrap_or("error"))
    )]
    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>> {
        async move {
            trace!(client_id = %id, "Preparing to send response");
            
            if let Some(ch) = self.response_channels.remove(&id) {
                // Check if this is a disconnect error
                let should_disconnect = match &result {
                    Err(err) if matches!(err.kind(), ErrorKind::Disconnect) => {
                        info!(client_id = %id, "Client disconnected");
                        true
                    },
                    Err(err) => {
                        debug!(client_id = %id, error = %err, "Sending error response");
                        false
                    },
                    Ok(_) => {
                        trace!(client_id = %id, "Sending successful response");
                        false
                    }
                };
                
                // Clean up attested contracts if client is disconnecting
                if should_disconnect {
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
                    
                    if !tokens_to_remove.is_empty() {
                        debug!(
                            client_id = %id,
                            token_count = tokens_to_remove.len(),
                            "Cleaning up attested contracts"
                        );
                        
                        for token in tokens_to_remove {
                            if let Some((contract, _)) = self.attested_contracts.remove(&token) {
                                trace!(
                                    client_id = %id,
                                    token = ?token,
                                    contract = %contract,
                                    "Removed attested contract mapping"
                                );
                            }
                        }
                    }
                }
                
                // Send the result and handle channel state
                match ch.send(HostCallbackResult::Result { id, result }) {
                    Ok(_) if !should_disconnect => {
                        trace!(client_id = %id, "Connection still active, preserving channel");
                        self.response_channels.insert(id, ch);
                    },
                    Ok(_) => {
                        info!(client_id = %id, "Response sent, client disconnected");
                    },
                    Err(_) => {
                        info!(client_id = %id, "Failed to send response, client disconnected");
                    }
                }
            } else {
                warn!(client_id = %id, "Client not found in response channels");
            }
            
            Ok(())
        }
        .boxed()
    }
}
