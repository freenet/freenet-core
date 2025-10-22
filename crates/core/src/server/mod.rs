//! Handles external client connections (HTTP/WebSocket).
//!
//! This module acts as the bridge between external clients and the Freenet node's core logic.
//! It parses `ClientRequest`s, sends them to the main node event loop (`node::Node`) via an
//! internal channel, and forwards `HostResponse`s back to the clients.
//!
//! See [`../architecture.md`](../architecture.md) for its place in the overall architecture.

pub(crate) mod app_packaging;
pub(crate) mod errors;
pub(crate) mod http_gateway;
pub(crate) mod path_handlers;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::*,
};

use http_gateway::HttpGateway;
use tower_http::trace::TraceLayer;

use crate::{
    client_events::{websocket::WebSocketProxy, AuthToken, BoxedClient, ClientId, HostResult},
    config::WebsocketApiConfig,
};

pub use app_packaging::WebApp;

// Export types needed for integration testing
pub use http_gateway::{AttestedContract, AttestedContractMap};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientConnection {
    NewConnection {
        callbacks: tokio::sync::mpsc::UnboundedSender<HostCallbackResult>,
        assigned_token: Option<(AuthToken, ContractInstanceId)>,
    },
    Request {
        client_id: ClientId,
        req: Box<ClientRequest<'static>>,
        auth_token: Option<AuthToken>,
        attested_contract: Option<ContractInstanceId>,
    },
}

#[derive(Debug)]
pub(crate) enum HostCallbackResult {
    NewId {
        id: ClientId,
    },
    Result {
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    },
    SubscriptionChannel {
        id: ClientId,
        key: ContractKey,
        callback: tokio::sync::mpsc::UnboundedReceiver<HostResult>,
    },
}

fn serve(socket: SocketAddr, router: axum::Router) {
    tokio::spawn(async move {
        tracing::info!("HTTP gateway listening on {}", socket);
        let listener = tokio::net::TcpListener::bind(socket).await.unwrap();
        axum::serve(listener, router).await.map_err(|e| {
            tracing::error!("Error while running HTTP gateway server: {e}");
        })
    });
}

pub mod local_node {
    use freenet_stdlib::client_api::{ClientRequest, ErrorKind};
    use std::net::{IpAddr, SocketAddr};
    use tower_http::trace::TraceLayer;

    use crate::{
        client_events::{websocket::WebSocketProxy, ClientEventsProxy, OpenRequest},
        contract::{Executor, ExecutorError},
    };

    use super::{http_gateway::HttpGateway, serve};

    pub async fn run_local_node(mut executor: Executor, socket: SocketAddr) -> anyhow::Result<()> {
        match socket.ip() {
            IpAddr::V4(ip) if !ip.is_loopback() => {
                anyhow::bail!("invalid ip: {ip}, expecting localhost")
            }
            IpAddr::V6(ip) if !ip.is_loopback() => {
                anyhow::bail!("invalid ip: {ip}, expecting localhost")
            }
            _ => {}
        }
        let (mut gw, gw_router) = HttpGateway::as_router(&socket);
        let (mut ws_proxy, ws_router) = WebSocketProxy::create_router(gw_router);

        serve(socket, ws_router.layer(TraceLayer::new_for_http()));

        // TODO: use combinator instead
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        enum Receiver {
            Ws,
            Gw,
        }
        let mut receiver;
        loop {
            let req = tokio::select! {
                req = ws_proxy.recv() => {
                    receiver = Receiver::Ws;
                    req?
                }
                req = gw.recv() => {
                    receiver = Receiver::Gw;
                    req?
                }
            };
            let OpenRequest {
                client_id: id,
                request,
                notification_channel,
                token,
                ..
            } = req;
            tracing::trace!(cli_id = %id, "got request -> {request}");

            let res = match *request {
                ClientRequest::ContractOp(op) => {
                    executor
                        .contract_requests(op, id, notification_channel)
                        .await
                }
                ClientRequest::DelegateOp(op) => {
                    let attested_contract = token.and_then(|token| {
                        gw.attested_contracts
                            .get(&token)
                            .map(|entry| entry.contract_id)
                    });
                    executor.delegate_request(op, attested_contract.as_ref())
                }
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        tracing::info!("disconnecting cause: {cause}");
                    }
                    // fixme: token must live for a bit to allow reconnections
                    if let Some(rm_token) = gw.attested_contracts.iter().find_map(|entry| {
                        let (k, attested) = entry.pair();
                        (attested.client_id == id).then(|| k.clone())
                    }) {
                        gw.attested_contracts.remove(&rm_token);
                    }
                    continue;
                }
                _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
            };

            match res {
                Ok(res) => {
                    match receiver {
                        Receiver::Ws => ws_proxy.send(id, Ok(res)).await?,
                        Receiver::Gw => gw.send(id, Ok(res)).await?,
                    };
                }
                Err(err) if err.is_request() => {
                    let err = ErrorKind::RequestError(err.unwrap_request());
                    match receiver {
                        Receiver::Ws => {
                            ws_proxy.send(id, Err(err.into())).await?;
                        }
                        Receiver::Gw => {
                            gw.send(id, Err(err.into())).await?;
                        }
                    };
                }
                Err(err) => {
                    tracing::error!("{err}");
                    let err = Err(ErrorKind::Unhandled {
                        cause: format!("{err}").into(),
                    }
                    .into());
                    match receiver {
                        Receiver::Ws => {
                            ws_proxy.send(id, err).await?;
                        }
                        Receiver::Gw => {
                            gw.send(id, err).await?;
                        }
                    };
                }
            }
        }
    }
}

pub async fn serve_gateway(config: WebsocketApiConfig) -> [BoxedClient; 2] {
    let (gw, ws_proxy) = serve_gateway_in(config).await;
    [Box::new(gw), Box::new(ws_proxy)]
}

/// Serves the gateway and returns the concrete types (for integration testing).
/// This allows tests to access internal state like the attested_contracts map.
pub async fn serve_gateway_for_test(
    config: WebsocketApiConfig,
) -> (
    http_gateway::HttpGateway,
    crate::client_events::websocket::WebSocketProxy,
) {
    serve_gateway_in(config).await
}

pub(crate) async fn serve_gateway_in(config: WebsocketApiConfig) -> (HttpGateway, WebSocketProxy) {
    let ws_socket = (config.address, config.port).into();

    // Create a shared attested_contracts map with token expiration support
    let attested_contracts: AttestedContractMap = Arc::new(DashMap::new());

    // Spawn background task to clean up expired tokens
    spawn_token_cleanup_task(
        attested_contracts.clone(),
        config.token_ttl_seconds,
        config.token_cleanup_interval_seconds,
    );

    // Pass the shared map to both HttpGateway and WebSocketProxy
    let (gw, gw_router) =
        HttpGateway::as_router_with_attested_contracts(&ws_socket, attested_contracts.clone());
    let (ws_proxy, ws_router) =
        WebSocketProxy::create_router_with_attested_contracts(gw_router, attested_contracts);

    serve(ws_socket, ws_router.layer(TraceLayer::new_for_http()));
    (gw, ws_proxy)
}

/// Spawns a background task that periodically removes expired authentication tokens.
///
/// Tokens that haven't been used for the specified TTL duration will be removed from the map.
/// This prevents memory leaks and ensures old tokens don't remain valid indefinitely.
///
/// # Arguments
/// * `attested_contracts` - The shared map of authentication tokens
/// * `token_ttl_seconds` - How long tokens remain valid without activity (in seconds)
/// * `cleanup_interval_seconds` - How often to run the cleanup task (in seconds)
fn spawn_token_cleanup_task(
    attested_contracts: AttestedContractMap,
    token_ttl_seconds: u64,
    cleanup_interval_seconds: u64,
) {
    let token_ttl = Duration::from_secs(token_ttl_seconds);
    let cleanup_interval = Duration::from_secs(cleanup_interval_seconds);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.tick().await; // Skip the first immediate tick

        loop {
            interval.tick().await;

            // Clean up expired tokens
            let now = Instant::now();
            let initial_count = attested_contracts.len();

            // Remove tokens that haven't been accessed in token_ttl
            attested_contracts.retain(|token, attested| {
                let elapsed = now.duration_since(attested.last_accessed);
                let should_keep = elapsed < token_ttl;

                if !should_keep {
                    tracing::info!(
                        ?token,
                        contract_id = ?attested.contract_id,
                        client_id = ?attested.client_id,
                        elapsed_hours = elapsed.as_secs() / 3600,
                        "Removing expired authentication token"
                    );
                }

                should_keep
            });

            let removed_count = initial_count - attested_contracts.len();
            if removed_count > 0 {
                tracing::debug!(
                    removed_count,
                    remaining_count = attested_contracts.len(),
                    "Token cleanup completed"
                );
            }
        }
    });
}
