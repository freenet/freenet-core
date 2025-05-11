use std::{fmt::Display, net::Ipv4Addr, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dashmap::DashMap;
use freenet::generated::{
    topology::ControllerResponse, ChangesWrapper, ContractChange, PeerChange, TryFromFbs,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

mod v1;

/// Network metrics server. Records metrics and data from a test network that can be used for
/// analysis and visualization.
#[derive(clap::Parser, Clone)]
pub struct ServerConfig {
    /// If provided, the server will save the event logs in this directory.
    #[arg(long)]
    pub log_directory: Option<PathBuf>,
}

/// Starts the server and returns a handle to the server thread
/// and a handle to the  changes recorder thread if changes record path was provided.
pub async fn start_server(
    config: &ServerConfig,
) -> (
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
) {
    let changes_record_path = config.log_directory.clone();
    let (changes, rx) = tokio::sync::broadcast::channel(10000);
    let changes_recorder = changes_record_path.map(|data_dir| {
        tokio::task::spawn(async move {
            if let Err(err) = crate::network_metrics_server::record_saver(data_dir, rx).await {
                tracing::error!(error = %err, "Record saver failed");
            }
        })
    });
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let barrier_cp = barrier.clone();
    let server = tokio::task::spawn(async move {
        if let Err(err) = crate::network_metrics_server::run_server(barrier_cp, changes).await {
            tracing::error!(error = %err, "Network metrics server failed");
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    barrier.wait().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    (server, changes_recorder)
}

async fn run_server(
    barrier: Arc<tokio::sync::Barrier>,
    changes: tokio::sync::broadcast::Sender<Change>,
) -> anyhow::Result<()> {
    v1::run_server(barrier, changes).await
}

async fn push_stats(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
) -> axum::response::Response {
    let on_upgrade = move |ws: WebSocket| async move {
        if let Err(error) = push_interface(ws, state).await {
            tracing::error!("{error}");
        }
    };
    ws.on_upgrade(on_upgrade)
}

async fn push_interface(ws: WebSocket, state: Arc<ServerState>) -> anyhow::Result<()> {
    let (mut tx, mut rx) = ws.split();
    while let Some(msg) = rx.next().await {
        let received_random_id = rand::random::<u64>();
        match msg {
            Ok(msg) => {
                let msg = match msg {
                    Message::Binary(data) => data,
                    Message::Text(data) => data.into_bytes(),
                    Message::Close(_) => break,
                    Message::Ping(ping) => {
                        tx.send(Message::Pong(ping)).await?;
                        continue;
                    }
                    _ => continue,
                };

                let mut decoding_errors = vec![];

                match ContractChange::try_decode_fbs(&msg) {
                    Ok(ContractChange::PutFailure(_err)) => todo!(),
                    Ok(change) => {
                        if let Err(err) = state.save_record(ChangesWrapper::ContractChange(change))
                        {
                            tracing::error!(error = %err, "Failed saving report");
                            tx.send(Message::Binary(ControllerResponse::into_fbs_bytes(Err(
                                format!("{err}"),
                            ))))
                            .await?;
                        }
                        continue;
                    }
                    Err(decoding_error) => {
                        tracing::warn!(%received_random_id, error = %decoding_error, "Failed to decode message from 1st ContractChange");
                        decoding_errors.push(decoding_error.to_string());
                    }
                }

                match PeerChange::try_decode_fbs(&msg) {
                    Ok(PeerChange::Error(err)) => {
                        tracing::error!(error = %err.message(), "Received error from peer");
                        break;
                    }
                    Ok(change) => {
                        if let Err(err) = state.save_record(ChangesWrapper::PeerChange(change)) {
                            tracing::error!(error = %err, "Failed saving report");
                            tx.send(Message::Binary(ControllerResponse::into_fbs_bytes(Err(
                                format!("{err}"),
                            ))))
                            .await?;
                        }
                        continue;
                    }
                    Err(decoding_error) => {
                        tracing::warn!(error = %decoding_error, "Failed to decode message");
                        decoding_errors.push(decoding_error.to_string());
                    }
                }

                tracing::error!(%received_random_id, "The message was not decoded by any fbs type");
                tx.send(Message::Binary(ControllerResponse::into_fbs_bytes(Err(
                    decoding_errors.join(", "),
                ))))
                .await?;
            }
            Err(e) => {
                tracing::debug!("Websocket error: {}", e);
                break;
            }
        }
    }
    Ok(())
}

async fn pull_peer_changes(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
) -> axum::response::Response {
    let on_upgrade = move |ws: WebSocket| async move {
        if let Err(error) = pull_interface(ws, state).await {
            tracing::error!("{error}");
        }
    };
    ws.on_upgrade(on_upgrade)
}

async fn pull_interface(ws: WebSocket, state: Arc<ServerState>) -> anyhow::Result<()> {
    let (mut tx, _) = ws.split();
    for peer in state.peer_data.iter() {
        let msg = PeerChange::current_state_msg(
            peer.key().clone(),
            peer.value().location,
            peer.value().connections.iter(),
        );
        tx.send(Message::Binary(msg)).await?;
    }

    for transaction in state.transactions_data.iter() {
        tracing::info!("sending transaction data");
        for change in transaction.value() {
            let msg = match change {
                Change::PutRequest {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                } => {
                    tracing::info!("sending put request");
                    ContractChange::put_request_msg(
                        tx_id.clone(),
                        key.to_string(),
                        requester.to_string(),
                        target.to_string(),
                        *timestamp,
                        *contract_location,
                    )
                }
                Change::PutSuccess {
                    tx_id,
                    key,
                    target,
                    timestamp,
                    requester,
                    contract_location,
                } => {
                    tracing::info!("sending put success");
                    ContractChange::put_success_msg(
                        tx_id.clone(),
                        key.to_string(),
                        requester.to_string(),
                        target.to_string(),
                        *timestamp,
                        *contract_location,
                    )
                }
                Change::BroadcastEmitted {
                    tx_id,
                    upstream,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    sender,
                    timestamp,
                    contract_location,
                } => {
                    tracing::info!("sending broadcast emitted");
                    ContractChange::broadcast_emitted_msg(
                        tx_id.clone(),
                        upstream,
                        broadcast_to.iter().map(|s| s.to_string()).collect(),
                        *broadcasted_to,
                        key,
                        sender,
                        *timestamp,
                        *contract_location,
                    )
                }
                Change::BroadcastReceived {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                } => {
                    tracing::info!("sending broadcast received");
                    ContractChange::broadcast_received_msg(
                        tx_id,
                        target,
                        requester,
                        key,
                        *timestamp,
                        *contract_location,
                    )
                }

                Change::GetContract {
                    requester,
                    transaction,
                    key,
                    contract_location,
                    timestamp,
                    target,
                } => {
                    tracing::info!("sending get contract");
                    ContractChange::get_contract_msg(
                        requester,
                        target,
                        transaction,
                        key,
                        *contract_location,
                        *timestamp,
                    )
                }
                Change::SubscribedToContract {
                    requester,
                    transaction,
                    key,
                    contract_location,
                    at_peer,
                    at_peer_location,
                    timestamp,
                } => {
                    tracing::info!("sending subscribed to contract");
                    ContractChange::subscribed_msg(
                        requester,
                        transaction,
                        key,
                        *contract_location,
                        at_peer,
                        *at_peer_location,
                        *timestamp,
                    )
                }

                Change::UpdateRequest {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                } => {
                    tracing::info!("sending update request");
                    ContractChange::update_request_msg(
                        tx_id.clone(),
                        key,
                        requester,
                        target,
                        *timestamp,
                        *contract_location,
                    )
                }
                Change::UpdateSuccess {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                } => {
                    tracing::info!("sending update success");
                    ContractChange::update_success_msg(
                        tx_id.clone(),
                        key,
                        requester,
                        target,
                        *timestamp,
                        *contract_location,
                    )
                }
                Change::UpdateFailure {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                } => {
                    tracing::info!("sending update failure");
                    ContractChange::update_failure_msg(
                        tx_id.clone(),
                        key,
                        requester,
                        target,
                        *timestamp,
                        *contract_location,
                    )
                }

                _ => continue,
            };
            tx.send(Message::Binary(msg)).await?;
        }
    }

    let mut changes = state.changes.subscribe();
    while let Ok(msg) = changes.recv().await {
        match msg {
            Change::AddedConnection {
                transaction,
                from,
                to,
            } => {
                let msg = PeerChange::added_connection_msg(
                    transaction.as_ref(),
                    (from.0 .0, from.1),
                    (to.0 .0, to.1),
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::RemovedConnection { from, at } => {
                let msg = PeerChange::removed_connection_msg(at.0, from.0);
                tx.send(Message::Binary(msg)).await?;
            }
            Change::PutRequest {
                tx_id,
                key,
                requester,
                target,
                timestamp,
                contract_location,
            } => {
                tracing::debug!(%tx_id, %key, %requester, %target, "sending put request");
                let msg = ContractChange::put_request_msg(
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::PutSuccess {
                tx_id,
                key,
                target,
                requester,
                timestamp,
                contract_location,
            } => {
                tracing::debug!(%tx_id, %key, %requester, %target, "sending put success");
                let msg = ContractChange::put_success_msg(
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::BroadcastEmitted {
                tx_id,
                upstream,
                broadcast_to,
                broadcasted_to,
                key,
                sender,
                timestamp,
                contract_location,
            } => {
                let msg = ContractChange::broadcast_emitted_msg(
                    tx_id,
                    upstream,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    sender,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::BroadcastReceived {
                tx_id,
                key,
                requester,
                target,
                timestamp,
                contract_location,
            } => {
                let msg = ContractChange::broadcast_received_msg(
                    tx_id,
                    target,
                    requester,
                    key,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::GetContract {
                requester,
                transaction,
                key,
                contract_location,
                timestamp,
                target,
            } => {
                let msg = ContractChange::get_contract_msg(
                    requester,
                    target,
                    transaction,
                    key,
                    contract_location,
                    timestamp,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::SubscribedToContract {
                requester,
                transaction,
                key,
                contract_location,
                at_peer,
                at_peer_location,
                timestamp,
            } => {
                let msg = ContractChange::subscribed_msg(
                    requester,
                    transaction,
                    key,
                    contract_location,
                    at_peer,
                    at_peer_location,
                    timestamp,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::UpdateRequest {
                tx_id,
                key,
                requester,
                target,
                timestamp,
                contract_location,
            } => {
                let msg = ContractChange::update_request_msg(
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::UpdateSuccess {
                tx_id,
                key,
                requester,
                target,
                timestamp,
                contract_location,
            } => {
                let msg = ContractChange::update_success_msg(
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
            Change::UpdateFailure {
                tx_id,
                key,
                requester,
                target,
                timestamp,
                contract_location,
            } => {
                let msg = ContractChange::update_failure_msg(
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                );
                tx.send(Message::Binary(msg)).await?;
            }
        }
    }
    Ok(())
}

struct ServerState {
    changes: tokio::sync::broadcast::Sender<Change>,
    peer_data: DashMap<String, PeerData>,
    transactions_data: DashMap<String, Vec<Change>>,
    contract_data: DashMap<String, ContractData>,
}

struct PeerData {
    connections: Vec<(String, f64)>,
    location: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContractData {
    location: f64,
    connections: Vec<String>,
    key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Change {
    AddedConnection {
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        transaction: Option<String>,
        from: (PeerIdHumanReadable, f64),
        to: (PeerIdHumanReadable, f64),
    },
    RemovedConnection {
        from: PeerIdHumanReadable,
        at: PeerIdHumanReadable,
    },
    PutRequest {
        tx_id: String,
        key: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    },
    PutSuccess {
        tx_id: String,
        key: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    },
    BroadcastEmitted {
        tx_id: String,
        upstream: String,
        broadcast_to: Vec<String>,
        broadcasted_to: usize,
        key: String,
        sender: String,
        timestamp: u64,
        contract_location: f64,
    },
    BroadcastReceived {
        tx_id: String,
        key: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    },
    GetContract {
        requester: String,
        transaction: String,
        key: String,
        contract_location: f64,
        timestamp: u64,
        target: String,
    },
    SubscribedToContract {
        requester: String,
        transaction: String,
        key: String,
        contract_location: f64,
        at_peer: String,
        at_peer_location: f64,
        timestamp: u64,
    },
    UpdateRequest {
        tx_id: String,
        key: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    },
    UpdateSuccess {
        tx_id: String,
        key: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    },
    UpdateFailure {
        tx_id: String,
        key: String,
        requester: String,
        target: String,
        timestamp: u64,
        contract_location: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct PeerIdHumanReadable(String);

impl Display for PeerIdHumanReadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl From<String> for PeerIdHumanReadable {
    fn from(peer_id: String) -> Self {
        Self(peer_id)
    }
}

impl ServerState {
    fn save_record(&self, change: ChangesWrapper) -> Result<(), anyhow::Error> {
        match change {
            ChangesWrapper::PeerChange(PeerChange::AddedConnection(added)) => {
                let from_peer_id = String::from_utf8(added.from().bytes().to_vec())?;
                let from_loc = added.from_location();

                let to_peer_id = String::from_utf8(added.to().bytes().to_vec())?;
                let to_loc = added.to_location();

                tracing::info!(%from_peer_id, %to_peer_id, "--addedconnection adding connection");

                match self.peer_data.entry(from_peer_id.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        let connections = &mut occ.get_mut().connections;
                        connections.push((to_peer_id.clone(), to_loc));
                        connections.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                        connections.dedup();
                    }
                    dashmap::mapref::entry::Entry::Vacant(vac) => {
                        vac.insert(PeerData {
                            connections: vec![(to_peer_id.clone(), to_loc)],
                            location: from_loc,
                        });
                    }
                }

                match self.peer_data.entry(to_peer_id.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        let connections = &mut occ.get_mut().connections;
                        connections.push((from_peer_id.clone(), from_loc));
                        connections.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                        connections.dedup();
                    }
                    dashmap::mapref::entry::Entry::Vacant(vac) => {
                        vac.insert(PeerData {
                            connections: vec![(from_peer_id.clone(), from_loc)],
                            location: to_loc,
                        });
                    }
                }

                let _ = self.changes.send(Change::AddedConnection {
                    transaction: added.transaction().map(|s| s.to_owned()),
                    from: (from_peer_id.into(), from_loc),
                    to: (to_peer_id.into(), to_loc),
                });
            }
            ChangesWrapper::PeerChange(PeerChange::RemovedConnection(removed)) => {
                let from_peer_id = String::from_utf8(removed.from().bytes().to_vec())?;
                let at_peer_id = String::from_utf8(removed.at().bytes().to_vec())?;

                if let Some(mut entry) = self.peer_data.get_mut(&from_peer_id) {
                    entry
                        .connections
                        .retain(|(peer_id, _)| peer_id != &at_peer_id);
                }

                if let Some(mut entry) = self.peer_data.get_mut(&at_peer_id) {
                    entry
                        .connections
                        .retain(|(peer_id, _)| peer_id != &from_peer_id);
                }

                let _ = self.changes.send(Change::RemovedConnection {
                    from: from_peer_id.into(),
                    at: at_peer_id.into(),
                });
            }
            ChangesWrapper::ContractChange(ContractChange::PutRequest(change)) => {
                let tx_id = change.transaction().to_string();
                let key = change.key().to_string();
                let requester = change.requester().to_string();
                let target = change.target().to_string();
                let timestamp = change.timestamp();
                let contract_location = change.contract_location();

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                if let Some(_entry) = self.transactions_data.get_mut(&tx_id) {
                    tracing::error!("this tx should not be included on transactions_data");
                } else {
                    self.transactions_data.insert(
                        tx_id.clone(),
                        vec![Change::PutRequest {
                            tx_id: tx_id.clone(),
                            key: key.clone(),
                            requester: requester.clone(),
                            target: target.clone(),
                            timestamp,
                            contract_location,
                        }],
                    );
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- putrequest");

                let _ = self.changes.send(Change::PutRequest {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                });
            }
            ChangesWrapper::ContractChange(ContractChange::PutSuccess(change)) => {
                let tx_id = change.transaction().to_string();
                let key = change.key().to_string();
                let requester = change.requester().to_string();
                let target = change.target().to_string();
                let timestamp = change.timestamp();
                let contract_location = change.contract_location();

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                match self.transactions_data.entry(tx_id.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        tracing::info!("found transaction data, adding PutSuccess to changes");
                        let changes = occ.get_mut();
                        changes.push(Change::PutSuccess {
                            tx_id: tx_id.clone(),
                            key: key.clone(),
                            target: target.clone(),
                            requester: requester.clone(),
                            timestamp,
                            contract_location,
                        });
                        //connections.sort_unstable_by(|a, b| a.cmp(&b.0));
                        //connections.dedup();
                    }
                    dashmap::mapref::entry::Entry::Vacant(_vac) => {
                        // this should not happen
                        tracing::error!("this tx should be included on transactions_data. It should exists a PutRequest before the PutSuccess.");
                    }
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- putsuccess");

                match self.contract_data.entry(key.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        let connections = &mut occ.get_mut().connections;

                        connections.push(target.clone());
                        //connections.sort_unstable_by(|a, b| a.cmp(&b.0));
                        //connections.dedup();
                    }
                    dashmap::mapref::entry::Entry::Vacant(vac) => {
                        vac.insert(ContractData {
                            connections: vec![target.clone()],
                            location: contract_location,
                            key: key.clone(),
                        });
                    }
                }

                tracing::debug!("after contract_data updates");

                let _ = self.changes.send(Change::PutSuccess {
                    tx_id,
                    key,
                    target,
                    requester,
                    timestamp,
                    contract_location,
                });
            }

            ChangesWrapper::ContractChange(ContractChange::BroadcastEmitted(broadcast_data)) => {
                let tx_id = broadcast_data.transaction().to_string();
                let upstream = broadcast_data.upstream().to_string();
                let broadcast_to = broadcast_data
                    .broadcast_to()
                    .unwrap()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>();
                let broadcasted_to = broadcast_data.broadcasted_to();
                let key = broadcast_data.key().to_string();
                let sender = broadcast_data.sender().to_string();

                let timestamp = broadcast_data.timestamp();
                let contract_location = broadcast_data.contract_location();

                if broadcast_to.is_empty() {
                    return Err(anyhow::anyhow!("broadcast_to is empty"));
                }

                tracing::info!(?broadcast_to, "save_record broadcast_to");

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                match self.transactions_data.entry(tx_id.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        let changes = occ.get_mut();
                        changes.push(Change::BroadcastEmitted {
                            tx_id: tx_id.clone(),
                            upstream: upstream.clone(),
                            broadcast_to: broadcast_to.clone(),
                            broadcasted_to: broadcasted_to as usize,
                            key: key.clone(),
                            sender: sender.clone(),
                            timestamp,
                            contract_location,
                        });

                        //connections.sort_unstable_by(|a, b| a.cmp(&b.0));
                        //connections.dedup();
                    }
                    dashmap::mapref::entry::Entry::Vacant(_vac) => {
                        // this should not happen
                        unreachable!("this tx should be included on transactions_data. It should exists a PutRequest before BroadcastEmitted.");
                    }
                }

                tracing::debug!(%tx_id, %key, %upstream, %sender, "checking values from save_record -- broadcastemitted");

                let _ = self.changes.send(Change::BroadcastEmitted {
                    tx_id,
                    upstream,
                    broadcast_to,
                    broadcasted_to: broadcasted_to as usize,
                    key,
                    sender,
                    timestamp,
                    contract_location,
                });
            }

            ChangesWrapper::ContractChange(ContractChange::BroadcastReceived(broadcast_data)) => {
                let tx_id = broadcast_data.transaction().to_string();
                let key = broadcast_data.key().to_string();
                let requester = broadcast_data.requester().to_string();
                let target = broadcast_data.target().to_string();
                let timestamp = broadcast_data.timestamp();
                let contract_location = broadcast_data.contract_location();

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                match self.transactions_data.entry(tx_id.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        let changes = occ.get_mut();
                        changes.push(Change::BroadcastReceived {
                            tx_id: tx_id.clone(),
                            key: key.clone(),
                            requester: requester.clone(),
                            target: target.clone(),
                            timestamp,
                            contract_location,
                        });

                        //connections.sort_unstable_by(|a, b| a.cmp(&b.0));
                        //connections.dedup();
                    }
                    dashmap::mapref::entry::Entry::Vacant(_vac) => {
                        // this should not happen
                        tracing::error!("this tx should be included on transactions_data. It should exists a PutRequest before BroadcastReceived.");
                    }
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- broadcastreceived");

                let _ = self.changes.send(Change::BroadcastReceived {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                });
            }

            ChangesWrapper::ContractChange(ContractChange::GetContract(get_contract_data)) => {
                let requester = get_contract_data.requester().to_string();
                let transaction = get_contract_data.transaction().to_string();
                let key = get_contract_data.key().to_string();
                let contract_location = get_contract_data.contract_location();
                let timestamp = get_contract_data.timestamp();
                let target = get_contract_data.target().to_string();

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                if transaction.is_empty() {
                    return Err(anyhow::anyhow!("transaction is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if contract_location.is_nan() {
                    return Err(anyhow::anyhow!("contract_location is not a number"));
                }

                if timestamp == 0 {
                    return Err(anyhow::anyhow!("timestamp is invalid"));
                }

                if target.is_empty() {
                    return Err(anyhow::anyhow!("target is empty"));
                }

                if let Some(mut transactions) = self.transactions_data.get_mut(&transaction) {
                    transactions.push(Change::GetContract {
                        requester: requester.clone(),
                        transaction: transaction.clone(),
                        key: key.clone(),
                        contract_location,
                        timestamp,
                        target: target.clone(),
                    });
                } else {
                    self.transactions_data.insert(
                        transaction.clone(),
                        vec![Change::GetContract {
                            requester: requester.clone(),
                            transaction: transaction.clone(),
                            key: key.clone(),
                            contract_location,
                            timestamp,
                            target: target.clone(),
                        }],
                    );
                }

                tracing::debug!(%key, %contract_location, "checking values from save_record -- get_contract");

                let _ = self.changes.send(Change::GetContract {
                    requester,
                    transaction,
                    key,
                    contract_location,
                    timestamp,
                    target,
                });
            }
            ChangesWrapper::ContractChange(ContractChange::SubscribedToContract(
                subscribe_data,
            )) => {
                let requester = subscribe_data.requester().to_string();
                let transaction = subscribe_data.transaction().to_string();
                let key = subscribe_data.key().to_string();
                let contract_location = subscribe_data.contract_location();
                let at_peer = subscribe_data.at_peer().to_string();
                let at_peer_location = subscribe_data.at_peer_location();
                let timestamp = subscribe_data.timestamp();

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                if transaction.is_empty() {
                    return Err(anyhow::anyhow!("transaction is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if at_peer.is_empty() {
                    return Err(anyhow::anyhow!("at_peer is empty"));
                }

                if contract_location.is_nan() {
                    return Err(anyhow::anyhow!("contract_location is not a number"));
                }

                if timestamp == 0 {
                    return Err(anyhow::anyhow!("timestamp is invalid"));
                }

                if let Some(mut transactions_list) = self.transactions_data.get_mut(&transaction) {
                    transactions_list.push(Change::SubscribedToContract {
                        requester: requester.clone(),
                        transaction: transaction.clone(),
                        key: key.clone(),
                        contract_location,
                        at_peer: at_peer.clone(),
                        at_peer_location,
                        timestamp,
                    });
                } else {
                    self.transactions_data.insert(
                        transaction.clone(),
                        vec![Change::SubscribedToContract {
                            requester: requester.clone(),
                            transaction: transaction.clone(),
                            key: key.clone(),
                            contract_location,
                            at_peer: at_peer.clone(),
                            at_peer_location,
                            timestamp,
                        }],
                    );
                }

                tracing::debug!(%key, %contract_location, "checking values from save_record -- subscribed_to msg");

                let _ = self.changes.send(Change::SubscribedToContract {
                    requester,
                    transaction,
                    key,
                    contract_location,
                    at_peer,
                    at_peer_location,
                    timestamp,
                });
            }

            ChangesWrapper::ContractChange(ContractChange::UpdateRequest(update_request)) => {
                let tx_id = update_request.transaction().to_string();
                let key = update_request.key().to_string();
                let requester = update_request.requester().to_string();
                let target = update_request.target().to_string();
                let timestamp = update_request.timestamp();
                let contract_location = update_request.contract_location();

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                if target.is_empty() {
                    return Err(anyhow::anyhow!("target is empty"));
                }

                if let Some(mut transactions) = self.transactions_data.get_mut(&tx_id) {
                    transactions.push(Change::UpdateRequest {
                        tx_id: tx_id.clone(),
                        requester: requester.clone(),
                        key: key.clone(),
                        contract_location,
                        timestamp,
                        target: target.clone(),
                    });
                } else {
                    self.transactions_data.insert(
                        tx_id.clone(),
                        vec![Change::UpdateRequest {
                            tx_id: tx_id.clone(),
                            requester: requester.clone(),
                            key: key.clone(),
                            contract_location,
                            timestamp,
                            target: target.clone(),
                        }],
                    );
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- updaterequest");

                let _ = self.changes.send(Change::UpdateRequest {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                });
            }
            ChangesWrapper::ContractChange(ContractChange::UpdateSuccess(update_success)) => {
                let tx_id = update_success.transaction().to_string();
                let key = update_success.key().to_string();
                let requester = update_success.requester().to_string();
                let target = update_success.target().to_string();
                let timestamp = update_success.timestamp();
                let contract_location = update_success.contract_location();

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                if target.is_empty() {
                    return Err(anyhow::anyhow!("target is empty"));
                }

                if let Some(mut transactions) = self.transactions_data.get_mut(&tx_id) {
                    transactions.push(Change::UpdateSuccess {
                        tx_id: tx_id.clone(),
                        requester: requester.clone(),
                        key: key.clone(),
                        contract_location,
                        timestamp,
                        target: target.clone(),
                    });
                } else {
                    self.transactions_data.insert(
                        tx_id.clone(),
                        vec![Change::UpdateSuccess {
                            tx_id: tx_id.clone(),
                            requester: requester.clone(),
                            key: key.clone(),
                            contract_location,
                            timestamp,
                            target: target.clone(),
                        }],
                    );
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- updatesuccess");

                let _ = self.changes.send(Change::UpdateSuccess {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                });
            }
            ChangesWrapper::ContractChange(ContractChange::UpdateFailure(update_failure)) => {
                let tx_id = update_failure.transaction().to_string();
                let key = update_failure.key().to_string();
                let requester = update_failure.requester().to_string();
                let target = update_failure.target().to_string();
                let timestamp = update_failure.timestamp();
                let contract_location = update_failure.contract_location();

                if tx_id.is_empty() {
                    return Err(anyhow::anyhow!("tx_id is empty"));
                }

                if key.is_empty() {
                    return Err(anyhow::anyhow!("key is empty"));
                }

                if requester.is_empty() {
                    return Err(anyhow::anyhow!("requester is empty"));
                }

                if target.is_empty() {
                    return Err(anyhow::anyhow!("target is empty"));
                }

                if let Some(mut transactions) = self.transactions_data.get_mut(&tx_id) {
                    transactions.push(Change::UpdateFailure {
                        tx_id: tx_id.clone(),
                        requester: requester.clone(),
                        key: key.clone(),
                        contract_location,
                        timestamp,
                        target: target.clone(),
                    });
                } else {
                    self.transactions_data.insert(
                        tx_id.clone(),
                        vec![Change::UpdateFailure {
                            tx_id: tx_id.clone(),
                            requester: requester.clone(),
                            key: key.clone(),
                            contract_location,
                            timestamp,
                            target: target.clone(),
                        }],
                    );
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- updatefailure");

                let _ = self.changes.send(Change::UpdateFailure {
                    tx_id,
                    key,
                    requester,
                    target,
                    timestamp,
                    contract_location,
                });
            }

            _ => unreachable!(),
        }
        Ok(())
    }
}

async fn record_saver(
    data_dir: PathBuf,
    mut incoming_rec: tokio::sync::broadcast::Receiver<Change>,
) -> anyhow::Result<()> {
    use std::io::Write;
    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir)?;
    }
    let log_file = data_dir.join("network-metrics");
    tracing::info!("Recording logs to {log_file:?}");
    let mut fs = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(log_file)?,
    );

    #[derive(Serialize)]
    struct WithTimestamp {
        timestamp: chrono::DateTime<chrono::Utc>,
        #[serde(flatten)]
        change: Change,
    }

    // todo: this ain't flushing correctly after test ends,
    // for now flushing each single time we get a new record
    // let mut batch = Vec::with_capacity(1024);
    while let Ok(change) = incoming_rec.recv().await {
        let change = WithTimestamp {
            change,
            timestamp: chrono::Utc::now(),
        };
        serde_json::to_writer(&mut fs, &change)?;
        fs.write_all(b"\n")?;
        fs.flush()?;
        // batch.push(record);
        // if batch.len() > 0 {
        //     let batch = std::mem::replace(&mut batch, Vec::with_capacity(1024));
        // let result = tokio::task::spawn_blocking(move || {
        //     let mut serialized = Vec::with_capacity(batch.len() * 128);
        //     for rec in batch {
        //         let mut rec = serde_json::to_vec(&rec).unwrap();
        //         rec.push(b'\n');
        //         serialized.push(rec);
        //     }
        //     serialized
        // })
        // .await?;
        // for rec in result {
        //     fs.write_all(&rec).await?;
        //     fs.flush().await?;
        // }
        // }
    }
    // tracing::warn!(?batch, "Saving records");
    // for rec in batch {
    //     let mut rec = serde_json::to_vec(&rec).unwrap();
    //     rec.push(b'\n');
    //     fs.write_all(&rec).await?;
    // }
    // fs.flush().await?;
    tracing::warn!("Finished saving records");
    Ok(())
}
