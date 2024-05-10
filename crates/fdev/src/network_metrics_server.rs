use std::{net::Ipv4Addr, path::PathBuf, sync::Arc, time::Duration};

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
use freenet::{
    dev_tool::PeerId,
    generated::{
        topology::ControllerResponse, ChangesWrapper, ContractChange, PeerChange, TryFromFbs,
    },
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

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
    const DEFAULT_PORT: u16 = 55010;

    let port = std::env::var("FDEV_NETWORK_METRICS_SERVER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let router = Router::new()
        .route("/", get(home))
        .route("/push-stats/", get(push_stats))
        .route("/pull-stats/peer-changes/", get(pull_peer_changes))
        .with_state(Arc::new(ServerState {
            changes,
            peer_data: DashMap::new(),
            transactions_data: DashMap::new(),
        }));

    tracing::info!("Starting metrics server on port {port}");
    barrier.wait().await;
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn home() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("Location", "/pull-stats/")
        .body(Body::empty())
        .expect("should be valid response")
        .into_response()
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
                    }
                    Err(decoding_error) => {
                        tracing::error!(error = %decoding_error, "Failed to decode message");
                        tx.send(Message::Binary(ControllerResponse::into_fbs_bytes(Err(
                            format!("{decoding_error}"),
                        ))))
                        .await?;
                    }
                }
                match ContractChange::try_decode_fbs(&msg) {
                    Ok(ContractChange::PutFailure(_err)) => {
                        // FIXME: handle put failure
                        tracing::error!(error = "Failed to put contract");
                    }
                    Ok(change) => {
                        if let Err(err) = state.save_record(ChangesWrapper::ContractChange(change))
                        {
                            tracing::error!(error = %err, "Failed saving report");
                            tx.send(Message::Binary(ControllerResponse::into_fbs_bytes(Err(
                                format!("{err}"),
                            ))))
                            .await?;
                        }
                    }
                    Err(decoding_error) => {
                        tracing::error!(error = %decoding_error, "Failed to decode message");
                        tx.send(Message::Binary(ControllerResponse::into_fbs_bytes(Err(
                            format!("{decoding_error}"),
                        ))))
                        .await?;
                    }
                }
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
            } => {
                tracing::debug!(%tx_id, %key, %requester, %target, "sending put request");
                let msg = ContractChange::put_request_msg(tx_id, key, requester, target);
                tx.send(Message::Binary(msg)).await?;
            }
            Change::PutSuccess { tx_id, key, target } => {
                let msg = ContractChange::put_success_msg(tx_id, key, target.clone(), target);
                tx.send(Message::Binary(msg)).await?;
            }
        }
    }
    Ok(())
}

struct ServerState {
    changes: tokio::sync::broadcast::Sender<Change>,
    peer_data: DashMap<PeerId, PeerData>,
    transactions_data: DashMap<String, Vec<String>>,
}

struct PeerData {
    connections: Vec<(PeerId, f64)>,
    location: f64,
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
    },
    PutSuccess {
        tx_id: String,
        key: String,
        target: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PeerIdHumanReadable(PeerId);

impl From<PeerId> for PeerIdHumanReadable {
    fn from(peer_id: PeerId) -> Self {
        Self(peer_id)
    }
}

impl std::fmt::Display for PeerIdHumanReadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.addr)
    }
}

impl ServerState {
    fn save_record(&self, change: ChangesWrapper) -> Result<(), anyhow::Error> {
        match change {
            ChangesWrapper::PeerChange(PeerChange::AddedConnection(added)) => {
                let from_peer_id: PeerId = bincode::deserialize(added.from().bytes())?;
                let from_loc = added.from_location();

                let to_peer_id: PeerId = bincode::deserialize(added.to().bytes())?;
                let to_loc = added.to_location();

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
                let from_peer_id = bincode::deserialize(removed.from().bytes())?;
                let at_peer_id = bincode::deserialize(removed.at().bytes())?;

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

                if self.transactions_data.get_mut(&tx_id).is_some() {
                    unreachable!(
                        "found an already included in logs transaction when it should create it."
                    );
                } else {
                    self.transactions_data.insert(
                        tx_id.clone(),
                        vec![format!(
                            "tx_id {} key {} req {} target {} state PutRequest",
                            tx_id, key, requester, target
                        )],
                    );
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- putrequest");

                let _ = self.changes.send(Change::PutRequest {
                    tx_id,
                    key,
                    requester,
                    target,
                });
            }
            ChangesWrapper::ContractChange(ContractChange::PutSuccess(change)) => {
                let tx_id = change.transaction().to_string();
                let key = change.key().to_string();
                let requester = change.requester().to_string();
                let target = change.target().to_string();

                if let Some(mut entry) = self.transactions_data.get_mut(&tx_id) {
                    entry.push(format!(
                        "tx_id {} key {} req {} target {} state PutSuccess",
                        tx_id, key, requester, target
                    ));
                } else {
                    tracing::error!("transaction data not found for this tx when it should.");
                    unreachable!()
                }

                tracing::debug!(%tx_id, %key, %requester, %target, "checking values from save_record -- putsuccess");

                let _ = self.changes.send(Change::PutSuccess { tx_id, key, target });
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
