use std::{net::Ipv4Addr, path::PathBuf, str::FromStr, sync::Arc};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router, Server,
};
use dashmap::DashMap;
use freenet::{
    dev_tool::PeerId,
    generated::{topology::ControllerResponse, PeerChange, TryFromFbs},
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

pub async fn run_server(data_dir: Option<PathBuf>) -> anyhow::Result<()> {
    const DEFAULT_PORT: u16 = 55010;

    let port = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let (changes, rx) = tokio::sync::broadcast::channel(100);
    let router = Router::new()
        .route("/", get(home))
        .route("/push-stats/", get(push_stats))
        .route("/pull-stats/", get(pull_stats))
        .with_state(Arc::new(ServerState {
            changes,
            peer_data: DashMap::new(),
        }));
    if let Some(data_dir) = data_dir {
        tokio::task::spawn(async move {
            if let Err(err) = record_saver(data_dir, rx).await {
                tracing::error!(error = %err, "Record saver failed");
            }
        });
    }

    Server::bind(&(Ipv4Addr::LOCALHOST, port).into())
        .serve(router.into_make_service())
        .await?;
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
                        if let Err(err) = state.save_record(change) {
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
                tracing::debug!("websocket error: {}", e);
                break;
            }
        }
    }
    Ok(())
}

async fn pull_stats(
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
            *peer.key(),
            peer.value().location,
            peer.value().connections.iter(),
        );
        tx.send(Message::Binary(msg)).await?;
    }
    let mut changes = state.changes.subscribe();
    while let Ok(msg) = changes.recv().await {
        match msg {
            Change::AddedConnection { from, to } => {
                let msg = PeerChange::added_connection_msg(from, to);
                tx.send(Message::Binary(msg)).await?;
            }
            Change::RemovedConnection { from, at } => {
                let msg = PeerChange::removed_connection_msg(at, from);
                tx.send(Message::Binary(msg)).await?;
            }
        }
    }
    Ok(())
}

struct ServerState {
    changes: tokio::sync::broadcast::Sender<Change>,
    peer_data: DashMap<PeerId, PeerData>,
}

struct PeerData {
    connections: Vec<(PeerId, f64)>,
    location: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Change {
    AddedConnection {
        from: (PeerId, f64),
        to: (PeerId, f64),
    },
    RemovedConnection {
        from: PeerId,
        at: PeerId,
    },
}

impl ServerState {
    fn save_record(&self, change: PeerChange<'_>) -> Result<(), anyhow::Error> {
        match change {
            PeerChange::AddedConnection(added) => {
                let from_peer_id = PeerId::from_str(added.from())?;
                let from_loc = added.from_location();

                let to_peer_id = PeerId::from_str(added.to())?;
                let to_loc = added.to_location();

                match self.peer_data.entry(from_peer_id) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        occ.get_mut().connections.push((to_peer_id, to_loc));
                    }
                    dashmap::mapref::entry::Entry::Vacant(vac) => {
                        vac.insert(PeerData {
                            connections: vec![(to_peer_id, to_loc)],
                            location: from_loc,
                        });
                    }
                }

                match self.peer_data.entry(to_peer_id) {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        occ.get_mut().connections.push((from_peer_id, from_loc));
                    }
                    dashmap::mapref::entry::Entry::Vacant(vac) => {
                        vac.insert(PeerData {
                            connections: vec![(from_peer_id, from_loc)],
                            location: to_loc,
                        });
                    }
                }

                let _ = self.changes.send(Change::AddedConnection {
                    from: (from_peer_id, from_loc),
                    to: (to_peer_id, to_loc),
                });
            }
            PeerChange::RemovedConnection(removed) => {
                let from_peer_id = PeerId::from_str(removed.from())?;
                let at_peer_id = PeerId::from_str(removed.at())?;

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
                    from: from_peer_id,
                    at: at_peer_id,
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
    use tokio::io::AsyncWriteExt;
    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir)?;
    }
    let mut fs = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(data_dir.join("network-metrics"))
        .await?;

    let mut batch = Vec::with_capacity(1024);
    while let Ok(record) = incoming_rec.recv().await {
        batch.push(record);
        if batch.len() > 100 {
            let batch = std::mem::replace(&mut batch, Vec::with_capacity(1024));
            let result = tokio::task::spawn_blocking(move || {
                let mut serialized = Vec::with_capacity(batch.len() * 128);
                for rec in batch {
                    let rec = serde_json::to_vec(&rec).unwrap();
                    serialized.push(rec);
                }
                serialized
            })
            .await?;
            for rec in result {
                fs.write_all(&rec).await?;
            }
        }
    }
    Ok(())
}
