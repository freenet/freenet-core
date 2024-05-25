use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::OpenOptions,
    net::TcpStream,
    sync::{
        mpsc::{self},
        Mutex,
    },
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{
    config::GlobalExecutor,
    contract::StoreResponse,
    generated::ContractChange,
    message::{MessageStats, NetMessage, NetMessageV1, Transaction},
    node::PeerId,
    operations::{connect, get::GetMsg, put::PutMsg, subscribe::SubscribeMsg},
    ring::{Location, PeerKeyLocation, Ring},
    router::RouteEvent,
    DynError,
};

#[cfg(feature = "trace-ot")]
pub(crate) use opentelemetry_tracer::OTEventRegister;
pub(crate) use test::TestEventListener;

use crate::node::OpManager;

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct ListenerLogId(usize);

/// A type that reacts to incoming messages from the network and records information about them.
pub(crate) trait NetEventRegister: std::any::Any + Send + Sync + 'static {
    fn register_events<'a>(
        &'a self,
        events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()>;
    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<()>;
    fn trait_clone(&self) -> Box<dyn NetEventRegister>;
    fn get_router_events(&self, number: usize) -> BoxFuture<Result<Vec<RouteEvent>, DynError>>;
}

#[cfg(feature = "trace-ot")]
pub(crate) struct CombinedRegister<const N: usize>([Box<dyn NetEventRegister>; N]);

#[cfg(feature = "trace-ot")]
impl<const N: usize> CombinedRegister<N> {
    pub fn new(registries: [Box<dyn NetEventRegister>; N]) -> Self {
        Self(registries)
    }
}

#[cfg(feature = "trace-ot")]
impl<const N: usize> NetEventRegister for CombinedRegister<N> {
    fn register_events<'a>(
        &'a self,
        events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async move {
            for registry in &self.0 {
                registry.register_events(events.clone()).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<()> {
        async move {
            for reg in &mut self.0 {
                reg.notify_of_time_out(tx).await;
            }
        }
        .boxed()
    }

    fn get_router_events(&self, number: usize) -> BoxFuture<Result<Vec<RouteEvent>, DynError>> {
        async move {
            for reg in &self.0 {
                let events = reg.get_router_events(number).await?;
                if !events.is_empty() {
                    return Ok(events);
                }
            }
            Ok(vec![])
        }
        .boxed()
    }
}

#[cfg(feature = "trace-ot")]
impl<const N: usize> Clone for CombinedRegister<N> {
    fn clone(&self) -> Self {
        let mut i = 0;
        let cloned: [Box<dyn NetEventRegister>; N] = [None::<()>; N].map(|_| {
            let cloned = self.0[i].trait_clone();
            i += 1;
            cloned
        });
        Self(cloned)
    }
}

#[derive(Clone)]
pub(crate) struct NetEventLog<'a> {
    tx: &'a Transaction,
    peer_id: PeerId,
    kind: EventKind,
}

impl<'a> NetEventLog<'a> {
    pub fn route_event(tx: &'a Transaction, ring: &'a Ring, route_event: &RouteEvent) -> Self {
        let peer_id = ring.get_peer_key().unwrap().clone();
        NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Route(route_event.clone()),
        }
    }

    pub fn connected(ring: &'a Ring, peer: PeerId, location: Location) -> Self {
        let peer_id = ring.get_peer_key().unwrap().clone();
        NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::Connected {
                this: ring.own_location(),
                connected: PeerKeyLocation {
                    peer,
                    location: Some(location),
                },
            }),
        }
    }

    pub fn disconnected(ring: &'a Ring, from: &'a PeerId) -> Self {
        let peer_id = ring.get_peer_key().unwrap().clone();
        NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Disconnected { from: from.clone() },
        }
    }

    pub fn from_outbound_msg(msg: &'a NetMessage, ring: &'a Ring) -> Either<Self, Vec<Self>> {
        let peer_id = ring.get_peer_key().unwrap();
        let kind = match msg {
            NetMessage::V1(NetMessageV1::Connect(connect::ConnectMsg::Response {
                msg:
                    connect::ConnectResponse::AcceptedBy {
                        accepted, acceptor, ..
                    },
                ..
            })) => {
                let this_peer = ring.own_location();
                if *accepted {
                    EventKind::Connect(ConnectEvent::Connected {
                        this: this_peer,
                        connected: PeerKeyLocation {
                            peer: acceptor.peer.clone(),
                            location: acceptor.location,
                        },
                    })
                } else {
                    EventKind::Ignored
                }
            }
            _ => EventKind::Ignored,
        };
        Either::Left(NetEventLog {
            tx: msg.id(),
            peer_id,
            kind,
        })
    }

    pub fn from_inbound_msg_v1(
        msg: &'a NetMessageV1,
        op_manager: &'a OpManager,
    ) -> Either<Self, Vec<Self>> {
        let kind = match msg {
            NetMessageV1::Connect(connect::ConnectMsg::Response {
                msg:
                    connect::ConnectResponse::AcceptedBy {
                        acceptor,
                        accepted,
                        joiner,
                        ..
                    },
                ..
            }) => {
                let this_peer = &op_manager.ring.get_peer_key().unwrap();
                let mut events = vec![];
                if *accepted {
                    events.push(NetEventLog {
                        tx: msg.id(),
                        peer_id: this_peer.clone(),
                        kind: EventKind::Connect(ConnectEvent::Finished {
                            initiator: joiner.clone(),
                            location: acceptor.location.unwrap(),
                        }),
                    });
                }
                return Either::Right(events);
            }
            NetMessageV1::Put(PutMsg::RequestPut {
                contract,
                target,
                id,
                ..
            }) => {
                let this_peer = &op_manager.ring.get_peer_key().unwrap();
                let key = contract.key();
                EventKind::Put(PutEvent::Request {
                    requester: this_peer.clone(),
                    target: target.clone(),
                    key,
                    id: *id,
                })
            }
            NetMessageV1::Put(PutMsg::SuccessfulPut { id, target, key }) => {
                EventKind::Put(PutEvent::PutSuccess {
                    id: *id,
                    requester: op_manager.ring.get_peer_key().unwrap(),
                    target: target.clone(),
                    key: key.clone(),
                })
            }
            NetMessageV1::Put(PutMsg::Broadcasting {
                new_value,
                broadcast_to,
                key,
                ..
            }) => EventKind::Put(PutEvent::BroadcastEmitted {
                broadcast_to: broadcast_to.clone(),
                key: key.clone(),
                value: new_value.clone(),
            }),
            NetMessageV1::Put(PutMsg::BroadcastTo {
                sender,
                new_value,
                key,
                ..
            }) => EventKind::Put(PutEvent::BroadcastReceived {
                requester: sender.peer.clone(),
                key: key.clone(),
                value: new_value.clone(),
            }),
            NetMessageV1::Get(GetMsg::ReturnGet {
                key,
                value: StoreResponse { state: Some(_), .. },
                ..
            }) => EventKind::Get { key: key.clone() },
            NetMessageV1::Subscribe(SubscribeMsg::ReturnSub {
                subscribed: true,
                key,
                sender,
                ..
            }) => EventKind::Subscribed {
                key: key.clone(),
                at: sender.clone(),
            },
            _ => EventKind::Ignored,
        };
        Either::Left(NetEventLog {
            tx: msg.id(),
            peer_id: op_manager.ring.get_peer_key().unwrap().clone(),
            kind,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
struct NetLogMessage {
    tx: Transaction,
    datetime: DateTime<Utc>,
    peer_id: PeerId,
    kind: EventKind,
}

impl NetLogMessage {
    fn to_log_message<'a>(
        log: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> impl Iterator<Item = NetLogMessage> + Send + 'a {
        let erased_iter = match log {
            Either::Left(one) => Box::new([one].into_iter())
                as Box<dyn std::iter::Iterator<Item = NetEventLog<'_>> + Send + 'a>,
            Either::Right(multiple) => Box::new(multiple.into_iter())
                as Box<dyn std::iter::Iterator<Item = NetEventLog<'_>> + Send + 'a>,
        };
        erased_iter.into_iter().map(NetLogMessage::from)
    }

    /// Signals whether this message closes a transaction span.
    ///
    /// In case of isolated events where the span is not being tracked it should return true.
    #[cfg(feature = "trace-ot")]
    fn span_completed(&self) -> bool {
        match &self.kind {
            EventKind::Connect(ConnectEvent::Finished { .. }) => true,
            EventKind::Connect(_) => false,
            EventKind::Put(PutEvent::PutSuccess { .. }) => true,
            EventKind::Put(_) => false,
            _ => false,
        }
    }
}

impl<'a> From<NetEventLog<'a>> for NetLogMessage {
    fn from(log: NetEventLog<'a>) -> NetLogMessage {
        NetLogMessage {
            datetime: Utc::now(),
            tx: *log.tx,
            kind: log.kind,
            peer_id: log.peer_id.clone(),
        }
    }
}

#[cfg(feature = "trace-ot")]
impl<'a> From<&'a NetLogMessage> for Option<Vec<opentelemetry::KeyValue>> {
    fn from(msg: &'a NetLogMessage) -> Self {
        use opentelemetry::KeyValue;
        let map: Option<Vec<KeyValue>> = match &msg.kind {
            EventKind::Connect(ConnectEvent::StartConnection { from }) => Some(vec![
                KeyValue::new("phase", "start"),
                KeyValue::new("initiator", format!("{from}")),
            ]),
            EventKind::Connect(ConnectEvent::Connected { this, connected }) => Some(vec![
                KeyValue::new("phase", "connected"),
                KeyValue::new("from", format!("{this}")),
                KeyValue::new("to", format!("{connected}")),
            ]),
            EventKind::Connect(ConnectEvent::Finished {
                initiator,
                location,
            }) => Some(vec![
                KeyValue::new("phase", "finished"),
                KeyValue::new("initiator", format!("{initiator}")),
                KeyValue::new("location", location.as_f64()),
            ]),
            _ => None,
        };
        map.map(|mut map| {
            map.push(KeyValue::new("peer_id", format!("{}", msg.peer_id)));
            map
        })
    }
}

#[derive(Clone)]
pub(crate) struct EventRegister {
    log_file: Arc<PathBuf>,
    log_sender: mpsc::Sender<NetLogMessage>,
}

/// Records from a new session must have higher than this ts.
static NEW_RECORDS_TS: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();
static FILE_LOCK: Mutex<()> = Mutex::const_new(());

const EVENT_REGISTER_BATCH_SIZE: usize = 100;

const DEFAULT_METRICS_SERVER_PORT: u16 = 55010;

impl EventRegister {
    #[cfg(not(test))]
    const MAX_LOG_RECORDS: usize = 100_000;
    #[cfg(test)]
    const MAX_LOG_RECORDS: usize = 10_000;

    const BATCH_SIZE: usize = EVENT_REGISTER_BATCH_SIZE;

    pub fn new(event_log_path: PathBuf) -> Self {
        let (log_sender, log_recv) = mpsc::channel(1000);
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let log_file = Arc::new(event_log_path);
        GlobalExecutor::spawn(Self::record_logs(log_recv, log_file.clone()));
        Self {
            log_sender,
            log_file,
        }
    }

    async fn record_logs(
        mut log_recv: mpsc::Receiver<NetLogMessage>,
        event_log_path: Arc<PathBuf>,
    ) {
        use futures::StreamExt;

        tokio::time::sleep(std::time::Duration::from_millis(200)).await; // wait for the node to start
        let mut event_log = match OpenOptions::new()
            .write(true)
            .read(true)
            .open(&*event_log_path)
            .await
        {
            Ok(file) => file,
            Err(err) => {
                tracing::error!("Failed openning log file {:?} with: {err}", event_log_path);
                panic!("Failed openning log file"); // fixme: propagate this to the main event loop
            }
        };
        let mut num_written = 0;
        let mut log_batch = Vec::with_capacity(Self::BATCH_SIZE);

        let mut num_recs = Self::num_lines(event_log_path.as_path())
            .await
            .expect("non IO error");

        let mut ws = connect_to_metrics_server().await;

        loop {
            let ws_recv = if let Some(ws) = &mut ws {
                ws.next().boxed()
            } else {
                futures::future::pending().boxed()
            };
            tokio::select! {
                log = log_recv.recv() => {
                    let Some(log) = log else { break; };
                    if let Some(ws) = ws.as_mut() {
                        send_to_metrics_server(ws, &log).await;
                    }
                    Self::persist_log(&mut log_batch, &mut num_written, &mut num_recs, &mut event_log, log).await;
                }
                ws_msg = ws_recv => {
                    if let Some((ws, ws_msg)) = ws.as_mut().zip(ws_msg) {
                        received_from_metrics_server(ws, ws_msg).await;
                    }
                }
            }
        }

        // store remaining logs
        let mut batch_serialized_data = Vec::with_capacity(log_batch.len() * 1024);
        for log_item in log_batch {
            let mut serialized = match bincode::serialize(&log_item) {
                Err(err) => {
                    tracing::error!("Failed serializing log: {err}");
                    break;
                }
                Ok(serialized) => serialized,
            };
            {
                use byteorder::{BigEndian, WriteBytesExt};
                batch_serialized_data
                    .write_u32::<BigEndian>(serialized.len() as u32)
                    .expect("enough memory");
            }
            batch_serialized_data.append(&mut serialized);
        }
        if !batch_serialized_data.is_empty() {
            use tokio::io::AsyncWriteExt;
            let _guard = FILE_LOCK.lock().await;
            if let Err(err) = event_log.write_all(&batch_serialized_data).await {
                tracing::error!("Failed writting to event log: {err}");
                panic!("Failed writting event log");
            }
        }
    }

    async fn persist_log(
        log_batch: &mut Vec<NetLogMessage>,
        num_written: &mut usize,
        num_recs: &mut usize,
        event_log: &mut tokio::fs::File,
        log: NetLogMessage,
    ) {
        log_batch.push(log);
        let mut batch_buf = vec![];

        if log_batch.len() >= Self::BATCH_SIZE {
            let num_logs: usize = log_batch.len();
            let moved_batch = std::mem::replace(log_batch, Vec::with_capacity(Self::BATCH_SIZE));
            let serialization_task = tokio::task::spawn_blocking(move || {
                let mut batch_serialized_data = Vec::with_capacity(Self::BATCH_SIZE * 1024);
                for log_item in &moved_batch {
                    let mut serialized = match bincode::serialize(log_item) {
                        Err(err) => {
                            tracing::error!("Failed serializing log: {err}");
                            return Err(err);
                        }
                        Ok(serialized) => serialized,
                    };
                    {
                        use byteorder::{BigEndian, WriteBytesExt};
                        batch_serialized_data
                            .write_u32::<BigEndian>(serialized.len() as u32)
                            .expect("enough memory");
                    }
                    batch_serialized_data.append(&mut serialized);
                }
                Ok(batch_serialized_data)
            });

            match serialization_task.await {
                Ok(Ok(serialized_data)) => {
                    // tracing::debug!(bytes = %serialized_data.len(), %num_logs, "serialized logs");
                    batch_buf = serialized_data;
                    *num_written += num_logs;
                    log_batch.clear(); // Clear the batch for new data
                }
                _ => {
                    panic!("Failed serializing log");
                }
            }
        }

        if *num_written >= Self::BATCH_SIZE {
            {
                use tokio::io::AsyncWriteExt;
                let _guard = FILE_LOCK.lock().await;
                if let Err(err) = event_log.write_all(&batch_buf).await {
                    tracing::error!("Failed writting to event log: {err}");
                    panic!("Failed writting event log");
                }
            }
            *num_recs += *num_written;
            *num_written = 0;
        }

        // Check the number of lines and truncate if needed
        if *num_recs > Self::MAX_LOG_RECORDS {
            const REMOVE_RECS: usize = 1000 + EVENT_REGISTER_BATCH_SIZE; // making space for 1000 new records
            if let Err(err) = Self::truncate_records(event_log, REMOVE_RECS).await {
                tracing::error!("Failed truncating log file: {:?}", err);
                panic!("Failed truncating log file");
            }
            *num_recs -= REMOVE_RECS;
        }
    }

    async fn num_lines(path: &Path) -> io::Result<usize> {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = tokio::io::BufReader::new(File::open(path).await?);
        let mut num_records = 0;
        let mut buf = [0; 4]; // Read the u32 length prefix

        loop {
            let bytes_read = file.read_exact(&mut buf).await;
            if bytes_read.is_err() {
                break;
            }
            num_records += 1;

            // Seek to the next record without reading its contents
            let length = u32::from_le_bytes(buf) as u64;
            if (file.seek(io::SeekFrom::Current(length as i64)).await).is_err() {
                break;
            }
        }

        Ok(num_records)
    }

    async fn truncate_records(
        file: &mut tokio::fs::File,
        remove_records: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

        let _guard = FILE_LOCK.lock().await;
        file.rewind().await?;
        // tracing::debug!(position = file.stream_position().await.unwrap());
        let mut records_count = 0;
        while records_count < remove_records {
            let mut length_bytes = [0u8; 4];
            if let Err(error) = file.read_exact(&mut length_bytes).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            let length = u32::from_be_bytes(length_bytes);
            if let Err(error) = file.seek(io::SeekFrom::Current(length as i64)).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            records_count += 1;
        }

        // Copy the rest of the file to the buffer
        let mut buffer = Vec::new();
        if let Err(error) = file.read_to_end(&mut buffer).await {
            if !matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                let pos = file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
        }

        // Seek back to the beginning and write the remaining content
        file.rewind().await?;
        file.write_all(&buffer).await?;

        // Truncate the file to the new size
        file.set_len(buffer.len() as u64).await?;
        file.seek(io::SeekFrom::End(0)).await?;
        Ok(())
    }

    pub async fn get_router_events(
        max_event_number: usize,
        event_log_path: &Path,
    ) -> Result<Vec<RouteEvent>, DynError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        const MAX_EVENT_HISTORY: usize = 10_000;
        let event_num = max_event_number.min(MAX_EVENT_HISTORY);

        // tracing::info!(?event_log_path);
        let _guard: tokio::sync::MutexGuard<'_, ()> = FILE_LOCK.lock().await;
        let mut file =
            tokio::io::BufReader::new(OpenOptions::new().read(true).open(event_log_path).await?);

        let new_records_ts = NEW_RECORDS_TS
            .get()
            .expect("set on initialization")
            .duration_since(std::time::UNIX_EPOCH)
            .expect("should be older than unix epoch")
            .as_secs() as i64;

        let mut records = Vec::with_capacity(event_num);
        while records.len() < event_num {
            // Read the length prefix
            let length = match file.read_u32().await {
                Ok(l) => l,
                Err(error) => {
                    if !matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                        let pos = file.stream_position().await;
                        tracing::error!(%error, ?pos, "error while trying to read file");
                        return Err(error.into());
                    } else {
                        break;
                    }
                }
            };
            let mut buf = vec![0; length as usize];
            file.read_exact(&mut buf).await?;
            records.push(buf);
            if records.len() == event_num {
                break;
            }
        }

        if records.is_empty() {
            return Ok(vec![]);
        }

        let deserialized_records = tokio::task::spawn_blocking(move || {
            let mut filtered = vec![];
            for buf in records {
                let record: NetLogMessage = bincode::deserialize(&buf).map_err(|e| {
                    tracing::error!(?buf, "deserialization error");
                    e
                })?;
                // tracing::info!(?record);
                if let EventKind::Route(outcome) = record.kind {
                    let record_ts = record.datetime.timestamp();
                    if record_ts >= new_records_ts {
                        filtered.push(outcome);
                    }
                }
            }
            Ok::<_, DynError>(filtered)
        })
        .await??;

        Ok(deserialized_records)
    }
}

impl NetEventRegister for EventRegister {
    fn register_events<'a>(
        &'a self,
        logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async {
            for log_msg in NetLogMessage::to_log_message(logs) {
                let _ = self.log_sender.send(log_msg).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(&mut self, _: Transaction) -> BoxFuture<()> {
        async {}.boxed()
    }

    fn get_router_events(&self, number: usize) -> BoxFuture<Result<Vec<RouteEvent>, DynError>> {
        async move { EventRegister::get_router_events(number, &self.log_file).await }.boxed()
    }
}

async fn connect_to_metrics_server() -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let port = std::env::var("FDEV_NETWORK_METRICS_SERVER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_METRICS_SERVER_PORT);

    tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/push-stats/"))
        .await
        .map(|(ws_stream, _)| {
            tracing::info!("Connected to network metrics server");
            ws_stream
        })
        .ok()
}

async fn send_to_metrics_server(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    send_msg: &NetLogMessage,
) {
    use crate::generated::PeerChange;
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let res = match &send_msg.kind {
        EventKind::Connect(ConnectEvent::Connected {
            this:
                PeerKeyLocation {
                    peer: from_peer,
                    location: Some(from_loc),
                },
            connected:
                PeerKeyLocation {
                    peer: to_peer,
                    location: Some(to_loc),
                },
        }) => {
            let msg = PeerChange::added_connection_msg(
                (&send_msg.tx != Transaction::NULL).then(|| send_msg.tx.to_string()),
                (from_peer.clone(), from_loc.as_f64()),
                (to_peer.clone(), to_loc.as_f64()),
            );
            ws_stream.send(Message::Binary(msg)).await
        }
        EventKind::Disconnected { from } => {
            let msg = PeerChange::removed_connection_msg(from.clone(), send_msg.peer_id.clone());
            ws_stream.send(Message::Binary(msg)).await
        }
        EventKind::Put(PutEvent::Request {
            requester,
            key,
            target,
            ..
        }) => {
            let msg = ContractChange::put_request_msg(
                send_msg.tx.to_string(),
                key.to_string(),
                requester.to_string(),
                target.peer.to_string(),
            );
            ws_stream.send(Message::Binary(msg)).await
        }
        EventKind::Put(PutEvent::PutSuccess {
            requester,
            target,
            key,
            ..
        }) => {
            let msg = ContractChange::put_success_msg(
                send_msg.tx.to_string(),
                key.to_string(),
                requester.to_string(),
                target.peer.to_string(),
            );
            ws_stream.send(Message::Binary(msg)).await
        }
        _ => Ok(()),
    };
    if let Err(error) = res {
        tracing::warn!(%error, "Error while sending message to network metrics server");
    }
}

async fn received_from_metrics_server(
    ws_stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    msg: tokio_tungstenite::tungstenite::Result<tokio_tungstenite::tungstenite::Message>,
) {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    match msg {
        Ok(Message::Ping(ping)) => {
            let _ = ws_stream.send(Message::Pong(ping)).await;
        }
        Ok(Message::Close(_)) => {
            if let Err(error) = ws_stream.send(Message::Close(None)).await {
                tracing::warn!(%error, "Error while closing websocket with network metrics server");
            }
        }
        _ => {}
    }
}

#[cfg(feature = "trace-ot")]
mod opentelemetry_tracer {
    #[cfg(not(test))]
    use std::collections::HashMap;
    use std::time::Duration;

    use dashmap::DashMap;
    use opentelemetry::{
        global,
        trace::{self, Span},
        KeyValue,
    };

    use super::*;

    struct OTSpan {
        inner: global::BoxedSpan,
        last_log: SystemTime,
    }

    impl OTSpan {
        fn new(transaction: Transaction) -> Self {
            use trace::{Tracer, TracerProvider};

            let tracer = {
                let tracer_provider = global::tracer_provider();
                tracer_provider.versioned_tracer(
                    "freenet",
                    Some(env!("CARGO_PKG_VERSION")),
                    Some("https://opentelemetry.io/schemas/1.21.0"),
                    None,
                )
            };
            let tx_bytes = transaction.as_bytes();
            let mut span_id = [0; 8];
            span_id.copy_from_slice(&tx_bytes[8..]);
            let start_time = transaction.started();
            let inner = tracer.build(trace::SpanBuilder {
                name: transaction.transaction_type().description().into(),
                start_time: Some(start_time),
                span_id: Some(trace::SpanId::from_bytes(span_id)),
                trace_id: Some(trace::TraceId::from_bytes(tx_bytes)),
                attributes: Some(vec![
                    KeyValue::new("transaction", transaction.to_string()),
                    KeyValue::new("tx_type", transaction.transaction_type().description()),
                ]),
                ..Default::default()
            });
            OTSpan {
                inner,
                last_log: SystemTime::now(),
            }
        }

        fn add_log(&mut self, log: &NetLogMessage) {
            // NOTE: if we need to add some standard attributes in the future take a look at
            // https://docs.rs/opentelemetry-semantic-conventions/latest/opentelemetry_semantic_conventions/
            let ts = SystemTime::UNIX_EPOCH
                + Duration::from_nanos(
                    ((log.datetime.timestamp() * 1_000_000_000)
                        + log.datetime.timestamp_subsec_nanos() as i64) as u64,
                );
            self.last_log = ts;
            if let Some(log_vals) = <Option<Vec<_>>>::from(log) {
                self.inner.add_event_with_timestamp(
                    log.tx.transaction_type().description(),
                    ts,
                    log_vals,
                );
            }
        }
    }

    impl Drop for OTSpan {
        fn drop(&mut self) {
            self.inner.end_with_timestamp(self.last_log);
        }
    }

    impl trace::Span for OTSpan {
        delegate::delegate! {
            to self.inner {
                fn span_context(&self) -> &trace::SpanContext;
                fn is_recording(&self) -> bool;
                fn set_attribute(&mut self, attribute: opentelemetry::KeyValue);
                fn set_status(&mut self, status: trace::Status);
                fn end_with_timestamp(&mut self, timestamp: SystemTime);
            }
        }

        fn add_event_with_timestamp<T>(
            &mut self,
            _: T,
            _: SystemTime,
            _: Vec<opentelemetry::KeyValue>,
        ) where
            T: Into<std::borrow::Cow<'static, str>>,
        {
            unreachable!("not explicitly called")
        }

        fn update_name<T>(&mut self, _: T)
        where
            T: Into<std::borrow::Cow<'static, str>>,
        {
            unreachable!("shouldn't change span name")
        }
    }

    #[derive(Clone)]
    pub(crate) struct OTEventRegister {
        log_sender: mpsc::Sender<NetLogMessage>,
        finished_tx_notifier: mpsc::Sender<Transaction>,
    }

    /// For tests running in a single process is importart that span tracking is global across threads and simulated peers.  
    static UNIQUE_REGISTER: std::sync::OnceLock<DashMap<Transaction, OTSpan>> =
        std::sync::OnceLock::new();

    impl OTEventRegister {
        pub fn new() -> Self {
            if cfg!(test) {
                UNIQUE_REGISTER.get_or_init(DashMap::new);
            }
            let (sender, finished_tx_notifier) = mpsc::channel(100);
            let (log_sender, log_recv) = mpsc::channel(1000);
            NEW_RECORDS_TS.get_or_init(SystemTime::now);
            GlobalExecutor::spawn(Self::record_logs(log_recv, finished_tx_notifier));
            Self {
                log_sender,
                finished_tx_notifier: sender,
            }
        }

        async fn record_logs(
            mut log_recv: mpsc::Receiver<NetLogMessage>,
            mut finished_tx_notifier: mpsc::Receiver<Transaction>,
        ) {
            #[cfg(not(test))]
            let mut logs = HashMap::new();

            #[cfg(not(test))]
            fn process_log(logs: &mut HashMap<Transaction, OTSpan>, log: NetLogMessage) {
                let span_completed = log.span_completed();
                match logs.entry(log.tx) {
                    std::collections::hash_map::Entry::Occupied(mut val) => {
                        {
                            let span = val.get_mut();
                            span.add_log(&log);
                        }
                        if span_completed {
                            let (_, _span) = val.remove_entry();
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(empty) => {
                        let span = empty.insert(OTSpan::new(log.tx));
                        // does not make much sense to treat a single isolated event as a span,
                        // so just ignore those in case they were to happen
                        if !span_completed {
                            span.add_log(&log);
                        }
                    }
                }
            }

            #[cfg(test)]
            fn process_log(logs: &DashMap<Transaction, OTSpan>, log: NetLogMessage) {
                let span_completed = log.span_completed();
                match logs.entry(log.tx) {
                    dashmap::mapref::entry::Entry::Occupied(mut val) => {
                        {
                            let span = val.get_mut();
                            span.add_log(&log);
                        }
                        if span_completed {
                            let (_, _span) = val.remove_entry();
                        }
                    }
                    dashmap::mapref::entry::Entry::Vacant(empty) => {
                        let mut span = empty.insert(OTSpan::new(log.tx));
                        // does not make much sense to treat a single isolated event as a span,
                        // so just ignore those in case they were to happen
                        if !span_completed {
                            span.add_log(&log);
                        }
                    }
                }
            }

            #[cfg(not(test))]
            fn cleanup_timed_out(logs: &mut HashMap<Transaction, OTSpan>, tx: Transaction) {
                if let Some(_span) = logs.remove(&tx) {}
            }

            #[cfg(test)]
            fn cleanup_timed_out(logs: &DashMap<Transaction, OTSpan>, tx: Transaction) {
                if let Some((_, _span)) = logs.remove(&tx) {}
            }

            loop {
                tokio::select! {
                    log_msg = log_recv.recv() => {
                        if let Some(log) = log_msg {
                            #[cfg(not(test))]
                            {
                                process_log(&mut logs, log);
                            }
                            #[cfg(test)]
                            {
                                process_log(UNIQUE_REGISTER.get().expect("should be set"), log);
                            }
                        } else {
                            break;
                        }
                    }
                    finished_tx = finished_tx_notifier.recv() => {
                        if let Some(tx) = finished_tx {
                            #[cfg(not(test))]
                            {
                                cleanup_timed_out(&mut logs, tx);
                            }
                            #[cfg(test)]
                            {
                                cleanup_timed_out(UNIQUE_REGISTER.get().expect("should be set"), tx);
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    impl NetEventRegister for OTEventRegister {
        fn register_events<'a>(
            &'a self,
            logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> BoxFuture<'a, ()> {
            async {
                for log_msg in NetLogMessage::to_log_message(logs) {
                    let _ = self.log_sender.send(log_msg).await;
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<()> {
            async move {
                if cfg!(test) {
                    let _ = self.finished_tx_notifier.send(tx).await;
                }
            }
            .boxed()
        }

        fn get_router_events(
            &self,
            _number: usize,
        ) -> BoxFuture<Result<Vec<RouteEvent>, DynError>> {
            async { Ok(vec![]) }.boxed()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
// todo: make this take by ref instead, probably will need an owned version
enum EventKind {
    Connect(ConnectEvent),
    Put(PutEvent),
    // todo: make this a sequence like Put
    Get {
        key: ContractKey,
    },
    Route(RouteEvent),
    // todo: add update sequences too
    Subscribed {
        key: ContractKey,
        at: PeerKeyLocation,
    },
    Ignored,
    Disconnected {
        from: PeerId,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
enum ConnectEvent {
    StartConnection {
        from: PeerId,
    },
    Connected {
        this: PeerKeyLocation,
        connected: PeerKeyLocation,
    },
    Finished {
        initiator: PeerId,
        location: Location,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
enum PutEvent {
    Request {
        id: Transaction,
        requester: PeerId,
        key: ContractKey,
        target: PeerKeyLocation,
    },
    PutSuccess {
        id: Transaction,
        requester: PeerId,
        target: PeerKeyLocation,
        key: ContractKey,
    },
    BroadcastEmitted {
        /// subscribed peers
        broadcast_to: Vec<PeerKeyLocation>,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
    },
    BroadcastReceived {
        /// peer who started the broadcast op
        requester: PeerId,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
    },
}

#[cfg(feature = "trace")]
pub(crate) mod tracer {
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{Layer, Registry};

    use crate::DynError;

    pub fn init_tracer(level: Option<LevelFilter>) -> Result<(), DynError> {
        let default_filter = if cfg!(any(test, debug_assertions)) {
            LevelFilter::DEBUG
        } else {
            LevelFilter::INFO
        };
        let default_filter = level.unwrap_or(default_filter);
        let filter_layer = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(default_filter.into())
            .from_env_lossy()
            .add_directive("stretto=off".parse().expect("infallible"))
            .add_directive("sqlx=error".parse().expect("infallible"));

        // use opentelemetry_sdk::propagation::TraceContextPropagator;
        use tracing_subscriber::layer::SubscriberExt;

        let disabled_logs = std::env::var("FREENET_DISABLE_LOGS").is_ok();
        let to_stderr = std::env::var("FREENET_LOG_TO_STDERR").is_ok();
        let layers = {
            let fmt_layer = tracing_subscriber::fmt::layer().with_level(true);
            let fmt_layer = if cfg!(any(test, debug_assertions)) {
                fmt_layer.with_file(true).with_line_number(true)
            } else {
                fmt_layer
            };
            let fmt_layer = if to_stderr {
                fmt_layer.with_writer(std::io::stderr).boxed()
            } else {
                fmt_layer.boxed()
            };

            #[cfg(feature = "trace-ot")]
            {
                let disabled_ot_traces = std::env::var("FREENET_DISABLE_TRACES").is_ok();
                let identifier = if let Ok(peer) = std::env::var("FREENET_PEER_ID") {
                    format!("freenet-core-{peer}")
                } else {
                    "freenet-core".to_string()
                };
                println!("setting OT collector with identifier: {identifier}");
                let tracing_ot_layer = {
                    // Connect the Jaeger OT tracer with the tracing middleware
                    // FIXME: remove
                    #[allow(deprecated)]
                    let ot_jaeger_tracer =
                        opentelemetry_jaeger::config::agent::AgentPipeline::default()
                            .with_service_name(identifier)
                            .install_simple()?;
                    // Get a tracer which will route OT spans to a Jaeger agent
                    tracing_opentelemetry::layer().with_tracer(ot_jaeger_tracer)
                };
                if !disabled_logs && !disabled_ot_traces {
                    fmt_layer.and_then(tracing_ot_layer).boxed()
                } else if !disabled_ot_traces {
                    tracing_ot_layer.boxed()
                } else {
                    return Ok(());
                }
            }
            #[cfg(not(feature = "trace-ot"))]
            {
                if disabled_logs {
                    return Ok(());
                }
                fmt_layer.boxed()
            }
        };
        let filtered = layers.with_filter(filter_layer);
        // Create a subscriber which includes the tracing Jaeger OT layer and a fmt layer
        let subscriber = Registry::default().with(filtered);

        // Set the global subscriber
        tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
        Ok(())
    }
}

pub(super) mod test {
    use dashmap::DashMap;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering::SeqCst},
    };

    use super::*;
    use crate::{node::testing_impl::NodeLabel, ring::Distance};

    static LOG_ID: AtomicUsize = AtomicUsize::new(0);

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn event_register_read_write() -> Result<(), DynError> {
        use std::time::Duration;
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");
        std::fs::File::create(&log_path)?;

        // force a truncation
        const TEST_LOGS: usize = EventRegister::MAX_LOG_RECORDS + 100;
        let register = EventRegister::new(log_path.clone());
        let bytes = crate::util::test::random_bytes_2mb();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];
        for _ in 0..TEST_LOGS {
            let tx: Transaction = gen.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = gen.arbitrary()?;
            peers.push(peer);
        }
        let mut total_route_events = 0;
        for i in 0..TEST_LOGS {
            let kind: EventKind = gen.arbitrary()?;
            if matches!(kind, EventKind::Route(_)) {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }
        register.register_events(Either::Right(events)).await;
        while register.log_sender.capacity() != 1000 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let ev =
            EventRegister::get_router_events(EventRegister::MAX_LOG_RECORDS, &log_path).await?;
        assert_eq!(ev.len(), total_route_events);
        Ok(())
    }

    #[derive(Clone)]
    pub(crate) struct TestEventListener {
        node_labels: Arc<DashMap<NodeLabel, PeerId>>,
        tx_log: Arc<DashMap<Transaction, Vec<ListenerLogId>>>,
        logs: Arc<tokio::sync::Mutex<Vec<NetLogMessage>>>,
        network_metrics_server:
            Arc<tokio::sync::Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    }

    impl TestEventListener {
        pub async fn new() -> Self {
            TestEventListener {
                node_labels: Arc::new(DashMap::new()),
                tx_log: Arc::new(DashMap::new()),
                logs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                network_metrics_server: Arc::new(tokio::sync::Mutex::new(
                    connect_to_metrics_server().await,
                )),
            }
        }

        pub fn add_node(&mut self, label: NodeLabel, peer: PeerId) {
            self.node_labels.insert(label, peer);
        }

        pub fn is_connected(&self, peer: &PeerId) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Connect(ConnectEvent::Connected { .. }))
            })
        }

        pub fn has_put_contract(&self, peer: &PeerId, for_key: &ContractKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            let put_ops = logs.iter().filter_map(|l| match &l.kind {
                EventKind::Put(ev) => Some((&l.tx, ev)),
                _ => None,
            });
            let put_ops: HashMap<_, Vec<_>> = put_ops.fold(HashMap::new(), |mut acc, (id, ev)| {
                acc.entry(id).or_default().push(ev);
                acc
            });

            for (_tx, events) in put_ops {
                let mut is_expected_key = false;
                let mut is_expected_peer = false;
                for ev in events {
                    match ev {
                        PutEvent::Request { key, .. } if key != for_key => break,
                        PutEvent::Request { key, .. } if key == for_key => {
                            is_expected_key = true;
                        }
                        PutEvent::PutSuccess { requester, .. } if requester == peer => {
                            is_expected_peer = true;
                        }
                        _ => {}
                    }
                }
                if is_expected_peer && is_expected_key {
                    return true;
                }
            }
            false
        }

        /// The contract was broadcasted from one peer to an other successfully.
        #[cfg(test)]
        pub fn contract_broadcasted(&self, for_key: &ContractKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            let put_broadcast_ops = logs.iter().filter_map(|l| match &l.kind {
                EventKind::Put(ev @ PutEvent::BroadcastEmitted { .. })
                | EventKind::Put(ev @ PutEvent::BroadcastReceived { .. }) => Some((&l.tx, ev)),
                _ => None,
            });
            let put_broadcast_by_tx: HashMap<_, Vec<_>> =
                put_broadcast_ops.fold(HashMap::new(), |mut acc, (id, ev)| {
                    acc.entry(id).or_default().push(ev);
                    acc
                });
            for (_tx, events) in put_broadcast_by_tx {
                let mut was_emitted = false;
                let mut was_received = false;
                for ev in events {
                    match ev {
                        PutEvent::BroadcastEmitted { key, .. } if key.clone() == *for_key => {
                            was_emitted = true;
                        }
                        PutEvent::BroadcastReceived { key, .. } if key.clone() == *for_key => {
                            was_received = true;
                        }
                        _ => {}
                    }
                }
                if was_emitted && was_received {
                    return true;
                }
            }
            false
        }

        pub fn has_got_contract(&self, peer: &PeerId, expected_key: &ContractKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Get { ref key } if key == expected_key  )
            })
        }

        pub fn is_subscribed_to_contract(&self, peer: &PeerId, expected_key: &ContractKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Subscribed { ref key, .. } if key == expected_key  )
            })
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(&self, peer: PeerId) -> Box<dyn Iterator<Item = (PeerId, Distance)>> {
            let Ok(logs) = self.logs.try_lock() else {
                return Box::new([].into_iter());
            };
            let disconnects = logs
                .iter()
                .filter(|l| matches!(l.kind, EventKind::Disconnected { .. }))
                .fold(HashMap::<_, Vec<_>>::new(), |mut map, log| {
                    map.entry(log.peer_id.clone())
                        .or_default()
                        .push(log.datetime);
                    map
                });

            let iter = logs
                .iter()
                .filter_map(|l| {
                    if let EventKind::Connect(ConnectEvent::Connected { this, connected }) = &l.kind
                    {
                        let disconnected = disconnects
                            .get(&connected.peer)
                            .iter()
                            .flat_map(|dcs| dcs.iter())
                            .any(|dc| dc > &l.datetime);
                        if let Some((this_loc, conn_loc)) = this.location.zip(connected.location) {
                            if this.peer == peer && !disconnected {
                                return Some((connected.peer.clone(), conn_loc.distance(this_loc)));
                            }
                        }
                    }
                    None
                })
                .collect::<HashMap<_, _>>()
                .into_iter();
            Box::new(iter)
        }

        fn create_log(log: NetEventLog) -> (NetLogMessage, ListenerLogId) {
            let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
            let NetEventLog { peer_id, kind, .. } = log;
            let msg_log = NetLogMessage {
                datetime: Utc::now(),
                tx: *log.tx,
                peer_id: peer_id.clone(),
                kind,
            };
            (msg_log, log_id)
        }
    }

    impl super::NetEventRegister for TestEventListener {
        fn register_events<'a>(
            &'a self,
            logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> BoxFuture<'a, ()> {
            async {
                match logs {
                    Either::Left(log) => {
                        let tx = log.tx;
                        let (msg_log, log_id) = Self::create_log(log);
                        if let Some(conn) = &mut *self.network_metrics_server.lock().await {
                            send_to_metrics_server(conn, &msg_log).await;
                        }
                        self.logs.lock().await.push(msg_log);
                        self.tx_log.entry(*tx).or_default().push(log_id);
                    }
                    Either::Right(logs) => {
                        let logs_list = &mut *self.logs.lock().await;
                        let mut lock = self.network_metrics_server.lock().await;
                        for log in logs {
                            let tx = log.tx;
                            let (msg_log, log_id) = Self::create_log(log);
                            if let Some(conn) = &mut *lock {
                                send_to_metrics_server(conn, &msg_log).await;
                            }
                            logs_list.push(msg_log);
                            self.tx_log.entry(*tx).or_default().push(log_id);
                        }
                    }
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(&mut self, _: Transaction) -> BoxFuture<()> {
            async {}.boxed()
        }

        fn get_router_events(
            &self,
            _number: usize,
        ) -> BoxFuture<Result<Vec<RouteEvent>, DynError>> {
            async { Ok(vec![]) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_get_connections() -> Result<(), anyhow::Error> {
        use crate::ring::Location;
        let peer_id = PeerId::random();
        let loc = Location::try_from(0.5)?;
        let tx = Transaction::new::<connect::ConnectMsg>();
        let locations = [
            (PeerId::random(), Location::try_from(0.5)?),
            (PeerId::random(), Location::try_from(0.75)?),
            (PeerId::random(), Location::try_from(0.25)?),
        ];

        let listener = TestEventListener::new().await;
        let futs = futures::stream::FuturesUnordered::from_iter(locations.iter().map(
            |(other, location)| {
                listener.register_events(Either::Left(NetEventLog {
                    tx: &tx,
                    peer_id: peer_id.clone(),
                    kind: EventKind::Connect(ConnectEvent::Connected {
                        this: PeerKeyLocation {
                            peer: peer_id.clone(),
                            location: Some(loc),
                        },
                        connected: PeerKeyLocation {
                            peer: other.clone(),
                            location: Some(*location),
                        },
                    }),
                }))
            },
        ));

        futures::future::join_all(futs).await;

        let distances: Vec<_> = listener.connections(peer_id).collect();
        assert!(distances.len() == 3);
        assert!(
            (distances.iter().map(|(_, l)| l.as_f64()).sum::<f64>() - 0.5f64).abs() < f64::EPSILON
        );
        Ok(())
    }
}
