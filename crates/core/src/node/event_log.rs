use std::{io, path::Path, time::SystemTime};

use chrono::{DateTime, Utc};
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::OpenOptions,
    sync::mpsc::{self},
};

use super::PeerKey;
use crate::{
    config::GlobalExecutor,
    contract::StoreResponse,
    message::{Message, Transaction},
    operations::{connect, get::GetMsg, put::PutMsg, subscribe::SubscribeMsg},
    ring::PeerKeyLocation,
    router::RouteEvent,
    DynError,
};

#[cfg(test)]
pub(super) use test_utils::TestEventListener;

use super::OpManager;

#[derive(Debug, Clone, Copy)]
struct ListenerLogId(usize);

/// A type that reacts to incoming messages from the network.
/// It injects itself at the message event loop.
///
/// This type then can emit it's own information to adjacent systems
/// or is a no-op.
pub(crate) trait EventLogRegister: std::any::Any + Send + Sync + 'static {
    fn register_events<'a>(
        &'a mut self,
        events: Either<EventLog<'a>, Vec<EventLog<'a>>>,
    ) -> BoxFuture<'a, ()>;
    fn trait_clone(&self) -> Box<dyn EventLogRegister>;
    fn as_any(&self) -> &dyn std::any::Any
    where
        Self: Sized,
    {
        self as _
    }
}

pub(crate) struct EventLog<'a> {
    tx: &'a Transaction,
    peer_id: &'a PeerKey,
    kind: EventKind,
}

impl<'a> EventLog<'a> {
    pub fn route_event(
        tx: &'a Transaction,
        op_storage: &'a OpManager,
        route_event: &RouteEvent,
    ) -> Self {
        EventLog {
            tx,
            peer_id: &op_storage.ring.peer_key,
            kind: EventKind::Route(route_event.clone()),
        }
    }

    pub fn from_outbound_msg(
        msg: &'a Message,
        op_storage: &'a OpManager,
    ) -> Either<Self, Vec<Self>> {
        let kind = match msg {
            Message::Connect(connect::ConnectMsg::Response {
                msg:
                    connect::ConnectResponse::AcceptedBy {
                        peers,
                        your_location,
                        your_peer_id,
                    },
                ..
            }) => {
                let this_peer = op_storage.ring.own_location();
                if peers.contains(&this_peer) {
                    EventKind::Connected {
                        this: this_peer,
                        connected: PeerKeyLocation {
                            peer: *your_peer_id,
                            location: Some(*your_location),
                        },
                    }
                } else {
                    EventKind::Ignored
                }
            }
            _ => EventKind::Ignored,
        };
        Either::Left(EventLog {
            tx: msg.id(),
            peer_id: &op_storage.ring.peer_key,
            kind,
        })
    }

    pub fn from_inbound_msg(
        msg: &'a Message,
        op_storage: &'a OpManager,
    ) -> Either<Self, Vec<Self>> {
        let kind = match msg {
            Message::Connect(connect::ConnectMsg::Response {
                msg:
                    connect::ConnectResponse::AcceptedBy {
                        peers,
                        your_location,
                        your_peer_id,
                    },
                ..
            }) => {
                return Either::Right(
                    peers
                        .iter()
                        .map(|peer| {
                            let kind: EventKind = EventKind::Connected {
                                this: PeerKeyLocation {
                                    peer: *your_peer_id,
                                    location: Some(*your_location),
                                },
                                connected: *peer,
                            };
                            EventLog {
                                tx: msg.id(),
                                peer_id: &op_storage.ring.peer_key,
                                kind,
                            }
                        })
                        .collect(),
                );
            }
            Message::Put(PutMsg::RequestPut {
                contract, target, ..
            }) => {
                let key = contract.key();
                EventKind::Put(PutEvent::Request {
                    performer: target.peer,
                    key,
                })
            }
            Message::Put(PutMsg::SuccessfulUpdate { new_value, .. }) => {
                EventKind::Put(PutEvent::PutSuccess {
                    requester: op_storage.ring.peer_key,
                    value: new_value.clone(),
                })
            }
            Message::Put(PutMsg::Broadcasting {
                new_value,
                broadcast_to,
                key,
                ..
            }) => EventKind::Put(PutEvent::BroadcastEmitted {
                broadcast_to: broadcast_to.clone(),
                key: key.clone(),
                value: new_value.clone(),
            }),
            Message::Put(PutMsg::BroadcastTo {
                sender,
                new_value,
                key,
                ..
            }) => EventKind::Put(PutEvent::BroadcastReceived {
                requester: sender.peer,
                key: key.clone(),
                value: new_value.clone(),
            }),
            Message::Get(GetMsg::ReturnGet {
                key,
                value: StoreResponse { state: Some(_), .. },
                ..
            }) => EventKind::Get { key: key.clone() },
            Message::Subscribe(SubscribeMsg::ReturnSub {
                subscribed: true,
                key,
                sender,
                ..
            }) => EventKind::Subscribed {
                key: key.clone(),
                at: *sender,
            },
            _ => EventKind::Ignored,
        };
        Either::Left(EventLog {
            tx: msg.id(),
            peer_id: &op_storage.ring.peer_key,
            kind,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct LogMessage {
    tx: Transaction,
    datetime: DateTime<Utc>,
    peer_id: PeerKey,
    kind: EventKind,
}

#[derive(Clone)]
pub(crate) struct EventRegister {
    log_sender: mpsc::Sender<LogMessage>,
}

/// Records from a new session must have higher than this ts.
static NEW_RECORDS_TS: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();

impl EventRegister {
    pub fn new() -> Self {
        let (log_sender, log_recv) = mpsc::channel(1000);
        NEW_RECORDS_TS.set(SystemTime::now()).expect("non set");
        GlobalExecutor::spawn(Self::record_logs(log_recv));
        Self { log_sender }
    }

    async fn record_logs(mut log_recv: mpsc::Receiver<LogMessage>) {
        const MAX_LOG_RECORDS: usize = 100_000;
        const BATCH_SIZE: usize = 100;

        async fn num_lines(path: &Path) -> io::Result<usize> {
            use tokio::fs::File;
            use tokio::io::{AsyncBufReadExt, BufReader};

            let file = File::open(path).await.expect("Failed to open log file");
            let reader = BufReader::new(file);
            let mut num_lines = 0;
            let mut lines = reader.lines();
            while lines.next_line().await?.is_some() {
                num_lines += 1;
            }
            Ok(num_lines)
        }

        async fn truncate_lines(
            file: &mut tokio::fs::File,
            lines_to_keep: usize,
        ) -> Result<(), Box<dyn std::error::Error>> {
            use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt};

            file.seek(io::SeekFrom::Start(0)).await?;
            let file_metadata = file.metadata().await?;
            let file_size = file_metadata.len();
            let mut reader = tokio::io::BufReader::new(file);

            let mut buffer = Vec::with_capacity(file_size as usize);
            let mut lines_count = 0;

            let mut line = Vec::new();
            let mut discard_bytes = 0;

            while lines_count < lines_to_keep {
                let bytes_read = reader.read_until(b'\n', &mut line).await?;
                if bytes_read == 0 {
                    // EOF
                    break;
                }
                lines_count += 1;
                discard_bytes += bytes_read;
                line.clear();
            }

            // Copy the rest of the file to the buffer
            while let Ok(bytes_read) = reader.read_buf(&mut buffer).await {
                if bytes_read == 0 {
                    // EOF
                    break;
                }
            }

            // Seek back to the beginning and write the remaining content            let file = reader.into_inner();
            let file = reader.into_inner();
            file.seek(io::SeekFrom::Start(0)).await?;
            file.write_all(&buffer).await?;

            // Truncate the file to the new size
            file.set_len(file_size - discard_bytes as u64).await?;
            file.seek(io::SeekFrom::End(0)).await?;
            Ok(())
        }

        use tokio::io::AsyncWriteExt;
        let event_log_path = crate::config::Config::conf().event_log();
        let mut event_log = match OpenOptions::new().write(true).open(&event_log_path).await {
            Ok(file) => file,
            Err(err) => {
                tracing::error!("Failed openning log file {:?} with: {err}", event_log_path);
                panic!("Failed openning log file"); // todo: propagate this to the main thread
            }
        };
        let mut num_written = 0;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * 1024);
        let mut log_batch = Vec::with_capacity(BATCH_SIZE);
        while let Some(log) = log_recv.recv().await {
            log_batch.push(log);

            if log_batch.len() >= BATCH_SIZE {
                let moved_batch = std::mem::replace(&mut log_batch, Vec::with_capacity(BATCH_SIZE));
                let serialization_task = tokio::task::spawn_blocking(move || {
                    let mut batch_serialized_data = Vec::new();
                    for log_item in &moved_batch {
                        if let Err(err) =
                            bincode::serialize_into(&mut batch_serialized_data, log_item)
                        {
                            // Handle the error appropriately
                            tracing::error!("Failed serializing log: {err}");
                            return Err(err);
                        }
                        batch_serialized_data.push(b'\n');
                    }
                    Ok(batch_serialized_data)
                });

                match serialization_task.await {
                    Ok(Ok(mut serialized_data)) => {
                        batch_buf.append(&mut serialized_data);
                        num_written += log_batch.len();
                        log_batch.clear(); // Clear the batch for new data
                    }
                    _ => {
                        panic!("Failed serializing log");
                    }
                }
            }

            if num_written >= BATCH_SIZE {
                if let Err(err) = event_log.write_all(&batch_buf).await {
                    tracing::error!("Failed writting to event log: {err}");
                    panic!("Failed writting event log");
                }
                num_written = 0;
                batch_buf.clear();
            }

            // Check the number of lines and truncate if needed
            let num_lines = num_lines(event_log_path.as_path())
                .await
                .expect("non IO error");
            if num_lines > MAX_LOG_RECORDS {
                let truncate_to = num_lines - MAX_LOG_RECORDS + 900; // make some extra space removing 1000 old records
                if let Err(err) = truncate_lines(&mut event_log, truncate_to).await {
                    tracing::error!("Failed truncating log file: {:?}", err);
                    panic!("Failed truncating log file");
                }
            }
        }
    }

    pub async fn get_router_events(max_event_number: usize) -> Result<Vec<RouteEvent>, DynError> {
        use tokio::io::AsyncReadExt;
        const BUF_SIZE: usize = 4096;
        const MAX_EVENT_HISTORY: usize = 10_000;
        let event_num = max_event_number.min(MAX_EVENT_HISTORY);

        let event_log_path = crate::config::Config::conf().event_log();
        let mut event_log = OpenOptions::new().read(true).open(event_log_path).await?;

        let mut buf = [0; BUF_SIZE];
        let mut records = Vec::with_capacity(event_num);
        let mut partial_record = vec![];
        let mut record_start = 0;

        let new_records_ts = NEW_RECORDS_TS
            .get()
            .expect("set on initialization")
            .duration_since(std::time::UNIX_EPOCH)
            .expect("should be older than unix epoch")
            .as_secs() as i64;
        while records.len() < event_num {
            let bytes_read = event_log.read(&mut buf).await?;
            if bytes_read == 0 {
                break; // EOF
            }

            let mut found_newline = false;
            for (i, byte) in buf.iter().enumerate().take(bytes_read) {
                if byte == &b'\n' {
                    found_newline = true;
                    let rec = &buf[record_start..i];
                    let deser_record: LogMessage = if partial_record.is_empty() {
                        record_start = i + 1;
                        bincode::deserialize(rec)?
                    } else {
                        partial_record.extend(rec);
                        let rec = bincode::deserialize(&partial_record)?;
                        partial_record.clear();
                        rec
                    };
                    if let EventKind::Route(outcome) = deser_record.kind {
                        let record_ts = deser_record.datetime.timestamp();
                        if record_ts > new_records_ts {
                            records.push(outcome);
                        }
                    }
                }
                if records.len() == event_num {
                    break; // Reached the desired event number
                }
            }
            if !found_newline {
                break; // No more data to read, and event_num not reached
            }
        }
        Ok(records)
    }
}

impl EventLogRegister for EventRegister {
    fn register_events<'a>(
        &'a mut self,
        logs: Either<EventLog<'a>, Vec<EventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async {
            match logs {
                Either::Left(log) => {
                    let log_msg = LogMessage {
                        datetime: Utc::now(),
                        tx: *log.tx,
                        kind: log.kind,
                        peer_id: *log.peer_id,
                    };
                    let _ = self.log_sender.send(log_msg).await;
                }
                Either::Right(logs) => {
                    for log in logs {
                        let log_msg = LogMessage {
                            datetime: Utc::now(),
                            tx: *log.tx,
                            kind: log.kind,
                            peer_id: *log.peer_id,
                        };
                        let _ = self.log_sender.send(log_msg).await;
                    }
                }
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn EventLogRegister> {
        Box::new(self.clone())
    }
}

#[derive(Serialize, Deserialize)]
// todo: make this take by ref instead
enum EventKind {
    Connected {
        this: PeerKeyLocation,
        connected: PeerKeyLocation,
    },
    Put(PutEvent),
    Get {
        key: ContractKey,
    },
    Route(RouteEvent),
    Subscribed {
        key: ContractKey,
        at: PeerKeyLocation,
    },
    Ignored,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum PutEvent {
    Request {
        performer: PeerKey,
        key: ContractKey,
    },
    PutSuccess {
        requester: PeerKey,
        value: WrappedState,
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
        requester: PeerKey,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
    },
}

#[cfg(test)]
pub(super) mod test_utils {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering::SeqCst},
            Arc,
        },
    };

    use dashmap::DashMap;
    use parking_lot::Mutex;

    use super::*;
    use crate::{node::tests::NodeLabel, ring::Distance};

    static LOG_ID: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    pub(crate) struct TestEventListener {
        node_labels: Arc<DashMap<NodeLabel, PeerKey>>,
        tx_log: Arc<DashMap<Transaction, Vec<ListenerLogId>>>,
        logs: Arc<Mutex<Vec<LogMessage>>>,
    }

    impl TestEventListener {
        pub fn new() -> Self {
            TestEventListener {
                node_labels: Arc::new(DashMap::new()),
                tx_log: Arc::new(DashMap::new()),
                logs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn add_node(&mut self, label: NodeLabel, peer: PeerKey) {
            self.node_labels.insert(label, peer);
        }

        pub fn is_connected(&self, peer: &PeerKey) -> bool {
            let logs = self.logs.lock();
            logs.iter()
                .any(|log| &log.peer_id == peer && matches!(log.kind, EventKind::Connected { .. }))
        }

        pub fn has_put_contract(
            &self,
            peer: &PeerKey,
            for_key: &ContractKey,
            expected_value: &WrappedState,
        ) -> bool {
            let logs = self.logs.lock();
            let put_ops = logs.iter().filter_map(|l| match &l.kind {
                EventKind::Put(ev) => Some((&l.tx, ev)),
                _ => None,
            });
            let put_ops: HashMap<_, Vec<_>> = put_ops.fold(HashMap::new(), |mut acc, (id, ev)| {
                acc.entry(id).or_default().push(ev);
                acc
            });

            for (_tx, events) in put_ops {
                let mut is_expected_value = false;
                let mut is_expected_key = false;
                let mut is_expected_peer = false;
                for ev in events {
                    match ev {
                        PutEvent::Request { key, .. } if key != for_key => break,
                        PutEvent::Request { key, .. } if key == for_key => {
                            is_expected_key = true;
                        }
                        PutEvent::PutSuccess { requester, value }
                            if requester == peer && value == expected_value =>
                        {
                            is_expected_peer = true;
                            is_expected_value = true;
                        }
                        _ => {}
                    }
                }
                if is_expected_value && is_expected_peer && is_expected_key {
                    return true;
                }
            }
            false
        }

        /// The contract was broadcasted from one peer to an other successfully.
        pub fn contract_broadcasted(&self, for_key: &ContractKey) -> bool {
            let logs = self.logs.lock();
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

        pub fn has_got_contract(&self, peer: &PeerKey, expected_key: &ContractKey) -> bool {
            let logs = self.logs.lock();
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Get { ref key } if key == expected_key  )
            })
        }

        pub fn is_subscribed_to_contract(
            &self,
            peer: &PeerKey,
            expected_key: &ContractKey,
        ) -> bool {
            let logs = self.logs.lock();
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Subscribed { ref key, .. } if key == expected_key  )
            })
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(&self, peer: PeerKey) -> impl Iterator<Item = (PeerKey, Distance)> {
            let logs = self.logs.lock();
            logs.iter()
                .filter_map(|l| {
                    if let EventKind::Connected { this, connected } = l.kind {
                        if this.peer == peer {
                            return Some((
                                connected.peer,
                                connected
                                    .location
                                    .expect("set location")
                                    .distance(this.location.unwrap()),
                            ));
                        }
                    }
                    None
                })
                .collect::<HashMap<_, _>>()
                .into_iter()
        }

        fn create_log(log: EventLog) -> (LogMessage, ListenerLogId) {
            let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
            let EventLog { peer_id, kind, .. } = log;
            let msg_log = LogMessage {
                datetime: Utc::now(),
                tx: *log.tx,
                peer_id: *peer_id,
                kind,
            };
            (msg_log, log_id)
        }
    }

    impl super::EventLogRegister for TestEventListener {
        fn register_events<'a>(
            &'a mut self,
            logs: Either<EventLog<'a>, Vec<EventLog<'a>>>,
        ) -> BoxFuture<'a, ()> {
            match logs {
                Either::Left(log) => {
                    let tx = log.tx;
                    let (msg_log, log_id) = Self::create_log(log);
                    self.logs.lock().push(msg_log);
                    self.tx_log.entry(*tx).or_default().push(log_id);
                }
                Either::Right(logs) => {
                    let logs_list = &mut *self.logs.lock();
                    for log in logs {
                        let tx = log.tx;
                        let (msg_log, log_id) = Self::create_log(log);
                        logs_list.push(msg_log);
                        self.tx_log.entry(*tx).or_default().push(log_id);
                    }
                }
            }
            async {}.boxed()
        }

        fn trait_clone(&self) -> Box<dyn EventLogRegister> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_get_connections() -> Result<(), anyhow::Error> {
        use crate::ring::Location;
        let peer_id = PeerKey::random();
        let loc = Location::try_from(0.5)?;
        let tx = Transaction::new::<connect::ConnectMsg>();
        let locations = [
            (PeerKey::random(), Location::try_from(0.5)?),
            (PeerKey::random(), Location::try_from(0.75)?),
            (PeerKey::random(), Location::try_from(0.25)?),
        ];

        let mut listener = TestEventListener::new();
        locations.iter().for_each(|(other, location)| {
            listener.register_events(Either::Left(EventLog {
                tx: &tx,
                peer_id: &peer_id,
                kind: EventKind::Connected {
                    this: PeerKeyLocation {
                        peer: peer_id,
                        location: Some(loc),
                    },
                    connected: PeerKeyLocation {
                        peer: *other,
                        location: Some(*location),
                    },
                },
            }));
        });

        let distances: Vec<_> = listener.connections(peer_id).collect();
        assert!(distances.len() == 3);
        assert!(
            (distances.iter().map(|(_, l)| l.as_f64()).sum::<f64>() - 0.5f64).abs() < f64::EPSILON
        );
        Ok(())
    }
}
