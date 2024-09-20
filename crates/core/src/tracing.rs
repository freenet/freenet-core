use std::{path::PathBuf, sync::Arc, time::SystemTime};

use chrono::{DateTime, Utc};
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::mpsc};
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
};

#[cfg(feature = "trace-ot")]
pub(crate) use opentelemetry_tracer::OTEventRegister;
pub(crate) use test::TestEventListener;

use crate::node::OpManager;

/// An append-only log for network events.
mod aof;

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
    fn get_router_events(&self, number: usize) -> BoxFuture<anyhow::Result<Vec<RouteEvent>>>;
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

    fn get_router_events(&self, number: usize) -> BoxFuture<anyhow::Result<Vec<RouteEvent>>> {
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
        let peer_id = ring.connection_manager.get_peer_key().unwrap().clone();
        NetEventLog {
            tx,
            peer_id,
            kind: EventKind::Route(route_event.clone()),
        }
    }

    pub fn connected(ring: &'a Ring, peer: PeerId, location: Location) -> Self {
        let peer_id = ring.connection_manager.get_peer_key().unwrap().clone();
        NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Connect(ConnectEvent::Connected {
                this: ring.connection_manager.own_location(),
                connected: PeerKeyLocation {
                    peer,
                    location: Some(location),
                },
            }),
        }
    }

    pub fn disconnected(ring: &'a Ring, from: &'a PeerId) -> Self {
        let peer_id = ring.connection_manager.get_peer_key().unwrap().clone();
        NetEventLog {
            tx: Transaction::NULL,
            peer_id,
            kind: EventKind::Disconnected { from: from.clone() },
        }
    }

    pub fn from_outbound_msg(msg: &'a NetMessage, ring: &'a Ring) -> Either<Self, Vec<Self>> {
        let Some(peer_id) = ring.connection_manager.get_peer_key() else {
            return Either::Right(vec![]);
        };
        let kind = match msg {
            NetMessage::V1(NetMessageV1::Connect(connect::ConnectMsg::Response {
                msg:
                    connect::ConnectResponse::AcceptedBy {
                        accepted, acceptor, ..
                    },
                ..
            })) => {
                let this_peer = ring.connection_manager.own_location();
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
                let this_peer = &op_manager.ring.connection_manager.get_peer_key().unwrap();
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
                let this_peer = &op_manager.ring.connection_manager.get_peer_key().unwrap();
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
                    requester: op_manager.ring.connection_manager.get_peer_key().unwrap(),
                    target: target.clone(),
                    key: *key,
                })
            }
            NetMessageV1::Put(PutMsg::Broadcasting {
                new_value,
                broadcast_to,
                key,
                ..
            }) => EventKind::Put(PutEvent::BroadcastEmitted {
                broadcast_to: broadcast_to.clone(),
                key: *key,
                value: new_value.clone(),
            }),
            NetMessageV1::Put(PutMsg::BroadcastTo {
                sender,
                new_value,
                key,
                ..
            }) => EventKind::Put(PutEvent::BroadcastReceived {
                requester: sender.peer.clone(),
                key: *key,
                value: new_value.clone(),
            }),
            NetMessageV1::Get(GetMsg::ReturnGet {
                key,
                value: StoreResponse { state: Some(_), .. },
                ..
            }) => EventKind::Get { key: *key },
            NetMessageV1::Subscribe(SubscribeMsg::ReturnSub {
                subscribed: true,
                key,
                sender,
                ..
            }) => EventKind::Subscribed {
                key: *key,
                at: sender.clone(),
            },
            _ => EventKind::Ignored,
        };
        Either::Left(NetEventLog {
            tx: msg.id(),
            peer_id: op_manager
                .ring
                .connection_manager
                .get_peer_key()
                .unwrap()
                .clone(),
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

const DEFAULT_METRICS_SERVER_PORT: u16 = 55010;

impl EventRegister {
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
        let mut event_log = match aof::LogFile::open(event_log_path.as_path()).await {
            Ok(file) => file,
            Err(err) => {
                tracing::error!("Failed openning log file {:?} with: {err}", event_log_path);
                panic!("Failed openning log file"); // fixme: propagate this to the main event loop
            }
        };

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
                    event_log.persist_log(log).await;
                }
                ws_msg = ws_recv => {
                    if let Some((ws, ws_msg)) = ws.as_mut().zip(ws_msg) {
                        received_from_metrics_server(ws, ws_msg).await;
                    }
                }
            }
        }

        // store remaining logs
        let moved_batch = std::mem::replace(&mut event_log.batch, aof::Batch::new(aof::BATCH_SIZE));
        let batch_writes = moved_batch.num_writes;
        match aof::LogFile::encode_batch(&moved_batch) {
            Ok(batch_serialized_data) => {
                if !batch_serialized_data.is_empty()
                    && event_log.write_all(&batch_serialized_data).await.is_err()
                {
                    panic!("Failed writting event log");
                }
                event_log.update_recs(batch_writes);
            }
            Err(err) => {
                tracing::error!("Failed encode batch: {err}");
            }
        }
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

    fn get_router_events(&self, number: usize) -> BoxFuture<anyhow::Result<Vec<RouteEvent>>> {
        async move { aof::LogFile::get_router_events(number, &self.log_file).await }.boxed()
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
                tracer_provider
                    .tracer_builder("freenet")
                    .with_version(env!("CARGO_PKG_VERSION"))
                    .with_schema_url("https://opentelemetry.io/schemas/1.21.0")
                    .build()
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

        fn add_link(&mut self, span_context: trace::SpanContext, attributes: Vec<KeyValue>) {
            self.inner.add_link(span_context, attributes);
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

        fn get_router_events(&self, _number: usize) -> BoxFuture<anyhow::Result<Vec<RouteEvent>>> {
            async { Ok(vec![]) }.boxed()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

impl EventKind {
    const CONNECT: u8 = 0;
    const PUT: u8 = 1;
    const GET: u8 = 2;
    const ROUTE: u8 = 3;
    const SUBSCRIBED: u8 = 4;
    const IGNORED: u8 = 5;
    const DISCONNECTED: u8 = 6;

    const fn varint_id(&self) -> u8 {
        match self {
            EventKind::Connect(_) => Self::CONNECT,
            EventKind::Put(_) => Self::PUT,
            EventKind::Get { .. } => Self::GET,
            EventKind::Route(_) => Self::ROUTE,
            EventKind::Subscribed { .. } => Self::SUBSCRIBED,
            EventKind::Ignored => Self::IGNORED,
            EventKind::Disconnected { .. } => Self::DISCONNECTED,
        }
    }
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

    pub fn init_tracer(level: Option<LevelFilter>, endpoint: Option<String>) -> anyhow::Result<()> {
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
            let fmt_layer = tracing_subscriber::fmt::layer().with_level(true).pretty();
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
            #[cfg(not(feature = "trace-ot"))]
            {
                let _ = endpoint;
            }

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
                    let ot_jaeger_tracer = opentelemetry_otlp::new_pipeline()
                        .tracing()
                        .with_exporter(
                            opentelemetry_otlp::new_exporter()
                                .tonic()
                                .with_endpoint(endpoint.unwrap_or_default()),
                        )
                        .with_trace_config(
                            opentelemetry_sdk::trace::Config::default().with_resource(
                                opentelemetry_sdk::Resource::new(vec![
                                    opentelemetry::KeyValue::new(identifier, "tracing-jaeger"),
                                ]),
                            ),
                        )
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
    use crate::{node::testing_impl::NodeLabel, ring::Distance, transport::TransportPublicKey};

    static LOG_ID: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    pub(crate) struct TestEventListener {
        node_labels: Arc<DashMap<NodeLabel, TransportPublicKey>>,
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

        pub fn add_node(&mut self, label: NodeLabel, peer: TransportPublicKey) {
            self.node_labels.insert(label, peer);
        }

        pub fn is_connected(&self, peer: &TransportPublicKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            logs.iter().any(|log| {
                &log.peer_id.pub_key == peer
                    && matches!(log.kind, EventKind::Connect(ConnectEvent::Connected { .. }))
            })
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(
            &self,
            key: &TransportPublicKey,
        ) -> Box<dyn Iterator<Item = (PeerId, Distance)>> {
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
                            if &this.peer.pub_key == key && !disconnected {
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

        fn get_router_events(&self, _number: usize) -> BoxFuture<anyhow::Result<Vec<RouteEvent>>> {
            async { Ok(vec![]) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_get_connections() -> anyhow::Result<()> {
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

        let distances: Vec<_> = listener.connections(&peer_id.pub_key).collect();
        assert!(distances.len() == 3);
        assert!(
            (distances.iter().map(|(_, l)| l.as_f64()).sum::<f64>() - 0.5f64).abs() < f64::EPSILON
        );
        Ok(())
    }
}
