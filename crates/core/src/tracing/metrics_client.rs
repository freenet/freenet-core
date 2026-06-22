use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{generated::ContractChange, message::Transaction, node::PeerId, ring::Location};

// Items from sibling submodules (via root re-exports) are accessible via `use super::*`.
use super::*;

const DEFAULT_METRICS_SERVER_PORT: u16 = 55010;

pub(crate) async fn connect_to_metrics_server() -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>>
{
    let port = std::env::var("FDEV_NETWORK_METRICS_SERVER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_METRICS_SERVER_PORT);

    tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/v1/push-stats/"))
        .await
        .map(|(ws_stream, _)| {
            tracing::info!("Connected to network metrics server");
            ws_stream
        })
        .ok()
}

pub(crate) async fn send_to_metrics_server(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    send_msg: &NetLogMessage,
) {
    use crate::generated::PeerChange;
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let res = match &send_msg.kind {
        EventKind::Connect(ConnectEvent::Connected {
            this: this_peer,
            connected: connected_peer,
            ..
        }) => {
            // Both peers must have known locations to send to metrics server
            if let (Some(from_loc), Some(to_loc)) =
                (this_peer.location(), connected_peer.location())
            {
                let this_id = PeerId::new(
                    this_peer.pub_key().clone(),
                    this_peer
                        .socket_addr()
                        .expect("this peer should have address"),
                );
                let connected_id = PeerId::new(
                    connected_peer.pub_key().clone(),
                    connected_peer
                        .socket_addr()
                        .expect("connected peer should have address"),
                );
                let msg = PeerChange::added_connection_msg(
                    (&send_msg.tx != Transaction::NULL).then(|| send_msg.tx.to_string()),
                    (this_id.to_string(), from_loc.as_f64()),
                    (connected_id.to_string(), to_loc.as_f64()),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Disconnected { from, .. } => {
            let msg = PeerChange::removed_connection_msg(
                from.clone().to_string(),
                send_msg.peer_id.clone().to_string(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Put(PutEvent::Request {
            requester,
            key,
            target,
            timestamp,
            ..
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
                let msg = ContractChange::put_request_msg(
                    send_msg.tx.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Put(PutEvent::PutSuccess {
            requester,
            target,
            key,
            timestamp,
            ..
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
                let msg = ContractChange::put_success_msg(
                    send_msg.tx.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Put(PutEvent::BroadcastEmitted {
            id,
            upstream,
            broadcast_to, // broadcast_to n peers
            broadcasted_to,
            key,
            sender,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_emitted_msg(
                id.to_string(),
                upstream.to_string(),
                broadcast_to.iter().map(|p| p.to_string()).collect(),
                *broadcasted_to,
                key.to_string(),
                sender.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Put(PutEvent::BroadcastReceived {
            id,
            target,
            requester,
            key,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_received_msg(
                id.to_string(),
                requester.to_string(),
                target.to_string(),
                key.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Get(GetEvent::GetSuccess {
            id,
            key,
            timestamp,
            requester,
            target,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::get_contract_msg(
                requester.to_string(),
                target.to_string(),
                id.to_string(),
                key.to_string(),
                contract_location.as_f64(),
                *timestamp,
            );

            ws_stream.send(Message::Binary(msg.into())).await
        }
        // GetEvent::Request and GetEvent::GetNotFound fall through to catch-all
        // TODO(#2456): Add FlatBuffer messages for GetEvent::Request and GetEvent::GetNotFound
        // when metrics server is enhanced to support these event types.
        EventKind::Subscribe(SubscribeEvent::SubscribeSuccess {
            id,
            key,
            at,
            timestamp,
            requester,
            ..
        }) => {
            if let (Some(at_addr), Some(at_loc)) = (at.socket_addr(), at.location()) {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let at_id = PeerId::new(at.pub_key().clone(), at_addr);
                let msg = ContractChange::subscribed_msg(
                    requester.to_string(),
                    id.to_string(),
                    key.to_string(),
                    contract_location.as_f64(),
                    at_id.to_string(),
                    at_loc.as_f64(),
                    *timestamp,
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        // SubscribeEvent::Request and SubscribeEvent::SubscribeNotFound fall through to catch-all
        // TODO(#2456): Add FlatBuffer messages for SubscribeEvent::Request and SubscribeEvent::SubscribeNotFound
        // when metrics server is enhanced to support these event types.
        EventKind::Update(UpdateEvent::Request {
            id,
            requester,
            key,
            target,
            timestamp,
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
                let msg = ContractChange::update_request_msg(
                    id.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Update(UpdateEvent::UpdateSuccess {
            id,
            requester,
            target,
            key,
            timestamp,
            ..
        }) => {
            if let Some(target_addr) = target.socket_addr() {
                let contract_location = Location::from_contract_key(key.as_bytes());
                let target_id = PeerId::new(target.pub_key().clone(), target_addr);
                let msg = ContractChange::update_success_msg(
                    id.to_string(),
                    key.to_string(),
                    requester.to_string(),
                    target_id.to_string(),
                    *timestamp,
                    contract_location.as_f64(),
                );
                ws_stream.send(Message::Binary(msg.into())).await
            } else {
                Ok(())
            }
        }
        EventKind::Update(UpdateEvent::BroadcastEmitted {
            id,
            upstream,
            broadcast_to, // broadcast_to n peers
            broadcasted_to,
            key,
            sender,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_emitted_msg(
                id.to_string(),
                upstream.to_string(),
                broadcast_to.iter().map(|p| p.to_string()).collect(),
                *broadcasted_to,
                key.to_string(),
                sender.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Update(UpdateEvent::BroadcastReceived {
            id,
            target,
            requester,
            key,
            timestamp,
            ..
        }) => {
            let contract_location = Location::from_contract_key(key.as_bytes());
            let msg = ContractChange::broadcast_received_msg(
                id.to_string(),
                target.to_string(),
                requester.to_string(),
                key.to_string(),
                *timestamp,
                contract_location.as_f64(),
            );
            ws_stream.send(Message::Binary(msg.into())).await
        }
        EventKind::Connect(_)
        | EventKind::Put(_)
        | EventKind::Get(_)
        | EventKind::Subscribe(_)
        | EventKind::Route(_)
        | EventKind::Update(_)
        | EventKind::Transfer(_)
        | EventKind::Lifecycle(_)
        | EventKind::Ignored
        | EventKind::Timeout { .. }
        | EventKind::TransportSnapshot(_)
        | EventKind::InterestSync(_)
        | EventKind::RoutingDecision(_)
        | EventKind::RouterSnapshot(_) => Ok(()),
    };
    if let Err(error) = res {
        tracing::warn!(%error, "Error while sending message to network metrics server");
    }
}

pub(crate) async fn received_from_metrics_server(
    ws_stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    msg: tokio_tungstenite::tungstenite::Result<tokio_tungstenite::tungstenite::Message>,
) {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    match msg {
        Ok(Message::Ping(ping)) => {
            if let Err(e) = ws_stream.send(Message::Pong(ping)).await {
                tracing::debug!(error = %e, "failed to send pong to metrics server");
            }
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
    use std::time::{Duration, SystemTime};

    use dashmap::DashMap;
    use opentelemetry::{
        Context, KeyValue, global,
        trace::{self, Span, TraceContextExt},
    };
    use tokio::sync::mpsc;

    use futures::FutureExt;

    use crate::config::GlobalExecutor;

    use super::*;

    struct OTSpan {
        inner: global::BoxedSpan,
        last_log: SystemTime,
    }

    impl OTSpan {
        fn new(transaction: Transaction) -> Self {
            use trace::Tracer;

            let tracer = global::tracer("freenet");
            let tx_bytes = transaction.as_bytes();
            let mut span_id = [0; 8];
            span_id.copy_from_slice(&tx_bytes[8..]);
            let start_time = transaction.started();
            // opentelemetry 0.32 removed the `trace_id`/`span_id` fields from
            // `SpanBuilder`; trace identity is now seeded from the parent
            // `Context`. We anchor the span on a deterministic remote
            // `SpanContext` derived from the transaction bytes so all events of
            // a transaction continue to share a stable trace_id. The child span
            // receives a fresh span_id from the SDK id generator (no longer
            // settable through the public API), with our deterministic span_id
            // recorded as the parent span_id.
            let parent_span_context = trace::SpanContext::new(
                trace::TraceId::from_bytes(tx_bytes),
                trace::SpanId::from_bytes(span_id),
                trace::TraceFlags::SAMPLED,
                true,
                trace::TraceState::default(),
            );
            let parent_cx = Context::current().with_remote_span_context(parent_span_context);
            let builder = trace::SpanBuilder::from_name(
                transaction.transaction_type().description().to_string(),
            )
            .with_start_time(start_time)
            .with_attributes(vec![
                KeyValue::new("transaction", transaction.to_string()),
                KeyValue::new("tx_type", transaction.transaction_type().description()),
            ]);
            let inner = tracer.build_with_context(builder, &parent_cx);
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
            unreachable!("add_event_with_timestamp is not explicitly called on OTSpan")
        }

        fn update_name<T>(&mut self, _: T)
        where
            T: Into<std::borrow::Cow<'static, str>>,
        {
            unreachable!("update_name shouldn't be called on OTSpan as span name is fixed")
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

    /// For tests running in a single process is important that span tracking is global across threads and simulated peers.
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
                crate::deterministic_select! {
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
                    },
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
                    },
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
                    // Non-blocking, same rationale as `EventRegister`: this is
                    // awaited from the network event loop's hot path via
                    // `DynamicRegister`, so a blocking `.send().await` here would
                    // wedge the loop if the consumer stalls. Drop on full
                    // (channel-safety.md). Best-effort OT telemetry; `trace-ot`
                    // is a non-default debug build. `match` (not `let _ =`)
                    // satisfies the crate's `let_underscore_must_use` deny lint.
                    match self.log_sender.try_send(log_msg) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(
            &mut self,
            tx: Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> BoxFuture<'_, ()> {
            async move {
                if cfg!(test) {
                    // Non-blocking, same rationale as `register_events` above.
                    // Best-effort; intentionally discard the result (drop on
                    // full or closed). `#[allow]` per the crate convention for
                    // deliberate `must_use` discards (e.g. tracing.rs:1831).
                    #[allow(clippy::let_underscore_must_use)]
                    let _ = self.finished_tx_notifier.try_send(tx);
                }
            }
            .boxed()
        }

        fn get_router_events(
            &self,
            _number: usize,
        ) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
            async { Ok(vec![]) }.boxed()
        }
    }
}

// `pub(crate)` (not `pub(super)`): `OTEventRegister` is re-exported again as
// `pub(crate)` from `tracing.rs` and constructed in `node.rs` / `testing_impl`,
// so it must be visible crate-wide. The narrower `pub(super)` broke the
// `trace-ot` build after the tracing module split (see #4225).
#[cfg(feature = "trace-ot")]
pub(crate) use opentelemetry_tracer::OTEventRegister;
