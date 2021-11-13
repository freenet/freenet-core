use std::sync::atomic::Ordering::SeqCst;
use std::{borrow::Cow, sync::atomic::AtomicUsize, time::Instant};

use crate::{
    conn_manager::PeerKey,
    message::{Message, Transaction},
};

#[cfg(test)]
pub(super) use test_utils::TestEventListener;

static LOG_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy)]
pub(crate) struct ListenerLogId(usize);

/// A type that reacts to incoming messages from the network.
/// It injects itself at the message event loop.
///
/// This type then can emit it's own information to adjacent systems
/// or is a no-op.
pub(crate) trait EventListener {
    fn event_received(&mut self, ev: EventLog) -> ListenerLogId;
    fn finish(&mut self, id: ListenerLogId, status: LogOpStatus);
}

pub(crate) struct EventLog<'a> {
    tx: &'a Transaction,
    peer_id: &'a PeerKey,
    event_str: Cow<'a, str>,
}

impl<'a> EventLog<'a> {
    pub fn new(msg: &Message, peer_id: &PeerKey) -> Self {
        todo!()
    }
}

pub(crate) enum LogOpStatus {
    Ok,
    Failure(String),
}

impl Default for LogOpStatus {
    fn default() -> Self {
        Self::Ok
    }
}

struct MessageLog {
    log_id: ListenerLogId,
    peer_id: PeerKey,
    ts: Instant,
    kind: String,
    status: LogOpStatus,
}

pub(super) struct EventRegister {}

impl EventRegister {
    pub fn new() -> Self {
        EventRegister {}
    }
}

impl EventListener for EventRegister {
    fn event_received(&mut self, log: EventLog) -> ListenerLogId {
        let (_msg_log, log_id) = create_log(log);
        // TODO
        log_id
    }

    fn finish(&mut self, id: ListenerLogId, status: LogOpStatus) {
        todo!()
    }
}

#[inline]
fn create_log(log: EventLog) -> (MessageLog, ListenerLogId) {
    let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
    let EventLog {
        peer_id, event_str, ..
    } = log;
    let msg_log = MessageLog {
        log_id,
        ts: Instant::now(),
        peer_id: *peer_id,
        kind: event_str.into_owned(),
        status: LogOpStatus::default(),
    };
    (msg_log, log_id)
}

#[cfg(test)]
mod test_utils {
    use std::sync::Arc;

    use dashmap::DashMap;
    use parking_lot::RwLock;

    use super::*;

    #[derive(Clone)]
    pub(in crate::node) struct TestEventListener {
        node_labels: Arc<DashMap<String, PeerKey>>,
        tx_log: Arc<DashMap<Transaction, Vec<ListenerLogId>>>,
        logs: Arc<RwLock<Vec<MessageLog>>>,
    }

    impl TestEventListener {
        pub fn new() -> Self {
            TestEventListener {
                node_labels: Arc::new(DashMap::new()),
                tx_log: Arc::new(DashMap::new()),
                logs: Arc::new(RwLock::new(Vec::new())),
            }
        }

        pub fn add_node(&mut self, label: String, peer: PeerKey) {
            self.node_labels.insert(label, peer);
        }
    }

    impl super::EventListener for TestEventListener {
        fn event_received(&mut self, log: EventLog) -> ListenerLogId {
            let tx = log.tx;
            let mut logs = self.logs.write();
            let (msg_log, log_id) = create_log(log);
            logs.push(msg_log);
            std::mem::drop(logs);
            self.tx_log.entry(*tx).or_default().push(log_id);
            log_id
        }

        fn finish(&mut self, id: ListenerLogId, status: LogOpStatus) {
            let guard = &mut *self.logs.write();
            let log = &mut guard[id.0];
            log.status = status;
        }
    }
}
