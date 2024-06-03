use byteorder::ByteOrder;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, Error},
};

use std::{path::Path, sync::atomic::AtomicUsize};

use tokio::sync::Mutex;

use super::{DynError, EventKind, NetLogMessage, RouteEvent, NEW_RECORDS_TS};

static FILE_LOCK: Mutex<()> = Mutex::const_new(());

const RECORD_LENGTH: usize = core::mem::size_of::<u32>();
const EVENT_KIND_LENGTH: usize = 1;
const EVENT_LOG_HEADER_SIZE: usize = RECORD_LENGTH + EVENT_KIND_LENGTH; // len + varint id
#[cfg(not(test))]
pub(super) const MAX_LOG_RECORDS: usize = 100_000;
#[cfg(test)]
pub(super) const MAX_LOG_RECORDS: usize = 10_000;
pub(super) const REMOVE_RECS: usize = 1000 + EVENT_REGISTER_BATCH_SIZE; // making space for 1000 new records
const EVENT_REGISTER_BATCH_SIZE: usize = 100;
pub(super) const BATCH_SIZE: usize = EVENT_REGISTER_BATCH_SIZE;

type DefaultEndian = byteorder::BigEndian;

#[derive(Debug, Default, Copy, Clone)]
pub(super) struct States {
    total: usize,
    connect_events: usize,
    put_events: usize,
    get_events: usize,
    route_events: usize,
    subscribed_events: usize,
    ignored_events: usize,
    disconnected_events: usize,
}

impl core::ops::AddAssign for States {
    fn add_assign(&mut self, rhs: Self) {
        self.total += rhs.total;
        self.connect_events += rhs.connect_events;
        self.put_events += rhs.put_events;
        self.get_events += rhs.get_events;
        self.route_events += rhs.route_events;
        self.subscribed_events += rhs.subscribed_events;
        self.ignored_events += rhs.ignored_events;
        self.disconnected_events += rhs.disconnected_events;
    }
}

impl core::ops::SubAssign<u8> for States {
    fn sub_assign(&mut self, rhs: u8) {
        self.total = self.total.saturating_sub(1);
        match rhs {
            EventKind::CONNECT => self.connect_events = self.connect_events.saturating_sub(1),
            EventKind::PUT => self.put_events = self.put_events.saturating_sub(1),
            EventKind::GET => self.get_events = self.get_events.saturating_sub(1),
            EventKind::ROUTE => self.route_events = self.route_events.saturating_sub(1),
            EventKind::SUBSCRIBED => {
                self.subscribed_events = self.subscribed_events.saturating_sub(1)
            }
            EventKind::IGNORED => self.ignored_events = self.ignored_events.saturating_sub(1),
            EventKind::DISCONNECTED => {
                self.disconnected_events = self.disconnected_events.saturating_sub(1)
            }
            _ => unreachable!(),
        }
    }
}

impl core::ops::AddAssign<u8> for States {
    fn add_assign(&mut self, rhs: u8) {
        self.total += 1;

        match rhs {
            EventKind::CONNECT => self.connect_events += 1,
            EventKind::PUT => self.put_events += 1,
            EventKind::GET => self.get_events += 1,
            EventKind::ROUTE => self.route_events += 1,
            EventKind::SUBSCRIBED => self.subscribed_events += 1,
            EventKind::IGNORED => self.ignored_events += 1,
            EventKind::DISCONNECTED => self.disconnected_events += 1,
            _ => unreachable!(),
        }
    }
}

impl core::ops::SubAssign for States {
    fn sub_assign(&mut self, rhs: Self) {
        self.total -= rhs.total;
        self.connect_events -= rhs.connect_events;
        self.put_events -= rhs.put_events;
        self.get_events -= rhs.get_events;
        self.route_events -= rhs.route_events;
        self.subscribed_events -= rhs.subscribed_events;
        self.ignored_events -= rhs.ignored_events;
        self.disconnected_events -= rhs.disconnected_events;
    }
}

pub(super) struct Batch {
    pub batch: Vec<NetLogMessage>,
    pub states: States,
}

impl Batch {
    #[inline]
    pub fn new(cap: usize) -> Self {
        Self {
            batch: Vec::with_capacity(cap),
            states: Default::default(),
        }
    }

    #[inline]
    fn push(&mut self, log: NetLogMessage) {
        match log.kind.varint_id() {
            EventKind::CONNECT => self.states.connect_events += 1,
            EventKind::PUT => self.states.put_events += 1,
            EventKind::GET => self.states.get_events += 1,
            EventKind::ROUTE => self.states.route_events += 1,
            EventKind::SUBSCRIBED => self.states.subscribed_events += 1,
            EventKind::IGNORED => self.states.ignored_events += 1,
            EventKind::DISCONNECTED => self.states.disconnected_events += 1,
            _ => unreachable!(),
        }
        self.states.total += 1;
        self.batch.push(log);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    #[inline]
    fn clear(&mut self) {
        self.batch.clear();
        self.states = Default::default();
    }
}

pub(super) struct LogFile {
    file: BufReader<File>,
    // make this configurable?
    max_log_records: usize,
    pub(super) batch: Batch,
    current_states: States,
    states: States,
}

impl LogFile {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .await?;
        let mut file = BufReader::new(file);
        tracing::error!("open {}", path.display());
        let states = Self::num_lines(&mut file).await.expect("non IO error");
        tracing::error!("states {:?}", states);
        Ok(Self {
            file,
            max_log_records: MAX_LOG_RECORDS,
            batch: Batch {
                batch: Vec::with_capacity(BATCH_SIZE),
                states: Default::default(),
            },
            current_states: Default::default(),
            states,
        })
    }

    pub(super) fn update_states(&mut self, states: States) {
        self.states += states;
    }

    pub fn encode_log(
        log: &NetLogMessage,
    ) -> bincode::Result<([u8; EVENT_LOG_HEADER_SIZE], Vec<u8>)> {
        let serialized = bincode::serialize(&log)?;
        let mut header = [0; EVENT_LOG_HEADER_SIZE];
        DefaultEndian::write_u32(&mut header, serialized.len() as u32);
        header[4] = log.kind.varint_id(); // event kind
        Ok((header, serialized))
    }

    async fn num_lines(file: &mut (impl AsyncRead + AsyncSeek + Unpin)) -> io::Result<States> {
        let mut num_records = 0;
        let mut connect_events: usize = 0;
        let mut put_events: usize = 0;
        let mut get_events: usize = 0;
        let mut route_events: usize = 0;
        let mut subscribed_events: usize = 0;
        let mut ignored_events: usize = 0;
        let mut disconnected_events: usize = 0;

        let mut buf = [0; EVENT_LOG_HEADER_SIZE]; // Read the u32 length prefix + u8 event kind

        loop {
            let bytes_read = file.read_exact(&mut buf).await;
            if bytes_read.is_err() {
                break;
            }
            num_records += 1;

            // Seek to the next record without reading its contents
            let length = DefaultEndian::read_u32(&buf[..4]) as u64;

            match buf[4] {
                EventKind::CONNECT => connect_events += 1,
                EventKind::PUT => put_events += 1,
                EventKind::GET => get_events += 1,
                EventKind::ROUTE => route_events += 1,
                EventKind::SUBSCRIBED => subscribed_events += 1,
                EventKind::IGNORED => ignored_events += 1,
                EventKind::DISCONNECTED => disconnected_events += 1,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Unknown event kind",
                    ))
                }
            }

            if (file.seek(io::SeekFrom::Current(length as i64)).await).is_err() {
                break;
            }
        }

        Ok(States {
            total: num_records,
            connect_events,
            put_events,
            get_events,
            route_events,
            subscribed_events,
            ignored_events,
            disconnected_events,
        })
    }

    pub async fn persist_log(&mut self, log: NetLogMessage) {
        self.batch.push(log);
        let mut batch_buf = vec![];

        if self.batch.len() >= BATCH_SIZE {
            let moved_batch = std::mem::replace(&mut self.batch, Batch::new(BATCH_SIZE));
            let batch_states = moved_batch.states;
            let serialization_task =
                tokio::task::spawn_blocking(move || Self::encode_batch(&moved_batch));

            match serialization_task.await {
                Ok(Ok(serialized_data)) => {
                    batch_buf = serialized_data;
                    self.current_states += batch_states;
                    self.batch.clear(); // Clear the batch for new data
                }
                _ => {
                    panic!("Failed serializing log");
                }
            }
        }

        if self.current_states.total >= BATCH_SIZE {
            {
                let res = self.write_all(&batch_buf).await;
                if res.is_err() {
                    panic!("Failed writing to log file");
                }
            }
            self.states += self.current_states;
            self.current_states = Default::default();
        }

        // Check the number of lines and truncate if needed
        if self.states.total > self.max_log_records {
            tracing::info!("before truncating {:?}", self.states);

            if let Err(err) = self.truncate_records(REMOVE_RECS).await {
                tracing::error!("Failed truncating log file: {:?}", err);
                panic!("Failed truncating log file");
            }
        }
    }

    pub async fn truncate_records(
        &mut self,
        remove_records: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _guard = FILE_LOCK.lock().await;
        self.file.rewind().await?;
        // tracing::debug!(position = file.stream_position().await.unwrap());
        let mut records_count = 0;
        let mut removed_states = States::default();
        while records_count < remove_records {
            let mut header = [0u8; EVENT_LOG_HEADER_SIZE];
            if let Err(error) = self.file.read_exact(&mut header).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = self.file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            let length = DefaultEndian::read_u32(&header[..4]);
            if let Err(error) = self.file.seek(io::SeekFrom::Current(length as i64)).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = self.file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            removed_states += header[4];
            records_count += 1;
        }

        // Copy the rest of the file to the buffer
        let mut buffer = Vec::new();
        if let Err(error) = self.file.read_to_end(&mut buffer).await {
            if !matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                let pos = self.file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
        }

        self.states -= removed_states;

        tracing::error!("removed {removed_states:?} remaining {:?}", self.states);

        // Seek back to the beginning and write the remaining content
        self.file.rewind().await?;
        self.file.get_mut().rewind().await?;
        self.file.write_all(&buffer).await?;
        // Truncate the file to the new size
        self.file.get_ref().set_len(buffer.len() as u64).await?;
        self.file.get_ref().sync_all().await?;

        {
            self.file.rewind().await?;
            let records = Self::get_router_events_in(MAX_LOG_RECORDS, &mut self.file)
                .await
                .unwrap();
            tracing::error!("records {:?}", records.len());
        }

        self.file.seek(io::SeekFrom::End(0)).await?;
        Ok(())
    }

    pub async fn get_router_events(
        max_event_number: usize,
        event_log_path: &Path,
    ) -> Result<Vec<RouteEvent>, DynError> {
        const MAX_EVENT_HISTORY: usize = 10_000;
        let event_num = max_event_number.min(MAX_EVENT_HISTORY);

        let _guard: tokio::sync::MutexGuard<'_, ()> = FILE_LOCK.lock().await;
        let mut file = BufReader::new(OpenOptions::new().read(true).open(event_log_path).await?);

        Self::get_router_events_in(event_num, &mut file).await
    }

    async fn get_router_events_in(
        event_num: usize,
        file: &mut (impl AsyncRead + AsyncSeek + Unpin),
    ) -> Result<Vec<RouteEvent>, DynError> {
        let new_records_ts = NEW_RECORDS_TS
            .get()
            .expect("set on initialization")
            .duration_since(std::time::UNIX_EPOCH)
            .expect("should be older than unix epoch")
            .as_secs() as i64;

        let mut records = Vec::with_capacity(event_num);
        let mut num_records = 0;
        while num_records < event_num {
            let mut header = [0; EVENT_LOG_HEADER_SIZE];

            // Read the length prefix
            if let Err(error) = file.read_exact(&mut header).await {
                if !matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    let pos = file.stream_position().await;
                    tracing::error!(%error, ?pos, "error while trying to read file");
                    return Err(error.into());
                } else {
                    break;
                }
            }

            let length = DefaultEndian::read_u32(&header[..4]);
            if header[4] == EventKind::ROUTE {
                let mut buf = vec![0; length as usize];
                file.read_exact(&mut buf).await?;
                records.push(buf);
            } else {
                file.seek(io::SeekFrom::Current(length as i64)).await?;
            }

            num_records += 1;
        }

        tracing::info!(len = records.len(), total = num_records, "records read");

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

    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let _guard = FILE_LOCK.lock().await;
        if let Err(err) = self.file.get_mut().write_all(data).await {
            tracing::error!("Failed writting to event log: {err}");
            return Err(err);
        }

        if let Err(err) = self.file.get_mut().sync_all().await {
            tracing::error!("Failed syncing event log: {err}");
            return Err(err);
        }
        Ok(())
    }

    pub fn encode_batch(batch: &Batch) -> bincode::Result<Vec<u8>> {
        let mut batch_serialized_data = Vec::with_capacity(BATCH_SIZE * 1024);
        for log_item in &batch.batch {
            let (header, mut serialized) = match Self::encode_log(log_item) {
                Err(err) => {
                    tracing::error!("Failed serializing log: {err}");
                    return Err(err);
                }
                Ok(serialized) => serialized,
            };

            batch_serialized_data.extend_from_slice(&header);
            batch_serialized_data.append(&mut serialized);
        }

        Ok(batch_serialized_data)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use tracing::level_filters::LevelFilter;

    use crate::{
        dev_tool::{PeerId, Transaction},
        tracing::NetEventLog,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write() -> Result<(), DynError> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        crate::config::set_logger(Some(LevelFilter::TRACE));
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // force a truncation
        const TEST_LOGS: usize = MAX_LOG_RECORDS;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];

        for _ in 0..TEST_LOGS {
            let tx: Transaction = gen.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = PeerId::random();
            peers.push(peer);
        }
        let mut total_route_events: usize = 0;

        for i in 0..TEST_LOGS {
            let kind: EventKind = gen.arbitrary()?;
            // The route events in first REMOVE_RECS will be dropped
            if matches!(kind, EventKind::Route(_)) {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }

        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        let ev = LogFile::get_router_events(TEST_LOGS, &log_path).await?;
        assert_eq!(ev.len(), total_route_events);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write_small() -> Result<(), DynError> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        crate::config::set_logger(Some(LevelFilter::TRACE));
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // force a truncation
        const TEST_LOGS: usize = 100;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];

        for _ in 0..TEST_LOGS {
            let tx: Transaction = gen.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = PeerId::random();
            peers.push(peer);
        }
        let mut total_route_events: usize = 0;

        for i in 0..TEST_LOGS {
            let kind: EventKind = gen.arbitrary()?;
            // The route events in first REMOVE_RECS will be dropped
            if matches!(kind, EventKind::Route(_)) {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }

        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        let ev = LogFile::get_router_events(TEST_LOGS, &log_path).await?;
        assert_eq!(ev.len(), total_route_events);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write_truncate() -> Result<(), DynError> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        crate::config::set_logger(Some(LevelFilter::TRACE));
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // force a truncation
        const TEST_LOGS: usize = MAX_LOG_RECORDS + 100;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];

        for _ in 0..TEST_LOGS {
            let tx: Transaction = gen.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = PeerId::random();
            peers.push(peer);
        }
        let mut total_route_events: usize = 0;

        for i in 0..TEST_LOGS {
            let kind: EventKind = gen.arbitrary()?;
            // The route events in first REMOVE_RECS will be dropped
            if matches!(kind, EventKind::Route(_)) && i >= REMOVE_RECS {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }

        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        let ev = LogFile::get_router_events(TEST_LOGS, &log_path).await?;
        assert_eq!(ev.len(), total_route_events);
        Ok(())
    }
}
