use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use byteorder::ByteOrder;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, Error},
    sync::Mutex,
};

use super::{EventKind, NEW_RECORDS_TS, NetLogMessage, RouteEvent};

static FILE_LOCK: Mutex<()> = Mutex::const_new(());

/// Log the incompatible-format warning only once per process lifetime.
static LOGGED_COMPAT_WARNING: AtomicBool = AtomicBool::new(false);

const RECORD_LENGTH: usize = core::mem::size_of::<u32>();
const EVENT_KIND_LENGTH: usize = 1;
const EVENT_LOG_HEADER_SIZE: usize = RECORD_LENGTH + EVENT_KIND_LENGTH; // len + varint id
#[cfg(not(test))]
pub(super) const MAX_LOG_RECORDS: usize = 100_000;
#[cfg(test)]
pub(super) const MAX_LOG_RECORDS: usize = 10_000;
// Drop ~25% of records on each compaction. `truncate_records` rewrites every
// surviving record to disk, so a small REMOVE_RECS forces the entire file to
// be copied on every ~REMOVE_RECS-record append cycle. With a flat 1005
// against the 100_000-record cap this produced ~99x write amplification,
// observed via /proc/$pid/io as ~9 MB/s of sustained disk writes from
// compaction churn alone on a busy gateway. Scaling with MAX_LOG_RECORDS
// keeps the trim fraction constant across cfg(test) and cfg(not(test)); the
// +BATCH_SIZE buffer preserves the original "headroom for one fresh batch"
// intent.
//
// 25% (rather than e.g. 50%) is a tradeoff between write amplification and
// the size of the trailing window of router-training events on disk. At 25%,
// amortized rewrite cost is ~3x records-per-record-shed and the file
// oscillates between ~75% and ~100% of the cap; at 50% it would be ~1.5x
// but the on-disk window would shrink toward 50% of the cap, halving the
// router-training history available after each compaction. 25% sits at a
// sane point in that tradeoff without losing meaningful history.
pub(super) const REMOVE_RECS: usize = MAX_LOG_RECORDS / 4 + EVENT_REGISTER_BATCH_SIZE;
// Compile-time guards. truncate_records rewrites every surviving record on
// each pass, so REMOVE_RECS must be a meaningful fraction of the cap
// (lower-bound ensures bounded write amplification) but never more than half
// the cap (upper-bound prevents a future REMOVE_RECS = MAX_LOG_RECORDS misuse
// from clearing the entire log and leaving the steady-state band's lower
// edge at zero or underflowed).
const _: () = assert!(
    REMOVE_RECS * 5 >= MAX_LOG_RECORDS,
    "REMOVE_RECS must drop at least 20% of MAX_LOG_RECORDS per compaction; \
     truncate_records rewrites the entire surviving log on every pass, so a \
     smaller fraction makes every ~REMOVE_RECS appends rewrite ~MAX_LOG_RECORDS \
     bytes (the gateway-write-amplification bug fixed by this guard)"
);
const _: () = assert!(
    REMOVE_RECS * 2 <= MAX_LOG_RECORDS,
    "REMOVE_RECS must drop at most 50% of MAX_LOG_RECORDS per compaction; \
     a larger fraction shrinks the post-compaction record window below half \
     the cap and risks clearing the log entirely if REMOVE_RECS approaches \
     MAX_LOG_RECORDS"
);
// The truncation tests assume one full BATCH_SIZE batch flush carries
// num_recs from MAX_LOG_RECORDS to MAX_LOG_RECORDS + BATCH_SIZE; that only
// holds when MAX_LOG_RECORDS is a multiple of BATCH_SIZE. Pin the relation
// at compile time so changing either constant doesn't silently shift the
// trigger point and invalidate the regression tests.
const _: () = assert!(
    MAX_LOG_RECORDS % EVENT_REGISTER_BATCH_SIZE == 0,
    "MAX_LOG_RECORDS must be a multiple of EVENT_REGISTER_BATCH_SIZE so \
     compaction triggers exactly at MAX_LOG_RECORDS + BATCH_SIZE"
);
// Reduced from 100 to 5 to ensure events are written more frequently,
// especially important for integration tests which generate few events
const EVENT_REGISTER_BATCH_SIZE: usize = 5;
pub(super) const BATCH_SIZE: usize = EVENT_REGISTER_BATCH_SIZE;

type DefaultEndian = byteorder::BigEndian;

pub(super) struct Batch {
    pub batch: Vec<NetLogMessage>,
    pub num_writes: usize,
}

impl Batch {
    #[inline]
    pub fn new(cap: usize) -> Self {
        Self {
            batch: Vec::with_capacity(cap),
            num_writes: 0,
        }
    }

    #[inline]
    fn push(&mut self, log: NetLogMessage) {
        self.num_writes += 1;
        self.batch.push(log);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    #[allow(dead_code)]
    #[inline]
    fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    #[allow(dead_code)]
    #[inline]
    fn clear(&mut self) {
        self.batch.clear();
        self.num_writes = 0;
    }
}

pub(super) struct LogFile {
    file: Option<BufReader<File>>,
    path: PathBuf,
    rewrite_path: PathBuf,
    // make this configurable?
    max_log_records: usize,
    pub(super) batch: Batch,
    num_writes: usize,
    num_recs: usize,
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
        let num_recs = Self::num_lines(&mut file).await.expect("non IO error");
        Ok(Self {
            file: Some(file),
            path: path.to_path_buf(),
            rewrite_path: path.with_extension("rewrite"),
            max_log_records: MAX_LOG_RECORDS,
            batch: Batch {
                batch: Vec::with_capacity(BATCH_SIZE),
                num_writes: 0,
            },
            num_writes: 0,
            num_recs,
        })
    }

    pub(super) fn update_recs(&mut self, recs: usize) {
        self.num_recs += recs;
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

    async fn num_lines(file: &mut (impl AsyncRead + AsyncSeek + Unpin)) -> io::Result<usize> {
        let mut num_records = 0;

        let mut buf = [0; EVENT_LOG_HEADER_SIZE]; // Read the u32 length prefix + u8 event kind

        loop {
            let bytes_read = file.read_exact(&mut buf).await;
            if bytes_read.is_err() {
                break;
            }
            num_records += 1;

            // Seek to the next record without reading its contents
            let length = DefaultEndian::read_u32(&buf[..4]) as u64;

            // Accept all event kinds - unknown kinds are skipped gracefully
            // for forward/backward compatibility between versions
            let _event_kind = buf[4];

            if (file.seek(io::SeekFrom::Current(length as i64)).await).is_err() {
                break;
            }
        }

        Ok(num_records)
    }

    pub async fn persist_log(&mut self, log: NetLogMessage) {
        self.batch.push(log);
        let mut batch_buf = vec![];

        if self.batch.len() >= BATCH_SIZE {
            let moved_batch = std::mem::replace(&mut self.batch, Batch::new(BATCH_SIZE));
            let batch_writes = moved_batch.num_writes;
            let serialization_task =
                tokio::task::spawn_blocking(move || Self::encode_batch(&moved_batch));

            match serialization_task.await {
                Ok(Ok(serialized_data)) => {
                    batch_buf = serialized_data;
                    self.num_writes += batch_writes;
                }
                Ok(Err(err)) => {
                    tracing::error!("Failed serializing event log batch: {err}");
                    return;
                }
                Err(err) => {
                    tracing::error!("Event log serialization task panicked: {err}");
                    return;
                }
            }
        }

        if self.num_writes >= BATCH_SIZE {
            if let Err(err) = self.write_all(&batch_buf).await {
                tracing::error!("Failed writing to event log file: {err}");
                // Drop the batch rather than crashing the node
                self.num_writes = 0;
                return;
            }
            self.num_recs += self.num_writes;
            self.num_writes = 0;
        }

        // Check the number of lines and truncate if needed
        if self.num_recs > self.max_log_records {
            if let Err(err) = self.truncate_records(REMOVE_RECS).await {
                tracing::error!("Failed truncating event log file: {err}");
            }
        }
    }

    pub async fn truncate_records(
        &mut self,
        remove_records: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _guard = FILE_LOCK.lock().await;
        let mut file = self.file.take().unwrap();
        file.rewind().await?;
        file.get_mut().rewind().await?;

        let mut records_count = 0;
        while records_count < remove_records {
            let mut header = [0u8; EVENT_LOG_HEADER_SIZE];
            if let Err(error) = file.read_exact(&mut header).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            let length = DefaultEndian::read_u32(&header[..4]);
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

        let mut bk = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&self.rewrite_path)
            .await?;

        let mut num_recs = 0;
        loop {
            let mut header = [0u8; EVENT_LOG_HEADER_SIZE];
            if let Err(error) = file.read_exact(&mut header).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }

            let length = DefaultEndian::read_u32(&header[..4]);
            let mut buf = vec![0u8; EVENT_LOG_HEADER_SIZE + length as usize];
            buf[..EVENT_LOG_HEADER_SIZE].copy_from_slice(&header);
            if let Err(error) = file.read_exact(&mut buf[EVENT_LOG_HEADER_SIZE..]).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = file.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }

            num_recs += 1;

            if let Err(error) = bk.write_all(&buf).await {
                let pos = bk.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to write file");
                return Err(error.into());
            }
        }

        self.num_recs = num_recs;

        drop(bk);
        drop(file);
        std::fs::remove_file(&self.path)?;
        std::fs::rename(&self.rewrite_path, &self.path)?;

        self.file = Some(BufReader::new(
            OpenOptions::new()
                .read(true)
                .append(true)
                .write(true)
                .open(&self.path)
                .await?,
        ));

        Ok(())
    }

    /// Read all events from an AOF file (for event aggregation).
    pub async fn read_all_events(event_log_path: &Path) -> anyhow::Result<Vec<NetLogMessage>> {
        let _guard: tokio::sync::MutexGuard<'_, ()> = FILE_LOCK.lock().await;
        let mut file = BufReader::new(OpenOptions::new().read(true).open(event_log_path).await?);

        Self::read_all_events_from(&mut file).await
    }

    async fn read_all_events_from(
        file: &mut (impl AsyncRead + AsyncSeek + Unpin),
    ) -> anyhow::Result<Vec<NetLogMessage>> {
        let new_records_ts = NEW_RECORDS_TS
            .get()
            .map(|ts| {
                ts.duration_since(std::time::UNIX_EPOCH)
                    .expect("should be older than unix epoch")
                    .as_secs() as i64
            })
            .unwrap_or(0);

        let mut buffers = Vec::new();
        loop {
            let mut header = [0; EVENT_LOG_HEADER_SIZE];

            // Read the length prefix
            if let Err(error) = file.read_exact(&mut header).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                } else {
                    let pos = file.stream_position().await;
                    tracing::error!(%error, ?pos, "error while trying to read file");
                    return Err(error.into());
                }
            }

            let length = DefaultEndian::read_u32(&header[..4]);
            let mut buf = vec![0; length as usize];
            file.read_exact(&mut buf).await?;
            buffers.push(buf);
        }

        if buffers.is_empty() {
            return Ok(vec![]);
        }

        // Deserialize in blocking task to avoid blocking async runtime
        let deserialized = tokio::task::spawn_blocking(move || {
            let mut events = vec![];
            for buf in buffers {
                match bincode::deserialize::<NetLogMessage>(&buf) {
                    Ok(record) => {
                        let record_ts = record.datetime.timestamp();
                        if record_ts >= new_records_ts {
                            events.push(record);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(?e, "Failed to deserialize event record");
                    }
                }
            }
            events
        })
        .await?;

        Ok(deserialized)
    }

    pub async fn get_router_events(
        max_event_number: usize,
        event_log_path: &Path,
    ) -> anyhow::Result<Vec<RouteEvent>> {
        const MAX_EVENT_HISTORY: usize = 10_000;
        let event_num = max_event_number.min(MAX_EVENT_HISTORY);

        let _guard: tokio::sync::MutexGuard<'_, ()> = FILE_LOCK.lock().await;
        let mut file = BufReader::new(OpenOptions::new().read(true).open(event_log_path).await?);

        Self::get_router_events_in(event_num, &mut file).await
    }

    async fn get_router_events_in(
        event_num: usize,
        file: &mut (impl AsyncRead + AsyncSeek + Unpin),
    ) -> anyhow::Result<Vec<RouteEvent>> {
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

        if records.is_empty() {
            return Ok(vec![]);
        }

        let deserialized_records = tokio::task::spawn_blocking(move || {
            let mut filtered = vec![];
            let mut skipped = 0usize;
            for buf in records {
                let record: NetLogMessage = match bincode::deserialize(&buf) {
                    Ok(r) => r,
                    Err(_) => {
                        // Skip records from older versions with incompatible
                        // serialization format (e.g. PeerId field order change
                        // in v0.2.9). The event log is only used for router
                        // model training, so skipping stale records is safe.
                        skipped += 1;
                        continue;
                    }
                };
                if let EventKind::Route(outcome) = record.kind {
                    let record_ts = record.datetime.timestamp();
                    if record_ts >= new_records_ts {
                        filtered.push(outcome);
                    }
                }
            }
            if skipped > 0 && !LOGGED_COMPAT_WARNING.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    skipped,
                    "skipped event log records with incompatible serialization format \
                     (from a previous version); this warning will not repeat"
                );
            }
            Ok::<_, anyhow::Error>(filtered)
        })
        .await??;

        Ok(deserialized_records)
    }

    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let _guard = FILE_LOCK.lock().await;
        let file = self.file.as_mut().unwrap();
        if let Err(err) = file.get_mut().write_all(data).await {
            tracing::error!("Failed writting to event log: {err}");
            return Err(err);
        }

        if let Err(err) = file.get_mut().sync_all().await {
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

    use crate::{
        dev_tool::{PeerId, Transaction},
        tracing::NetEventLog,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write() -> anyhow::Result<()> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // force a truncation
        const TEST_LOGS: usize = MAX_LOG_RECORDS;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];

        for _ in 0..TEST_LOGS {
            let tx: Transaction = unstructured.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = PeerId::random();
            peers.push(peer);
        }
        let mut total_route_events: usize = 0;

        for i in 0..TEST_LOGS {
            let kind: EventKind = unstructured.arbitrary()?;
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
    async fn read_write_small() -> anyhow::Result<()> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // force a truncation
        const TEST_LOGS: usize = 100;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];

        for _ in 0..TEST_LOGS {
            let tx: Transaction = unstructured.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = PeerId::random();
            peers.push(peer);
        }
        let mut total_route_events: usize = 0;

        for i in 0..TEST_LOGS {
            let kind: EventKind = unstructured.arbitrary()?;
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
    async fn read_write_truncate() -> anyhow::Result<()> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // force a truncation
        const TEST_LOGS: usize = MAX_LOG_RECORDS + 100;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let mut transactions = vec![];
        let mut peers = vec![];
        let mut events = vec![];

        for _ in 0..TEST_LOGS {
            let tx: Transaction = unstructured.arbitrary()?;
            transactions.push(tx);
            let peer: PeerId = PeerId::random();
            peers.push(peer);
        }
        let mut total_route_events: usize = 0;

        for i in 0..TEST_LOGS {
            let kind: EventKind = unstructured.arbitrary()?;
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

    /// Verify the runtime behaviour matches the compile-time invariant on
    /// REMOVE_RECS: after a compaction fires, the log must have shed enough
    /// records that the next compaction is many appends away (not the very
    /// next batch). Regression test for the gateway-write-amplification bug
    /// where REMOVE_RECS=1005 against MAX_LOG_RECORDS=100_000 caused every
    /// compaction to rewrite ~99% of the file to drop ~1% of records,
    /// producing ~9 MB/s of sustained disk writes from compaction churn.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn truncation_leaves_meaningful_headroom() -> anyhow::Result<()> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Push exactly enough records to trigger one compaction with no
        // straggler appends afterwards, so num_recs reflects the post-trim
        // count rather than the steady-state fill-back.
        // The first compaction triggers at the batch boundary just above
        // MAX_LOG_RECORDS (i.e. at MAX_LOG_RECORDS + BATCH_SIZE records).
        const TEST_LOGS: usize = MAX_LOG_RECORDS + BATCH_SIZE;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let mut transactions = Vec::with_capacity(TEST_LOGS);
        let mut peers = Vec::with_capacity(TEST_LOGS);
        let mut events = Vec::with_capacity(TEST_LOGS);

        for _ in 0..TEST_LOGS {
            transactions.push(unstructured.arbitrary::<Transaction>()?);
            peers.push(PeerId::random());
        }
        for i in 0..TEST_LOGS {
            let kind: EventKind = unstructured.arbitrary()?;
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }
        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        // After compaction, num_recs should sit at most 80% of MAX_LOG_RECORDS.
        // With the original REMOVE_RECS = 1005 against MAX = 10_000 this would
        // land at ~9000 (90%) and the assertion would fail.
        assert!(
            log.num_recs * 5 <= MAX_LOG_RECORDS * 4,
            "after compaction num_recs={} is > 80% of MAX_LOG_RECORDS={}; \
             a small REMOVE_RECS makes every compaction rewrite ~all records \
             to drop a sliver",
            log.num_recs,
            MAX_LOG_RECORDS,
        );
        Ok(())
    }

    /// Steady-state regression: push enough records for multiple compactions
    /// and confirm the on-disk record count stays within the
    /// `[MAX_LOG_RECORDS - REMOVE_RECS, MAX_LOG_RECORDS + BATCH_SIZE]` band.
    /// Without the fix the band shrinks to `[MAX - 1005, MAX + BATCH_SIZE]`,
    /// i.e. compaction fires after every ~1005 appends and the file
    /// oscillates between 90% and 100% full instead of 75% and 100%.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sustained_load_keeps_log_bounded() -> anyhow::Result<()> {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Push enough records to force at least three compactions so the
        // assertion captures steady-state behaviour, not a single-pass
        // post-trim count.
        const TEST_LOGS: usize = 3 * MAX_LOG_RECORDS;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();

        // arbitrary::Unstructured returns default values once its buffer is
        // exhausted, so re-prime per record to keep the EventKind mix
        // varied enough that compaction logic exercises route-event paths.
        for i in 0..TEST_LOGS {
            // Cycle through the buffer to keep entropy across many records.
            let offset = (i * 31) % bytes.len();
            let mut unstructured = arbitrary::Unstructured::new(&bytes[offset..]);
            let tx: Transaction = unstructured.arbitrary()?;
            let peer = PeerId::random();
            let kind: EventKind = unstructured.arbitrary()?;
            let event = NetEventLog {
                tx: &tx,
                peer_id: peer,
                kind,
            };
            for msg in NetLogMessage::to_log_message(either::Either::Left(event)) {
                log.persist_log(msg).await;
            }
        }

        let upper_bound = MAX_LOG_RECORDS + BATCH_SIZE;
        let lower_bound = MAX_LOG_RECORDS.saturating_sub(REMOVE_RECS);
        assert!(
            log.num_recs >= lower_bound && log.num_recs <= upper_bound,
            "num_recs={} outside steady-state band [{lower_bound}, {upper_bound}] \
             after pushing {TEST_LOGS} records",
            log.num_recs,
        );
        Ok(())
    }
}
