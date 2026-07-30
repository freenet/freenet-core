use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use byteorder::ByteOrder;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, Error},
    sync::Mutex,
};

use super::{NEW_RECORDS_TS, NetLogMessage};

static FILE_LOCK: Mutex<()> = Mutex::const_new(());

const RECORD_LENGTH: usize = core::mem::size_of::<u32>();
const EVENT_KIND_LENGTH: usize = 1;
const EVENT_LOG_HEADER_SIZE: usize = RECORD_LENGTH + EVENT_KIND_LENGTH; // len + varint id

// Segment-based AOF: the event log is split into a sequence of files
// `<base>.NNNNNN` where NNNNNN is a zero-padded segment index. Compaction
// drops the oldest segment with an O(1) unlink instead of rewriting every
// surviving record into a new file. This bounds per-pass disk I/O to one
// segment size rather than full-log size — a busy gateway used to spend
// ~9 MB/s rewriting the entire surviving log to drop ~1% of records.
//
// `SEGMENT_INDEX_DIGITS` is wide enough to represent every `u32` index
// (`u32::MAX = 4_294_967_295`, 10 digits). The scan filter requires the
// suffix to be exactly this many ASCII digits, so a narrower width would
// silently drop newly-created segments once the index passed the boundary
// — a long-running gateway would lose data. Pad to the full u32 width and
// the scanner stays consistent across the whole index range.
const SEGMENT_INDEX_DIGITS: usize = 10;

#[cfg(not(test))]
pub(super) const MAX_RECORDS_PER_SEGMENT: usize = 5_000;
#[cfg(test)]
pub(super) const MAX_RECORDS_PER_SEGMENT: usize = 500;

#[cfg(not(test))]
pub(super) const MAX_LOG_RECORDS: usize = 100_000;
#[cfg(test)]
pub(super) const MAX_LOG_RECORDS: usize = 10_000;

// `MAX_RECORDS_PER_SEGMENT` is sized to a meaningful fraction of
// `MAX_LOG_RECORDS` so steady-state retention is roughly bounded between
// `MAX_LOG_RECORDS - MAX_RECORDS_PER_SEGMENT` (right after a drop) and
// `MAX_LOG_RECORDS + MAX_RECORDS_PER_SEGMENT - 1` (right before the next
// drop). At the prod ratio (5_000 / 100_000) the on-disk window oscillates
// in a 5% band, which preserves enough router-training history to be useful
// without forcing a large segment scan on read.
const _: () = assert!(
    MAX_RECORDS_PER_SEGMENT * 5 <= MAX_LOG_RECORDS,
    "MAX_RECORDS_PER_SEGMENT must be at most 20% of MAX_LOG_RECORDS so the \
     post-drop record window stays within a sane band of the cap"
);
const _: () = assert!(
    MAX_RECORDS_PER_SEGMENT * 50 >= MAX_LOG_RECORDS,
    "MAX_RECORDS_PER_SEGMENT must be at least 2% of MAX_LOG_RECORDS, otherwise \
     segment count grows unboundedly large for the same retention budget"
);

// Reduced from 100 to 5 to ensure events are written more frequently,
// especially important for integration tests which generate few events.
const EVENT_REGISTER_BATCH_SIZE: usize = 5;
pub(super) const BATCH_SIZE: usize = EVENT_REGISTER_BATCH_SIZE;

// Segment rotation triggers exactly when the active segment fills, so keep
// the per-segment cap a multiple of the batch size to avoid a half-full
// final batch wedging the comparison.
const _: () = assert!(
    MAX_RECORDS_PER_SEGMENT % EVENT_REGISTER_BATCH_SIZE == 0,
    "MAX_RECORDS_PER_SEGMENT must be a multiple of EVENT_REGISTER_BATCH_SIZE"
);

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

#[derive(Debug, Clone)]
struct Segment {
    index: u32,
    record_count: usize,
}

pub(super) struct LogFile {
    file: Option<BufReader<File>>,
    base_path: PathBuf,
    /// All segments on disk, oldest at the front. The back is the active
    /// segment and is the only one that receives appends. Closed segments
    /// are immutable once they're behind a younger sibling.
    segments: VecDeque<Segment>,
    max_records_per_segment: usize,
    max_log_records: usize,
    pub(super) batch: Batch,
    num_writes: usize,
    num_recs: usize,
}

impl LogFile {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let base_path = path.as_ref().to_path_buf();
        if let Some(parent) = base_path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
        }
        Self::migrate_legacy(&base_path).await?;

        let indices = Self::scan_segment_indices(&base_path)?;
        let mut segments: VecDeque<Segment> = VecDeque::with_capacity(indices.len().max(1));
        let mut total = 0usize;
        for idx in indices {
            let count = Self::count_and_repair(&Self::segment_path(&base_path, idx)).await?;
            segments.push_back(Segment {
                index: idx,
                record_count: count,
            });
            total += count;
        }

        if segments.is_empty() {
            // Fresh install (or freshly-migrated empty stub). Create segment 0
            // as the active segment so subsequent appends have a target.
            let first = Self::segment_path(&base_path, 0);
            OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&first)
                .await?;
            segments.push_back(Segment {
                index: 0,
                record_count: 0,
            });
        }

        let active_index = segments.back().expect("non-empty").index;
        let active_path = Self::segment_path(&base_path, active_index);
        let active_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&active_path)
            .await?;
        Ok(Self {
            file: Some(BufReader::new(active_file)),
            base_path,
            segments,
            max_records_per_segment: MAX_RECORDS_PER_SEGMENT,
            max_log_records: MAX_LOG_RECORDS,
            batch: Batch {
                batch: Vec::with_capacity(BATCH_SIZE),
                num_writes: 0,
            },
            num_writes: 0,
            num_recs: total,
        })
    }

    pub(super) fn update_recs(&mut self, recs: usize) {
        self.num_recs += recs;
        if let Some(active) = self.segments.back_mut() {
            active.record_count += recs;
        }
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

    fn segment_path(base: &Path, index: u32) -> PathBuf {
        let parent = base
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = base
            .file_name()
            .expect("event log base path must have a file name");
        let mut buf = std::ffi::OsString::from(name);
        buf.push(format!(".{:0width$}", index, width = SEGMENT_INDEX_DIGITS));
        parent.join(buf)
    }

    fn scan_segment_indices(base: &Path) -> io::Result<Vec<u32>> {
        let parent = base
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let target_name = base
            .file_name()
            .expect("event log base path must have a file name")
            .to_string_lossy()
            .into_owned();
        let prefix = format!("{}.", target_name);
        let mut indices = Vec::new();
        let read_dir = match std::fs::read_dir(parent) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(indices),
            Err(e) => return Err(e),
        };
        for entry in read_dir {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str.strip_prefix(&prefix) {
                if rest.len() == SEGMENT_INDEX_DIGITS && rest.bytes().all(|b| b.is_ascii_digit()) {
                    if let Ok(idx) = rest.parse::<u32>() {
                        indices.push(idx);
                    }
                }
            }
        }
        indices.sort_unstable();
        Ok(indices)
    }

    /// If a legacy single-file event log exists at `base_path`, migrate it
    /// into segment 0 so the segment-aware open path can ingest it. Empty
    /// stubs (e.g. created by `config.rs` on first-time setup) are silently
    /// removed instead of taking up a segment slot.
    ///
    /// If `base_path` exists *and* one or more `<base>.NNNNNN` segments are
    /// already present we refuse with an error instead of guessing the
    /// chronological ordering. The new code never writes to `base_path`, so
    /// the only realistic way to land in this state is a partial migration
    /// from a prior crash, where the legacy data is older than the
    /// segments. Renaming the legacy file to a fresh index would surface
    /// it as the newest segment, corrupting downstream tooling that
    /// assumes filename order matches chronological order. Refusing gives
    /// the operator a chance to inspect and merge by hand.
    async fn migrate_legacy(base_path: &Path) -> io::Result<()> {
        let metadata = match tokio::fs::metadata(base_path).await {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if !metadata.is_file() {
            return Ok(());
        }
        if metadata.len() == 0 {
            tokio::fs::remove_file(base_path).await.ok();
            return Ok(());
        }
        let existing = Self::scan_segment_indices(base_path)?;
        if !existing.is_empty() {
            return Err(io::Error::other(format!(
                "legacy event log {} exists alongside segment files {:?}; refusing to \
                 guess chronological order — please archive or remove one before restart",
                base_path.display(),
                existing,
            )));
        }
        let target = Self::segment_path(base_path, 0);
        tokio::fs::rename(base_path, &target).await?;
        tracing::info!(
            from = %base_path.display(),
            to = %target.display(),
            "migrated legacy single-file event log to segment-based AOF"
        );
        Ok(())
    }

    /// Count complete records in a segment file. If a partial trailing
    /// record is detected (e.g. node crashed mid-write), truncate the file
    /// to the end of the last complete record so subsequent appends start
    /// from a clean offset.
    async fn count_and_repair(segment_path: &Path) -> io::Result<usize> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path)
            .await?;
        let total_len = file.metadata().await?.len();
        let mut reader = BufReader::new(file);
        let mut num_records: usize = 0;
        let mut last_good_offset: u64 = 0;
        let mut header = [0u8; EVENT_LOG_HEADER_SIZE];
        loop {
            let pos_before = reader.stream_position().await?;
            match reader.read_exact(&mut header).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
                Ok(_) => {}
            }
            let length = DefaultEndian::read_u32(&header[..4]) as u64;
            let record_end = pos_before + EVENT_LOG_HEADER_SIZE as u64 + length;
            if record_end > total_len {
                tracing::warn!(
                    file = %segment_path.display(),
                    last_good = last_good_offset,
                    "partial record detected at end of segment; truncating to last complete record"
                );
                break;
            }
            reader.seek(io::SeekFrom::Start(record_end)).await?;
            last_good_offset = record_end;
            num_records += 1;
        }
        if last_good_offset < total_len {
            let file = reader.into_inner();
            file.set_len(last_good_offset).await?;
            file.sync_all().await?;
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
            self.update_recs(self.num_writes);
            self.num_writes = 0;
        }

        // Rotate when the active segment is full. We trim the on-disk record
        // window inside `rotate_segment`, after the new segment is opened, so
        // the active file is always non-null when persist_log returns.
        let active_full = self
            .segments
            .back()
            .map(|s| s.record_count >= self.max_records_per_segment)
            .unwrap_or(false);
        if active_full {
            if let Err(err) = self.rotate_segment().await {
                tracing::error!("Failed rotating event log segment: {err}");
            }
        }
    }

    /// Close the active segment, open the next one, and drop the oldest
    /// segments while the on-disk record count exceeds the configured cap.
    /// Holding `FILE_LOCK` here serializes rotation against
    /// `read_all_events` (which takes the same lock), so reader paths never
    /// observe a half-rotated state. The lock
    /// is *not* held across the unlocked `update_recs` call in
    /// `persist_log` — readers inspect the filesystem rather than in-memory
    /// state, so the brief gap between the locked `write_all` and the
    /// locked `rotate_segment` is benign. The `NotFound` arms in the read
    /// paths are defensive (they cover externally-deleted segments, e.g.
    /// an operator pruning files) rather than a guard against a real
    /// rotation race.
    pub(super) async fn rotate_segment(&mut self) -> io::Result<()> {
        let _guard = FILE_LOCK.lock().await;

        // Compute the next index BEFORE closing the active file so an
        // overflow returns Err with `self.file` still wired up — otherwise
        // a subsequent `persist_log` would unwrap a `None` and crash.
        let next_index = match self.segments.back().map(|s| s.index.checked_add(1)) {
            Some(Some(next)) => next,
            Some(None) => {
                return Err(io::Error::other("event log segment index overflowed u32"));
            }
            None => 0,
        };

        if let Some(reader) = self.file.take() {
            let file = reader.into_inner();
            if let Err(e) = file.sync_all().await {
                tracing::warn!(error = %e, "failed to fsync active segment before rotate");
            }
            // Drop closes the file.
        }

        let next_path = Self::segment_path(&self.base_path, next_index);
        let new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&next_path)
            .await?;
        self.file = Some(BufReader::new(new_file));
        self.segments.push_back(Segment {
            index: next_index,
            record_count: 0,
        });

        // The drop loop preserves `segments.len() > 1` so we never unlink
        // the active segment — even if the cap is configured below
        // `max_records_per_segment`, the active segment survives.
        while self.num_recs > self.max_log_records && self.segments.len() > 1 {
            let dropped = self.segments.pop_front().expect("checked non-empty");
            let dropped_path = Self::segment_path(&self.base_path, dropped.index);
            match tokio::fs::remove_file(&dropped_path).await {
                Ok(_) => {
                    self.num_recs = self.num_recs.saturating_sub(dropped.record_count);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    self.num_recs = self.num_recs.saturating_sub(dropped.record_count);
                }
                Err(e) => {
                    // Re-add the segment to keep accounting consistent and
                    // surface the error to the caller.
                    self.segments.push_front(dropped);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Read all events from the segment-based AOF (for event aggregation).
    /// Reads any pre-migration legacy single-file log first (so offline
    /// tooling like `analyze_event_logs` keeps working against an
    /// untouched legacy log) followed by every segment in age order.
    pub async fn read_all_events(event_log_path: &Path) -> anyhow::Result<Vec<NetLogMessage>> {
        let _guard = FILE_LOCK.lock().await;
        let new_records_ts = NEW_RECORDS_TS
            .get()
            .map(|ts| {
                ts.duration_since(std::time::UNIX_EPOCH)
                    .expect("should be older than unix epoch")
                    .as_secs() as i64
            })
            .unwrap_or(0);

        let indices = Self::scan_segment_indices(event_log_path)?;
        let mut buffers: Vec<Vec<u8>> = Vec::new();

        // Read the pre-migration legacy log first if present. Once
        // `LogFile::open` runs against the path the legacy file is
        // renamed to segment 0, but standalone consumers of this API
        // (offline aggregator, ad-hoc analysis scripts) may invoke it
        // before any writer has had a chance to migrate.
        if indices.is_empty() {
            if let Ok(file) = OpenOptions::new().read(true).open(event_log_path).await {
                let mut reader = BufReader::new(file);
                Self::read_record_buffers(&mut reader, &mut buffers, None).await?;
            }
        }

        for idx in indices {
            let path = Self::segment_path(event_log_path, idx);
            match OpenOptions::new().read(true).open(&path).await {
                Ok(file) => {
                    let mut reader = BufReader::new(file);
                    Self::read_record_buffers(&mut reader, &mut buffers, None).await?;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    // Segment was unlinked between the directory scan and
                    // open (operator pruning, external tool). Defensive —
                    // FILE_LOCK already serializes our own rotation.
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        if buffers.is_empty() {
            return Ok(vec![]);
        }

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

    /// Read raw record payloads from a segment file. The optional `limit` is
    /// the maximum number of records the function will append to `out`
    /// before returning.
    async fn read_record_buffers(
        file: &mut (impl AsyncRead + AsyncSeek + Unpin),
        out: &mut Vec<Vec<u8>>,
        limit: Option<usize>,
    ) -> anyhow::Result<()> {
        loop {
            if let Some(l) = limit {
                if out.len() >= l {
                    return Ok(());
                }
            }
            let mut header = [0u8; EVENT_LOG_HEADER_SIZE];
            match file.read_exact(&mut header).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => {
                    let pos = file.stream_position().await;
                    tracing::error!(%e, ?pos, "error while trying to read file");
                    return Err(e.into());
                }
                Ok(_) => {}
            }
            let length = DefaultEndian::read_u32(&header[..4]);
            let mut buf = vec![0u8; length as usize];
            file.read_exact(&mut buf).await?;
            out.push(buf);
        }
    }

    /// Append an encoded batch to the active segment.
    ///
    /// Deliberately does NOT `fsync`. The event log is a local diagnostic
    /// record, not authoritative state: nothing in the node reads it back to
    /// make decisions, and losing the last few records to a power cut costs
    /// nothing. A per-batch `sync_all()` (data *and* metadata) used to run here,
    /// and because an append also changes the file size it forced a full
    /// filesystem journal commit every `BATCH_SIZE` (5) events. Measured on a
    /// live 0.2.111 peer that was 95% of ALL fsyncs the process issued and the
    /// dominant source of a 4-6x write amplification, turning ~61 MiB/hour of
    /// logical appends into hundreds of MiB/hour of real disk writes.
    ///
    /// Durability that actually matters is preserved elsewhere:
    /// [`Self::rotate_segment`] still fsyncs the active segment before closing
    /// it, so a completed segment is on disk before the next one opens, and
    /// [`Self::load_segment_records`] already truncates a torn tail at the last
    /// good record offset on startup. Readers are unaffected either way —
    /// `read_all_events` sees written bytes through the page cache immediately;
    /// fsync is about surviving power loss, not visibility.
    ///
    /// It DOES still `flush`. That is not a durability barrier: `tokio::fs::File`
    /// keeps an internal buffer and hands writes to a background pool, so
    /// without an explicit flush the bytes may not reach the OS at all and a
    /// reader (or `std::fs::metadata`) can observe a zero-length segment. The
    /// flush issues the `write(2)`, putting the data in the page cache where
    /// readers see it immediately; the kernel then writes it back on its own
    /// schedule instead of one journal commit per five events.
    ///
    /// do NOT re-add a per-batch fsync here — see #4968.
    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let _guard = FILE_LOCK.lock().await;
        let file = self.file.as_mut().unwrap();
        if let Err(err) = file.get_mut().write_all(data).await {
            tracing::error!("Failed writting to event log: {err}");
            return Err(err);
        }

        if let Err(err) = file.get_mut().flush().await {
            tracing::error!("Failed flushing event log: {err}");
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

    /// Unlink every `<base>.NNNNNNNNNN` segment beside `base`, reporting what
    /// was freed. The base file itself is never touched — only the segments
    /// matching the exact grammar [`Self::scan_segment_indices`] accepts, so a
    /// neighbouring `_EVENT_LOG.backup`, `_EVENT_LOG.0000000001.tmp` or
    /// `_EVENT_LOG_LOCAL.0000000000` survives.
    ///
    /// Mechanism only: it deletes whatever it is pointed at. The decision of
    /// *whether* a sweep is allowed lives in
    /// [`reclaim_orphaned_event_log`], which is the only production caller.
    ///
    /// Nothing here is propagated as an error: this runs on the startup path,
    /// and neither a segment we cannot remove (bad permissions, a stray
    /// directory occupying the name, a Windows share lock) nor an unreadable
    /// directory may stop the node from booting. Both are counted in
    /// `failures` and logged, so a non-zero count means bytes may remain
    /// rather than the problem being swallowed. `NotFound` on the unlink is
    /// not a failure — the file being gone is the outcome we wanted.
    fn reclaim_segments(base: &Path) -> EventLogReclaim {
        let mut outcome = EventLogReclaim::default();
        let indices = match Self::scan_segment_indices(base) {
            Ok(indices) => indices,
            Err(e) => {
                tracing::warn!(
                    base = %base.display(),
                    error = %e,
                    "could not scan for orphaned event-log segments; skipping reclaim"
                );
                outcome.failures += 1;
                return outcome;
            }
        };
        for idx in indices {
            let path = Self::segment_path(base, idx);
            // Size before unlink: after a successful remove there is nothing
            // left to stat. A stat failure only costs byte accuracy, so it
            // must not skip the removal.
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    outcome.files_removed += 1;
                    outcome.bytes_reclaimed += size;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    outcome.failures += 1;
                    tracing::warn!(
                        segment = %path.display(),
                        error = %e,
                        "failed to remove orphaned event-log segment; leaving it in place"
                    );
                }
            }
        }
        outcome
    }
}

/// What a single [`reclaim_orphaned_event_log`] sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EventLogReclaim {
    /// Segment files successfully unlinked.
    pub files_removed: usize,
    /// Bytes freed, summed from each removed segment's size before unlink.
    pub bytes_reclaimed: u64,
    /// Segments we tried and failed to unlink, plus one if the directory scan
    /// itself failed (in which case nothing was attempted). Startup continues
    /// regardless; counted rather than swallowed so a non-zero value says
    /// bytes may still be sitting on disk.
    pub failures: usize,
}

impl EventLogReclaim {
    fn is_empty(&self) -> bool {
        self.files_removed == 0 && self.failures == 0
    }
}

/// Delete `_EVENT_LOG` segments left behind by a node that used to write the
/// diagnostic event log, when this node is not writing it (#5035).
///
/// #4968 flipped the log OFF by default in network mode, but nothing removed
/// what earlier releases had already written: measured 2026-07-29, 224 MB /
/// 23 files on a user peer (17% of its data dir) and 412 MB / 24 files on the
/// nova gateway, last written the day 0.2.112 deployed. Nothing reads the log
/// back — it is a purely local forensic record, and `fdev verify-state`
/// consumes the Local-mode `_EVENT_LOG_LOCAL` instead — so once it is
/// switched off the segments have no consumer at all.
///
/// Returns `None` when no sweep is permitted, `Some(outcome)` when one ran.
/// The two guards:
///
/// - **The log must be disabled.** An operator who deliberately turned it on
///   (`--enable-event-log`) keeps every segment; this is the guard that must
///   never regress, because getting it wrong destroys forensic history that
///   was asked for.
/// - **Network mode only.** In Local mode `Config::event_log()` resolves to
///   `_EVENT_LOG_LOCAL`, which `fdev verify-state` reads. Local mode defaults
///   the log ON so the first guard usually covers it, but an explicit
///   `--enable-event-log false` on a local node would otherwise sweep the very
///   segments that tooling consumes.
///
/// One-shot in practice: after the first disabled startup there is nothing
/// left to remove, and the node writes no new segments while the log is off.
pub(crate) fn reclaim_orphaned_event_log(
    config: &crate::config::Config,
) -> Option<EventLogReclaim> {
    if config.event_log_enabled() {
        return None;
    }
    if !matches!(config.mode, crate::contract::OperationMode::Network) {
        return None;
    }

    let base = config.event_log();
    let outcome = LogFile::reclaim_segments(&base);

    // Silent when there was nothing to do, which is every startup after the
    // first. `info!` and not `debug!`: `debug!` compiles out in release, and
    // the reclaim is exactly the kind of one-off disk change an operator needs
    // to see in a release build's log.
    if !outcome.is_empty() {
        tracing::info!(
            files_removed = outcome.files_removed,
            bytes_reclaimed = outcome.bytes_reclaimed,
            failures = outcome.failures,
            base = %base.display(),
            "reclaimed orphaned event-log segments (the diagnostic event log is \
             disabled on this node, so nothing reads them)"
        );
    }
    Some(outcome)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::{
        dev_tool::{PeerId, Transaction},
        tracing::{EventKind, NetEventLog},
    };

    use super::*;

    fn ensure_records_ts() {
        NEW_RECORDS_TS.get_or_init(SystemTime::now);
    }

    // ----------------------------------------------------------------------
    // #5035: reclaiming orphaned `_EVENT_LOG` segments once the log is off.
    // ----------------------------------------------------------------------

    /// Write `len` bytes to `path`, returning `len` so callers can accumulate
    /// the exact byte total the reclaim is expected to report.
    fn write_file(path: &Path, len: usize) -> u64 {
        std::fs::write(path, vec![b'x'; len]).expect("write fixture file");
        len as u64
    }

    fn exists(dir: &Path, name: &str) -> bool {
        dir.join(name).exists()
    }

    /// Seed a data dir with three `_EVENT_LOG` segments plus a set of
    /// near-miss names that share the prefix but not the grammar, and the
    /// zero-length `_EVENT_LOG` base stub `ConfigPathsArgs::build` writes.
    /// Returns the byte total of the three real segments.
    fn seed_segments_and_decoys(dir: &Path) -> u64 {
        let mut expected = 0;
        expected += write_file(&dir.join("_EVENT_LOG.0000000000"), 1_000);
        expected += write_file(&dir.join("_EVENT_LOG.0000000001"), 2_000);
        expected += write_file(&dir.join("_EVENT_LOG.0000004294"), 4_000);

        // Base stub: written unconditionally at config build, and the file the
        // active log would append to. Never a reclaim target.
        write_file(&dir.join("_EVENT_LOG"), 0);
        // `fdev verify-state` reads these; the prefix matches but the grammar
        // does not (`_EVENT_LOG` is followed by `_`, not `.`).
        write_file(&dir.join("_EVENT_LOG_LOCAL"), 500);
        write_file(&dir.join("_EVENT_LOG_LOCAL.0000000000"), 500);
        // Operator/tooling files that merely start with the same prefix.
        write_file(&dir.join("_EVENT_LOG.backup"), 500);
        write_file(&dir.join("_EVENT_LOG.0000000001.tmp"), 500);
        write_file(&dir.join("_EVENT_LOG_notes.txt"), 500);
        // Right length, wrong alphabet.
        write_file(&dir.join("_EVENT_LOG.00000000ab"), 500);
        // Nine digits and eleven digits: off-by-one on either side.
        write_file(&dir.join("_EVENT_LOG.000000001"), 500);
        write_file(&dir.join("_EVENT_LOG.00000000012"), 500);
        expected
    }

    fn assert_decoys_survive(dir: &Path) {
        for name in [
            "_EVENT_LOG",
            "_EVENT_LOG_LOCAL",
            "_EVENT_LOG_LOCAL.0000000000",
            "_EVENT_LOG.backup",
            "_EVENT_LOG.0000000001.tmp",
            "_EVENT_LOG_notes.txt",
            "_EVENT_LOG.00000000ab",
            "_EVENT_LOG.000000001",
            "_EVENT_LOG.00000000012",
        ] {
            assert!(
                exists(dir, name),
                "{name} does not match the `<base>.<10 digits>` segment grammar and \
                 must survive the reclaim (#5035)"
            );
        }
    }

    /// The mechanism removes exactly the files matching the segment grammar,
    /// and reports the byte total it actually removed.
    #[test]
    fn reclaim_segments_removes_only_exact_segment_names() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();
        let expected_bytes = seed_segments_and_decoys(dir);

        let outcome = LogFile::reclaim_segments(&dir.join("_EVENT_LOG"));

        assert_eq!(
            outcome,
            EventLogReclaim {
                files_removed: 3,
                bytes_reclaimed: expected_bytes,
                failures: 0,
            },
            "the reported outcome must match the three seeded segments exactly"
        );
        for name in [
            "_EVENT_LOG.0000000000",
            "_EVENT_LOG.0000000001",
            "_EVENT_LOG.0000004294",
        ] {
            assert!(!exists(dir, name), "{name} should have been reclaimed");
        }
        assert_decoys_survive(dir);
    }

    /// A segment we cannot unlink is counted, logged, and does not stop the
    /// sweep or panic. A directory occupying a segment name makes
    /// `remove_file` fail deterministically (`EISDIR`) on every platform and
    /// regardless of whether the test runs as root, unlike a chmod.
    #[test]
    fn reclaim_segments_counts_failures_without_panicking() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();
        let removable = write_file(&dir.join("_EVENT_LOG.0000000000"), 1_000)
            + write_file(&dir.join("_EVENT_LOG.0000000002"), 3_000);
        std::fs::create_dir(dir.join("_EVENT_LOG.0000000001")).unwrap();

        let outcome = LogFile::reclaim_segments(&dir.join("_EVENT_LOG"));

        assert_eq!(
            outcome,
            EventLogReclaim {
                files_removed: 2,
                bytes_reclaimed: removable,
                failures: 1,
            },
            "the unremovable segment must be counted as a failure while the other \
             two are still reclaimed, and its bytes must not be reported as freed"
        );
        assert!(
            exists(dir, "_EVENT_LOG.0000000001"),
            "the unremovable entry must still be there"
        );
    }

    /// Nothing on disk, nothing reported — and no error out of a data dir that
    /// does not exist yet.
    #[test]
    fn reclaim_segments_reports_nothing_when_there_is_nothing_to_do() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert_eq!(
            LogFile::reclaim_segments(&temp_dir.path().join("_EVENT_LOG")),
            EventLogReclaim::default()
        );
        assert_eq!(
            LogFile::reclaim_segments(&temp_dir.path().join("no-such-dir/_EVENT_LOG")),
            EventLogReclaim::default(),
            "a missing directory is not a failure"
        );
    }

    async fn build_config(
        dir: &Path,
        mode: crate::contract::OperationMode,
    ) -> crate::config::Config {
        crate::config::event_log_test_args(dir, mode)
            .build()
            .await
            .expect("build Config")
    }

    /// THE guard: an operator who deliberately enabled the event log keeps
    /// every segment. Losing forensic history that was explicitly asked for is
    /// the one failure mode this change must never have.
    #[tokio::test]
    async fn reclaim_refuses_while_the_event_log_is_enabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();
        let mut args =
            crate::config::event_log_test_args(dir, crate::contract::OperationMode::Network);
        args.enable_event_log = Some(true);
        let config = args.build().await.expect("build Config");
        // `ConfigPathsArgs::build` (inside `build()`) writes the `_EVENT_LOG`
        // stub, so seed the segments after it, not before.
        seed_segments_and_decoys(dir);
        assert!(config.event_log_enabled(), "precondition: log is ON");

        let decision = reclaim_orphaned_event_log(&config);

        assert_eq!(
            decision, None,
            "with the log enabled the reclaim must decline to run at all"
        );
        for name in [
            "_EVENT_LOG.0000000000",
            "_EVENT_LOG.0000000001",
            "_EVENT_LOG.0000004294",
        ] {
            assert!(
                exists(dir, name),
                "{name} must survive: this node is still writing its event log"
            );
        }
        assert_decoys_survive(dir);
    }

    /// The case the issue is about: a network node on the post-#4968 default
    /// sweeps the segments an earlier release left behind.
    #[tokio::test]
    async fn reclaim_sweeps_segments_when_the_log_is_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();
        let config = build_config(dir, crate::contract::OperationMode::Network).await;
        let expected_bytes = seed_segments_and_decoys(dir);
        assert!(
            !config.event_log_enabled(),
            "precondition: network mode defaults the log OFF (#4968)"
        );

        let decision = reclaim_orphaned_event_log(&config);

        assert_eq!(
            decision,
            Some(EventLogReclaim {
                files_removed: 3,
                bytes_reclaimed: expected_bytes,
                failures: 0,
            }),
            "a disabled network node must sweep, and report what it freed"
        );
        assert_decoys_survive(dir);

        // Idempotent: the second startup finds nothing and reports nothing.
        assert_eq!(
            reclaim_orphaned_event_log(&config),
            Some(EventLogReclaim::default()),
            "the sweep is one-shot; a later startup has nothing left to remove"
        );
    }

    /// Local mode is where `fdev verify-state` reads `_EVENT_LOG_LOCAL`.
    /// Disabling the log there must still not sweep those segments.
    #[tokio::test]
    async fn reclaim_never_touches_the_local_mode_event_log() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path();
        let mut args =
            crate::config::event_log_test_args(dir, crate::contract::OperationMode::Local);
        args.enable_event_log = Some(false);
        let config = args.build().await.expect("build Config");
        assert!(
            !config.event_log_enabled(),
            "precondition: an explicit false disables the log even in local mode"
        );
        assert!(
            config.event_log().ends_with("_EVENT_LOG_LOCAL"),
            "precondition: local mode resolves to the _LOCAL base name"
        );
        let bytes = write_file(&dir.join("_EVENT_LOG_LOCAL.0000000000"), 1_000)
            + write_file(&dir.join("_EVENT_LOG_LOCAL.0000000001"), 1_000);
        assert!(bytes > 0);

        let decision = reclaim_orphaned_event_log(&config);

        assert_eq!(
            decision, None,
            "local mode must decline: fdev verify-state consumes _EVENT_LOG_LOCAL"
        );
        assert!(exists(dir, "_EVENT_LOG_LOCAL.0000000000"));
        assert!(exists(dir, "_EVENT_LOG_LOCAL.0000000001"));
    }

    /// Source pin (#4968): `write_all` must NOT fsync on every batch.
    ///
    /// A `sync_all()` used to run after each `BATCH_SIZE` (5) events. Because
    /// an append also changes the file size, that forced a full filesystem
    /// journal commit per batch: measured on a live 0.2.111 peer it was 95% of
    /// every fsync the process issued and the dominant source of a 4-6x write
    /// amplification. Durability that matters is preserved by the fsync in
    /// `rotate_segment` (a completed segment is on disk before the next opens)
    /// and by the torn-tail truncation on load, both of which this pin leaves
    /// free to keep syncing.
    ///
    /// A behavioral test can't catch a re-added fsync (it changes only I/O
    /// scheduling, not observable output), so the pin reads the source.
    #[test]
    fn write_all_does_not_fsync_per_batch() {
        let src = include_str!("aof.rs");
        // Cut at `mod tests`, NOT at the first `#[cfg(test)]` — there are
        // `#[cfg(test)]` constants near the top of this file, so cutting there
        // would drop nearly all production code and make this pin vacuous.
        let prod = src.split("mod tests").next().unwrap();

        // Built with `concat!` so this needle cannot match its own source text.
        let sig = concat!("pub async fn ", "write_all");
        let start = prod
            .find(sig)
            .expect("pin guard: write_all not found in the production slice");
        let after = &prod[start..];
        // The next same-level item is `pub fn encode_batch`.
        let end = after.find("\n    pub fn ").unwrap_or(after.len());
        let body = &after[..end];

        // Vacuity guard: prove we actually extracted the real function body
        // rather than an empty or mis-sliced range, which would make the
        // assertion below pass no matter what the code does.
        assert!(
            body.contains("write_all(data)"),
            "pin guard: the extracted slice does not contain the append call, so \
             the extraction is wrong and this pin proves nothing. Slice:\n{body}"
        );

        assert!(
            !body.contains(concat!("sync", "_all")),
            "write_all must NOT fsync per batch (#4968). Durability is already \
             covered by rotate_segment's fsync and the torn-tail truncation on \
             load; a per-batch fsync costs ~5x write amplification on every user \
             peer for a purely diagnostic log. Offending body:\n{body}"
        );
    }

    /// Count the `Route` records the log surfaces through its public reader.
    ///
    /// These tests exercise the AOF write / rotate / legacy-migrate machinery
    /// and use the route-record count purely as the read-back assertion. They
    /// used to call `LogFile::get_router_events`, which was deleted as dead code
    /// in the #4808 follow-up (nothing consumed `EventKind::Route` from the log
    /// any more); `read_all_events` is the surviving reader and covers the same
    /// segments, so the coverage is unchanged.
    async fn count_route_records(log_path: &Path) -> anyhow::Result<usize> {
        let recs = LogFile::read_all_events(log_path).await?;
        Ok(recs
            .iter()
            .filter(|m| matches!(m.kind, EventKind::Route(_)))
            .count())
    }

    fn build_events(
        unstructured: &mut arbitrary::Unstructured<'_>,
        count: usize,
    ) -> arbitrary::Result<(Vec<Transaction>, Vec<PeerId>, Vec<EventKind>)> {
        let mut transactions = Vec::with_capacity(count);
        let mut peers = Vec::with_capacity(count);
        let mut kinds = Vec::with_capacity(count);
        for _ in 0..count {
            transactions.push(unstructured.arbitrary()?);
            peers.push(PeerId::random());
            kinds.push(unstructured.arbitrary()?);
        }
        Ok((transactions, peers, kinds))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        const TEST_LOGS: usize = MAX_LOG_RECORDS;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, TEST_LOGS)?;
        let mut total_route_events: usize = 0;
        let mut events = Vec::with_capacity(TEST_LOGS);
        for i in 0..TEST_LOGS {
            if matches!(kinds[i], EventKind::Route(_)) {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind: kinds[i].clone(),
            });
        }

        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        let route_records = count_route_records(&log_path).await?;
        assert_eq!(route_records, total_route_events);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write_small() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        const TEST_LOGS: usize = 100;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, TEST_LOGS)?;
        let mut total_route_events: usize = 0;
        let mut events = Vec::with_capacity(TEST_LOGS);
        for i in 0..TEST_LOGS {
            if matches!(kinds[i], EventKind::Route(_)) {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind: kinds[i].clone(),
            });
        }

        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        let route_records = count_route_records(&log_path).await?;
        assert_eq!(route_records, total_route_events);
        Ok(())
    }

    /// Push enough records to overflow the cap and verify only the oldest
    /// segment is dropped (record-count band of MAX_RECORDS_PER_SEGMENT
    /// rather than a full-log rewrite).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_write_truncate() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Push exactly one segment beyond the cap so the first segment
        // (records 0..MAX_RECORDS_PER_SEGMENT) gets dropped and the
        // surviving record count lands at MAX_LOG_RECORDS.
        const TEST_LOGS: usize = MAX_LOG_RECORDS + MAX_RECORDS_PER_SEGMENT;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, TEST_LOGS)?;
        let mut total_route_events: usize = 0;
        let mut events = Vec::with_capacity(TEST_LOGS);
        for i in 0..TEST_LOGS {
            if matches!(kinds[i], EventKind::Route(_)) && i >= MAX_RECORDS_PER_SEGMENT {
                total_route_events += 1;
            }
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind: kinds[i].clone(),
            });
        }

        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        let route_records = count_route_records(&log_path).await?;
        assert_eq!(route_records, total_route_events);
        Ok(())
    }

    /// Steady-state regression: push enough records for many segment
    /// rotations and confirm the on-disk record count stays within
    /// `[MAX_LOG_RECORDS - MAX_RECORDS_PER_SEGMENT, MAX_LOG_RECORDS + MAX_RECORDS_PER_SEGMENT]`.
    /// Without segment-based AOF the gateway used to spend ~9 MB/s rewriting
    /// the surviving log on every batch flush; the segment design caps
    /// per-pass disk I/O at one segment file unlink.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sustained_load_keeps_log_bounded() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        const TEST_LOGS: usize = 3 * MAX_LOG_RECORDS;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();

        for i in 0..TEST_LOGS {
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

        let upper_bound = MAX_LOG_RECORDS + MAX_RECORDS_PER_SEGMENT;
        let lower_bound = MAX_LOG_RECORDS.saturating_sub(MAX_RECORDS_PER_SEGMENT);
        assert!(
            log.num_recs >= lower_bound && log.num_recs <= upper_bound,
            "num_recs={} outside steady-state band [{lower_bound}, {upper_bound}] \
             after pushing {TEST_LOGS} records",
            log.num_recs,
        );
        // Total segment count should also stay bounded — at most
        // ceil(MAX_LOG_RECORDS / MAX_RECORDS_PER_SEGMENT) full segments
        // plus one active partial segment.
        let max_segments = MAX_LOG_RECORDS.div_ceil(MAX_RECORDS_PER_SEGMENT) + 1;
        assert!(
            log.segments.len() <= max_segments,
            "segment count {} exceeds bound {max_segments}",
            log.segments.len(),
        );
        Ok(())
    }

    /// Verify that the active segment fills, rotates, and drops the oldest
    /// segment file once the on-disk record count exceeds the cap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn segment_rotation_drops_oldest() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Push exactly enough records to fill MAX_LOG_RECORDS plus one more
        // segment so the first segment is dropped on rotation.
        const TEST_LOGS: usize = MAX_LOG_RECORDS + MAX_RECORDS_PER_SEGMENT;

        let mut log = LogFile::open(&log_path).await?;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, TEST_LOGS)?;
        let mut events = Vec::with_capacity(TEST_LOGS);
        for i in 0..TEST_LOGS {
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind: kinds[i].clone(),
            });
        }
        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }

        // The very first segment file (index 0) must be gone once the cap
        // is exceeded.
        let dropped = LogFile::segment_path(&log_path, 0);
        assert!(
            !dropped.exists(),
            "expected oldest segment {} to be unlinked after overflow",
            dropped.display(),
        );

        // We should have at least two surviving segment files.
        let indices = LogFile::scan_segment_indices(&log_path)?;
        assert!(
            indices.len() >= 2,
            "expected multiple surviving segments, got {indices:?}",
        );
        Ok(())
    }

    /// Closing and reopening the log must reconstruct `num_recs` from the
    /// segments on disk. Reads after reopen must match the records that
    /// survived rotation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_reopens_segments() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        const TEST_LOGS: usize = MAX_RECORDS_PER_SEGMENT * 3 + BATCH_SIZE;

        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, TEST_LOGS)?;

        // Phase 1 — open, write, drop.
        let recs_before;
        let segs_before;
        {
            let mut log = LogFile::open(&log_path).await?;
            let mut events = Vec::with_capacity(TEST_LOGS);
            for i in 0..TEST_LOGS {
                events.push(NetEventLog {
                    tx: &transactions[i],
                    peer_id: peers[i].clone(),
                    kind: kinds[i].clone(),
                });
            }
            for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
                log.persist_log(msg).await;
            }
            recs_before = log.num_recs;
            segs_before = log.segments.len();
        }

        // Phase 2 — reopen and verify recovered state matches.
        let log = LogFile::open(&log_path).await?;
        assert_eq!(
            log.num_recs, recs_before,
            "num_recs after reopen ({}) != before drop ({recs_before})",
            log.num_recs
        );
        assert_eq!(
            log.segments.len(),
            segs_before,
            "segment count after reopen != before drop",
        );
        Ok(())
    }

    /// Simulate a crash mid-segment-write by appending an oversized record
    /// header followed by missing payload. The next open must detect the
    /// partial record, truncate the segment, and surface the correct count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partial_record_repair_on_open() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Write a few clean batches first so segment 0 exists.
        const CLEAN_LOGS: usize = BATCH_SIZE * 3;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, CLEAN_LOGS)?;
        {
            let mut log = LogFile::open(&log_path).await?;
            let mut events = Vec::with_capacity(CLEAN_LOGS);
            for i in 0..CLEAN_LOGS {
                events.push(NetEventLog {
                    tx: &transactions[i],
                    peer_id: peers[i].clone(),
                    kind: kinds[i].clone(),
                });
            }
            for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
                log.persist_log(msg).await;
            }
        }

        // Append a torn record: a header claiming a 256-byte payload but no
        // payload bytes follow. Mimics a power-loss after the header was
        // flushed but before the body finished.
        let active = LogFile::segment_path(&log_path, 0);
        let pre_size = std::fs::metadata(&active)?.len();
        let mut torn = vec![0u8; EVENT_LOG_HEADER_SIZE];
        DefaultEndian::write_u32(&mut torn[..4], 256);
        torn[4] = 0;
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new().append(true).open(&active)?;
            f.write_all(&torn)?;
            f.sync_all()?;
        }
        let post_size = std::fs::metadata(&active)?.len();
        assert!(post_size > pre_size, "torn header should grow the file");

        // Reopen — count_and_repair should truncate the partial record.
        let log = LogFile::open(&log_path).await?;
        assert_eq!(log.num_recs, CLEAN_LOGS);
        let repaired_size = std::fs::metadata(&active)?.len();
        assert_eq!(
            repaired_size, pre_size,
            "active segment should be truncated back to last complete record",
        );
        Ok(())
    }

    /// A pre-existing single-file legacy event log must be migrated into
    /// `<base>.000000` on the next open, and its records must remain
    /// readable through the public API.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_single_file_migration() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Build a payload using the same batch encoding the writer uses, so
        // the migrated file matches the on-disk record format byte-for-byte.
        const LEGACY_LOGS: usize = BATCH_SIZE * 4;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, LEGACY_LOGS)?;
        let mut events = Vec::with_capacity(LEGACY_LOGS);
        for i in 0..LEGACY_LOGS {
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind: kinds[i].clone(),
            });
        }
        let log_messages: Vec<_> =
            NetLogMessage::to_log_message(either::Either::Right(events)).collect();
        let mut payload = Vec::new();
        for msg in &log_messages {
            let (header, mut serialized) = LogFile::encode_log(msg).expect("encode");
            payload.extend_from_slice(&header);
            payload.append(&mut serialized);
        }
        std::fs::write(&log_path, &payload)?;

        // Open — migration should rename the legacy file to segment 0.
        let log = LogFile::open(&log_path).await?;
        assert_eq!(log.num_recs, LEGACY_LOGS);
        assert!(
            !log_path.exists(),
            "legacy single-file log should have been renamed during migration",
        );
        let segment_zero = LogFile::segment_path(&log_path, 0);
        assert!(
            segment_zero.exists(),
            "segment 0 should exist after migration",
        );

        // Reads through the public API must surface the migrated records.
        let route_count = log_messages
            .iter()
            .filter(|m| matches!(m.kind, EventKind::Route(_)))
            .count();
        let route_records = count_route_records(&log_path).await?;
        assert_eq!(route_records, route_count);
        Ok(())
    }

    /// An empty stub at the legacy path (as created by `config.rs` at
    /// first-time setup) must be silently removed instead of dragged into
    /// segment 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn empty_legacy_stub_is_removed() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");
        std::fs::write(&log_path, [])?;

        let log = LogFile::open(&log_path).await?;
        assert_eq!(log.num_recs, 0);
        assert!(
            !log_path.exists(),
            "empty stub should have been unlinked during migration",
        );
        let segment_zero = LogFile::segment_path(&log_path, 0);
        assert!(
            segment_zero.exists(),
            "fresh segment 0 should be created after stub removal",
        );
        Ok(())
    }

    /// `read_all_events` must concatenate records across segments in age
    /// order — the offline aggregator and `state_verifier` both rely on
    /// this. Push enough records to span multiple segments without
    /// triggering a drop, then assert the count and ordering survive a
    /// round-trip.
    ///
    /// `EventKind`'s arbitrary derive can occasionally generate values
    /// that don't bincode-roundtrip cleanly (a pre-existing quirk that
    /// also affects existing route-only tests, but those only count
    /// `EventKind::Route` so non-route deserialization failures are
    /// invisible). We pre-validate each generated message here so the
    /// test asserts a precise count against what's actually round-trippable
    /// without papering over real read-path bugs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_all_events_spans_segments() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Two and a half segments' worth, well below MAX_LOG_RECORDS so no
        // drops happen and we can assert the exact event count.
        const TEST_LOGS: usize = MAX_RECORDS_PER_SEGMENT * 2 + BATCH_SIZE * 10;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let mut transactions = Vec::with_capacity(TEST_LOGS);
        let mut peers = Vec::with_capacity(TEST_LOGS);
        for _ in 0..TEST_LOGS {
            transactions.push(unstructured.arbitrary::<Transaction>()?);
            peers.push(PeerId::random());
        }
        let mut events = Vec::with_capacity(TEST_LOGS);
        for i in 0..TEST_LOGS {
            let kind: EventKind = unstructured.arbitrary()?;
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }
        let messages: Vec<NetLogMessage> =
            NetLogMessage::to_log_message(either::Either::Right(events)).collect();
        let roundtrippable: Vec<NetLogMessage> = messages
            .iter()
            .filter(|m| {
                bincode::serialize(m)
                    .ok()
                    .and_then(|bytes| bincode::deserialize::<NetLogMessage>(&bytes).ok())
                    .is_some()
            })
            .cloned()
            .collect();
        let expected = roundtrippable.len();
        assert!(
            expected >= TEST_LOGS - 100,
            "lost too many records to bincode roundtrip ({}/{TEST_LOGS}) — \
             pre-existing arbitrary-EventKind quirk has gotten worse",
            expected
        );

        {
            let mut log = LogFile::open(&log_path).await?;
            for msg in messages {
                log.persist_log(msg).await;
            }
        }

        let read = LogFile::read_all_events(&log_path).await?;
        assert_eq!(read.len(), expected);
        // Ordering: records must come out in the same order they were
        // appended, segment by segment.
        for (i, msg) in read.iter().enumerate() {
            assert_eq!(
                msg.tx, roundtrippable[i].tx,
                "record {i} surfaced out of segment-append order",
            );
        }
        Ok(())
    }

    /// `read_all_events` against an untouched legacy single-file log
    /// (no `LogFile::open` has migrated it yet, e.g. an offline
    /// aggregator running before any node restart) must still surface
    /// every record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_all_events_reads_legacy_without_migration() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        const LEGACY_LOGS: usize = BATCH_SIZE * 4;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let mut transactions = Vec::with_capacity(LEGACY_LOGS);
        let mut peers = Vec::with_capacity(LEGACY_LOGS);
        for _ in 0..LEGACY_LOGS {
            transactions.push(unstructured.arbitrary::<Transaction>()?);
            peers.push(PeerId::random());
        }
        let mut events = Vec::with_capacity(LEGACY_LOGS);
        for i in 0..LEGACY_LOGS {
            let kind: EventKind = unstructured.arbitrary()?;
            events.push(NetEventLog {
                tx: &transactions[i],
                peer_id: peers[i].clone(),
                kind,
            });
        }
        let log_messages: Vec<_> =
            NetLogMessage::to_log_message(either::Either::Right(events)).collect();
        let roundtrippable_count = log_messages
            .iter()
            .filter(|m| {
                bincode::serialize(m)
                    .ok()
                    .and_then(|b| bincode::deserialize::<NetLogMessage>(&b).ok())
                    .is_some()
            })
            .count();
        let mut payload = Vec::new();
        for msg in &log_messages {
            let (header, mut serialized) = LogFile::encode_log(msg).expect("encode");
            payload.extend_from_slice(&header);
            payload.append(&mut serialized);
        }
        std::fs::write(&log_path, &payload)?;
        // Deliberately do NOT call LogFile::open — exercise the
        // standalone-reader path.

        let read = LogFile::read_all_events(&log_path).await?;
        assert_eq!(read.len(), roundtrippable_count);
        Ok(())
    }

    /// A torn record may stop in the middle of the header, not just after
    /// it. The repair pass must handle both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partial_header_is_truncated_on_open() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        const CLEAN_LOGS: usize = BATCH_SIZE * 2;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, CLEAN_LOGS)?;
        {
            let mut log = LogFile::open(&log_path).await?;
            let mut events = Vec::with_capacity(CLEAN_LOGS);
            for i in 0..CLEAN_LOGS {
                events.push(NetEventLog {
                    tx: &transactions[i],
                    peer_id: peers[i].clone(),
                    kind: kinds[i].clone(),
                });
            }
            for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
                log.persist_log(msg).await;
            }
        }

        let active = LogFile::segment_path(&log_path, 0);
        let pre_size = std::fs::metadata(&active)?.len();

        // Append only 3 of the 5 header bytes — a power-loss in the
        // middle of the header itself.
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new().append(true).open(&active)?;
            f.write_all(&[0u8, 0u8, 0u8])?;
            f.sync_all()?;
        }
        assert_eq!(std::fs::metadata(&active)?.len(), pre_size + 3);

        let log = LogFile::open(&log_path).await?;
        assert_eq!(log.num_recs, CLEAN_LOGS);
        assert_eq!(
            std::fs::metadata(&active)?.len(),
            pre_size,
            "partial header bytes should be truncated back to the last good record",
        );
        Ok(())
    }

    /// A zero-byte segment file (e.g. a crash between
    /// `OpenOptions::create` and the first append during rotation) must
    /// be tolerated on reopen — count it as zero records, treat it as the
    /// active segment, and let the next append land there normally.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zero_byte_stray_segment_is_recovered() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Populate segment 0 with a couple of clean batches.
        const FIRST_BATCH: usize = BATCH_SIZE * 2;
        let bytes = crate::util::test::random_bytes_2mb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let (transactions, peers, kinds) = build_events(&mut unstructured, FIRST_BATCH)?;
        {
            let mut log = LogFile::open(&log_path).await?;
            let mut events = Vec::with_capacity(FIRST_BATCH);
            for i in 0..FIRST_BATCH {
                events.push(NetEventLog {
                    tx: &transactions[i],
                    peer_id: peers[i].clone(),
                    kind: kinds[i].clone(),
                });
            }
            for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
                log.persist_log(msg).await;
            }
        }

        // Simulate a mid-rotation crash: a fresh empty `<base>.000001`
        // exists alongside the populated `<base>.000000`.
        let stray = LogFile::segment_path(&log_path, 1);
        std::fs::write(&stray, [])?;

        let mut log = LogFile::open(&log_path).await?;
        assert_eq!(log.num_recs, FIRST_BATCH);
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.segments.back().unwrap().index, 1);
        assert_eq!(log.segments.back().unwrap().record_count, 0);

        // Subsequent writes should land in the stray (now-active) segment.
        let (more_tx, more_peers, more_kinds) = build_events(&mut unstructured, BATCH_SIZE)?;
        let mut events = Vec::with_capacity(BATCH_SIZE);
        for i in 0..BATCH_SIZE {
            events.push(NetEventLog {
                tx: &more_tx[i],
                peer_id: more_peers[i].clone(),
                kind: more_kinds[i].clone(),
            });
        }
        for msg in NetLogMessage::to_log_message(either::Either::Right(events)) {
            log.persist_log(msg).await;
        }
        assert!(std::fs::metadata(&stray)?.len() > 0);
        assert_eq!(log.num_recs, FIRST_BATCH + BATCH_SIZE);
        Ok(())
    }

    /// `migrate_legacy` must refuse — not silently corrupt — when both a
    /// legacy single-file log and segment files exist at the same base
    /// path. The realistic cause is a partial migration crash; we choose
    /// to surface the conflict to the operator instead of guessing the
    /// chronological ordering.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn migrate_refuses_when_legacy_and_segments_coexist() -> anyhow::Result<()> {
        ensure_records_ts();
        let temp_dir = tempfile::tempdir()?;
        let log_path = temp_dir.path().join("event_log");

        // Create a non-empty legacy file and a sibling segment.
        std::fs::write(&log_path, b"not really a record but enough to be non-empty")?;
        let segment = LogFile::segment_path(&log_path, 0);
        std::fs::write(&segment, [])?;

        let result = LogFile::open(&log_path).await;
        assert!(
            result.is_err(),
            "expected open to refuse with both legacy and segment files present",
        );
        // Both files must be untouched so the operator can resolve.
        assert!(
            log_path.exists(),
            "legacy file should not be renamed on refusal"
        );
        assert!(segment.exists(), "existing segment must be left in place");
        Ok(())
    }
}
