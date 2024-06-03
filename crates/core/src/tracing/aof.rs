use byteorder::ByteOrder;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, Error},
};

use std::path::Path;

use tokio::sync::Mutex;

use super::{DynError, EventKind, NetLogMessage, RouteEvent, NEW_RECORDS_TS};

static FILE_LOCK: Mutex<()> = Mutex::const_new(());

const RECORD_LENGTH: usize = core::mem::size_of::<u32>();
const EVENT_KIND_LENGTH: usize = 1;
const EVENT_LOG_HEADER_SIZE: usize = RECORD_LENGTH + EVENT_KIND_LENGTH; // len + varint id
#[cfg(not(test))]
const MAX_LOG_RECORDS: usize = 100_000;
#[cfg(test)]
const MAX_LOG_RECORDS: usize = 10_000;
const EVENT_REGISTER_BATCH_SIZE: usize = 100;
const BATCH_SIZE: usize = EVENT_REGISTER_BATCH_SIZE;

type DefaultEndian = byteorder::BigEndian;

pub(super) struct LogFile {
    reader: BufReader<File>,
    writer: File,
    // make this configurable?
    max_log_records: usize,
    pub(super) batch: Vec<NetLogMessage>,
    num_written: usize,
    num_recs: usize,
}

impl LogFile {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .await?;
        let mut reader = BufReader::new(file);
        let num_recs = Self::num_lines(&mut reader).await.expect("non IO error");
        let writer = reader.get_ref().try_clone().await?;
        Ok(Self {
            reader,
            writer,
            max_log_records: MAX_LOG_RECORDS,
            batch: Vec::with_capacity(BATCH_SIZE),
            num_written: 0,
            num_recs,
        })
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

    pub async fn num_lines(file: &mut (impl AsyncRead + AsyncSeek + Unpin)) -> io::Result<usize> {
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
            let num_logs: usize = self.batch.len();
            let moved_batch = std::mem::replace(&mut self.batch, Vec::with_capacity(BATCH_SIZE));
            let serialization_task = tokio::task::spawn_blocking(move || {
                let mut batch_serialized_data = Vec::with_capacity(BATCH_SIZE * 1024);
                for log_item in &moved_batch {
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
            });

            match serialization_task.await {
                Ok(Ok(serialized_data)) => {
                    // tracing::debug!(bytes = %serialized_data.len(), %num_logs, "serialized logs");
                    batch_buf = serialized_data;
                    self.num_written += num_logs;
                    self.batch.clear(); // Clear the batch for new data
                }
                _ => {
                    panic!("Failed serializing log");
                }
            }
        }

        if self.num_written >= BATCH_SIZE {
            {
                let res = self.write_all(&batch_buf).await;
                if res.is_err() {
                    panic!("Failed writing to log file");
                }
            }
            self.num_recs += self.num_written;
            self.num_written = 0;
        }

        // Check the number of lines and truncate if needed
        if self.num_recs > self.max_log_records {
            const REMOVE_RECS: usize = 1000 + EVENT_REGISTER_BATCH_SIZE; // making space for 1000 new records
            if let Err(err) = self.truncate_records(REMOVE_RECS).await {
                tracing::error!("Failed truncating log file: {:?}", err);
                panic!("Failed truncating log file");
            }
            self.num_recs -= REMOVE_RECS;
        }
    }

    pub async fn truncate_records(
        &mut self,
        remove_records: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _guard = FILE_LOCK.lock().await;
        self.writer.rewind().await?;
        // tracing::debug!(position = file.stream_position().await.unwrap());
        let mut records_count = 0;
        while records_count < remove_records {
            let mut length_bytes = [0u8; EVENT_LOG_HEADER_SIZE];
            if let Err(error) = self.reader.read_exact(&mut length_bytes).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = self.reader.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            let length = DefaultEndian::read_u32(&length_bytes[..4]);
            if let Err(error) = self.reader.seek(io::SeekFrom::Current(length as i64)).await {
                if matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                    break;
                }
                let pos = self.reader.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
            records_count += 1;
        }

        // Copy the rest of the file to the buffer
        let mut buffer = Vec::new();
        if let Err(error) = self.reader.read_to_end(&mut buffer).await {
            if !matches!(error.kind(), io::ErrorKind::UnexpectedEof) {
                let pos = self.reader.stream_position().await;
                tracing::error!(%error, ?pos, "error while trying to read file");
                return Err(error.into());
            }
        }

        // Seek back to the beginning and write the remaining content
        self.reader.rewind().await?;
        self.writer.write_all(&buffer).await?;

        // Truncate the file to the new size
        self.writer.set_len(buffer.len() as u64).await?;
        self.writer.sync_all().await?;
        self.writer.seek(io::SeekFrom::End(0)).await?;
        Ok(())
    }

    pub async fn get_router_events(
        max_event_number: usize,
        event_log_path: &Path,
    ) -> Result<Vec<RouteEvent>, DynError> {
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
            if num_records == event_num {
                break;
            }
        }

        tracing::info!(len = records.len(), "records read");

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
        if let Err(err) = self.writer.write_all(&data).await {
            tracing::error!("Failed writting to event log: {err}");
            return Err(err);
        }

        if let Err(err) = self.writer.sync_all().await {
            tracing::error!("Failed syncing event log: {err}");
            return Err(err);
        }
        Ok(())
    }
}
