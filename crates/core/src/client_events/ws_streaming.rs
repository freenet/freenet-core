//! WebSocket message streaming protocol for large payloads.
//!
//! All WebSocket messages use a 1-byte type prefix:
//! - `0x00` + payload = complete message (small, no chunking needed)
//! - `0x01` + 4 bytes (total_chunks LE) + payload = stream chunk
//!
//! The chunk header is 5 bytes (`CHUNK_HEADER_SIZE`): the 1-byte type prefix
//! followed by a single little-endian `u32` total_chunks field.

use bytes::Bytes;

const MSG_COMPLETE: u8 = 0x00;
const MSG_CHUNK: u8 = 0x01;

/// 1 (type) + 4 (total_chunks).
pub(crate) const CHUNK_HEADER_SIZE: usize = 5;

/// Default chunk payload size: 256 KiB.
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Messages larger than this threshold are chunked.
pub(crate) const CHUNK_THRESHOLD: usize = 512 * 1024;

/// Chunks to send before yielding to the tokio runtime, allowing pings and
/// other control messages through the select loop.
pub(crate) const MAX_CHUNKS_PER_BATCH: usize = 32;

/// Maximum `total_chunks` accepted from the wire.
/// Based on MAX_STATE_SIZE (50 MiB) / DEFAULT_CHUNK_SIZE.
const MAX_TOTAL_CHUNKS: u32 = 256;

/// Parsed streaming message.
#[derive(Debug)]
pub(crate) enum StreamMessage<'a> {
    Complete(&'a [u8]),
    Chunk {
        total_chunks: u32,
        payload: &'a [u8],
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamError {
    #[error("message too short: expected at least {expected} bytes, got {actual}")]
    MessageTooShort { expected: usize, actual: usize },
    #[error("unknown message type prefix: 0x{0:02x}")]
    UnknownMessageType(u8),
    #[error("total_chunks is zero")]
    ZeroTotalChunks,
    #[error("total_chunks {total_chunks} exceeds maximum {max}")]
    TotalChunksTooLarge { total_chunks: u32, max: u32 },
    #[error("total_chunks mismatch (expected {expected}, got {actual})")]
    TotalChunksMismatch { expected: u32, actual: u32 },
}

/// Wraps a serialized payload as a complete (non-chunked) streaming message.
pub(crate) fn wrap_complete(data: Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(1 + data.len());
    buf.push(MSG_COMPLETE);
    buf.extend_from_slice(&data);
    buf.into()
}

/// Splits a serialized payload into chunked streaming messages.
///
/// Uses `Bytes::slice()` for zero-copy fragmentation of the payload portion.
pub(crate) fn chunk_payload(data: Bytes) -> Vec<Bytes> {
    if data.is_empty() {
        let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE);
        buf.push(MSG_CHUNK);
        buf.extend_from_slice(&1u32.to_le_bytes());
        return vec![buf.into()];
    }

    let total_chunks = data.len().div_ceil(DEFAULT_CHUNK_SIZE);
    let mut chunks = Vec::with_capacity(total_chunks);

    for i in 0..total_chunks {
        let start = i * DEFAULT_CHUNK_SIZE;
        let end = (start + DEFAULT_CHUNK_SIZE).min(data.len());
        let payload_slice = data.slice(start..end);

        let mut header = Vec::with_capacity(CHUNK_HEADER_SIZE);
        header.push(MSG_CHUNK);
        header.extend_from_slice(&(total_chunks as u32).to_le_bytes());

        let mut frame = Vec::with_capacity(CHUNK_HEADER_SIZE + payload_slice.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload_slice);
        chunks.push(Bytes::from(frame));
    }

    chunks
}

/// Parses a raw WebSocket binary message into a streaming protocol message.
pub(crate) fn parse_message(data: &[u8]) -> Result<StreamMessage<'_>, StreamError> {
    if data.is_empty() {
        return Err(StreamError::MessageTooShort {
            expected: 1,
            actual: 0,
        });
    }

    match data[0] {
        MSG_COMPLETE => Ok(StreamMessage::Complete(&data[1..])),
        MSG_CHUNK => {
            if data.len() < CHUNK_HEADER_SIZE {
                return Err(StreamError::MessageTooShort {
                    expected: CHUNK_HEADER_SIZE,
                    actual: data.len(),
                });
            }
            let total_chunks = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);

            if total_chunks == 0 {
                return Err(StreamError::ZeroTotalChunks);
            }
            if total_chunks > MAX_TOTAL_CHUNKS {
                return Err(StreamError::TotalChunksTooLarge {
                    total_chunks,
                    max: MAX_TOTAL_CHUNKS,
                });
            }

            Ok(StreamMessage::Chunk {
                total_chunks,
                payload: &data[CHUNK_HEADER_SIZE..],
            })
        }
        other => Err(StreamError::UnknownMessageType(other)),
    }
}

/// Sequential reassembly buffer for chunked streams.
pub(crate) struct ChunkReassemblyBuffer {
    data: Vec<u8>,
    total_chunks: u32,
    received: u32,
}

impl ChunkReassemblyBuffer {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            total_chunks: 0,
            received: 0,
        }
    }

    /// Receives a chunk and returns the fully reassembled payload when all chunks arrive.
    ///
    /// Returns `Ok(None)` if more chunks are needed.
    pub fn receive_chunk(
        &mut self,
        total_chunks: u32,
        payload: &[u8],
    ) -> Result<Option<Vec<u8>>, StreamError> {
        if self.received == 0 {
            self.total_chunks = total_chunks;
            self.data
                .reserve(total_chunks as usize * DEFAULT_CHUNK_SIZE);
        } else if self.total_chunks != total_chunks {
            return Err(StreamError::TotalChunksMismatch {
                expected: self.total_chunks,
                actual: total_chunks,
            });
        }

        self.data.extend_from_slice(payload);
        self.received += 1;

        if self.received == self.total_chunks {
            let result = std::mem::take(&mut self.data);
            self.received = 0;
            self.total_chunks = 0;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_complete_roundtrip() {
        let data = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let wrapped = wrap_complete(data.clone());
        assert_eq!(wrapped[0], MSG_COMPLETE);
        match parse_message(&wrapped).unwrap() {
            StreamMessage::Complete(payload) => assert_eq!(payload, &data[..]),
            StreamMessage::Chunk { .. } => panic!("expected Complete"),
        }
    }

    #[test]
    fn test_chunk_small_payload() {
        let data = Bytes::from(vec![42u8; 1024]);
        let chunks = chunk_payload(data.clone());
        assert_eq!(chunks.len(), 1);

        match parse_message(&chunks[0]).unwrap() {
            StreamMessage::Chunk {
                total_chunks,
                payload,
            } => {
                assert_eq!(total_chunks, 1);
                assert_eq!(payload, &data[..]);
            }
            StreamMessage::Complete(_) => panic!("expected Chunk"),
        }
    }

    #[test]
    fn test_chunk_large_payload_roundtrip() {
        // 600 KiB payload → 3 chunks (256 + 256 + 88)
        let data: Vec<u8> = (0..600 * 1024).map(|i| (i % 256) as u8).collect();
        let chunks = chunk_payload(Bytes::from(data.clone()));
        assert_eq!(chunks.len(), 3);

        let mut reassembly = ChunkReassemblyBuffer::new();
        for (i, chunk) in chunks.iter().enumerate() {
            match parse_message(chunk).unwrap() {
                StreamMessage::Chunk {
                    total_chunks,
                    payload,
                } => {
                    assert_eq!(total_chunks, 3);
                    let result = reassembly.receive_chunk(total_chunks, payload).unwrap();
                    if i < 2 {
                        assert!(result.is_none());
                    } else {
                        assert_eq!(result.unwrap(), data);
                    }
                }
                StreamMessage::Complete(_) => panic!("expected Chunk"),
            }
        }
    }

    #[test]
    fn test_chunk_empty_payload() {
        let chunks = chunk_payload(Bytes::new());
        assert_eq!(chunks.len(), 1);

        match parse_message(&chunks[0]).unwrap() {
            StreamMessage::Chunk {
                total_chunks,
                payload,
            } => {
                assert_eq!(total_chunks, 1);
                assert!(payload.is_empty());

                let mut reassembly = ChunkReassemblyBuffer::new();
                let result = reassembly.receive_chunk(total_chunks, payload).unwrap();
                assert_eq!(result.unwrap(), Vec::<u8>::new());
            }
            StreamMessage::Complete(_) => panic!("expected Chunk"),
        }
    }

    #[test]
    fn test_parse_empty_message_error() {
        let err = parse_message(&[]).unwrap_err();
        assert!(matches!(err, StreamError::MessageTooShort { .. }));
    }

    #[test]
    fn test_parse_unknown_type_error() {
        let err = parse_message(&[0xFF, 1, 2, 3]).unwrap_err();
        assert!(matches!(err, StreamError::UnknownMessageType(0xFF)));
    }

    #[test]
    fn test_parse_truncated_chunk_header() {
        let err = parse_message(&[MSG_CHUNK, 0, 0]).unwrap_err();
        assert!(matches!(err, StreamError::MessageTooShort { .. }));
    }

    #[test]
    fn test_zero_total_chunks() {
        let mut data = vec![MSG_CHUNK];
        data.extend_from_slice(&0u32.to_le_bytes());
        let err = parse_message(&data).unwrap_err();
        assert!(matches!(err, StreamError::ZeroTotalChunks));
    }

    #[test]
    fn test_total_chunks_too_large() {
        let mut data = vec![MSG_CHUNK];
        data.extend_from_slice(&1000u32.to_le_bytes());
        let err = parse_message(&data).unwrap_err();
        assert!(matches!(err, StreamError::TotalChunksTooLarge { .. }));
    }

    #[test]
    fn test_total_chunks_mismatch() {
        let mut reassembly = ChunkReassemblyBuffer::new();
        reassembly.receive_chunk(3, &[1, 2, 3]).unwrap();
        let err = reassembly.receive_chunk(5, &[4, 5, 6]).unwrap_err();
        assert!(matches!(err, StreamError::TotalChunksMismatch { .. }));
    }

    #[test]
    fn test_payload_at_exact_chunk_boundary() {
        // Exactly 2 * 256 KiB → 2 chunks
        let data = vec![0xAB; DEFAULT_CHUNK_SIZE * 2];
        let chunks = chunk_payload(Bytes::from(data.clone()));
        assert_eq!(chunks.len(), 2);

        let mut reassembly = ChunkReassemblyBuffer::new();
        for chunk in &chunks {
            if let StreamMessage::Chunk {
                total_chunks,
                payload,
            } = parse_message(chunk).unwrap()
            {
                if let Some(reassembled) = reassembly.receive_chunk(total_chunks, payload).unwrap()
                {
                    assert_eq!(reassembled, data);
                }
            }
        }
    }

    #[test]
    fn test_reassembly_resets_after_completion() {
        let data_a = Bytes::from(vec![0xAA; DEFAULT_CHUNK_SIZE * 2]);
        let data_b = Bytes::from(vec![0xBB; DEFAULT_CHUNK_SIZE * 3]);

        let mut reassembly = ChunkReassemblyBuffer::new();

        // First message
        for chunk in &chunk_payload(data_a.clone()) {
            if let StreamMessage::Chunk {
                total_chunks,
                payload,
            } = parse_message(chunk).unwrap()
            {
                if let Some(r) = reassembly.receive_chunk(total_chunks, payload).unwrap() {
                    assert_eq!(r, &data_a[..]);
                }
            }
        }

        // Second message reuses the same buffer
        for chunk in &chunk_payload(data_b.clone()) {
            if let StreamMessage::Chunk {
                total_chunks,
                payload,
            } = parse_message(chunk).unwrap()
            {
                if let Some(r) = reassembly.receive_chunk(total_chunks, payload).unwrap() {
                    assert_eq!(r, &data_b[..]);
                }
            }
        }
    }
}
