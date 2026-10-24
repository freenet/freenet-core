//! The format of one message on a QUIC unidirectional stream (#5940, step e3).
//!
//! Every message a node sends over QUIC gets its own unidirectional stream:
//! a fixed-size header, then `metadata_len` bytes of metadata, then
//! `total_len` bytes of payload, then the end of the stream. The header is
//! [`HEADER_LEN`] bytes, so the receiver reads it with one fixed-size read
//! and knows everything it must check before it allocates anything:
//!
//! ```text
//! offset  size  field
//! 0       1     version       HEADER_VERSION (1)
//! 1       1     kind          0 Short, 1 Stream, 2 Control
//! 2       4     stream_id     big-endian u32 (Stream only; 0 otherwise)
//! 6       8     total_len     big-endian u64, payload bytes
//! 14      4     metadata_len  big-endian u32 (Stream only; 0 otherwise)
//! ```
//!
//! - `Short`: one serialized message of any size the UDP transport carries
//!   (a short message or an oversized one it would send as a transport
//!   stream), so `total_len` is bounded by [`check_declared_stream_length`].
//! - `Stream`: a caller-named stream ([`StreamId::next`]); its payload is fed
//!   into an ordinary `StreamHandle` by the inbound adapter. The metadata is
//!   the message that announces the stream (the UDP transport embeds it in
//!   fragment #1), at most [`MAX_METADATA_BYTES`] (1120, what fits in
//!   fragment #1 on UDP). The sender leaves out metadata that does not fit,
//!   as UDP does; the caller sends the announcing message separately as a
//!   short message as well, as on UDP. Empty metadata is sent as none
//!   (`metadata_len` 0) and arrives as `None`, deliberately (see
//!   `path::frame_metadata`).
//! - `Control`: a QUIC path control frame, at most [`MAX_CONTROL_BYTES`].
//!
//! [`FrameHeader::decode`] refuses anything else, and never panics: an
//! unknown version or kind, a length the receiver would refuse, metadata on
//! a kind that has none, a nonzero stream id where none is allowed, a stream
//! id that is not caller-named, and a header cut short. The sender builds
//! its header through the same checks ([`FrameHeader::stream`] and the
//! others), so it never sends a header the receiver refuses.
//!
//! A new kind or field needs a new header version: a receiver refuses a
//! version it does not know, so a sender may use one only after the peer
//! said it understands it.

use super::super::StreamId;
use super::super::peer_connection::streaming::{
    StreamLengthRejection, check_declared_stream_length,
};
use super::super::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

/// The header version this node writes and the only one it reads.
pub(crate) const HEADER_VERSION: u8 = 1;

/// Length of the header in bytes.
pub(crate) const HEADER_LEN: usize = 18;

/// Most metadata bytes a `Stream` header may declare: 1120, the most the
/// UDP transport embeds in fragment #1 (`embeds_metadata`: `1 + 8 + len <
/// FRAGMENT_PAYLOAD_SIZE`). The metadata is the message announcing the
/// stream (a GET response or PUT header naming the contract, its size and a
/// few flags). As on UDP, the CALLER also sends that message separately as
/// a short message, which carries any length; `QuicPath` leaves out of the
/// `Stream` frame any metadata that does not fit, as `send_stream` does on
/// UDP. It is read into memory before the payload and is not charged to
/// the inbound budget, so it is held to one fragment's worth: at most 256
/// streams x 1120 bytes per connection.
pub(crate) const MAX_METADATA_BYTES: u32 = (FRAGMENT_PAYLOAD_SIZE - 1 - 8 - 1) as u32;

/// Most payload bytes a `Control` frame may declare. Control frames are a
/// few bytes (an attempt number); they are not charged to the inbound
/// budget, so the bound is what limits their memory.
pub(crate) const MAX_CONTROL_BYTES: u64 = 4 * 1024;

/// What a stream carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameKind {
    /// One complete serialized message.
    Short,
    /// A caller-named stream with optional metadata.
    Stream,
    /// A QUIC path control frame.
    Control,
}

impl FrameKind {
    fn to_byte(self) -> u8 {
        match self {
            FrameKind::Short => 0,
            FrameKind::Stream => 1,
            FrameKind::Control => 2,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(FrameKind::Short),
            1 => Some(FrameKind::Stream),
            2 => Some(FrameKind::Control),
            _ => None,
        }
    }
}

/// A validated header. Only [`FrameHeader::decode`] and the constructors
/// make one, so every header in hand passed the receiver's checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    kind: FrameKind,
    stream_id: StreamId,
    total_len: u64,
    metadata_len: u32,
}

/// Why a header was refused. Each has its own counter on the receiving
/// path (`QuicInboundOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HeaderError {
    /// The stream ended before [`HEADER_LEN`] bytes.
    #[error("the stream ended after {0} of {HEADER_LEN} header bytes")]
    Truncated(usize),
    /// A header version this node does not read.
    #[error("unknown header version {0}")]
    UnknownVersion(u8),
    /// A kind byte this node does not know.
    #[error("unknown frame kind {0}")]
    UnknownKind(u8),
    /// `total_len` is 0 or above `MAX_STREAM_TOTAL_BYTES` (Short, Stream).
    #[error("invalid declared length: {0:?}")]
    InvalidLength(StreamLengthRejection),
    /// A `Control` frame above [`MAX_CONTROL_BYTES`], or empty.
    #[error("invalid control frame length {0}")]
    InvalidControlLength(u64),
    /// `metadata_len` above [`MAX_METADATA_BYTES`].
    #[error("metadata length {0} above the limit")]
    MetadataTooLong(u32),
    /// Metadata on a Short or Control frame.
    #[error("metadata on a frame kind that has none")]
    UnexpectedMetadata,
    /// A nonzero stream id on a Short or Control frame.
    #[error("a stream id on a frame kind that has none")]
    UnexpectedStreamId,
    /// A Stream frame whose id is not caller-named ([`StreamId::next`]).
    #[error("stream id {0} is not caller-named")]
    NotCallerNamed(StreamId),
}

impl FrameHeader {
    /// The header of a short message of `len` bytes.
    pub(crate) fn short(len: u64) -> Result<Self, HeaderError> {
        Self::checked(FrameKind::Short, StreamId(0), len, 0)
    }

    /// The header of stream `stream_id` with `total_len` payload bytes and
    /// `metadata_len` metadata bytes.
    pub(crate) fn stream(
        stream_id: StreamId,
        total_len: u64,
        metadata_len: usize,
    ) -> Result<Self, HeaderError> {
        let metadata_len = u32::try_from(metadata_len).unwrap_or(u32::MAX);
        Self::checked(FrameKind::Stream, stream_id, total_len, metadata_len)
    }

    /// The header of a control frame of `len` bytes.
    pub(crate) fn control(len: u64) -> Result<Self, HeaderError> {
        Self::checked(FrameKind::Control, StreamId(0), len, 0)
    }

    /// The receiver's checks, in one place for both directions.
    fn checked(
        kind: FrameKind,
        stream_id: StreamId,
        total_len: u64,
        metadata_len: u32,
    ) -> Result<Self, HeaderError> {
        match kind {
            FrameKind::Short | FrameKind::Control => {
                if stream_id.0 != 0 {
                    return Err(HeaderError::UnexpectedStreamId);
                }
                if metadata_len != 0 {
                    return Err(HeaderError::UnexpectedMetadata);
                }
            }
            FrameKind::Stream => {
                if !stream_id.is_caller_named() {
                    return Err(HeaderError::NotCallerNamed(stream_id));
                }
                if metadata_len > MAX_METADATA_BYTES {
                    return Err(HeaderError::MetadataTooLong(metadata_len));
                }
            }
        }
        match kind {
            FrameKind::Short | FrameKind::Stream => {
                check_declared_stream_length(total_len).map_err(HeaderError::InvalidLength)?;
            }
            FrameKind::Control => {
                if total_len == 0 || total_len > MAX_CONTROL_BYTES {
                    return Err(HeaderError::InvalidControlLength(total_len));
                }
            }
        }
        Ok(Self {
            kind,
            stream_id,
            total_len,
            metadata_len,
        })
    }

    /// Parse and check a header. `bytes` is what the stream delivered,
    /// [`HEADER_LEN`] bytes unless it ended early; anything shorter is
    /// [`HeaderError::Truncated`]. Bytes after the header are ignored here.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, HeaderError> {
        let Some(header) = bytes.get(..HEADER_LEN) else {
            return Err(HeaderError::Truncated(bytes.len()));
        };
        if header[0] != HEADER_VERSION {
            return Err(HeaderError::UnknownVersion(header[0]));
        }
        let kind = FrameKind::from_byte(header[1]).ok_or(HeaderError::UnknownKind(header[1]))?;
        let mut stream_id = [0u8; 4];
        stream_id.copy_from_slice(&header[2..6]);
        let mut total_len = [0u8; 8];
        total_len.copy_from_slice(&header[6..14]);
        let mut metadata_len = [0u8; 4];
        metadata_len.copy_from_slice(&header[14..18]);
        Self::checked(
            kind,
            StreamId(u32::from_be_bytes(stream_id)),
            u64::from_be_bytes(total_len),
            u32::from_be_bytes(metadata_len),
        )
    }

    /// The header's bytes.
    pub(crate) fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0] = HEADER_VERSION;
        out[1] = self.kind.to_byte();
        out[2..6].copy_from_slice(&self.stream_id.0.to_be_bytes());
        out[6..14].copy_from_slice(&self.total_len.to_be_bytes());
        out[14..18].copy_from_slice(&self.metadata_len.to_be_bytes());
        out
    }

    pub(crate) fn kind(&self) -> FrameKind {
        self.kind
    }

    pub(crate) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Payload bytes, excluding the metadata.
    pub(crate) fn total_len(&self) -> u64 {
        self.total_len
    }

    pub(crate) fn metadata_len(&self) -> u32 {
        self.metadata_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::peer_connection::streaming::MAX_STREAM_TOTAL_BYTES;

    /// A caller-named id, without touching the thread's id counter.
    fn caller_named(n: u32) -> StreamId {
        StreamId(0x8000_0000 | n)
    }

    /// Raw header bytes with the given fields, valid or not.
    fn raw(version: u8, kind: u8, stream_id: u32, total_len: u64, metadata_len: u32) -> Vec<u8> {
        let mut out = vec![version, kind];
        out.extend_from_slice(&stream_id.to_be_bytes());
        out.extend_from_slice(&total_len.to_be_bytes());
        out.extend_from_slice(&metadata_len.to_be_bytes());
        out
    }

    #[test]
    fn headers_round_trip_with_the_documented_layout() {
        let stream = FrameHeader::stream(caller_named(7), 1_000_000, 300).unwrap();
        let bytes = stream.encode();
        assert_eq!(
            bytes.to_vec(),
            raw(1, 1, 0x8000_0007, 1_000_000, 300),
            "byte layout"
        );
        assert_eq!(FrameHeader::decode(&bytes), Ok(stream));
        for header in [
            FrameHeader::short(1).unwrap(),
            FrameHeader::short(MAX_STREAM_TOTAL_BYTES).unwrap(),
            FrameHeader::control(MAX_CONTROL_BYTES).unwrap(),
            FrameHeader::stream(caller_named(1), 1, 0).unwrap(),
            FrameHeader::stream(
                caller_named(1),
                MAX_STREAM_TOTAL_BYTES,
                MAX_METADATA_BYTES as usize,
            )
            .unwrap(),
        ] {
            assert_eq!(FrameHeader::decode(&header.encode()), Ok(header));
        }
        // Bytes after the header belong to the metadata or payload.
        let mut longer = stream.encode().to_vec();
        longer.extend_from_slice(b"payload");
        assert_eq!(FrameHeader::decode(&longer), Ok(stream));
    }

    #[test]
    fn every_bound_is_refused_at_its_edge() {
        let cases: Vec<(Vec<u8>, HeaderError)> = vec![
            (
                raw(1, 0, 0, 0, 0),
                HeaderError::InvalidLength(StreamLengthRejection::Empty),
            ),
            (
                raw(1, 0, 0, MAX_STREAM_TOTAL_BYTES + 1, 0),
                HeaderError::InvalidLength(StreamLengthRejection::TooLarge),
            ),
            (
                raw(1, 1, 0x8000_0001, 0, 0),
                HeaderError::InvalidLength(StreamLengthRejection::Empty),
            ),
            (
                raw(1, 1, 0x8000_0001, MAX_STREAM_TOTAL_BYTES + 1, 0),
                HeaderError::InvalidLength(StreamLengthRejection::TooLarge),
            ),
            (
                raw(1, 1, 0x8000_0001, u64::MAX, 0),
                HeaderError::InvalidLength(StreamLengthRejection::TooLarge),
            ),
            (
                raw(1, 1, 0x8000_0001, 10, MAX_METADATA_BYTES + 1),
                HeaderError::MetadataTooLong(MAX_METADATA_BYTES + 1),
            ),
            (raw(1, 2, 0, 0, 0), HeaderError::InvalidControlLength(0)),
            (
                raw(1, 2, 0, MAX_CONTROL_BYTES + 1, 0),
                HeaderError::InvalidControlLength(MAX_CONTROL_BYTES + 1),
            ),
            (raw(1, 0, 0, 10, 1), HeaderError::UnexpectedMetadata),
            (raw(1, 2, 0, 10, 1), HeaderError::UnexpectedMetadata),
            (raw(1, 0, 5, 10, 0), HeaderError::UnexpectedStreamId),
            (raw(1, 2, 5, 10, 0), HeaderError::UnexpectedStreamId),
            (
                raw(1, 1, 5, 10, 0),
                HeaderError::NotCallerNamed(StreamId(5)),
            ),
            (raw(0, 0, 0, 10, 0), HeaderError::UnknownVersion(0)),
            (raw(2, 0, 0, 10, 0), HeaderError::UnknownVersion(2)),
            (raw(1, 3, 0, 10, 0), HeaderError::UnknownKind(3)),
            (raw(1, 0xFF, 0, 10, 0), HeaderError::UnknownKind(0xFF)),
        ];
        for (bytes, expected) in cases {
            assert_eq!(FrameHeader::decode(&bytes), Err(expected), "{bytes:?}");
        }
        // The edges themselves are accepted.
        assert!(FrameHeader::decode(&raw(1, 1, 0x8000_0001, 10, MAX_METADATA_BYTES)).is_ok());
        assert!(FrameHeader::decode(&raw(1, 2, 0, MAX_CONTROL_BYTES, 0)).is_ok());
        assert!(FrameHeader::decode(&raw(1, 2, 0, 1, 0)).is_ok());
        assert!(FrameHeader::decode(&raw(1, 0, 0, MAX_STREAM_TOTAL_BYTES, 0)).is_ok());
    }

    #[test]
    fn a_header_cut_short_is_truncated_at_every_length() {
        let bytes = FrameHeader::short(10).unwrap().encode();
        for len in 0..HEADER_LEN {
            assert_eq!(
                FrameHeader::decode(&bytes[..len]),
                Err(HeaderError::Truncated(len))
            );
        }
    }

    /// The metadata bound is exactly what the UDP transport embeds in
    /// fragment #1 (`outbound_stream.rs`: `1 + 8 + len < MAX_DATA_SIZE`,
    /// which is `FRAGMENT_PAYLOAD_SIZE`), so it stays one fragment's worth
    /// of uncharged memory per stream.
    #[test]
    fn the_metadata_bound_is_what_udp_embeds_in_fragment_one() {
        use crate::transport::peer_connection::embeds_metadata;
        assert!(embeds_metadata(MAX_METADATA_BYTES as usize));
        assert!(!embeds_metadata(MAX_METADATA_BYTES as usize + 1));
        assert_eq!(MAX_METADATA_BYTES, 1120);
    }

    /// The sender's constructors refuse what the receiver would.
    #[test]
    fn the_sender_cannot_build_a_header_the_receiver_refuses() {
        assert!(FrameHeader::short(0).is_err());
        assert!(FrameHeader::short(MAX_STREAM_TOTAL_BYTES + 1).is_err());
        assert!(FrameHeader::control(MAX_CONTROL_BYTES + 1).is_err());
        assert!(FrameHeader::stream(StreamId(1), 10, 0).is_err());
        assert!(FrameHeader::stream(caller_named(1), 10, MAX_METADATA_BYTES as usize + 1).is_err());
        assert!(FrameHeader::stream(caller_named(1), 10, usize::MAX).is_err());
    }

    /// Headers built from each field's valid and edge values: the decoder
    /// accepts exactly those an independent statement of the rules accepts,
    /// round-trips every accepted one, and never panics. Arbitrary bytes
    /// never panic it either.
    #[test]
    fn decode_matches_the_rules_and_never_panics() {
        use crate::config::GlobalRng;
        let _seed = GlobalRng::seed_guard(0x0059_40E3);
        let pick = |choices: &[u64]| choices[GlobalRng::random_range(0..choices.len())];
        let mut accepted = 0;
        let mut refused = 0;
        for _ in 0..20_000 {
            let version = pick(&[1, 1, 1, 0, 2]) as u8;
            let kind = pick(&[0, 1, 2, 3]) as u8;
            let stream_id = pick(&[
                0,
                0x8000_0000 | GlobalRng::random_range(0..0x7FFF_FFFFu64),
                GlobalRng::random_range(1..0x8000_0000u64),
            ]) as u32;
            let total_len = pick(&[
                0,
                1,
                GlobalRng::random_range(1..=MAX_CONTROL_BYTES + 1),
                GlobalRng::random_range(1..=MAX_STREAM_TOTAL_BYTES),
                MAX_STREAM_TOTAL_BYTES,
                MAX_STREAM_TOTAL_BYTES + 1,
                u64::MAX,
            ]);
            let metadata_len = pick(&[
                0,
                0,
                GlobalRng::random_range(1..=u64::from(MAX_METADATA_BYTES)),
                u64::from(MAX_METADATA_BYTES),
                u64::from(MAX_METADATA_BYTES) + 1,
                u64::from(u32::MAX),
            ]) as u32;
            let valid = version == 1
                && match kind {
                    0 => {
                        stream_id == 0
                            && metadata_len == 0
                            && (1..=MAX_STREAM_TOTAL_BYTES).contains(&total_len)
                    }
                    1 => {
                        stream_id & 0x8000_0000 != 0
                            && metadata_len <= MAX_METADATA_BYTES
                            && (1..=MAX_STREAM_TOTAL_BYTES).contains(&total_len)
                    }
                    2 => {
                        stream_id == 0
                            && metadata_len == 0
                            && (1..=MAX_CONTROL_BYTES).contains(&total_len)
                    }
                    _ => false,
                };
            let bytes = raw(version, kind, stream_id, total_len, metadata_len);
            match FrameHeader::decode(&bytes) {
                Ok(header) => {
                    assert!(valid, "accepted {bytes:?}");
                    assert_eq!(header.encode().to_vec(), bytes);
                    accepted += 1;
                }
                Err(_) => {
                    assert!(!valid, "refused a valid header {bytes:?}");
                    refused += 1;
                }
            }
        }
        assert!(accepted > 500 && refused > 500, "{accepted} / {refused}");

        for _ in 0..10_000 {
            let len = GlobalRng::random_range(0usize..=HEADER_LEN + 4);
            let mut bytes = vec![0u8; len];
            GlobalRng::fill_bytes(&mut bytes);
            // Half the time the known version, so later checks are reached.
            if len > 0 && GlobalRng::random_bool(0.5) {
                bytes[0] = HEADER_VERSION;
            }
            if let Ok(header) = FrameHeader::decode(&bytes) {
                assert_eq!(FrameHeader::decode(&header.encode()), Ok(header));
            }
        }
    }
}
