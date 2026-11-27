//! Wire format between the node and its compile worker.
//!
//! The node writes one request to the worker's stdin and reads one response from
//! its stdout. The response carries a verdict and a few numbers for logging; it
//! never carries compiled code. All integers are little-endian. Every length is
//! checked against its cap before anything is allocated.

use std::io::{self, Read, Write};

/// First bytes of a request and of a response.
pub(crate) const MAGIC: [u8; 4] = *b"FNCW";
/// Bumped when either frame changes.
pub(crate) const PROTOCOL_VERSION: u16 = 1;
/// Longest reason text a response may carry, in bytes.
pub(crate) const MAX_REASON_BYTES: usize = 256;
/// Largest module a request may carry (the node's own code size limit).
pub(crate) const MAX_REQUEST_WASM_BYTES: usize = super::super::compile_limits::MAX_MODULE_BYTES;
/// Size of the request header, before the module bytes.
pub(crate) const REQUEST_HEADER_BYTES: usize = 4 + 2 + 8 + 16 + 1 + 3 * 8 + 4;
/// Largest response frame.
pub(crate) const MAX_RESPONSE_BYTES: usize = 4 + 2 + 1 + 2 * 8 + 2 + MAX_REASON_BYTES;

/// Request flag: the node's engine consumes fuel (it changes the engine identity).
pub(crate) const FLAG_CONSUME_FUEL: u8 = 1;

/// What the worker found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Status {
    /// The scan accepted the module and the engine compiled it within the caps.
    Ok = 0,
    /// The engine found the module invalid (a property of the bytes).
    Invalid = 1,
    /// The scan refused the module.
    RefusedScan = 2,
    /// The compile failed because memory ran out.
    OutOfMemory = 3,
    /// The worker failed for a reason that is not a property of the bytes.
    InternalError = 4,
    /// Magic, protocol version, engine identity or build identity differ.
    VersionMismatch = 5,
    /// A limit or protection the worker was asked to install could not be
    /// installed (for example an inherited hard limit below the requested one).
    /// Nothing was judged: not a property of the bytes.
    LimitInstallFailed = 6,
}

impl Status {
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Ok,
            1 => Self::Invalid,
            2 => Self::RefusedScan,
            3 => Self::OutOfMemory,
            4 => Self::InternalError,
            5 => Self::VersionMismatch,
            6 => Self::LimitInstallFailed,
            _ => return None,
        })
    }
}

/// Limits the worker applies to itself (and the parent enforces from outside).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Caps {
    /// CPU time, all threads summed, in milliseconds.
    pub cpu_ms: u64,
    /// Private writable memory (`RLIMIT_DATA`), in bytes.
    pub data_bytes: u64,
    /// Wall-clock time, in milliseconds.
    pub wall_ms: u64,
}

/// The node's identity as the worker must see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub engine_compat_id: u64,
    pub build_id: [u8; 16],
    pub flags: u8,
}

/// A decoded request header; the module bytes follow it on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestHeader {
    pub version: u16,
    pub identity: Identity,
    pub caps: Caps,
    pub wasm_len: u32,
}

/// A decoded response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Response {
    pub status: Status,
    /// The worker's own peak resident memory (`VmHWM`), in KiB.
    pub vm_hwm_kib: u64,
    /// The worker's own CPU time, in milliseconds.
    pub cpu_ms: u64,
    /// Text for logs only; the parent never acts on it.
    pub reason: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ProtocolError {
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported protocol version {0}")]
    BadVersion(u16),
    #[error("unknown status {0}")]
    BadStatus(u8),
    #[error("module length {0} is over the limit")]
    TooLong(u64),
    #[error("reason length {0} is over the limit")]
    ReasonTooLong(usize),
    #[error("reason is not UTF-8")]
    ReasonNotUtf8,
    #[error("truncated frame")]
    Truncated,
    #[error("i/o error: {0}")]
    Io(String),
}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io(e.to_string())
        }
    }
}

/// Encode the request header for `wasm_len` bytes of module.
pub(crate) fn encode_request_header(identity: &Identity, caps: &Caps, wasm_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(REQUEST_HEADER_BYTES);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    out.extend_from_slice(&identity.engine_compat_id.to_le_bytes());
    out.extend_from_slice(&identity.build_id);
    out.push(identity.flags);
    out.extend_from_slice(&caps.cpu_ms.to_le_bytes());
    out.extend_from_slice(&caps.data_bytes.to_le_bytes());
    out.extend_from_slice(&caps.wall_ms.to_le_bytes());
    out.extend_from_slice(&u32::try_from(wasm_len).unwrap_or(u32::MAX).to_le_bytes());
    debug_assert_eq!(out.len(), REQUEST_HEADER_BYTES);
    out
}

/// Read and validate a request header. An unknown version is reported as such
/// (the caller answers `VersionMismatch`); a length over the cap is an error
/// before any allocation.
pub(crate) fn read_request_header(r: &mut impl Read) -> Result<RequestHeader, ProtocolError> {
    let mut b = [0u8; REQUEST_HEADER_BYTES];
    r.read_exact(&mut b)?;
    if b[0..4] != MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    // The `try_into().expect(..)` calls below convert a slice with constant
    // bounds into an array of that same width, from a buffer whose length is the
    // fixed header size read just above, so they cannot fail.
    let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().expect("8 bytes"));
    let version = u16::from_le_bytes([b[4], b[5]]);
    let mut build_id = [0u8; 16];
    build_id.copy_from_slice(&b[14..30]);
    let wasm_len = u32::from_le_bytes(b[55..59].try_into().expect("4 bytes"));
    if wasm_len as usize > MAX_REQUEST_WASM_BYTES {
        return Err(ProtocolError::TooLong(u64::from(wasm_len)));
    }
    Ok(RequestHeader {
        version,
        identity: Identity {
            engine_compat_id: u64_at(6),
            build_id,
            flags: b[30],
        },
        caps: Caps {
            cpu_ms: u64_at(31),
            data_bytes: u64_at(39),
            wall_ms: u64_at(47),
        },
        wasm_len,
    })
}

/// Read exactly `len` module bytes without trusting `len` beyond its cap.
pub(crate) fn read_wasm(r: &mut impl Read, len: u32) -> Result<Vec<u8>, ProtocolError> {
    if len as usize > MAX_REQUEST_WASM_BYTES {
        return Err(ProtocolError::TooLong(u64::from(len)));
    }
    let mut v = Vec::new();
    // `take` + `read_to_end` allocates as bytes arrive, so a frame that declares
    // 10 MiB and carries 3 bytes does not make us reserve 10 MiB.
    let n = r.take(u64::from(len)).read_to_end(&mut v)?;
    if n != len as usize {
        return Err(ProtocolError::Truncated);
    }
    Ok(v)
}

/// Encode a response; the reason is cut to [`MAX_REASON_BYTES`] on a character
/// boundary.
pub(crate) fn encode_response(resp: &Response) -> Vec<u8> {
    let mut end = resp.reason.len().min(MAX_REASON_BYTES);
    while !resp.reason.is_char_boundary(end) {
        end -= 1;
    }
    let reason = &resp.reason.as_bytes()[..end];
    let mut out = Vec::with_capacity(MAX_RESPONSE_BYTES);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    out.push(resp.status as u8);
    out.extend_from_slice(&resp.vm_hwm_kib.to_le_bytes());
    out.extend_from_slice(&resp.cpu_ms.to_le_bytes());
    out.extend_from_slice(&(reason.len() as u16).to_le_bytes());
    out.extend_from_slice(reason);
    out
}

/// Decode a response from `r`, reading no more than [`MAX_RESPONSE_BYTES`].
pub(crate) fn read_response(r: &mut impl Read) -> Result<Response, ProtocolError> {
    let mut h = [0u8; 4 + 2 + 1 + 2 * 8 + 2];
    r.read_exact(&mut h)?;
    if h[0..4] != MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    let version = u16::from_le_bytes([h[4], h[5]]);
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::BadVersion(version));
    }
    let status = Status::from_u8(h[6]).ok_or(ProtocolError::BadStatus(h[6]))?;
    // Fixed-width slices of the fixed-size response header read above: the
    // conversions to arrays cannot fail.
    let vm_hwm_kib = u64::from_le_bytes(h[7..15].try_into().expect("8 bytes"));
    let cpu_ms = u64::from_le_bytes(h[15..23].try_into().expect("8 bytes"));
    let reason_len = u16::from_le_bytes([h[23], h[24]]) as usize;
    if reason_len > MAX_REASON_BYTES {
        return Err(ProtocolError::ReasonTooLong(reason_len));
    }
    let mut reason = vec![0u8; reason_len];
    r.read_exact(&mut reason)?;
    let reason = String::from_utf8(reason).map_err(|_| ProtocolError::ReasonNotUtf8)?;
    Ok(Response {
        status,
        vm_hwm_kib,
        cpu_ms,
        reason,
    })
}

/// Write `frame` and flush.
pub(crate) fn write_all(w: &mut impl Write, frame: &[u8]) -> io::Result<()> {
    w.write_all(frame)?;
    w.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> Identity {
        Identity {
            engine_compat_id: 0x0123_4567_89ab_cdef,
            build_id: [7; 16],
            flags: FLAG_CONSUME_FUEL,
        }
    }
    fn caps() -> Caps {
        Caps {
            cpu_ms: 20_000,
            data_bytes: 512 << 20,
            wall_ms: 30_000,
        }
    }

    #[test]
    fn request_round_trips() {
        let mut frame = encode_request_header(&identity(), &caps(), 3);
        frame.extend_from_slice(b"abc");
        let mut r = &frame[..];
        let h = read_request_header(&mut r).unwrap();
        assert_eq!(h.version, PROTOCOL_VERSION);
        assert_eq!(h.identity, identity());
        assert_eq!(h.caps, caps());
        assert_eq!(read_wasm(&mut r, h.wasm_len).unwrap(), b"abc");
    }

    #[test]
    fn request_rejects_bad_magic_truncation_and_oversize() {
        let mut frame = encode_request_header(&identity(), &caps(), 0);
        frame[0] = b'X';
        assert_eq!(
            read_request_header(&mut &frame[..]),
            Err(ProtocolError::BadMagic)
        );
        let frame = encode_request_header(&identity(), &caps(), 0);
        assert_eq!(
            read_request_header(&mut &frame[..REQUEST_HEADER_BYTES - 1]),
            Err(ProtocolError::Truncated)
        );
        let frame = encode_request_header(&identity(), &caps(), MAX_REQUEST_WASM_BYTES + 1);
        assert!(matches!(
            read_request_header(&mut &frame[..]),
            Err(ProtocolError::TooLong(_))
        ));
    }

    #[test]
    fn declared_length_beyond_available_bytes_is_truncated_without_big_allocation() {
        // Declares the maximum but supplies 3 bytes.
        let data = b"abc";
        let err = read_wasm(&mut &data[..], MAX_REQUEST_WASM_BYTES as u32).unwrap_err();
        assert_eq!(err, ProtocolError::Truncated);
    }

    #[test]
    fn version_is_reported_to_the_caller() {
        let mut frame = encode_request_header(&identity(), &caps(), 0);
        frame[4] = 9;
        let h = read_request_header(&mut &frame[..]).unwrap();
        assert_eq!(h.version, 9, "the worker answers VersionMismatch on this");
    }

    #[test]
    fn response_round_trips_for_every_status() {
        for status in [
            Status::Ok,
            Status::Invalid,
            Status::RefusedScan,
            Status::OutOfMemory,
            Status::InternalError,
            Status::VersionMismatch,
            Status::LimitInstallFailed,
        ] {
            let resp = Response {
                status,
                vm_hwm_kib: 1234,
                cpu_ms: 99,
                reason: "because".into(),
            };
            let frame = encode_response(&resp);
            assert!(frame.len() <= MAX_RESPONSE_BYTES);
            assert_eq!(read_response(&mut &frame[..]).unwrap(), resp);
        }
    }

    #[test]
    fn long_reason_is_cut_on_a_char_boundary() {
        let resp = Response {
            status: Status::Invalid,
            vm_hwm_kib: 0,
            cpu_ms: 0,
            reason: "é".repeat(500),
        };
        let frame = encode_response(&resp);
        assert!(frame.len() <= MAX_RESPONSE_BYTES);
        let back = read_response(&mut &frame[..]).unwrap();
        assert!(back.reason.len() <= MAX_REASON_BYTES);
        assert!(back.reason.chars().all(|c| c == 'é'));
    }

    #[test]
    fn response_rejects_bad_frames() {
        let good = encode_response(&Response {
            status: Status::Ok,
            vm_hwm_kib: 1,
            cpu_ms: 2,
            reason: String::new(),
        });
        let mut bad = good.clone();
        bad[0] = 0;
        assert_eq!(read_response(&mut &bad[..]), Err(ProtocolError::BadMagic));
        let mut bad = good.clone();
        bad[4] = 7;
        assert_eq!(
            read_response(&mut &bad[..]),
            Err(ProtocolError::BadVersion(7))
        );
        let mut bad = good.clone();
        bad[6] = 200;
        assert_eq!(
            read_response(&mut &bad[..]),
            Err(ProtocolError::BadStatus(200))
        );
        let mut bad = good.clone();
        bad[23] = 0xff;
        bad[24] = 0xff;
        assert!(matches!(
            read_response(&mut &bad[..]),
            Err(ProtocolError::ReasonTooLong(_))
        ));
        assert_eq!(
            read_response(&mut &good[..good.len() - 1]),
            Err(ProtocolError::Truncated)
        );
    }
}
