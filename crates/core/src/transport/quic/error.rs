//! QUIC error kinds and application error codes (#5940, step e4).
//!
//! quinn reports failures through several error types (`ConnectionError`,
//! `ConnectError`, `WriteError`, `ReadError`), and the path and framing add
//! their own (`QuicSendError`, `HeaderError`). Everything that leaves the
//! QUIC code as a [`TransportError`] goes through a `From` impl here, which
//! maps it to one [`QuicErrorKind`]. Every match is exhaustive, so a new
//! quinn variant fails to compile here until it is given a kind.
//!
//! Application error codes. QUIC carries a code in three frames, and the
//! frame type says which namespace a code belongs to. Each namespace also
//! has its own numeric range, so a code is never ambiguous in a log line or
//! a counter, whichever frame carried it:
//!
//! | Range | Frame | Where |
//! |---|---|---|
//! | `0x00..=0x0f` | CONNECTION_CLOSE (application) | [`close_code`] |
//! | `0x10..=0x1f` | STOP_SENDING (receiver refuses a stream) | `path::stop_code` |
//! | `0x20..=0x2f` | RESET_STREAM (sender abandons a stream) | `path::reset_code` |
//!
//! All fit a one-byte QUIC varint (below 64). The ranges are checked at
//! compile time below.
//!
//! [`TransportError`]: crate::transport::TransportError

#[cfg(test)]
mod tests;

use super::framing::HeaderError;
use super::liveness::QuicAvailability;
use super::path::{QuicSendError, reset_code, stop_code};
use super::{QuicAllowError, QuicConnectError};
use crate::transport::metrics::QuicCloseCode;
use crate::transport::{QuicError, QuicErrorKind, QuicRefusal, TransportError};

/// Application error codes this node closes a QUIC connection with
/// (CONNECTION_CLOSE). The peer reads them as [`QuicErrorKind`]s through
/// [`QuicCloseCode::from_code`]. Range `0x00..=0x0f`.
pub(crate) mod close_code {
    /// The node dropped the peer: the connection is no longer wanted (the
    /// handle was dropped, or the node removed the peer).
    pub(crate) const NODE_DROPPED_PEER: u32 = 0x00;
    /// Downgrade: stop using QUIC with this node and fall back to the UDP
    /// session, which stays up. Sent when the QUIC path is broken on this
    /// side (its receive task ended); also reserved for e5's fallback.
    pub(crate) const DOWNGRADE: u32 = 0x01;
    /// The key the peer proved is not the one this node has on record for
    /// it (identity binding, e5).
    pub(crate) const IDENTITY_MISMATCH: u32 = 0x02;
    /// Every close code, for the range and duplicate checks. A source-scrape
    /// test checks that each `const` in this module is listed.
    pub(crate) const ALL: [u32; 3] = [NODE_DROPPED_PEER, DOWNGRADE, IDENTITY_MISMATCH];
}

/// The first code above each namespace's range.
const CLOSE_CODE_END: u32 = 0x10;
const STOP_CODE_END: u32 = 0x20;
const RESET_CODE_END: u32 = 0x30;

const _: () = {
    let close = close_code::ALL;
    let stop = stop_code::ALL;
    let reset = reset_code::ALL;
    // No code is used twice, within or across the namespaces.
    let all_len = close.len() + stop.len() + reset.len();
    let mut all = [0u32; 3 + 8 + 2];
    assert!(all.len() == all_len, "update the combined length");
    let mut n = 0;
    let mut i = 0;
    while i < close.len() {
        all[n] = close[i];
        n += 1;
        i += 1;
    }
    let mut i = 0;
    while i < stop.len() {
        all[n] = stop[i];
        n += 1;
        i += 1;
    }
    let mut i = 0;
    while i < reset.len() {
        all[n] = reset[i];
        n += 1;
        i += 1;
    }
    let mut i = 0;
    while i < all.len() {
        let mut j = i + 1;
        while j < all.len() {
            assert!(all[i] != all[j], "an application error code is used twice");
            j += 1;
        }
        i += 1;
    }
    let mut i = 0;
    while i < close.len() {
        assert!(close[i] < CLOSE_CODE_END, "close codes are 0x00..=0x0f");
        i += 1;
    }
    let mut i = 0;
    while i < stop.len() {
        assert!(
            stop[i] >= CLOSE_CODE_END && stop[i] < STOP_CODE_END,
            "stop codes are 0x10..=0x1f"
        );
        i += 1;
    }
    let mut i = 0;
    while i < reset.len() {
        assert!(
            reset[i] >= STOP_CODE_END && reset[i] < RESET_CODE_END,
            "reset codes are 0x20..=0x2f"
        );
        i += 1;
    }
};

impl QuicCloseCode {
    /// The code on the wire.
    pub(crate) const fn code(self) -> u32 {
        match self {
            Self::NodeDroppedPeer => close_code::NODE_DROPPED_PEER,
            Self::Downgrade => close_code::DOWNGRADE,
            Self::IdentityMismatch => close_code::IDENTITY_MISMATCH,
        }
    }

    /// The close code a peer sent, if this node knows it.
    pub(crate) fn from_code(code: u64) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|close| u64::from(close.code()) == code)
    }

    /// The reason phrase sent with the code (for the peer's logs only).
    pub(crate) const fn reason(self) -> &'static [u8] {
        match self {
            Self::NodeDroppedPeer => b"node dropped peer",
            Self::Downgrade => b"downgrade to udp",
            Self::IdentityMismatch => b"identity mismatch",
        }
    }
}

/// QUIC transport error codes 0x100 to 0x1ff carry a TLS alert (RFC 9001
/// section 4.8).
fn is_tls_alert(code: u64) -> bool {
    (0x100..0x200).contains(&code)
}

/// QUIC transport error codes (RFC 9000 section 20.1) this module tells
/// apart.
const NO_ERROR: u64 = 0x0;
const CONNECTION_REFUSED: u64 = 0x2;

/// A QUIC transport error code, sent by the peer or raised here: a TLS
/// alert is a refused handshake, `NO_ERROR` an ordinary close,
/// `CONNECTION_REFUSED` a refusal by the peer's stack, anything else a
/// protocol error. That includes a LOCAL `INTERNAL_ERROR`: quinn raises it
/// for an endpoint whose driver was dropped (quinn 0.11.12 connection.rs
/// line 1112), but quinn-proto 0.11.19 also raises it on a healthy
/// endpoint for a peer that leaves too many gaps in a stream or crypto
/// buffer (connection/streams/recv.rs line 83, connection/mod.rs line
/// 2134). Only the endpoint's availability tells the two apart, so the
/// endpoint case is attributed by [`attribute`], not here.
fn of_transport_code(code: u64) -> QuicErrorKind {
    if is_tls_alert(code) {
        return QuicErrorKind::Refused {
            reason: QuicRefusal::Handshake,
        };
    }
    match code {
        NO_ERROR => QuicErrorKind::ConnectionLost,
        CONNECTION_REFUSED => QuicErrorKind::Refused {
            reason: QuicRefusal::ConnectionRefused,
        },
        code => QuicErrorKind::Protocol { code },
    }
}

/// `kind`, attributed to the endpoint when its availability is `Down`:
/// every connection-scope kind then becomes `EndpointDown`, whichever quinn
/// or path error it came from (quinn ends an endpoint's connections with a
/// local INTERNAL_ERROR or, after its implicit close, LocallyClosed, and
/// either can surface through a read, a write or a send). Two kinds keep
/// their meaning: stream-scope kinds (they end one stream, not the
/// connection) and `Refused { IdentityMismatch }`, a security signal that
/// must stay visible. With any other availability `kind` is unchanged.
pub(in crate::transport) fn attribute(
    availability: QuicAvailability,
    kind: QuicErrorKind,
) -> QuicErrorKind {
    let down = match availability {
        QuicAvailability::Down => true,
        QuicAvailability::Disabled | QuicAvailability::Up | QuicAvailability::Released => false,
    };
    match kind {
        QuicErrorKind::Refused {
            reason: QuicRefusal::IdentityMismatch,
        }
        | QuicErrorKind::Reset { .. }
        | QuicErrorKind::Stopped { .. }
        | QuicErrorKind::InvalidFrame
        | QuicErrorKind::StreamClosed => kind,
        QuicErrorKind::ConnectionLost
        | QuicErrorKind::Refused {
            reason:
                QuicRefusal::Handshake
                | QuicRefusal::Version
                | QuicRefusal::ConnectionRefused
                | QuicRefusal::Downgrade
                | QuicRefusal::AllowSetFull
                | QuicRefusal::KeyConflict
                | QuicRefusal::UnusableKey
                | QuicRefusal::Local,
        }
        | QuicErrorKind::TimedOut
        | QuicErrorKind::EndpointDown
        | QuicErrorKind::Protocol { .. } => {
            if down {
                QuicErrorKind::EndpointDown
            } else {
                kind
            }
        }
    }
}

/// Whether a stream the peer STOPPED with `code` may succeed on a retry
/// (a new stream), like the UDP transport's `OutboundStreamFailed`. Not
/// retryable, because the same message fails the same way every time:
/// `HEADER_REFUSED`, `OVERLONG` and `FRAGMENT_REFUSED` (the receiver's
/// buffer refused a fragment of this shape). Every other stop code is
/// retryable: budget, channel-full, eviction, id-in-use, cancellation by
/// the consumer, and stop codes this node does not know.
pub(in crate::transport) fn stop_is_retryable(code: u64) -> bool {
    ![
        stop_code::HEADER_REFUSED,
        stop_code::OVERLONG,
        stop_code::FRAGMENT_REFUSED,
    ]
    .into_iter()
    .any(|deterministic| u64::from(deterministic) == code)
}

/// Whether a stream RESET with `code` may succeed on a retry. Not
/// retryable: `LENGTH_MISMATCH` (the source delivered a different length
/// than declared). Every other reset code is retryable, including
/// `SENDER_ABORTED` (the source failed) and reset codes this node does not
/// know. Classified by namespace: a reset carrying a value that is a
/// deterministic STOP code is still a retryable reset.
pub(in crate::transport) fn reset_is_retryable(code: u64) -> bool {
    code != u64::from(reset_code::LENGTH_MISMATCH)
}

impl From<quinn::ConnectionError> for QuicErrorKind {
    fn from(error: quinn::ConnectionError) -> Self {
        match error {
            quinn::ConnectionError::VersionMismatch => Self::Refused {
                reason: QuicRefusal::Version,
            },
            quinn::ConnectionError::TransportError(error) => {
                of_transport_code(u64::from(error.code))
            }
            quinn::ConnectionError::ConnectionClosed(close) => {
                of_transport_code(u64::from(close.error_code))
            }
            quinn::ConnectionError::ApplicationClosed(close) => {
                match QuicCloseCode::from_code(close.error_code.into_inner()) {
                    Some(QuicCloseCode::Downgrade) => Self::Refused {
                        reason: QuicRefusal::Downgrade,
                    },
                    Some(QuicCloseCode::IdentityMismatch) => Self::Refused {
                        reason: QuicRefusal::IdentityMismatch,
                    },
                    // A code a newer peer added still ends the connection.
                    Some(QuicCloseCode::NodeDroppedPeer) | None => Self::ConnectionLost,
                }
            }
            quinn::ConnectionError::Reset | quinn::ConnectionError::LocallyClosed => {
                Self::ConnectionLost
            }
            quinn::ConnectionError::TimedOut => Self::TimedOut,
            quinn::ConnectionError::CidsExhausted => Self::Refused {
                reason: QuicRefusal::Local,
            },
        }
    }
}

impl From<quinn::ConnectError> for QuicErrorKind {
    fn from(error: quinn::ConnectError) -> Self {
        match error {
            quinn::ConnectError::EndpointStopping => Self::EndpointDown,
            quinn::ConnectError::CidsExhausted
            | quinn::ConnectError::InvalidServerName(_)
            | quinn::ConnectError::InvalidRemoteAddress(_)
            | quinn::ConnectError::NoDefaultClientConfig
            | quinn::ConnectError::UnsupportedVersion => Self::Refused {
                reason: QuicRefusal::Local,
            },
        }
    }
}

impl From<quinn::WriteError> for QuicErrorKind {
    fn from(error: quinn::WriteError) -> Self {
        match error {
            quinn::WriteError::Stopped(code) => Self::Stopped {
                code: code.into_inner(),
            },
            quinn::WriteError::ConnectionLost(error) => error.into(),
            quinn::WriteError::ClosedStream | quinn::WriteError::ZeroRttRejected => {
                Self::StreamClosed
            }
        }
    }
}

impl From<quinn::ReadError> for QuicErrorKind {
    fn from(error: quinn::ReadError) -> Self {
        match error {
            quinn::ReadError::Reset(code) => Self::Reset {
                code: code.into_inner(),
            },
            quinn::ReadError::ConnectionLost(error) => error.into(),
            quinn::ReadError::ClosedStream
            | quinn::ReadError::IllegalOrderedRead
            | quinn::ReadError::ZeroRttRejected => Self::StreamClosed,
        }
    }
}

impl From<HeaderError> for QuicErrorKind {
    fn from(_: HeaderError) -> Self {
        Self::InvalidFrame
    }
}

impl From<QuicSendError> for QuicErrorKind {
    fn from(error: QuicSendError) -> Self {
        match error {
            QuicSendError::InvalidFrame(error) => error.into(),
            // The path reset the stream with these codes.
            QuicSendError::LengthMismatch { .. } => Self::Reset {
                code: u64::from(reset_code::LENGTH_MISMATCH),
            },
            QuicSendError::Source(_) => Self::Reset {
                code: u64::from(reset_code::SENDER_ABORTED),
            },
            QuicSendError::Stopped(code) => Self::Stopped { code },
            QuicSendError::ConnectionLost(error) => error.into(),
            QuicSendError::Closed => Self::StreamClosed,
        }
    }
}

impl From<QuicConnectError> for QuicErrorKind {
    fn from(error: QuicConnectError) -> Self {
        match error {
            QuicConnectError::AllowSetFull => Self::Refused {
                reason: QuicRefusal::AllowSetFull,
            },
            QuicConnectError::UnusableKey(_) => Self::Refused {
                reason: QuicRefusal::UnusableKey,
            },
            QuicConnectError::Tls(_) => Self::Refused {
                reason: QuicRefusal::Local,
            },
            QuicConnectError::Connect(error) => error.into(),
            QuicConnectError::Connection(error) => error.into(),
        }
    }
}

impl From<QuicAllowError> for QuicErrorKind {
    fn from(error: QuicAllowError) -> Self {
        let reason = match error {
            QuicAllowError::AllowSetFull => QuicRefusal::AllowSetFull,
            QuicAllowError::KeyConflict => QuicRefusal::KeyConflict,
            QuicAllowError::UnusableKey(_) => QuicRefusal::UnusableKey,
        };
        Self::Refused { reason }
    }
}

impl QuicError {
    /// Any error the QUIC code produces, mapped to its kind. Without the
    /// endpoint's availability: for a connection-scope error use
    /// `QuicEndpoint::error_kind`, which attributes it to a down endpoint.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::transport) fn of(error: impl Into<QuicErrorKind>) -> Self {
        QuicError(error.into())
    }

    /// The kind, for the transport's own handling (e5's fallback).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::transport) fn kind(&self) -> QuicErrorKind {
        self.0
    }
}

/// Any error the QUIC code produces, as a [`TransportError::Quic`]. Like
/// `QuicError::of`, unattributed: build connection-scope errors from
/// `QuicEndpoint::error_kind`.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::transport) fn transport_error(error: impl Into<QuicErrorKind>) -> TransportError {
    TransportError::from(error.into())
}
