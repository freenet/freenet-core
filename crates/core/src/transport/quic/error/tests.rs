//! Each quinn, framing and path error maps to the kind written here, one row
//! per variant. The rows are written out, not derived from the mapping, so
//! a changed mapping fails a row.

use bytes::Bytes;
use quinn::{ApplicationClose, ConnectionClose, ConnectionError, TransportErrorCode, VarInt};

use super::*;
use crate::transport::peer_connection::streaming::StreamError;
use crate::transport::{ErrorScope, StreamId};

fn app_close(code: u64) -> ConnectionError {
    ConnectionError::ApplicationClosed(ApplicationClose {
        error_code: VarInt::from_u64(code).unwrap(),
        reason: Bytes::from_static(b"x"),
    })
}

fn peer_abort(code: TransportErrorCode) -> ConnectionError {
    ConnectionError::ConnectionClosed(ConnectionClose {
        error_code: code,
        frame_type: None,
        reason: Bytes::new(),
    })
}

fn local_error(code: TransportErrorCode) -> ConnectionError {
    ConnectionError::TransportError(code.into())
}

fn refused(reason: QuicRefusal) -> QuicErrorKind {
    QuicErrorKind::Refused { reason }
}

/// Every `ConnectionError` variant, and each close code a peer can send.
#[test]
fn connection_errors_map_to_their_kinds() {
    let protocol_violation = u64::from(TransportErrorCode::PROTOCOL_VIOLATION);
    let cases: Vec<(&str, ConnectionError, QuicErrorKind)> = vec![
        (
            "version mismatch",
            ConnectionError::VersionMismatch,
            refused(QuicRefusal::Version),
        ),
        (
            "local TLS alert (bad certificate)",
            local_error(TransportErrorCode::crypto(42)),
            refused(QuicRefusal::Handshake),
        ),
        (
            "local TLS alert, lowest code",
            local_error(TransportErrorCode::crypto(0)),
            refused(QuicRefusal::Handshake),
        ),
        (
            "local TLS alert, highest code",
            local_error(TransportErrorCode::crypto(0xff)),
            refused(QuicRefusal::Handshake),
        ),
        (
            "local protocol violation",
            local_error(TransportErrorCode::PROTOCOL_VIOLATION),
            QuicErrorKind::Protocol {
                code: protocol_violation,
            },
        ),
        (
            "peer TLS alert (decrypt error)",
            peer_abort(TransportErrorCode::crypto(51)),
            refused(QuicRefusal::Handshake),
        ),
        (
            "peer stack closed without error",
            peer_abort(TransportErrorCode::NO_ERROR),
            QuicErrorKind::ConnectionLost,
        ),
        (
            "peer protocol violation",
            peer_abort(TransportErrorCode::PROTOCOL_VIOLATION),
            QuicErrorKind::Protocol {
                code: protocol_violation,
            },
        ),
        (
            "peer dropped this node (close code 0)",
            app_close(0),
            QuicErrorKind::ConnectionLost,
        ),
        (
            "peer asks to downgrade (close code 1)",
            app_close(1),
            refused(QuicRefusal::Downgrade),
        ),
        (
            "peer reports identity mismatch (close code 2)",
            app_close(2),
            refused(QuicRefusal::IdentityMismatch),
        ),
        (
            "unknown close code from a newer peer",
            app_close(0x0f),
            QuicErrorKind::ConnectionLost,
        ),
        (
            "a stop code used as a close code",
            app_close(u64::from(stop_code::BUDGET_REFUSED)),
            QuicErrorKind::ConnectionLost,
        ),
        (
            "stateless reset",
            ConnectionError::Reset,
            QuicErrorKind::ConnectionLost,
        ),
        (
            "idle timeout",
            ConnectionError::TimedOut,
            QuicErrorKind::TimedOut,
        ),
        (
            "closed here",
            ConnectionError::LocallyClosed,
            QuicErrorKind::ConnectionLost,
        ),
        (
            "connection IDs exhausted",
            ConnectionError::CidsExhausted,
            refused(QuicRefusal::Local),
        ),
    ];
    for (name, error, expected) in cases {
        assert_eq!(QuicErrorKind::from(error), expected, "{name}");
    }
}

/// Just past the TLS-alert range on either side is a protocol error, not a
/// refused handshake.
#[test]
fn only_the_tls_alert_range_is_a_refused_handshake() {
    for (code, alert) in [
        (0xff_u64, false),
        (0x100, true),
        (0x1ff, true),
        (0x200, false),
    ] {
        assert_eq!(is_tls_alert(code), alert, "{code:#x}");
        let expected = if alert {
            refused(QuicRefusal::Handshake)
        } else {
            QuicErrorKind::Protocol { code }
        };
        assert_eq!(of_transport_code(code), expected, "{code:#x}");
    }
}

#[test]
fn connect_errors_map_to_their_kinds() {
    let addr: std::net::SocketAddr = "192.0.2.1:1".parse().unwrap();
    let cases: Vec<(&str, quinn::ConnectError, QuicErrorKind)> = vec![
        (
            "endpoint stopping",
            quinn::ConnectError::EndpointStopping,
            QuicErrorKind::EndpointDown,
        ),
        (
            "CIDs exhausted",
            quinn::ConnectError::CidsExhausted,
            refused(QuicRefusal::Local),
        ),
        (
            "invalid server name",
            quinn::ConnectError::InvalidServerName("x".into()),
            refused(QuicRefusal::Local),
        ),
        (
            "invalid remote address",
            quinn::ConnectError::InvalidRemoteAddress(addr),
            refused(QuicRefusal::Local),
        ),
        (
            "no default client config",
            quinn::ConnectError::NoDefaultClientConfig,
            refused(QuicRefusal::Local),
        ),
        (
            "unsupported version",
            quinn::ConnectError::UnsupportedVersion,
            refused(QuicRefusal::Local),
        ),
    ];
    for (name, error, expected) in cases {
        assert_eq!(QuicErrorKind::from(error), expected, "{name}");
    }
}

#[test]
fn stream_errors_map_to_their_kinds() {
    let code = VarInt::from_u32(stop_code::CHANNEL_FULL);
    let writes: Vec<(&str, quinn::WriteError, QuicErrorKind)> = vec![
        (
            "stopped",
            quinn::WriteError::Stopped(code),
            QuicErrorKind::Stopped {
                code: u64::from(stop_code::CHANNEL_FULL),
            },
        ),
        (
            "connection lost while writing",
            quinn::WriteError::ConnectionLost(ConnectionError::TimedOut),
            QuicErrorKind::TimedOut,
        ),
        (
            "closed stream",
            quinn::WriteError::ClosedStream,
            QuicErrorKind::StreamClosed,
        ),
        (
            "0-RTT rejected",
            quinn::WriteError::ZeroRttRejected,
            QuicErrorKind::StreamClosed,
        ),
    ];
    for (name, error, expected) in writes {
        assert_eq!(QuicErrorKind::from(error), expected, "write: {name}");
    }
    let reset = VarInt::from_u32(reset_code::SENDER_ABORTED);
    let reads: Vec<(&str, quinn::ReadError, QuicErrorKind)> = vec![
        (
            "reset",
            quinn::ReadError::Reset(reset),
            QuicErrorKind::Reset {
                code: u64::from(reset_code::SENDER_ABORTED),
            },
        ),
        (
            "connection lost while reading",
            quinn::ReadError::ConnectionLost(app_close(1)),
            refused(QuicRefusal::Downgrade),
        ),
        (
            "closed stream",
            quinn::ReadError::ClosedStream,
            QuicErrorKind::StreamClosed,
        ),
        (
            "illegal ordered read",
            quinn::ReadError::IllegalOrderedRead,
            QuicErrorKind::StreamClosed,
        ),
        (
            "0-RTT rejected",
            quinn::ReadError::ZeroRttRejected,
            QuicErrorKind::StreamClosed,
        ),
    ];
    for (name, error, expected) in reads {
        assert_eq!(QuicErrorKind::from(error), expected, "read: {name}");
    }
}

/// The path's and framing's errors (e3).
#[test]
fn path_and_framing_errors_map_to_their_kinds() {
    let aborted = QuicErrorKind::Reset {
        code: u64::from(reset_code::SENDER_ABORTED),
    };
    let cases: Vec<(&str, QuicSendError, QuicErrorKind)> = vec![
        (
            "invalid frame",
            QuicSendError::InvalidFrame(HeaderError::NotCallerNamed(StreamId::next())),
            QuicErrorKind::InvalidFrame,
        ),
        (
            "length mismatch",
            QuicSendError::LengthMismatch {
                declared: 2,
                delivered: 1,
            },
            aborted,
        ),
        (
            "source failed",
            QuicSendError::Source(StreamError::InactivityTimeout),
            aborted,
        ),
        (
            "stopped",
            QuicSendError::Stopped(u64::from(stop_code::EVICTED)),
            QuicErrorKind::Stopped {
                code: u64::from(stop_code::EVICTED),
            },
        ),
        (
            "connection lost",
            QuicSendError::ConnectionLost(app_close(2)),
            refused(QuicRefusal::IdentityMismatch),
        ),
        ("closed", QuicSendError::Closed, QuicErrorKind::StreamClosed),
    ];
    for (name, error, expected) in cases {
        assert_eq!(QuicErrorKind::from(error), expected, "{name}");
    }
    assert_eq!(
        QuicErrorKind::from(HeaderError::Truncated(3)),
        QuicErrorKind::InvalidFrame
    );
}

/// The endpoint's own errors (`connect`, `allow_peer`).
#[test]
fn endpoint_errors_map_to_their_kinds() {
    let connects: Vec<(&str, QuicConnectError, QuicErrorKind)> = vec![
        (
            "allow-set full",
            QuicConnectError::AllowSetFull,
            refused(QuicRefusal::AllowSetFull),
        ),
        (
            "unusable key",
            QuicConnectError::UnusableKey(super::super::identity::UnusableKey),
            refused(QuicRefusal::UnusableKey),
        ),
        (
            "TLS configuration",
            QuicConnectError::Tls(super::super::config::TlsConfigError::Rustls(
                rustls::Error::General("x".into()),
            )),
            refused(QuicRefusal::Local),
        ),
        (
            "endpoint stopping",
            QuicConnectError::Connect(quinn::ConnectError::EndpointStopping),
            QuicErrorKind::EndpointDown,
        ),
        (
            "handshake refused",
            QuicConnectError::Connection(peer_abort(TransportErrorCode::crypto(42))),
            refused(QuicRefusal::Handshake),
        ),
    ];
    for (name, error, expected) in connects {
        assert_eq!(QuicErrorKind::from(error), expected, "connect: {name}");
    }
    let allows: Vec<(&str, QuicAllowError, QuicErrorKind)> = vec![
        (
            "allow-set full",
            QuicAllowError::AllowSetFull,
            refused(QuicRefusal::AllowSetFull),
        ),
        (
            "key conflict",
            QuicAllowError::KeyConflict,
            refused(QuicRefusal::KeyConflict),
        ),
        (
            "unusable key",
            QuicAllowError::UnusableKey(super::super::identity::UnusableKey),
            refused(QuicRefusal::UnusableKey),
        ),
    ];
    for (name, error, expected) in allows {
        assert_eq!(QuicErrorKind::from(error), expected, "allow: {name}");
    }
}

/// Stream kinds keep the connection; the rest end it. Written out per kind,
/// independently of `TransportError::scope`.
#[test]
fn stream_kinds_keep_the_connection_and_the_rest_end_it() {
    let cases = [
        (QuicErrorKind::ConnectionLost, ErrorScope::Connection),
        (QuicErrorKind::Reset { code: 0x21 }, ErrorScope::Stream),
        (QuicErrorKind::Stopped { code: 0x12 }, ErrorScope::Stream),
        (refused(QuicRefusal::Downgrade), ErrorScope::Connection),
        (QuicErrorKind::TimedOut, ErrorScope::Connection),
        (QuicErrorKind::EndpointDown, ErrorScope::Connection),
        (
            QuicErrorKind::Protocol { code: 0xa },
            ErrorScope::Connection,
        ),
        (QuicErrorKind::InvalidFrame, ErrorScope::Stream),
        (QuicErrorKind::StreamClosed, ErrorScope::Stream),
    ];
    for (kind, scope) in cases {
        let error = transport_error(kind);
        assert_eq!(error.scope(), scope, "{kind:?}");
        assert!(matches!(error, TransportError::Quic(e) if e.kind() == kind));
        assert_eq!(QuicError::of(kind).kind(), kind);
    }
}

/// Each close code goes out as its fixed wire value, comes back as the same
/// code, and nothing else maps to one. The values are written out: a peer
/// running another release reads them.
#[test]
fn close_codes_have_fixed_distinct_wire_values() {
    let expected = [
        (QuicCloseCode::NodeDroppedPeer, 0),
        (QuicCloseCode::Downgrade, 1),
        (QuicCloseCode::IdentityMismatch, 2),
    ];
    assert_eq!(expected.len(), QuicCloseCode::COUNT);
    for (code, wire) in expected {
        assert_eq!(code.code(), wire, "{code:?}");
        assert_eq!(QuicCloseCode::from_code(u64::from(wire)), Some(code));
        assert!(!code.reason().is_empty());
    }
    for unknown in [3_u64, 0x0f, 0x11, 0x21, u64::from(u32::MAX)] {
        assert_eq!(QuicCloseCode::from_code(unknown), None, "{unknown:#x}");
    }
}
