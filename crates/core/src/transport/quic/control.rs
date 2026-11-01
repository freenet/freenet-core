//! The payload of a QUIC path control frame (#5940).
//!
//! `Ready` and `ReadyAck` finish an upgrade on a new QUIC connection, each as
//! a `Control` frame ([`super::framing`]) on its own uni stream. Control
//! frames take no streamed-message permit, so on a steady path
//! (`config::STREAMED_SEND_LIMIT`) streamed messages do not keep them waiting
//! for a stream slot.
//!
//! ```text
//! offset  size  field
//! 0       1     version   CONTROL_VERSION (1)
//! 1       1     type      0 Ready, 1 ReadyAck
//! 2       4     attempt   big-endian u32, the upgrade attempt
//! 6       4     caps      big-endian u32, capability bits
//! ```
//!
//! [`CONTROL_LEN`] (10) bytes, exactly. `attempt` is the `attempt` of the
//! `UpgradeControl` Offer this connection was made for, so a frame from an
//! earlier attempt can be told apart. `caps` carries what the sender
//! supports on this path. [`caps::DATA`] is the only bit: a path carries data
//! only if both sides set it (`upgrade::machine`).
//!
//! Compatibility rules:
//! - Unknown caps bits are ignored: [`QuicControl::decode`] clears every bit
//!   not in [`caps::KNOWN`], so nothing on this node can act on a bit it
//!   does not know. A new bit is how a newer node says it supports more.
//! - An unknown version, an unknown type, or a length other than
//!   [`CONTROL_LEN`] is refused ([`ControlError`]), never a panic. The
//!   receiving path counts each, only through `QuicPath::decode_control`
//!   ([`QuicControl::decode`] counts nothing). The sender's send reports
//!   `Sent`. During path setup, an unknown version or type also makes the
//!   receiver close the path with `close_code::CONTROL_REFUSED`, which the
//!   sender reads as a downgrade (`upgrade::machine`); otherwise the sender
//!   learns nothing.
//! - A new type or field needs a new version. Bytes past the end are refused,
//!   so a field cannot be appended silently. `UpgradeControl` is different:
//!   bincode tolerates trailing bytes there, so a field appended to it would
//!   be ignored and it grows by caps-gated variants instead.
//! - A sender cannot learn from these frames that the peer reads a new
//!   version, because `caps` travels in `Ready` and `ReadyAck` themselves.
//!   `Ready` stays at version 1 until the sender knows by another route: the
//!   caps of the `UpgradeControl::Offer` it received (the client side only),
//!   or a release floor on the peer's version, as `may_send_upgrade` is.

/// The control payload version this node writes and the only one it reads.
pub(crate) const CONTROL_VERSION: u8 = 1;

/// Length of a version 1 control payload in bytes.
pub(crate) const CONTROL_LEN: usize = 10;

/// Capability bits carried in [`QuicControl`].
pub(crate) mod caps {
    /// The sender reads application data on this path, and may send it to a
    /// peer that sets this bit too. Each side sets it from its own release's
    /// share of pairs (`upgrade::slice`). A node before this bit clears it on
    /// decode and sends 0, so the pair stays in shadow.
    pub(crate) const DATA: u32 = 1;
    /// Every bit this node knows.
    pub(crate) const KNOWN: u32 = DATA;
}

/// A control message on a QUIC path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum QuicControl {
    /// The sender's path is up and reads control frames.
    Ready { attempt: u32, caps: u32 },
    /// The reply to `Ready`.
    ReadyAck { attempt: u32, caps: u32 },
}

/// Why a control payload was refused. Each has its own counter on the
/// receiving path (`QuicInboundOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ControlError {
    /// No bytes at all. Not reachable from a real stream: the frame header
    /// refuses a control length of 0.
    #[error("empty control payload")]
    Empty,
    /// A version this node does not read.
    #[error("unknown control version {0}")]
    UnknownVersion(u8),
    /// A type byte this node does not know.
    #[error("unknown control type {0}")]
    UnknownType(u8),
    /// A version 1 payload that is not [`CONTROL_LEN`] bytes long.
    #[error("control payload of {0} bytes, expected {CONTROL_LEN}")]
    WrongLength(usize),
}

const TYPE_READY: u8 = 0;
const TYPE_READY_ACK: u8 = 1;

#[cfg_attr(not(test), allow(dead_code))]
impl QuicControl {
    /// The attempt the frame belongs to.
    pub(crate) fn attempt(&self) -> u32 {
        match *self {
            Self::Ready { attempt, .. } | Self::ReadyAck { attempt, .. } => attempt,
        }
    }

    /// The capability bits, only those in [`caps::KNOWN`] once decoded.
    pub(crate) fn caps(&self) -> u32 {
        match *self {
            Self::Ready { caps, .. } | Self::ReadyAck { caps, .. } => caps,
        }
    }

    /// The payload's bytes.
    pub(crate) fn encode(&self) -> [u8; CONTROL_LEN] {
        let (kind, attempt, caps) = match *self {
            Self::Ready { attempt, caps } => (TYPE_READY, attempt, caps),
            Self::ReadyAck { attempt, caps } => (TYPE_READY_ACK, attempt, caps),
        };
        let mut out = [0u8; CONTROL_LEN];
        out[0] = CONTROL_VERSION;
        out[1] = kind;
        out[2..6].copy_from_slice(&attempt.to_be_bytes());
        out[6..10].copy_from_slice(&caps.to_be_bytes());
        out
    }

    /// Parse a payload. The version is checked first, so a payload of a
    /// later version is `UnknownVersion` whatever its length.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        let Some(&version) = bytes.first() else {
            return Err(ControlError::Empty);
        };
        if version != CONTROL_VERSION {
            return Err(ControlError::UnknownVersion(version));
        }
        let Ok(bytes) = <&[u8; CONTROL_LEN]>::try_from(bytes) else {
            return Err(ControlError::WrongLength(bytes.len()));
        };
        let mut attempt = [0u8; 4];
        attempt.copy_from_slice(&bytes[2..6]);
        let attempt = u32::from_be_bytes(attempt);
        let mut caps = [0u8; 4];
        caps.copy_from_slice(&bytes[6..10]);
        let caps = u32::from_be_bytes(caps) & caps::KNOWN;
        match bytes[1] {
            TYPE_READY => Ok(Self::Ready { attempt, caps }),
            TYPE_READY_ACK => Ok(Self::ReadyAck { attempt, caps }),
            other => Err(ControlError::UnknownType(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::framing::MAX_CONTROL_BYTES;
    use super::*;

    /// Both types round trip, with the extreme attempt values; the bytes
    /// are the documented layout.
    #[test]
    fn control_frames_round_trip() {
        for control in [
            QuicControl::Ready {
                attempt: 0,
                caps: 0,
            },
            QuicControl::Ready {
                attempt: u32::MAX,
                caps: caps::KNOWN,
            },
            QuicControl::ReadyAck {
                attempt: 0x0102_0304,
                caps: 0,
            },
            QuicControl::ReadyAck {
                attempt: u32::MAX,
                caps: caps::KNOWN,
            },
        ] {
            let bytes = control.encode();
            assert_eq!(QuicControl::decode(&bytes), Ok(control));
        }
        assert_eq!(
            QuicControl::ReadyAck {
                attempt: 0x0102_0304,
                caps: 0,
            }
            .encode(),
            [1, 1, 1, 2, 3, 4, 0, 0, 0, 0]
        );
        assert_eq!(
            QuicControl::Ready {
                attempt: 7,
                caps: 0
            }
            .encode(),
            [1, 0, 0, 0, 0, 7, 0, 0, 0, 0]
        );
        assert_eq!(
            QuicControl::Ready {
                attempt: 7,
                caps: caps::DATA,
            }
            .encode(),
            [1, 0, 0, 0, 0, 7, 0, 0, 0, 1],
            "the data bit is the low bit of the field"
        );
        const { assert!(CONTROL_LEN as u64 <= MAX_CONTROL_BYTES) };
    }

    /// A newer node may set caps bits this node does not know: the frame
    /// still decodes, with those bits cleared and the known one kept.
    #[test]
    fn unknown_caps_bits_are_ignored() {
        for (kind, raw_caps, kept) in [
            (0u8, u32::MAX, caps::DATA),
            (1, 0x8000_0001, caps::DATA),
            (0, 1, caps::DATA),
            (1, 0x8000_0000, 0),
            (0, 2, 0),
            (1, 0, 0),
        ] {
            let mut bytes = [1, kind, 0, 0, 0, 9, 0, 0, 0, 0];
            bytes[6..10].copy_from_slice(&raw_caps.to_be_bytes());
            let control = QuicControl::decode(&bytes).expect("decodes");
            assert_eq!(control.attempt(), 9);
            assert_eq!(control.caps(), kept, "raw {raw_caps:#x}");
        }
    }

    /// An unknown version is refused as such at any length (a later version
    /// may be longer or shorter); so are an unknown type and a version 1
    /// payload of the wrong length, including bytes appended at the end.
    #[test]
    fn unknown_versions_types_and_lengths_are_refused() {
        let ready = QuicControl::Ready {
            attempt: 1,
            caps: 0,
        }
        .encode();
        for version in [0u8, 2, 0xff] {
            for len in [1, CONTROL_LEN, CONTROL_LEN + 6, MAX_CONTROL_BYTES as usize] {
                let mut bytes = vec![0u8; len];
                bytes[0] = version;
                assert_eq!(
                    QuicControl::decode(&bytes),
                    Err(ControlError::UnknownVersion(version)),
                    "version {version}, {len} bytes"
                );
            }
        }
        for kind in [2u8, 0x7f, 0xff] {
            let mut bytes = ready;
            bytes[1] = kind;
            assert_eq!(
                QuicControl::decode(&bytes),
                Err(ControlError::UnknownType(kind))
            );
        }
        assert_eq!(QuicControl::decode(&[]), Err(ControlError::Empty));
        for len in [1, 2, CONTROL_LEN - 1] {
            assert_eq!(
                QuicControl::decode(&ready[..len]),
                Err(ControlError::WrongLength(len))
            );
        }
        let mut longer = ready.to_vec();
        longer.push(0);
        assert_eq!(
            QuicControl::decode(&longer),
            Err(ControlError::WrongLength(CONTROL_LEN + 1))
        );
    }
}
