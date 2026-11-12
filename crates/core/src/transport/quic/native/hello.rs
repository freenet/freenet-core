//! The `Hello` each side of a native connection sends first (#5940, design
//! v4 section 2.1): its protocol version, its capability bits and the address
//! it sees the other side at. It replaces what the UDP handshake's intro and
//! ack carry, since a native connection has no UDP handshake.
//!
//! The layout is FROZEN; a later field goes in the TLV tail, which a reader
//! skips when it does not know the type:
//!
//! ```text
//! magic "FNH1" (4) | total_len u16 BE (2, the whole Hello, magic included) |
//! protoc_version (8, the UDP handshake's version bytes) | caps u32 LE (4) |
//! observed address family u8 (1: 4 or 6) | address (4 or 16) | port u16 BE (2) |
//! TLV tail: (type u8, len u16 BE, value)* up to total_len
//! ```
//!
//! `total_len` comes right after the magic so a reader knows how much to read,
//! and can refuse more than [`HELLO_MAX`], from the first six bytes
//! ([`declared_len`]). Decoding refuses and never panics.
//!
//! A TLV type has no must-understand bit: every reader skips a type it does
//! not know. A field that an older reader must not ignore therefore cannot be
//! sent as a TLV to that reader; send it only to a peer whose version is known
//! to read it.
//!
//! The observed address is encoded in the family of the `SocketAddr` given.
//! The caller hands in an IPv4 peer's address as IPv4 (an IPv4-mapped IPv6
//! address normalized first, as the demux does), so the reader's counters
//! compare like with like.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The first four bytes of every `Hello`.
pub(crate) const HELLO_MAGIC: [u8; 4] = *b"FNH1";

/// The largest `Hello`, in bytes, magic included. A peer that declares more
/// is refused (`close_code::HELLO_REFUSED`).
///
/// Effectively frozen: every release that reads a `Hello` refuses one longer
/// than its own `HELLO_MAX`, so raising it lets a newer node send what older
/// ones refuse. A sender must stay within the smallest limit any peer it talks
/// to has.
pub(crate) const HELLO_MAX: usize = 1024;

/// Magic and `total_len`: what a reader needs to know how long the `Hello` is.
pub(crate) const HELLO_PREFIX_LEN: usize = 6;

/// Everything before the address: prefix, version, caps and family.
const BEFORE_ADDRESS: usize = HELLO_PREFIX_LEN + 8 + 4 + 1;

/// The shortest `Hello`: an IPv4 address and port, no TLVs.
const SHORTEST: usize = BEFORE_ADDRESS + 4 + 2;

const FAMILY_V4: u8 = 4;
const FAMILY_V6: u8 = 6;

/// A TLV's type and length.
const TLV_HEADER_LEN: usize = 3;

/// One side's `Hello`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Hello {
    /// The sender's 8 version bytes, in the UDP handshake's format
    /// (`version_cmp`), carried unchanged so both formats (and any later one)
    /// read as they do there.
    pub protoc_version: [u8; 8],
    /// Capability bits; none are defined yet, and a reader ignores the ones it
    /// does not know.
    pub caps: u32,
    /// The address the sender sees the receiver at. Feeds only counters and
    /// logs.
    pub observed: SocketAddr,
}

/// Why bytes are not a `Hello` this node accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HelloError {
    /// Fewer bytes than the prefix, or than the fixed fields need.
    #[error("too short for a Hello")]
    TooShort,
    /// The first four bytes are not [`HELLO_MAGIC`].
    #[error("not a Hello (bad magic)")]
    BadMagic,
    /// `total_len` is above [`HELLO_MAX`], or an encode would exceed it.
    #[error("Hello longer than {HELLO_MAX} bytes")]
    TooLong,
    /// `total_len` is below the fixed fields, or differs from the bytes read.
    #[error("Hello length does not match its total_len")]
    LengthMismatch,
    /// The address family is neither 4 nor 6.
    #[error("unknown address family in a Hello")]
    BadFamily,
    /// A TLV runs past `total_len`.
    #[error("truncated TLV in a Hello")]
    TruncatedTlv,
}

/// The `total_len` the first six bytes declare, refused when it is not a
/// `Hello` or above [`HELLO_MAX`]. A reader calls this before reading the
/// rest, so a peer cannot make it buffer more than [`HELLO_MAX`].
pub(crate) fn declared_len(prefix: &[u8; HELLO_PREFIX_LEN]) -> Result<usize, HelloError> {
    if prefix[..4] != HELLO_MAGIC {
        return Err(HelloError::BadMagic);
    }
    let total = usize::from(u16::from_be_bytes([prefix[4], prefix[5]]));
    if total > HELLO_MAX {
        return Err(HelloError::TooLong);
    }
    if total < SHORTEST {
        return Err(HelloError::LengthMismatch);
    }
    Ok(total)
}

impl Hello {
    /// The encoding, with no TLVs.
    pub(crate) fn encode(&self) -> Vec<u8> {
        // No TLVs and at most a 16-byte address: always within HELLO_MAX.
        self.encode_with_tlvs(&[])
            .unwrap_or_else(|_| unreachable!("a Hello without TLVs fits HELLO_MAX"))
    }

    /// The encoding with `tlvs` in the tail, refused if it would exceed
    /// [`HELLO_MAX`].
    pub(crate) fn encode_with_tlvs(&self, tlvs: &[(u8, &[u8])]) -> Result<Vec<u8>, HelloError> {
        let mut out = Vec::with_capacity(SHORTEST + 12);
        out.extend_from_slice(&HELLO_MAGIC);
        out.extend_from_slice(&[0, 0]); // total_len, set below
        out.extend_from_slice(&self.protoc_version);
        out.extend_from_slice(&self.caps.to_le_bytes());
        match self.observed.ip() {
            IpAddr::V4(ip) => {
                out.push(FAMILY_V4);
                out.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                out.push(FAMILY_V6);
                out.extend_from_slice(&ip.octets());
            }
        }
        out.extend_from_slice(&self.observed.port().to_be_bytes());
        for (kind, value) in tlvs {
            let len = u16::try_from(value.len()).map_err(|_| HelloError::TooLong)?;
            out.push(*kind);
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(value);
        }
        if out.len() > HELLO_MAX {
            return Err(HelloError::TooLong);
        }
        // Within HELLO_MAX (1024), so it fits a u16.
        let total = out.len() as u16;
        out[4..6].copy_from_slice(&total.to_be_bytes());
        Ok(out)
    }

    /// Decode a whole `Hello`: exactly `total_len` bytes. TLVs of any type
    /// are checked for length and skipped; none is defined yet.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, HelloError> {
        let prefix: &[u8; HELLO_PREFIX_LEN] = bytes
            .get(..HELLO_PREFIX_LEN)
            .and_then(|p| p.try_into().ok())
            .ok_or(HelloError::TooShort)?;
        let total = declared_len(prefix)?;
        if bytes.len() != total {
            return Err(HelloError::LengthMismatch);
        }
        let mut at = HELLO_PREFIX_LEN;
        let mut take = |n: usize| -> Result<&[u8], HelloError> {
            let field = bytes.get(at..at + n).ok_or(HelloError::TooShort)?;
            at += n;
            Ok(field)
        };
        let mut protoc_version = [0u8; 8];
        protoc_version.copy_from_slice(take(8)?);
        let caps = u32::from_le_bytes(take(4)?.try_into().map_err(|_| HelloError::TooShort)?);
        let ip = match take(1)?[0] {
            FAMILY_V4 => {
                let octets: [u8; 4] = take(4)?.try_into().map_err(|_| HelloError::TooShort)?;
                IpAddr::V4(Ipv4Addr::from(octets))
            }
            FAMILY_V6 => {
                let octets: [u8; 16] = take(16)?.try_into().map_err(|_| HelloError::TooShort)?;
                IpAddr::V6(Ipv6Addr::from(octets))
            }
            _ => return Err(HelloError::BadFamily),
        };
        let port = u16::from_be_bytes(take(2)?.try_into().map_err(|_| HelloError::TooShort)?);
        let mut rest = &bytes[at..];
        while !rest.is_empty() {
            let header = rest.get(..TLV_HEADER_LEN).ok_or(HelloError::TruncatedTlv)?;
            let len = usize::from(u16::from_be_bytes([header[1], header[2]]));
            rest = rest
                .get(TLV_HEADER_LEN + len..)
                .ok_or(HelloError::TruncatedTlv)?;
        }
        Ok(Self {
            protoc_version,
            caps,
            observed: SocketAddr::new(ip, port),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A format-1 version (0.2.157, floor 0.2.150), as the UDP handshake
    /// encodes it.
    const V1: [u8; 8] = [0xFF, 0, 2, 0, 157, 0, 150, 1];
    /// A format-2 version (#6189): 1.0.0 with floor 0.2.158, the bytes
    /// `version_cmp::tests::cross_series_bytes_are_frozen` pins.
    const V2: [u8; 8] = [0xFF, 1, 0, 0, 0, 0xC2, 0x9E, 2];

    fn v4() -> Hello {
        Hello {
            protoc_version: V1,
            caps: 0,
            observed: "203.0.113.7:31337".parse().unwrap(),
        }
    }

    /// The IPv4 form, byte for byte.
    #[test]
    fn an_ipv4_hello_has_these_bytes() {
        let bytes = v4().encode();
        let expected: Vec<u8> = [
            &b"FNH1"[..],
            &[0, 25],             // total_len
            &V1,                  // version
            &[0, 0, 0, 0],        // caps
            &[4, 203, 0, 113, 7], // family, address
            &[0x7A, 0x69],        // port 31337
        ]
        .concat();
        assert_eq!(bytes, expected);
        assert_eq!(Hello::decode(&bytes), Ok(v4()));
    }

    /// The IPv6 form with a format-2 version, capability bits and a TLV of a
    /// type no release defines, byte for byte. The TLV is skipped and the
    /// version bytes come through unchanged.
    #[test]
    fn an_ipv6_hello_with_a_format_2_version_and_a_tlv_has_these_bytes() {
        let hello = Hello {
            protoc_version: V2,
            caps: 0x0102_0304,
            observed: "[2001:db8::1]:443".parse().unwrap(),
        };
        let bytes = hello.encode_with_tlvs(&[(0x7F, &[0xAA, 0xBB])]).unwrap();
        let expected: Vec<u8> = [
            &b"FNH1"[..],
            &[0, 42],                  // total_len: 37 + 5
            &V2,                       // version, format 2
            &[0x04, 0x03, 0x02, 0x01], // caps, little-endian
            &[
                6, 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            ],
            &[0x01, 0xBB],             // port 443
            &[0x7F, 0, 2, 0xAA, 0xBB], // TLV
        ]
        .concat();
        assert_eq!(bytes, expected);
        assert_eq!(Hello::decode(&bytes), Ok(hello));
    }

    /// The prefix alone says how much to read, and refuses more than
    /// HELLO_MAX before anything else is read.
    #[test]
    fn the_prefix_declares_the_length() {
        let bytes = v4().encode();
        let prefix: [u8; HELLO_PREFIX_LEN] = bytes[..HELLO_PREFIX_LEN].try_into().unwrap();
        assert_eq!(declared_len(&prefix), Ok(25));
        let at_max = [b'F', b'N', b'H', b'1', 0x04, 0x00];
        assert_eq!(declared_len(&at_max), Ok(HELLO_MAX));
        let over = [b'F', b'N', b'H', b'1', 0x04, 0x01];
        assert_eq!(declared_len(&over), Err(HelloError::TooLong));
        let short = [b'F', b'N', b'H', b'1', 0, 24];
        assert_eq!(declared_len(&short), Err(HelloError::LengthMismatch));
        let foreign = [b'G', b'E', b'T', b' ', 0, 25];
        assert_eq!(declared_len(&foreign), Err(HelloError::BadMagic));
    }

    /// A Hello of exactly HELLO_MAX bytes encodes and decodes; one byte more
    /// is refused by the encoder.
    #[test]
    fn hello_max_is_the_limit_both_ways() {
        let fill = vec![0u8; HELLO_MAX - 25 - TLV_HEADER_LEN];
        let bytes = v4().encode_with_tlvs(&[(1, &fill)]).unwrap();
        assert_eq!(bytes.len(), HELLO_MAX);
        assert_eq!(Hello::decode(&bytes), Ok(v4()));
        let fill = vec![0u8; HELLO_MAX - 25 - TLV_HEADER_LEN + 1];
        assert_eq!(
            v4().encode_with_tlvs(&[(1, &fill)]),
            Err(HelloError::TooLong)
        );
    }

    /// Each malformed input is refused with its own error, never a panic.
    #[test]
    fn malformed_hellos_are_refused() {
        let good = v4().encode();
        let with = |edit: &dyn Fn(&mut Vec<u8>)| {
            let mut bytes = good.clone();
            edit(&mut bytes);
            Hello::decode(&bytes)
        };
        assert_eq!(Hello::decode(&[]), Err(HelloError::TooShort));
        assert_eq!(Hello::decode(&good[..5]), Err(HelloError::TooShort));
        assert_eq!(with(&|b| b[0] = b'X'), Err(HelloError::BadMagic));
        // total_len above HELLO_MAX.
        assert_eq!(with(&|b| b[4] = 0x05), Err(HelloError::TooLong));
        // total_len that disagrees with the bytes: one more, one less.
        assert_eq!(with(&|b| b.push(0)), Err(HelloError::LengthMismatch));
        assert_eq!(with(&|b| b[5] = 26), Err(HelloError::LengthMismatch));
        assert_eq!(
            with(&|b| {
                b.pop();
                b[5] = 24;
            }),
            Err(HelloError::LengthMismatch)
        );
        // An unknown family.
        assert_eq!(with(&|b| b[18] = 5), Err(HelloError::BadFamily));
        // Family 6 with only an IPv4 address's bytes.
        assert_eq!(with(&|b| b[18] = 6), Err(HelloError::TooShort));
        // A TLV whose length runs past the end, and a TLV header cut short.
        assert_eq!(
            with(&|b| {
                b.extend_from_slice(&[9, 0, 5, 1]);
                b[5] = 29;
            }),
            Err(HelloError::TruncatedTlv)
        );
        assert_eq!(
            with(&|b| {
                b.extend_from_slice(&[9, 0]);
                b[5] = 27;
            }),
            Err(HelloError::TruncatedTlv)
        );
    }

    /// Every truncation and extension of each golden frame (IPv4, IPv6 with a
    /// TLV, and one of exactly HELLO_MAX bytes) decodes or is refused, without
    /// a panic, and so does every single-byte change of it: at every length for
    /// the short frames, at its own length for the HELLO_MAX frame.
    #[test]
    fn decoding_never_panics_on_short_or_mutated_input() {
        let v6 = Hello {
            protoc_version: V2,
            caps: 0x0102_0304,
            observed: "[2001:db8::1]:443".parse().unwrap(),
        }
        .encode_with_tlvs(&[(0x7F, &[0xAA, 0xBB])])
        .unwrap();
        let fill = vec![0u8; HELLO_MAX - 25 - TLV_HEADER_LEN];
        let max = v4().encode_with_tlvs(&[(1, &fill)]).unwrap();
        let mutate = |bytes: &[u8]| {
            for i in 0..bytes.len() {
                for flip in [0x01u8, 0x80, 0xFF] {
                    let mut mutated = bytes.to_vec();
                    mutated[i] ^= flip;
                    drop(Hello::decode(&mutated));
                }
            }
        };
        for (good, every_length) in [(v4().encode(), true), (v6, true), (max, false)] {
            for len in 0..=good.len() + 4 {
                let mut bytes = good.clone();
                bytes.resize(len, 0xAA);
                drop(Hello::decode(&bytes));
                if every_length {
                    mutate(&bytes);
                }
            }
            mutate(&good);
        }
    }

    /// Several TLVs in a row, a zero-length one among them, are each skipped,
    /// byte for byte.
    #[test]
    fn several_tlvs_and_an_empty_one_are_skipped() {
        let bytes = v4()
            .encode_with_tlvs(&[(0x10, &[1, 2, 3]), (0x11, &[]), (0x12, &[4])])
            .unwrap();
        let expected: Vec<u8> = [
            &b"FNH1"[..],
            &[0, 38], // total_len: 25 + 6 + 3 + 4
            &V1,
            &[0, 0, 0, 0],
            &[4, 203, 0, 113, 7],
            &[0x7A, 0x69],
            &[0x10, 0, 3, 1, 2, 3], // TLV with three bytes
            &[0x11, 0, 0],          // zero-length TLV
            &[0x12, 0, 1, 4],       // TLV with one byte
        ]
        .concat();
        assert_eq!(bytes, expected);
        assert_eq!(Hello::decode(&bytes), Ok(v4()));
        // A zero-length TLV alone, last.
        let bytes = v4().encode_with_tlvs(&[(0x11, &[])]).unwrap();
        assert_eq!(bytes.len(), 28);
        assert_eq!(Hello::decode(&bytes), Ok(v4()));
    }
}
