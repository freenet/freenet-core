use crate::config::{MIN_COMPATIBLE_VERSION, PCK_VERSION};

/// New-format marker byte. Old peers see major=0xFF (!=their major) → clean reject.
const NEW_FORMAT_MARKER: u8 = 0xFF;
/// Format version byte in position 7. Old peers see reserved!=0 → clean reject.
const FORMAT_VERSION: u8 = 1;

/// Our version in the new 8-byte wire format.
///
/// Layout: `[0xFF, major, minor, patch_hi, patch_lo, min_patch_hi, min_patch_lo, format=1]`
///
/// Old peers (0.1.152 and earlier) used: `[major, minor, patch_u8, flags(4), reserved=0]`
/// They see byte[0]=0xFF as major mismatch → clean ack_error. No crypto confusion.
pub(super) const PROTOC_VERSION: [u8; 8] = encode_new_format(PCK_VERSION, MIN_COMPATIBLE_VERSION);

/// Parsed version info from an 8-byte wire version field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VersionInfo {
    pub version: (u8, u8, u16),
    /// None for old-format peers (they don't advertise min_compatible).
    pub min_compatible: Option<(u8, u8, u16)>,
}

impl std::fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (maj, min, pat) = self.version;
        write!(f, "{maj}.{min}.{pat}")
    }
}

/// Encode version + min_compatible into the new 8-byte format.
const fn encode_new_format(version: &str, min_compatible: &str) -> [u8; 8] {
    let (major, minor, patch) = parse_semver(version);
    let (_min_maj, _min_min, min_patch) = parse_semver(min_compatible);
    [
        NEW_FORMAT_MARKER,
        major,
        minor,
        (patch >> 8) as u8,
        patch as u8,
        (min_patch >> 8) as u8,
        min_patch as u8,
        FORMAT_VERSION,
    ]
}

/// Parse "X.Y.Z" or "X.Y.Z-prerelease" into (major, minor, patch as u16).
const fn parse_semver(version: &str) -> (u8, u8, u16) {
    let mut major = 0u8;
    let mut minor = 0u8;
    let mut patch = 0u16;
    let mut state = 0u8; // 0: major, 1: minor, 2: patch

    let bytes = version.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'.' {
            state += 1;
        } else if c >= b'0' && c <= b'9' {
            let digit = (c - b'0') as u16;
            match state {
                0 => major = (major as u16 * 10 + digit) as u8,
                1 => minor = (minor as u16 * 10 + digit) as u8,
                2 => patch = patch * 10 + digit,
                _ => {}
            }
        } else {
            break; // Pre-release suffix — ignored for wire format.
        }
        i += 1;
    }
    (major, minor, patch)
}

/// Parse 8 version bytes. Detects old (byte[7]==0) vs new (byte[0]==0xFF, byte[7]!=0).
pub(super) fn parse_version_bytes(bytes: &[u8; 8]) -> VersionInfo {
    if bytes[0] == NEW_FORMAT_MARKER && bytes[7] != 0 {
        // New format
        let major = bytes[1];
        let minor = bytes[2];
        let patch = u16::from_be_bytes([bytes[3], bytes[4]]);
        let min_patch = u16::from_be_bytes([bytes[5], bytes[6]]);
        VersionInfo {
            version: (major, minor, patch),
            min_compatible: Some((major, minor, min_patch)),
        }
    } else {
        // Old format: [major, minor, patch_u8, flags(4), reserved=0]
        let major = bytes[0];
        let minor = bytes[1];
        let patch = bytes[2] as u16;
        VersionInfo {
            version: (major, minor, patch),
            min_compatible: None,
        }
    }
}

/// Version tuple comparison: returns true if a >= b.
/// Uses lexicographic tuple ordering, which matches semver precedence.
pub(super) fn ver_ge(a: (u8, u8, u16), b: (u8, u8, u16)) -> bool {
    a >= b
}

/// Returns true if the remote's min_compatible exceeds our local version,
/// meaning we MUST update to connect to this peer.
pub(super) fn remote_requires_newer_than_us(local: &[u8; 8], remote_bytes: &[u8; 8]) -> bool {
    let remote_info = parse_version_bytes(remote_bytes);
    let local_info = parse_version_bytes(local);
    remote_info
        .min_compatible
        .is_some_and(|min| !ver_ge(local_info.version, min))
}

/// Bidirectional compatibility check.
///
/// - Old remote (no min_compatible): check `remote_ver >= local_min_compatible`
/// - New remote: check both directions
pub(super) fn is_compatible(local: &[u8; 8], remote: &[u8; 8]) -> Result<VersionInfo, String> {
    let local_info = parse_version_bytes(local);
    let remote_info = parse_version_bytes(remote);

    // Check: remote version >= our min_compatible
    if let Some(local_min) = local_info.min_compatible {
        if !ver_ge(remote_info.version, local_min) {
            return Err(format!(
                "remote {} too old for our min_compatible {}.{}.{}",
                remote_info, local_min.0, local_min.1, local_min.2,
            ));
        }
    }

    // Check: our version >= remote's min_compatible (if remote is new-format)
    if let Some(remote_min) = remote_info.min_compatible {
        if !ver_ge(local_info.version, remote_min) {
            return Err(format!(
                "our {} too old for remote's min_compatible {}.{}.{}",
                local_info, remote_min.0, remote_min.1, remote_min.2,
            ));
        }
    }

    Ok(remote_info)
}

/// Check if a version mismatch from an old peer means WE need to update.
///
/// When an old peer rejects us (they do strict exact-match), parse their
/// version from the error. If their version >= our min_compatible, they're
/// compatible with us — they just don't know it. We don't need to update.
pub(super) fn old_peer_rejection_means_we_must_update(
    remote_version_str: &str,
    local: &[u8; 8],
) -> bool {
    let local_info = parse_version_bytes(local);
    let (rmaj, rmin, rpatch) = parse_semver(remote_version_str);

    if let Some(local_min) = local_info.min_compatible {
        // If remote < our min, we don't care about their rejection
        // (they're too old for us anyway)
        if !ver_ge((rmaj, rmin, rpatch), local_min) {
            return false;
        }
    }

    // Remote is compatible with us. Check if WE are compatible with them.
    // Old peers have strict matching, so their "min_compatible" is effectively
    // their exact version. If our version < their version, we may need to update.
    // But actually, an old peer rejects anyone != their exact version.
    // Since we're new-format and they're old-format, they'll always reject us.
    // The question is: should we update? Only if they're NEWER than us.
    !ver_ge(local_info.version, (rmaj, rmin, rpatch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_semver() {
        assert_eq!(parse_semver("0.1.152"), (0, 1, 152));
        assert_eq!(parse_semver("0.1.256"), (0, 1, 256));
        assert_eq!(parse_semver("0.1.65535"), (0, 1, 65535));
        assert_eq!(parse_semver("1.2.3-alpha"), (1, 2, 3));
        assert_eq!(parse_semver("10.20.300"), (10, 20, 300));
    }

    #[test]
    fn test_new_format_roundtrip() {
        let encoded = encode_new_format("0.1.153", "0.1.152");
        let info = parse_version_bytes(&encoded);
        assert_eq!(info.version, (0, 1, 153));
        assert_eq!(info.min_compatible, Some((0, 1, 152)));
    }

    #[test]
    fn test_old_format_parsing() {
        // Old format: [major=0, minor=1, patch=152, flags(4), reserved=0]
        let old = [0u8, 1, 152, 0, 0, 0, 0, 0];
        let info = parse_version_bytes(&old);
        assert_eq!(info.version, (0, 1, 152));
        assert_eq!(info.min_compatible, None);
    }

    #[test]
    fn test_old_format_with_flags() {
        // Old format with pre-release flags: still old format because byte[7]=0
        let old = [0u8, 1, 152, 0x12, 0x34, 0x56, 0x78, 0];
        let info = parse_version_bytes(&old);
        assert_eq!(info.version, (0, 1, 152));
        assert_eq!(info.min_compatible, None);
    }

    #[test]
    fn test_new_accepts_old_compatible() {
        // New peer (0.1.153, min=0.1.152) accepts old peer (0.1.152)
        let new = encode_new_format("0.1.153", "0.1.152");
        let old = [0u8, 1, 152, 0, 0, 0, 0, 0];
        let result = is_compatible(&new, &old);
        assert!(result.is_ok(), "new should accept compatible old peer");
        assert_eq!(result.unwrap().version, (0, 1, 152));
    }

    #[test]
    fn test_new_rejects_old_incompatible() {
        // New peer (0.1.155, min=0.1.153) rejects old peer (0.1.152)
        let new = encode_new_format("0.1.155", "0.1.153");
        let old = [0u8, 1, 152, 0, 0, 0, 0, 0];
        let result = is_compatible(&new, &old);
        assert!(result.is_err(), "new should reject incompatible old peer");
    }

    #[test]
    fn test_new_to_new_bidirectional() {
        // Both new format, compatible
        let a = encode_new_format("0.1.155", "0.1.153");
        let b = encode_new_format("0.1.153", "0.1.152");
        assert!(is_compatible(&a, &b).is_ok());
        assert!(is_compatible(&b, &a).is_ok());
    }

    #[test]
    fn test_new_to_new_asymmetric_reject() {
        // A requires min 0.1.154, B is 0.1.153 → bidirectional check fails both ways
        // because B (0.1.153) < A's min (0.1.154)
        let a = encode_new_format("0.1.155", "0.1.154");
        let b = encode_new_format("0.1.153", "0.1.150");
        assert!(
            is_compatible(&a, &b).is_err(),
            "A should reject B (too old)"
        );
        assert!(
            is_compatible(&b, &a).is_err(),
            "B should also reject A (B knows it's too old for A's min)"
        );

        // One-way compatible: A requires min 0.1.152, B is 0.1.153 → both accept
        // B requires min 0.1.150, A is 0.1.155 → both accept
        let a2 = encode_new_format("0.1.155", "0.1.152");
        let b2 = encode_new_format("0.1.153", "0.1.150");
        assert!(is_compatible(&a2, &b2).is_ok(), "A2 should accept B2");
        assert!(is_compatible(&b2, &a2).is_ok(), "B2 should accept A2");
    }

    #[test]
    fn test_patch_256_and_above() {
        let v = encode_new_format("0.1.256", "0.1.200");
        let info = parse_version_bytes(&v);
        assert_eq!(info.version, (0, 1, 256));
        assert_eq!(info.min_compatible, Some((0, 1, 200)));

        let v2 = encode_new_format("0.1.65535", "0.1.1000");
        let info2 = parse_version_bytes(&v2);
        assert_eq!(info2.version, (0, 1, 65535));
        assert_eq!(info2.min_compatible, Some((0, 1, 1000)));
    }

    #[test]
    fn test_old_peer_rejection_analysis() {
        let local = encode_new_format("0.1.153", "0.1.152");

        // Old peer 0.1.152 rejects us — but 0.1.152 >= our min 0.1.152,
        // and 0.1.152 < 0.1.153 (our version), so we are newer. No update needed.
        assert!(
            !old_peer_rejection_means_we_must_update("0.1.152", &local),
            "should not trigger update when we are newer"
        );

        // Old peer 0.1.154 rejects us — they're newer. We should update.
        assert!(
            old_peer_rejection_means_we_must_update("0.1.154", &local),
            "should trigger update when remote is newer"
        );

        // Old peer 0.1.153 rejects us — same version. No update needed.
        assert!(
            !old_peer_rejection_means_we_must_update("0.1.153", &local),
            "should not trigger update for same version"
        );
    }

    #[test]
    fn test_protoc_version_is_new_format() {
        // Verify our compiled PROTOC_VERSION uses the new format
        assert_eq!(
            PROTOC_VERSION[0], NEW_FORMAT_MARKER,
            "byte 0 must be 0xFF marker"
        );
        assert_eq!(
            PROTOC_VERSION[7], FORMAT_VERSION,
            "byte 7 must be format version"
        );
    }

    #[test]
    fn test_remote_requires_newer_than_us() {
        // Remote requires min 0.1.154, we are 0.1.153 → we are too old
        let local = encode_new_format("0.1.153", "0.1.152");
        let remote = encode_new_format("0.1.155", "0.1.154");
        assert!(remote_requires_newer_than_us(&local, &remote));

        // Remote requires min 0.1.152, we are 0.1.153 → we are fine
        let remote2 = encode_new_format("0.1.155", "0.1.152");
        assert!(!remote_requires_newer_than_us(&local, &remote2));

        // Old-format remote (no min_compatible) → never triggers urgent
        let old_remote = [0u8, 1, 155, 0, 0, 0, 0, 0];
        assert!(!remote_requires_newer_than_us(&local, &old_remote));
    }

    #[test]
    fn test_ver_ge() {
        assert!(ver_ge((0, 1, 153), (0, 1, 152)));
        assert!(ver_ge((0, 1, 152), (0, 1, 152)));
        assert!(!ver_ge((0, 1, 151), (0, 1, 152)));
        assert!(ver_ge((0, 2, 0), (0, 1, 999)));
        assert!(ver_ge((1, 0, 0), (0, 255, 65535)));
    }

    /// Wire format only encodes min_patch, inheriting major.minor from the version.
    /// This is a deliberate constraint: during the 0.x era, all releases share
    /// major=0, minor=1. When major/minor changes, bump the format byte.
    #[test]
    fn test_min_compatible_shares_major_minor_with_version() {
        // min_compatible always gets the same major.minor as the version
        let v = encode_new_format("0.1.200", "0.1.150");
        let info = parse_version_bytes(&v);
        assert_eq!(info.version, (0, 1, 200));
        assert_eq!(info.min_compatible, Some((0, 1, 150)));

        // Even if min_compatible had a different major.minor in the source string,
        // only the patch is preserved (major.minor inherited from version).
        let v2 = encode_new_format("1.0.5", "0.1.3");
        let info2 = parse_version_bytes(&v2);
        assert_eq!(info2.version, (1, 0, 5));
        // min_compatible inherits (1, 0, _) from version, patch=3 from min_compat
        assert_eq!(info2.min_compatible, Some((1, 0, 3)));
    }

    /// Old peer well below our min_compatible should not trigger our self-update.
    #[test]
    fn test_old_peer_rejection_below_min_compatible() {
        let local = encode_new_format("0.1.153", "0.1.152");

        // Old peer 0.1.100 is WAY below our min_compatible (0.1.152).
        // Their rejection doesn't matter — they're too old for us anyway.
        assert!(
            !old_peer_rejection_means_we_must_update("0.1.100", &local),
            "should not trigger update for peer below our min_compatible"
        );
    }

    /// parse_semver with adversarial/malformed input.
    #[test]
    fn test_parse_semver_malformed() {
        // Empty string
        assert_eq!(parse_semver(""), (0, 0, 0));
        // Missing patch
        assert_eq!(parse_semver("0.1"), (0, 1, 0));
        // Non-numeric
        assert_eq!(parse_semver("abc"), (0, 0, 0));
        // Extra dots
        assert_eq!(parse_semver("0.1.2.3"), (0, 1, 2));
        // Leading zeros (parsed as decimal digits)
        assert_eq!(parse_semver("0.1.007"), (0, 1, 7));
    }

    /// Old-format always has byte[7]==0. New format always has byte[7]!=0.
    #[test]
    fn test_format_detection_byte7() {
        // Old format: byte[7] = 0
        let old = [0u8, 1, 152, 0, 0, 0, 0, 0];
        assert!(parse_version_bytes(&old).min_compatible.is_none());

        // New format: byte[7] = FORMAT_VERSION (1)
        let new = encode_new_format("0.1.153", "0.1.152");
        assert_eq!(new[7], FORMAT_VERSION);
        assert!(parse_version_bytes(&new).min_compatible.is_some());

        // Hypothetical old peer with major=255: byte[0]=0xFF, byte[7]=0
        // Should be parsed as OLD format (byte[7]==0)
        let old_255 = [0xFF, 1, 152, 0, 0, 0, 0, 0];
        let info = parse_version_bytes(&old_255);
        assert!(
            info.min_compatible.is_none(),
            "byte[0]=0xFF but byte[7]=0 → old format"
        );
        assert_eq!(info.version, (0xFF, 1, 152));
    }

    /// Both old-format peers: is_compatible always succeeds (no min_compatible check).
    #[test]
    fn test_both_old_format_always_compatible() {
        let a = [0u8, 1, 152, 0, 0, 0, 0, 0];
        let b = [0u8, 1, 151, 0, 0, 0, 0, 0];
        // Both old format — no range checking possible, both accept
        assert!(is_compatible(&a, &b).is_ok());
        assert!(is_compatible(&b, &a).is_ok());
    }

    /// Cross-major version peers are always incompatible because min_compatible
    /// inherits major.minor from the version (wire format only encodes min_patch).
    /// A 1.0.x peer's min_compatible is always (1, 0, _), which no 0.x peer satisfies.
    #[test]
    fn test_cross_major_version_always_incompatible() {
        let v1 = encode_new_format("1.0.5", "1.0.3");
        let v0 = encode_new_format("0.1.200", "0.1.150");
        // v0 (0.1.200) < v1's min (1.0.3) → v1 rejects v0
        assert!(
            is_compatible(&v1, &v0).is_err(),
            "v1 should reject v0 (cross-major incompatible)"
        );
        // v1 (1.0.5) >= v0's min (0.1.150) → v0 accepts v1, but v1 (1.0.5) satisfies
        // v0's min anyway. However, the bidirectional check means v0 also checks
        // v1's min: v0 (0.1.200) < (1.0.3) → reject.
        assert!(
            is_compatible(&v0, &v1).is_err(),
            "v0 should also reject v1 (v0's version < v1's min)"
        );
    }
}

/// Verifies that handshake completion is atomic - state transitions directly
/// from GatewayHandshake to Established with no intermediate None state.
#[test]
fn test_atomic_handshake_completion_no_packet_loss() {
    use super::ConnectionStateManager;
    use crate::simulation::VirtualTime;
    use crate::transport::packet_data::{PacketData, UnknownEncryption};
    use std::net::SocketAddr;
    use tokio::net::UdpSocket;
    use tokio::sync::mpsc;

    let time = VirtualTime::new();
    let mut manager: ConnectionStateManager<UdpSocket, VirtualTime> =
        ConnectionStateManager::new(time);
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

    // Create a channel for the handshake
    let (tx, _rx) = mpsc::channel::<PacketData<UnknownEncryption>>(16);

    // Start gateway handshake
    let started = manager.start_gateway_handshake(addr, tx);
    assert!(started, "Should start gateway handshake");
    assert!(
        manager.has_gateway_handshake(&addr),
        "Should be in GatewayHandshake state"
    );

    // Create channel for established connection
    let (established_tx, _established_rx) = mpsc::channel::<PacketData<UnknownEncryption>>(16);

    // Complete the handshake atomically
    let completed = manager.complete_gateway_handshake(addr, established_tx);
    assert!(completed, "Should complete gateway handshake");

    // Verify state is Established (not None) - proves no gap
    assert!(
        manager.is_established(&addr),
        "State must be Established immediately after completion - proves no race gap"
    );
    assert!(
        manager.get_state(&addr).is_some(),
        "get_state() must return Some immediately after transition"
    );
}

/// Proves RecentlyClosed state prevents misrouting packets to asymmetric decryption.
///
/// Without RecentlyClosed, packets arriving after connection close would fall through
/// to the asymmetric decryption handler (expensive and wrong).
#[test]
fn test_recently_closed_prevents_asymmetric_decryption() {
    use super::{ConnectionState, ConnectionStateManager, RECENTLY_CLOSED_DURATION};
    use crate::simulation::VirtualTime;
    use crate::transport::packet_data::{PacketData, UnknownEncryption};
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::UdpSocket;
    use tokio::sync::mpsc;

    let time = VirtualTime::new();
    let mut manager: ConnectionStateManager<UdpSocket, VirtualTime> =
        ConnectionStateManager::new(time.clone());
    let addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();

    // Create an established connection
    let (tx, _rx) = mpsc::channel::<PacketData<UnknownEncryption>>(16);
    manager.start_gateway_handshake(addr, tx);
    let (established_tx, _established_rx) = mpsc::channel::<PacketData<UnknownEncryption>>(16);
    manager.complete_gateway_handshake(addr, established_tx);
    assert!(manager.is_established(&addr));

    // Close the connection
    manager.mark_closed(&addr);

    // Verify state is RecentlyClosed (not removed)
    let state = manager.get_state(&addr);
    assert!(
        state.is_some(),
        "State should exist as RecentlyClosed, not be removed"
    );
    assert!(
        matches!(state, Some(ConnectionState::RecentlyClosed { .. })),
        "State must be RecentlyClosed - prevents packets from falling through"
    );

    // Verify it's not treated as unknown (which would trigger asymmetric decryption)
    assert!(!manager.is_established(&addr), "Should not be Established");
    assert!(
        manager.get_state(&addr).is_some(),
        "Should NOT return None - that would trigger asymmetric handler"
    );

    // Advance time past expiration and cleanup
    time.advance(RECENTLY_CLOSED_DURATION + Duration::from_millis(1));
    manager.cleanup_expired();

    // Now the state should be gone
    assert!(
        manager.get_state(&addr).is_none(),
        "State should be cleaned up after expiration"
    );
}

/// Regression test for #4406 — a too-short asymmetric intro payload must
/// not panic when parsed.
///
/// Asymmetric encryption to our public key does not authenticate the
/// sender (the key is public), so any peer can encrypt an arbitrarily
/// short payload to us. The outbound NAT-traversal `decrypt_asym` path
/// used to slice `decrypted.data()[..8]` / `[8..24]` with no length
/// check, so a decrypted payload shorter than `PROTOC_VERSION.len() + 16`
/// bytes triggered an index-out-of-bounds panic — a remote,
/// unauthenticated denial of service.
///
/// Two layers are exercised:
///  1. The pure `split_asym_intro_payload` bounds check, swept across
///     every length from empty up to one byte below the minimum, plus
///     the exact boundary and an over-long payload.
///  2. The full attacker path: a real keypair, a short payload encrypted
///     to the public key, then asymmetric decryption + parse — proving
///     the end-to-end flow returns `None` instead of panicking.
#[test]
fn test_short_asym_intro_packet_does_not_panic() {
    use super::{ASYM_INTRO_MIN_LEN, MAX_PACKET_SIZE, PROTOC_VERSION, split_asym_intro_payload};
    use crate::transport::crypto::TransportKeypair;
    use crate::transport::packet_data::{PacketData, UnknownEncryption};

    // Layer 1: every too-short length must return None, never panic.
    for len in 0..ASYM_INTRO_MIN_LEN {
        let payload = vec![0u8; len];
        assert!(
            split_asym_intro_payload(&payload).is_none(),
            "payload of len {len} (< {ASYM_INTRO_MIN_LEN}) must be rejected, not sliced"
        );
    }

    // Boundary: exactly the minimum length parses successfully and splits
    // into the version prefix and the 16-byte symmetric key.
    let exact: Vec<u8> = (0..ASYM_INTRO_MIN_LEN as u8).collect();
    let (protoc, key) =
        split_asym_intro_payload(&exact).expect("minimum-length payload must parse");
    assert_eq!(&protoc[..], &exact[..PROTOC_VERSION.len()]);
    assert_eq!(&key[..], &exact[PROTOC_VERSION.len()..ASYM_INTRO_MIN_LEN]);

    // Over-long payloads parse too (extra trailing bytes are ignored).
    let long = vec![7u8; ASYM_INTRO_MIN_LEN + 32];
    assert!(
        split_asym_intro_payload(&long).is_some(),
        "over-long payload must still parse"
    );

    // Layer 2: full attacker path. Encrypt a short payload to our public
    // key, decrypt it, and parse — this is exactly what an unauthenticated
    // remote peer can send to the outbound handshake path.
    let keypair = TransportKeypair::new();
    for short_len in [0usize, 1, 7, PROTOC_VERSION.len(), ASYM_INTRO_MIN_LEN - 1] {
        let short_payload = vec![0xABu8; short_len];
        let encrypted = keypair.public().encrypt(&short_payload);
        let packet = PacketData::<UnknownEncryption, MAX_PACKET_SIZE>::from_buf(&encrypted);
        let decrypted = packet
            .try_decrypt_asym(keypair.secret())
            .expect("we encrypted to our own key, so decryption must succeed");
        assert_eq!(
            decrypted.data().len(),
            short_len,
            "decrypted payload should round-trip to the original short length"
        );
        // The bug: this call previously panicked for short_len < 24.
        assert!(
            split_asym_intro_payload(decrypted.data()).is_none(),
            "short asym intro payload (len {short_len}) must be rejected without panicking"
        );
    }
}

/// Regression invariant for #3959 — the UDP listener forwarding path must
/// never block, regardless of receiver state.
///
/// Under the deleted `fast_channel` (built on `crossbeam::channel`), a
/// wedged crossbeam slot could send `try_send` into an indefinite
/// `sched_yield` spin (`Backoff::snooze`). The reproduction in production
/// required cross-thread interleaving inside crossbeam internals and is
/// not synthetically reproducible from a unit test, so this test does
/// NOT reproduce the original wedge — instead it pins the structural
/// contract that the fix establishes: the per-peer inbound `Sender`
/// returns immediately on capacity exhaustion (`Full`), without
/// blocking the listener task. The two methods exercised
/// (`try_send_established` and `try_send_gateway_handshake`) are
/// invoked from `UdpPacketsListener::listen` for every inbound packet,
/// so any future regression that reintroduces a blocking `try_send`
/// here would refreeze the node the same way.
///
/// Run as `#[tokio::test]` so the channel construction lives inside a
/// runtime context — `mpsc::channel` itself does not require one, but
/// keeping the test in a runtime keeps it forward-compatible if anyone
/// extends `ConnectionStateManager` to use async primitives later.
#[tokio::test]
async fn test_try_send_established_never_blocks_when_full() {
    use super::ConnectionStateManager;
    use super::mock_transport::MockSocket;
    use crate::simulation::VirtualTime;
    use crate::transport::packet_data::{PacketData, UnknownEncryption};
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    const CAPACITY: usize = 4;
    const ATTEMPTS: usize = 1_000;
    // Generous budget — 1k non-blocking try_sends should complete in
    // microseconds. Compare to the production wedge that spun 4 hours.
    const BUDGET: Duration = Duration::from_secs(1);

    let time = VirtualTime::new();
    let mut manager: ConnectionStateManager<MockSocket, VirtualTime> =
        ConnectionStateManager::new(time);
    let addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // Establish a connection with a tiny channel and no consumer ever reading from it.
    let (tx, _rx) = mpsc::channel::<PacketData<UnknownEncryption>>(CAPACITY);
    let started = manager.start_gateway_handshake(addr, tx.clone());
    assert!(started, "Should start gateway handshake");
    let completed = manager.complete_gateway_handshake(addr, tx);
    assert!(completed, "Should complete gateway handshake");

    // Build a dummy packet to forward repeatedly.
    let make_packet = || PacketData::<UnknownEncryption>::from_buf([0u8; 32]);

    let started_at = Instant::now();
    let mut sent_count = 0;
    let mut full_count = 0;
    for _ in 0..ATTEMPTS {
        match manager.try_send_established(&addr, make_packet()) {
            Ok(true) => sent_count += 1,
            Ok(false) => full_count += 1,
            Err(()) => panic!("connection unexpectedly disconnected"),
        }
    }
    let elapsed = started_at.elapsed();

    assert!(
        elapsed < BUDGET,
        "{ATTEMPTS} try_send_established calls took {elapsed:?} (budget {BUDGET:?}) — \
         the listener forwarding path must not block, regardless of receiver state"
    );
    assert!(
        sent_count <= CAPACITY,
        "Bounded channel (capacity {CAPACITY}) accepted {sent_count} messages with no consumer"
    );
    assert!(
        full_count >= ATTEMPTS - CAPACITY,
        "Expected at least {} Full results, got {full_count}",
        ATTEMPTS - CAPACITY
    );
}

/// Companion to `test_try_send_established_never_blocks_when_full` —
/// the gateway-handshake forwarding path is also invoked synchronously
/// from the listener loop (`connection_handler.rs:1352`) and was
/// equally vulnerable to the crossbeam wedge before #3959. This test
/// pins the same non-blocking invariant for that path.
#[tokio::test]
async fn test_try_send_gateway_handshake_never_blocks_when_full() {
    use super::ConnectionStateManager;
    use super::mock_transport::MockSocket;
    use crate::simulation::VirtualTime;
    use crate::transport::packet_data::{PacketData, UnknownEncryption};
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    const CAPACITY: usize = 4;
    const ATTEMPTS: usize = 1_000;
    const BUDGET: Duration = Duration::from_secs(1);

    let time = VirtualTime::new();
    let mut manager: ConnectionStateManager<MockSocket, VirtualTime> =
        ConnectionStateManager::new(time);
    let addr: SocketAddr = "127.0.0.1:8003".parse().unwrap();

    let (tx, _rx) = mpsc::channel::<PacketData<UnknownEncryption>>(CAPACITY);
    let started = manager.start_gateway_handshake(addr, tx);
    assert!(started, "Should start gateway handshake");

    let make_packet = || PacketData::<UnknownEncryption>::from_buf([0u8; 32]);

    let started_at = Instant::now();
    let mut full_count = 0;
    for _ in 0..ATTEMPTS {
        match manager.try_send_gateway_handshake(&addr, make_packet()) {
            Ok(true) => {}
            Ok(false) => full_count += 1,
            Err(()) => panic!("handshake state unexpectedly missing"),
        }
    }
    let elapsed = started_at.elapsed();

    assert!(
        elapsed < BUDGET,
        "{ATTEMPTS} try_send_gateway_handshake calls took {elapsed:?} (budget {BUDGET:?})"
    );
    assert!(
        full_count >= ATTEMPTS - CAPACITY,
        "Expected at least {} Full results, got {full_count}",
        ATTEMPTS - CAPACITY
    );
}

/// Regression for #3961 — completes the trio of listener-forwarding
/// non-blocking tests with the NAT-traversal path. Like `try_send_established`
/// and `try_send_gateway_handshake`, `try_send_nat_traversal` is invoked
/// synchronously from `UdpPacketsListener::listen` for every inbound
/// packet that maps to an in-flight NAT-traversal handshake. The prior
/// implementation (`send_nat_traversal` with `.send().await`) could
/// stall the listener for every other peer if a single per-handshake
/// consumer was slow. This test pins the new non-blocking contract.
#[tokio::test]
async fn test_try_send_nat_traversal_never_blocks_when_full() {
    use super::ConnectionStateManager;
    use super::mock_transport::MockSocket;
    use crate::simulation::VirtualTime;
    use crate::transport::packet_data::{PacketData, UnknownEncryption};
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::sync::{mpsc, oneshot};

    const CAPACITY: usize = 4;
    const ATTEMPTS: usize = 1_000;
    const BUDGET: Duration = Duration::from_secs(1);

    let time = VirtualTime::new();
    let mut manager: ConnectionStateManager<MockSocket, VirtualTime> =
        ConnectionStateManager::new(time);
    let addr: SocketAddr = "127.0.0.1:8004".parse().unwrap();

    let (tx, _rx) = mpsc::channel::<PacketData<UnknownEncryption>>(CAPACITY);
    let (result_tx, _result_rx) = oneshot::channel();
    let started = manager.start_nat_traversal(addr, tx, result_tx);
    assert!(started, "Should start NAT traversal");

    let make_packet = || PacketData::<UnknownEncryption>::from_buf([0u8; 32]);

    let started_at = Instant::now();
    let mut full_count = 0;
    for _ in 0..ATTEMPTS {
        match manager.try_send_nat_traversal(&addr, make_packet()) {
            Ok(super::TrySendNatTraversalOutcome::Sent) => {}
            Ok(super::TrySendNatTraversalOutcome::Full) => full_count += 1,
            Ok(super::TrySendNatTraversalOutcome::Closed) => {
                panic!("receiver still alive; should not be Closed");
            }
            Err(()) => panic!("NAT traversal state unexpectedly missing"),
        }
    }
    let elapsed = started_at.elapsed();

    assert!(
        elapsed < BUDGET,
        "{ATTEMPTS} try_send_nat_traversal calls took {elapsed:?} (budget {BUDGET:?})"
    );
    assert!(
        full_count >= ATTEMPTS - CAPACITY,
        "Expected at least {} Full results, got {full_count}",
        ATTEMPTS - CAPACITY
    );
}

#[test]
fn test_gateway_connection_rate_limiter() {
    use super::{
        GW_RAMP_PHASE1_DURATION, GW_RAMP_PHASE1_RATE, GW_RAMP_PHASE2_DURATION, GW_RAMP_PHASE2_RATE,
        GatewayConnectionRateLimiter,
    };
    use crate::simulation::VirtualTime;
    use std::time::Duration;

    let time = VirtualTime::new();
    // `time_source` is itself VirtualTime here, so no separate override is needed.
    let mut limiter = GatewayConnectionRateLimiter::new(time.clone(), None);

    // Phase 1: should allow GW_RAMP_PHASE1_RATE connections per second
    for _ in 0..GW_RAMP_PHASE1_RATE {
        assert!(limiter.try_accept(), "Should accept within phase 1 rate");
    }
    // Next one should be rejected
    assert!(
        !limiter.try_accept(),
        "Should reject when phase 1 rate exceeded"
    );

    // Advance past 1 second — window should reset inside try_accept
    time.advance(Duration::from_millis(1001));
    assert!(
        limiter.try_accept(),
        "Should accept after window reset in phase 1"
    );

    // Advance into phase 2 (past phase 1 boundary + past current window).
    // Use the SAME limiter to verify try_accept handles long time gaps
    // and correctly resets its sliding window across phase transitions.
    time.advance(GW_RAMP_PHASE1_DURATION);
    assert_eq!(
        limiter.current_rate_limit(),
        Some(GW_RAMP_PHASE2_RATE),
        "Should be in phase 2 after advancing past phase 1 duration"
    );

    for _ in 0..GW_RAMP_PHASE2_RATE {
        assert!(limiter.try_accept(), "Should accept within phase 2 rate");
    }
    assert!(
        !limiter.try_accept(),
        "Should reject when phase 2 rate exceeded"
    );

    // Advance into unlimited phase (past phase 2 boundary).
    // Again, same limiter instance — validates window reset over long gaps.
    time.advance(GW_RAMP_PHASE2_DURATION);
    assert_eq!(
        limiter.current_rate_limit(),
        None,
        "Should be unlimited after phase 2 duration"
    );

    // Should accept unlimited connections
    for _ in 0..1000 {
        assert!(limiter.try_accept(), "Should accept unlimited in phase 3");
    }
}

/// Regression test for the gateway admission clock used by
/// `test_thundering_herd_connect_storm` and every other SimNetwork test.
///
/// In a SimNetwork the connection handler is built with a `RealTime` source
/// (`config_listener`) plus a simulation `VirtualTime` override. The admission
/// ramp must advance on `max(real_elapsed, virtual_elapsed)` so it is correct in
/// both execution modes:
/// - **Virtual-time-driven** (the thundering-herd test advances `VirtualTime`
///   while real time barely moves): the ramp must follow the virtual clock or it
///   never progresses and admission stalls nondeterministically.
/// - **Real-time-driven** (the virtual clock is never advanced): the ramp must
///   follow the real clock or the 1-second window never resets and the gateway
///   blocks every peer after the first burst, so the network never forms.
///
/// The two halves below drive exactly one clock each (using two independently
/// controllable `VirtualTime` instances, one standing in for the real source).
/// A revert to either single clock fails one half: pure-virtual fails the
/// real-driven half, pure-real fails the virtual-driven half.
#[test]
fn test_gateway_rate_limiter_admission_clock_is_max_of_real_and_virtual() {
    use super::{GW_RAMP_PHASE1_RATE, GW_RAMP_PHASE2_RATE, GatewayConnectionRateLimiter};
    use crate::simulation::VirtualTime;
    use std::time::Duration;

    // Drives the ramp through phase 1 -> reset -> phase 2 by advancing only the
    // named clock, asserting the OTHER clock staying frozen does not matter.
    fn drive(advance: impl Fn(&mut GatewayConnectionRateLimiter<VirtualTime>)) {
        let real = VirtualTime::new();
        let virtual_clock = VirtualTime::new();
        let mut limiter = GatewayConnectionRateLimiter::new(real, Some(virtual_clock));
        // Phase 1: exactly GW_RAMP_PHASE1_RATE admits, then throttled.
        for _ in 0..GW_RAMP_PHASE1_RATE {
            assert!(limiter.try_accept(), "phase 1 admits within rate");
        }
        assert!(
            !limiter.try_accept(),
            "phase 1 rate exhausted (no clock advanced)"
        );
        advance(&mut limiter);
    }

    // Helpers to advance one clock of the limiter under test.
    fn adv_virtual(l: &mut GatewayConnectionRateLimiter<VirtualTime>, d: Duration) {
        l.virtual_clock.as_ref().unwrap().advance(d);
    }
    fn adv_real(l: &mut GatewayConnectionRateLimiter<VirtualTime>, d: Duration) {
        l.time_source.advance(d);
    }

    // Virtual-time-driven: advancing ONLY the virtual clock ramps the limiter.
    drive(|l| {
        adv_virtual(l, Duration::from_millis(1001));
        assert!(l.try_accept(), "virtual-clock advance resets the window");
        adv_virtual(l, Duration::from_secs(31));
        assert_eq!(
            l.current_rate_limit(),
            Some(GW_RAMP_PHASE2_RATE),
            "virtual-clock advance ramps into phase 2"
        );
    });

    // Real-time-driven (the case that regressed under a virtual-only clock):
    // advancing ONLY the real clock, with the virtual clock frozen, ramps it too.
    drive(|l| {
        adv_real(l, Duration::from_millis(1001));
        assert!(l.try_accept(), "real-clock advance resets the window");
        adv_real(l, Duration::from_secs(31));
        assert_eq!(
            l.current_rate_limit(),
            Some(GW_RAMP_PHASE2_RATE),
            "real-clock advance ramps into phase 2"
        );
    });

    // Pin max() vs a hypothetical sum: advancing BOTH clocks by 20s leaves
    // effective elapsed at max(20s, 20s) = 20s, still phase 1. A sum would reach
    // 40s and report phase 2, a min would report phase 1 only because one term is
    // frozen — this both-clocks case distinguishes max from sum.
    drive(|l| {
        adv_real(l, Duration::from_secs(20));
        adv_virtual(l, Duration::from_secs(20));
        assert_eq!(
            l.current_rate_limit(),
            Some(GW_RAMP_PHASE1_RATE),
            "effective elapsed is max(20s, 20s) = 20s (phase 1), not the 40s sum"
        );
    });
}
