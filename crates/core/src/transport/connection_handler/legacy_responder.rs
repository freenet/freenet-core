//! The gateway's reply to a legacy UDP intro, once gateways no longer run the
//! legacy handshake (phase D of the QUIC-native connect plan, step D-5).
//!
//! A node that still connects over UDP sends an intro: its protocol version and
//! a session key, encrypted to the gateway's public key. After phase D the
//! gateway does not complete that handshake. It decrypts the intro, reads the
//! version, and answers with the version-mismatch ack that every released node
//! already understands. The old node then sets its version-mismatch flag and
//! runs an update check, which is the way out of the old network.
//!
//! The responder holds no per-peer state. Each intro gets at most one reply,
//! the reply does not depend on the intro's size, and a global token bucket
//! limits how many intros are decrypted per second.
//!
//! Shipped dark: [`LEGACY_RESPONDER_ENABLED`] is `false` until step D-8 turns
//! it on. While it is `false` the gateway handshake is unchanged.

use aes_gcm::{Aes128Gcm, KeyInit};

use super::version_cmp;
use crate::simulation::TimeSource;
use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::{PacketData, SymmetricAES};
use crate::transport::symmetric_message::SymmetricMessage;

/// Whether gateways answer legacy intros statelessly instead of running the
/// legacy handshake. Off until D-8.
pub(crate) const LEGACY_RESPONDER_ENABLED: bool = false;

/// Intros decrypted per second, across all senders.
pub(super) const LEGACY_RESPONDER_DECRYPTS_PER_SEC: u64 = 20;

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// A global token bucket: refills at [`LEGACY_RESPONDER_DECRYPTS_PER_SEC`] and
/// holds at most one second of tokens.
pub(super) struct DecryptBucket<T: TimeSource> {
    time_source: T,
    /// Available allowance in nanoseconds of refill; one decrypt costs
    /// `NANOS_PER_SEC / rate`.
    allowance_nanos: u64,
    last_nanos: u64,
}

impl<T: TimeSource> DecryptBucket<T> {
    const COST_NANOS: u64 = NANOS_PER_SEC / LEGACY_RESPONDER_DECRYPTS_PER_SEC;

    pub(super) fn new(time_source: T) -> Self {
        let last_nanos = time_source.now_nanos();
        Self {
            time_source,
            allowance_nanos: NANOS_PER_SEC,
            last_nanos,
        }
    }

    /// Take one token; `false` means the intro must be dropped undecrypted.
    pub(super) fn try_take(&mut self) -> bool {
        let now = self.time_source.now_nanos();
        let elapsed = now.saturating_sub(self.last_nanos);
        self.last_nanos = self.last_nanos.max(now);
        self.allowance_nanos = self
            .allowance_nanos
            .saturating_add(elapsed)
            .min(NANOS_PER_SEC);
        if self.allowance_nanos >= Self::COST_NANOS {
            self.allowance_nanos -= Self::COST_NANOS;
            true
        } else {
            false
        }
    }
}

/// The responder a listener holds: on when [`LEGACY_RESPONDER_ENABLED`] is, or
/// when a test names a reply version.
pub(super) fn build<T: TimeSource>(
    time_source: T,
    test_override: Option<String>,
) -> Option<LegacyResponder<T>> {
    match test_override {
        Some(version) => Some(LegacyResponder::with_reply_version(time_source, &version)),
        None => LEGACY_RESPONDER_ENABLED.then(|| LegacyResponder::new(time_source)),
    }
}

/// What the responder did with one intro.
pub(super) enum Outcome {
    /// The bucket was empty. Nothing was decrypted.
    RateLimited,
    /// The intro did not decrypt, or was too short to hold a version and key.
    Invalid,
    /// Send this packet back to the sender.
    Reply(Box<PacketData<SymmetricAES>>),
}

pub(super) struct LegacyResponder<T: TimeSource> {
    bucket: DecryptBucket<T>,
    /// The version named in the reply: this binary's, unless a test says
    /// otherwise.
    reply_version: String,
}

impl<T: TimeSource> LegacyResponder<T> {
    pub(super) fn new(time_source: T) -> Self {
        Self::with_reply_version(time_source, env!("CARGO_PKG_VERSION"))
    }

    pub(super) fn with_reply_version(time_source: T, reply_version: &str) -> Self {
        Self {
            bucket: DecryptBucket::new(time_source),
            reply_version: reply_version.to_owned(),
        }
    }

    /// Handle one intro packet (the raw datagram).
    pub(super) fn respond(&mut self, secret: &TransportSecretKey, intro: &[u8]) -> Outcome {
        self.respond_then(secret, intro, crate::transport::signal_urgent_update)
    }

    /// [`Self::respond`] with the urgent-update signal as `urgent`, so a test
    /// can see it fire without latching the process-wide flag.
    fn respond_then(
        &mut self,
        secret: &TransportSecretKey,
        intro: &[u8],
        urgent: impl FnOnce(),
    ) -> Outcome {
        if !self.bucket.try_take() {
            return Outcome::RateLimited;
        }
        let Ok(plain) = secret.decrypt(intro) else {
            return Outcome::Invalid;
        };
        let version_len = version_cmp::PROTOC_VERSION.len();
        let (Some(version), Some(key)) = (
            plain.get(..version_len),
            plain.get(version_len..version_len + 16),
        ) else {
            return Outcome::Invalid;
        };
        if let Ok(version) = <[u8; 8]>::try_from(version) {
            // Version discovery, as for an intro the handshake would parse.
            crate::transport::report_peer_version(
                version_cmp::parse_version_bytes(&version).version,
            );
            // As the handshake this replaces: a sender whose floor is above
            // this build asks it to update.
            if version_cmp::remote_requires_newer_than_us(&version_cmp::PROTOC_VERSION, &version) {
                urgent();
            }
        }
        let Ok(key) = Aes128Gcm::new_from_slice(key) else {
            return Outcome::Invalid;
        };
        match SymmetricMessage::ack_error_for_version(&key, &self.reply_version) {
            Ok(packet) => Outcome::Reply(Box::new(packet)),
            Err(_) => Outcome::Invalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::mock_transport::{Channels, MockSocket, create_mock_peer};
    use super::super::{OutboundConnectionHandler, UdpTransportConfig};
    use super::*;
    use crate::simulation::VirtualTime;
    use crate::transport::crypto::TransportKeypair;
    use crate::transport::packet_data::{MAX_PACKET_SIZE, UnknownEncryption};
    use crate::transport::{TransportError, version_mismatch_generation};
    use dashmap::DashMap;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    /// The intro a released node sends: its version and a session key,
    /// encrypted to the gateway's key. 73 bytes on the wire.
    fn intro_to(gw: &TransportKeypair, plaintext_len: usize) -> PacketData<UnknownEncryption> {
        let mut plain = vec![0u8; plaintext_len];
        plain[..8].copy_from_slice(&version_cmp::PROTOC_VERSION);
        for b in plain.iter_mut().skip(8).take(16) {
            *b = 7;
        }
        let p = PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&plain, &gw.public);
        PacketData::from_buf(p.data())
    }

    fn responder(vt: &VirtualTime) -> LegacyResponder<VirtualTime> {
        LegacyResponder::with_reply_version(vt.clone(), "99.0.0")
    }

    fn reply_len(outcome: Outcome) -> usize {
        match outcome {
            Outcome::Reply(p) => p.data().len(),
            Outcome::RateLimited => panic!("rate limited"),
            Outcome::Invalid => panic!("invalid"),
        }
    }

    /// The reply is bounded by the request: it is at most a quarter larger
    /// than the smallest intro a released node sends (73 bytes; the reply
    /// cannot be smaller, because the shortest text an old node acts on is
    /// "expected version" plus a version), and it does not grow with the intro.
    #[test]
    fn reply_is_bounded_by_the_intro_and_does_not_grow_with_it() {
        let gw = TransportKeypair::new();
        let mut r = LegacyResponder::new(VirtualTime::new());
        let intro = intro_to(&gw, 24);
        assert_eq!(intro.data().len(), 73, "a released node's intro size");
        let reply = reply_len(r.respond(&gw.secret, intro.data()));
        assert!(
            reply * 4 <= intro.data().len() * 5,
            "reply {reply} is more than 1.25x the {}-byte intro",
            intro.data().len()
        );

        // A padded intro gets the same reply: reply size is not attacker-chosen.
        let big = intro_to(&gw, 1000);
        assert!(big.data().len() > 1000);
        let vt = VirtualTime::new();
        let mut r2 = LegacyResponder::new(vt);
        assert_eq!(reply_len(r2.respond(&gw.secret, big.data())), reply);
    }

    #[test]
    fn invalid_intros_get_no_reply() {
        let gw = TransportKeypair::new();
        let other = TransportKeypair::new();
        let mut r = LegacyResponder::new(VirtualTime::new());
        // Encrypted to someone else.
        let wrong_key = intro_to(&other, 24);
        assert!(matches!(
            r.respond(&gw.secret, wrong_key.data()),
            Outcome::Invalid
        ));
        // Decrypts, but too short to hold a version and a key.
        let short = intro_to(&gw, 10);
        assert!(matches!(
            r.respond(&gw.secret, short.data()),
            Outcome::Invalid
        ));
    }

    /// Past 20 intros a second the responder drops without decrypting. A
    /// garbage intro is `Invalid` when it reaches the decrypt and
    /// `RateLimited` when it does not, so the outcome shows which.
    #[test]
    fn global_bucket_drops_over_20_per_second_without_decrypting() {
        let gw = TransportKeypair::new();
        let other = TransportKeypair::new();
        let vt = VirtualTime::new();
        let mut r = responder(&vt);
        let garbage = intro_to(&other, 24);
        for i in 0..LEGACY_RESPONDER_DECRYPTS_PER_SEC {
            assert!(
                matches!(r.respond(&gw.secret, garbage.data()), Outcome::Invalid),
                "intro {i} within the rate must reach the decrypt"
            );
        }
        for _ in 0..100 {
            assert!(matches!(
                r.respond(&gw.secret, garbage.data()),
                Outcome::RateLimited
            ));
        }
        // One token comes back every 50 ms.
        vt.advance(Duration::from_millis(50));
        assert!(matches!(
            r.respond(&gw.secret, garbage.data()),
            Outcome::Invalid
        ));
        assert!(matches!(
            r.respond(&gw.secret, garbage.data()),
            Outcome::RateLimited
        ));
        // A long idle period refills to one second's worth, not more.
        vt.advance(Duration::from_secs(60));
        let admitted = (0..100)
            .filter(|_| !matches!(r.respond(&gw.secret, garbage.data()), Outcome::RateLimited))
            .count();
        assert_eq!(admitted as u64, LEGACY_RESPONDER_DECRYPTS_PER_SEC);
    }

    /// Dark until D-8, which flips this constant together with the switch-on
    /// of D-3 and D-4.
    #[test]
    fn responder_is_off_by_default() {
        const { assert!(!LEGACY_RESPONDER_ENABLED) };
        assert!(build(VirtualTime::new(), None).is_none());
        assert!(build(VirtualTime::new(), Some("1.2.3".into())).is_some());
    }

    /// The real client handshake against a gateway running the responder: the
    /// client reports a version mismatch (the error that makes it check for
    /// an update) and sets the update-check flag. The gateway names a newer
    /// version than the client's, as a gateway on a newer release does.
    #[tokio::test]
    async fn released_client_handshake_sees_version_mismatch() {
        let channels: Channels = Arc::new(DashMap::new());
        let gw_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 47_610).into();
        let gw_keypair = TransportKeypair::new();
        let gw_pub = gw_keypair.public.clone();
        let socket = Arc::new(
            MockSocket::new(
                Default::default(),
                Default::default(),
                gw_addr,
                channels.clone(),
            )
            .await,
        );
        let (_gw_handler, mut gw_conns, _listen) = OutboundConnectionHandler::config_listener(
            socket,
            gw_keypair,
            true,
            gw_addr,
            UdpTransportConfig {
                legacy_responder_override: Some("99.0.0".to_string()),
                ..UdpTransportConfig::default()
            },
            None,
            None,
        )
        .expect("gateway setup");

        let (_peer_pub, mut peer, _peer_addr) = create_mock_peer(Default::default(), channels)
            .await
            .unwrap();
        let before = version_mismatch_generation();
        let attempt = peer.connect(gw_pub, gw_addr).await;
        let result = tokio::time::timeout(Duration::from_secs(30), attempt)
            .await
            .expect("the handshake must end, not time out");
        match result {
            Err(TransportError::ProtocolVersionMismatch { expected, .. }) => {
                assert_eq!(expected, "99.0.0");
            }
            Err(e) => panic!("expected a version mismatch, got {e}"),
            Ok(_) => panic!("the responder must not complete a handshake"),
        }
        assert!(
            version_mismatch_generation() > before,
            "the client must flag the mismatch so its update check runs"
        );
        assert!(
            gw_conns.new_connection_notifier.try_recv().is_err(),
            "the gateway must not accept a connection"
        );
    }

    /// An intro naming a build whose floor is above this one makes the
    /// gateway ask to update, as the handshake it replaces does; an intro at
    /// this build's own version does not.
    #[test]
    fn newer_sender_signals_urgent_update() {
        let gw = TransportKeypair::new();
        let own = version_cmp::PROTOC_VERSION;
        let (major, minor) = (own[1], own[2]);
        let patch = u16::from_be_bytes([own[3], own[4]]);
        let [p_hi, p_lo] = (patch + 2).to_be_bytes();
        let [f_hi, f_lo] = (patch + 1).to_be_bytes();
        let newer = [0xFF, major, minor, p_hi, p_lo, f_hi, f_lo, own[7]];
        assert!(
            version_cmp::remote_requires_newer_than_us(&own, &newer),
            "premise"
        );

        let intro_with = |version: [u8; 8]| {
            let mut plain = vec![7u8; 24];
            plain[..8].copy_from_slice(&version);
            let p = PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&plain, &gw.public);
            PacketData::<UnknownEncryption>::from_buf(p.data())
        };
        let fired = std::cell::Cell::new(0);
        let mut r = responder(&VirtualTime::new());
        let out = r.respond_then(&gw.secret, intro_with(newer).data(), || {
            fired.set(fired.get() + 1)
        });
        assert!(matches!(out, Outcome::Reply(_)));
        assert_eq!(fired.get(), 1);
        r.respond_then(&gw.secret, intro_with(own).data(), || {
            fired.set(fired.get() + 1)
        });
        assert_eq!(
            fired.get(),
            1,
            "an intro at our own version must not signal"
        );
    }

    /// The listener must `continue` after answering, or the legacy handshake
    /// would also run on the same intro. The behavioural test cannot see this
    /// deterministically, so pin it in the source.
    #[test]
    fn listener_stops_after_answering_a_legacy_intro() {
        let src: String = include_str!("../connection_handler.rs")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            src.contains("self.answer_legacy_intro(&packet_data, remote_addr); continue;"),
            "the listener must continue after answer_legacy_intro"
        );
    }
}
