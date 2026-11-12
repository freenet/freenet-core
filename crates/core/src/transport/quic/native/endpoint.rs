//! The endpoint's native side (#5940, design 2.4): allowing a peer as
//! native, dialling one, and sending a punch. These are visible to the native
//! module only, so nothing outside it can set up a native connection or send
//! a punch, and the setup (`setup`) is the only caller in production. The
//! connect op reaches the setup once A5 widens its visibility, in a diff that
//! shows it.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::transport::crypto::TransportPublicKey;
use crate::transport::demux::PUNCH_LEN;
use crate::transport::quic::identity::PeerKey;
use crate::transport::quic::{
    QuicAllowError, QuicConnectError, QuicConnection, QuicEndpoint, QuicPeerPermit, config,
};

impl QuicEndpoint {
    /// Send one punch datagram (`punch::punch_datagram`) to `to` on the
    /// endpoint's socket, without waiting. It is charged to the egress cap
    /// like any unpaced send. A send the socket cannot take now is lost, as a
    /// punch lost on the way would be.
    pub(in crate::transport::quic::native) fn send_punch(
        &self,
        datagram: &[u8; PUNCH_LEN],
        to: SocketAddr,
    ) {
        if let Err(error) = self.sources.try_send(datagram, to) {
            tracing::debug!(%to, %error, "A QUIC-native punch was not sent");
        }
    }

    /// `allow_peer` for a native connection: an attempt from `addr` is
    /// accepted with the native ALPN only, so it completes only if the client
    /// dialled it as native ([`Self::dial_native`]).
    pub(in crate::transport::quic::native) fn allow_native_peer(
        &self,
        addr: SocketAddr,
        expected: &TransportPublicKey,
    ) -> Result<QuicPeerPermit, QuicAllowError> {
        self.allow_peer_for(addr, expected, config::Alpn::Native)
    }

    /// `connect` for a native connection: TLS offers the native ALPN only,
    /// and the client starts from [`config::NATIVE_INITIAL_RTT`]. The server
    /// must have allowed this node with [`Self::allow_native_peer`]; an
    /// upgrade server refuses the handshake. The caller bounds the wait:
    /// quinn gives up only at the idle timeout.
    pub(in crate::transport::quic::native) async fn dial_native(
        &self,
        addr: SocketAddr,
        expected: &TransportPublicKey,
    ) -> Result<QuicConnection, QuicConnectError> {
        let peer = PeerKey::of(expected)?;
        let tls = self.identity.client_tls_for(&peer, config::Alpn::Native)?;
        let config = config::client_config(Arc::new(tls), self.native_transport.clone());
        self.connect_with(addr, config, peer).await
    }

    /// [`Self::allow_native_peer`] for a test outside the native module.
    #[cfg(test)]
    pub(in crate::transport::quic) fn allow_native_peer_for_test(
        &self,
        addr: SocketAddr,
        expected: &TransportPublicKey,
    ) -> Result<QuicPeerPermit, QuicAllowError> {
        self.allow_native_peer(addr, expected)
    }

    /// [`Self::dial_native`] for a test outside the native module.
    #[cfg(test)]
    pub(in crate::transport::quic) async fn dial_native_for_test(
        &self,
        addr: SocketAddr,
        expected: &TransportPublicKey,
    ) -> Result<QuicConnection, QuicConnectError> {
        self.dial_native(addr, expected).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::GlobalRng;
    use crate::transport::demux::classify;
    use crate::transport::metrics::DemuxClass;
    use crate::transport::quic::config::ALPN;
    use crate::transport::quic::tests::{SEED, bounded, drive, pair};

    /// A punch goes out on the endpoint's own socket: one datagram of
    /// `PUNCH_LEN` bytes from the endpoint's address, starting with the reserved
    /// byte, which the receiving demux drops as `Other`.
    #[test]
    fn a_punch_goes_out_on_the_endpoints_socket() {
        bounded(|| async move {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let pair = pair("quic-punch", net.addr(1), net.addr(2), None).await;
            let datagram = crate::transport::quic::native::punch::punch_datagram();
            pair.a.send_punch(&datagram, pair.b_addr);
            let sent = pair.sent();
            assert_eq!(sent.len(), 1, "{sent:?}");
            let punch = &sent[0];
            assert_eq!((punch.from, punch.to), (pair.a_addr, pair.b_addr));
            assert_eq!(punch.bytes.len(), crate::transport::demux::PUNCH_LEN);
            assert_eq!(punch.bytes[0], crate::transport::demux::PUNCH_FIRST_BYTE);
            assert_eq!(punch.bytes, datagram);
            assert_eq!(classify(&punch.bytes), DemuxClass::Other);
        });
    }

    /// The ALPN protocol `conn`'s handshake settled on.
    fn negotiated(conn: &QuicConnection) -> Option<Vec<u8>> {
        conn.inner()
            .handshake_data()
            .expect("handshake data")
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .expect("rustls handshake data")
            .protocol
    }

    /// A native dial to an address allowed as native negotiates the native ALPN
    /// on both sides, and the attempt reads as native.
    #[test]
    fn a_native_dial_to_a_native_waiter_negotiates_the_native_alpn() {
        bounded(|| async move {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let pair = pair("quic-native-alpn", net.addr(1), net.addr(2), None).await;
            let _permit = pair
                .b
                .allow_native_peer(pair.a_addr, pair.a_keys.public())
                .expect("room");
            let (a, b, b_addr, b_key) = (
                pair.a.clone(),
                pair.b.clone(),
                pair.b_addr,
                pair.b_keys.public().clone(),
            );
            let (client, server) = drive(&pair.time, Duration::from_secs(60), async move {
                tokio::join!(a.dial_native(b_addr, &b_key), async {
                    let attempt = b.accept().await.expect("an attempt");
                    assert!(attempt.is_native(), "allowed as native");
                    attempt.establish().await
                })
            })
            .await;
            let (client, server) = (client.expect("client"), server.expect("server"));
            assert_eq!(negotiated(&client).as_deref(), Some(config::NATIVE_ALPN));
            assert_eq!(negotiated(&server).as_deref(), Some(config::NATIVE_ALPN));
        });
    }

    /// A dial of one kind to an address allowed for the other fails its
    /// handshake on both sides: each attempt offers exactly one ALPN.
    #[test]
    fn a_dial_to_a_waiter_of_the_other_kind_is_refused() {
        bounded(|| async move {
            for native_dial in [true, false] {
                let _seed = GlobalRng::seed_guard(SEED);
                let net = crate::transport::test_net::subnet();
                let pair = pair("quic-alpn-kind", net.addr(1), net.addr(2), None).await;
                let _permit = if native_dial {
                    pair.b.allow_peer(pair.a_addr, pair.a_keys.public())
                } else {
                    pair.b.allow_native_peer(pair.a_addr, pair.a_keys.public())
                }
                .expect("room");
                let (a, b, b_addr, b_key) = (
                    pair.a.clone(),
                    pair.b.clone(),
                    pair.b_addr,
                    pair.b_keys.public().clone(),
                );
                let (client, server) = drive(&pair.time, Duration::from_secs(60), async move {
                    tokio::join!(
                        async {
                            if native_dial {
                                a.dial_native(b_addr, &b_key).await.map(|_| ())
                            } else {
                                a.connect(b_addr, &b_key).await.map(|_| ())
                            }
                        },
                        async {
                            let attempt = b.accept().await.expect("an attempt");
                            assert_eq!(attempt.is_native(), !native_dial);
                            attempt.establish().await.map(|_| ())
                        }
                    )
                })
                .await;
                assert!(
                    matches!(client, Err(QuicConnectError::Connection(_))),
                    "native dial {native_dial}: {client:?}"
                );
                assert!(server.is_err(), "native dial {native_dial}: {server:?}");
            }
        });
    }

    /// An upgrade attempt from one address still negotiates the upgrade ALPN
    /// while another address waits as native on the same endpoint, and the
    /// native one negotiates its own.
    #[test]
    fn an_upgrade_attempt_beside_a_native_waiter_negotiates_the_upgrade_alpn() {
        bounded(|| async move {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let mut pair = pair("quic-alpn-beside", net.addr(1), net.addr(2), None).await;
            let c_addr = net.addr(3);
            let (c, c_keys) = pair.third(c_addr).await;
            let _upgrade = pair
                .b
                .allow_peer(pair.a_addr, pair.a_keys.public())
                .expect("room");
            let _native = pair
                .b
                .allow_native_peer(c_addr, c_keys.public())
                .expect("room");
            let (a, b, b_addr, b_key) = (
                pair.a.clone(),
                pair.b.clone(),
                pair.b_addr,
                pair.b_keys.public().clone(),
            );
            let a_addr = pair.a_addr;
            let (upgrade, native, accepted) =
                drive(&pair.time, Duration::from_secs(60), async move {
                    tokio::join!(
                        a.connect(b_addr, &b_key),
                        c.dial_native(b_addr, &b_key),
                        async {
                            let mut accepted = Vec::new();
                            for _ in 0..2 {
                                let attempt = b.accept().await.expect("an attempt");
                                let (source, native) = (attempt.source(), attempt.is_native());
                                let connection = attempt.establish().await.expect("established");
                                accepted.push((source, native, connection));
                            }
                            accepted
                        }
                    )
                })
                .await;
            assert_eq!(
                negotiated(&upgrade.expect("upgrade")).as_deref(),
                Some(ALPN)
            );
            assert_eq!(
                negotiated(&native.expect("native")).as_deref(),
                Some(config::NATIVE_ALPN)
            );
            assert_eq!(accepted.len(), 2);
            for (source, native, connection) in &accepted {
                assert_eq!(*native, *source == c_addr, "{source}");
                assert!(*source == a_addr || *source == c_addr, "{source}");
                let expected = if *native { config::NATIVE_ALPN } else { ALPN };
                assert_eq!(
                    negotiated(connection).as_deref(),
                    Some(expected),
                    "{source}"
                );
            }
        });
    }

    /// An address allowed for one kind of connection is refused for the other
    /// while its permit is alive, as for another key, and can be allowed for the
    /// other once it is gone.
    #[test]
    fn an_address_is_allowed_for_one_kind_at_a_time() {
        bounded(|| async move {
            let net = crate::transport::test_net::subnet();
            let pair = pair("quic-alpn-one-kind", net.addr(1), net.addr(2), None).await;
            let upgrade = pair
                .b
                .allow_peer(pair.a_addr, pair.a_keys.public())
                .expect("room");
            assert!(matches!(
                pair.b.allow_native_peer(pair.a_addr, pair.a_keys.public()),
                Err(QuicAllowError::KeyConflict)
            ));
            let again = pair
                .b
                .allow_peer(pair.a_addr, pair.a_keys.public())
                .expect("same kind");
            drop((upgrade, again));
            let native = pair
                .b
                .allow_native_peer(pair.a_addr, pair.a_keys.public())
                .expect("free again");
            assert!(matches!(
                pair.b.allow_peer(pair.a_addr, pair.a_keys.public()),
                Err(QuicAllowError::KeyConflict)
            ));
            drop(native);
        });
    }
}
