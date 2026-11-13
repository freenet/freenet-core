//! The `Hello` exchange that opens a native connection (#5940, design 2.1).
//!
//! Each side opens one unidirectional stream, writes its [`Hello`] and ends
//! the stream; each reads the other's first stream. The read is bounded by
//! [`HELLO_MAX`](super::hello::HELLO_MAX), checked from the six-byte prefix
//! before the rest is read, and the whole exchange by the caller's deadline
//! on the caller's clock.
//!
//! A malformed or oversized `Hello` is closed with `HELLO_REFUSED`. A peer
//! whose version this node cannot talk to is closed with
//! `VERSION_INCOMPATIBLE`, this node's 8 version bytes as the reason, so the
//! peer can tell whether it is the one that must update. The version is
//! checked by the UDP handshake's own rules
//! ([`connection_handler::native_version`]).
//!
//! [`connection_handler::native_version`]: crate::transport::connection_handler::native_version

use std::time::Duration;

use super::hello::{HELLO_PREFIX_LEN, Hello, HelloError, declared_len};
use crate::simulation::TimeSource;
use crate::transport::connection_handler::native_version;
use crate::transport::metrics::QuicCloseCode;
use crate::transport::quic::QuicConnection;

/// The other side's `Hello`, and the version it carried as a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerHello {
    pub hello: Hello,
    pub version: (u8, u8, u16),
}

/// Why the exchange did not give a usable `Hello`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HelloFailure {
    /// Nothing complete arrived within the deadline.
    #[error("no Hello within the setup window")]
    TimedOut,
    /// The peer's `Hello` was malformed or too long; this node closed the
    /// connection with `HELLO_REFUSED`.
    #[error("the peer's Hello was refused: {0}")]
    Refused(HelloError),
    /// The peer's version cannot talk to this node's; this node closed the
    /// connection with `VERSION_INCOMPATIBLE`.
    #[error("the peer's version is incompatible: {0}")]
    Incompatible(String),
    /// The peer closed with `VERSION_INCOMPATIBLE`.
    #[error("the peer cannot talk to this node's version")]
    RejectedVersion,
    /// The peer closed with `HELLO_REFUSED`.
    #[error("the peer refused this node's Hello")]
    PeerRefusedHello,
    /// The connection ended or a stream failed for another reason.
    #[error("the connection ended during the Hello exchange: {0}")]
    ConnectionLost(String),
}

/// What reading the peer's `Hello` stream gave.
enum Read {
    Hello(Hello),
    Malformed(HelloError),
    Lost(String),
}

/// Send `own` and read the peer's `Hello` on `connection`, within `limit` on
/// `clock`. `peer_is_gateway` says whether a peer's `VERSION_INCOMPATIBLE`
/// may set the version-mismatch signal, as only a gateway's does on UDP.
///
/// On a refused or incompatible `Hello` this closes the connection with the
/// matching code. On a timeout or a lost connection it closes nothing: the
/// caller drops the connection.
pub(crate) async fn exchange<T: TimeSource>(
    connection: &QuicConnection,
    own: &Hello,
    peer_is_gateway: bool,
    clock: &T,
    limit: Duration,
) -> Result<PeerHello, HelloFailure> {
    let sender = connection.inner().clone();
    let reader = connection.inner().clone();
    let bytes = own.encode();
    let send = async move {
        let mut stream = sender.open_uni().await.map_err(|e| e.to_string())?;
        stream.write_all(&bytes).await.map_err(|e| e.to_string())?;
        stream.finish().map_err(|e| e.to_string())
    };
    let receive = async move {
        let mut stream = match reader.accept_uni().await {
            Ok(stream) => stream,
            Err(e) => return Read::Lost(e.to_string()),
        };
        let mut prefix = [0u8; HELLO_PREFIX_LEN];
        match stream.read_exact(&mut prefix).await {
            Ok(()) => {}
            Err(quinn::ReadExactError::FinishedEarly(_)) => {
                return Read::Malformed(HelloError::TooShort);
            }
            Err(quinn::ReadExactError::ReadError(e)) => return Read::Lost(e.to_string()),
        }
        let total = match declared_len(&prefix) {
            Ok(total) => total,
            Err(error) => return Read::Malformed(error),
        };
        let rest = match stream.read_to_end(total - HELLO_PREFIX_LEN).await {
            Ok(rest) => rest,
            Err(quinn::ReadToEndError::TooLong) => {
                return Read::Malformed(HelloError::LengthMismatch);
            }
            Err(quinn::ReadToEndError::Read(e)) => return Read::Lost(e.to_string()),
        };
        let mut whole = prefix.to_vec();
        whole.extend_from_slice(&rest);
        match Hello::decode(&whole) {
            Ok(hello) => Read::Hello(hello),
            Err(error) => Read::Malformed(error),
        }
    };
    let both = async move { tokio::join!(send, receive) };
    let Some((sent, read)) = clock.timeout(limit, both).await else {
        return Err(HelloFailure::TimedOut);
    };
    let outcome = match read {
        Read::Malformed(error) => {
            connection.close(QuicCloseCode::HelloRefused);
            return Err(HelloFailure::Refused(error));
        }
        Read::Hello(hello) => match native_version::check(&hello.protoc_version) {
            Ok(version) => Ok(PeerHello { hello, version }),
            Err(reason) => {
                connection
                    .closer()
                    .close_with_reason(QuicCloseCode::VersionIncompatible, &native_version::OWN);
                return Err(HelloFailure::Incompatible(reason));
            }
        },
        Read::Lost(reason) => Err(reason),
    };
    match (outcome, sent) {
        (Ok(peer), Ok(())) => Ok(peer),
        (Ok(_), Err(reason)) | (Err(reason), _) => {
            Err(peer_close(connection, peer_is_gateway)
                .unwrap_or(HelloFailure::ConnectionLost(reason)))
        }
    }
}

/// The failure a peer's close names, if it closed with one of the native
/// setup codes; a gateway's `VERSION_INCOMPATIBLE` is handed to the version
/// rules.
fn peer_close(connection: &QuicConnection, peer_is_gateway: bool) -> Option<HelloFailure> {
    let quinn::ConnectionError::ApplicationClosed(close) = connection.inner().close_reason()?
    else {
        return None;
    };
    match QuicCloseCode::from_code(close.error_code.into_inner())? {
        QuicCloseCode::VersionIncompatible => {
            native_version::rejected_by(&close.reason, peer_is_gateway);
            Some(HelloFailure::RejectedVersion)
        }
        QuicCloseCode::HelloRefused => Some(HelloFailure::PeerRefusedHello),
        QuicCloseCode::NodeDroppedPeer
        | QuicCloseCode::Downgrade
        | QuicCloseCode::IdentityMismatch
        | QuicCloseCode::SendAbortLimit
        | QuicCloseCode::PeerNotAcknowledging
        | QuicCloseCode::ControlRefused
        | QuicCloseCode::SessionEnded
        | QuicCloseCode::RecycleForData
        | QuicCloseCode::OverBudget => None,
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::simulation::VirtualTime;
    use crate::transport::quic::QuicPeerPermit;
    use crate::transport::quic::tests::{Pair, bounded, drive, pair};

    const LIMIT: Duration = Duration::from_secs(8);

    fn hello(version: [u8; 8], observed: SocketAddr) -> Hello {
        Hello {
            protoc_version: version,
            caps: 0,
            observed,
        }
    }

    /// `a` connected to `b`: (a's connection, b's connection, b's permit).
    async fn connected(pair: &Pair) -> (QuicConnection, QuicConnection, QuicPeerPermit) {
        let permit = pair
            .b
            .allow_peer(pair.a_addr, pair.a_keys.public())
            .expect("allow-set has room");
        let a = pair.a.clone();
        let b = pair.b.clone();
        let (b_addr, b_key) = (pair.b_addr, pair.b_keys.public().clone());
        let (client, server) = drive(&pair.time, LIMIT, async move {
            tokio::join!(a.connect(b_addr, &b_key), async {
                b.accept().await.expect("an attempt").establish().await
            })
        })
        .await;
        (
            client.expect("client handshake"),
            server.expect("server handshake"),
            permit,
        )
    }

    /// Run `exchange` on both connections at once with their own Hellos.
    async fn both(
        time: &VirtualTime,
        a: QuicConnection,
        a_hello: Hello,
        b: QuicConnection,
        b_hello: Hello,
    ) -> (
        Result<PeerHello, HelloFailure>,
        Result<PeerHello, HelloFailure>,
        QuicConnection,
        QuicConnection,
    ) {
        let clock = time.clone();
        drive(time, LIMIT * 2, async move {
            let (ra, rb) = tokio::join!(
                exchange(&a, &a_hello, false, &clock, LIMIT),
                exchange(&b, &b_hello, false, &clock, LIMIT),
            );
            (ra, rb, a, b)
        })
        .await
    }

    /// The application close code and reason the connection ended with.
    fn closed_with(connection: &QuicConnection) -> Option<(QuicCloseCode, Vec<u8>)> {
        let quinn::ConnectionError::ApplicationClosed(close) = connection.inner().close_reason()?
        else {
            return None;
        };
        Some((
            QuicCloseCode::from_code(close.error_code.into_inner())?,
            close.reason.to_vec(),
        ))
    }

    /// Both sides get the other's Hello, with the version as a tuple.
    #[test]
    fn both_sides_receive_the_others_hello() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-hello-ok", net.addr(1), net.addr(2), None).await;
            let (a, b, _permit) = connected(&pair).await;
            let a_hello = hello(native_version::OWN, pair.b_addr);
            let b_hello = Hello {
                caps: 7,
                ..hello(native_version::OWN, pair.a_addr)
            };
            let (ra, rb, _a, _b) = both(&pair.time, a, a_hello.clone(), b, b_hello.clone()).await;
            let own = crate::transport::connection_handler::OWN_VERSION;
            assert_eq!(
                ra,
                Ok(PeerHello {
                    hello: b_hello,
                    version: own
                })
            );
            assert_eq!(
                rb,
                Ok(PeerHello {
                    hello: a_hello,
                    version: own
                })
            );
        });
    }

    /// A version below this build's floor: the reader closes with
    /// VERSION_INCOMPATIBLE and its own version bytes as the reason, and the
    /// other side reads that as a rejection of its version.
    #[test]
    fn an_incompatible_version_is_closed_with_this_nodes_version() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-hello-old", net.addr(1), net.addr(2), None).await;
            let (a, b, _permit) = connected(&pair).await;
            // 0.1.0 with floor 0.1.0: below every floor this crate can have,
            // and not asking this node to update.
            let old = [0xFF, 0, 1, 0, 0, 0, 0, 1];
            let (ra, rb, _a, b) = both(
                &pair.time,
                a,
                hello(native_version::OWN, pair.b_addr),
                b,
                hello(old, pair.a_addr),
            )
            .await;
            assert!(matches!(ra, Err(HelloFailure::Incompatible(_))), "{ra:?}");
            // b is this build sending an old version, so its own check passes:
            // it may finish before a's close arrives, or read that close.
            assert!(
                matches!(rb, Ok(_) | Err(HelloFailure::RejectedVersion)),
                "{rb:?}"
            );
            let b = drive(&pair.time, LIMIT, async move {
                let _closed = b.inner().closed().await;
                b
            })
            .await;
            assert_eq!(
                closed_with(&b),
                Some((
                    QuicCloseCode::VersionIncompatible,
                    native_version::OWN.to_vec()
                ))
            );
        });
    }

    /// A peer that closes with VERSION_INCOMPATIBLE: the exchange reads it as
    /// a rejection of this node's version, not as a lost connection; with
    /// HELLO_REFUSED, as a refusal of its Hello.
    #[test]
    fn a_peers_setup_close_is_read_by_its_code() {
        bounded(|| async {
            for (code, expected) in [
                (
                    QuicCloseCode::VersionIncompatible,
                    HelloFailure::RejectedVersion,
                ),
                (QuicCloseCode::HelloRefused, HelloFailure::PeerRefusedHello),
            ] {
                let net = crate::transport::test_net::subnet();
                let pair = pair("native-hello-closed", net.addr(1), net.addr(2), None).await;
                let (a, b, _permit) = connected(&pair).await;
                let clock = pair.time.clone();
                let own = hello(native_version::OWN, pair.b_addr);
                b.closer().close_with_reason(code, &native_version::OWN);
                let ra = drive(&pair.time, LIMIT * 2, async move {
                    let ra = exchange(&a, &own, true, &clock, LIMIT).await;
                    drop(b);
                    ra
                })
                .await;
                assert_eq!(ra, Err(expected), "{code:?}");
            }
        });
    }

    /// A peer that closes with any other code, RECYCLE_FOR_DATA (a ramp's
    /// recycle, which a native connection never sees from an honest peer)
    /// included, is no setup failure of its own: the exchange reads it as a
    /// lost connection.
    #[test]
    fn a_peers_other_close_codes_read_as_a_lost_connection() {
        bounded(|| async {
            for code in [QuicCloseCode::RecycleForData, QuicCloseCode::Downgrade] {
                let net = crate::transport::test_net::subnet();
                let pair = pair("native-hello-other-close", net.addr(1), net.addr(2), None).await;
                let (a, b, _permit) = connected(&pair).await;
                let clock = pair.time.clone();
                let own = hello(native_version::OWN, pair.b_addr);
                b.closer().close(code);
                let ra = drive(&pair.time, LIMIT * 2, async move {
                    let ra = exchange(&a, &own, true, &clock, LIMIT).await;
                    drop(b);
                    ra
                })
                .await;
                assert!(
                    matches!(ra, Err(HelloFailure::ConnectionLost(_))),
                    "{code:?}: {ra:?}"
                );
            }
        });
    }

    /// A Hello declaring more than HELLO_MAX is refused from its prefix:
    /// HELLO_REFUSED, before the rest is read.
    #[test]
    fn an_oversized_hello_is_refused_from_its_prefix() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-hello-big", net.addr(1), net.addr(2), None).await;
            let (a, b, _permit) = connected(&pair).await;
            let clock = pair.time.clone();
            let own = hello(native_version::OWN, pair.b_addr);
            let (ra, b) = drive(&pair.time, LIMIT * 2, async move {
                let raw = async {
                    let mut stream = b.inner().open_uni().await.expect("open");
                    // Declares 1025 bytes, then never sends them.
                    stream
                        .write_all(&[b'F', b'N', b'H', b'1', 0x04, 0x01])
                        .await
                        .expect("write");
                };
                let (ra, ()) = tokio::join!(exchange(&a, &own, false, &clock, LIMIT), raw);
                // Let b read the close.
                let _ = b.inner().closed().await;
                (ra, b)
            })
            .await;
            assert_eq!(ra, Err(HelloFailure::Refused(HelloError::TooLong)));
            assert_eq!(
                closed_with(&b),
                Some((QuicCloseCode::HelloRefused, b"hello refused".to_vec()))
            );
        });
    }

    /// A Hello cut short by the end of its stream is refused: before the
    /// six-byte prefix (empty, or 3 bytes) as too short, after it as a length
    /// mismatch.
    #[test]
    fn a_hello_ending_early_is_refused() {
        bounded(|| async {
            let full = hello(native_version::OWN, "198.51.100.1:1".parse().unwrap())
                .encode()
                .len();
            for (keep, expected) in [
                (0, HelloError::TooShort),
                (3, HelloError::TooShort),
                (full - 1, HelloError::LengthMismatch),
            ] {
                let net = crate::transport::test_net::subnet();
                let pair = pair("native-hello-short", net.addr(1), net.addr(2), None).await;
                let (a, b, _permit) = connected(&pair).await;
                let clock = pair.time.clone();
                let own = hello(native_version::OWN, pair.b_addr);
                let bytes = hello(native_version::OWN, pair.a_addr).encode();
                let ra = drive(&pair.time, LIMIT * 2, async move {
                    let raw = async {
                        let mut stream = b.inner().open_uni().await.expect("open");
                        stream.write_all(&bytes[..keep]).await.expect("write");
                        stream.finish().expect("finish");
                    };
                    let (ra, ()) = tokio::join!(exchange(&a, &own, false, &clock, LIMIT), raw);
                    drop(b);
                    ra
                })
                .await;
                assert_eq!(ra, Err(HelloFailure::Refused(expected)), "{keep} bytes");
            }
        });
    }

    /// A peer that sends nothing: the exchange ends at its deadline, on the
    /// virtual clock, and closes nothing.
    #[test]
    fn a_silent_peer_times_out_at_the_deadline() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-hello-silent", net.addr(1), net.addr(2), None).await;
            let (a, b, _permit) = connected(&pair).await;
            let clock = pair.time.clone();
            let own = hello(native_version::OWN, pair.b_addr);
            let started = pair.time.now();
            let (ra, a, _b) = drive(&pair.time, LIMIT * 2, async move {
                let ra = exchange(&a, &own, false, &clock, LIMIT).await;
                (ra, a, b)
            })
            .await;
            assert_eq!(ra, Err(HelloFailure::TimedOut));
            assert!(pair.time.now() - started >= LIMIT);
            assert!(a.inner().close_reason().is_none(), "nothing closed");
        });
    }
}
