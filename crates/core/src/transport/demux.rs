//! Splits inbound datagrams by their first byte, so a QUIC endpoint can share
//! the UDP transport's port (#5940).
//!
//! Every packet the UDP transport accepts starts with a plaintext type byte,
//! [`PACKET_TYPE_INTRO`] or [`PACKET_TYPE_SYMMETRIC`]. Every QUIC packet has
//! the fixed bit or the long-header bit set in its first byte. The two sets
//! of first bytes do not overlap, so the first byte alone says which protocol
//! a datagram belongs to.

use std::io;
use std::net::SocketAddr;

use super::Socket;
use super::crypto::{PACKET_TYPE_INTRO, PACKET_TYPE_SYMMETRIC};
use super::metrics::{DemuxClass, TRANSPORT_METRICS};

/// Bits of a QUIC packet's first byte of which at least one is always set.
///
/// `0x80` is the header form bit (RFC 9000 section 17.2: set on every long
/// header packet). `0x40` is the fixed bit (RFC 9000 sections 17.2 and 17.3:
/// set on every long header packet and every short header packet, except
/// Version Negotiation, which has the form bit set instead). The rule is
/// exact only while the QUIC endpoint never clears the fixed bit, so QUIC bit
/// greasing (RFC 9287) must stay disabled on it.
const QUIC_FIRST_BYTE_MASK: u8 = 0xC0;

/// How many non-legacy datagrams in a row are dropped before the receive loop
/// yields to the runtime. `tokio::net::UdpSocket` already takes part in
/// cooperative budgeting, so this only matters for sockets that can return
/// ready forever without yielding, such as the in-memory simulation socket
/// with a deep inbox.
const DROPS_BETWEEN_YIELDS: u32 = 64;

/// Classify a datagram by its first byte. An empty datagram is
/// [`DemuxClass::Other`].
pub(crate) fn classify(packet: &[u8]) -> DemuxClass {
    match packet.first() {
        Some(&PACKET_TYPE_INTRO | &PACKET_TYPE_SYMMETRIC) => DemuxClass::Legacy,
        Some(first) if first & QUIC_FIRST_BYTE_MASK != 0 => DemuxClass::Quic,
        _ => DemuxClass::Other,
    }
}

/// A [`Socket`] that hands the listener only the datagrams of the UDP
/// transport and counts the rest.
///
/// It exists so a QUIC endpoint can use the same UDP port as the UDP
/// transport (#5940). Until that endpoint exists, QUIC-shaped datagrams are
/// dropped here and counted as `demux_quic`; datagrams that are neither are
/// dropped and counted as `demux_other`. Legacy datagrams are returned
/// unchanged, and are counted as `demux_legacy` so the ratio is visible.
///
/// It wraps the socket below the [`Socket`] trait, so every implementation of
/// the trait (the real UDP sockets and `SimulationSocket`) gets the same
/// behaviour.
pub struct DemuxSocket<S> {
    inner: S,
}

impl<S: Socket> Socket for DemuxSocket<S> {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            inner: S::bind(addr).await?,
        })
    }

    /// Errors from the inner socket are returned unchanged, because the
    /// listener's receive error tracking depends on them.
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mut dropped_in_a_row: u32 = 0;
        loop {
            let (len, from) = self.inner.recv_from(buf).await?;
            let class = classify(&buf[..len]);
            TRANSPORT_METRICS.record_demux(from, class);
            if class == DemuxClass::Legacy {
                return Ok((len, from));
            }
            dropped_in_a_row += 1;
            if dropped_in_a_row % DROPS_BETWEEN_YIELDS == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.inner.send_to(buf, target).await
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.inner.send_to_blocking(buf, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// The expected class of a first byte, written out as literal ranges
    /// rather than through the constants the implementation uses.
    fn expected_class(first: u8) -> DemuxClass {
        match first {
            0x01 | 0x02 => DemuxClass::Legacy,
            0x40..=0xFF => DemuxClass::Quic,
            _ => DemuxClass::Other,
        }
    }

    #[test]
    fn classify_matches_the_rule_for_every_first_byte() {
        for first in 0..=255u8 {
            for packet in [vec![first], vec![first, 0xAA], vec![first; 1200]] {
                assert_eq!(
                    classify(&packet),
                    expected_class(first),
                    "first byte {first:#04x}, length {}",
                    packet.len()
                );
            }
        }
        assert_eq!(classify(&[]), DemuxClass::Other, "empty datagram");
    }

    /// Only the first byte matters: later bytes never change the class.
    #[test]
    fn classify_ignores_everything_after_the_first_byte() {
        assert_eq!(classify(&[0x01, 0xFF, 0xFF]), DemuxClass::Legacy);
        assert_eq!(classify(&[0x00, 0x01, 0x02]), DemuxClass::Other);
        assert_eq!(classify(&[0x3F, 0xC0, 0x40]), DemuxClass::Other);
    }

    enum Step {
        Packet(SocketAddr, Vec<u8>),
        Error(io::ErrorKind),
    }

    /// An inner socket that replays a script, then waits forever.
    struct ScriptedSocket {
        script: Mutex<VecDeque<Step>>,
        sent: Mutex<Vec<(Vec<u8>, SocketAddr)>>,
    }

    impl ScriptedSocket {
        fn new(script: Vec<Step>) -> Self {
            Self {
                script: Mutex::new(script.into()),
                sent: Mutex::new(Vec::new()),
            }
        }
    }

    impl Socket for ScriptedSocket {
        async fn bind(_addr: SocketAddr) -> io::Result<Self> {
            Ok(Self::new(Vec::new()))
        }

        async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            let step = self.script.lock().unwrap().pop_front();
            match step {
                Some(Step::Packet(from, data)) => {
                    buf[..data.len()].copy_from_slice(&data);
                    Ok((data.len(), from))
                }
                Some(Step::Error(kind)) => Err(io::Error::new(kind, "scripted")),
                None => std::future::pending().await,
            }
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
            self.sent.lock().unwrap().push((buf.to_vec(), target));
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
            self.sent.lock().unwrap().push((buf.to_vec(), target));
            Ok(buf.len())
        }
    }

    /// Receive, failing the test instead of hanging when no datagram is
    /// returned (the script runs dry and the inner socket then waits forever).
    async fn recv_within_timeout(
        socket: &DemuxSocket<ScriptedSocket>,
        buf: &mut [u8],
    ) -> (usize, SocketAddr) {
        tokio::time::timeout(std::time::Duration::from_secs(5), socket.recv_from(buf))
            .await
            .expect("a legacy datagram is returned")
            .unwrap()
    }

    #[tokio::test]
    async fn recv_returns_only_legacy_and_counts_every_datagram() {
        let from: SocketAddr = "10.254.1.1:4001".parse().unwrap();
        let legacy = vec![PACKET_TYPE_SYMMETRIC, 9, 8, 7];
        let socket = DemuxSocket {
            inner: ScriptedSocket::new(vec![
                Step::Packet(from, vec![0xC3, 1, 2, 3]),
                Step::Packet(from, vec![0x00, 1, 2, 3]),
                Step::Packet(from, Vec::new()),
                Step::Packet(from, legacy.clone()),
            ]),
        };
        let mut buf = [0u8; 64];
        let (len, got_from) = recv_within_timeout(&socket, &mut buf).await;
        assert_eq!(got_from, from);
        assert_eq!(&buf[..len], legacy.as_slice());
        assert_eq!(
            TRANSPORT_METRICS.demux_from(from),
            vec![
                DemuxClass::Quic,
                DemuxClass::Other,
                DemuxClass::Other,
                DemuxClass::Legacy
            ]
        );
    }

    /// A long run of dropped datagrams ends at the next legacy one, and the
    /// loop lets other tasks run on the way. The test runtime has one thread
    /// and the inner socket is always ready, so the other task can only run
    /// if the receive loop yields.
    #[tokio::test]
    async fn recv_survives_a_flood_of_dropped_datagrams() {
        let from: SocketAddr = "10.254.1.2:4002".parse().unwrap();
        let flood = 3 * DROPS_BETWEEN_YIELDS as usize + 1;
        let mut script: Vec<Step> = (0..flood)
            .map(|_| Step::Packet(from, vec![0x40, 0]))
            .collect();
        script.push(Step::Packet(from, vec![PACKET_TYPE_INTRO]));
        let socket = DemuxSocket {
            inner: ScriptedSocket::new(script),
        };
        let other_task_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = other_task_ran.clone();
        tokio::spawn(async move { flag.store(true, std::sync::atomic::Ordering::SeqCst) });
        let mut buf = [0u8; 8];
        let (len, _) = recv_within_timeout(&socket, &mut buf).await;
        assert_eq!(&buf[..len], &[PACKET_TYPE_INTRO]);
        assert!(
            other_task_ran.load(std::sync::atomic::Ordering::SeqCst),
            "the receive loop must yield during a flood of dropped datagrams"
        );
        let log = TRANSPORT_METRICS.demux_from(from);
        assert_eq!(log.len(), flood + 1);
        assert_eq!(
            log.iter().filter(|c| **c == DemuxClass::Quic).count(),
            flood
        );
    }

    #[tokio::test]
    async fn recv_returns_inner_errors_unchanged() {
        let socket = DemuxSocket {
            inner: ScriptedSocket::new(vec![Step::Error(io::ErrorKind::ConnectionReset)]),
        };
        let mut buf = [0u8; 8];
        let err = socket.recv_from(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(err.to_string(), "scripted");
    }

    #[tokio::test]
    async fn send_passes_through() {
        let target: SocketAddr = "10.254.1.3:4003".parse().unwrap();
        let socket = DemuxSocket {
            inner: ScriptedSocket::new(Vec::new()),
        };
        assert_eq!(socket.send_to(&[0xC0, 1], target).await.unwrap(), 2);
        assert_eq!(socket.send_to_blocking(&[0x01], target).unwrap(), 1);
        assert_eq!(
            *socket.inner.sent.lock().unwrap(),
            vec![(vec![0xC0, 1], target), (vec![0x01], target)]
        );
    }

    /// Through the real bind path of the transport: a QUIC-shaped datagram
    /// sent to a bound transport is counted and dropped, and a legacy
    /// connection between two transports still establishes and carries a
    /// message. Fails if the transport binds its socket without the wrapper,
    /// because nothing would then be counted for either sender.
    #[tokio::test]
    async fn real_transport_counts_quic_and_still_connects() -> anyhow::Result<()> {
        use super::super::{TransportKeypair, UdpTransportSocket, create_connection_handler};
        use std::net::{IpAddr, Ipv4Addr};
        use std::time::Duration;

        fn free_udp_port() -> u16 {
            std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }
        async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while !done() {
                assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        let host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let gateway_port = free_udp_port();
        let joiner_port = free_udp_port();
        let gateway_addr = SocketAddr::new(host, gateway_port);
        let joiner_addr = SocketAddr::new(host, joiner_port);
        let gateway_keypair = TransportKeypair::new();
        let gateway_pub = gateway_keypair.public().clone();

        let (_gateway_out, mut gateway_in, _gateway_task) =
            create_connection_handler::<UdpTransportSocket>(
                gateway_keypair,
                host,
                gateway_port,
                true,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        let (mut joiner_out, _joiner_in, _joiner_task) =
            create_connection_handler::<UdpTransportSocket>(
                TransportKeypair::new(),
                host,
                joiner_port,
                false,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;

        // A QUIC-shaped datagram and a stray one, from a unique address.
        let injector = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        let injector_addr = injector.local_addr()?;
        injector.send_to(&[0xC1, 0, 0, 0], gateway_addr)?;
        injector.send_to(&[0x00, 0, 0, 0], gateway_addr)?;
        wait_until("the injected datagrams are counted", || {
            TRANSPORT_METRICS.demux_from(injector_addr).len() == 2
        })
        .await;
        assert_eq!(
            TRANSPORT_METRICS.demux_from(injector_addr),
            vec![DemuxClass::Quic, DemuxClass::Other]
        );

        // A legacy connection still works.
        let connecting = joiner_out.connect(gateway_pub, gateway_addr).await;
        let accepting = gateway_in.next_connection();
        let (joiner_conn, gateway_conn) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(connecting, accepting)
        })
        .await?;
        let mut joiner_conn = joiner_conn?;
        let mut gateway_conn = gateway_conn.expect("the gateway accepts the connection");
        joiner_conn.send("through the demux").await?;
        let received = tokio::time::timeout(Duration::from_secs(10), gateway_conn.recv()).await??;
        assert_eq!(&received[8..], b"through the demux");

        let seen = TRANSPORT_METRICS.demux_from(joiner_addr);
        assert!(!seen.is_empty(), "the joiner's packets were counted");
        assert!(
            seen.iter().all(|c| *c == DemuxClass::Legacy),
            "every joiner packet is legacy: {seen:?}"
        );
        Ok(())
    }
}
