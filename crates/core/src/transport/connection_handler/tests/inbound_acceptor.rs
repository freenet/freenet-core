use super::super::mock_transport::{MockSocket, create_mock_gateway, create_mock_peer};
use super::super::*;
use crate::transport::InboundAcceptor;
use dashmap::DashMap;

/// A connection a joiner opens to a gateway comes out of the gateway's
/// acceptor, through `&mut dyn InboundAcceptor`, as the same connection the
/// raw handler would have yielded: from the joiner's address, with its
/// version. A `next_connection` dropped before anything arrived (here, by a
/// timeout) loses nothing.
#[tokio::test]
async fn acceptor_yields_the_joiners_connection_and_survives_cancellation() -> anyhow::Result<()> {
    let channels = Arc::new(DashMap::new());
    let (_peer_pub, mut peer, peer_addr) =
        create_mock_peer(Default::default(), channels.clone()).await?;
    let (gw_pub, (_gw_out, gw_inbound), gw_addr) =
        create_mock_gateway(Default::default(), channels).await?;
    let mut handler = InboundConnectionHandler {
        new_connection_notifier: gw_inbound,
    };
    let acceptor: &mut dyn InboundAcceptor = &mut handler;

    assert!(
        tokio::time::timeout(Duration::from_millis(20), acceptor.next_connection())
            .await
            .is_err(),
        "premise: nothing to accept yet"
    );

    let joiner = GlobalExecutor::spawn(async move {
        let pending = peer.connect(gw_pub, gw_addr).await;
        let conn = tokio::time::timeout(Duration::from_secs(60), pending).await??;
        Ok::<_, anyhow::Error>(conn)
    });
    let conn = tokio::time::timeout(Duration::from_secs(10), acceptor.next_connection())
        .await?
        .expect("the gateway accepts the joiner");
    let _joiner_side = joiner.await??;

    assert_eq!(conn.remote_addr(), peer_addr);
    assert_eq!(
        conn.remote_version(),
        Some(version_cmp::parse_version_bytes(&PROTOC_VERSION).version)
    );
    Ok(())
}

/// Once the transport stops delivering connections, the acceptor yields
/// `None`, which is what ends the handshake driver's accept loop.
#[tokio::test]
async fn acceptor_yields_none_once_the_transport_stops() {
    let (sender, receiver) = mpsc::channel::<PeerConnection<MockSocket>>(1);
    drop(sender);
    let mut handler = InboundConnectionHandler {
        new_connection_notifier: receiver,
    };
    let acceptor: &mut dyn InboundAcceptor = &mut handler;
    assert!(acceptor.next_connection().await.is_none());
}

/// A `next_connection` that was already waiting when a connection arrived,
/// and is then dropped without being polled again, loses nothing: the next
/// call yields that connection.
#[tokio::test]
async fn acceptor_keeps_a_connection_that_arrives_while_a_dropped_call_waited() -> anyhow::Result<()>
{
    let channels = Arc::new(DashMap::new());
    let (_peer_pub, mut peer, peer_addr) =
        create_mock_peer(Default::default(), channels.clone()).await?;
    let (gw_pub, (_gw_out, mut gw_inbound), gw_addr) =
        create_mock_gateway(Default::default(), channels).await?;
    let joiner = GlobalExecutor::spawn(async move {
        let pending = peer.connect(gw_pub, gw_addr).await;
        let conn = tokio::time::timeout(Duration::from_secs(60), pending).await??;
        Ok::<_, anyhow::Error>(conn)
    });
    let real = tokio::time::timeout(Duration::from_secs(10), gw_inbound.recv())
        .await?
        .expect("premise: the gateway accepts the joiner");
    let _joiner_side = joiner.await??;

    // Feed that connection through a queue the test controls.
    let (tx, rx) = mpsc::channel(1);
    let mut handler = InboundConnectionHandler {
        new_connection_notifier: rx,
    };
    let acceptor: &mut dyn InboundAcceptor = &mut handler;
    let mut waiting = acceptor.next_connection();
    assert!(
        futures::poll!(&mut waiting).is_pending(),
        "premise: the call is waiting"
    );
    assert!(tx.send(real).await.is_ok(), "queued");
    drop(waiting);

    let conn = tokio::time::timeout(Duration::from_secs(1), acceptor.next_connection())
        .await?
        .expect("the connection is still delivered");
    assert_eq!(conn.remote_addr(), peer_addr);
    Ok(())
}
