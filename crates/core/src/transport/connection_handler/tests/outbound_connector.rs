use super::super::mock_transport::MockSocket;
use super::super::*;
use crate::transport::OutboundConnector;

type Queue = mpsc::Receiver<(SocketAddr, ConnectionEvent<MockSocket>)>;

fn handler() -> (OutboundConnectionHandler<MockSocket>, Queue) {
    let (send_queue, queue) = mpsc::channel(1);
    let handler = OutboundConnectionHandler {
        send_queue,
        expected_non_gateway: Arc::new(DashSet::new()),
    };
    (handler, queue)
}

fn remote() -> (TransportPublicKey, SocketAddr) {
    (
        TransportKeypair::new().public().clone(),
        "192.0.2.7:31337".parse().unwrap(),
    )
}

/// The first stage queues a start for exactly the key and address given,
/// and registers the remote IP as expected (so the transport accepts the
/// peer's handshake reply); the second stage passes the transport's error
/// through unchanged.
#[tokio::test]
async fn connect_queues_the_start_and_passes_the_transport_error_through() {
    let (handler, mut queue) = handler();
    let (key, addr) = remote();
    let connector: &dyn OutboundConnector = &handler;

    let pending = connector
        .connect(key.clone(), addr, NatAttemptOrigin::ConnectAcceptor)
        .await;

    assert!(handler.expected_non_gateway.contains(&addr.ip()));
    let (
        queued_addr,
        ConnectionEvent::ConnectionStart {
            remote_public_key,
            open_connection,
            origin,
        },
    ) = queue.recv().await.expect("a start is queued");
    assert_eq!(queued_addr, addr);
    assert_eq!(remote_public_key, key);
    assert_eq!(
        origin,
        NatAttemptOrigin::ConnectAcceptor,
        "origin passes through"
    );

    open_connection
        .send(Err(TransportError::ProtocolVersionMismatch {
            expected: "1".into(),
            actual: "2".into(),
        }))
        .ok()
        .expect("the pending connect is still waiting");
    let Err(err) = pending.await else {
        panic!("expected the transport's error");
    };
    assert!(
        matches!(err, TransportError::ProtocolVersionMismatch { .. }),
        "{err:?}"
    );
}

/// If the transport drops the attempt without answering, the connect fails
/// as `ConnectionEstablishmentFailure`.
#[tokio::test]
async fn connect_reports_an_abandoned_attempt_as_an_establishment_failure() {
    let (handler, mut queue) = handler();
    let (key, addr) = remote();
    let connector: &dyn OutboundConnector = &handler;

    let pending = connector
        .connect(key, addr, NatAttemptOrigin::RingPeerNoTransport)
        .await;
    drop(queue.recv().await.expect("a start is queued"));

    let Err(err) = pending.await else {
        panic!("expected a failure");
    };
    assert!(
        matches!(err, TransportError::ConnectionEstablishmentFailure { .. }),
        "{err:?}"
    );
}

/// If the transport's queue is gone, the connect fails as `ChannelClosed`.
#[tokio::test]
async fn connect_reports_a_closed_transport_as_channel_closed() {
    let (handler, queue) = handler();
    drop(queue);
    let (key, addr) = remote();
    let connector: &dyn OutboundConnector = &handler;

    let Err(err) = connector
        .connect(key, addr, NatAttemptOrigin::RingPeerNoTransport)
        .await
        .await
    else {
        panic!("expected a failure");
    };
    assert!(matches!(err, TransportError::ChannelClosed), "{err:?}");
}
