use super::super::mock_transport::MockSocket;
use super::super::*;
use crate::transport::{ListenerExit, TransportHandle};

/// Each way a listener task can end maps to its own `ListenerExit`, carrying
/// the transport error or the join error's own text.
#[tokio::test]
async fn listener_exit_maps_each_join_result() {
    let ended = tokio::spawn(async { TransportError::ChannelClosed }).await;
    assert!(
        matches!(
            ListenerExit::from_join(ended),
            ListenerExit::Error(TransportError::ChannelClosed)
        ),
        "a returned error"
    );

    let panicked = tokio::spawn(async { panic!("listener blew up") })
        .await
        .map(|()| TransportError::ChannelClosed)
        .unwrap_err();
    let text = panicked.to_string();
    let ListenerExit::Panicked(message) = ListenerExit::from_join(Err(panicked)) else {
        panic!("a panic is Panicked");
    };
    assert_eq!(message, text);

    let pending = tokio::spawn(std::future::pending::<TransportError>());
    pending.abort();
    let cancelled = pending.await.unwrap_err();
    let text = cancelled.to_string();
    let ListenerExit::Cancelled(message) = ListenerExit::from_join(Err(cancelled)) else {
        panic!("an abort is Cancelled");
    };
    assert_eq!(message, text);
}

/// The handle wires each part to the UDP transport's own: outbound connect
/// and expected-inbound registration write ONE expected-IP set, the acceptor
/// reads the transport's inbound queue, and the liveness future resolves with
/// what the listen task returned.
#[tokio::test]
async fn udp_handle_shares_one_expected_ip_set_and_reports_the_listener() {
    let expected = Arc::new(DashSet::new());
    let (send_queue, _queue) = mpsc::channel(1);
    let outbound = OutboundConnectionHandler::<MockSocket> {
        send_queue,
        expected_non_gateway: expected.clone(),
    };
    let (inbound_tx, inbound_rx) = mpsc::channel(1);
    let inbound = InboundConnectionHandler {
        new_connection_notifier: inbound_rx,
    };
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let listen_task = tokio::spawn(async move {
        drop(stop_rx.await);
        TransportError::ChannelClosed
    });

    let TransportHandle {
        connector,
        mut acceptor,
        expected_inbound,
        listener,
    } = udp_transport_handle(outbound, inbound, listen_task);

    let inbound_addr: SocketAddr = "192.0.2.10:1000".parse().unwrap();
    expected_inbound.expect_incoming(inbound_addr);
    assert!(expected.contains(&inbound_addr.ip()), "expect_incoming");

    let outbound_addr: SocketAddr = "192.0.2.11:1000".parse().unwrap();
    let _pending = connector
        .connect(TransportKeypair::new().public().clone(), outbound_addr)
        .await;
    assert!(expected.contains(&outbound_addr.ip()), "connect");

    drop(inbound_tx);
    assert!(acceptor.next_connection().await.is_none());

    stop_tx.send(()).unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(5), listener)
        .await
        .expect("the listener exit is reported");
    assert!(
        matches!(exit, ListenerExit::Error(TransportError::ChannelClosed)),
        "{exit:?}"
    );
}
