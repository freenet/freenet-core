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
        quic_availability: crate::transport::quic::QuicAvailabilityWatch::disabled(),
        upgrade_site: Default::default(),
        quic_endpoint: None,
        quic_stack: std::sync::Weak::new(),
    };
    let (inbound_tx, inbound_rx) = mpsc::channel(1);
    let inbound = InboundConnectionHandler {
        new_connection_notifier: inbound_rx,
        native_joins: None,
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
        quic_endpoint: _,
        quic_availability: _,
    } = udp_transport_handle(outbound, inbound, listen_task, None);

    let inbound_addr: SocketAddr = "192.0.2.10:1000".parse().unwrap();
    expected_inbound.expect_incoming(inbound_addr);
    assert!(expected.contains(&inbound_addr.ip()), "expect_incoming");

    let outbound_addr: SocketAddr = "192.0.2.11:1000".parse().unwrap();
    let _pending = connector
        .connect(
            TransportKeypair::new().public().clone(),
            outbound_addr,
            NatAttemptOrigin::RingPeerNoTransport,
        )
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

/// The listener runs while ANY connector is alive, and exits with
/// `ConnectionClosed` once the last is dropped. This is why the event loop
/// keeps its own connector (`event_loop_keeps_a_connector_alive`): dropping
/// the handshake driver's copy alone must not stop the listener.
#[tokio::test]
async fn listener_outlives_a_dropped_connector_while_a_clone_remains() {
    let channels = Arc::new(dashmap::DashMap::new());
    let addr: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 47_510).into();
    let socket = MockSocket::new(Default::default(), Default::default(), addr, channels).await;
    let (outbound, inbound, listen_task) = OutboundConnectionHandler::config_listener(
        Arc::new(socket),
        TransportKeypair::new(),
        false,
        addr,
        UdpTransportConfig::default(),
        None,
        None,
    )
    .expect("listener setup");
    let TransportHandle {
        connector,
        acceptor: _acceptor,
        expected_inbound: _expected,
        mut listener,
        quic_endpoint: _,
        quic_availability: _,
    } = udp_transport_handle(outbound, inbound, listen_task, None);
    let keepalive = Arc::clone(&connector);

    drop(connector);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut listener)
            .await
            .is_err(),
        "the listener must keep running while a connector remains"
    );

    drop(keepalive);
    let exit = tokio::time::timeout(Duration::from_secs(5), listener)
        .await
        .expect("the listener stops once every connector is gone");
    assert!(
        matches!(exit, ListenerExit::Error(TransportError::ConnectionClosed(a)) if a == addr),
        "{exit:?}"
    );
}

/// `new_listener` puts each `UdpTransportConfig` knob into the listener field
/// of the same name: a swap of the two `Option<usize>` knobs is caught here.
#[tokio::test]
async fn udp_transport_config_reaches_the_listener_by_name() {
    let channels = Arc::new(dashmap::DashMap::new());
    let addr: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 47_511).into();
    let socket = MockSocket::new(Default::default(), Default::default(), addr, channels).await;
    let global = Arc::new(GlobalBandwidthManager::new(1_000_000, None));
    let (listener, _outbound, _inbound) = OutboundConnectionHandler::new_listener(
        Arc::new(socket),
        TransportKeypair::new(),
        false,
        addr,
        UdpTransportConfig {
            bandwidth_limit: Some(1_111),
            global_bandwidth: Some(global.clone()),
            ledbat_min_ssthresh: Some(2_222),
            congestion_config: None,
            ack_version_floor_override: Some((9, 8, 7)),
            legacy_responder_override: None,
            quic: None,
            quic_policy: None,
            advertised_version_override: Some((0, 2, 148)),
            task_registrar: None,
        },
        None,
        None,
    );
    assert_eq!(listener.bandwidth_limit, Some(1_111));
    assert_eq!(listener.ledbat_min_ssthresh, Some(2_222));
    assert!(Arc::ptr_eq(
        listener
            .global_bandwidth
            .as_ref()
            .expect("global bandwidth"),
        &global
    ));
    assert_eq!(listener.ack_version_floor_override, Some((9, 8, 7)));
    assert_eq!(
        version_cmp::parse_version_bytes(&listener.advertised_version).version,
        (0, 2, 148)
    );
    assert!(
        listener.congestion_config.is_some(),
        "an unset congestion config defaults, as before"
    );
}

/// A death the breaker marker recorded.
fn recorded(_: std::time::SystemTime) -> bool {
    true
}

/// A death the breaker marker could not record.
fn not_recorded(_: std::time::SystemTime) -> bool {
    false
}

/// An endpoint death that cannot be recorded in the breaker marker does not
/// end the listener liveness: the next start could not count the exit, so a
/// death at every start would restart the node without bound. The cooldown
/// end and the listen task still end it.
#[tokio::test(start_paused = true)]
async fn an_unrecorded_endpoint_death_does_not_end_the_listener() {
    use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};
    use std::time::Duration;
    let (down_tx, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
    let listen_task = tokio::spawn(std::future::pending::<TransportError>());
    let liveness = tokio::spawn(listener_liveness(
        listen_task,
        Some(GatewayQuicRestartWatch {
            endpoint: Some(watch),
            restore_after: Some(Duration::from_secs(600)),
        }),
        not_recorded,
    ));
    down_tx.send(QuicAvailability::Down).unwrap();
    tokio::time::sleep(Duration::from_secs(599)).await;
    assert!(
        !liveness.is_finished(),
        "an unrecorded death is not an exit"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let exit = liveness.await.unwrap();
    assert!(matches!(exit, ListenerExit::QuicCooldownEnded), "{exit:?}");
}

/// An endpoint already down when the transport opens is watched like a
/// live one, and its death ends the listener liveness at once.
#[tokio::test]
async fn an_endpoint_down_at_open_is_watched() {
    use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};
    use std::time::{Duration, SystemTime};
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    let (_down_tx, down) = QuicAvailabilityWatch::for_test(QuicAvailability::Down);
    let watch = gateway_quic_restart(true, true, false, down, None, now).expect("watched");
    assert!(watch.endpoint.is_some());
    let listen_task = tokio::spawn(std::future::pending::<TransportError>());
    let exit = tokio::time::timeout(
        Duration::from_secs(5),
        listener_liveness(listen_task, Some(watch), recorded),
    )
    .await
    .expect("an earlier death ends it");
    assert!(matches!(exit, ListenerExit::QuicEndpointDown), "{exit:?}");
}

/// With an endpoint to watch (a supervised gateway, #5940), the listener
/// liveness also ends as `QuicEndpointDown` when that endpoint dies, and not
/// when it is released; without one, endpoint death does not end it, and the
/// listen task's own end is reported as before.
#[tokio::test]
async fn listener_liveness_reports_endpoint_death_only_when_watched() {
    use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};
    use std::time::Duration;
    let watching = |endpoint| {
        Some(GatewayQuicRestartWatch {
            endpoint: Some(endpoint),
            restore_after: None,
        })
    };

    let (down_tx, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
    let listen_task = tokio::spawn(std::future::pending::<TransportError>());
    let liveness = tokio::spawn(listener_liveness(listen_task, watching(watch), recorded));
    tokio::task::yield_now().await;
    assert!(!liveness.is_finished());
    down_tx.send(QuicAvailability::Down).unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(5), liveness)
        .await
        .expect("endpoint death ends it")
        .unwrap();
    assert!(matches!(exit, ListenerExit::QuicEndpointDown), "{exit:?}");

    let (released_tx, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let listen_task = tokio::spawn(async move {
        drop(stop_rx.await);
        TransportError::ChannelClosed
    });
    let liveness = tokio::spawn(listener_liveness(listen_task, watching(watch), recorded));
    released_tx.send(QuicAvailability::Released).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!liveness.is_finished(), "a release is not a death");
    stop_tx.send(()).unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(5), liveness)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(exit, ListenerExit::Error(TransportError::ChannelClosed)),
        "{exit:?}"
    );

    // Not watched: a death does not end it.
    let (unwatched_tx, _watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
    let listen_task = tokio::spawn(std::future::pending::<TransportError>());
    let liveness = tokio::spawn(listener_liveness(listen_task, None, recorded));
    unwatched_tx.send(QuicAvailability::Down).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!liveness.is_finished(), "an unwatched death is not an exit");
    liveness.abort();
}

/// A gateway whose QUIC is off for a cooldown ends its listener liveness
/// as `QuicCooldownEnded` once the cooldown is over, and not before.
#[tokio::test(start_paused = true)]
async fn listener_liveness_ends_when_the_cooldown_ends() {
    use std::time::Duration;
    let listen_task = tokio::spawn(std::future::pending::<TransportError>());
    let liveness = tokio::spawn(listener_liveness(
        listen_task,
        Some(GatewayQuicRestartWatch {
            endpoint: None,
            restore_after: Some(Duration::from_secs(600)),
        }),
        recorded,
    ));
    tokio::time::sleep(Duration::from_secs(599)).await;
    assert!(!liveness.is_finished(), "not before the cooldown ends");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let exit = liveness.await.unwrap();
    assert!(matches!(exit, ListenerExit::QuicCooldownEnded), "{exit:?}");
}

/// Who watches what: a peer and an unsupervised gateway nothing; a
/// supervised gateway its live endpoint, and the cooldown end, never sooner
/// than `QUIC_RESTORE_MIN_UPTIME`.
#[test]
fn only_a_supervised_gateway_restarts_for_quic() {
    use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};
    use std::time::{Duration, SystemTime};
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    let up = || QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
    let at = Some(now + Duration::from_secs(600));

    assert!(
        gateway_quic_restart(false, true, false, up(), at, now).is_none(),
        "a peer"
    );
    assert!(
        gateway_quic_restart(true, false, false, up(), at, now).is_none(),
        "unsupervised"
    );
    let watch = gateway_quic_restart(true, true, false, up(), None, now).expect("endpoint");
    assert!(watch.endpoint.is_some() && watch.restore_after.is_none());

    let off = QuicAvailabilityWatch::disabled();
    let watch = gateway_quic_restart(true, true, false, off, at, now).expect("cooldown");
    assert!(watch.endpoint.is_none());
    assert_eq!(watch.restore_after, Some(Duration::from_secs(600)));

    let soon = Some(now + Duration::from_secs(5));
    let watch = gateway_quic_restart(
        true,
        true,
        false,
        QuicAvailabilityWatch::disabled(),
        soon,
        now,
    )
    .expect("cooldown");
    assert_eq!(
        watch.restore_after,
        Some(QUIC_RESTORE_MIN_UPTIME),
        "floored"
    );
    let past = Some(now - Duration::from_secs(5));
    let watch = gateway_quic_restart(
        true,
        true,
        false,
        QuicAvailabilityWatch::disabled(),
        past,
        now,
    )
    .expect("cooldown");
    assert_eq!(watch.restore_after, Some(QUIC_RESTORE_MIN_UPTIME));

    assert!(
        gateway_quic_restart(
            true,
            true,
            false,
            QuicAvailabilityWatch::disabled(),
            None,
            now
        )
        .is_none(),
        "QUIC off by config: nothing to restore"
    );
    assert!(
        gateway_quic_restart(true, false, true, up(), at, now).is_none(),
        "an unverified supervisor does not turn restarts on"
    );
}
