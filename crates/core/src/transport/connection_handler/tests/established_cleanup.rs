use super::super::mock_transport::MockSocket;
use super::super::*;

fn manager() -> ConnectionStateManager<MockSocket, RealTime> {
    ConnectionStateManager::new(RealTime::new())
}

/// Establishes `addr` through the gateway-handshake path and returns the
/// receiving end of its inbound packet channel.
fn establish(
    manager: &mut ConnectionStateManager<MockSocket, RealTime>,
    addr: &str,
) -> mpsc::Receiver<InboundPacket> {
    let addr: SocketAddr = addr.parse().unwrap();
    let (packets, _) = mpsc::channel(1);
    assert!(manager.start_gateway_handshake(addr, packets));
    let (inbound, rx) = mpsc::channel(1);
    assert!(manager.complete_gateway_handshake(addr, inbound));
    rx
}

/// An `Established` entry whose `PeerConnection` has ended (inbound receiver
/// dropped) is only replaced when the same address sends again, so without the
/// sweep it holds its channel buffer for the life of the process. A live one
/// must survive the same sweep.
#[test]
fn cleanup_expired_drops_established_entries_with_a_closed_channel() {
    let mut m = manager();
    let live_addr: SocketAddr = "192.0.2.1:1000".parse().unwrap();
    let dead_addr: SocketAddr = "192.0.2.2:1000".parse().unwrap();
    let _live_rx = establish(&mut m, "192.0.2.1:1000");
    let dead_rx = establish(&mut m, "192.0.2.2:1000");

    m.cleanup_expired();
    assert!(m.is_established(&live_addr));
    assert!(
        m.is_established(&dead_addr),
        "a connection whose receiver is still alive must not be swept"
    );

    drop(dead_rx);
    m.cleanup_expired();
    assert!(
        m.get_state(&dead_addr).is_none(),
        "an Established entry with a closed inbound channel must be removed"
    );
    assert!(m.is_established(&live_addr), "a live entry must be kept");
}

/// The sweep touches only `Established` and `RecentlyClosed`: in-progress
/// handshakes keep their entries.
#[test]
fn cleanup_expired_keeps_handshakes_in_progress() {
    let mut m = manager();
    let gw: SocketAddr = "192.0.2.3:1".parse().unwrap();
    let nat: SocketAddr = "192.0.2.4:1".parse().unwrap();
    let (gw_packets, _gw_rx) = mpsc::channel(1);
    assert!(m.start_gateway_handshake(gw, gw_packets));
    let (nat_packets, _nat_rx) = mpsc::channel(1);
    let (result, _result_rx) = oneshot::channel();
    assert!(m.start_nat_traversal(nat, nat_packets, result));

    m.cleanup_expired();
    assert!(m.has_gateway_handshake(&gw));
    assert!(m.has_nat_traversal(&nat));
}
