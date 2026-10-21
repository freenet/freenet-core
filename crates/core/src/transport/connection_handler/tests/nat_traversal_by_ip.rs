use super::super::mock_transport::MockSocket;
use super::super::*;

fn manager() -> ConnectionStateManager<MockSocket, RealTime> {
    ConnectionStateManager::new(RealTime::new())
}

fn start_traversal(manager: &mut ConnectionStateManager<MockSocket, RealTime>, addr: &str) {
    let (packets, _) = mpsc::channel(1);
    let (result, _) = oneshot::channel();
    assert!(manager.start_nat_traversal(addr.parse().unwrap(), packets, result));
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// Any port of the IP matches, including the extremes; neighbouring IPs and
/// the other address family do not.
#[test]
fn matches_every_port_of_the_ip_and_nothing_else() {
    for port in [0, 9, u16::MAX] {
        let mut m = manager();
        start_traversal(&mut m, &format!("192.0.2.5:{port}"));
        assert!(m.has_nat_traversal_to_ip(ip("192.0.2.5")), "port {port}");
        assert!(!m.has_nat_traversal_to_ip(ip("192.0.2.4")));
        assert!(!m.has_nat_traversal_to_ip(ip("192.0.2.6")));
        assert!(!m.has_nat_traversal_to_ip(ip("::ffff:192.0.2.5")));
    }
    for port in [0, 9, u16::MAX] {
        let mut m = manager();
        start_traversal(&mut m, &format!("[2001:db8::5]:{port}"));
        assert!(m.has_nat_traversal_to_ip(ip("2001:db8::5")), "port {port}");
        assert!(!m.has_nat_traversal_to_ip(ip("2001:db8::4")));
        assert!(!m.has_nat_traversal_to_ip(ip("2001:db8::6")));
    }
}

/// Only an outbound traversal counts: a gateway handshake or an
/// established connection to the IP does not.
#[test]
fn only_a_traversal_counts() {
    let mut m = manager();
    let (packets, _) = mpsc::channel(1);
    assert!(m.start_gateway_handshake("192.0.2.7:1".parse().unwrap(), packets));
    assert!(!m.has_nat_traversal_to_ip(ip("192.0.2.7")));
    start_traversal(&mut m, "192.0.2.7:2");
    assert!(m.has_nat_traversal_to_ip(ip("192.0.2.7")));
    let (inbound, _) = mpsc::channel(1);
    assert!(
        m.complete_nat_traversal("192.0.2.7:2".parse().unwrap(), inbound)
            .is_some()
    );
    assert!(
        !m.has_nat_traversal_to_ip(ip("192.0.2.7")),
        "established is not in progress"
    );
}
