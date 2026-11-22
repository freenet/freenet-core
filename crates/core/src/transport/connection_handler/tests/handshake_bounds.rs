//! Bounds on state a gateway keeps for senders it has not authenticated:
//! pending handshakes, the per-sender intro budget and the expected-IP set.

use std::net::Ipv6Addr;
use std::time::Duration;

use super::super::mock_transport::MockSocket;
use super::super::*;
use crate::simulation::VirtualTime;

type Manager = ConnectionStateManager<MockSocket, VirtualTime>;

fn manager() -> (Manager, VirtualTime) {
    let time = VirtualTime::new();
    (ConnectionStateManager::new(time.clone()), time)
}

fn addr(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

/// Starts a gateway handshake and returns its receiver, which the caller
/// keeps alive for as long as the handshake should look pending.
fn pend(m: &mut Manager, a: SocketAddr) -> mpsc::Receiver<PacketData<UnknownEncryption>> {
    let (tx, rx) = mpsc::channel(1);
    assert!(m.start_gateway_handshake(a, tx));
    rx
}

/// Every source port of one IP draws on one intro budget, so changing the
/// port does not buy more attempts; other IPs are unaffected, and the budget
/// refills when the window passes.
#[test]
fn intro_budget_is_shared_across_source_ports() {
    let (mut m, time) = manager();
    for port in 0..GW_INTRO_BUDGET_PER_GROUP as u16 {
        let a = addr(&format!("192.0.2.1:{}", 40_000 + port));
        assert!(!m.intro_budget_exhausted(&a), "port {port} within budget");
        m.record_intro(a);
    }
    assert!(
        m.intro_budget_exhausted(&addr("192.0.2.1:50000")),
        "a fresh port of the same IP must find the budget used up"
    );
    assert!(
        !m.intro_budget_exhausted(&addr("192.0.2.2:40000")),
        "another IP has its own budget"
    );
    time.advance(GW_INTRO_BUDGET_WINDOW + Duration::from_millis(1));
    assert!(
        !m.intro_budget_exhausted(&addr("192.0.2.1:50000")),
        "the budget refills after the window"
    );
}

/// IPv6 senders are grouped by /64: interface identifiers within one prefix
/// share a budget, a different prefix does not. An IPv4-mapped address counts
/// as the IPv4 address it carries, not as one big group.
#[test]
fn intro_budget_groups_ipv6_by_64_prefix() {
    let (mut m, _time) = manager();
    for i in 0..GW_INTRO_BUDGET_PER_GROUP {
        let a = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, i as u16), 1000));
        m.record_intro(a);
    }
    let same_prefix = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 9, 9, 9, 9), 2000));
    let other_prefix = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 2, 0, 0, 0, 1), 1000));
    assert!(m.intro_budget_exhausted(&same_prefix));
    assert!(!m.intro_budget_exhausted(&other_prefix));

    for i in 0..GW_INTRO_BUDGET_PER_GROUP {
        m.record_intro(SocketAddr::from((
            Ipv4Addr::new(198, 51, 100, 1).to_ipv6_mapped(),
            1000 + i as u16,
        )));
    }
    assert!(m.intro_budget_exhausted(&addr("198.51.100.1:5")));
    assert!(
        !m.intro_budget_exhausted(&addr("198.51.100.2:5")),
        "a mapped address must not pull every IPv4 address into its group"
    );
}

/// The budget table cannot grow past its limit however many sender IPs
/// appear, and a new sender is still admitted at the limit.
#[test]
fn intro_budget_table_is_bounded() {
    let (mut m, _time) = manager();
    let extra = 2_000usize;
    for i in 0..GW_MAX_TRACKED_INTRO_GROUPS + extra {
        let a = SocketAddr::from((Ipv4Addr::from(0x0a00_0000u32 + i as u32), 1000));
        m.record_intro(a);
        assert!(m.intro_budgets.len() <= GW_MAX_TRACKED_INTRO_GROUPS);
    }
    let newcomer = addr("203.0.113.9:1000");
    assert!(!m.intro_budget_exhausted(&newcomer));
    m.record_intro(newcomer);
    assert!(m.intro_budgets.len() <= GW_MAX_TRACKED_INTRO_GROUPS);
    assert!(m.intro_budgets.contains_key(&newcomer.ip()));
}

/// Expired budget entries are removed by the periodic sweep.
#[test]
fn intro_budget_entries_expire() {
    let (mut m, time) = manager();
    m.record_intro(addr("192.0.2.1:1"));
    m.cleanup_expired();
    assert_eq!(m.intro_budgets.len(), 1, "still inside the window");
    time.advance(GW_INTRO_BUDGET_WINDOW + Duration::from_millis(1));
    m.cleanup_expired();
    assert!(m.intro_budgets.is_empty());
}

/// One sender group cannot hold more than its share of pending handshakes,
/// however many source ports it uses; other senders are still admitted.
#[test]
fn pending_handshakes_are_capped_per_sender_group() {
    let (mut m, _time) = manager();
    let mut keep = Vec::new();
    for port in 0..GW_MAX_PENDING_HANDSHAKES_PER_GROUP as u16 {
        let a = addr(&format!("192.0.2.1:{}", 40_000 + port));
        assert_eq!(m.gateway_admission(a, keep.len()), GatewayAdmission::Admit);
        keep.push(pend(&mut m, a));
    }
    assert_eq!(
        m.gateway_admission(addr("192.0.2.1:50000"), keep.len()),
        GatewayAdmission::GroupFull
    );
    assert_eq!(
        m.gateway_admission(addr("192.0.2.2:40000"), keep.len()),
        GatewayAdmission::Admit,
        "one sender must not lock out another"
    );

    // IPv6: the cap applies to the /64.
    let mut keep6 = Vec::new();
    for i in 0..GW_MAX_PENDING_HANDSHAKES_PER_GROUP {
        let a = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, i as u16), 1000));
        keep6.push(pend(&mut m, a));
    }
    let a = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 7, 7, 7, 7), 1000));
    assert_eq!(m.gateway_admission(a, 0), GatewayAdmission::GroupFull);
    let other = SocketAddr::from((Ipv6Addr::new(0x2001, 0xdb8, 0, 2, 0, 0, 0, 1), 1000));
    assert_eq!(m.gateway_admission(other, 0), GatewayAdmission::Admit);
}

/// The total of pending handshakes is capped regardless of sender.
#[test]
fn pending_handshakes_are_capped_in_total() {
    let (m, _time) = manager();
    let a = addr("192.0.2.1:40000");
    assert_eq!(
        m.gateway_admission(a, GW_MAX_PENDING_HANDSHAKES - 1),
        GatewayAdmission::Admit
    );
    assert_eq!(
        m.gateway_admission(a, GW_MAX_PENDING_HANDSHAKES),
        GatewayAdmission::GlobalFull
    );
}

/// A finished or failed handshake frees its share.
#[test]
fn a_finished_handshake_frees_its_share() {
    let (mut m, _time) = manager();
    let mut keep = Vec::new();
    for port in 0..GW_MAX_PENDING_HANDSHAKES_PER_GROUP as u16 {
        keep.push(pend(&mut m, addr(&format!("192.0.2.1:{}", 40_000 + port))));
    }
    let probe = addr("192.0.2.1:50000");
    assert_eq!(m.gateway_admission(probe, 0), GatewayAdmission::GroupFull);
    m.fail_gateway_handshake(&addr("192.0.2.1:40000"));
    assert_eq!(m.gateway_admission(probe, 0), GatewayAdmission::Admit);
}

/// Stale-entry removal looks only at the sender's own IP: it removes a closed
/// session there and leaves live sessions and other IPs alone.
#[test]
fn remove_stale_from_ip_touches_only_that_ip() {
    let (mut m, _time) = manager();
    let establish = |m: &mut Manager, a: &str| {
        let a = addr(a);
        let _hs = pend(m, a);
        let (inbound, rx) = mpsc::channel(1);
        assert!(m.complete_gateway_handshake(a, inbound));
        rx
    };
    let dead_same_ip = establish(&mut m, "192.0.2.1:1000");
    let _live_same_ip = establish(&mut m, "192.0.2.1:1001");
    let dead_other_ip = establish(&mut m, "192.0.2.2:1000");
    drop(dead_same_ip);
    drop(dead_other_ip);

    m.remove_stale_from_ip(addr("192.0.2.1:2000"));
    assert!(m.get_state(&addr("192.0.2.1:1000")).is_none());
    assert!(m.is_established(&addr("192.0.2.1:1001")));
    assert!(
        m.get_state(&addr("192.0.2.2:1000")).is_some(),
        "another IP's entry is not this call's business"
    );
}

/// The expected-IP set cannot grow past its limit.
#[test]
fn expected_ip_set_is_bounded() {
    let set = DashSet::new();
    for i in 0..MAX_EXPECTED_NON_GATEWAY * 3 {
        record_expected_ip(&set, IpAddr::V4(Ipv4Addr::from(0x0a00_0000u32 + i as u32)));
        assert!(set.len() <= MAX_EXPECTED_NON_GATEWAY);
    }
    let last = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1));
    record_expected_ip(&set, last);
    assert!(set.contains(&last), "the newest entry is always recorded");
}

/// The gateway branch of the listener applies the bounds: it charges the
/// per-group intro budget, checks admission, and no longer keys its limit by
/// `IP:port`. Source scrape, because the listener loop owns the sockets and
/// tasks and cannot be driven from a unit test.
#[test]
fn gateway_listener_applies_the_bounds() {
    let src = include_str!("../../connection_handler.rs");
    let start = src
        .find("// Gateway intro packet handling")
        .expect("gateway branch");
    let end = src[start..]
        .find("gw_connection_tasks.push(task);")
        .expect("end of gateway branch")
        + start;
    let branch = &src[start..end];
    for needle in [
        "intro_budget_exhausted(&remote_addr)",
        "gateway_admission(remote_addr, gw_connection_tasks.len())",
        "record_intro(remote_addr)",
    ] {
        assert!(branch.contains(needle), "gateway branch must call {needle}");
    }
    assert!(
        !branch.contains("is_rate_limited") && !branch.contains("record_asym_attempt"),
        "the gateway branch must not key its intro limit by IP:port"
    );
    let unsolicited = src
        .find("Ignoring unknown packet (not an intro")
        .expect("non-gateway branch");
    assert!(
        !src[unsolicited - 400..unsolicited].contains("expected_non_gateway.insert")
            && !src[unsolicited - 400..unsolicited].contains("record_expected_ip"),
        "an unsolicited datagram must not be recorded as an expected peer"
    );
}
