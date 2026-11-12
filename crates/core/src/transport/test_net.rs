//! Simulated addresses for transport tests, each test its own subnet.
//!
//! Several test logs are process-wide and keyed by the remote's socket
//! address (`TRANSPORT_METRICS.quic_events_of` and its siblings), so two
//! tests that pick the same address can read each other's events when they
//! run in one process (#6115). Tests used to pick their subnets as
//! hand-written literals, and duplicates went unnoticed: `cargo nextest`, as
//! CI runs it, gives each test its own process, so only plain `cargo test`
//! sees a collision.
//!
//! [`subnet`] hands out a different `/24` on every call, from one counter
//! for the whole process, so no two callers ever share one. The transport
//! test modules build their simulated addresses only from it; the
//! `no_literal_test_addresses` pin below fails on a literal one.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};

/// The first second octet handed out. No literal address elsewhere in
/// `crates/` is in `10.128.0.0` to `10.191.255.255`: they use `10.0` to
/// `10.101` and `10.2xx`. The one computed address that can land in it is a
/// ring-location probe in `operations/subscribe/op_ctx_task.rs`, which no
/// socket or simulated network ever sees.
const FIRST_OCTET: u32 = 128;
const LAST_OCTET: u32 = 191;

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A `/24` no other caller in this process gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Subnet {
    b: u8,
    c: u8,
}

/// The next unused subnet. Panics after 16384 in one process.
pub(crate) fn subnet() -> Subnet {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let b = FIRST_OCTET + n / 256;
    assert!(
        b <= LAST_OCTET,
        "test_net handed out every subnet ({n} calls)"
    );
    Subnet {
        b: b as u8,
        c: (n % 256) as u8,
    }
}

impl Subnet {
    /// `10.b.c.host:5000`.
    pub(crate) fn addr(self, host: u8) -> SocketAddr {
        self.addr_port(host, 5000)
    }

    /// `10.b.c.host:port`.
    pub(crate) fn addr_port(self, host: u8, port: u16) -> SocketAddr {
        SocketAddr::from((self.ip(host), port))
    }

    /// `10.b.c.host`.
    pub(crate) fn ip(self, host: u8) -> Ipv4Addr {
        Ipv4Addr::new(10, self.b, self.c, host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subnets_are_distinct_and_in_range() {
        let taken: Vec<Subnet> = (0..600).map(|_| subnet()).collect();
        let mut sorted = taken.clone();
        sorted.sort_by_key(|s| (s.b, s.c));
        sorted.dedup();
        assert_eq!(sorted.len(), taken.len(), "no subnet handed out twice");
        for s in taken {
            assert!((FIRST_OCTET..=LAST_OCTET).contains(&u32::from(s.b)));
            assert_eq!(s.addr(2), SocketAddr::from(([10, s.b, s.c, 2], 5000)));
        }
    }

    /// The test modules that build simulated QUIC networks or read the
    /// address-keyed test logs. A new one belongs here.
    const MODULES: [(&str, &str); 13] = [
        ("quic/tests.rs", include_str!("quic/tests.rs")),
        ("quic/path/tests.rs", include_str!("quic/path/tests.rs")),
        (
            "quic/path/tests/route.rs",
            include_str!("quic/path/tests/route.rs"),
        ),
        (
            "quic/path/tests/pacer.rs",
            include_str!("quic/path/tests/pacer.rs"),
        ),
        (
            "quic/path/tests/node_counts.rs",
            include_str!("quic/path/tests/node_counts.rs"),
        ),
        // Production code too: its test module builds a simulated pair.
        ("quic/path/inbound.rs", include_str!("quic/path/inbound.rs")),
        ("quic/memory_tests.rs", include_str!("quic/memory_tests.rs")),
        ("quic/liveness.rs", include_str!("quic/liveness.rs")),
        (
            "quic/recycle/cap_tests.rs",
            include_str!("quic/recycle/cap_tests.rs"),
        ),
        (
            "quic/upgrade/driver/tests.rs",
            include_str!("quic/upgrade/driver/tests.rs"),
        ),
        (
            "peer_connection/ack_policy_tests.rs",
            include_str!("peer_connection/ack_policy_tests.rs"),
        ),
        (
            "peer_connection/ack_policy_tests/quic_data.rs",
            include_str!("peer_connection/ack_policy_tests/quic_data.rs"),
        ),
        (
            "peer_connection/ack_policy_tests/quic_send.rs",
            include_str!("peer_connection/ack_policy_tests/quic_send.rs"),
        ),
    ];

    /// Marks a literal address the scan below accepts.
    const EXEMPT: &str = concat!("test_net", ": exempt (");

    /// The literal private addresses in `source`, comments stripped first:
    /// a quoted `10.` address, `Ipv4Addr::new(10,`, or an octet array
    /// starting `([10,` (as `SocketAddr::from(([10, ...], port))` takes). The QUIC test modules
    /// take every simulated address from [`subnet`], so any hit is a subnet
    /// picked by hand, which is how #6114 collided with #6110's test. This is
    /// stricter than rejecting duplicates: a unique literal today is a
    /// duplicate as soon as someone copies it.
    ///
    /// A line carrying the [`EXEMPT`] comment, `test_net: exempt (<why>)`,
    /// is skipped. It is for an address that is on no simulated network and
    /// that no address-keyed test log is read for.
    fn literal_addresses(source: &str) -> Vec<(usize, String)> {
        let mut hits = Vec::new();
        for (n, line) in source.lines().enumerate() {
            if line.contains(EXEMPT) {
                continue;
            }
            let code = line.split_once("//").map_or(line, |(code, _)| code);
            let compact: String = code.split_whitespace().collect();
            if code.contains("\"10.")
                || compact.contains("Ipv4Addr::new(10,")
                || compact.contains("([10,")
            {
                hits.push((n + 1, line.trim().to_string()));
            }
        }
        hits
    }

    #[test]
    fn no_literal_test_addresses() {
        let mut found = Vec::new();
        for (path, source) in MODULES {
            for (line, text) in literal_addresses(source) {
                found.push(format!("{path}:{line}: {text}"));
            }
        }
        assert!(
            found.is_empty(),
            "take the subnet from transport::test_net::subnet() instead:\n{}",
            found.join("\n")
        );
    }

    /// The scan finds each form it looks for, and not one in a comment.
    #[test]
    fn the_literal_scan_finds_what_it_looks_for() {
        let source = concat!(
            "let a: SocketAddr = \"10.201.3.1:5000\".parse().unwrap();\n",
            "let b = SocketAddr::from((Ipv4Addr::new( 10, 201, 3, 2), 5000));\n",
            "// \"10.201.3.3:5000\" in a comment\n",
            "let c = net.addr(1);\n",
            "let d = Ipv4Addr::new(10, 77, 0, 1); // test_net: exempt (no network)\n",
            "let e = \"10.9.9.9:1\"; // test_net: exempt-ish is not the marker\n",
            "let f = SocketAddr::from(([10, 201, 3, 4], 5000));\n",
            "let g = SocketAddr::from(([ 10 , 201, octet, 5], 5000));\n",
            "// SocketAddr::from(([10, 201, 3, 6], 5000)) in a comment\n",
        );
        let lines: Vec<usize> = literal_addresses(source)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(lines, [1, 2, 6, 7, 8]);
    }
}
