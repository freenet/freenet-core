//! The design's 6.2 matrix: native setups between two endpoints behind NATs
//! (`transport::nat_model`), across filtering classes, which side starts
//! late and by how much, punch loss and round-trip time.
//!
//! Each cell runs `connect_native` (the client, the lower key's role) and
//! `accept_native` (the server) on one simulated network, the late side
//! starting `skew` after the other, every datagram delayed by half the
//! round trip and each punch dropped with the cell's probability (seeded per
//! run). A run succeeds when both sides finish the Hello exchange inside
//! their windows.
//!
//! The whole matrix runs in about 30 s (debug build), so it runs with the
//! library tests rather than only nightly. It asserts the design's decision
//! criterion: at least 95% success at 800 ms and 10% punch loss in every
//! class and order, over the skews the connect op can produce. A single
//! attempt that passes it stays (the design's 6.2 decision).
//!
//! Not measured here: in-flight attempts per waiter and half-open server
//! states (the waiter takes one attempt by construction, `NativeWaiters`).
//!
//! A lost punch is lost between the two NATs: it has already opened its
//! sender's binding (`nat_model`), as a real one has, so punch loss only
//! costs the arrivals, and the punches every 250 ms cover them. That is why
//! even 20% loss at 800 ms establishes every run.

use super::*;
use crate::config::GlobalRng;
use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
use crate::transport::in_memory_socket::{
    PacketDeliveryDecision, clear_network_nat, set_network_nat, set_packet_delivery_callback,
};
use crate::transport::nat_model::{DEFAULT_BINDING_LIFETIME, NatFiltering};
use crate::transport::quic::tests::{bounded, drive, pair};
use std::sync::atomic::{AtomicU16, Ordering};

/// Which side starts late.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Late {
    Client,
    Server,
}

/// One cell of the matrix.
#[derive(Debug, Clone, Copy)]
struct Cell {
    client_nat: NatFiltering,
    server_nat: NatFiltering,
    late: Late,
    skew: Duration,
    punch_loss_pct: u64,
    rtt: Duration,
}

/// How one run ended, by side.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RunEnd {
    Established,
    Failed {
        client: Option<String>,
        server: Option<String>,
    },
}

/// Ports for the runs: each run binds two fresh addresses, since the
/// simulated network registry is keyed by address.
static NEXT_PORT: AtomicU16 = AtomicU16::new(20_000);

/// Runs one setup in `cell` with `seed`; returns how it ended and how many
/// datagrams the NATs dropped.
async fn run_cell(cell: Cell, seed: u64, net: crate::transport::test_net::Subnet) -> (RunEnd, u64) {
    let _seed = GlobalRng::seed_guard(seed);
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    let network = format!("native-matrix-{port}");
    let (client_addr, server_addr) = (net.addr_port(1, port), net.addr_port(2, port));
    let pair = pair(&network, client_addr, server_addr, None).await;
    set_network_nat(
        &network,
        pair.time.clone(),
        [
            (client_addr.ip(), cell.client_nat),
            (server_addr.ip(), cell.server_nat),
        ],
        DEFAULT_BINDING_LIFETIME,
    );
    let rng = std::sync::Mutex::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    let one_way = cell.rtt / 2;
    let loss = cell.punch_loss_pct;
    set_packet_delivery_callback(
        &network,
        Some(Arc::new(move |_, _, _, datagram: &[u8]| {
            let punch = datagram.len() == PUNCH_LEN && datagram.first() == Some(&PUNCH_FIRST_BYTE);
            if punch && loss > 0 {
                // splitmix64, so the losses repeat from the seed.
                let mut state = rng.lock().unwrap();
                *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = *state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                if z % 100 < loss {
                    return PacketDeliveryDecision::Drop;
                }
            }
            PacketDeliveryDecision::DelayedDelivery(one_way)
        })),
    );
    let waiters = Arc::new(NativeWaiters::default());
    let (a, b, clock) = (pair.a.clone(), pair.b.clone(), pair.time.clone());
    let (a_key, b_key) = (pair.a_keys.public().clone(), pair.b_keys.public().clone());
    let w = waiters.clone();
    let (client, server) = drive(&pair.time, Duration::from_secs(60), async move {
        let router = {
            let (b, w) = (b.clone(), w.clone());
            tokio::spawn(async move {
                while let Some(attempt) = b.accept().await {
                    w.route(attempt);
                }
            })
        };
        let budgets = (PunchBudget::default(), PunchBudget::default());
        let to_server = NativePeer {
            addr: server_addr,
            key: &b_key,
            is_gateway: false,
        };
        let to_client = NativePeer {
            addr: client_addr,
            key: &a_key,
            is_gateway: false,
        };
        let delay = |late: Late| {
            let clock = clock.clone();
            async move {
                if cell.late == late {
                    clock.sleep(cell.skew).await;
                }
            }
        };
        // The late side is the joiner: its deadline is the common fallback
        // time, measured from the early side's start (the acceptor's
        // decision), as `plan_acceptor_dial` sets it. The early side's window
        // is its own.
        let fallback_at = clock.now() + NATIVE_FALLBACK_AFTER;
        let deadline = |late: Late| {
            if cell.late == late {
                fallback_at
            } else {
                fallback_at + Duration::from_secs(60)
            }
        };
        let both = tokio::join!(
            async {
                delay(Late::Client).await;
                connect_native(&a, &budgets.0, to_server, 0, &clock, deadline(Late::Client)).await
            },
            async {
                delay(Late::Server).await;
                accept_native(
                    &b,
                    &w,
                    &budgets.1,
                    to_client,
                    0,
                    &clock,
                    deadline(Late::Server),
                )
                .await
            },
        );
        router.abort();
        both
    })
    .await;
    set_packet_delivery_callback(&network, None);
    let filtered = crate::transport::in_memory_socket::nat_filtered(&network)
        .values()
        .sum();
    clear_network_nat(&network);
    let end = match (client, server) {
        (Ok(_), Ok(_)) => RunEnd::Established,
        (client, server) => RunEnd::Failed {
            client: client.err().map(|e| format!("{e}")),
            server: server.err().map(|e| format!("{e}")),
        },
    };
    (end, filtered)
}

/// Runs `seeds` setups in `cell` and returns how many established and the
/// failures by kind.
fn run_seeds(cell: Cell, seeds: std::ops::Range<u64>) -> (usize, usize, Vec<RunEnd>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let total = seeds.end - seeds.start;
    bounded(move || async move {
        let net = crate::transport::test_net::subnet();
        let mut ends = Vec::new();
        for seed in seeds {
            ends.push(run_cell(cell, 0x0059_40A5_C000 + seed, net).await.0);
        }
        tx.send(ends).ok();
    });
    let ends = rx.recv().expect("the runs ended");
    let ok = ends.iter().filter(|e| **e == RunEnd::Established).count();
    let failures = ends
        .into_iter()
        .filter(|e| *e != RunEnd::Established)
        .collect();
    (ok, total as usize, failures)
}

/// One row per filtering class, both sides behind it, each order, at a
/// 300 ms round trip with no loss and 1 s of skew: every run establishes.
fn pr_rows() -> Vec<Cell> {
    let mut rows = Vec::new();
    for nat in NatFiltering::ALL {
        for late in [Late::Client, Late::Server] {
            rows.push(Cell {
                client_nat: nat,
                server_nat: nat,
                late,
                skew: Duration::from_secs(1),
                punch_loss_pct: 0,
                rtt: Duration::from_millis(300),
            });
        }
    }
    rows
}

#[test]
fn native_setups_establish_behind_each_nat_class() {
    for cell in pr_rows() {
        let (ok, total, failures) = run_seeds(cell, 0..5);
        assert_eq!(ok, total, "{cell:?}: {failures:?}");
    }
}

/// The premise that the NATs are in the path: the client starts 2 s before
/// the server, so its first punches and Initials reach the server's NAT
/// before the server has sent anything toward it, and are dropped there; the
/// setup still establishes once the server punches.
#[test]
fn the_nat_is_in_the_path() {
    let cell = Cell {
        client_nat: NatFiltering::PortRestricted,
        server_nat: NatFiltering::PortRestricted,
        late: Late::Server,
        skew: Duration::from_secs(2),
        punch_loss_pct: 0,
        rtt: Duration::from_millis(300),
    };
    let (tx, rx) = std::sync::mpsc::channel();
    bounded(move || async move {
        let net = crate::transport::test_net::subnet();
        tx.send(run_cell(cell, 0x0059_40A5_C0FF, net).await).ok();
    });
    let (end, filtered) = rx.recv().expect("the run ended");
    assert_eq!(end, RunEnd::Established);
    assert!(
        filtered > 0,
        "premise: the server's NAT dropped the early datagrams"
    );
}

/// The matrix and the 6.2 decision criterion.
#[test]
fn native_setup_matrix_meets_the_decision_criterion() {
    let mut classes: Vec<(NatFiltering, NatFiltering)> =
        NatFiltering::ALL.iter().map(|n| (*n, *n)).collect();
    classes.push((NatFiltering::Full, NatFiltering::PortRestricted));
    classes.push((NatFiltering::PortRestricted, NatFiltering::Full));
    let rtts: &[u64] = &[50, 300, 800];
    let losses: &[u64] = &[0, 10, 20];
    // The skews the connect op can produce (`plan_acceptor_dial`): a late
    // joiner is dialled native only while at least `MIN_WINDOW_LEFT` (4 s)
    // of the acceptor's window is left, so a late server (the acceptor is the
    // client, 8 s) starts at most 4 s late and a late client (the acceptor is
    // the server, 12 s) at most 8 s late, plus the response's travel.
    // `a_server_starting_more_than_four_seconds_late_misses_the_client` shows
    // why the server's bound matters.
    let skews = |late: Late| -> &'static [u64] {
        match late {
            Late::Client => &[0, 2, 4, 6, 8],
            Late::Server => &[0, 2, 4],
        }
    };
    let seeds = 0..20;
    let mut criterion_failures = Vec::new();
    for &(client_nat, server_nat) in &classes {
        for &rtt in rtts {
            for &loss in losses {
                for late in [Late::Client, Late::Server] {
                    for &skew in skews(late) {
                        let cell = Cell {
                            client_nat,
                            server_nat,
                            late,
                            skew: Duration::from_secs(skew),
                            punch_loss_pct: loss,
                            rtt: Duration::from_millis(rtt),
                        };
                        let (ok, total, failures) = run_seeds(cell, seeds.clone());
                        let rate = ok as f64 / total as f64;
                        println!(
                            "native-matrix client={} server={} late={late:?} skew={skew}s \
                             loss={loss}% rtt={rtt}ms: {ok}/{total}",
                            client_nat.label(),
                            server_nat.label()
                        );
                        if rtt == 800 && loss == 10 && rate < 0.95 {
                            criterion_failures.push((cell, ok, total, failures));
                        }
                    }
                }
            }
        }
    }
    assert!(
        criterion_failures.is_empty(),
        "cells below 95% at 800 ms, 10% punch loss: {criterion_failures:#?}"
    );
}

/// The reason the joiner's plan keeps a late server within 4 s of its client
/// (10.2 item 10): a server that starts 6 s after the client has about 2 s of
/// the client's 8 s window left, too little at 800 ms for the handshake and
/// the Hello, so the setup fails.
#[test]
fn a_server_starting_more_than_four_seconds_late_misses_the_client() {
    let cell = Cell {
        client_nat: NatFiltering::PortRestricted,
        server_nat: NatFiltering::PortRestricted,
        late: Late::Server,
        skew: Duration::from_secs(6),
        punch_loss_pct: 10,
        rtt: Duration::from_millis(800),
    };
    let (ok, total, _) = run_seeds(cell, 0..4);
    assert_eq!(
        ok, 0,
        "a server 6 s late misses the client ({ok}/{total} established)"
    );
}
