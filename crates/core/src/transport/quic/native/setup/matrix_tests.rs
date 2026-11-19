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
//! class and order, over the skews the connect op can produce. One attempt
//! meets it except under handshake loss (gate 15), which the client's
//! overlapping second attempt (`OVERLAP_BEFORE_END`) brings to the criterion.
//!
//! Not measured here: half-open server states.
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
    /// Conditions beyond the original matrix (gates 15 and 17).
    extra: Extra,
}

/// Conditions the original matrix held fixed: no loss on QUIC handshake
/// packets, a constant delay, and the default binding lifetime.
#[derive(Debug, Clone, Copy)]
struct Extra {
    /// Loss on QUIC long-header Initial and Handshake packets (gate 15).
    handshake_loss_pct: u64,
    /// Each datagram's one-way delay is `rtt / 2` plus a uniform draw in
    /// `[0, jitter)`, so datagrams can arrive out of order (gate 17).
    jitter: Duration,
    /// How long a NAT binding lasts after its last refresh (gate 17).
    binding_lifetime: Duration,
}

impl Default for Extra {
    fn default() -> Self {
        Self {
            handshake_loss_pct: 0,
            jitter: Duration::ZERO,
            binding_lifetime: DEFAULT_BINDING_LIFETIME,
        }
    }
}

/// Whether `datagram` is a QUIC long-header Initial or Handshake packet.
fn is_quic_handshake(datagram: &[u8]) -> bool {
    match datagram.first() {
        // Long header (0x80) with the fixed bit (0x40); the packet type is
        // bits 4-5: 0 Initial, 2 Handshake.
        Some(first) if first & 0xC0 == 0xC0 => matches!((first >> 4) & 0x3, 0 | 2),
        _ => false,
    }
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

/// How one run went: its end, and what the NATs dropped (all, and those
/// dropped by a binding that had expired).
#[derive(Debug, Clone, PartialEq, Eq)]
struct CellRun {
    end: RunEnd,
    filtered: u64,
    expired: u64,
}

/// Runs one setup in `cell` with `seed` at `port` of `net` (each run binds
/// two fresh addresses, since the simulated network registry is keyed by
/// address; the port comes from the run's place in its seed range, not from
/// a process-wide counter, so addresses do not depend on test order).
async fn run_cell(
    cell: Cell,
    seed: u64,
    net: crate::transport::test_net::Subnet,
    port: u16,
) -> CellRun {
    let _seed = GlobalRng::seed_guard(seed);
    let network = format!("native-matrix-{}-{port}", net.ip(1));
    let (client_addr, server_addr) = (net.addr_port(1, port), net.addr_port(2, port));
    let pair = pair(&network, client_addr, server_addr, None).await;
    set_network_nat(
        &network,
        pair.time.clone(),
        [
            (client_addr.ip(), cell.client_nat),
            (server_addr.ip(), cell.server_nat),
        ],
        cell.extra.binding_lifetime,
    );
    let rng = std::sync::Mutex::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    // splitmix64, so the losses and delays repeat from the seed.
    let draw = move || {
        let mut state = rng.lock().unwrap();
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let one_way = cell.rtt / 2;
    let loss = cell.punch_loss_pct;
    let extra = cell.extra;
    set_packet_delivery_callback(
        &network,
        Some(Arc::new(move |_, _, _, datagram: &[u8]| {
            let punch = datagram.len() == PUNCH_LEN && datagram.first() == Some(&PUNCH_FIRST_BYTE);
            if punch && loss > 0 && draw() % 100 < loss {
                return PacketDeliveryDecision::Drop;
            }
            if extra.handshake_loss_pct > 0
                && is_quic_handshake(datagram)
                && draw() % 100 < extra.handshake_loss_pct
            {
                return PacketDeliveryDecision::Drop;
            }
            let jitter_nanos = u64::try_from(extra.jitter.as_nanos()).unwrap_or(u64::MAX);
            let jitter = if jitter_nanos > 0 {
                Duration::from_nanos(draw() % jitter_nanos)
            } else {
                Duration::ZERO
            };
            PacketDeliveryDecision::DelayedDelivery(one_way + jitter)
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
    let expired = crate::transport::in_memory_socket::nat_expired(&network);
    clear_network_nat(&network);
    let end = match (client, server) {
        (Ok(_), Ok(_)) => RunEnd::Established,
        (client, server) => RunEnd::Failed {
            client: client.err().map(|e| format!("{e}")),
            server: server.err().map(|e| format!("{e}")),
        },
    };
    CellRun {
        end,
        filtered,
        expired,
    }
}

/// Runs `seeds` setups in `cell`, in seed order, on a subnet of their own.
fn run_seeds_full(cell: Cell, seeds: std::ops::Range<u64>) -> Vec<CellRun> {
    let (tx, rx) = std::sync::mpsc::channel();
    bounded(move || async move {
        let net = crate::transport::test_net::subnet();
        let mut runs = Vec::new();
        for (i, seed) in seeds.enumerate() {
            let port = 20_000 + u16::try_from(i).expect("fewer seeds than ports");
            runs.push(run_cell(cell, 0x0059_40A5_C000 + seed, net, port).await);
        }
        tx.send(runs).ok();
    });
    rx.recv().expect("the runs ended")
}

/// Runs `seeds` setups in `cell` and returns how many established and the
/// failures by kind.
fn run_seeds(cell: Cell, seeds: std::ops::Range<u64>) -> (usize, usize, Vec<RunEnd>) {
    let total = seeds.end - seeds.start;
    let ends: Vec<RunEnd> = run_seeds_full(cell, seeds)
        .into_iter()
        .map(|run| run.end)
        .collect();
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
                extra: Extra::default(),
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
        extra: Extra::default(),
    };
    let (tx, rx) = std::sync::mpsc::channel();
    bounded(move || async move {
        let net = crate::transport::test_net::subnet();
        tx.send(run_cell(cell, 0x0059_40A5_C0FF, net, 20_000).await)
            .ok();
    });
    let CellRun { end, filtered, .. } = rx.recv().expect("the run ended");
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
    let (mut runs, mut one_sided) = (0, Vec::new());
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
                            extra: Extra::default(),
                        };
                        let (ok, total, failures) = run_seeds(cell, seeds.clone());
                        runs += total;
                        one_sided.extend(
                            failures
                                .iter()
                                .filter(|f| f.one_sided())
                                .map(|f| (cell, f.clone())),
                        );
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
    // 10.2 item 20: a one-sided success (one side holds a connection the
    // other never finished) is accepted and counted while it is rare. If
    // this fails, holding the UDP loser until the native connection has
    // carried a frame becomes a gate before native goes above 0%.
    println!("native-matrix one-sided: {} of {runs}", one_sided.len());
    assert!(
        one_sided.len() * 100 <= runs,
        "one-sided setups above 1% ({} of {runs}): {one_sided:#?}",
        one_sided.len()
    );
}

impl RunEnd {
    /// Exactly one side finished the setup.
    fn one_sided(&self) -> bool {
        matches!(self, Self::Failed { client, server } if client.is_some() != server.is_some())
    }
}

/// 10.2 item 20: one-sided successes where they are likeliest, a late side
/// near the edge of the other side's window at 800 ms and 10% punch loss.
/// The matrix stops at 4 s (late server) and 8 s (late client); past that
/// the late side has too little of the other's window left, and a setup
/// could finish on one side only. Measured, not asserted beyond the premise
/// that these cells fail: the rate is printed.
#[test]
fn one_sided_setups_at_the_window_edge() {
    let mut report = Vec::new();
    let mut failed = 0;
    for (late, skew_ms) in [
        (Late::Server, 4_500u64),
        (Late::Server, 5_000),
        (Late::Server, 5_500),
        (Late::Server, 6_000),
        (Late::Client, 8_500),
        (Late::Client, 9_000),
        (Late::Client, 10_000),
        (Late::Client, 11_000),
    ] {
        let cell = Cell {
            client_nat: NatFiltering::PortRestricted,
            server_nat: NatFiltering::PortRestricted,
            late,
            skew: Duration::from_millis(skew_ms),
            punch_loss_pct: 10,
            rtt: Duration::from_millis(800),
            extra: Extra::default(),
        };
        let (ok, total, failures) = run_seeds(cell, 0..10);
        failed += failures.len();
        let one_sided = failures.iter().filter(|f| f.one_sided()).count();
        report.push((late, skew_ms, ok, one_sided, total));
    }
    println!("native-matrix window edge (late, skew ms, established, one-sided, runs): {report:?}");
    assert!(failed > 0, "premise: setups failed at the window edge");
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
        extra: Extra::default(),
    };
    let (ok, total, _) = run_seeds(cell, 0..4);
    assert_eq!(
        ok, 0,
        "a server 6 s late misses the client ({ok}/{total} established)"
    );
}

/// The class pairs every gate row runs: each endpoint-independent class on
/// both sides, and full cone against port-restricted both ways.
fn class_pairs() -> Vec<(NatFiltering, NatFiltering)> {
    let mut pairs: Vec<_> = NatFiltering::ALL.iter().map(|n| (*n, *n)).collect();
    pairs.push((NatFiltering::Full, NatFiltering::PortRestricted));
    pairs.push((NatFiltering::PortRestricted, NatFiltering::Full));
    pairs
}

/// Seeds per gate cell.
const SEEDS: u64 = 20;
/// Seeds per cell for gate 15, whose criterion is judged on more.
const GATE_15_SEEDS: u64 = 100;

/// One gate cell's results, for the gate tests.
struct GateCell {
    cell: Cell,
    ok: usize,
    total: usize,
    one_sided: usize,
    failures: Vec<RunEnd>,
    expired: u64,
}

/// Runs `cells`, [`SEEDS`] seeds each (gate 15: [`GATE_15_SEEDS`]), several cells at a
/// time (each cell's runs stay on one thread, in seed order, so results are
/// as if run alone), printing each.
fn run_gate_cells(gate: &str, cells: Vec<Cell>) -> Vec<GateCell> {
    let threads = std::thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 8));
    let seeds = if gate == "15" {
        0..GATE_15_SEEDS
    } else {
        0..SEEDS
    };
    let mut results = Vec::new();
    for chunk in cells.chunks(threads) {
        let runs: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = chunk
                .iter()
                .map(|cell| {
                    let (cell, seeds) = (*cell, seeds.clone());
                    scope.spawn(move || (cell, run_seeds_full(cell, seeds)))
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("cell ran"))
                .collect()
        });
        for (cell, runs) in runs {
            let ok = runs.iter().filter(|r| r.end == RunEnd::Established).count();
            let failures: Vec<RunEnd> = runs
                .iter()
                .map(|r| r.end.clone())
                .filter(|e| *e != RunEnd::Established)
                .collect();
            let one_sided = failures.iter().filter(|f| f.one_sided()).count();
            let expired = runs.iter().map(|r| r.expired).sum();
            println!(
                "native-matrix gate {gate} client={} server={} late={:?} skew={:?} rtt={:?} \
                 {:?}: {ok}/{} one-sided {one_sided} expired {expired}",
                cell.client_nat.label(),
                cell.server_nat.label(),
                cell.late,
                cell.skew,
                cell.rtt,
                cell.extra,
                runs.len()
            );
            results.push(GateCell {
                cell,
                ok,
                total: runs.len(),
                one_sided,
                failures,
                expired,
            });
        }
    }
    results
}

/// Every cell of `results` at `floor` or more, and one-sided setups at most
/// `one_sided_pct` percent of the runs.
fn assert_gate(gate: &str, results: &[GateCell], floor: f64, one_sided_pct: usize) {
    let below: Vec<_> = results
        .iter()
        .filter(|r| (r.ok as f64) < floor * r.total as f64)
        .map(|r| (r.cell, r.ok, r.total, &r.failures))
        .collect();
    assert!(
        below.is_empty(),
        "gate {gate} cells below {floor}: {below:#?}"
    );
    let runs: usize = results.iter().map(|r| r.total).sum();
    let one_sided: usize = results.iter().map(|r| r.one_sided).sum();
    println!("native-matrix gate {gate} one-sided: {one_sided} of {runs}");
    assert!(
        one_sided * 100 <= runs * one_sided_pct,
        "gate {gate}: one-sided setups above {one_sided_pct}% ({one_sided} of {runs})"
    );
}

/// A gate cell for `client_nat` against `server_nat`, 10% punch loss.
fn gate_cell(
    (client_nat, server_nat): (NatFiltering, NatFiltering),
    late: Late,
    skew_ms: u64,
    rtt_ms: u64,
    extra: Extra,
) -> Cell {
    Cell {
        client_nat,
        server_nat,
        late,
        skew: Duration::from_millis(skew_ms),
        punch_loss_pct: 10,
        rtt: Duration::from_millis(rtt_ms),
        extra,
    }
}

/// Gate 14 (design note 10.2): a late server up to 4 s plus the response's
/// one-way travel, since the joiner plans from the response's arrival. MEETS
/// the 95% criterion (20 of 20 in every cell).
#[test]
fn native_setup_gate_14_late_server_with_travel() {
    let mut cells = Vec::new();
    for pair in class_pairs() {
        for rtt in [300u64, 800] {
            cells.push(gate_cell(
                pair,
                Late::Server,
                4_000 + rtt / 2,
                rtt,
                Extra::default(),
            ));
        }
    }
    assert_gate("14", &run_gate_cells("14", cells), 0.95, 1);
}

/// Gate 15: 10% loss on QUIC Initial and Handshake packets as well as on
/// punches, at 800 ms, judged on 100 seeds per cell. One attempt did not
/// meet the 95% criterion (80-85% on 20 seeds); overlapping attempts
/// (`OVERLAP_BEFORE_END`) are held to it. A premise checks the loss is
/// applied.
#[test]
fn native_setup_gate_15_handshake_loss_meets_the_criterion() {
    let lossy = Extra {
        handshake_loss_pct: 10,
        ..Extra::default()
    };
    let mut cells = Vec::new();
    for pair in class_pairs() {
        for (late, skew) in [
            (Late::Server, 0u64),
            (Late::Server, 2_000),
            (Late::Server, 4_000),
            (Late::Client, 0),
            (Late::Client, 4_000),
            (Late::Client, 8_000),
        ] {
            cells.push(gate_cell(pair, late, skew, 800, lossy));
        }
    }
    let results = run_gate_cells("15", cells);
    assert_gate("15", &results, 0.95, 1);
    let failures: usize = results.iter().map(|r| r.failures.len()).sum();
    assert!(failures > 0, "premise: the handshake loss is applied");
}

/// Gate 17, jitter: each datagram delayed up to half the round trip more,
/// so datagrams reorder. Costs nothing at 300 ms or away from the window
/// edges (95% floor there). At 800 ms on the edges (late server 4 s, late
/// client 8 s) it does NOT meet the criterion: 10 and 7 of 20 measured,
/// mostly one-sided; held to a 25% baseline. Option (b) (PR-4) is the gate
/// for these. A premise checks the jitter is applied.
#[test]
fn native_setup_gate_17_jitter_baseline() {
    let mut edge = Vec::new();
    let mut rest = Vec::new();
    for pair in class_pairs() {
        for rtt in [300u64, 800] {
            let jitter = Extra {
                jitter: Duration::from_millis(rtt / 2),
                ..Extra::default()
            };
            for (late, skew) in [
                (Late::Server, 0u64),
                (Late::Server, 4_000),
                (Late::Client, 0),
                (Late::Client, 8_000),
            ] {
                let cell = gate_cell(pair, late, skew, rtt, jitter);
                if rtt == 800 && skew > 0 {
                    edge.push(cell);
                } else {
                    rest.push(cell);
                }
            }
        }
    }
    assert_gate("17-jitter", &run_gate_cells("17-jitter", rest), 0.95, 1);
    let edge = run_gate_cells("17-jitter-edge", edge);
    // Measured 7 to 13 of 20 one-sided per edge cell pair; the bound leaves
    // margin. Above 1% makes option (b) a gate (10.2 item 20).
    assert_gate("17-jitter-edge", &edge, 0.25, 60);
    let failures: usize = edge.iter().map(|r| r.failures.len()).sum();
    assert!(failures > 0, "premise: the jitter is applied");
}

/// Gate 17, binding lifetime: NOT demonstrated. A 5 s lifetime (shorter than
/// every window, and far under any real NAT's) never expires a binding
/// inside a setup, which the expiry count shows (zero). That is a
/// measurement of this model with these timings (the setup punches every
/// 250 ms and every QUIC packet refreshes the binding), so it fails if the
/// punch interval or the refresh rule changes, and it is not a claim that
/// the setup survives short bindings. A lifetime short enough to expire one
/// (200 ms) is under the round trip of every cell (300 and 800 ms), so every
/// setup fails whatever it does; rows at it were dropped as uninformative.
/// The shortest lifetime that could matter (between the punch interval and
/// about one round trip) is unmeasured.
#[test]
fn native_setup_gate_17_binding_lifetime() {
    let at = |lifetime_ms: u64| Extra {
        binding_lifetime: Duration::from_millis(lifetime_ms),
        ..Extra::default()
    };
    let mut cells = Vec::new();
    for pair in class_pairs() {
        for rtt in [300u64, 800] {
            for (late, skew) in [
                (Late::Server, 0u64),
                (Late::Server, 4_000),
                (Late::Client, 0),
                (Late::Client, 8_000),
            ] {
                cells.push(gate_cell(pair, late, skew, rtt, at(5_000)));
            }
        }
    }
    let five_s = run_gate_cells("17-binding-5s", cells);
    assert_gate("17-binding-5s", &five_s, 0.95, 1);
    let expired: u64 = five_s.iter().map(|r| r.expired).sum();
    assert_eq!(expired, 0, "a 5 s binding never expires inside a setup");
}

/// Determinism: the same cell run twice, on different subnets and so
/// different addresses, ends the same way for every seed. The margins above
/// are only meaningful if a seed decides its run.
#[test]
fn a_gate_cell_runs_the_same_twice() {
    let cell = gate_cell(
        (NatFiltering::PortRestricted, NatFiltering::PortRestricted),
        Late::Server,
        2_000,
        800,
        Extra {
            handshake_loss_pct: 10,
            jitter: Duration::from_millis(200),
            ..Extra::default()
        },
    );
    let ends = || -> Vec<RunEnd> {
        run_seeds_full(cell, 0..20)
            .into_iter()
            .map(|r| r.end)
            .collect()
    };
    let first = ends();
    assert!(
        first.iter().any(|e| *e != RunEnd::Established),
        "premise: some runs fail, so there is something to differ"
    );
    assert_eq!(ends(), first);
}

/// Symmetric NAT (A5c-2): a native setup with a symmetric peer never
/// connects, in either role and whatever the other side's class, full cone
/// included. Each side dials the other's own address, the stand-in for what a
/// peer learns of a symmetric node (the port its NAT opened toward a gateway,
/// which admits nobody else), and the setup admits only the peer's known
/// address. Strict NAT is Decision 7, tracked in #6247 (not a 1.0 must).
#[test]
fn native_setups_with_a_symmetric_nat() {
    let sym = NatFiltering::Symmetric;
    let mut report = Vec::new();
    for (client_nat, server_nat) in [
        (sym, sym),
        (sym, NatFiltering::PortRestricted),
        (NatFiltering::PortRestricted, sym),
        (sym, NatFiltering::Full),
        (NatFiltering::Full, sym),
    ] {
        for late in [Late::Client, Late::Server] {
            let cell = Cell {
                client_nat,
                server_nat,
                late,
                skew: Duration::from_secs(1),
                punch_loss_pct: 0,
                rtt: Duration::from_millis(300),
                extra: Extra::default(),
            };
            let (ok, total, failures) = run_seeds(cell, 0..5);
            let one_sided = failures.iter().filter(|f| f.one_sided()).count();
            report.push((
                client_nat.label(),
                server_nat.label(),
                late,
                ok,
                total,
                one_sided,
                failures.first().cloned(),
            ));
        }
    }
    for row in &report {
        println!("native-matrix symmetric {row:?}");
    }
    let established: Vec<_> = report.iter().filter(|row| row.3 > 0).collect();
    assert!(
        established.is_empty(),
        "a native setup with a symmetric peer connected: {established:?}"
    );
    // Both sides failed, not one, in every run: no one-sided success.
    for row in &report {
        assert_eq!(row.5, 0, "one-sided: {row:?}");
    }
}

/// The symmetric model is in the path: with the server's NAT made
/// port-restricted, the same cell connects every run.
#[test]
fn a_symmetric_pair_connects_once_neither_side_is_symmetric() {
    let cell = Cell {
        client_nat: NatFiltering::PortRestricted,
        server_nat: NatFiltering::PortRestricted,
        late: Late::Server,
        skew: Duration::from_secs(1),
        punch_loss_pct: 0,
        rtt: Duration::from_millis(300),
        extra: Extra::default(),
    };
    let (ok, total, failures) = run_seeds(cell, 0..5);
    assert_eq!(ok, total, "{failures:?}");
}
