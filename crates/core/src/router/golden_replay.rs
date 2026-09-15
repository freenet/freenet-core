//! Golden replay of routing decisions against the soaked build (#4485).
//!
//! The legacy prediction stack (Renegade, the fixed blend, the residual
//! correction) was removed after the hierarchical estimator had been soaked with
//! `FREENET_ROUTING_HIERARCHICAL=1` on a production gateway. That evidence
//! carries over to this build only if it makes the same routing decisions, so
//! this test replays fixed route-event sequences and compares every decision
//! with `golden/<scenario>.txt`. Those files were generated on origin/main
//! `d9fa29522` with the hierarchical flag forced on and the residual correction
//! forced off, which is the soak's configuration.
//!
//! Each line compared covers the full ranked candidate list: which peer sits
//! at each position, its distance, and the bits of every estimate the router
//! acted on (failure probability, time to response start, transfer speed and
//! the expected total time built from them), plus the strategy and the event
//! count behind the decision. Comparison is bit-for-bit; no epsilon.
//!
//! # The one window that is NOT compared
//!
//! On the soaked build, a timing stage the hierarchical estimator could not yet
//! estimate (fewer than 30 samples) fell back to the legacy stack, which
//! blended Renegade in once Renegade's own stage held 10 samples. This build
//! falls back to the same isotonic estimate without the blend. So while either
//! timing stage holds 10 to 29 samples, decisions can legitimately differ. The
//! soaked build's output there also depended on the host wall clock (Renegade's
//! time feature), so it could not be pinned anyway. Those decisions are written
//! as `W` and skipped. Which decisions fall in the window is computed by this
//! harness from the event stream, identically on both builds, so the window
//! cannot widen without the next decision failing.
//! `a_cold_timing_stage_falls_back_to_the_isotonic_estimate_alone` in
//! `router.rs` pins what this build does inside it.
//!
//! # The second, narrower difference
//!
//! The soaked build priced a candidate whose isotonic transfer-speed estimate
//! was zero (the additive per-peer EWMA driven to the clamp) at an unroutable
//! transfer cost. This build floors that estimate. The decisions where the
//! soaked build did this OUTSIDE the cold window are listed in
//! `golden/degenerate.txt`, derived from its decision trace, and skipped here;
//! every other decision must still match. So the floor cannot reach any
//! decision beyond the listed ones without this test failing.
//!
//! The window is computed without looking at the decision, so a few early
//! distance-only decisions (the first 50 events, before prediction starts)
//! fall in it and are skipped too, although nothing in them can differ.
//!
//! # Regenerating
//!
//! Set `FREENET_ROUTER_GOLDEN_WRITE=1` to rewrite the golden files instead of
//! comparing against them. Only ever do that on a build whose routing has itself
//! been soaked. Regenerating to make this test pass defeats its only purpose.
//! Set `FREENET_ROUTER_GOLDEN_TRACE=<dir>` to also write every decision's full
//! values, window included, as JSON lines, for comparing two builds offline.

use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::node::network_status::OpType;
use crate::util::time_source::SharedMockTimeSource;

/// Decisions the soaked build made on a degenerate transfer-speed estimate
/// outside the cold window: `<scenario> <decision>` per line. See the module
/// docs.
const DEGENERATE: &str = include_str!("golden/degenerate.txt");

/// Where the golden files live.
const GOLDEN_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/router/golden");

/// A routing decision is taken every this many events.
const DECISION_EVERY: usize = 8;

/// Candidates offered per decision, one of them without a known location. More
/// than the router's 25-peer window, so the distance truncation is exercised.
const CANDIDATES: usize = 30;

/// Samples at which a timing stage is no longer cold, on each side of the
/// window: Renegade predicted from 10, the hierarchical stage from 30.
const RENEGADE_MIN_SAMPLES: usize = 10;
const HIERARCHICAL_MIN_SAMPLES: usize = 30;

/// The soak's configuration, held for the duration of a replay.
///
/// On the soaked build this was the only line of the file that differed:
///
/// ```ignore
/// fn soaked_configuration() -> (HierarchicalOverrideGuard, CorrectionOverrideGuard) {
///     (force_hierarchical_routing(true), force_residual_correction(false))
/// }
/// ```
///
/// In this build both switches are gone and the soaked configuration is the
/// only one, so there is nothing to hold.
struct SoakedConfiguration;

fn soaked_configuration() -> SoakedConfiguration {
    SoakedConfiguration
}

/// SplitMix64: the traffic's own generator, so the event stream does not depend
/// on how many draws the router makes from `GlobalRng`.
struct Prng(u64);

impl Prng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn normal(&mut self) -> f64 {
        let u1 = self.unit().max(1e-12);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }

    fn location(&mut self) -> Location {
        Location::try_from(self.unit()).expect("unit interval is a valid location")
    }
}

/// FNV-1a, fixed arithmetic so the golden files cannot drift with the Rust
/// version the way `DefaultHasher` could.
struct Fnv(u64);

impl Fnv {
    fn new() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }

    fn add(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

struct Scenario {
    name: &'static str,
    seed: u64,
    events: usize,
    peers: usize,
    /// Share of successes that are timed (a GET's response).
    timed: f64,
    /// Share of successes that also carry a payload transfer.
    transfer: f64,
    base_failure: f64,
    /// Simulated seconds between events, which drives the forgetting horizons.
    clock_step_secs: u64,
    /// From this event on, half the peers invert their reliability.
    drift_at: Option<usize>,
}

/// Balanced traffic: every stage warms up quickly.
const MIXED: Scenario = Scenario {
    name: "mixed_traffic",
    seed: 0x4485_0001,
    events: 2400,
    peers: 40,
    timed: 0.45,
    transfer: 0.30,
    base_failure: 0.08,
    clock_step_secs: 5,
    drift_at: None,
};

/// Shaped like a gateway's stream: about 2 route events a minute, most of them
/// untimed (subscribes, puts, relayed outcomes), timed GETs about 4% and payload
/// transfers about 2%. The timing stages spend a long time cold.
const PRODUCTION_LIKE: Scenario = Scenario {
    name: "production_like",
    seed: 0x4485_0002,
    events: 3000,
    peers: 60,
    timed: 0.04,
    transfer: 0.02,
    base_failure: 0.05,
    clock_step_secs: 27,
    drift_at: None,
};

/// Peers change behaviour halfway through, at a pace the forgetting horizons act on.
const DRIFT: Scenario = Scenario {
    name: "drift",
    seed: 0x4485_0003,
    events: 2400,
    peers: 30,
    timed: 0.35,
    transfer: 0.20,
    base_failure: 0.10,
    clock_step_secs: 60,
    drift_at: Some(1200),
};

/// Timing so rare that neither timing stage ever reaches 10 samples, so every
/// prediction-based decision uses the isotonic fallback for timing.
const TIMING_STARVED: Scenario = Scenario {
    name: "timing_starved",
    seed: 0x4485_0004,
    events: 1600,
    peers: 40,
    timed: 0.004,
    transfer: 0.0,
    base_failure: 0.10,
    clock_step_secs: 10,
    drift_at: None,
};

/// A simulated peer's latent behaviour.
struct SimPeer {
    key: PeerKeyLocation,
    failure_bias: f64,
    /// A contract band this peer serves badly (per-(peer, band) effect).
    bad_band: usize,
    latency_log_bias: f64,
    speed_log: f64,
}

fn make_peers(rng: &mut Prng, count: usize) -> Vec<SimPeer> {
    let pub_key = PeerKeyLocation::random().pub_key().clone();
    (0..count)
        .map(|index| {
            let addr = std::net::SocketAddr::from((
                [
                    10 + (rng.below(200) as u8),
                    rng.below(256) as u8,
                    rng.below(256) as u8,
                    1 + rng.below(250) as u8,
                ],
                1024 + index as u16,
            ));
            SimPeer {
                key: PeerKeyLocation::new(pub_key.clone(), addr),
                failure_bias: rng.unit() * 0.4 - 0.1,
                bad_band: rng.below(8),
                latency_log_bias: rng.normal() * 0.4,
                speed_log: (200_000.0f64).ln() + rng.normal() * 0.8,
            }
        })
        .collect()
}

struct Stream<'a> {
    scenario: &'a Scenario,
    rng: Prng,
    peers: Vec<SimPeer>,
    unlocated: PeerKeyLocation,
}

impl Stream<'_> {
    /// The next route event, always about a peer with a known location, as
    /// the peers a node routes to have. The unlocated peer appears only as a
    /// routing candidate.
    fn event(&mut self, index: usize) -> RouteEvent {
        let scenario = self.scenario;
        // Skewed popularity: a few peers carry most of the traffic.
        let pick = self.rng.unit();
        let peer = &self.peers[((pick * pick) * self.peers.len() as f64) as usize];
        let contract_location = self.rng.location();
        let distance = peer
            .key
            .location()
            .map(|location| contract_location.distance(location).as_f64())
            .unwrap_or(0.5);
        let band = ((contract_location.as_f64() * 8.0) as usize).min(7);
        let drifted = scenario.drift_at.is_some_and(|at| {
            index >= at && peer.key.socket_addr().is_some_and(|a| a.port() % 2 == 0)
        });
        let bias = if drifted {
            -peer.failure_bias
        } else {
            peer.failure_bias
        };
        let band_penalty = if band == peer.bad_band { 0.3 } else { 0.0 };
        let p_failure =
            (scenario.base_failure + 0.4 * distance + bias + band_penalty).clamp(0.0, 0.95);

        let outcome = if self.rng.unit() < p_failure {
            RouteOutcome::Failure
        } else if self.rng.unit() < scenario.timed {
            let latency_log = (0.08f64).ln()
                + 2.0 * distance
                + if drifted {
                    -peer.latency_log_bias
                } else {
                    peer.latency_log_bias
                }
                + 0.5 * self.rng.normal();
            let transfers =
                scenario.timed > 0.0 && self.rng.unit() < scenario.transfer / scenario.timed;
            let (payload_size, payload_transfer_time) = if transfers {
                let bytes = 1_000 + self.rng.below(500_000);
                let speed = (peer.speed_log - 1.5 * distance + 0.5 * self.rng.normal()).exp();
                (bytes, Duration::from_secs_f64(bytes as f64 / speed))
            } else {
                (0, Duration::ZERO)
            };
            RouteOutcome::Success {
                time_to_response_start: Duration::from_secs_f64(latency_log.exp()),
                payload_size,
                payload_transfer_time,
            }
        } else {
            RouteOutcome::SuccessUntimed
        };
        let op_type = match (&outcome, self.rng.below(10)) {
            (RouteOutcome::Success { .. }, _) => Some(OpType::Get),
            (_, 0) => None,
            (_, 1..=4) => Some(OpType::Subscribe),
            (_, 5..=7) => Some(OpType::Put),
            _ => Some(OpType::Get),
        };
        RouteEvent {
            peer: peer.key.clone(),
            contract_location,
            outcome,
            op_type,
        }
    }

    /// Distinct peers for one decision, plus the unlocated one.
    fn candidates(&mut self) -> Vec<PeerKeyLocation> {
        let mut indices: Vec<usize> = (0..self.peers.len()).collect();
        for i in 0..indices.len() {
            let j = i + self.rng.below(indices.len() - i);
            indices.swap(i, j);
        }
        let mut candidates: Vec<PeerKeyLocation> = indices
            .into_iter()
            .take(CANDIDATES - 1)
            .map(|index| self.peers[index].key.clone())
            .collect();
        let position = self.rng.below(candidates.len() + 1);
        candidates.insert(position, self.unlocated.clone());
        candidates
    }
}

fn strategy_tag(strategy: &RoutingStrategy) -> &'static str {
    match strategy {
        RoutingStrategy::DistanceBased => "D",
        RoutingStrategy::PredictionBased => "P",
        RoutingStrategy::PredictionFallback => "F",
    }
}

/// The index of `peer` in `candidates`, by address identity.
fn position_of(candidates: &[PeerKeyLocation], peer: &PeerKeyLocation) -> usize {
    candidates
        .iter()
        .position(|candidate| std::ptr::eq(candidate, peer))
        .expect("the router returns references into the candidate slice")
}

/// Replay one scenario: the golden lines, and (when tracing) the full trace.
fn replay(
    scenario: &Scenario,
    hook: &mut dyn FnMut(&Router, &[PeerKeyLocation], Location),
) -> (Vec<String>, Vec<serde_json::Value>) {
    let _soaked = soaked_configuration();
    // `select_closest_peers` shuffles with `GlobalRng` before its distance cut.
    let _rng = GlobalRng::seed_guard(scenario.seed);
    let mut rng = Prng(scenario.seed);
    let peers = make_peers(&mut rng, scenario.peers);
    let unlocated = PeerKeyLocation::with_unknown_addr(peers[0].key.pub_key().clone());
    let mut stream = Stream {
        scenario,
        rng,
        peers,
        unlocated,
    };
    let clock = SharedMockTimeSource::new();
    let mut router = Router::new(&[])
        .with_time_source(Arc::new(clock.clone()))
        .with_max_connections(200);

    let (mut timed, mut transfers) = (0usize, 0usize);
    let in_window =
        |samples: usize| (RENEGADE_MIN_SAMPLES..HIERARCHICAL_MIN_SAMPLES).contains(&samples);
    let mut lines = Vec::new();
    let mut trace = Vec::new();
    for index in 0..scenario.events {
        if index % DECISION_EVERY == 0 {
            let candidates = stream.candidates();
            let target = stream.rng.location();
            hook(&router, &candidates, target);
            // Every decision is taken, window or not, so the shuffle's draws
            // from `GlobalRng` stay aligned between builds.
            let (selected, decision) = router.select_k_best_peers_with_telemetry(
                candidates.iter(),
                target,
                candidates.len(),
            );
            let window = in_window(timed) || in_window(transfers);
            let positions: Vec<usize> = selected
                .iter()
                .map(|peer| position_of(&candidates, peer))
                .collect();
            let mut hash = Fnv::new();
            hash.add(decision.total_routing_events as u64);
            for (position, candidate) in positions.iter().zip(&decision.candidates) {
                hash.add(*position as u64);
                hash.add(candidate.distance.to_bits());
                hash.add(u64::from(candidate.selected));
                match &candidate.prediction {
                    Some(prediction) => {
                        hash.add(prediction.failure_probability.to_bits());
                        hash.add(prediction.time_to_response_start.to_bits());
                        hash.add(prediction.transfer_speed_bps.to_bits());
                        hash.add(prediction.expected_total_time.to_bits());
                    }
                    None => hash.add(u64::MAX),
                }
            }
            let strategy = strategy_tag(&decision.strategy);
            if window {
                lines.push(format!("{index} W"));
            } else {
                let top: Vec<String> = positions.iter().take(5).map(|p| p.to_string()).collect();
                lines.push(format!(
                    "{index} {strategy} n={} top={} h={:016x}",
                    positions.len(),
                    top.join(","),
                    hash.0
                ));
            }
            trace.push(serde_json::json!({
                "index": index,
                "window": window,
                "timed": timed,
                "transfers": transfers,
                "strategy": strategy,
                "ranked": positions,
                "candidates": decision.candidates.iter().map(|candidate| {
                    candidate.prediction.as_ref().map(|prediction| serde_json::json!([
                        prediction.failure_probability,
                        prediction.time_to_response_start,
                        prediction.transfer_speed_bps,
                        prediction.expected_total_time,
                    ]))
                }).collect::<Vec<_>>(),
            }));
        }
        clock.advance_time(Duration::from_secs(scenario.clock_step_secs));
        let event = stream.event(index);
        if let RouteOutcome::Success {
            payload_transfer_time,
            ..
        } = &event.outcome
        {
            timed += 1;
            if !payload_transfer_time.is_zero() {
                transfers += 1;
            }
        }
        router.add_event(event);
    }
    (lines, trace)
}

/// Minimum decisions of each kind a scenario must compare, so a harness change
/// that silently moves everything into the skipped window or the distance-only
/// regime fails instead of passing vacuously.
struct Coverage {
    distance_based: usize,
    prediction_based: usize,
}

/// Replay a scenario by name, calling `hook` with the router, the candidates
/// and the target just before each routing decision. For tests elsewhere in
/// the router that need realistic router states.
pub(super) fn visit_decisions(
    name: &str,
    hook: &mut dyn FnMut(&Router, &[PeerKeyLocation], Location),
) {
    let scenario = [&MIXED, &PRODUCTION_LIKE, &DRIFT, &TIMING_STARVED]
        .into_iter()
        .find(|scenario| scenario.name == name)
        .unwrap_or_else(|| panic!("no scenario named {name}"));
    replay(scenario, hook);
}

fn check(scenario: &Scenario, coverage: Coverage) {
    let (lines, trace) = replay(scenario, &mut |_, _, _| {});

    if let Ok(dir) = std::env::var("FREENET_ROUTER_GOLDEN_TRACE") {
        let path = std::path::Path::new(&dir).join(format!("{}.jsonl", scenario.name));
        let body: String = trace.iter().map(|line| format!("{line}\n")).collect();
        std::fs::write(&path, body).expect("write trace");
    }

    let path = std::path::Path::new(GOLDEN_DIR).join(format!("{}.txt", scenario.name));
    if std::env::var("FREENET_ROUTER_GOLDEN_WRITE").is_ok() {
        std::fs::create_dir_all(GOLDEN_DIR).expect("create golden dir");
        let mut body = format!(
            "# Golden routing decisions for scenario `{}` (see router/golden_replay.rs).\n\
             # Generated on the soaked build; `W` marks cold-window decisions, not compared.\n",
            scenario.name
        );
        for line in &lines {
            body.push_str(line);
            body.push('\n');
        }
        std::fs::write(&path, body).expect("write golden file");
        return;
    }

    let golden = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let expected: Vec<&str> = golden
        .lines()
        .filter(|line| !line.starts_with('#'))
        .collect();
    assert_eq!(
        expected.len(),
        lines.len(),
        "scenario {}: decision count differs from the golden file",
        scenario.name
    );
    let degenerate: Vec<&str> = DEGENERATE
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.strip_prefix(&format!("{} ", scenario.name)))
        .collect();
    let mut skipped_degenerate = 0;
    for (expected, actual) in expected.iter().zip(&lines) {
        let index = expected.split(' ').next().unwrap_or_default();
        if degenerate.contains(&index) {
            assert!(
                !expected.ends_with(" W"),
                "scenario {}: decision {index} is listed as degenerate but lies in the window",
                scenario.name
            );
            skipped_degenerate += 1;
            continue;
        }
        assert_eq!(
            *expected, actual,
            "scenario {}: routing decision differs from the soaked build",
            scenario.name
        );
    }
    assert_eq!(
        skipped_degenerate,
        degenerate.len(),
        "scenario {}: every listed degenerate decision must exist in the golden file",
        scenario.name
    );

    let count = |tag: &str| {
        lines
            .iter()
            .filter(|line| line.split(' ').nth(1) == Some(tag))
            .count()
    };
    assert!(
        count("D") >= coverage.distance_based,
        "scenario {}: only {} distance-based decisions compared",
        scenario.name,
        count("D")
    );
    assert!(
        count("P") + count("F") >= coverage.prediction_based,
        "scenario {}: only {} prediction-based decisions compared",
        scenario.name,
        count("P") + count("F")
    );
}

#[test]
fn routing_matches_the_soaked_build_on_mixed_traffic() {
    check(
        &MIXED,
        Coverage {
            distance_based: 3,
            prediction_based: 250,
        },
    );
}

#[test]
fn routing_matches_the_soaked_build_on_production_like_traffic() {
    check(
        &PRODUCTION_LIKE,
        Coverage {
            distance_based: 3,
            prediction_based: 100,
        },
    );
}

#[test]
fn routing_matches_the_soaked_build_under_drift() {
    check(
        &DRIFT,
        Coverage {
            distance_based: 3,
            prediction_based: 250,
        },
    );
}

#[test]
fn routing_matches_the_soaked_build_with_starved_timing() {
    check(
        &TIMING_STARVED,
        Coverage {
            distance_based: 3,
            prediction_based: 150,
        },
    );
}

/// Prints how long `Router::add_event` and a routing decision take on a
/// saturated router in the soaked configuration. `add_event` runs entirely under
/// `ring.router.write()`, so its duration IS the write-lock hold per route
/// event; a decision runs under the read lock. Not an assertion: a debug test
/// binary says little about a release node. Measure with
/// `cargo test -p freenet --lib --release route_event_lock_hold -- --nocapture --test-threads=1`.
#[test]
fn route_event_lock_hold_on_a_saturated_router() {
    const WARM_UP: usize = 6_000;
    const MEASURED: usize = 3_000;
    const DECISIONS: usize = 1_000;
    let _soaked = soaked_configuration();
    let _rng = GlobalRng::seed_guard(0x4485_BE4C);
    let mut rng = Prng(0x4485_BE4C);
    let peers = make_peers(&mut rng, 32);
    let contracts: Vec<Location> = (0..256).map(|_| rng.location()).collect();
    let event = |i: usize| RouteEvent {
        peer: peers[(i * 7) % peers.len()].key.clone(),
        contract_location: contracts[(i * 13) % contracts.len()],
        outcome: match i % 10 {
            0 => RouteOutcome::Failure,
            1..=3 => RouteOutcome::SuccessUntimed,
            _ => RouteOutcome::Success {
                time_to_response_start: Duration::from_millis(50 + (i % 97) as u64),
                payload_size: 5000,
                payload_transfer_time: Duration::from_millis(20 + (i % 31) as u64),
            },
        },
        op_type: Some(OpType::Get),
    };
    let clock = SharedMockTimeSource::new();
    let mut router = Router::new(&[])
        .with_time_source(Arc::new(clock.clone()))
        .with_max_connections(200);
    for i in 0..WARM_UP {
        clock.advance_time(Duration::from_secs(1));
        router.add_event(event(i));
    }
    let mut holds: Vec<f64> = Vec::with_capacity(MEASURED);
    for i in WARM_UP..WARM_UP + MEASURED {
        clock.advance_time(Duration::from_secs(1));
        let event = event(i);
        let start = std::time::Instant::now();
        router.add_event(event);
        holds.push(start.elapsed().as_secs_f64() * 1e6);
    }
    let candidates: Vec<PeerKeyLocation> = peers.iter().take(30).map(|p| p.key.clone()).collect();
    let mut decisions: Vec<f64> = Vec::with_capacity(DECISIONS);
    for _ in 0..DECISIONS {
        let target = rng.location();
        let start = std::time::Instant::now();
        let selected = router.select_k_best_peers(candidates.iter(), target, 3);
        decisions.push(start.elapsed().as_secs_f64() * 1e6);
        assert!(!selected.is_empty());
    }
    let summary = |label: &str, samples: &mut Vec<f64>| {
        samples.sort_by(f64::total_cmp);
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
        eprintln!(
            "{label}: mean {mean:.1}us p50 {:.1}us p99 {:.1}us max {:.1}us (n={})",
            at(0.5),
            at(0.99),
            at(1.0),
            samples.len()
        );
    };
    summary(
        "add_event (router write-lock hold per route event)",
        &mut holds,
    );
    summary(
        "routing decision, 30 candidates (read lock)",
        &mut decisions,
    );
}
