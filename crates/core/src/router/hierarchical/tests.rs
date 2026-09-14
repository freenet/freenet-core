use super::*;
use crate::config::GlobalRng;

fn uniform() -> f64 {
    GlobalRng::random_range(0.0..1.0)
}

fn normal() -> f64 {
    let u1 = uniform().max(f64::MIN_POSITIVE);
    let u2 = uniform();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

fn event(distance: f64, y: f64) -> Event {
    Event {
        distance,
        y,
        time: 0.0,
        seq: 0,
        slot: 0,
        generation: 0,
        band: 0,
    }
}

fn sorted_window(mut events: Vec<Event>) -> Vec<Event> {
    events.sort_by(window_order);
    events
}

fn is_monotone(curve: &Curve, ascending: bool) -> bool {
    curve.blocks.windows(2).all(|pair| {
        if ascending {
            pair[0].y < pair[1].y
        } else {
            pair[0].y > pair[1].y
        }
    })
}

// ---------------------------------------------------------------------------
// Curve
// ---------------------------------------------------------------------------

/// The curve is a reimplementation, so it is pinned against the crate the
/// reference used: same blocks, same interpolation, same extrapolation.
#[test]
fn curve_matches_pav_regression() {
    let _guard = GlobalRng::seed_guard(0x4485_c0de);
    for ascending in [true, false] {
        for trial in 0..20 {
            let len = 5 + trial * 37;
            let events: Vec<Event> = (0..len)
                .map(|_| {
                    // Quantised distances so equal-x pooling is exercised.
                    let distance = (uniform() * 40.0).floor() / 80.0;
                    let y = if trial % 2 == 0 {
                        f64::from(u8::from(uniform() < 0.1 + distance))
                    } else {
                        distance * if ascending { 2.0 } else { -2.0 } + normal()
                    };
                    event(distance, y)
                })
                .collect();
            let window = sorted_window(events.clone());
            let ours = Curve::pav(
                window.iter().map(|e| Block {
                    x: e.distance,
                    y: e.y,
                    w: 1.0,
                }),
                ascending,
            )
            .expect("non-empty input fits");
            let points: Vec<pav_regression::Point<f64>> = events
                .iter()
                .map(|e| pav_regression::Point::new(e.distance, e.y))
                .collect();
            let theirs = if ascending {
                pav_regression::IsotonicRegression::new_ascending(&points)
            } else {
                pav_regression::IsotonicRegression::new_descending(&points)
            }
            .expect("crate fits");
            assert_eq!(
                ours.blocks.len(),
                theirs.get_points().len(),
                "block count, ascending={ascending} trial={trial}"
            );
            for query in 0..=60 {
                let x = -0.05 + query as f64 * 0.01;
                let a = ours.value(x).expect("value exists");
                let b = theirs.interpolate(x).expect("crate value exists");
                assert!(
                    (a - b).abs() < 1e-9,
                    "value at {x}: ours {a} vs crate {b} (ascending={ascending}, trial={trial})"
                );
            }
        }
    }
}

/// The bake-off reference's `Curve::refit(raw, now)` with `horizon: None,
/// shrink: true`, transcribed verbatim onto `pav_regression`, so the production
/// shrinkage is pinned to the design that was actually evaluated rather than to
/// a re-derivation of it.
fn reference_shrunk_curve(window: &[Event]) -> pav_regression::IsotonicRegression<f64> {
    use pav_regression::{IsotonicRegression, Point};
    let weight = |_: &Event| 1.0f64;
    let points: Vec<Point<f64>> = window
        .iter()
        .map(|r| Point::new_with_weight(r.distance, r.y, weight(r)))
        .collect();
    let fit = IsotonicRegression::new_ascending(&points).unwrap();
    let blocks = fit.get_points_sorted();
    let total: f64 = blocks.iter().map(|b| b.weight()).sum();
    if blocks.len() < 2 || total <= 0.0 {
        return fit;
    }
    let grand = blocks.iter().map(|b| b.weight() * b.y()).sum::<f64>() / total;
    let (mut ss, mut sw) = (0.0, 0.0);
    for r in window {
        if let Some(f) = fit.interpolate(r.distance) {
            let w = weight(r);
            ss += w * (r.y - f).powi(2);
            sw += w;
        }
    }
    let s2 = if sw > 0.0 { ss / sw } else { 0.0 };
    let tau2 = (blocks
        .iter()
        .map(|b| (b.y() - grand).powi(2) - s2 / b.weight())
        .sum::<f64>()
        / blocks.len() as f64)
        .max(0.0);
    let shrunk: Vec<Point<f64>> = blocks
        .iter()
        .map(|b| {
            let factor = if tau2 > 0.0 {
                tau2 / (tau2 + s2 / b.weight())
            } else {
                0.0
            };
            Point::new_with_weight(*b.x(), grand + factor * (b.y() - grand), b.weight())
        })
        .collect();
    IsotonicRegression::new_ascending(&shrunk).unwrap()
}

#[test]
fn shrunk_curve_matches_the_bakeoff_reference() {
    let _guard = GlobalRng::seed_guard(0x4485_4ef0);
    for trial in 0..12 {
        let len = 5 + trial * 211;
        let window = sorted_window(
            (0..len)
                .map(|_| {
                    let distance = uniform() * 0.5;
                    let y = if trial % 2 == 0 {
                        f64::from(u8::from(uniform() < 0.02 + distance * 0.3))
                    } else {
                        -1.0 + distance + 0.5 * normal()
                    };
                    event(distance, y)
                })
                .collect(),
        );
        let ours = Curve::fit_shrunk(&window, true).unwrap();
        let reference = reference_shrunk_curve(&window);
        for query in 0..=60 {
            let x = -0.05 + query as f64 * 0.01;
            let a = ours.value(x).unwrap();
            let b = reference.interpolate(x).unwrap();
            assert!(
                (a - b).abs() < 1e-9,
                "trial {trial} at {x}: production {a} vs reference {b}"
            );
        }
    }
}

/// A sparse end block is pulled toward the pooled mean and the result stays
/// monotone and inside the raw curve's range.
///
/// Measured, and weaker than the reference's module docs suggest: a single
/// failure in the last block is pulled from 1.0 to about 0.77, not to the
/// pool. The between-block variance is estimated FROM the blocks, and on binary
/// outcomes PAV's tiny end blocks are themselves what inflate it. This test pins
/// the direction, which the design guarantees, and not a magnitude it does not.
#[test]
fn shrinkage_pulls_a_sparse_block_toward_the_pool_and_stays_monotone() {
    let _guard = GlobalRng::seed_guard(0x4485_0b10);
    let mut events = Vec::new();
    for _ in 0..2_000 {
        let distance = uniform() * 0.4;
        let y = f64::from(u8::from(uniform() < 0.02));
        events.push(event(distance, y));
    }
    // Five events at the far end, one of them a failure.
    for i in 0..5 {
        events.push(event(0.49 + i as f64 * 0.001, f64::from(u8::from(i == 4))));
    }
    let window = sorted_window(events);
    let raw = Curve::pav(
        window.iter().map(|e| Block {
            x: e.distance,
            y: e.y,
            w: 1.0,
        }),
        true,
    )
    .unwrap();
    let shrunk = Curve::fit_shrunk(&window, true).unwrap();
    let raw_end = raw.blocks.last().unwrap().y;
    let shrunk_end = shrunk.blocks.last().unwrap().y;
    assert!(
        raw_end >= 0.19,
        "the unshrunk curve reads the sparse block at face value, got {raw_end}"
    );
    assert!(
        shrunk_end < raw_end,
        "shrinkage must pull the sparse end block toward the pool: raw {raw_end}, \
         shrunk {shrunk_end}"
    );
    let raw_low = raw.blocks.first().unwrap().y;
    assert!(
        shrunk
            .blocks
            .iter()
            .all(|block| block.y >= raw_low - 1e-12 && block.y <= raw_end + 1e-12),
        "shrinkage toward the pool cannot leave the raw curve's range"
    );
    assert!(
        is_monotone(&shrunk, true),
        "shrunk curve must stay monotone"
    );

    let descending = Curve::fit_shrunk(
        &sorted_window(
            (0..500)
                .map(|_| {
                    let d = uniform() * 0.5;
                    event(d, 5.0 - 4.0 * d + normal())
                })
                .collect(),
        ),
        false,
    )
    .unwrap();
    assert!(
        is_monotone(&descending, false),
        "descending shrunk curve must stay monotone"
    );
}

/// With no between-block signal, shrinkage must not widen the spread PAV
/// manufactures out of noise.
///
/// Measured, and deliberately not dressed up: on 3000 flat N(0,1) draws the
/// shrinkage narrows a 0.27 spread only to 0.25. The reference estimates the
/// between-block variance from the PAV blocks, whose small end blocks are
/// exactly the noise it is meant to discount, so it discounts little. Pinned as
/// "never wider" so a future change that genuinely strengthens the shrinkage can
/// tighten it, and one that inverts it fails.
#[test]
fn shrinkage_flattens_a_curve_with_no_signal() {
    let _guard = GlobalRng::seed_guard(0x4485_f1a7);
    let window = sorted_window(
        (0..3_000)
            .map(|_| event(uniform() * 0.5, 1.0 + normal()))
            .collect(),
    );
    let raw = Curve::pav(
        window.iter().map(|e| Block {
            x: e.distance,
            y: e.y,
            w: 1.0,
        }),
        true,
    )
    .unwrap();
    let shrunk = Curve::fit_shrunk(&window, true).unwrap();
    let spread = |curve: &Curve| curve.value(0.49).unwrap() - curve.value(0.01).unwrap();
    assert!(
        spread(&shrunk) <= spread(&raw),
        "a flat truth must yield a flatter shrunk curve: raw spread {}, shrunk {}",
        spread(&raw),
        spread(&shrunk)
    );
}

#[test]
fn curve_refuses_non_finite_queries() {
    let window = sorted_window((0..10).map(|i| event(i as f64 / 20.0, 0.1)).collect());
    let curve = Curve::fit_shrunk(&window, true).unwrap();
    assert_eq!(curve.value(f64::NAN), None);
    assert_eq!(curve.value(f64::INFINITY), None);
    assert!(curve.value(0.2).is_some());
}

#[test]
fn curve_needs_a_minimum_of_points() {
    let window = sorted_window((0..4).map(|i| event(i as f64 / 10.0, 0.1)).collect());
    assert!(Curve::fit_shrunk(&window, true).is_none());
}

// ---------------------------------------------------------------------------
// Level: decay algebra and variance components
// ---------------------------------------------------------------------------

/// Epoch-scaled storage must equal naive per-event exponential forgetting.
#[test]
fn epoch_scaled_moments_equal_naive_forgetting() {
    let horizon = 6.0;
    let mut level = Level::new(Some(horizon));
    level.reset(10.0);
    let events = [
        (10.0, 0.3, 0usize, 1usize),
        (11.5, -0.2, 0, 1),
        (12.0, 0.7, 1, 3),
        (17.25, 0.1, 0, 2),
    ];
    for &(time, residual, slot, band) in &events {
        level.add(Some(slot), band, level.weight(time), residual);
    }
    let now = 20.0;
    let scale = level.scale(now);
    let naive = |filter: &dyn Fn(usize, usize) -> bool| {
        events
            .iter()
            .filter(|(_, _, slot, band)| filter(*slot, *band))
            .fold((0.0, 0.0), |(n, s), &(t, r, _, _)| {
                let w = (-(now - t) / horizon).exp();
                (n + w, s + w * r)
            })
    };
    let (root_n, root_sum) = naive(&|_, _| true);
    assert!((level.root.n * scale - root_n).abs() < 1e-12);
    assert!((level.root.sum * scale - root_sum).abs() < 1e-12);
    let (peer_n, _) = naive(&|slot, _| slot == 0);
    assert!((level.nodes[0].peer.n * scale - peer_n).abs() < 1e-12);
    let (cell_n, cell_sum) = naive(&|slot, band| slot == 0 && band == 1);
    assert!((level.nodes[0].cells[1].n * scale - cell_n).abs() < 1e-12);
    assert!((level.nodes[0].cells[1].mean() - cell_sum / cell_n).abs() < 1e-12);

    // Incrementally-maintained squared counts equal a recount.
    let (sq_peers, sq_cells) = (level.sq_peers, level.sq_cells);
    level.recount_squares();
    assert!((sq_peers - level.sq_peers).abs() < 1e-9);
    assert!((sq_cells - level.sq_cells).abs() < 1e-9);
}

#[test]
fn integer_rate_ratio_only_accepts_exact_small_powers() {
    assert_eq!(integer_rate_ratio(24.0, 6.0), Some(4));
    assert_eq!(integer_rate_ratio(6.0, 1.5), Some(4));
    assert_eq!(integer_rate_ratio(24.0, 5.0), None);
    assert_eq!(integer_rate_ratio(6.0, 6.0), None);
    assert_eq!(integer_rate_ratio(1.5, 6.0), None);
    assert_eq!(integer_rate_ratio(100.0, 1.0), None);
}

/// A refit derives faster horizons' weights as powers of slower ones; the
/// result must equal plain exponential forgetting for every horizon.
#[test]
fn rebuilt_levels_equal_naive_exponential_forgetting() {
    let _guard = GlobalRng::seed_guard(0x4485_4eb1);
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 10_000, 64);
    let mut times = Vec::new();
    for i in 0..777u32 {
        let time = i as f64 * 0.037;
        times.push(time);
        stage.observe(&(i % 9), uniform(), uniform() * 0.5, normal(), time);
    }
    let now = *times.last().unwrap();
    stage.refit(now);
    for level in &stage.levels {
        let naive: f64 = times
            .iter()
            .map(|t| level.horizon_hours.map_or(1.0, |h| (-(now - t) / h).exp()))
            .sum();
        let rebuilt = level.root.n * level.scale(now);
        assert!(
            (rebuilt - naive).abs() < 1e-9 * naive.max(1.0),
            "horizon {:?}: rebuilt n {rebuilt} vs naive {naive}",
            level.horizon_hours
        );
    }
}

/// Method of moments recovers known variance components.
#[test]
fn variance_components_recover_the_generating_values() {
    let _guard = GlobalRng::seed_guard(0x4485_7a02);
    let (sigma, tau_peer, tau_cell) = (1.0, 0.5, 0.3);
    let mut level = Level::new(None);
    level.reset(0.0);
    for slot in 0..60 {
        let peer_effect = tau_peer * normal();
        for band in 0..BANDS {
            let cell_effect = tau_cell * normal();
            for _ in 0..40 {
                level.add(
                    Some(slot),
                    band,
                    1.0,
                    peer_effect + cell_effect + sigma * normal(),
                );
            }
        }
    }
    level.recount_squares();
    let components = level.compute_components().expect("components exist");
    assert!(
        (components.sigma2 - 1.0).abs() < 0.08,
        "sigma2 {}",
        components.sigma2
    );
    assert!(
        (components.tau2_peer - 0.25).abs() < 0.12,
        "tau2_peer {}",
        components.tau2_peer
    );
    assert!(
        (components.tau2_cell - 0.09).abs() < 0.04,
        "tau2_cell {}",
        components.tau2_cell
    );
}

#[test]
fn variance_components_need_within_cell_replication() {
    let mut level = Level::new(None);
    level.reset(0.0);
    // One event per cell: no within-cell degrees of freedom.
    for slot in 0..10 {
        level.add(Some(slot), slot % BANDS, 1.0, 0.5);
    }
    level.recount_squares();
    assert_eq!(level.compute_components(), None);
    assert_eq!(level.residual(Some(0), 0, 0.0), 0.0);
}

/// The two shrinkage limits: an unknown peer gets only the root's pooled
/// offset, and a heavily-observed cell gets (almost) its own mean.
#[test]
fn shrinkage_limits_no_data_to_pool_and_lots_of_data_to_cell_mean() {
    let _guard = GlobalRng::seed_guard(0x4485_11a1);
    let mut level = Level::new(None);
    level.reset(0.0);
    for slot in 0..=30 {
        for band in 0..BANDS {
            for _ in 0..20 {
                level.add(Some(slot), band, 1.0, 0.2 * normal());
            }
        }
    }
    // One cell with an effect of +2 and a lot of evidence.
    for _ in 0..2_000 {
        level.add(Some(30), 5, 1.0, 2.0 + 0.2 * normal());
    }
    level.recount_squares();
    level.components = level.compute_components();
    assert!(level.components.is_some());

    let hot = level.residual(Some(30), 5, 0.0);
    let cell_mean = level.nodes[30].cells[5].mean();
    assert!(
        (hot - cell_mean).abs() < 0.05,
        "abundant evidence must yield the cell's own mean: {hot} vs {cell_mean}"
    );
    let unknown = level.residual(None, 0, 0.0);
    assert!(
        unknown.abs() < hot.abs() * 0.5,
        "an unknown peer must not inherit another peer's cell effect: {unknown}"
    );
    let other_band = level.residual(Some(30), 2, 0.0);
    assert!(
        other_band < hot,
        "a band with no data for that peer shrinks toward the peer, not the hot cell: \
         {other_band} vs {hot}"
    );
}

// ---------------------------------------------------------------------------
// Stage
// ---------------------------------------------------------------------------

#[test]
fn cold_stage_predicts_nothing() {
    let stage: Stage<u32> = Stage::new(Target::Failure);
    assert_eq!(stage.predict(&1, 0.3, 0.1, 0.0), None);
    assert!(!stage.diagnostics().active);
}

/// With data but no peer structure the prediction is the curve.
#[test]
fn prediction_equals_the_curve_when_there_is_no_hierarchy_signal() {
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime);
    for i in 0..400u32 {
        let distance = (i % 50) as f64 / 100.0;
        stage.observe(&(i % 7), (i % 13) as f64 / 13.0, distance, distance, 0.0);
    }
    let curve = stage.curve.as_ref().unwrap().value(0.25).unwrap();
    let predicted = stage.predict(&3, 0.4, 0.25, 0.0).unwrap();
    assert!(
        (predicted - curve).abs() < 1e-6,
        "noise-free data carries no residual: predicted {predicted}, curve {curve}"
    );
}

/// `observe` must return the forecast `predict` would have made just before.
#[test]
fn observe_returns_the_pre_learning_forecast() {
    let _guard = GlobalRng::seed_guard(0x4485_9e1d);
    let mut stage: Stage<u32> = Stage::new(Target::Failure);
    for i in 0..3_000u32 {
        let peer = i % 23;
        let contract = uniform();
        let distance = uniform() * 0.5;
        let y = f64::from(u8::from(uniform() < 0.05 + distance * 0.3));
        let before = stage.predict(&peer, contract, distance, i as f64 / 60.0);
        let returned = stage.observe(&peer, contract, distance, y, i as f64 / 60.0);
        assert_eq!(before, returned, "event {i}");
    }
}

#[test]
fn window_stays_bounded_and_sorted() {
    let _guard = GlobalRng::seed_guard(0x4485_b0d1);
    let capacity = 1_000;
    let mut stage: Stage<u32> = Stage::with_limits(Target::Failure, capacity, 64);
    let total = 5_321u64;
    for i in 0..total {
        stage.observe(
            &((i % 40) as u32),
            uniform(),
            uniform() * 0.5,
            f64::from(u8::from(uniform() < 0.1)),
            i as f64 / 60.0,
        );
        assert!(stage.diagnostics().window_events <= capacity + REFIT_EVERY);
    }
    stage.refit(total as f64 / 60.0);
    assert_eq!(stage.sorted.len(), capacity);
    assert!(
        stage
            .sorted
            .windows(2)
            .all(|pair| window_order(&pair[0], &pair[1]).is_le()),
        "window must be sorted after in-place merges"
    );
    let mut seqs: Vec<u64> = stage.sorted.iter().map(|e| e.seq).collect();
    seqs.sort_unstable();
    let expected: Vec<u64> = (total - capacity as u64..total).collect();
    assert_eq!(seqs, expected, "window must hold exactly the newest events");
}

/// Peers churn: the table is bounded, newcomers are admitted by evicting the
/// least recently used, and evictions are counted.
#[test]
fn peer_table_is_bounded_by_lru_eviction() {
    let max_peers = 64;
    let mut stage: Stage<u32> = Stage::with_limits(Target::Failure, 2_000, max_peers);
    // A long-lived peer used on every other event must survive the churn.
    for i in 0..2_000u32 {
        let peer = if i % 2 == 0 { 0 } else { 1 + i };
        stage.observe(&peer, 0.5, 0.1, 0.0, 0.0);
        assert!(stage.peers.index.len() <= max_peers);
        for level in &stage.levels {
            assert!(level.nodes.len() <= max_peers);
        }
    }
    let diagnostics = stage.diagnostics();
    assert!(diagnostics.peers <= max_peers);
    assert!(
        diagnostics.peer_evictions >= 1_000 - max_peers as u64,
        "evictions must be counted, got {}",
        diagnostics.peer_evictions
    );
    assert!(
        stage.peers.lookup(&0).is_some(),
        "the busy peer must not be evicted"
    );
    assert!(
        stage.peers.lookup(&2000).is_some(),
        "the newest peer must be admitted, not refused"
    );
    assert!(
        stage.peers.lookup(&3).is_none(),
        "the oldest one-shot peer must be gone"
    );
}

/// An evicted slot's statistics must not leak into the peer that reuses it.
#[test]
fn a_reused_slot_starts_clean() {
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 5_000, 2);
    for i in 0..300 {
        stage.observe(&1, 0.1, (i % 10) as f64 / 20.0, 5.0, 0.0);
        stage.observe(&2, 0.1, (i % 10) as f64 / 20.0, 0.0, 0.0);
    }
    // Peer 3 evicts peer 1 (least recently used) and takes its slot.
    stage.observe(&3, 0.1, 0.2, 2.5, 0.0);
    let slot = stage.peers.lookup(&3).unwrap();
    for level in &stage.levels {
        let node = level.nodes[slot];
        assert!(
            (node.peer.n - level.weight(0.0)).abs() < 1e-9 || node.peer.n == 0.0,
            "reused slot must hold only the newcomer's evidence, n={}",
            node.peer.n
        );
    }
    stage.refit(0.0);
    assert!(stage.diagnostics().orphaned_at_last_refit >= 300);
}

/// Non-finite and out-of-range inputs are refused and counted, and never reach
/// a prediction.
#[test]
fn non_finite_inputs_are_rejected_and_counted() {
    let _guard = GlobalRng::seed_guard(0x4485_0a0a);
    let mut stage: Stage<u32> = Stage::new(Target::Failure);
    for i in 0..500u32 {
        stage.observe(&(i % 5), uniform(), uniform() * 0.5, 0.0, 0.0);
    }
    let rejected_before = stage.diagnostics().rejected;
    stage.observe(&1, 0.2, 0.1, f64::NAN, 0.0);
    stage.observe(&1, 0.2, f64::INFINITY, 1.0, 0.0);
    stage.observe(&1, 0.2, 0.1, 2.0, 0.0);
    stage.observe(&1, 0.2, 0.1, -1.0, 0.0);
    assert_eq!(stage.diagnostics().rejected, rejected_before + 4);
    // Garbage time and contract location are tolerated, not learned as garbage.
    stage.observe(&1, f64::NAN, 0.1, 1.0, f64::NAN);
    stage.observe(&1, f64::INFINITY, 0.1, 1.0, f64::INFINITY);
    stage.observe(&1, -3.0, 0.1, 1.0, f64::NEG_INFINITY);
    for query in [0.0, 0.1, 0.49, 0.5, 1.0, -1.0] {
        let p = stage.predict(&1, 0.3, query, 0.0).unwrap();
        assert!((0.0..=1.0).contains(&p), "prediction {p} at {query}");
    }
    assert_eq!(stage.predict(&1, 0.3, f64::NAN, 0.0), None);
    assert!(stage.predict(&1, f64::NAN, 0.1, f64::NAN).is_some());

    let mut timing: Stage<u32> = Stage::new(Target::LogResponseTime);
    timing.observe(&1, 0.2, 0.1, f64::NEG_INFINITY, 0.0);
    assert_eq!(timing.diagnostics().rejected, 1);
}

/// A wall clock that steps backwards is read as "no time passed", and a long
/// silence cannot overflow the epoch-scaled weights.
#[test]
fn clock_steps_and_long_gaps_stay_finite() {
    let _guard = GlobalRng::seed_guard(0x4485_c10c);
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime);
    let mut time = 0.0;
    for i in 0..2_000u32 {
        time += if i % 97 == 0 { 10_000.0 } else { 0.01 };
        let t = if i % 13 == 0 { time - 50.0 } else { time };
        let y = normal();
        stage.observe(&(i % 11), uniform(), uniform() * 0.5, y, t);
        if let Some(p) = stage.predict(&(i % 11), 0.5, 0.2, t) {
            assert!(p.is_finite(), "prediction must stay finite, got {p} at {i}");
        }
    }
    for level in &stage.levels {
        assert!(level.root.n.is_finite() && level.root.sum.is_finite());
        assert!(level.sq_peers.is_finite() && level.sq_cells.is_finite());
    }
    // Querying far in the future (no observe) must not produce NaN either.
    let far = stage.predict(&3, 0.5, 0.2, time + 1.0e7).unwrap();
    assert!(far.is_finite());
}

/// Horizon selection: on stationary data nothing forgets; once peer behaviour
/// drifts, a forgetting horizon takes over.
#[test]
fn horizon_selection_switches_to_forgetting_after_drift() {
    let _guard = GlobalRng::seed_guard(0x4485_d71f);
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime);
    let peers = 20u32;
    let effects: Vec<f64> = (0..peers).map(|_| normal()).collect();
    let mut index = 0u64;
    let mut run = |stage: &mut Stage<u32>, events: u64, flip: bool| {
        for _ in 0..events {
            let peer = GlobalRng::random_range(0..peers);
            let distance = uniform() * 0.5;
            let effect = effects[peer as usize] * if flip { -1.0 } else { 1.0 };
            let y = 2.0 * distance + effect + 0.3 * normal();
            // 60 events per hour, the reference's rate.
            stage.observe(&peer, uniform(), distance, y, index as f64 / 60.0);
            index += 1;
        }
    };
    run(&mut stage, 3_000, false);
    assert_eq!(
        stage.diagnostics().selected_horizon_hours,
        None,
        "stationary traffic must keep the no-forgetting horizon, losses {:?}",
        stage.loss
    );
    run(&mut stage, 1_500, true);
    assert!(
        stage.diagnostics().selected_horizon_hours.is_some(),
        "after every peer's effect flips a forgetting horizon must win, losses {:?}",
        stage.loss
    );
}

/// Every refit re-derives residuals against the new curve, so the hierarchy
/// never models a curve that no longer exists.
#[test]
fn residuals_are_recomputed_against_the_current_curve() {
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 10_000, 64);
    // Phase 1: flat at 0. Phase 2: flat at 3. Every peer identical.
    for i in 0..1_000u32 {
        let y = if i < 500 { 0.0 } else { 3.0 };
        stage.observe(&(i % 10), 0.5, (i % 50) as f64 / 100.0, y, 0.0);
    }
    stage.refit(0.0);
    let curve = stage.curve.as_ref().unwrap();
    for level in &stage.levels {
        let expected: f64 = stage
            .sorted
            .iter()
            .map(|e| e.y - curve.value(e.distance).unwrap())
            .sum();
        assert!(
            (level.root.sum - expected).abs() < 1e-6,
            "root residual sum {} must match the current curve's {expected}",
            level.root.sum
        );
    }
}

/// Refit cost at a full production window. Printed for the PR; the assertion
/// is only that it ran at full size, since a wall-clock bound in a unit test is
/// a flaky test. Run with `--release -- --nocapture` for the real number.
#[test]
fn refit_and_prediction_cost_at_a_full_window() {
    let _guard = GlobalRng::seed_guard(0x4485_be7c);
    let mut stage: Stage<u32> = Stage::new(Target::Failure);
    let peers = 200u32;
    for i in 0..WINDOW_EVENTS as u64 + 7 {
        let distance = uniform() * 0.5;
        let y = f64::from(u8::from(uniform() < 0.02 + distance * 0.2));
        stage.observe(
            &GlobalRng::random_range(0..peers),
            uniform(),
            distance,
            y,
            i as f64 / 600.0,
        );
    }
    let now = WINDOW_EVENTS as f64 / 600.0;
    stage.refit(now);
    assert_eq!(stage.sorted.len(), WINDOW_EVENTS);

    // Phase-by-phase, mirroring `Stage::refit`, so a regression can be located.
    let rounds = 40u32;
    // Minimum over rounds, not mean: this runs on shared machines, and the
    // minimum is the least contaminated estimate of the work itself.
    let mut phases = [std::time::Duration::MAX; 4];
    for round in 0..rounds {
        for i in 0..REFIT_EVERY {
            stage.fresh.push(Event {
                distance: uniform() * 0.5,
                y: 0.0,
                time: now,
                seq: stage.next_seq,
                slot: (i % 200) as u32,
                generation: 0,
                band: (round as usize % BANDS) as u8,
            });
            stage.next_seq += 1;
        }
        let mut lap = std::time::Instant::now();
        let mut mark = |phase: usize| {
            phases[phase] = phases[phase].min(lap.elapsed());
            lap = std::time::Instant::now();
        };
        stage.merge_fresh();
        mark(0);
        stage.curve = Curve::fit_shrunk(&stage.sorted, true);
        mark(1);
        for level in &mut stage.levels {
            level.reset(now);
        }
        assert!(stage.prepare());
        mark(2);
        stage.rebuild_levels(now);
        mark(3);
    }
    let [merge, curve, prepare, levels] = phases;
    let per_refit = merge + curve + prepare + levels;

    let queries = 25 * 3 * 1_000;
    let start = std::time::Instant::now();
    let mut acc = 0.0;
    for q in 0..queries {
        acc += stage
            .predict(&(q as u32 % peers), 0.37, (q % 500) as f64 / 1000.0, now)
            .unwrap_or(0.0);
    }
    let per_prediction = start.elapsed() / queries as u32;
    eprintln!(
        "#4485 hierarchical cost over {WINDOW_EVENTS} events: refit {per_refit:?} = merge \
         {merge:?} + curve {curve:?} + re-anchor {prepare:?} + levels {levels:?}; \
         prediction {per_prediction:?} (sum {acc:.3}); curve blocks {}",
        stage.curve.as_ref().map_or(0, |c| c.blocks.len())
    );
}

// ---------------------------------------------------------------------------
// HierarchicalRouting
// ---------------------------------------------------------------------------

#[test]
fn routing_bundle_feeds_each_stage_from_its_own_outcomes() {
    let mut routing = HierarchicalRouting::new();
    let peer = PeerKeyLocation::random();
    let contract = Location::new(0.3);
    for i in 0..200 {
        let outcome = match i % 4 {
            0 => RoutingOutcome {
                success: false,
                time_to_response_start_secs: None,
                transfer_speed_bps: None,
            },
            1 => RoutingOutcome {
                success: true,
                time_to_response_start_secs: None,
                transfer_speed_bps: None,
            },
            2 => RoutingOutcome {
                success: true,
                time_to_response_start_secs: Some(0.25),
                transfer_speed_bps: Some(50_000.0),
            },
            _ => RoutingOutcome {
                success: true,
                // Zero-length responses cannot be logged; they must be refused.
                time_to_response_start_secs: Some(0.0),
                transfer_speed_bps: Some(0.0),
            },
        };
        routing.observe_at(&peer, contract, 0.1 + (i % 10) as f64 / 40.0, &outcome, 0.0);
    }
    let [failure, response, transfer] = routing.diagnostics();
    assert_eq!(failure.window_events, 200);
    assert_eq!(response.window_events, 50);
    assert_eq!(response.rejected, 50);
    assert_eq!(transfer.window_events, 50);
    assert_eq!(transfer.rejected, 50);

    let estimate = routing.estimate_at(&peer, contract, 0.2, 0.0);
    let failure = estimate.failure_probability.unwrap();
    assert!((failure - 0.25).abs() < 0.05, "failure {failure}");
    let seconds = estimate.time_to_response_start_secs.unwrap();
    assert!((seconds - 0.25).abs() < 1e-6, "seconds {seconds}");
    let speed = estimate.transfer_speed_bps.unwrap();
    assert!((speed - 50_000.0).abs() < 1e-3, "speed {speed}");
}

/// Per-candidate cost of a full three-stage estimate with real peer keys, the
/// figure that matters for a routing decision (about 25 candidates). Printed,
/// not asserted, for the reason given on the refit cost test.
#[test]
fn routing_estimate_cost_per_candidate() {
    let _guard = GlobalRng::seed_guard(0x4485_ca4d);
    let peers: Vec<PeerKeyLocation> = (0..200).map(|_| PeerKeyLocation::random()).collect();
    let mut routing = HierarchicalRouting::new();
    for i in 0..WINDOW_EVENTS {
        let peer = &peers[GlobalRng::random_range(0..peers.len())];
        let outcome = RoutingOutcome {
            success: uniform() > 0.05,
            time_to_response_start_secs: Some(0.05 + uniform()),
            transfer_speed_bps: Some(1_000.0 + 50_000.0 * uniform()),
        };
        routing.observe_at(
            peer,
            Location::new(uniform()),
            uniform() * 0.5,
            &outcome,
            i as f64 / 600.0,
        );
    }
    let now = WINDOW_EVENTS as f64 / 600.0;
    let queries = 25_000;
    let start = std::time::Instant::now();
    let mut available = 0;
    for q in 0..queries {
        let estimate = routing.estimate_at(
            &peers[q % peers.len()],
            Location::new((q % 997) as f64 / 997.0),
            (q % 500) as f64 / 1000.0,
            now,
        );
        available += usize::from(estimate.transfer_speed_bps.is_some());
    }
    let per_candidate = start.elapsed() / queries as u32;
    eprintln!(
        "#4485 hierarchical estimate (3 stages, PeerKeyLocation keys): \
         {per_candidate:?} per candidate"
    );
    assert_eq!(
        available, queries,
        "every stage must be active after a full window"
    );
}
