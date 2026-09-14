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

/// Zipf-distributed index in `0..count`.
fn zipf(count: usize, exponent: f64) -> usize {
    let total: f64 = (1..=count).map(|k| (k as f64).powf(-exponent)).sum();
    let mut draw = uniform() * total;
    for k in 1..=count {
        draw -= (k as f64).powf(-exponent);
        if draw <= 0.0 {
            return k - 1;
        }
    }
    count - 1
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

fn raw_curve(window: &[Event], ascending: bool) -> Curve {
    Curve::pav(
        window.iter().map(|e| Block {
            x: e.distance,
            y: e.y,
            w: 1.0,
        }),
        ascending,
    )
    .expect("non-empty input fits")
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

/// `Stage::observe` with a throwaway scratch, returning the forecast value.
fn observe<K: Hash + Eq + Clone>(
    stage: &mut Stage<K>,
    peer: &K,
    contract: f64,
    distance: f64,
    y: f64,
    now: f64,
) -> Option<f64> {
    let mut scratch = Scratch::default();
    stage
        .observe(&mut scratch, peer, contract, distance, y, now)
        .map(|forecast| forecast.value)
}

fn predict<K: Hash + Eq + Clone>(
    stage: &Stage<K>,
    peer: &K,
    contract: f64,
    distance: f64,
) -> Option<f64> {
    stage
        .predict(peer, contract, distance)
        .map(|forecast| forecast.value)
}

fn refit<K: Hash + Eq + Clone>(stage: &mut Stage<K>, now: f64) {
    stage.refit(&mut Scratch::default(), now);
}

// ---------------------------------------------------------------------------
// Curve
// ---------------------------------------------------------------------------

/// Raw PAV and interpolation are pinned against `pav_regression`.
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
            let ours = raw_curve(&sorted_window(events.clone()), ascending);
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
            assert_eq!(ours.blocks.len(), theirs.get_points().len());
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

/// The shrinkage formula on a case small enough to compute by hand.
///
/// Blocks (means 1, 2, 3; weights 2, 3, 2). Within-block SS = 2 + 2 + 2 = 6 on
/// N - k = 4 df, so s2 = 1.5. Grand mean 2, between = 2*1 + 3*0 + 2*1 = 4,
/// W - sum w^2 / W = 7 - 17/7 = 32/7, so tau2 = (4 - 2*1.5) / (32/7) = 7/32.
/// The outer blocks move by B = tau2 / (tau2 + 1.5/2) = 7/31 toward 2.
#[test]
fn shrinkage_agrees_with_the_formula_on_a_hand_computed_case() {
    let window = sorted_window(vec![
        event(0.1, 0.0),
        event(0.1, 2.0),
        event(0.2, 1.0),
        event(0.2, 3.0),
        event(0.2, 2.0),
        event(0.3, 4.0),
        event(0.3, 2.0),
    ]);
    let raw = raw_curve(&window, true);
    let means: Vec<f64> = raw.blocks.iter().map(|b| b.y).collect();
    assert_eq!(
        means,
        vec![1.0, 2.0, 3.0],
        "the case must pool as described"
    );
    // The failure target allows five points; this case has seven.
    let shrunk = Curve::fit_shrunk(&window, Target::Failure).unwrap();
    assert!(
        Curve::fit_shrunk(&window, Target::LogResponseTime).is_none(),
        "a log target needs {MIN_CURVE_POINTS_LOG} points"
    );
    let b = 7.0 / 31.0;
    let expected = [2.0 - b, 2.0, 2.0 + b];
    for (block, want) in shrunk.blocks.iter().zip(expected) {
        assert!(
            (block.y - want).abs() < 1e-12,
            "block {} expected {want}",
            block.y
        );
    }
    assert_eq!(shrunk.within_variance, Some(1.5));
}

/// A lone extreme block at the end of an otherwise flat 2% curve is pulled
/// most of the way to the pool — the case the previous estimator left at 0.77.
#[test]
fn shrinkage_strongly_shrinks_a_lone_extreme_end_block() {
    let mut ends = Vec::new();
    for seed in 0..10u64 {
        let _guard = GlobalRng::seed_guard(0x4485_0b10 + seed);
        let mut events = Vec::new();
        for _ in 0..2_000 {
            events.push(event(
                uniform() * 0.4,
                f64::from(u8::from(uniform() < 0.02)),
            ));
        }
        for i in 0..5 {
            events.push(event(0.49 + i as f64 * 0.001, f64::from(u8::from(i == 4))));
        }
        let window = sorted_window(events);
        assert!(raw_curve(&window, true).blocks.last().unwrap().y >= 0.99);
        let shrunk = Curve::fit_shrunk(&window, Target::Failure).unwrap();
        assert!(is_monotone(&shrunk, true));
        ends.push(shrunk.blocks.last().unwrap().y);
    }
    ends.sort_by(f64::total_cmp);
    let median = ends[ends.len() / 2];
    assert!(
        median < 0.2,
        "a single failure in a five-event end block must be pulled most of the way \
         to the ~2% pool; median end value {median}, all {ends:?}"
    );
}

/// With no signal, the spread PAV manufactures out of noise is removed.
#[test]
fn shrinkage_flattens_flat_noise() {
    let mut raw_spread = 0.0;
    let mut shrunk_spread = 0.0;
    for seed in 0..10u64 {
        let _guard = GlobalRng::seed_guard(0x4485_f1a7 + seed);
        let window = sorted_window(
            (0..3_000)
                .map(|_| event(uniform() * 0.5, 1.0 + normal()))
                .collect(),
        );
        let spread = |c: &Curve| c.value(0.49).unwrap() - c.value(0.01).unwrap();
        raw_spread += spread(&raw_curve(&window, true));
        shrunk_spread += spread(&Curve::fit_shrunk(&window, Target::LogResponseTime).unwrap());
    }
    assert!(
        shrunk_spread < raw_spread * 0.25,
        "flat truth must give a near-flat curve: raw spread {raw_spread}, shrunk \
         {shrunk_spread} (summed over 10 seeds)"
    );
}

/// A real monotone trend backed by populated blocks survives the shrinkage.
#[test]
fn shrinkage_preserves_a_real_monotone_signal() {
    let _guard = GlobalRng::seed_guard(0x4485_516e);
    for (ascending, target, slope) in [
        (true, Target::LogResponseTime, 2.0),
        (false, Target::LogTransferSpeed, -2.0),
    ] {
        let window = sorted_window(
            (0..3_000)
                .map(|_| {
                    let d = uniform() * 0.5;
                    event(d, slope * d + 0.5 * normal())
                })
                .collect(),
        );
        let shrunk = Curve::fit_shrunk(&window, target).unwrap();
        assert!(is_monotone(&shrunk, ascending));
        for d in [0.1, 0.25, 0.4] {
            let value = shrunk.value(d).unwrap();
            assert!(
                (value - slope * d).abs() < 0.15,
                "trend must survive at {d}: {value} vs {}",
                slope * d
            );
        }
    }
}

#[test]
fn curve_refuses_non_finite_queries() {
    let window = sorted_window((0..10).map(|i| event(i as f64 / 20.0, 0.1)).collect());
    let curve = Curve::fit_shrunk(&window, Target::Failure).unwrap();
    assert_eq!(curve.value(f64::NAN), None);
    assert_eq!(curve.value(f64::INFINITY), None);
    assert!(curve.value(0.2).is_some());
}

#[test]
fn curves_need_a_target_specific_minimum_of_points() {
    let window = sorted_window((0..4).map(|i| event(i as f64 / 10.0, 0.1)).collect());
    assert!(Curve::fit_shrunk(&window, Target::Failure).is_none());
    let window = sorted_window((0..29).map(|i| event(i as f64 / 60.0, 0.1)).collect());
    assert!(Curve::fit_shrunk(&window, Target::Failure).is_some());
    assert!(Curve::fit_shrunk(&window, Target::LogResponseTime).is_none());
    let window = sorted_window((0..30).map(|i| event(i as f64 / 60.0, 0.1)).collect());
    assert!(Curve::fit_shrunk(&window, Target::LogTransferSpeed).is_some());
}

/// A log curve holds its end values beyond the fitted range; the failure curve
/// keeps the reference's centroid extrapolation (and is clamped to [0, 1]).
#[test]
fn log_curves_do_not_extrapolate_beyond_the_fitted_range() {
    let window = sorted_window(
        (0..60)
            .map(|i| {
                let d = 0.1 + i as f64 / 300.0;
                event(d, 10.0 * d)
            })
            .collect(),
    );
    let log = Curve::fit_shrunk(&window, Target::LogResponseTime).unwrap();
    let first = log.blocks.first().unwrap().y;
    let last = log.blocks.last().unwrap().y;
    assert_eq!(log.value(0.0), Some(first));
    assert_eq!(log.value(0.5), Some(last));
    let failure = Curve::pav(
        window.iter().map(|e| Block {
            x: e.distance,
            y: e.y / 10.0,
            w: 1.0,
        }),
        true,
    )
    .unwrap();
    assert!(
        failure.value(0.5).unwrap() > failure.blocks.last().unwrap().y,
        "the failure curve keeps linear extrapolation"
    );
}

// ---------------------------------------------------------------------------
// Level: decay algebra and variance components
// ---------------------------------------------------------------------------

/// Epoch-scaled storage must give the same means and Kish factors as naive
/// per-event exponential forgetting.
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
    let naive = |filter: &dyn Fn(usize, usize) -> bool| {
        events
            .iter()
            .filter(|(_, _, slot, band)| filter(*slot, *band))
            .fold((0.0, 0.0, 0.0), |(n, w2, s), &(t, r, _, _)| {
                let w = (-(now - t) / horizon).exp();
                (n + w, w2 + w * w, s + w * r)
            })
    };
    let (n, w2, sum) = naive(&|_, _| true);
    assert!((level.root.mean() - sum / n).abs() < 1e-12);
    assert!((level.root.mean_variance_factor() - w2 / (n * n)).abs() < 1e-12);
    let (n, w2, sum) = naive(&|slot, band| slot == 0 && band == 1);
    let cell = level.nodes[0].cells[1];
    assert!((cell.mean() - sum / n).abs() < 1e-12);
    assert!((cell.effective_n() - n * n / w2).abs() < 1e-9);

    let (sq_peers, sq_cells) = (level.sq_peers, level.sq_cells);
    level.recount_squares();
    assert!((sq_peers - level.sq_peers).abs() < 1e-9);
    assert!((sq_cells - level.sq_cells).abs() < 1e-9);
}

fn fill_level(level: &mut Level, observations: &[(usize, usize, f64)]) {
    for &(slot, band, y) in observations {
        level.add(Some(slot), band, 1.0, y);
    }
    level.recount_squares();
}

/// Method of moments recovers known components on balanced traffic.
#[test]
fn variance_components_recover_the_generating_values() {
    let _guard = GlobalRng::seed_guard(0x4485_7a02);
    let (sigma, tau_peer, tau_cell) = (1.0, 0.5, 0.3);
    let mut observations = Vec::new();
    for slot in 0..60 {
        let peer_effect = tau_peer * normal();
        for band in 0..BANDS {
            let cell_effect = tau_cell * normal();
            for _ in 0..40 {
                observations.push((slot, band, peer_effect + cell_effect + sigma * normal()));
            }
        }
    }
    let mut level = Level::new(None);
    fill_level(&mut level, &observations);
    let c = level.compute_components().expect("components exist");
    assert!((c.sigma2 - 1.0).abs() < 0.08, "sigma2 {}", c.sigma2);
    assert!(
        (c.tau2_peer - 0.25).abs() < 0.12,
        "tau2_peer {}",
        c.tau2_peer
    );
    assert!(
        (c.tau2_cell - 0.09).abs() < 0.04,
        "tau2_cell {}",
        c.tau2_cell
    );
}

/// The case the balanced test hides: routing locality puts most of a peer's
/// traffic in one home band, and traffic across peers is Zipf. A child that
/// dominates its parent makes a child-vs-parent contrast nearly zero, so the
/// earlier estimator read `tau2_cell` (and through it `tau2_peer`) far too low.
/// Averaged over seeds so the tolerance can be tight.
#[test]
fn variance_components_recover_the_truth_under_home_band_zipf_traffic() {
    let (sigma, tau_peer, tau_cell) = (1.0, 0.5, 0.3);
    let peers = 200;
    let seeds = 8;
    let (mut tp, mut tc, mut s2) = (0.0, 0.0, 0.0);
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_2b00 + seed);
        let peer_effects: Vec<f64> = (0..peers).map(|_| tau_peer * normal()).collect();
        let cell_effects: Vec<[f64; BANDS]> = (0..peers)
            .map(|_| std::array::from_fn(|_| tau_cell * normal()))
            .collect();
        let homes: Vec<usize> = (0..peers)
            .map(|_| GlobalRng::random_range(0..BANDS))
            .collect();
        let observations: Vec<(usize, usize, f64)> = (0..10_000)
            .map(|_| {
                let peer = zipf(peers, 1.2);
                let band = if uniform() < 0.9 {
                    homes[peer]
                } else {
                    GlobalRng::random_range(0..BANDS)
                };
                (
                    peer,
                    band,
                    peer_effects[peer] + cell_effects[peer][band] + sigma * normal(),
                )
            })
            .collect();
        let mut level = Level::new(None);
        fill_level(&mut level, &observations);
        let c = level.compute_components().expect("components exist");
        tp += c.tau2_peer / seeds as f64;
        tc += c.tau2_cell / seeds as f64;
        s2 += c.sigma2 / seeds as f64;
    }
    assert!((s2 - 1.0).abs() < 0.05, "sigma2 {s2}");
    assert!(
        (tc - 0.09).abs() < 0.03,
        "tau2_cell must be recovered under home-band traffic, got {tc} (truth 0.09)"
    );
    assert!(
        (tp - 0.25).abs() < 0.08,
        "tau2_peer must be recovered under Zipf traffic, got {tp} (truth 0.25)"
    );
}

/// A peer seen in one band only has no other cell to contrast against.
#[test]
fn single_band_peers_do_not_inform_tau2_cell() {
    let _guard = GlobalRng::seed_guard(0x4485_1b4d);
    let mut observations = Vec::new();
    for slot in 0..50 {
        // Huge between-peer spread, but every peer lives in one band.
        let effect = 3.0 * normal();
        for _ in 0..30 {
            observations.push((slot, slot % BANDS, effect + 0.2 * normal()));
        }
    }
    let mut level = Level::new(None);
    fill_level(&mut level, &observations);
    let c = level.compute_components().expect("components exist");
    assert_eq!(
        c.tau2_cell, 0.0,
        "with no multi-band peer there is no evidence for cell effects"
    );
    assert!(
        c.tau2_peer > 4.0,
        "peer spread must land in tau2_peer: {}",
        c.tau2_peer
    );
}

#[test]
fn variance_components_need_within_cell_replication() {
    let mut level = Level::new(None);
    for slot in 0..10 {
        level.add(Some(slot), slot % BANDS, 1.0, 0.5);
    }
    level.recount_squares();
    assert_eq!(level.compute_components(), None);
    assert_eq!(level.residual(Some(0), 0), None);
}

/// Kish effective size at a short horizon: 2 events per cell per hour at a
/// 1.5h horizon. Counting the raw weight sum overstates each cell's evidence
/// about 2x, which drove `tau2` to zero; counting `n^2 / sum w^2` recovers it.
#[test]
fn kish_counting_recovers_tau2_at_a_short_horizon() {
    let (hours, horizon, rate) = (48.0, 1.5, 2.0);
    let (sigma, tau_cell) = (1.0, 0.4);
    let (mut tc, mut s2) = (0.0, 0.0);
    let seeds = 6;
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_d3c4 + seed);
        let mut level = Level::new(Some(horizon));
        level.reset(hours);
        for slot in 0..50 {
            for band in 0..BANDS {
                let effect = tau_cell * normal();
                for _ in 0..(rate * hours) as usize {
                    let t = uniform() * hours;
                    level.add(Some(slot), band, level.weight(t), effect + sigma * normal());
                }
            }
        }
        level.recount_squares();
        let c = level.compute_components().expect("components exist");
        tc += c.tau2_cell / seeds as f64;
        s2 += c.sigma2 / seeds as f64;
    }
    assert!((s2 - 1.0).abs() < 0.08, "sigma2 {s2}");
    assert!(
        (tc - 0.16).abs() < 0.05,
        "tau2_cell must survive a short horizon, got {tc} (truth 0.16)"
    );
}

/// The two shrinkage limits: an unknown peer gets only the root's pooled
/// offset, and a heavily-observed cell gets (almost) its own mean.
#[test]
fn shrinkage_limits_no_data_to_pool_and_lots_of_data_to_cell_mean() {
    let _guard = GlobalRng::seed_guard(0x4485_11a1);
    let mut level = Level::new(None);
    for slot in 0..=30 {
        for band in 0..BANDS {
            for _ in 0..20 {
                level.add(Some(slot), band, 1.0, 0.2 * normal());
            }
        }
    }
    for _ in 0..2_000 {
        level.add(Some(30), 5, 1.0, 2.0 + 0.2 * normal());
    }
    level.recount_squares();
    level.components = level.compute_components();
    assert!(level.components.is_some());

    let hot = level.residual(Some(30), 5).unwrap().mean;
    let cell_mean = level.nodes[30].cells[5].mean();
    assert!(
        (hot - cell_mean).abs() < 0.05,
        "abundant evidence must yield the cell's own mean: {hot} vs {cell_mean}"
    );
    let unknown = level.residual(None, 0).unwrap();
    assert!(
        unknown.mean.abs() < hot.abs() * 0.5,
        "an unknown peer must not inherit another peer's cell effect: {}",
        unknown.mean
    );
    assert!(
        unknown.variance > level.residual(Some(30), 5).unwrap().variance,
        "an unknown peer carries more posterior variance than a well-observed cell"
    );
    let other_band = level.residual(Some(30), 2).unwrap().mean;
    assert!(
        other_band < hot,
        "a band with no data for that peer shrinks toward the peer, not the hot cell"
    );
}

// ---------------------------------------------------------------------------
// Stage
// ---------------------------------------------------------------------------

#[test]
fn cold_stage_predicts_nothing() {
    let stage: Stage<u32> = Stage::new(Target::Failure, 64);
    assert_eq!(predict(&stage, &1, 0.3, 0.1), None);
    assert!(!stage.diagnostics().active);
}

/// With data but no peer structure the prediction is the curve.
#[test]
fn prediction_equals_the_curve_when_there_is_no_hierarchy_signal() {
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime, 64);
    for i in 0..400u32 {
        let distance = (i % 50) as f64 / 100.0;
        observe(
            &mut stage,
            &(i % 7),
            (i % 13) as f64 / 13.0,
            distance,
            distance,
            0.0,
        );
    }
    let curve = stage.curve.as_ref().unwrap().value(0.25).unwrap();
    let predicted = predict(&stage, &3, 0.4, 0.25).unwrap();
    assert!(
        (predicted - curve).abs() < 1e-6,
        "noise-free data carries no residual: predicted {predicted}, curve {curve}"
    );
}

/// `observe` must return the forecast `predict` would have made just before.
#[test]
fn observe_returns_the_pre_learning_forecast() {
    let _guard = GlobalRng::seed_guard(0x4485_9e1d);
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let mut scratch = Scratch::default();
    for i in 0..3_000u32 {
        let peer = i % 23;
        let contract = uniform();
        let distance = uniform() * 0.5;
        let y = f64::from(u8::from(uniform() < 0.05 + distance * 0.3));
        let before = stage.predict(&peer, contract, distance);
        let returned = stage.observe(&mut scratch, &peer, contract, distance, y, i as f64 / 60.0);
        assert_eq!(before, returned, "event {i}");
    }
}

#[test]
fn window_stays_bounded_and_sorted() {
    let _guard = GlobalRng::seed_guard(0x4485_b0d1);
    let capacity = 1_000;
    let mut stage: Stage<u32> = Stage::with_limits(Target::Failure, capacity, 64);
    let mut scratch = Scratch::default();
    let total = 5_321u64;
    for i in 0..total {
        stage.observe(
            &mut scratch,
            &((i % 40) as u32),
            uniform(),
            uniform() * 0.5,
            f64::from(u8::from(uniform() < 0.1)),
            i as f64 / 60.0,
        );
        assert!(stage.diagnostics().window_events <= capacity + refit_interval(capacity, capacity));
        assert!(
            scratch.prepared.is_empty(),
            "the shared refit buffer holds nothing between refits"
        );
    }
    refit(&mut stage, total as f64 / 60.0);
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
    assert!(
        stage.sorted.capacity() <= capacity + refit_interval(capacity, capacity)
            && scratch.prepared.capacity() <= capacity,
        "allocations must stay at their bound: sorted {}, prepared {}",
        stage.sorted.capacity(),
        scratch.prepared.capacity()
    );
}

/// Once the window is full a refit runs every `max(50, window / 100)` events,
/// and every 50 while it fills.
#[test]
fn refit_cadence_slows_once_the_window_is_full() {
    assert_eq!(refit_interval(10_000, 9_999), 50);
    assert_eq!(refit_interval(10_000, 10_000), 100);
    assert_eq!(refit_interval(2_000, 2_000), 50);
    assert_eq!(refit_interval(40_000, 40_000), 400);

    let mut stage: Stage<u32> = Stage::with_limits(Target::Failure, 10_000, 64);
    let mut scratch = Scratch::default();
    for i in 0..10_000u32 {
        stage.observe(
            &mut scratch,
            &(i % 9),
            0.5,
            (i % 97) as f64 / 200.0,
            0.0,
            0.0,
        );
    }
    let before = stage.refits;
    for i in 0..1_000u32 {
        stage.observe(
            &mut scratch,
            &(i % 9),
            0.5,
            (i % 97) as f64 / 200.0,
            0.0,
            0.0,
        );
    }
    assert_eq!(
        stage.refits - before,
        10,
        "1000 events on a full 10k window must refit 10 times"
    );
}

/// Peers churn: the table is bounded, newcomers are admitted by evicting the
/// least recently used, and evictions are counted.
#[test]
fn peer_table_is_bounded_by_lru_eviction() {
    let max_peers = 64;
    let mut stage: Stage<u32> = Stage::with_limits(Target::Failure, 2_000, max_peers);
    // Peer 0 on every even event; a fresh peer 1 + i on every odd one, so the
    // one-shot peers are 2, 4, 6, ...
    for i in 0..2_000u32 {
        let peer = if i % 2 == 0 { 0 } else { 1 + i };
        observe(&mut stage, &peer, 0.5, 0.1, 0.0, 0.0);
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
        stage.peers.lookup(&2).is_none(),
        "the oldest one-shot peer must be gone"
    );
}

/// Batched eviction at the production-derived capacity (batch > 1): counts
/// advance in whole batches, and the squared counts maintained incrementally
/// through evictions and live adds equal an exact recount at every step.
#[test]
fn batched_eviction_at_production_capacity_keeps_squared_counts_exact() {
    let _guard = GlobalRng::seed_guard(0x4485_ba7c);
    let capacity = peer_capacity(Ring::DEFAULT_MAX_CONNECTIONS);
    assert_eq!(capacity, 400);
    let batch = (capacity / 64).max(1);
    assert!(batch > 1, "the production batch must exceed one");
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 10_000, capacity);
    let mut scratch = Scratch::default();
    let mut next_new = 0u32;
    let mut evictions_seen = 0;
    for i in 0..6_000u32 {
        // Two thirds of traffic to fresh peers, the rest to a stable set.
        let peer = if i % 3 == 0 {
            1_000_000 + (i / 3) % 50
        } else {
            next_new += 1;
            next_new
        };
        stage.observe(
            &mut scratch,
            &peer,
            uniform(),
            uniform() * 0.5,
            normal(),
            i as f64 / 600.0,
        );
        let evictions = stage.peers.evictions;
        assert_eq!(
            evictions % batch as u64,
            0,
            "evictions advance in whole batches"
        );
        if evictions > evictions_seen {
            evictions_seen = evictions;
            for level in &stage.levels {
                let mut recounted = level.clone();
                recounted.recount_squares();
                let close = |a: f64, b: f64| (a - b).abs() <= 1e-6 * b.abs().max(1.0);
                assert!(
                    close(level.sq_peers, recounted.sq_peers)
                        && close(level.sq_cells, recounted.sq_cells),
                    "incremental squared counts drifted after eviction at event {i}: \
                     {} vs {}, {} vs {}",
                    level.sq_peers,
                    recounted.sq_peers,
                    level.sq_cells,
                    recounted.sq_cells
                );
            }
        }
    }
    assert!(
        evictions_seen >= 4_000 - capacity as u64,
        "evictions {evictions_seen}"
    );
    assert_eq!(stage.diagnostics().peer_capacity, capacity);
    for key in 1_000_000..1_000_050 {
        assert!(
            stage.peers.lookup(&key).is_some(),
            "stable peer {key} must survive"
        );
    }
}

#[test]
fn peer_capacity_derives_from_max_connections_with_a_floor() {
    assert_eq!(peer_capacity(200), 400);
    assert_eq!(peer_capacity(1_000), 2_000);
    assert_eq!(peer_capacity(5), 64);
    assert_eq!(peer_capacity(usize::MAX), usize::MAX);
}

/// An evicted slot's statistics must not leak into the peer that reuses it.
#[test]
fn a_reused_slot_starts_clean() {
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 5_000, 2);
    for i in 0..300 {
        observe(&mut stage, &1, 0.1, (i % 10) as f64 / 20.0, 5.0, 0.0);
        observe(&mut stage, &2, 0.1, (i % 10) as f64 / 20.0, 0.0, 0.0);
    }
    // Peer 3 evicts peer 1 (least recently used) and takes its slot.
    observe(&mut stage, &3, 0.1, 0.2, 2.5, 0.0);
    let slot = stage.peers.lookup(&3).unwrap();
    for level in &stage.levels {
        let node = level.nodes[slot];
        assert!(
            (node.peer.n - level.weight(0.0)).abs() < 1e-9 || node.peer.n == 0.0,
            "reused slot must hold only the newcomer's evidence, n={}",
            node.peer.n
        );
    }
    refit(&mut stage, 0.0);
    assert!(stage.diagnostics().orphaned_at_last_refit >= 300);
}

/// Non-finite and out-of-range inputs are refused and counted, and never reach
/// a prediction.
#[test]
fn non_finite_inputs_are_rejected_and_counted() {
    let _guard = GlobalRng::seed_guard(0x4485_0a0a);
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    for i in 0..500u32 {
        observe(&mut stage, &(i % 5), uniform(), uniform() * 0.5, 0.0, 0.0);
    }
    let rejected_before = stage.diagnostics().rejected;
    observe(&mut stage, &1, 0.2, 0.1, f64::NAN, 0.0);
    observe(&mut stage, &1, 0.2, f64::INFINITY, 1.0, 0.0);
    observe(&mut stage, &1, 0.2, 0.1, 2.0, 0.0);
    observe(&mut stage, &1, 0.2, 0.1, -1.0, 0.0);
    assert_eq!(stage.diagnostics().rejected, rejected_before + 4);
    observe(&mut stage, &1, f64::NAN, 0.1, 1.0, f64::NAN);
    observe(&mut stage, &1, f64::INFINITY, 0.1, 1.0, f64::INFINITY);
    observe(&mut stage, &1, -3.0, 0.1, 1.0, f64::NEG_INFINITY);
    for query in [0.0, 0.1, 0.49, 0.5, 1.0, -1.0] {
        let p = predict(&stage, &1, 0.3, query).unwrap();
        assert!((0.0..=1.0).contains(&p), "prediction {p} at {query}");
    }
    assert_eq!(predict(&stage, &1, 0.3, f64::NAN), None);
    assert!(predict(&stage, &1, f64::NAN, 0.1).is_some());

    let mut timing: Stage<u32> = Stage::new(Target::LogResponseTime, 64);
    observe(&mut timing, &1, 0.2, 0.1, f64::NEG_INFINITY, 0.0);
    assert_eq!(timing.diagnostics().rejected, 1);
}

/// A clock that steps backwards is read as "no time passed", and a long
/// silence cannot overflow the epoch-scaled weights.
#[test]
fn clock_steps_and_long_gaps_stay_finite() {
    let _guard = GlobalRng::seed_guard(0x4485_c10c);
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime, 64);
    let mut time = 0.0;
    for i in 0..2_000u32 {
        time += if i % 97 == 0 { 10_000.0 } else { 0.01 };
        let t = if i % 13 == 0 { time - 50.0 } else { time };
        observe(
            &mut stage,
            &(i % 11),
            uniform(),
            uniform() * 0.5,
            normal(),
            t,
        );
        if let Some(forecast) = stage.predict(&(i % 11), 0.5, 0.2) {
            assert!(forecast.value.is_finite() && forecast.spread.is_finite());
        }
    }
    for level in &stage.levels {
        assert!(level.root.n.is_finite() && level.root.w2.is_finite());
        assert!(level.sq_peers.is_finite() && level.sq_cells.is_finite());
    }
}

/// Log predictions stay inside the observed range widened by the margin, even
/// for a peer whose offset would push them far outside it.
#[test]
fn log_predictions_are_bounded_by_the_observed_range() {
    let _guard = GlobalRng::seed_guard(0x4485_b0b0);
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime, 64);
    for i in 0..3_000u32 {
        let peer = i % 10;
        // Peer 0 is far slower than everyone else, but only near distance 0.
        let y = if peer == 0 {
            3.0
        } else {
            (0.05f64).ln() + 0.1 * normal()
        };
        observe(&mut stage, &peer, 0.3, uniform() * 0.1, y, 0.0);
    }
    let (low, high) = stage.observed_range;
    for (peer, distance) in [(0, 0.5), (0, 0.0), (7, 0.5), (99, 0.45)] {
        let value = predict(&stage, &peer, 0.3, distance).unwrap();
        assert!(
            value >= low - LOG_PREDICTION_MARGIN && value <= high + LOG_PREDICTION_MARGIN,
            "prediction {value} for peer {peer} at {distance} escaped [{low}, {high}] +- margin"
        );
    }
}

/// The selector scores the finished forecast the router would act on.
#[test]
fn horizon_loss_scores_the_clamped_forecast() {
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let forecasts = [0.9, 1.0, 0.4, 0.0].map(|value| Forecast { value, spread: 0.0 });
    stage.score(&forecasts, 1.0, 0.0);
    let expected = [0.01, 0.0, 0.36, 1.0];
    for (loss, want) in stage.loss.iter().zip(expected) {
        assert!((loss - want).abs() < 1e-12, "loss {loss} expected {want}");
    }
    assert_eq!(stage.selected(), 1);
}

/// Horizon selection: on stationary data nothing forgets; once peer behaviour
/// drifts, a forgetting horizon takes over.
#[test]
fn horizon_selection_switches_to_forgetting_after_drift() {
    let _guard = GlobalRng::seed_guard(0x4485_d71f);
    let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime, 64);
    let mut scratch = Scratch::default();
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
            stage.observe(
                &mut scratch,
                &peer,
                uniform(),
                distance,
                y,
                index as f64 / 60.0,
            );
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

/// Every refit re-derives residuals against the new curve.
#[test]
fn residuals_are_recomputed_against_the_current_curve() {
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 10_000, 64);
    for i in 0..1_000u32 {
        let y = if i < 500 { 0.0 } else { 3.0 };
        observe(&mut stage, &(i % 10), 0.5, (i % 50) as f64 / 100.0, y, 0.0);
    }
    refit(&mut stage, 0.0);
    let curve = stage.curve.as_ref().unwrap();
    for level in &stage.levels {
        let expected: f64 = stage
            .sorted
            .iter()
            .map(|e| e.y - stage.bound(curve.value(e.distance).unwrap()))
            .sum();
        assert!(
            (level.root.sum - expected).abs() < 1e-6,
            "root residual sum {} must match the current curve's {expected}",
            level.root.sum
        );
    }
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
        observe(
            &mut stage,
            &(i % 9),
            uniform(),
            uniform() * 0.5,
            normal(),
            time,
        );
    }
    let now = *times.last().unwrap();
    refit(&mut stage, now);
    for level in &stage.levels {
        let (n, w2) = times.iter().fold((0.0, 0.0), |(n, w2), t| {
            let w = level.horizon_hours.map_or(1.0, |h| (-(now - t) / h).exp());
            (n + w, w2 + w * w)
        });
        assert!(
            (level.root.n - n).abs() < 1e-9 * n.max(1.0)
                && (level.root.w2 - w2).abs() < 1e-9 * w2.max(1.0),
            "horizon {:?}: rebuilt ({}, {}) vs naive ({n}, {w2})",
            level.horizon_hours,
            level.root.n,
            level.root.w2
        );
    }
}

/// Refit cost at a full production window, phase by phase. Printed for the PR;
/// a wall-clock bound in a unit test is a flaky test. Run with
/// `--release -- --nocapture` for the real numbers.
#[test]
fn refit_and_prediction_cost_at_a_full_window() {
    let _guard = GlobalRng::seed_guard(0x4485_be7c);
    let peers = 200u32;
    let mut stage: Stage<u32> = Stage::new(Target::Failure, peer_capacity(peers as usize));
    let mut scratch = Scratch::default();
    for i in 0..WINDOW_EVENTS as u64 + 7 {
        let distance = uniform() * 0.5;
        let y = f64::from(u8::from(uniform() < 0.02 + distance * 0.2));
        stage.observe(
            &mut scratch,
            &GlobalRng::random_range(0..peers),
            uniform(),
            distance,
            y,
            i as f64 / 600.0,
        );
    }
    let now = WINDOW_EVENTS as f64 / 600.0;
    refit(&mut stage, now);
    assert_eq!(stage.sorted.len(), WINDOW_EVENTS);

    // Minimum over rounds: on a shared machine it is the least contaminated
    // estimate of the work itself.
    let rounds = 40u32;
    let interval = refit_interval(WINDOW_EVENTS, WINDOW_EVENTS);
    let mut phases = [std::time::Duration::MAX; 4];
    for round in 0..rounds {
        for i in 0..interval {
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
        stage.curve = Curve::fit_shrunk(&stage.sorted, Target::Failure);
        mark(1);
        for level in &mut stage.levels {
            level.reset(now);
        }
        assert!(stage.prepare(&mut scratch.prepared));
        mark(2);
        stage.rebuild_levels(&mut scratch.prepared, now);
        mark(3);
    }
    let [merge, curve, prepare, levels] = phases;
    let per_refit = merge + curve + prepare + levels;

    let queries = 25 * 3 * 1_000;
    let start = std::time::Instant::now();
    let mut acc = 0.0;
    for q in 0..queries {
        acc += predict(&stage, &(q as u32 % peers), 0.37, (q % 500) as f64 / 1000.0).unwrap_or(0.0);
    }
    let per_prediction = start.elapsed() / queries as u32;
    eprintln!(
        "#4485 hierarchical cost over {WINDOW_EVENTS} events: refit {per_refit:?} = merge \
         {merge:?} + curve {curve:?} + re-anchor {prepare:?} + levels {levels:?}, every \
         {interval} events once full ({:?} amortised per event); prediction \
         {per_prediction:?} (sum {acc:.3}); curve blocks {}",
        per_refit / interval as u32,
        stage.curve.as_ref().map_or(0, |c| c.blocks.len())
    );
}

// ---------------------------------------------------------------------------
// HierarchicalRouting
// ---------------------------------------------------------------------------

fn timed(seconds: Option<f64>, speed: Option<f64>) -> RoutingOutcome {
    RoutingOutcome {
        success: true,
        time_to_response_start_secs: seconds,
        transfer_speed_bps: speed,
    }
}

#[test]
fn routing_bundle_feeds_each_stage_from_its_own_outcomes() {
    let mut routing = HierarchicalRouting::new(200);
    let peer = PeerKeyLocation::random();
    let contract = Location::new(0.3);
    for i in 0..400 {
        let outcome = match i % 4 {
            0 => RoutingOutcome {
                success: false,
                time_to_response_start_secs: None,
                transfer_speed_bps: None,
            },
            1 => timed(None, None),
            2 => timed(Some(0.25), Some(50_000.0)),
            // A zero-byte payload and a sub-millisecond response.
            _ => timed(Some(0.0), Some(0.0)),
        };
        routing.observe_at(&peer, contract, 0.1 + (i % 10) as f64 / 40.0, &outcome, 0.0);
    }
    let [failure, response, transfer] = routing.diagnostics();
    assert_eq!(failure.window_events, 400);
    assert_eq!(response.window_events, 200, "floored times are learned");
    assert_eq!(response.rejected, 0);
    assert_eq!(routing.floored_response_times(), 100);
    assert_eq!(transfer.window_events, 100);
    assert_eq!(
        transfer.rejected, 0,
        "a zero-byte payload is not a speed sample, and not a rejection"
    );
    assert_eq!(routing.non_speed_samples(), 100);
    assert_eq!(routing.diagnostics()[0].peer_capacity, 400);
}

/// Timing and speed come back as the expectations the cost formula needs.
#[test]
fn timing_and_speed_estimates_are_expectations_not_medians() {
    let _guard = GlobalRng::seed_guard(0x4485_e7a1);
    let (mu_t, sd_t) = ((0.2f64).ln(), 0.8);
    let (mu_v, sd_v) = ((40_000.0f64).ln(), 0.9);
    let peers: Vec<PeerKeyLocation> = (0..10).map(|_| PeerKeyLocation::random()).collect();
    let mut routing = HierarchicalRouting::new(200);
    for i in 0..6_000 {
        let outcome = timed(
            Some((mu_t + sd_t * normal()).exp()),
            Some((mu_v + sd_v * normal()).exp()),
        );
        routing.observe_at(
            &peers[i % peers.len()],
            Location::new(uniform()),
            uniform() * 0.5,
            &outcome,
            0.0,
        );
    }
    let estimate = routing.estimate(&peers[3], Location::new(0.5), 0.25);
    let mean_time = (mu_t + sd_t * sd_t / 2.0).exp();
    let median_time = mu_t.exp();
    let time = estimate.time_to_response_start_secs.unwrap();
    assert!(
        (time / mean_time - 1.0).abs() < 0.1,
        "E[T] {mean_time}, median {median_time}, estimated {time}"
    );
    // bytes / speed must equal bytes * E[1/v] = bytes * exp(-mu + sd^2/2).
    let effective_speed = (mu_v - sd_v * sd_v / 2.0).exp();
    let speed = estimate.transfer_speed_bps.unwrap();
    assert!(
        (speed / effective_speed - 1.0).abs() < 0.1,
        "effective speed {effective_speed}, median {}, estimated {speed}",
        mu_v.exp()
    );
}

/// An unknown peer is priced as slower than a well-observed typical one: the
/// posterior variance is a deliberate cold-peer penalty.
#[test]
fn an_unknown_peer_carries_a_timing_penalty() {
    let _guard = GlobalRng::seed_guard(0x4485_c01d);
    let peers: Vec<PeerKeyLocation> = (0..40).map(|_| PeerKeyLocation::random()).collect();
    let effects: Vec<f64> = (0..peers.len()).map(|_| 0.6 * normal()).collect();
    let mut routing = HierarchicalRouting::new(200);
    for i in 0..8_000 {
        let p = i % peers.len();
        let seconds = ((0.2f64).ln() + effects[p] + 0.3 * normal()).exp();
        routing.observe_at(
            &peers[p],
            Location::new(uniform()),
            uniform() * 0.5,
            &timed(Some(seconds), None),
            0.0,
        );
    }
    let typical = (0..peers.len())
        .min_by(|&a, &b| effects[a].abs().total_cmp(&effects[b].abs()))
        .unwrap();
    let known = routing
        .estimate(&peers[typical], Location::new(0.5), 0.25)
        .time_to_response_start_secs
        .unwrap();
    let unknown = routing
        .estimate(&PeerKeyLocation::random(), Location::new(0.5), 0.25)
        .time_to_response_start_secs
        .unwrap();
    assert!(
        unknown > known,
        "an unseen peer must be priced slower than a typical known one: {unknown} vs {known}"
    );
}

/// Per-candidate cost of a full three-stage estimate with real peer keys.
/// Printed, not asserted.
#[test]
fn routing_estimate_cost_per_candidate() {
    let _guard = GlobalRng::seed_guard(0x4485_ca4d);
    let peers: Vec<PeerKeyLocation> = (0..200).map(|_| PeerKeyLocation::random()).collect();
    let mut routing = HierarchicalRouting::new(200);
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
    let queries = 25_000;
    let start = std::time::Instant::now();
    let mut available = 0;
    for q in 0..queries {
        let estimate = routing.estimate(
            &peers[q % peers.len()],
            Location::new((q % 997) as f64 / 997.0),
            (q % 500) as f64 / 1000.0,
        );
        available += usize::from(estimate.transfer_speed_bps.is_some());
    }
    let per_candidate = start.elapsed() / queries as u32;
    eprintln!(
        "#4485 hierarchical estimate (3 stages, PeerKeyLocation keys): {per_candidate:?} per \
         candidate; sizes: Event {} B, Prepared {} B, PeerNode {} B, Level {} B",
        std::mem::size_of::<Event>(),
        std::mem::size_of::<Prepared>(),
        std::mem::size_of::<PeerNode>(),
        std::mem::size_of::<Level>(),
    );
    assert_eq!(
        available, queries,
        "every stage must be active after a full window"
    );
}

use crate::ring::Ring;
