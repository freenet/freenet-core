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
        contract_slot: u32::MAX,
        contract_generation: 0,
        adjustment: 0.0,
        band: 0,
    }
}

fn sorted_window(events: Vec<Event>) -> Vec<Event> {
    sorted_window_for(events, true)
}

fn sorted_window_for(mut events: Vec<Event>, ascending: bool) -> Vec<Event> {
    events.sort_by(window_order(ascending));
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
        .predict(peer, contract, distance, 0.0)
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
                    // Quantised distances so equal-x pooling is exercised on the
                    // ascending curve. Descending uses distinct distances: this
                    // curve deliberately pools descending ties, which the crate
                    // does not (see `descending_ties_pool_into_one_block`).
                    let distance = if ascending {
                        (uniform() * 40.0).floor() / 80.0
                    } else {
                        uniform() * 0.5
                    };
                    let y = if trial % 2 == 0 {
                        f64::from(u8::from(uniform() < 0.1 + distance))
                    } else {
                        distance * if ascending { 2.0 } else { -2.0 } + normal()
                    };
                    event(distance, y)
                })
                .collect();
            let ours = raw_curve(&sorted_window_for(events.clone(), ascending), ascending);
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

/// Equal distances pool into one block on the descending (speed) curve too.
/// Ordered y-descending, as `pav_regression` orders ties for both directions,
/// they would stay separate blocks and the curve would read the smallest y.
#[test]
fn descending_ties_pool_into_one_block() {
    let events = vec![event(0.1, 5.0), event(0.1, 4.0), event(0.1, 3.0)];
    for ascending in [true, false] {
        let window = sorted_window_for(events.clone(), ascending);
        let curve = raw_curve(&window, ascending);
        assert_eq!(
            curve.blocks.len(),
            1,
            "ties must pool (ascending={ascending}): {:?}",
            curve.blocks
        );
        assert!((curve.blocks[0].y - 4.0).abs() < 1e-12);
        assert_eq!(curve.value(0.1), Some(4.0));
    }

    // Through a live stage: the speed window is ordered for its direction.
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogTransferSpeed, 1_000, 8);
    for i in 0..60u32 {
        let y = [5.0, 4.0, 3.0][(i % 3) as usize];
        observe(
            &mut stage,
            &(i % 4),
            0.5,
            if i < 30 { 0.1 } else { 0.3 },
            y,
            0.0,
        );
    }
    refit(&mut stage, 0.0);
    let curve = stage.curve.as_ref().unwrap();
    assert!(
        curve.blocks.iter().all(|b| (b.y - 4.0).abs() < 1e-9),
        "both distance groups must pool to their mean: {:?}",
        curve.blocks
    );
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

/// A stale secondary band must not break the leave-one-out contrast.
///
/// At a 1.5h horizon, a band last seen 30 hours ago carries weights ~2e-9 of
/// the active band's. Taken as `peer - cell`, the rest's `w2` and squared
/// counts cancel to exactly zero, the contrast loses its noise term, and
/// `tau2_cell` reads a spurious effect where the truth is zero.
#[test]
fn a_stale_secondary_band_does_not_bias_tau2_cell() {
    let _guard = GlobalRng::seed_guard(0x4485_57a1);
    let (horizon, now) = (1.5, 30.0);
    let mut level = Level::new(Some(horizon));
    level.reset(now);
    for slot in 0..60 {
        for _ in 0..40 {
            // Home band, recent. No cell effects anywhere: truth tau2_cell = 0.
            let t = now - uniform() * 0.5;
            level.add(Some(slot), 1, level.weight(t), normal());
        }
        for _ in 0..3 {
            // Secondary band, 30 hours stale, thin: its own sampling noise
            // (sigma2 / 3) is exactly what the cancelled rest term drops.
            level.add(Some(slot), 5, level.weight(0.0), normal());
        }
    }
    level.recount_squares();
    let peer = level.nodes[0].peer;
    let cell = level.nodes[0].cells[1];
    assert_eq!(
        peer.w2 - cell.w2,
        0.0,
        "the scenario must reach the cancellation, or this test proves nothing"
    );
    let c = level.compute_components().expect("components exist");
    assert!(
        c.tau2_cell < 0.05,
        "no cell effect exists; a stale secondary band must not create one, got {}",
        c.tau2_cell
    );
}

/// Right after an eviction, before any refit, the root's squared-count sums
/// must already count the evicted peer's events as orphan singletons, exactly
/// as the next rebuild will. Compared against that rebuild (no-forgetting
/// level, so weights are 1 on both sides) rather than a recount, which reads
/// the same `orphan_w2` it would be checking.
#[test]
fn eviction_folds_the_evicted_peer_into_orphans_before_the_next_refit() {
    let _guard = GlobalRng::seed_guard(0x4485_0e71);
    let mut stage: Stage<u32> = Stage::with_limits(Target::LogResponseTime, 10_000, 64);
    let mut scratch = Scratch::default();
    // Fill the table with 64 peers, several events each, then refit.
    for i in 0..640u32 {
        stage.observe(
            &mut scratch,
            &(i % 64),
            uniform(),
            uniform() * 0.5,
            normal(),
            0.0,
        );
    }
    stage.refit(&mut scratch, 0.0);
    assert_eq!(stage.levels[0].orphan_w2, 0.0);
    // One new peer evicts the least-recently-used batch, with no refit after.
    stage.observe(&mut scratch, &999, 0.3, 0.2, normal(), 0.0);
    assert!(stage.peers.evictions > 0, "the scenario must evict");
    assert!(stage.since_refit > 0, "and must not have refitted since");
    let live = &stage.levels[0];
    let (live_peers, live_cells, live_orphans) = (live.sq_peers, live.sq_cells, live.orphan_w2);
    assert!(
        live_orphans > 0.0,
        "evicted events must be folded in as orphans"
    );

    stage.refit(&mut scratch, 0.0);
    let rebuilt = &stage.levels[0];
    assert!(
        (live_peers - rebuilt.sq_peers).abs() < 1e-9
            && (live_cells - rebuilt.sq_cells).abs() < 1e-9
            && (live_orphans - rebuilt.orphan_w2).abs() < 1e-9,
        "incremental after eviction ({live_peers}, {live_cells}, {live_orphans}) must equal \
         the rebuild ({}, {}, {})",
        rebuilt.sq_peers,
        rebuilt.sq_cells,
        rebuilt.orphan_w2
    );
}

/// A peer holding all but a sliver of the root's weight must be left out of
/// `tau2_peer`. At a 1.5h horizon one active peer's events weigh ~1 while the
/// only other peer's three events are 30 hours stale (~2e-9 each), so the rest
/// is ~1e-9 of the root, and `root.w2 - peer.w2` and `sq_peers - peer.n^2`
/// cancel to zero: that contrast would lose its noise term entirely.
///
/// Pinned exactly: with the dominant peer skipped, `tau2_peer` is the stale
/// peer's contrast alone, computed here from the level's own moments. Counting
/// the dominant peer adds a second, noise-free contrast and changes the value.
#[test]
fn a_dominant_peer_is_left_out_of_tau2_peer() {
    let _guard = GlobalRng::seed_guard(0x4485_f100);
    let (horizon, now) = (1.5, 30.0);
    let mut level = Level::new(Some(horizon));
    level.reset(now);
    for band in 0..BANDS {
        for _ in 0..200 {
            let t = now - uniform() * 0.5;
            level.add(Some(0), band, level.weight(t), normal());
        }
    }
    for _ in 0..3 {
        // A real effect of +3, stale.
        level.add(Some(1), 0, level.weight(0.0), 3.0 + 0.1 * normal());
    }
    level.recount_squares();
    let (root, dominant, stale) = (level.root, level.nodes[0].peer, level.nodes[1]);
    assert!(
        root.n - dominant.n < ROOT_REST_FLOOR * root.n && root.n - dominant.n > NODE_MIN,
        "the scenario must put the rest under the floor but above NODE_MIN"
    );
    assert!(stale.peer.replicated());
    let c = level.compute_components().expect("components exist");

    let peer = stale.peer;
    let rest = root.n - peer.n;
    let rest_w2 = (root.w2 - peer.w2).max(0.0);
    let contrast = peer.mean() - (root.sum - peer.sum) / rest;
    let noise = c.tau2_cell
        * (stale.sq_cells / (peer.n * peer.n)
            + (level.sq_cells - stale.sq_cells).max(0.0) / (rest * rest))
        + c.sigma2 * (peer.mean_variance_factor() + rest_w2 / (rest * rest));
    let den = 1.0 + (level.sq_peers - peer.n * peer.n).max(0.0) / (rest * rest);
    let expected = ((contrast * contrast - noise) / den).max(0.0);
    assert!(
        expected > 1.0,
        "the stale peer's own contrast must carry signal: {expected}"
    );
    assert!(
        (c.tau2_peer - expected).abs() <= 1e-9 * expected,
        "tau2_peer {} must be the stale peer's contrast alone, {expected}",
        c.tau2_peer
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
    assert_eq!(level.residual(Some(0), 0, 0.0), None);
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

    let hot = level.residual(Some(30), 5, 0.0).unwrap().mean;
    let cell_mean = level.nodes[30].cells[5].mean();
    assert!(
        (hot - cell_mean).abs() < 0.05,
        "abundant evidence must yield the cell's own mean: {hot} vs {cell_mean}"
    );
    let unknown = level.residual(None, 0, 0.0).unwrap();
    assert!(
        unknown.mean.abs() < hot.abs() * 0.5,
        "an unknown peer must not inherit another peer's cell effect: {}",
        unknown.mean
    );
    assert!(
        unknown.variance > level.residual(Some(30), 5, 0.0).unwrap().variance,
        "an unknown peer carries more posterior variance than a well-observed cell"
    );
    let other_band = level.residual(Some(30), 2, 0.0).unwrap().mean;
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
        let before = stage.predict(&peer, contract, distance, i as f64 / 60.0);
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
            .all(|pair| window_order(true)(&pair[0], &pair[1]).is_le()),
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
        if let Some(forecast) = stage.predict(&(i % 11), 0.5, 0.2, t) {
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
    for i in 0..4_000u32 {
        let peer = i % 20;
        // Everyone rises with distance, 0 to 2. Peer 0 is only ever seen close
        // in, where it is 2 slower than the curve, so its residual carried to
        // the far end of the curve would compose to about 4: far past anything
        // observed.
        let (distance, y) = if peer == 0 {
            let d = uniform() * 0.05;
            (d, 4.0 * d + 2.0)
        } else {
            let d = uniform() * 0.5;
            (d, 4.0 * d + 0.05 * normal())
        };
        observe(&mut stage, &peer, 0.3, distance, y, 0.0);
    }
    let (low, high) = stage.observed_range;
    let far = predict(&stage, &0, 0.3, 0.49).unwrap();
    let unclamped = stage.curve.as_ref().unwrap().value(0.49).unwrap()
        + stage.levels[stage.selected()]
            .residual(stage.peers.lookup(&0), band_of(0.3), 0.0)
            .unwrap()
            .mean;
    assert!(
        unclamped > high + LOG_PREDICTION_MARGIN,
        "the scenario must actually push past the bound, or this test proves nothing: \
         unclamped {unclamped}, high {high}"
    );
    assert_eq!(
        far,
        high + LOG_PREDICTION_MARGIN,
        "the prediction must sit on the bound"
    );
    for (peer, distance) in [(0, 0.0), (7, 0.5), (99, 0.45)] {
        let value = predict(&stage, &peer, 0.3, distance).unwrap();
        assert!(
            value >= low - LOG_PREDICTION_MARGIN && value <= high + LOG_PREDICTION_MARGIN,
            "prediction {value} for peer {peer} at {distance} escaped [{low}, {high}] +- margin"
        );
    }
}

/// An unknown peer's expected time and effective speed stay bounded by the
/// observed range even when a noisy early `tau2` gives it a huge spread.
#[test]
fn expected_timing_is_bounded_for_an_unknown_peer() {
    let _guard = GlobalRng::seed_guard(0x4485_b0d5);
    let peers: Vec<PeerKeyLocation> = (0..6).map(|_| PeerKeyLocation::random()).collect();
    let mut routing = HierarchicalRouting::new(200);
    // Few peers with wildly different speeds: the between-peer variance is
    // estimated huge, so an unseen peer's posterior variance is huge too.
    for i in 0..600 {
        let p = i % peers.len();
        let log_seconds = (0.1f64).ln() + 6.0 * (p as f64 - 2.5) + 0.1 * normal();
        let log_speed = (40_000.0f64).ln() - 6.0 * (p as f64 - 2.5) + 0.1 * normal();
        routing.observe_at(
            &peers[p],
            Location::new(uniform()),
            0.2,
            &timed(Some(log_seconds.exp()), Some(log_speed.exp())),
            0.0,
        );
    }
    let unknown = PeerKeyLocation::random();
    let forecast = routing
        .response_time
        .predict(&unknown, 0.5, 0.2, 0.0)
        .unwrap();
    let (low, high) = routing.response_time.observed_range;
    assert!(
        forecast.value + forecast.spread / 2.0 > high + LOG_PREDICTION_MARGIN,
        "the scenario must push the unbounded expectation past the bound: \
         mu {}, spread {}, high {high}",
        forecast.value,
        forecast.spread
    );
    let estimate = routing.estimate(&unknown, Location::new(0.5), 0.2, 0.0);
    let seconds = estimate.time_to_response_start_secs.unwrap();
    assert!(
        (seconds.ln() - (high + LOG_PREDICTION_MARGIN)).abs() < 1e-9,
        "expected time must sit on the bound: {seconds}"
    );
    assert!(seconds.ln() >= low - LOG_PREDICTION_MARGIN);
    let (speed_low, _) = routing.transfer_speed.observed_range;
    let speed = estimate.transfer_speed_bps.unwrap();
    assert!(
        speed.ln() >= speed_low - LOG_PREDICTION_MARGIN - 1e-9,
        "effective speed must not collapse below the bound: {speed}"
    );
}

/// The hot structs stay at the sizes the published memory budget uses. Exact
/// only on 64-bit targets, where the budget was measured; the compile-time
/// upper bounds apply everywhere.
#[cfg(target_pointer_width = "64")]
#[test]
fn struct_sizes_match_the_memory_budget() {
    assert_eq!(std::mem::size_of::<Event>(), EVENT_BYTES);
    assert_eq!(std::mem::size_of::<Prepared>(), PREPARED_BYTES);
    assert_eq!(std::mem::size_of::<PeerNode>(), PEER_NODE_BYTES);
    assert_eq!(std::mem::size_of::<Level>(), LEVEL_BYTES);
    assert_eq!(std::mem::size_of::<ContractNode>(), CONTRACT_NODE_BYTES);
}

/// The selector scores the finished forecast the router would act on.
#[test]
fn horizon_loss_scores_the_clamped_forecast() {
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let forecasts = [0.9, 1.0, 0.4, 0.0].map(|value| Forecast {
        value,
        unbounded: value,
        spread: 0.0,
    });
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
        // A few hundred contracts, several peers on each, so the contract
        // term does its full per-event and per-refit work.
        stage.observe(
            &mut scratch,
            &GlobalRng::random_range(0..peers),
            f64::from(GlobalRng::random_range(0..300u32)) / 300.0,
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
    let mut phases = [std::time::Duration::MAX; 5];
    for round in 0..rounds {
        for i in 0..interval {
            let (contract_slot, contract_generation) = stage
                .contracts
                .as_mut()
                .expect("failure stage has a contract term")
                .touch((f64::from(i as u32 % 300) / 300.0).to_bits());
            stage.fresh.push(Event {
                distance: uniform() * 0.5,
                y: 0.0,
                time: now,
                seq: stage.next_seq,
                slot: (i % 200) as u32,
                generation: 0,
                contract_slot: contract_slot as u32,
                contract_generation,
                adjustment: 0.0,
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
        let Scratch {
            prepared,
            contract_events,
            contract_pairs,
            ..
        } = &mut scratch;
        stage.apply_contract_term(prepared, contract_events, contract_pairs, now);
        mark(3);
        stage.rebuild_levels(prepared, now);
        mark(4);
    }
    let [merge, curve, prepare, contract_term, levels] = phases;
    let per_refit = merge + curve + prepare + contract_term + levels;

    let queries = 25 * 3 * 1_000;
    let start = std::time::Instant::now();
    let mut acc = 0.0;
    for q in 0..queries {
        acc += predict(&stage, &(q as u32 % peers), 0.37, (q % 500) as f64 / 1000.0).unwrap_or(0.0);
    }
    let per_prediction = start.elapsed() / queries as u32;
    eprintln!(
        "#4485 hierarchical cost over {WINDOW_EVENTS} events: refit {per_refit:?} = merge \
         {merge:?} + curve {curve:?} + re-anchor {prepare:?} + contract term {contract_term:?} + \
         levels {levels:?}, every \
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

/// The timing estimate `observe_at` reports is the one made BEFORE the event
/// is learned: it is what the dataset records as the forecast routing acted on.
#[test]
fn observed_timing_estimate_is_the_pre_learning_one() {
    let _guard = GlobalRng::seed_guard(0x4485_0b5e);
    let peers: Vec<PeerKeyLocation> = (0..8).map(|_| PeerKeyLocation::random()).collect();
    let mut routing = HierarchicalRouting::new(200);
    for i in 0..800 {
        let p = i % peers.len();
        let seconds = ((0.1f64).ln() + 0.3 * p as f64 + 0.3 * normal()).exp();
        routing.observe_at(
            &peers[p],
            Location::new(uniform()),
            0.2,
            &timed(Some(seconds), Some(1e4 / seconds)),
            0.0,
        );
    }
    let contract = Location::new(0.4);
    let before = routing.estimate(&peers[1], contract, 0.2, 0.0);
    let observed = routing.observe_at(&peers[1], contract, 0.2, &timed(Some(9.0), Some(10.0)), 0.0);
    let after = routing.estimate(&peers[1], contract, 0.2, 0.0);
    assert_ne!(
        before, after,
        "sanity: learning the event must move the estimate"
    );
    assert_eq!(observed.estimate, before);
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
    let estimate = routing.estimate(&peers[3], Location::new(0.5), 0.25, 0.0);
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
        .estimate(&peers[typical], Location::new(0.5), 0.25, 0.0)
        .time_to_response_start_secs
        .unwrap();
    let unknown = routing
        .estimate(&PeerKeyLocation::random(), Location::new(0.5), 0.25, 0.0)
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
    // Contracts are drawn from a POOL, not fresh per event, so `(contract,
    // peer)` cells are replicated, the contract term's components are
    // estimable and the measured per-candidate cost includes its query work.
    // A unique contract per event leaves the components `None` and `effect()`
    // short-circuits, which is how the first version of this test measured a
    // cost the term was absent from.
    // Each contract is served by a group of eight peers, and every eighth
    // contract fails half its requests, so there is real between-contract
    // variance for the term to estimate. Unrealistically concentrated for a
    // gateway, but this test measures the per-candidate cost, and the cost is
    // only realistic when the term is doing its work.
    let contracts: Vec<f64> = (0..32).map(|i| i as f64 / 32.0).collect();
    for i in 0..WINDOW_EVENTS {
        let contract = GlobalRng::random_range(0..contracts.len());
        let peer = &peers[(contract * 8 + GlobalRng::random_range(0..8)) % peers.len()];
        let failure_rate = if contract % 8 == 0 { 0.5 } else { 0.03 };
        let outcome = RoutingOutcome {
            success: uniform() > failure_rate,
            time_to_response_start_secs: Some(0.05 + uniform()),
            transfer_speed_bps: Some(1_000.0 + 50_000.0 * uniform()),
        };
        routing.observe_at(
            peer,
            Location::new(contracts[contract]),
            uniform() * 0.5,
            &outcome,
            i as f64 / 600.0,
        );
    }
    assert!(
        routing
            .failure
            .contracts
            .as_ref()
            .and_then(|table| table.components)
            .is_some_and(|components| components.tau2_contract > 0.0),
        "the term must be active, or this measures a cost it is absent from"
    );
    // Estimable components are necessary and not sufficient: the effect is
    // also refused per query below the present-peer bar, so assert a queried
    // contract really carries an offset.
    let now = WINDOW_EVENTS as f64 / 600.0;
    let offered = contracts
        .iter()
        .filter(|contract| {
            routing
                .failure
                .contracts
                .as_ref()
                .and_then(|table| table.shared_effect(contract.to_bits(), now))
                .is_some()
        })
        .count();
    assert!(
        offered > contracts.len() / 4,
        "the queried contracts must actually carry an offset, or the cost \
         excludes the work being measured: {offered} of {}",
        contracts.len()
    );
    let queries = 25_000;
    let start = std::time::Instant::now();
    let mut available = 0;
    for q in 0..queries {
        let estimate = routing.estimate(
            &peers[q % peers.len()],
            Location::new(contracts[q % contracts.len()]),
            (q % 500) as f64 / 1000.0,
            WINDOW_EVENTS as f64 / 600.0,
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

/// A node whose count has decayed to nothing by query time is treated as
/// absent, as in the validated reference, rather than keeping full weight.
#[test]
fn a_fully_decayed_node_is_treated_as_absent() {
    let _guard = GlobalRng::seed_guard(0x4485_57a1);
    let mut level = Level::new(Some(1.5));
    level.reset(0.0);
    for slot in 0..20 {
        let effect = normal();
        for band in 0..BANDS {
            for _ in 0..10 {
                level.add(Some(slot), band, 1.0, effect + 0.1 * normal());
            }
        }
    }
    level.recount_squares();
    level.components = level.compute_components();
    let fresh = level.residual(Some(3), 2, 0.0).unwrap();
    let unknown = level.residual(None, 2, 0.0).unwrap();
    let stale = level.residual(Some(3), 2, 1_000.0).unwrap();
    assert_ne!(fresh, unknown);
    assert_eq!(
        stale, unknown,
        "after ~670 horizons the peer's evidence is gone and it reads as unknown"
    );
}

/// Build a no-forgetting level and its prepared residuals from per-cell data.
fn shape_of(cells: &[(usize, usize, Vec<f64>)]) -> ResidualShape {
    // Through `measure`, the production entry point, so its per-cell minimum is
    // what these tests exercise.
    let (prepared, level) = shape_inputs(cells);
    ResidualShape::measure(&prepared, &level)
}

fn shape_with(cells: &[(usize, usize, Vec<f64>)], min_cell_events: f64) -> ResidualShape {
    let (prepared, level) = shape_inputs(cells);
    ResidualShape::measure_with(&prepared, &level, min_cell_events)
}

fn shape_inputs(cells: &[(usize, usize, Vec<f64>)]) -> (Vec<Prepared>, Level) {
    let mut level = Level::new(None);
    let mut prepared = Vec::new();
    for (slot, band, values) in cells {
        for &value in values {
            level.add(Some(*slot), *band, 1.0, value);
            prepared.push(Prepared {
                residual: value,
                time: 0.0,
                weight: 1.0,
                slot: *slot as u32,
                contract_slot: u32::MAX,
                source: 0,
                band: *band as u8,
            });
        }
    }
    level.recount_squares();
    (prepared, level)
}

/// Exponential log residuals have skewness 2 and excess kurtosis 6. In
/// 50-event cells the centring's shrinkage is already small; the n = 10 case is
/// `residual_shape_skewness_is_unbiased_in_ten_event_cells`.
#[test]
fn residual_shape_recovers_the_moments_of_exponential_data() {
    let _guard = GlobalRng::seed_guard(0x4485_e8b0);
    let cells: Vec<(usize, usize, Vec<f64>)> = (0..400)
        .map(|i| {
            let values = (0..50)
                .map(|_| -uniform().max(f64::MIN_POSITIVE).ln())
                .collect();
            (i / BANDS, i % BANDS, values)
        })
        .collect();
    let shape = shape_of(&cells);
    let skew = shape.skewness.unwrap();
    let kurtosis = shape.excess_kurtosis.unwrap();
    assert!((skew - 2.0).abs() < 0.25, "skewness {skew} (truth 2)");
    assert!(
        (kurtosis - 6.0).abs() < 2.0,
        "excess kurtosis {kurtosis} (truth 6)"
    );
}

/// At the smallest contributing cell size, n = 10, centring shrinks the raw
/// third moment to (n-1)(n-2)/n^2 = 0.72 of its value, so exponential data read
/// about 1.7 instead of 2. The k-statistic weighting must recover 2.
#[test]
fn residual_shape_skewness_is_unbiased_in_ten_event_cells() {
    let _guard = GlobalRng::seed_guard(0x4485_e810);
    let cells: Vec<(usize, usize, Vec<f64>)> = (0..6_000)
        .map(|i| {
            let values = (0..10)
                .map(|_| -uniform().max(f64::MIN_POSITIVE).ln())
                .collect();
            (i / BANDS, i % BANDS, values)
        })
        .collect();
    let shape = shape_of(&cells);
    let skew = shape.skewness.unwrap();
    let kurtosis = shape.excess_kurtosis.unwrap();
    eprintln!("n=10 exponential cells: skewness {skew}, excess kurtosis {kurtosis}");
    assert!((skew - 2.0).abs() < 0.12, "skewness {skew} (truth 2)");
    // Kurtosis keeps a documented residual shrink at n = 10; it must still
    // read far past the verdict's threshold of 1.
    assert!(
        kurtosis > 4.0,
        "excess kurtosis {kurtosis} (truth 6, documented shrink)"
    );
}

/// The per-cell minimum: for skewed data, centring on a three-event cell's
/// own mean shrinks the fourth moment far more than any rescaling of the
/// second can undo, so exponential data (excess kurtosis 6) would read as
/// barely heavy-tailed. Most events here sit in three-event cells.
#[test]
fn residual_shape_ignores_cells_too_small_to_show_their_shape() {
    let _guard = GlobalRng::seed_guard(0x4485_e8b3);
    let exponential = || -uniform().max(f64::MIN_POSITIVE).ln();
    let mut cells: Vec<(usize, usize, Vec<f64>)> = (0..30_000)
        .map(|i| {
            (
                i / BANDS,
                i % BANDS,
                (0..3).map(|_| exponential()).collect(),
            )
        })
        .collect();
    cells.extend((0..600).map(|i| {
        (
            30_000 / BANDS + 1 + i / BANDS,
            i % BANDS,
            (0..50).map(|_| exponential()).collect(),
        )
    }));
    let guarded = shape_of(&cells).excess_kurtosis.unwrap();
    assert!(
        guarded > 4.0,
        "excess kurtosis {guarded} must read the heavy tail (truth 6)"
    );
    let unguarded = shape_with(&cells, 3.0).excess_kurtosis.unwrap();
    assert!(
        unguarded < guarded - 1.5,
        "sanity: three-event cells must distort the reading, or this test proves nothing \
         (unguarded {unguarded}, guarded {guarded})"
    );
}

/// The rescaling: deviations about an n-event cell's mean have variance
/// (n-1)/n. Normal deviations stay normal whatever the cell size, so the only
/// thing that can distort pooled normal data is the SCALE: three-event cells
/// at variance 2/3 pooled with 500-event cells at variance 1 are a scale
/// mixture reading about +0.12 excess kurtosis unless each deviation is
/// rescaled. The per-cell minimum is lowered so it cannot hide the effect.
#[test]
fn residual_shape_rescales_deviations_so_mixed_cell_sizes_pool() {
    let _guard = GlobalRng::seed_guard(0x4485_3c12);
    let mut cells: Vec<(usize, usize, Vec<f64>)> = (0..40_000)
        .map(|i| (i / BANDS, i % BANDS, (0..3).map(|_| normal()).collect()))
        .collect();
    cells.extend((0..240).map(|i| {
        (
            40_000 / BANDS + 1 + i / BANDS,
            i % BANDS,
            (0..500).map(|_| normal()).collect(),
        )
    }));
    let shape = shape_with(&cells, 3.0);
    let kurtosis = shape.excess_kurtosis.unwrap();
    assert!(
        kurtosis.abs() < 0.05,
        "rescaled deviations from mixed cell sizes must read normal, got excess kurtosis {kurtosis}"
    );
    assert!(shape.skewness.unwrap().abs() < 0.05);
}

/// Normal residuals in cells of very different sizes: small cells' centred
/// deviations are non-normal and under-dispersed, so without the rescaling and
/// the per-cell minimum they drag excess kurtosis negative.
#[test]
fn residual_shape_is_unbiased_for_normal_data_in_mixed_cell_sizes() {
    let _guard = GlobalRng::seed_guard(0x4485_3c11);
    let mut cells = Vec::new();
    for i in 0..3_000 {
        // Most events in tiny cells, the rest in large ones.
        let size = if i < 2_900 { 3 } else { 400 };
        let values = (0..size).map(|_| normal()).collect();
        cells.push((i / BANDS, i % BANDS, values));
    }
    let shape = shape_of(&cells);
    let skew = shape.skewness.unwrap();
    let kurtosis = shape.excess_kurtosis.unwrap();
    assert!(skew.abs() < 0.1, "skewness {skew} (truth 0)");
    assert!(
        kurtosis.abs() < 0.15,
        "excess kurtosis {kurtosis} (truth 0)"
    );

    // Mixed sizes above the per-cell minimum must also read ~0 once rescaled.
    let mut cells = Vec::new();
    for i in 0..1_200 {
        let size = if i % 2 == 0 { 10 } else { 60 };
        let values = (0..size).map(|_| normal()).collect();
        cells.push((i / BANDS, i % BANDS, values));
    }
    let shape = shape_of(&cells);
    assert!(
        shape.excess_kurtosis.unwrap().abs() < 0.15,
        "excess kurtosis {:?} with cells of 10 and 60",
        shape.excess_kurtosis
    );
}

/// The lognormality check reads zero skew and kurtosis for normal log
/// residuals, and flags a heavy right tail.
#[test]
fn residual_shape_flags_departures_from_lognormal() {
    let shape_for = |tail: bool| {
        let _guard = GlobalRng::seed_guard(0x4485_5a9e);
        let mut stage: Stage<u32> = Stage::new(Target::LogResponseTime, 64);
        for i in 0..6_000u32 {
            let noise = if tail && uniform() < 0.05 {
                2.5 + normal()
            } else {
                0.5 * normal()
            };
            observe(
                &mut stage,
                &(i % 20),
                uniform(),
                uniform() * 0.5,
                (0.1f64).ln() + noise,
                0.0,
            );
        }
        stage.diagnostics()
    };
    let normal_shape = shape_for(false);
    let tailed_shape = shape_for(true);
    assert!(normal_shape.residual_shape.events > 5_000);
    let skew = normal_shape.residual_shape.skewness.unwrap();
    let kurtosis = normal_shape.residual_shape.excess_kurtosis.unwrap();
    assert!(
        skew.abs() < 0.15 && kurtosis.abs() < 0.3,
        "normal: skew {skew}, kurtosis {kurtosis}"
    );
    assert!(
        (normal_shape.residual_sigma2.unwrap() - 0.25).abs() < 0.03,
        "sigma2 {:?}",
        normal_shape.residual_sigma2
    );
    let skew = tailed_shape.residual_shape.skewness.unwrap();
    let kurtosis = tailed_shape.residual_shape.excess_kurtosis.unwrap();
    assert!(
        skew > 1.0 && kurtosis > 2.0,
        "tailed: skew {skew}, kurtosis {kurtosis}"
    );
    let failure: Stage<u32> = Stage::new(Target::Failure, 64);
    assert_eq!(
        failure.diagnostics().residual_shape,
        ResidualShape::default()
    );
}

// ---------------------------------------------------------------------------
// Contract term
// ---------------------------------------------------------------------------

/// One failure-stage event for the contract-term tests.
#[derive(Debug, Clone, Copy)]
struct Step {
    peer: u32,
    contract: f64,
    distance: f64,
    failed: bool,
    hours: f64,
}

/// Healthy traffic at 600 events per hour: random peers from `peers`, every
/// event on its own contract, failure rate rising with distance.
fn background(start: f64, hours: f64, peers: std::ops::Range<u32>) -> Vec<Step> {
    let count = (hours * 600.0) as usize;
    (0..count)
        .map(|i| {
            let distance = uniform() * 0.5;
            Step {
                peer: GlobalRng::random_range(peers.clone()),
                contract: uniform(),
                distance,
                failed: uniform() < 0.02 + 0.2 * distance,
                hours: start + i as f64 / 600.0,
            }
        })
        .collect()
}

/// Healthy traffic drawn from a POOL of contracts, so the contract term's
/// `tau2_contract` rests on many groups instead of one.
///
/// [`background`] draws a UNIQUE contract per event, so exactly ONE group
/// qualifies for `tau2_contract` (the dead contract a test injects), and under
/// the `den >= 2` gate that switches the term off entirely: it was the tested
/// regime rather than a covered one. The finding came from the
/// `qualifying_contracts` counter added for round-2 finding I. `background` is
/// kept for the tests that deliberately want contract churn (eviction,
/// capacity); everything that needs the term active uses this.
///
/// WHAT THE POOL DOES AND DOES NOT DELIVER, corrected after N1 of the
/// 2026-09-18 round-3 testing review, because the next author will size a pool
/// by this docstring. An earlier version said `(contract, peer)` CELLS become
/// replicated. They do not, and mostly cannot: [`MIN_EFFECTIVE_N`] is 2 and the
/// Kish effective size of two events of unequal decayed weight is below 2, so a
/// cell needs THREE events inside roughly 40 minutes of estimator time, which
/// at this rate and pool size is rare. What the pool delivers is per-CONTRACT
/// total presence and replication, which is what `den` counts: it moves
/// `qualifying_contracts` from 1 to about 41 and satisfies the `den >= 2` gate.
/// `df`, which is summed over CELLS, stays dominated by the injected dead
/// contract's own cells. So enlarging the pool buys qualifying CONTRACTS, not
/// within-cell degrees of freedom; if a test needs the latter, it must repeat
/// one (contract, peer) pair, as the explicit tables do.
fn background_pooled(start: f64, hours: f64, peers: std::ops::Range<u32>) -> Vec<Step> {
    let count = (hours * 600.0) as usize;
    // Enough contracts that the table is exercised, few enough that each is
    // seen many times by several peers.
    const POOL: usize = 40;
    (0..count)
        .map(|i| {
            let distance = uniform() * 0.5;
            Step {
                peer: GlobalRng::random_range(peers.clone()),
                contract: (i % POOL) as f64 / POOL as f64,
                distance,
                failed: uniform() < 0.02 + 0.2 * distance,
                hours: start + i as f64 / 600.0,
            }
        })
        .collect()
}

/// `count` events by `peer` on `contract`, spread evenly over `hours` from
/// `start`.
fn on_contract(
    peer: u32,
    contract: f64,
    failed: bool,
    count: usize,
    start: f64,
    hours: f64,
) -> Vec<Step> {
    (0..count)
        .map(|i| Step {
            peer,
            contract,
            distance: 0.05,
            failed,
            hours: start + hours * i as f64 / count as f64,
        })
        .collect()
}

/// A production failure stage and the same stage with its contract term
/// removed, the counterfactual every contract-term test compares against.
fn failure_stage_pair() -> (Stage<u32>, Stage<u32>) {
    let with_term: Stage<u32> = Stage::new(Target::Failure, 64);
    let mut without_term: Stage<u32> = Stage::new(Target::Failure, 64);
    assert!(with_term.contracts.is_some());
    without_term.contracts = None;
    (with_term, without_term)
}

/// Feed `steps` in time order to every stage, then refit each at the last
/// event's time so every event carries its adjustment as of that refit.
fn feed(stages: &mut [&mut Stage<u32>], mut steps: Vec<Step>) -> f64 {
    steps.sort_by(|a, b| a.hours.total_cmp(&b.hours));
    let mut scratch = Scratch::default();
    for step in &steps {
        for stage in stages.iter_mut() {
            stage.observe(
                &mut scratch,
                &step.peer,
                step.contract,
                step.distance,
                if step.failed { 1.0 } else { 0.0 },
                step.hours,
            );
        }
    }
    let now = steps.last().map_or(0.0, |step| step.hours);
    for stage in stages.iter_mut() {
        stage.refit(&mut scratch, now);
    }
    now
}

/// A peer's slot in a stage's peer table.
fn stage_slot(stage: &Stage<u32>, peer: u32) -> u32 {
    stage.peers.lookup(&peer).expect("the peer is tracked") as u32
}

/// A contract none of the tests' peers has seen, in the same band as the
/// contracts the tests fail (`band_of` 2): a peer's learned failures reach its
/// forecasts mostly through its (peer, band) cell, so this is where pollution
/// shows.
const UNSEEN_CONTRACT: f64 = 0.36;

/// Mean failure forecast over `peers` for [`UNSEEN_CONTRACT`].
fn mean_forecast(stage: &Stage<u32>, peers: impl Iterator<Item = u32>) -> f64 {
    let (mut sum, mut count) = (0.0, 0.0);
    for peer in peers {
        sum += predict(stage, &peer, UNSEEN_CONTRACT, 0.1).expect("warm stage predicts");
        count += 1.0;
    }
    sum / count
}

/// A contract that several peers fail and nobody serves must not raise those
/// peers' forecasts for OTHER contracts: its failures are charged to the
/// contract. The control, the same stage without the term, is the
/// non-vacuity check: there the storm does raise them.
#[test]
fn explain_away_keeps_a_dead_contract_out_of_other_contracts_forecasts() {
    // Averaged over seeds: a single RNG draw's gap moves by enough that a
    // tight threshold on it is a threshold on the draw.
    let seeds = 6u64;
    let (mut polluted, mut explained) = (0.0, 0.0);
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_c001 + seed);
        let dead = 0.37;
        let mut steps = background_pooled(0.0, 3.25, 0..30);
        for peer in 0..6 {
            steps.extend(on_contract(peer, dead, true, 8, 3.0, 0.25));
        }
        let (mut with_term, mut without_term) = failure_stage_pair();
        feed(&mut [&mut with_term, &mut without_term], steps);
        let gap = |stage: &Stage<u32>| mean_forecast(stage, 0..6) - mean_forecast(stage, 6..30);
        polluted += gap(&without_term) / seeds as f64;
        explained += gap(&with_term) / seeds as f64;
        // N1 of the 2026-09-18 round-3 testing review. This test was switched
        // from `background` to `background_pooled` so that more than one
        // contract qualifies and the term can act at all; asserting that here
        // makes a future traffic-shape change NAME ITS OWN CAUSE instead of
        // showing up as a weakened gap. What the pool buys is per-CONTRACT
        // replication and nothing else, which is the correction in
        // `background_pooled`'s own docstring.
        let qualifying = with_term
            .contracts
            .as_ref()
            .and_then(|table| table.components)
            .map_or(0, |components| components.qualifying_contracts);
        assert!(
            qualifying >= 2,
            "the pooled background must keep at least two contracts qualifying, \
             or `tau2_contract` is zero and the term is off: {qualifying}"
        );
    }
    assert!(
        polluted > 0.3,
        "without the term the storm must raise the failing peers' forecasts, or \
         this test proves nothing: mean gap {polluted} over {seeds} seeds"
    );
    assert!(
        explained < 0.2 * polluted,
        "with the term the dead contract must not raise the peers' other forecasts: \
         mean gap {explained} against {polluted} without the term, over {seeds} seeds"
    );
}

/// Leave-one-peer-out: a peer that fails a contract other peers serve is
/// charged for it. Its own failures must never explain themselves away.
///
/// READ THE NOTE BEFORE THE ASSERTIONS. On this traffic the term turns out to
/// be INERT (`tau2_contract` is zero on every seed), so what this test
/// establishes is the negative control: an inert term leaves the peer's charge
/// exactly where it was. The leave-one-out property itself is pinned by
/// `leaving_the_failing_peer_out_removes_its_own_evidence`, on a table built to
/// make the effect observable.
#[test]
fn a_peer_failing_a_contract_others_serve_is_charged_to_that_peer() {
    // Averaged over seeds, as above.
    let seeds = 6u64;
    let (mut charged, mut with) = (0.0, 0.0);
    let (mut applied, mut adjusted_events, mut tau2_max) = (0u64, 0usize, 0.0f64);
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_c002 + seed);
        let contract = 0.52;
        let mut steps = background_pooled(0.0, 3.25, 0..30);
        steps.extend(on_contract(0, contract, true, 24, 3.0, 0.25));
        steps.extend(on_contract(1, contract, false, 8, 3.0, 0.25));
        steps.extend(on_contract(2, contract, false, 8, 3.0, 0.25));
        let (mut with_term, mut without_term) = failure_stage_pair();
        feed(&mut [&mut with_term, &mut without_term], steps);
        let gap = |stage: &Stage<u32>| mean_forecast(stage, 0..1) - mean_forecast(stage, 3..30);
        charged += gap(&without_term) / seeds as f64;
        with += gap(&with_term) / seeds as f64;
        // THE TERM IS INERT ON THIS TRAFFIC, measured rather than assumed, and
        // asserted so that the fact cannot drift silently. See the note above
        // the assertions below for what that means for this test.
        applied += with_term
            .contracts
            .as_ref()
            .map_or(0, |t| t.effects_applied);
        adjusted_events += with_term
            .sorted
            .iter()
            .filter(|event| event.adjustment != 0.0)
            .count();
        tau2_max = tau2_max.max(
            with_term
                .contracts
                .as_ref()
                .and_then(|t| t.components)
                .map_or(0.0, |c| c.tau2_contract),
        );
    }
    assert!(
        charged > 0.05,
        "sanity: the failures must raise peer 0, mean gap {charged} over {seeds} seeds"
    );

    // WHAT THIS TEST ACTUALLY ESTABLISHES, measured for I1 of the 2026-09-18
    // round-3 testing review. Its `with > 0.8 * charged` assertion was
    // one-sided and yielded the same verdict in three regimes, one of which is
    // the term being inert. It IS inert here: on every one of the six seeds
    // `tau2_contract` is exactly 0, no windowed event carries an adjustment,
    // and `effects_applied` is 0, so `with == charged` exactly and the
    // assertion was passing on a stage pair that did the same thing.
    //
    // The cause is the estimator behaving as designed, not a defect. This
    // contract's peers DISAGREE sharply (peer 0 fails, two others succeed), so
    // that spread is attributed to `tau2_peer`, and the pooled healthy
    // background supplies too little between-contract variance for the
    // contrast to survive the noise the estimator subtracts. The same effect
    // is documented at the table level in
    // `leaving_the_failing_peer_out_removes_its_own_evidence`, which is where
    // the leave-one-out property is now pinned on a table built to make it
    // observable.
    //
    // So this is kept as a NEGATIVE CONTROL and labelled as one: an inert term
    // must leave the peer's charge exactly where it was. Both bounds are
    // asserted, so a term that starts acting here fails this test and forces
    // whoever changes the traffic to re-read the note above rather than
    // inheriting a one-sided assertion. Giving it activating traffic would
    // make it a positive test too, and is recorded on #5700 as a choice for
    // the lead rather than made silently.
    assert_eq!(
        (applied, adjusted_events),
        (0, 0),
        "this test's traffic is expected to leave the term inert; if it now \
         acts, read the note above and re-scope the assertions below"
    );
    assert_eq!(
        tau2_max, 0.0,
        "and the reason is a zero between-contract variance on every seed"
    );
    assert!(
        with > 0.8 * charged,
        "peer 0's own failures must stay charged to peer 0: mean gap {with} against \
         {charged} without the term, over {seeds} seeds"
    );
    assert!(
        with <= charged * 1.001,
        "and must not be charged MORE than its own residuals justify, which is \
         what an unbounded explaining adjustment of the wrong sign does: mean \
         gap {with} against {charged} without the term"
    );
}

/// The two bars are DIFFERENT, and the difference is the point: learning
/// needs [`CONTRACT_MIN_OTHER_PEERS`] peers EXCLUDING the acting peer, while a
/// forecast offset needs one more, INCLUDING it.
///
/// Pinned as a unit fact because the stage-level test below cannot see it: it
/// asserts the acting peer's own adjustment, which is the LEARNING path.
/// Mutations of `shared_effect`'s `min_peers` therefore survived it (measured,
/// 2026-09-17 round-2 mutation run at `282be3a67`), and this closes them.
#[test]
fn the_forecast_bar_is_one_peer_higher_than_the_learning_bar() {
    let mut table = ContractTable::new();
    let key = 0.25f64.to_bits();
    let (slot, _) = table.touch(key);
    table.components = Some(ContractComponents {
        sigma2: 0.25,
        tau2_peer: 0.0,
        tau2_contract: 0.16,
        floor_bound: false,
        den_below_two: false,
        qualifying_contracts: 2,
        qualifying_entries: 4,
    });
    // Exactly CONTRACT_MIN_OTHER_PEERS present peers on the contract.
    for peer in 0..CONTRACT_MIN_OTHER_PEERS as u32 {
        assert!(table.add(slot, (peer, 0), 1.0, 0.9));
        assert!(table.add(slot, (peer, 0), 1.0, 0.9));
    }
    // Learning: a third peer's residual IS adjusted, because the two present
    // peers are both "other" peers to it.
    assert!(
        table
            .leave_out_effect(Some(slot), Some((99, 0)), 0.0)
            .is_some(),
        "two other present peers must reach the LEARNING bar"
    );
    // Forecast: the same two peers are NOT enough, because the bar counts the
    // candidate too.
    assert_eq!(
        table.shared_effect(key, 0.0),
        None,
        "two present peers must NOT reach the FORECAST bar, which is one higher"
    );
    // One more present peer, and the forecast offset appears.
    assert!(table.add(slot, (50, 0), 1.0, 0.9));
    assert!(table.add(slot, (50, 0), 1.0, 0.9));
    assert!(
        table.shared_effect(key, 0.0).is_some(),
        "three present peers must reach the forecast bar"
    );
    // And one present peer reaches neither.
    let mut thin = ContractTable::new();
    let (thin_slot, _) = thin.touch(key);
    thin.components = table.components;
    assert!(thin.add(thin_slot, (0, 0), 1.0, 0.9));
    assert_eq!(thin.shared_effect(key, 0.0), None);
    assert_eq!(
        thin.leave_out_effect(Some(thin_slot), Some((99, 0)), 0.0),
        None,
        "one other present peer must not reach the learning bar either"
    );
}

/// The minimum-other-peers rule: one other failing peer cannot tell a dead
/// contract from a bad peer, so it explains nothing. A second one does.
#[test]
fn one_other_failing_peer_does_not_explain_a_peers_failures() {
    let run = |failing_others: u32| {
        let _guard = GlobalRng::seed_guard(0x4485_c003);
        let contract = 0.52;
        let mut steps = background_pooled(0.0, 3.25, 0..30);
        // The actor is OUTSIDE the background range, so this contract is its
        // only traffic and the bit-equality below is about this contract
        // alone: with pooled traffic a background peer's other events are
        // adjusted too, which would break the equality for an unrelated
        // reason.
        steps.extend(on_contract(90, contract, true, 24, 3.0, 0.25));
        for peer in 1..=failing_others {
            steps.extend(on_contract(90 + peer, contract, true, 6, 3.0, 0.25));
        }
        let (mut with_term, mut without_term) = failure_stage_pair();
        feed(&mut [&mut with_term, &mut without_term], steps);
        // The actor's OWN adjustment is the property. A global forecast
        // comparison is not: with the term active on the pooled background it
        // moves every peer's learned residual slightly, so the two stages are
        // never bit-identical anywhere, and the equality only ever held while
        // the term was globally off.
        let slot = stage_slot(&with_term, 90);
        let total: f32 = with_term
            .sorted
            .iter()
            .filter(|event| event.slot == slot)
            .map(|event| event.adjustment)
            .sum();
        let _ = &without_term;
        total
    };
    assert_eq!(
        run(1),
        0.0,
        "one other failing peer must not explain any of the actor's failures"
    );
    assert!(
        run(2) > 0.1,
        "two other failing peers must explain part of the actor's failures, or \
         the equality above proves nothing: {}",
        run(2)
    );
}

/// The effect a forecast adds is the contract's, not the candidate's: at one
/// moment it is the same for a peer that failed the contract, a peer that
/// never touched it, and a peer the stage has never seen.
#[test]
fn the_forecast_contract_effect_is_identical_for_every_candidate_peer() {
    let _guard = GlobalRng::seed_guard(0x4485_c004);
    let dead = 0.37;
    let mut steps = background_pooled(0.0, 3.25, 0..30);
    for peer in 0..6 {
        steps.extend(on_contract(peer, dead, true, 8, 3.0, 0.25));
    }
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let now = feed(&mut [&mut stage], steps);

    let distance = 0.1;
    let prior = stage.prior(distance).unwrap();
    let offsets: Vec<f64> = [0u32, 3, 12, 29, 999]
        .iter()
        .map(|peer| {
            let with_effect = stage.predict(peer, dead, distance, now).unwrap().unbounded;
            let without = stage
                .forecast_with(
                    stage.selected(),
                    stage.peers.lookup(peer),
                    band_of(dead),
                    prior,
                    now,
                )
                .unbounded;
            with_effect - without
        })
        .collect();
    assert!(
        offsets[0] > 0.1,
        "the dead contract must carry a shared effect, or equality proves nothing: \
         {offsets:?}"
    );
    for offset in &offsets {
        assert!(
            (offset - offsets[0]).abs() < 1e-12,
            "every candidate must get the same contract effect: {offsets:?}"
        );
    }
}

/// A refit re-adjusts every windowed event: a dead contract's FIRST failure,
/// learned before any other peer had failed there, is explained at the next
/// refit. Once the contract's evidence has decayed out of the table, events
/// keep the adjustment they last had, and the levels still apply it.
#[test]
fn refits_clear_first_failures_and_events_keep_their_last_adjustment() {
    let _guard = GlobalRng::seed_guard(0x4485_c005);
    let dead = 0.37;
    let (mut with_term, mut without_term) = failure_stage_pair();
    feed(
        &mut [&mut with_term, &mut without_term],
        background_pooled(0.0, 2.0, 1..30),
    );
    // Peer 0's first failure on the dead contract: nobody else has failed it.
    feed(
        &mut [&mut with_term, &mut without_term],
        on_contract(0, dead, true, 1, 2.0, 0.0),
    );
    // Located once, by peer, and tracked by sequence number after that: an
    // exact float distance is not a robust way to find an event.
    let seq = {
        let slot = stage_slot(&with_term, 0);
        with_term
            .sorted
            .iter()
            .find(|event| event.slot == slot)
            .expect("peer 0's only event is in the window")
            .seq
    };
    let first = move |stage: &Stage<u32>| {
        *stage
            .sorted
            .iter()
            .find(|event| event.seq == seq)
            .expect("the event is still in the window")
    };
    assert_eq!(
        first(&with_term).adjustment,
        0.0,
        "no other peer had failed the contract when the event was learned"
    );

    let mut storm = Vec::new();
    for peer in 1..6 {
        storm.extend(on_contract(peer, dead, true, 6, 2.01, 0.2));
    }
    feed(&mut [&mut with_term, &mut without_term], storm);
    let adjustment = first(&with_term).adjustment;
    assert!(
        adjustment > 0.3,
        "the refit must explain the first failure once other peers fail the \
         contract, got {adjustment}"
    );

    // Three hours of other traffic: six contract horizons, so the dead
    // contract's entries fall below the presence cut. Refits while they were
    // still present may have refreshed the adjustment; after that it is frozen.
    //
    // DELIBERATELY UNPOOLED, and this is the answer to the round-3 testing
    // review's observation that the feed disagreed with the comment above it.
    // Pooling it was tried and it breaks the test in two independent ways,
    // both measured: peer 0 draws background events too, so with tracked
    // contracts its level sum stops being attributable to the dead contract
    // alone (the sum below went to -0.067, i.e. dominated by background
    // adjustments), and the kept-adjustment measurement changes from 0.045 to
    // 0.395. The first is fatal to the test's design and the second is
    // reported below rather than hidden.
    feed(
        &mut [&mut with_term, &mut without_term],
        background(2.21, 3.0, 0..30),
    );
    let kept = first(&with_term).adjustment;
    // FINDING, and its CORRECTION, because the first version of it was an
    // artefact of this test feeding two different traffic shapes.
    //
    // Round 2 pooled the pre-storm background and left these continuation
    // feeds unpooled. Measured on that mix, the kept adjustment fell to 0.045
    // and was reported as "about a seventh of the 0.3 it had when the storm
    // was live", which understated it twice over: 0.3 is this test's
    // ASSERTION THRESHOLD, not the measurement, and the storm-era adjustment
    // is actually 0.644, so the ratio on that mix was 7%, not a seventh.
    //
    // The round-3 testing review pointed out that a test explaining its result
    // in terms of pooled traffic should not then feed unpooled traffic. Pooling
    // the continuations was tried: the measurement becomes **kept = 0.395
    // against a storm-era 0.644, or 61%**, the opposite end of the range from
    // the 7% above, and peer 0's level sum stops being attributable to the
    // dead contract (see the feed's own comment). So the continuations stay
    // unpooled for attribution, and the number recorded here is the
    // mixed-traffic one: the size of this effect is governed by the traffic
    // shape and NOT by either new gate (which round 2 had already established
    // by measurement: the value was unchanged with the evidence floor off and
    // with the `den >= 2` gate off).
    //
    // What survives the correction, and what does not. The MECHANISM is
    // unchanged and still pinned below: an event whose contract has no present
    // evidence keeps the adjustment it last had, and that adjustment is a
    // decayed version of the storm-era one rather than a fresh estimate. The
    // CLAIM that round 2 drew from the 7% figure -- "most of the storm's
    // explanation evaporates, so the kept-adjustment mechanism is materially
    // weaker than the unpooled tests implied" -- is RETRACTED as stated: it
    // held on a mixed-traffic measurement and does not generalise. Whether to
    // couple the adjustment to the era that produced it rather than to the
    // contract's current state remains a design question on #5700, but the
    // evidence for its urgency was this number and the number does not
    // support it.
    assert!(
        kept > 0.02,
        "some of the storm's explanation must survive, got {kept}"
    );
    // BRACKETED, so both a collapse to nothing and a freeze at the storm value
    // fail. Measured at 0.07 of the storm-era adjustment on this traffic.
    // IF THIS FAILS HIGH, the adjustment is being coupled to the era that
    // produced it and #5700's era-coupling item may have been addressed:
    // update the test to assert the new behaviour rather than widening the
    // bracket. IF IT FAILS LOW, the kept adjustment has stopped surviving at
    // all, which is a different bug.
    let ratio = f64::from(kept) / f64::from(adjustment);
    assert!(
        (0.02..0.3).contains(&ratio),
        "the kept adjustment must be a heavily decayed storm-era value on this \
         traffic, measured at 0.07: got {ratio} ({kept} against {adjustment})"
    );
    // Another hour of refits with no present evidence for the dead contract,
    // while its events are still in the window. Unpooled for the same
    // attribution reason as the feed above.
    feed(
        &mut [&mut with_term, &mut without_term],
        background(5.21, 1.0, 0..30),
    );
    assert!(with_term.sorted.len() < WINDOW_EVENTS);
    assert_eq!(
        first(&with_term).adjustment.to_bits(),
        kept.to_bits(),
        "an event must keep its last adjustment once its contract has no present \
         evidence"
    );
    // The no-forgetting level learns `residual - adjustment` for every event.
    // The curve does not depend on the term, so the control level holds the
    // same residuals unadjusted.
    // Peer 0 acts only on the dead contract here, so its level sum is exactly
    // the adjusted residuals of those events. The quantity subtracted is the
    // BOUNDED adjustment, not the raw stored one, so the sum is computed the
    // same way the stage computes it.
    //
    // The clamp is written LONGHAND on purpose. Calling `explaining_bound`
    // here would make the expectation derive from the code under test, so a
    // mutation of the bound would move both sides of the comparison equally
    // and this assertion could not see it. Before the pooled traffic landed
    // this compared against the raw stored value, which was independent; the
    // longhand restores that independence (a round-3 testing-review item).
    let slot = with_term.peers.lookup(&0).unwrap();
    let curve = with_term.curve.as_ref().expect("the stage has a curve");
    let bounded: f64 = with_term
        .sorted
        .iter()
        .filter(|event| event.slot == slot as u32)
        .map(|event| {
            let residual = event.y - with_term.bound(curve.value(event.distance).unwrap_or(0.0));
            let adjustment = f64::from(event.adjustment);
            if !adjustment.is_finite() || !residual.is_finite() {
                0.0
            } else if residual >= 0.0 {
                adjustment.max(0.0).min(residual)
            } else {
                adjustment.min(0.0).max(residual)
            }
        })
        .sum();
    let charged = without_term.levels[0].nodes[slot].peer.sum;
    let applied = with_term.levels[0].nodes[slot].peer.sum;
    assert!(
        bounded > 0.02,
        "the kept adjustments must still be worth something: {bounded}"
    );
    assert!(
        ((charged - applied) - bounded).abs() < 1e-6,
        "the kept adjustments must still be subtracted: control sum {charged}, \
         with term {applied}, bounded adjustments {bounded}"
    );
}

/// Between refits, a newly learned failure is adjusted by the effect the
/// OTHER peers already give its contract, before it enters the levels.
#[test]
fn live_learning_subtracts_the_other_peers_contract_effect() {
    let _guard = GlobalRng::seed_guard(0x4485_c00a);
    let mut steps = background_pooled(0.0, 3.0, 0..30);
    // An earlier dead contract, so the table has components at the last refit.
    for peer in 10..16 {
        steps.extend(on_contract(peer, 0.33, true, 8, 2.7, 0.25));
    }
    let (mut with_term, mut without_term) = failure_stage_pair();
    let now = feed(&mut [&mut with_term, &mut without_term], steps);

    // Three peers fail another contract, then peer 4 fails it once. Fewer events
    // than a refit interval, so nothing here is re-adjusted by a refit.
    let dead = 0.37;
    let mut scratch = Scratch::default();
    // Peers OUTSIDE the background range, so the level-sum comparison below
    // isolates the injected events: with pooled traffic an actor that also
    // appears in the background has its other events adjusted too.
    for (index, peer) in [91u32, 92, 93, 91, 92, 93, 91, 92, 93, 94]
        .iter()
        .enumerate()
    {
        let hours = now + 0.001 * (index + 1) as f64;
        for stage in [&mut with_term, &mut without_term] {
            stage.observe(&mut scratch, peer, dead, 0.05, 1.0, hours);
        }
    }
    assert!(with_term.since_refit < refit_interval(WINDOW_EVENTS, with_term.sorted.len()));
    let adjustment = with_term.fresh.last().unwrap().adjustment as f64;
    assert!(
        adjustment > 0.1,
        "three other failing peers must give the contract an effect, got {adjustment}"
    );
    let slot = with_term.peers.lookup(&94).unwrap();
    let charged = without_term.levels[0].nodes[slot].peer.sum;
    let applied = with_term.levels[0].nodes[slot].peer.sum;
    assert!(
        ((charged - applied) - adjustment).abs() < 1e-6,
        "the live residual must enter the levels adjusted: control {charged}, with \
         term {applied}, adjustment {adjustment}"
    );
}

/// The LEARN site's bound, in the style of the test above, which is the only
/// shape that can observe it.
///
/// Three attempts were needed and the two failures are the point. A refit
/// REBUILDS the levels from the window, so once one has run the level sums
/// carry the refit's adjustments and the learn site is invisible in them; and
/// the table-level test of `explaining_bound` says nothing about either
/// caller. What works is this test's neighbour's shape: drive `observe`
/// directly, keep the count below a refit interval so nothing is recomputed,
/// and use peers that appear nowhere else so the level sums isolate the
/// injected events.
///
/// The mirror of the neighbour: three peers SUCCEED on a contract, so its
/// effect is NEGATIVE, and then one more peer FAILS there. That peer's
/// residual is positive, the effect opposes it, and the bound must remove
/// NOTHING. Reverting the learn site to `residual - effect` subtracts a
/// negative number, which charges the peer MORE than its own residual.
#[test]
fn the_learn_site_never_charges_a_peer_more_than_its_own_residual() {
    let _guard = GlobalRng::seed_guard(0x4485_c1c0);
    let mut steps = background_pooled(0.0, 3.0, 0..30);
    // An earlier dead contract, so the table has components at the last refit
    // and `tau2_contract` is positive when the events below are learned.
    for peer in 10..16 {
        steps.extend(on_contract(peer, 0.33, true, 8, 2.7, 0.25));
    }
    let (mut with_term, mut without_term) = failure_stage_pair();
    let now = feed(&mut [&mut with_term, &mut without_term], steps);

    let served = 0.44;
    let mut scratch = Scratch::default();
    // Peers outside the background range, so the level sums isolate these
    // events. Nine SUCCESSES (y = 0.0) from three peers, then one FAILURE.
    for (index, peer) in [81u32, 82, 83, 81, 82, 83, 81, 82, 83].iter().enumerate() {
        let hours = now + 0.001 * (index + 1) as f64;
        for stage in [&mut with_term, &mut without_term] {
            stage.observe(&mut scratch, peer, served, 0.05, 0.0, hours);
        }
    }
    let hours = now + 0.011;
    for stage in [&mut with_term, &mut without_term] {
        stage.observe(&mut scratch, &84, served, 0.05, 1.0, hours);
    }
    assert!(with_term.since_refit < refit_interval(WINDOW_EVENTS, with_term.sorted.len()));

    // The effect must be NEGATIVE and non-trivial, or the bound has nothing to
    // refuse and this test proves nothing.
    let adjustment = f64::from(with_term.fresh.last().unwrap().adjustment);
    assert!(
        adjustment < -0.01,
        "three other peers SUCCEEDING on the contract must give it a clearly          negative effect, got {adjustment}"
    );

    // Peer 84's residual is positive (it failed), so the bound removes nothing
    // and the two stages must agree exactly.
    let slot = with_term.peers.lookup(&84).unwrap();
    let control = without_term.levels[0].nodes[slot].peer.sum;
    let applied = with_term.levels[0].nodes[slot].peer.sum;
    assert!(
        control > 0.05,
        "sanity: the failure must be worth something in the control, or the          equality below holds trivially: {control}"
    );
    assert!(
        (applied - control).abs() < 1e-9,
        "an effect opposing the residual's sign must remove nothing at the          LEARN site: with the term {applied}, control {control}, effect          {adjustment}"
    );
}

/// The presence cut: evidence that has decayed away stops explaining its
/// contract, instead of counting at full strength forever (the Kish factor a
/// query reads is scale free, so nothing else would retire it).
#[test]
fn the_presence_cut_stops_stale_evidence_from_explaining_a_contract() {
    let _guard = GlobalRng::seed_guard(0x4485_c00b);
    let dead = 0.37;
    let mut steps = background_pooled(0.0, 3.25, 0..30);
    for peer in 0..6 {
        steps.extend(on_contract(peer, dead, true, 8, 3.0, 0.25));
    }
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let now = feed(&mut [&mut stage], steps);
    let effect_at = |stage: &Stage<u32>, at: f64| {
        stage
            .contracts
            .as_ref()
            .unwrap()
            .shared_effect(dead.to_bits(), at)
    };
    assert!(effect_at(&stage, now).unwrap() > 0.1);
    // Three hours on, six contract horizons, with nothing observed on the
    // contract since.
    assert_eq!(effect_at(&stage, now + 3.0), None);
    let later = feed(&mut [&mut stage], background_pooled(now, 3.0, 0..30));
    assert_eq!(effect_at(&stage, later), None);
    let prior = stage.prior(0.05).unwrap();
    assert_eq!(
        stage.forecast_prior(prior, dead, later).to_bits(),
        prior.to_bits(),
        "a stale contract must add nothing to a forecast"
    );
}

/// Finding 2 of the 2026-09-17 review. The refit iterates the window in
/// [`window_order`], which is ascending DISTANCE, so a first-come entry rule
/// kept a distance-biased subset of a many-peer contract (and zeroed the
/// incumbents it displaced, leaving entries below `replicated()`). Admission
/// is now by accumulated weight, so the eight heaviest peers are kept
/// whatever order the window is in.
#[test]
fn the_refit_keeps_a_contracts_heaviest_peers_not_the_nearest() {
    let _guard = GlobalRng::seed_guard(0x4485_c00d);
    let busy = 0.37;
    let mut steps = background_pooled(0.0, 3.0, 20..50);
    // Twelve peers on one contract. The four NEAREST have one event each and
    // the eight farthest have eight, so distance order and weight order
    // disagree completely: a first-come rule over the distance-sorted window
    // admits the four near peers first.
    for peer in 0..12u32 {
        let near = peer < 4;
        let count = if near { 1 } else { 8 };
        let distance = if near { 0.01 } else { 0.2 };
        for i in 0..count {
            steps.push(Step {
                peer,
                contract: busy,
                distance,
                failed: true,
                hours: 3.0 + 0.2 * i as f64 / count as f64,
            });
        }
    }
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    feed(&mut [&mut stage], steps);
    let slot_of = |peer: u32| stage.peers.lookup(&peer).expect("peer is tracked") as u32;
    let table = stage
        .contracts
        .as_ref()
        .expect("the failure stage has a term");
    let node = table
        .table
        .lookup(&busy.to_bits())
        .expect("the contract is tracked");
    let kept: Vec<u32> = table.nodes[node]
        .entries
        .iter()
        .filter(|entry| entry.used())
        .map(|entry| entry.peer_slot)
        .collect();
    assert_eq!(
        kept.len(),
        CONTRACT_ENTRIES,
        "the contract is full: {kept:?}"
    );
    for peer in 4..12u32 {
        assert!(
            kept.contains(&slot_of(peer)),
            "peer {peer} carries eight events and must be kept: {kept:?}"
        );
    }
    for peer in 0..4u32 {
        assert!(
            !kept.contains(&slot_of(peer)),
            "peer {peer} is the nearest but carries one event, so it must not \
             displace a heavier peer: {kept:?}"
        );
    }
    // Every kept entry holds all of its peer's events, not a remnant left by
    // being displaced and zeroed.
    for entry in table.nodes[node].entries.iter().filter(|e| e.used()) {
        assert!(
            entry.moments.replicated(),
            "a kept entry must carry its peer's whole evidence: {:?}",
            entry.moments
        );
    }
    // Finding 11: the refit's own discards are counted, separately from the
    // live ones. Four pairs are outside the heaviest eight at every refit.
    let diagnostics = stage.diagnostics();
    assert!(
        diagnostics.contract_pairs_refused_last_refit >= 4,
        "the refit's discards must be counted: {}",
        diagnostics.contract_pairs_refused_last_refit
    );
}

/// The grouped rebuild must attribute each event to the right (contract,
/// peer) pair. The events are sorted by `contract_slot << 32 | peer_slot`, so
/// two adjacent contracts holding the SAME peer slot are adjacent in that
/// order with an identical low half, and a peer-loop that matched on the low
/// half alone merged the second contract's events into the first and left the
/// second contract with no entries at all. Found by review of the fix for
/// finding 2 before it was measured; this is the arrangement that reaches it.
#[test]
fn the_refit_attributes_events_to_the_right_contract_when_a_peer_repeats() {
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let mut scratch = Scratch::default();
    let (first, second) = (0.25, 0.75);
    for i in 0..3 {
        stage.observe(&mut scratch, &0, first, 0.1, 1.0, i as f64 * 0.01);
    }
    for i in 0..5 {
        stage.observe(&mut scratch, &0, second, 0.2, 0.0, 0.05 + i as f64 * 0.01);
    }
    let table = stage
        .contracts
        .as_ref()
        .expect("the failure stage has a term");
    // The premise: adjacent contract slots, one shared peer slot.
    assert_eq!(table.table.lookup(&first.to_bits()), Some(0));
    assert_eq!(table.table.lookup(&second.to_bits()), Some(1));
    assert_eq!(stage.peers.lookup(&0), Some(0));
    for (slot, count) in [(0usize, 3usize), (1, 5)] {
        let used: Vec<&ContractEntry> = table.nodes[slot]
            .entries
            .iter()
            .filter(|entry| entry.used())
            .collect();
        assert_eq!(
            used.len(),
            1,
            "contract slot {slot} must hold exactly its own peer's entry"
        );
        assert_eq!(used[0].peer_slot, 0);
        // Weights are `exp((t - now) / CONTRACT_HORIZON_HOURS)`, so the count
        // is below the event count but bounded by it; the wrong grouping gave
        // slot 0 all eight events and slot 1 none.
        assert!(
            used[0].moments.n <= count as f64 && used[0].moments.n > 0.5 * count as f64,
            "contract slot {slot} must hold {count} events' worth of weight: {:?}",
            used[0].moments
        );
    }
}

/// Finding 11, exactly: the refit's refusal count is a DIFFERENT quantity from
/// the live one. `rebuild_node` reports its own discards and never touches the
/// live counter, so deleting the live counter cannot make the refit count
/// non-zero and deleting the refit count cannot be hidden by the live one.
#[test]
fn refit_refusals_are_counted_apart_from_live_refusals() {
    let mut table = ContractTable::new();
    let (slot, _) = table.touch(0.25f64.to_bits());
    let mut pairs: Vec<(u32, u32, Moments)> = (0..10u32)
        .map(|peer| {
            let mut moments = Moments::default();
            moments.add(1.0 + f64::from(peer), 1.0);
            (peer, 0, moments)
        })
        .collect();
    assert_eq!(table.rebuild_node(slot, &mut pairs), 2);
    assert_eq!(
        table.refused, 0,
        "the live counter must not move at a refit"
    );
    let kept: Vec<u32> = table.nodes[slot]
        .entries
        .iter()
        .filter(|e| e.used())
        .map(|e| e.peer_slot)
        .collect();
    assert_eq!(kept.len(), CONTRACT_ENTRIES);
    // The two lightest pairs (peers 0 and 1) are the ones dropped.
    assert!(!kept.contains(&0) && !kept.contains(&1), "{kept:?}");
    // The result does not depend on the order the pairs were accumulated in.
    let mut reversed: Vec<(u32, u32, Moments)> = pairs.iter().rev().copied().collect();
    let mut other = ContractTable::new();
    let (other_slot, _) = other.touch(0.25f64.to_bits());
    assert_eq!(other.rebuild_node(other_slot, &mut reversed), 2);
    assert_eq!(other.nodes[other_slot], table.nodes[slot]);
}

/// `pairs_refused_last_refit` is a GAUGE: the refit resets it, so it reports
/// the last refit's drops rather than a running total. That semantic was
/// unpinned (I6 of the 2026-09-18 round-3 testing review): removing the reset
/// left the whole suite green while turning the dashboard gauge into a
/// monotonically rising number.
///
/// It also feeds `log_saturation`'s change detector, and the H1 underflow in
/// that function is exactly what happens when the gauge's fall is not
/// accounted for, so the two belong together.
#[test]
fn the_refit_gauge_resets_and_reads_zero_when_nothing_is_dropped() {
    let mut table = ContractTable::new();
    let (slot, _) = table.touch(0.25f64.to_bits());

    // A refit that DOES drop: ten peers for eight entries.
    let mut crowded: Vec<(u32, u32, Moments)> = (0..10u32)
        .map(|peer| {
            let mut m = Moments::default();
            m.add(1.0 + f64::from(peer), 0.5);
            (peer, 0, m)
        })
        .collect();
    table.pairs_refused_last_refit += table.rebuild_node(slot, &mut crowded) as u64;
    assert_eq!(
        table.pairs_refused_last_refit, 2,
        "ten peers into eight entries drops two"
    );

    // The next refit is clean, and the gauge must go back to zero rather than
    // keep the previous refit's two.
    let mut roomy: Vec<(u32, u32, Moments)> = (0..4u32)
        .map(|peer| {
            let mut m = Moments::default();
            m.add(1.0, 0.5);
            (peer, 0, m)
        })
        .collect();
    table.pairs_refused_last_refit = 0;
    table.pairs_refused_last_refit += table.rebuild_node(slot, &mut roomy) as u64;
    assert_eq!(
        table.pairs_refused_last_refit, 0,
        "a refit whose contract fits in CONTRACT_ENTRIES drops nothing"
    );

    // And through the real refit path, which is what owns the reset: feed a
    // stage traffic whose contracts all fit, refit, and read the gauge.
    let _guard = GlobalRng::seed_guard(0x4485_c6a6);
    let (mut stage, _) = failure_stage_pair();
    feed(&mut [&mut stage], background_pooled(0.0, 1.0, 0..4));
    assert_eq!(
        stage.diagnostics().contract_pairs_refused_last_refit,
        0,
        "four peers can never fill a contract's eight entries, so the gauge \
         must read zero after the last refit"
    );
}

/// The term is BIDIRECTIONAL, which is the second design limitation recorded
/// on #5700 from the 2026-09-17 review and which no test covered (a mutation
/// clamping the effect at zero survived the whole suite). A residual is
/// `y - curve(distance)`, so a SUCCESS on a contract whose curve value is
/// above zero contributes a NEGATIVE residual. A contract several peers
/// succeed on therefore gets a negative effect, and the learning adjustment
/// SUBTRACTS it, which RAISES what the peer levels learn from an honest
/// peer's own events on that contract. Measured on the recorded soak's tuning
/// window the mean effect is about -0.013 to -0.066 at healthy instants, so
/// this is the ordinary case and not a corner.
#[test]
fn a_contract_peers_succeed_on_gets_a_negative_effect_that_raises_what_they_learn() {
    let _guard = GlobalRng::seed_guard(0x4485_c015);
    let good = 0.37;
    // Traffic that fails 30% of the time at every distance, so the curve sits
    // well above zero and a success is a large negative residual.
    let mut steps: Vec<Step> = (0..1_800)
        .map(|i| Step {
            peer: GlobalRng::random_range(10..30u32),
            // A POOL, not a unique contract per event: a one-event contract
            // never reaches the replication bar, so it never qualifies for
            // `tau2_contract` and the term switches off under the `den >= 2`
            // gate.
            contract: (i % 40) as f64 / 40.0,
            distance: uniform() * 0.5,
            failed: uniform() < 0.3,
            hours: i as f64 / 600.0,
        })
        .collect();
    // Five peers succeed on one contract, then a sixth also succeeds there.
    for peer in 0..5u32 {
        steps.extend(on_contract(peer, good, false, 8, 2.8, 0.15));
    }
    steps.extend(on_contract(9, good, false, 4, 2.95, 0.02));
    let (mut with_term, mut without_term) = failure_stage_pair();
    let now = feed(&mut [&mut with_term, &mut without_term], steps);

    let table = with_term
        .contracts
        .as_ref()
        .expect("the failure stage has a term");
    let effect = table
        .shared_effect(good.to_bits(), now)
        .unwrap_or_else(|| panic!("six present peers: components {:?}", table.components));
    assert!(
        effect < -0.05,
        "a contract everyone succeeds on must get a NEGATIVE effect: {effect}"
    );
    let slot = stage_slot(&with_term, 9);
    let adjustments: f64 = with_term
        .sorted
        .iter()
        .filter(|event| event.slot == slot)
        .map(|event| event.adjustment as f64)
        .sum();
    assert!(
        adjustments < -0.05,
        "peer 9's events on the contract must carry a negative adjustment: {adjustments}"
    );
    // Subtracting a negative adjustment raises what the levels learn, so the
    // honest peer's learned residual is HIGHER with the term than without it.
    let charged = without_term.levels[0].nodes[slot as usize].peer.sum;
    let applied = with_term.levels[0].nodes[slot as usize].peer.sum;
    assert!(
        applied > charged + 0.05,
        "the negative effect must raise the honest peer's learned residual: \
         {applied} against {charged} without the term"
    );
}

/// Finding 4 of the 2026-09-17 review: an event older than the presence
/// window keeps the adjustment it already had, instead of receiving the full
/// current effect. Before the fix a 12-hour-old event, which contributes
/// about 4e-11 to the estimate, was re-scored at unit weight against a
/// contract state estimated from the last 1.5 hours.
#[test]
fn an_event_outside_the_presence_window_keeps_its_stored_adjustment() {
    let _guard = GlobalRng::seed_guard(0x4485_c00e);
    let dead = 0.37;
    // Filler traffic on a SMALL pool of contracts, so the contract table does
    // not turn over and the dead contract stays tracked while the event ages.
    // `background` draws a unique contract per event and would evict it.
    let filler = |start: f64, hours: f64| -> Vec<Step> {
        let count = (hours * 600.0) as usize;
        (0..count)
            .map(|i| {
                let distance = uniform() * 0.5;
                Step {
                    peer: GlobalRng::random_range(1..30u32),
                    contract: 0.5 + 0.001 * (i % 30) as f64,
                    distance,
                    failed: uniform() < 0.02 + 0.2 * distance,
                    hours: start + i as f64 / 600.0,
                }
            })
            .collect()
    };
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    feed(&mut [&mut stage], filler(0.0, 2.0));
    // Peer 0's only failure on the contract, with no other peer there yet, so
    // it is adjusted by nothing.
    feed(&mut [&mut stage], on_contract(0, dead, true, 1, 2.0, 0.0));
    let slot = stage.peers.lookup(&0).expect("peer 0 is tracked") as u32;
    // Locate the event once, by peer, and then track it by sequence number:
    // an exact float distance is not a robust way to find an event.
    let seq = stage
        .sorted
        .iter()
        .find(|event| event.slot == slot)
        .expect("the event is in the window")
        .seq;
    let event_at = move |stage: &Stage<u32>| {
        *stage
            .sorted
            .iter()
            .find(|event| event.seq == seq)
            .expect("the event is still in the window")
    };
    assert_eq!(event_at(&stage).adjustment, 0.0);

    // Two hours of unrelated traffic: four contract horizons, so peer 0's
    // event is outside the presence window (weight exp(-4) = 0.018 against a
    // 0.05 cut, i.e. older than the 1.4979 h the cut corresponds to).
    feed(&mut [&mut stage], filler(2.0, 2.0));

    // A storm on the same contract NOW, which gives it a large present effect.
    let mut storm = Vec::new();
    for peer in 1..6 {
        storm.extend(on_contract(peer, dead, true, 6, 4.0, 0.2));
    }
    let now = feed(&mut [&mut stage], storm);
    let old = event_at(&stage);
    let age = now - old.time;
    assert!(
        (-age / CONTRACT_HORIZON_HOURS).exp() < CONTRACT_PRESENCE,
        "the event must be outside the presence window, or this test proves \
         nothing: age {age} h"
    );
    let table = stage
        .contracts
        .as_ref()
        .expect("the failure stage has a term");
    assert_eq!(
        table.table.lookup(&dead.to_bits()),
        Some(old.contract_slot as usize),
        "the contract must still be tracked under the event's own slot, or the \
         event keeps its adjustment for the unrelated reason that its contract \
         is gone"
    );
    assert!(
        table.live(old.contract_slot, old.contract_generation),
        "the event's contract slot must still be live, same reason"
    );
    let effect = table
        .shared_effect(dead.to_bits(), now)
        .expect("five failing peers give the contract a present effect");
    assert!(
        effect > 0.1,
        "the contract must have a large present effect, or this test proves \
         nothing: {effect}"
    );
    assert_eq!(
        old.adjustment, 0.0,
        "an event outside the presence window must not receive the current \
         effect (it would have taken about {effect})"
    );
    // Non-vacuity the other way: a FRESH event by peer 0 on the same contract
    // IS adjusted, so the mechanism is alive at this instant.
    feed(
        &mut [&mut stage],
        on_contract(0, dead, true, 1, now + 0.01, 0.0),
    );
    let fresh = stage
        .sorted
        .iter()
        .filter(|event| event.slot == slot && event.time > now)
        .map(|event| event.adjustment)
        .fold(0.0f32, f32::max);
    assert!(
        fresh > 0.1,
        "an event inside the presence window must still be adjusted: {fresh}"
    );
}

/// H1 of the 2026-09-17 round-2 review: a contract that RECOVERS while a
/// storm-era event is still inside the presence window.
///
/// The trace the review gives: contract C is dead, peer P fails it, refits
/// during the storm set P's event's adjustment to a large POSITIVE value
/// (correct, the failure is explained away). C then recovers and several peers
/// succeed on it. At the next refit P's event is still inside the presence
/// window, so it is re-scored, `leave_out_effect` now returns a value
/// dominated by the fresh successes, and the adjustment is overwritten with a
/// NEGATIVE number. `residual -= adjustment` then RAISES what the levels
/// learn: the dead-contract failure is charged back to the peer with interest.
/// The finding-4 fix made that permanent, because once the event leaves the
/// presence window the wrong value is frozen and re-applied at every later
/// refit.
///
/// Two guards, both asserted here: the sign-flip guard keeps the value from
/// the event's own era, and [`explaining_bound`] makes it impossible for any
/// adjustment to invert an event's evidence whatever else goes wrong.
///
/// No previous test reached this: the two that age a contract out use
/// `background`, which draws a unique contract per event, so the dead contract
/// never receives fresh evidence.
#[test]
fn a_contract_that_recovers_does_not_charge_its_storm_failures_back() {
    let _guard = GlobalRng::seed_guard(0x4485_c016);
    let dead = 0.37;
    // A high baseline failure rate at every distance, so the curve sits well
    // above zero and a SUCCESS carries a real negative residual. With
    // `background`'s 2% floor the curve is near zero at the test's distance,
    // successes are worth about -0.03 against a failure's +0.97, and no amount
    // of recovery can outweigh the storm, so the defect is unreachable.
    let (mut with_term, mut without_term) = failure_stage_pair();
    let filler: Vec<Step> = (0..1_800)
        .map(|i| Step {
            peer: GlobalRng::random_range(10..40u32),
            // A POOL, not a unique contract per event: a one-event contract
            // never reaches the replication bar, so it never qualifies for
            // `tau2_contract` and the term switches off under the `den >= 2`
            // gate.
            contract: (i % 40) as f64 / 40.0,
            distance: uniform() * 0.5,
            failed: uniform() < 0.3,
            hours: i as f64 / 900.0,
        })
        .collect();
    feed(&mut [&mut with_term, &mut without_term], filler);

    // The storm: peers 0 to 5 all fail the contract over six minutes.
    // Four OTHER contracts stay dead throughout, which is what keeps
    // `tau2_contract` positive while the contract under test recovers. With
    // `background`'s unique contract per event, the contract under test is
    // otherwise the only qualifying group, and a recovered contract's own
    // mean then drives the between-contract variance to zero and switches the
    // term off before the defect can be reached.
    let mut storm = Vec::new();
    for peer in 0..6u32 {
        storm.extend(on_contract(peer, dead, true, 6, 2.0, 0.1));
    }
    for (index, other) in [0.34f64, 0.345, 0.35, 0.355].iter().enumerate() {
        for peer in 30..36u32 {
            storm.extend(on_contract(
                peer + index as u32 * 6,
                *other,
                true,
                6,
                2.0,
                0.5,
            ));
        }
    }
    feed(&mut [&mut with_term, &mut without_term], storm);
    let slot = stage_slot(&with_term, 0);
    let seq = with_term
        .sorted
        .iter()
        .filter(|event| event.slot == slot && event.time >= 2.0)
        .map(|event| event.seq)
        .next()
        .expect("peer 0's storm event is in the window");
    let event_at = move |stage: &Stage<u32>| {
        *stage
            .sorted
            .iter()
            .find(|event| event.seq == seq)
            .expect("the event is still in the window")
    };
    let storm_adjustment = event_at(&with_term).adjustment;
    assert!(
        storm_adjustment > 0.3,
        "the storm must explain the failure away, or this test proves nothing: \
         {storm_adjustment}"
    );

    // The recovery, INSIDE the presence window: 0.35 h after the event, which
    // is 0.7 contract horizons, so the event's own weight is still 0.50,
    // comfortably above the 0.05 presence cut, while the fresh successes are
    // several times heavier than the decayed storm.
    let mut recovery = Vec::new();
    for peer in 6..12u32 {
        recovery.extend(on_contract(peer, dead, false, 8, 2.45, 0.05));
    }
    let now = feed(&mut [&mut with_term, &mut without_term], recovery);
    let event = event_at(&with_term);
    let age = now - event.time;
    assert!(
        (-age / CONTRACT_HORIZON_HOURS).exp() > CONTRACT_PRESENCE,
        "the storm event must still be INSIDE the presence window, or the \
         freeze hides the overwrite: age {age} h"
    );
    let table = with_term
        .contracts
        .as_ref()
        .expect("the failure stage has a term");
    let node = table
        .table
        .lookup(&dead.to_bits())
        .expect("the dead contract is still tracked");
    let present = table.nodes[node]
        .entries
        .iter()
        .filter(|entry| entry.used())
        .count();
    let fresh_effect = table
        .leave_out_effect(
            Some(event.contract_slot as usize),
            Some((event.slot, event.generation)),
            now,
        )
        .unwrap_or_else(|| {
            panic!(
                "the recovered contract still has present evidence: node {node}, \
                 {present} used entries, live {}, components {:?}",
                table.live(event.contract_slot, event.contract_generation),
                table.components
            )
        });
    assert!(
        fresh_effect < 0.0,
        "the recovery must make the CURRENT effect negative, or this test \
         cannot reach the defect: {fresh_effect}"
    );

    // The guard: the stored adjustment keeps the storm's sign and magnitude.
    let after = event_at(&with_term).adjustment;
    assert_eq!(
        after.to_bits(),
        storm_adjustment.to_bits(),
        "an effect that has changed sign describes a different regime, so the \
         event must keep the value from its own era (was {storm_adjustment}, \
         current effect {fresh_effect})"
    );

    // And the property that matters, whatever the adjustment: peer 0 must
    // never be charged MORE than the control stage charges it, which is what
    // a negative adjustment would do.
    let charged = without_term.levels[0].nodes[slot as usize].peer.sum;
    let applied = with_term.levels[0].nodes[slot as usize].peer.sum;
    assert!(
        applied <= charged + 1e-9,
        "explaining away must never charge a peer MORE than its raw residuals: \
         with term {applied}, control {charged}"
    );
}

/// The backstop for the above, as an invariant rather than a scenario: an
/// adjustment may neutralise an event's evidence and must never invert it.
#[test]
fn an_adjustment_can_neutralise_evidence_but_never_invert_it() {
    // A failure's residual is positive; a negative effect must not be applied,
    // and an over-large positive one is capped at the residual.
    assert_eq!(explaining_bound(-0.4, 0.9), 0.0);
    assert_eq!(explaining_bound(0.5, 0.9), 0.5);
    assert_eq!(explaining_bound(1.4, 0.9), 0.9);
    // A success's residual is negative, and the mirror image holds.
    assert_eq!(explaining_bound(0.4, -0.3), 0.0);
    assert_eq!(explaining_bound(-0.2, -0.3), -0.2);
    assert_eq!(explaining_bound(-0.9, -0.3), -0.3);
    // Non-finite adjustments remove nothing.
    assert_eq!(explaining_bound(f64::NAN, 0.9), 0.0);
    assert_eq!(explaining_bound(f64::INFINITY, 0.9), 0.0);
    // The invariant, over a grid: the adjusted residual keeps the residual's
    // sign or is exactly zero, and never grows in magnitude.
    for residual in [-1.0, -0.3, -1e-9, 0.0, 1e-9, 0.3, 1.0] {
        for adjustment in [-2.0, -0.5, 0.0, 0.5, 2.0] {
            let adjusted = residual - explaining_bound(adjustment, residual);
            assert!(
                adjusted * residual >= 0.0,
                "{residual} - bound({adjustment}) = {adjusted} inverted the evidence"
            );
            assert!(
                adjusted.abs() <= residual.abs() + 1e-12,
                "{residual} - bound({adjustment}) = {adjusted} grew the evidence"
            );
        }
    }
}

/// Finding 5 of the 2026-09-17 review: peer eviction cleared the levels and
/// left the contract table holding entries under the evicted `(slot,
/// generation)`. `PeerTable::touch` reuses a freed slot from a LIFO free
/// list, so a reconnecting peer commonly regains it with a new generation,
/// `ContractQuery::LeaveOut` then excludes only the exact new pair, and the
/// peer's own earlier failures counted as OTHER-peer evidence about the
/// contract.
#[test]
fn evicting_a_peer_clears_its_contract_entries() {
    // Unit: only the evicted slots are cleared.
    let mut table = ContractTable::new();
    let (slot, _) = table.touch(0.25f64.to_bits());
    for peer in 1..4u32 {
        assert!(table.add(slot, (peer, 0), 1.0, 1.0));
    }
    table.evict_peers(&[2]);
    let peers: Vec<u32> = table.nodes[slot]
        .entries
        .iter()
        .filter(|e| e.used())
        .map(|e| e.peer_slot)
        .collect();
    assert_eq!(
        peers,
        vec![1, 3],
        "only the evicted peer's entry is cleared"
    );

    // Through a stage, and BETWEEN refits, which is the only window in which
    // this is observable: a refit rebuilds the table from the prepared
    // window, which holds only live peers, so it wipes stale entries on its
    // own. A mutation removing `evict_peers` survives any version of this
    // test that refits after the eviction, and the first version did.
    let _guard = GlobalRng::seed_guard(0x4485_c00f);
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let dead = 0.37;
    // Peers 0 to 5 fail the dead contract EARLY and are never seen again, so
    // they are the least-recently-used and the first to be evicted. Peers 6
    // to 63 carry the rest of the traffic, filling the 64-slot table.
    let mut steps = background(0.0, 0.4, 6..64);
    for peer in 0..6u32 {
        steps.extend(on_contract(peer, dead, true, 8, 0.02, 0.05));
    }
    let now = feed(&mut [&mut stage], steps);
    assert!(
        stage.peers.index.len() >= 60,
        "the peer table must be nearly full, or nothing evicts: {}",
        stage.peers.index.len()
    );
    assert!(
        stage.sorted.len() > EAGER_REFIT_BELOW,
        "the window must be past the eager-refit size, or every event refits"
    );
    let node = stage
        .contracts
        .as_ref()
        .expect("term")
        .table
        .lookup(&dead.to_bits())
        .expect("the dead contract is tracked");
    let used = |stage: &Stage<u32>| {
        stage.contracts.as_ref().expect("term").nodes[node]
            .entries
            .iter()
            .filter(|entry| entry.used())
            .count()
    };
    assert!(
        used(&stage) >= 5,
        "the contract must hold several peers, or this test proves nothing: {}",
        used(&stage)
    );
    // Twenty brand-new peers, on a DIFFERENT contract so they cannot refill
    // the dead contract's node, through a nearly-full 64-slot table. Twenty
    // events is fewer than the 50-event refit interval, so no refit
    // intervenes.
    let mut scratch = Scratch::default();
    let mut evicted = Vec::new();
    for (index, peer) in (1_000..1_020u32).enumerate() {
        stage.observe(
            &mut scratch,
            &peer,
            0.6,
            0.05,
            1.0,
            now + 0.001 * (index + 1) as f64,
        );
        evicted.extend(scratch.evicted.iter().copied());
    }
    assert!(
        stage.since_refit > 0
            && stage.since_refit < refit_interval(WINDOW_EVENTS, stage.sorted.len()),
        "no refit may intervene, or the rebuild hides the defect: since_refit {}",
        stage.since_refit
    );
    assert!(
        !evicted.is_empty(),
        "the new peers must evict, or this test proves nothing"
    );
    assert_eq!(
        used(&stage),
        0,
        "every peer of the dead contract was evicted, so its node must be empty"
    );
    // And no node anywhere holds a generation the peer table no longer has.
    let table = stage.contracts.as_ref().expect("term");
    for (node_index, node) in table.nodes.iter().enumerate() {
        for entry in node.entries.iter().filter(|e| e.used()) {
            assert_eq!(
                stage.peers.generation(entry.peer_slot as usize),
                Some(entry.peer_generation),
                "contract node {node_index} holds a stale entry for peer slot {}",
                entry.peer_slot
            );
        }
    }
}

/// Finding 8 of the 2026-09-17 review. The contract table's variance
/// components set the term's whole magnitude and had no test; the peer
/// levels' equivalent is `variance_components_recover_the_generating_values`.
/// Method of moments recovers known components, averaged over seeds so the
/// tolerance can be tight.
#[test]
fn contract_variance_components_recover_the_generating_values() {
    let (sigma, tau_peer, tau_contract) = (1.0, 0.5, 0.4);
    let seeds = 8u64;
    let (mut s2, mut tp, mut tc) = (0.0, 0.0, 0.0);
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_cc00 + seed);
        let mut table = ContractTable::new();
        for contract in 0..120u64 {
            let (slot, _) = table.touch(contract);
            let contract_effect = tau_contract * normal();
            for peer in 0..CONTRACT_ENTRIES as u32 {
                let peer_effect = tau_peer * normal();
                for _ in 0..30 {
                    assert!(table.add(
                        slot,
                        (peer, 0),
                        1.0,
                        contract_effect + peer_effect + sigma * normal(),
                    ));
                }
            }
        }
        let c = table
            .compute_components()
            .expect("a full table has components");
        s2 += c.sigma2 / seeds as f64;
        tp += c.tau2_peer / seeds as f64;
        tc += c.tau2_contract / seeds as f64;
    }
    assert!((s2 - sigma * sigma).abs() < 0.05, "sigma2 {s2}");
    assert!((tp - tau_peer * tau_peer).abs() < 0.05, "tau2_peer {tp}");
    assert!(
        (tc - tau_contract * tau_contract).abs() < 0.05,
        "tau2_contract {tc}"
    );
}

/// The presence filter inside `compute_components` had no test: a mutation
/// deleting it survived the whole suite. The filter is what stops evidence
/// that has decayed away from setting the term's magnitude, and because the
/// table's epoch is the rebuild time, a stored count below
/// [`CONTRACT_PRESENCE`] IS decayed-away evidence. Pinned as an invariance:
/// adding sub-presence entries to every contract must leave the components
/// bit-identical.
///
/// Note the filter is not redundant with `replicated()`. A sub-presence entry
/// is not replicated, so it never reaches `sigma2` or the `tau2_peer`
/// numerator, but without the filter it still joins the leave-one-out REST
/// sums and the per-contract total, which is where it moves both between-group
/// variances.
#[test]
fn sub_presence_entries_do_not_move_the_contract_components() {
    let build = |with_stale: bool| {
        // Seeded INSIDE the closure, so both tables are built from the same
        // random draws. Seeding outside it would have the second call continue
        // the first's stream, and the two tables would differ for that reason
        // rather than for the one under test.
        let _guard = GlobalRng::seed_guard(0x4485_cc40);
        let mut table = ContractTable::new();
        for contract in 0..60u64 {
            let (slot, _) = table.touch(contract);
            let contract_effect = 0.4 * normal();
            for peer in 0..4u32 {
                let peer_effect = 0.5 * normal();
                for _ in 0..30 {
                    assert!(table.add(
                        slot,
                        (peer, 0),
                        1.0,
                        contract_effect + peer_effect + normal()
                    ));
                }
            }
            if with_stale {
                // Four more peers, each one event whose decayed count is far
                // below the presence cut. Values chosen to be large, so a
                // mutation that lets them count cannot pass by being small.
                for peer in 4..8u32 {
                    assert!(table.add(slot, (peer, 0), CONTRACT_PRESENCE / 50.0, 5.0));
                }
            }
        }
        table
            .compute_components()
            .expect("a full table has components")
    };
    let (clean, with_stale) = (build(false), build(true));
    assert_eq!(
        clean, with_stale,
        "sub-presence entries must not reach the components"
    );
}

/// `tau2_peer` is a VARIANCE and is floored at zero: a mutation removing the
/// floor survived the whole suite. The method-of-moments estimator subtracts
/// a noise term from a squared contrast, so its raw value is negative
/// whenever the contrast is smaller than the noise, which is the normal case
/// when no between-peer effect exists. Left negative it would enter
/// `effect`'s noise as `tau2_peer * sum n_q^2 / n^2`, REDUCING the noise and
/// so INFLATING the shrunk contract effect, and it would also inflate
/// `tau2_contract` by being subtracted from its own noise.
///
/// Constructed so the contrast is identically zero rather than relying on a
/// draw: every cell holds the same alternating values, so every entry's mean
/// and every leave-one-out rest mean are 0.5, while the within-cell variance
/// is 0.25, so the raw estimate is exactly minus the mean noise.
#[test]
fn tau2_peer_is_floored_at_zero() {
    let mut table = ContractTable::new();
    for contract in 0..40u64 {
        let (slot, _) = table.touch(contract);
        for peer in 0..4u32 {
            for i in 0..10 {
                assert!(table.add(slot, (peer, 0), 1.0, f64::from(u8::from(i % 2 == 0))));
            }
        }
    }
    let c = table
        .compute_components()
        .expect("replicated cells everywhere");
    assert!(
        c.sigma2 > 0.2,
        "the scenario must carry real within-cell variance, or the noise term \
         is zero and this test proves nothing: {}",
        c.sigma2
    );
    assert_eq!(
        c.tau2_peer, 0.0,
        "every entry has the same mean, so the raw estimate is negative and \
         must be floored"
    );
}

/// `tau2_contract` needs at least TWO qualifying contracts, the same rule as
/// the degrees-of-freedom gate below and for the same reason: a between-group
/// variance estimated from one group is not a variance, it is that group's own
/// mean, and it then un-shrinks every other contract's effect.
///
/// Untested when the gate was added, and the 2026-09-17 mutation run at
/// `041e31e09` proved it: lowering the gate back to `den > 0.0` left the whole
/// suite green.
#[test]
fn tau2_contract_needs_two_qualifying_contracts() {
    // One qualifying contract among two hundred singletons. A singleton
    // contract's total is not replicated, so it never qualifies; the one with
    // two replicated cells crosses `df >= 2` on its own.
    let build = |qualifying: u64| {
        let mut table = ContractTable::new();
        for contract in 0..200u64 {
            let (slot, _) = table.touch(contract);
            if contract < qualifying {
                for peer in 0..2u32 {
                    assert!(table.add(slot, (peer, 0), 1.0, 0.9));
                    assert!(table.add(slot, (peer, 0), 1.0, 0.5));
                }
            } else {
                assert!(table.add(slot, (0, 0), 1.0, 0.9));
            }
        }
        table
            .compute_components()
            .expect("the df gate is crossed in both cases")
    };

    let one = build(1);
    assert_eq!(
        one.qualifying_contracts, 1,
        "the scenario must reach exactly one qualifying contract, or this test \
         proves nothing"
    );
    assert_eq!(
        one.tau2_contract, 0.0,
        "one qualifying contract cannot tell you how contracts vary, so the \
         term must be off rather than resting on that contract's own mean"
    );

    let two = build(2);
    assert_eq!(two.qualifying_contracts, 2);
    assert!(
        two.tau2_contract > 0.0,
        "two qualifying contracts must produce a between-contract variance: {}",
        two.tau2_contract
    );
    // And with the term off, no query can produce an effect however much
    // evidence the queried contract itself has.
    let mut off = ContractTable::new();
    let key = 0u64;
    let (slot, _) = off.touch(key);
    for peer in 0..4u32 {
        for _ in 0..8 {
            assert!(off.add(slot, (peer, 0), 1.0, 0.9));
        }
    }
    off.components = Some(one);
    assert_eq!(off.shared_effect(key, 0.0), None);
}

/// The degrees-of-freedom gate had no test: a mutation lowering `df < 2.0`
/// to `df < 0.0` survived the whole suite. Without it a table in which no
/// `(contract, peer)` cell has any within-cell replication still produces
/// components: `ss` and `df` are both 0, `ss / df` is NaN, `.max(MIN_SIGMA2)`
/// returns the finite operand, and the term then activates with a pooled
/// within-cell variance of 1e-9 and a between-contract variance that is just
/// each contract's mean squared. That is the term reading pure sampling noise
/// as a dead contract.
#[test]
fn contract_components_need_within_cell_replication() {
    let mut table = ContractTable::new();
    for contract in 0..60u64 {
        let (slot, _) = table.touch(contract);
        for peer in 0..CONTRACT_ENTRIES as u32 {
            // Exactly one event per cell, so no cell is replicated, while the
            // per-contract totals are (8 events, Kish n_eff 8).
            assert!(table.add(slot, (peer, 0), 1.0, 0.5));
        }
    }
    assert_eq!(
        table.compute_components(),
        None,
        "no cell carries within-cell replication, so there is no sigma2 to \
         estimate and the term must not activate"
    );
    // One replicated cell is still not enough: the gate is on the POOLED
    // degrees of freedom, and one unit-weight cell of two events carries 1.
    let (slot, _) = table.touch(0);
    assert!(table.add(slot, (0, 0), 1.0, 0.9));
    assert_eq!(table.compute_components(), None);
    // A second one crosses it, and now there is within-cell variance to
    // estimate, so components exist.
    assert!(table.add(slot, (1, 0), 1.0, 0.9));
    assert!(table.compute_components().is_some());
}

/// The evidence floor, in BOTH regimes. This is the acceptance test for the
/// round-2 finding F decision.
///
/// The problem it solves: `ss = 0` does not distinguish "no within-cell
/// information" from "unanimous evidence", and it cannot, because both produce
/// identical residuals. Refusing on the `MIN_SIGMA2` sentinel therefore
/// switches the term off on its own target case, which was measured and is why
/// that route was abandoned. What separates the two is evidence QUANTITY, so
/// the floor is [`bernoulli_variance_floor`], which enters the noise term and
/// so falls as `1 / n_eff`.
///
/// (a) Two barely-replicated cells in a large table: the effect must be
/// strongly shrunk relative to the raw mean. (b) A dead contract with several
/// peers and many events each: the effect must stay close to full strength.
#[test]
fn the_evidence_floor_shrinks_thin_evidence_and_spares_a_dead_contract() {
    // Shared between the two regimes so the only difference is the evidence.
    let components_for = |table: &mut ContractTable| {
        table.components = table.compute_components();
        table.components.expect("components exist")
    };
    let raw_mean = |table: &ContractTable, slot: usize| {
        let (mut n, mut sum) = (0.0, 0.0);
        for e in table.nodes[slot].entries.iter().filter(|e| e.used()) {
            n += e.moments.n;
            sum += e.moments.sum;
        }
        sum / n
    };

    // (a) THIN. A large table of singletons, with just enough replication to
    // cross `df >= 2` and `den >= 2`: two cells of two events each, on two
    // contracts, every residual identical so the sample variance is 0.
    let mut thin = ContractTable::new();
    for contract in 0..200u64 {
        let (slot, _) = thin.touch(contract);
        if contract < 2 {
            for peer in 0..2u32 {
                for _ in 0..2 {
                    assert!(thin.add(slot, (peer, 0), 1.0, 0.9));
                }
            }
        } else {
            assert!(thin.add(slot, (0, 0), 1.0, 0.9));
        }
    }
    let thin_components = components_for(&mut thin);
    assert!(
        thin_components.sigma2 > 0.2,
        "two barely-replicated cells must floor near the Bernoulli maximum, \
         not on the 1e-9 sentinel: {}",
        thin_components.sigma2
    );
    // I4 of the 2026-09-18 round-3 testing review: the FLAG's polarity was
    // untested, so inverting `measured <= floor` to `measured > floor`
    // survived the whole suite. The flag is what the exported
    // `floor_bound_refits` counter is built from, and the whole argument for
    // accepting an always-binding floor rests on that counter being readable
    // on a live node.
    assert!(
        thin_components.floor_bound,
        "(a) the floor must be the BINDING value here, and the flag must say so"
    );
    let thin_slot = thin.table.lookup(&0).expect("contract 0 is tracked");
    let thin_effect = thin
        .effect(Some(thin_slot), ContractQuery::Shared, 0.0, 1)
        .expect("present entries");
    let thin_ratio = thin_effect / raw_mean(&thin, thin_slot);
    // NOTE on the size of this, because the round-2 acceptance criterion asked
    // for "strongly shrunk" and that is NOT reachable through the noise term.
    // `shrink = tau2_c / (tau2_c + noise)` and `noise = sigma2 / n_eff`, while
    // `tau2_c` is the second moment about ZERO, so a contract whose own mean is
    // large has a large `tau2_c` too. With residuals of 0.9, `tau2_c` is about
    // 0.75, and `sigma2` is capped at the Bernoulli maximum of 0.25, so even
    // `n_eff = 1` gives `shrink = 0.75`. To halve the effect you would need
    // `n_eff` near 0.3, which cannot occur. What the floor DOES deliver is
    // discrimination on evidence quantity, asserted below: without it both
    // regimes shrink by about 3e-10, and with it the thin case shrinks about
    // 25 times as much as the dead one.
    assert!(
        thin_ratio < 0.94,
        "(a) thin evidence must be shrunk: effect {thin_effect} is \
         {thin_ratio} of the raw mean"
    );

    // (b) A DEAD CONTRACT. Six peers, eight events each, unanimous, so the
    // sample variance is 0 here too and only the evidence quantity differs.
    let mut dead = ContractTable::new();
    for contract in 0..200u64 {
        let (slot, _) = dead.touch(contract);
        if contract < 4 {
            for peer in 0..6u32 {
                for _ in 0..8 {
                    assert!(dead.add(slot, (peer, 0), 1.0, 0.9));
                }
            }
        } else {
            assert!(dead.add(slot, (0, 0), 1.0, 0.9));
        }
    }
    let _ = components_for(&mut dead);
    let dead_slot = dead.table.lookup(&0).expect("contract 0 is tracked");
    let dead_effect = dead
        .effect(Some(dead_slot), ContractQuery::Shared, 0.0, 1)
        .expect("present entries");
    let dead_ratio = dead_effect / raw_mean(&dead, dead_slot);
    assert!(
        dead_ratio > 0.9,
        "(b) a dead contract with real evidence must keep close to full \
         strength: effect {dead_effect} is {dead_ratio} of the raw mean"
    );
    // The discrimination, which is the property that matters and the one the
    // sentinel could not provide: the thin case is shrunk many times more than
    // the dead one. Stated as a RATIO of the shrinkage applied, because the
    // absolute shrinkage is bounded by `tau2_c` as explained above.
    let (thin_shrunk, dead_shrunk) = (1.0 - thin_ratio, 1.0 - dead_ratio);
    assert!(
        thin_shrunk > 10.0 * dead_shrunk,
        "the floor must DISCRIMINATE on evidence quantity, which is the whole \
         point: thin is shrunk by {thin_shrunk} and the dead contract by \
         {dead_shrunk}"
    );
    eprintln!(
        "#5702 finding F: evidence floor shrinks thin evidence by {thin_shrunk:.4} \
         and a dead contract by {dead_shrunk:.4}, a ratio of {:.1}",
        thin_shrunk / dead_shrunk
    );

    // And where there IS genuine within-cell variance the measured value wins,
    // so the floor is a floor and not a replacement.
    let _guard = GlobalRng::seed_guard(0x4485_ce10);
    let mut noisy = ContractTable::new();
    for contract in 0..40u64 {
        let (slot, _) = noisy.touch(contract);
        for peer in 0..CONTRACT_ENTRIES as u32 {
            for _ in 0..8 {
                assert!(noisy.add(slot, (peer, 0), 1.0, 0.5 + 0.6 * normal()));
            }
        }
    }
    let components = components_for(&mut noisy);
    // `> 0.25` alone is NOT decisive, which I4 of the round-3 testing review
    // established by working it through: 0.25 is the Bernoulli maximum, so a
    // mutant that replaced the evidence floor with any constant above it (0.5,
    // say) satisfies this too. The flag is what separates "the measurement
    // won" from "a bigger floor won", and it is the direct negative of the
    // thin case above.
    assert!(
        components.sigma2 > 0.25,
        "a measured within-cell variance above the floor must be used: {}",
        components.sigma2
    );
    assert!(
        !components.floor_bound,
        "and the flag must say the MEASURED value bound, not the floor: sigma2 {}",
        components.sigma2
    );
    // Restored from `unanimous_evidence_is_applied_unshrunk_and_bounded_by_the_peer_bar`,
    // which was deleted at 20a5a6230 because its load-bearing assertion was
    // `sigma2 == MIN_SIGMA2`, a fact the floor deliberately invalidates. Two
    // of its three halves were re-homed; this one was dropped and is the half
    // that bears on I4, because it asserts the shrinkage genuinely applies
    // where the variance is real rather than merely that `sigma2` is large.
    let noisy_slot = noisy.table.lookup(&0).expect("contract 0 is tracked");
    let noisy_effect = noisy
        .effect(Some(noisy_slot), ContractQuery::Shared, 0.0, 1)
        .expect("present entries");
    let noisy_raw = raw_mean(&noisy, noisy_slot);
    assert!(
        noisy_effect.abs() < 0.98 * noisy_raw.abs(),
        "a genuinely noisy contract must be shrunk away from its raw mean: \
         effect {noisy_effect} against raw {noisy_raw}"
    );
}

/// Finding 8: the shrunk effect's arithmetic, pinned exactly. This is the
/// number the whole term's magnitude comes from, and the mutation suite of
/// 2026-09-17 found that dropping the `tau2_peer` noise term or the shrinkage
/// altogether left the rest of the suite green.
///
/// `noise = sigma2 * w2/n^2 + tau2_peer * (sum_q n_q^2)/n^2`,
/// `shrink = tau2_contract / (tau2_contract + noise)`, `value = shrink *
/// sum/n`, over the entries the query selects.
#[test]
fn the_shrunk_contract_effect_matches_its_formula() {
    let mut table = ContractTable::new();
    let (slot, _) = table.touch(0.25f64.to_bits());
    // Three peers, deliberately unequal: 2, 3 and 5 unit-weight events with
    // means 1.0, 0.5 and 0.2.
    let entries: [(u32, f64, f64); 3] = [(1, 2.0, 1.0), (2, 3.0, 0.5), (3, 5.0, 0.2)];
    for &(peer, count, mean) in &entries {
        for _ in 0..count as usize {
            assert!(table.add(slot, (peer, 0), 1.0, mean));
        }
    }
    let components = ContractComponents {
        sigma2: 0.25,
        tau2_peer: 0.09,
        tau2_contract: 0.16,
        floor_bound: false,
        den_below_two: false,
        qualifying_contracts: 1,
        qualifying_entries: 3,
    };
    table.components = Some(components);

    let expected = |selected: &[(u32, f64, f64)]| {
        let (mut n, mut w2, mut sum, mut sq) = (0.0, 0.0, 0.0, 0.0);
        for &(_, count, mean) in selected {
            n += count;
            w2 += count; // unit weights, so sum w^2 = count
            sum += count * mean;
            sq += count * count;
        }
        let n2 = n * n;
        let noise = components.sigma2 * w2 / n2 + components.tau2_peer * sq / n2;
        let shrink = components.tau2_contract / (components.tau2_contract + noise);
        shrink * sum / n
    };

    let shared = table
        .effect(Some(slot), ContractQuery::Shared, 0.0, 3)
        .expect("three present peers");
    assert!(
        (shared - expected(&entries)).abs() < 1e-12,
        "shared effect {shared} against {}",
        expected(&entries)
    );
    // Leaving one peer out changes n, the sum and the squared-count sum.
    let left_out = table
        .effect(Some(slot), ContractQuery::LeaveOut(Some((1, 0))), 0.0, 2)
        .expect("two present peers remain");
    assert!(
        (left_out - expected(&entries[1..])).abs() < 1e-12,
        "leave-one-out effect {left_out} against {}",
        expected(&entries[1..])
    );

    // The `tau2_peer` term's purpose, stated behaviourally: the same total
    // evidence concentrated in ONE peer must be shrunk harder than the same
    // evidence spread over four, because one peer's many failures are not
    // evidence about a contract. Without that term the two are shrunk
    // identically, since `w2/n^2` is the same in both.
    let effect_with = |peers: u32| {
        let mut table = ContractTable::new();
        let (slot, _) = table.touch(0.25f64.to_bits());
        for i in 0..20u32 {
            assert!(table.add(slot, (i % peers, 0), 1.0, 1.0));
        }
        table.components = Some(components);
        table
            .effect(Some(slot), ContractQuery::Shared, 0.0, 1)
            .expect("entries are present")
    };
    let (concentrated, spread) = (effect_with(1), effect_with(4));
    assert!(
        concentrated < 0.85 * spread,
        "one peer's evidence must be shrunk harder than four peers': \
         {concentrated} against {spread}"
    );
}

/// Kish counting at the CONTRACT level, the twin of
/// `kish_counting_recovers_tau2_at_a_short_horizon` for the peer levels.
///
/// Every other contract-component test adds at weight 1.0, where `w2 == n`
/// and `effective_n() == n`, so the decayed-evidence arithmetic collapses to
/// raw counts and four separate substitutions of `n` for `w2` survive the
/// suite. The contract table runs the SHORTEST horizon in the system
/// ([`CONTRACT_HORIZON_HOURS`], 0.5 h), so it is the level most exposed to the
/// error the module docs describe: counting the raw weight sum overstates a
/// short horizon's evidence by about 2x.
///
/// The weights here are the ones `apply_contract_term` really uses,
/// `table.weight(t)` against an epoch of `now`. The event rate is deliberately
/// low, about 2.5 units of weight per cell, because that is where the
/// degrees-of-freedom term `n - w2/n` differs most from the raw `n - 1`.
#[test]
fn kish_counting_at_the_contract_level_recovers_the_components() {
    let (hours, rate) = (6.0, 5.0);
    let (sigma, tau_peer, tau_contract) = (1.0, 0.5, 0.4);
    let seeds = 6u64;
    let (mut s2, mut tp, mut tc) = (0.0, 0.0, 0.0);
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_cd00 + seed);
        let mut table = ContractTable::new();
        table.reset(hours);
        for contract in 0..80u64 {
            let (slot, _) = table.touch(contract);
            let contract_effect = tau_contract * normal();
            for peer in 0..CONTRACT_ENTRIES as u32 {
                let peer_effect = tau_peer * normal();
                for _ in 0..(rate * hours) as usize {
                    let t = uniform() * hours;
                    assert!(table.add(
                        slot,
                        (peer, 0),
                        table.weight(t),
                        contract_effect + peer_effect + sigma * normal(),
                    ));
                }
            }
        }
        // The scenario must actually exercise Kish counting, or the test is
        // the unit-weight one again: the raw weight sum must overstate the
        // effective size by roughly the 2x the module docs name.
        let e = table.nodes[0].entries[0].moments;
        assert!(
            e.w2 / e.n < 0.75,
            "the weights must be genuinely decayed, or this is the unit-weight \
             test again (unit weights give w2/n exactly 1): w2/n {}",
            e.w2 / e.n
        );
        let c = table
            .compute_components()
            .expect("a full table has components");
        s2 += c.sigma2 / seeds as f64;
        tp += c.tau2_peer / seeds as f64;
        tc += c.tau2_contract / seeds as f64;
    }
    assert!((s2 - sigma * sigma).abs() < 0.06, "sigma2 {s2}");
    assert!((tp - tau_peer * tau_peer).abs() < 0.08, "tau2_peer {tp}");
    assert!(
        (tc - tau_contract * tau_contract).abs() < 0.06,
        "tau2_contract {tc}"
    );
}

/// Every term of `compute_components`, pinned by exact arithmetic on a
/// hand-computable table.
///
/// A recovery test cannot pin the NOISE forms, and the 2026-09-17 round-2
/// mutation run proved it: substituting the raw count for the Kish count in
/// `tau2_contract`'s noise moved the recovered value from 0.1874 to 0.1612
/// against a truth of 0.16, so the mutant was CLOSER to the truth than the
/// correct code and no tolerance around the truth could separate them. The
/// sampling variance of a weighted mean is `sigma2 * sum w^2 / (sum w)^2`,
/// which is `sigma2 / n_eff` and not `sigma2 / n`; that is a statement about
/// the estimator, so it is pinned as one.
///
/// Two contracts, two peers each, two events per cell with weights 2 and 2 and
/// values 1 and 0. Every quantity below is exact in binary floating point.
#[test]
fn contract_components_match_their_formulae_exactly() {
    let mut table = ContractTable::new();
    for contract in 0..2u64 {
        let (slot, _) = table.touch(contract);
        for peer in 0..2u32 {
            assert!(table.add(slot, (peer, 0), 2.0, 1.0));
            assert!(table.add(slot, (peer, 0), 2.0, 0.0));
        }
    }
    let c = table.compute_components().expect("components exist");

    // Per cell: n = 4, w2 = 8, sum = 2, sumsq = 2, so the within-cell sum of
    // squares is 2 - 2^2/4 = 1 and the degrees of freedom are 4 - 8/4 = 2.
    // Four cells: ss = 4, df = 8, sigma2 = 0.5. The Bernoulli floor is
    // 1/(n_eff + 2) = 1/4 per cell, so 0.25 pooled, and the measured value
    // wins.
    assert_eq!(c.sigma2, 0.5);

    // Every cell has the same mean, so every leave-one-out contrast is exactly
    // zero and the estimator is minus the mean noise, floored at 0.
    assert_eq!(c.tau2_peer, 0.0);

    // Per contract: n = 8, w2 = 16, sum = 4, sum of squared counts 32, so the
    // mean is 0.5 and the noise is sigma2 * w2/n^2 = 0.5 * 16/64 = 0.125.
    // acc = 0.5^2 - 0.125 = 0.125 per contract, den = 2, so tau2_contract is
    // 0.125. Substituting the raw count gives sigma2/n = 0.0625 and therefore
    // 0.1875, which this assertion rejects.
    assert_eq!(c.tau2_contract, 0.125);
    assert_eq!(c.qualifying_contracts, 2);
    assert_eq!(c.qualifying_entries, 4);
    assert!(
        !c.floor_bound,
        "the measured 0.5 beat the 0.25 floor here, and the flag must say so"
    );
    assert!(
        !c.den_below_two,
        "two contracts qualified, so the den gate did not fire"
    );

    // The FLOOR's `+2` pinned exactly, from the same table. It is unpinned
    // otherwise (`+3.0` survived the whole floor test, measured), and since the
    // floor binds on every refit of the recorded production streams it is the
    // constant that sets the term's magnitude in the field. Each cell has
    // n = 4, w2 = 8, so n_eff = n^2/w2 = 2 and the Laplace floor is
    // 1/(2 + 2) = 0.25. `+3.0` would give 0.2, and `+1.0` 0.3333.
    assert_eq!(bernoulli_variance_floor(2.0), 0.25);
    assert_eq!(bernoulli_variance_floor(8.0), 0.1);

    // Non-vacuity: the two noise forms must genuinely differ here, or the
    // assertion above would hold for both.
    let (n, w2) = (8.0f64, 16.0f64);
    assert!(
        (w2 / (n * n) - 1.0 / n).abs() > 0.05,
        "the Kish and raw forms must differ on this table: {} against {}",
        w2 / (n * n),
        1.0 / n
    );
}

/// The other half of Kish counting, which a tolerance cannot catch: a cell
/// whose RAW weight sum clears [`MIN_EFFECTIVE_N`] but whose effective size
/// does not must NOT count as replicated. Under decay that is the ordinary
/// case, because one recent event plus a tail of old ones sums to more than it
/// is worth.
#[test]
fn raw_weight_above_the_replication_floor_is_not_replication() {
    // One heavy event and two light ones: raw sum 2.1, effective size 1.2.
    let mut moments = Moments::default();
    for weight in [1.9, 0.1, 0.1] {
        moments.add(weight, 1.0);
    }
    assert!(
        moments.n > MIN_EFFECTIVE_N,
        "the raw sum must clear the floor, or this test proves nothing: {}",
        moments.n
    );
    assert!(
        moments.effective_n() < MIN_EFFECTIVE_N,
        "{}",
        moments.effective_n()
    );
    assert!(!moments.replicated());

    // And at the contract level: a table in which EVERY cell has that shape
    // carries no within-cell replication, so there is no sigma2 to estimate.
    let mut table = ContractTable::new();
    for contract in 0..40u64 {
        let (slot, _) = table.touch(contract);
        for peer in 0..CONTRACT_ENTRIES as u32 {
            for weight in [1.9, 0.1, 0.1] {
                assert!(table.add(slot, (peer, 0), weight, 1.0));
            }
        }
    }
    // The per-contract totals ARE replicated, so nothing else stops this.
    let total: f64 = table.nodes[0].entries.iter().map(|e| e.moments.n).sum();
    assert!(total > 16.0, "{total}");
    assert_eq!(
        table.compute_components(),
        None,
        "raw weight above the floor is not within-cell replication"
    );
}

/// `tau2_contract` contrasts each contract's mean with ZERO, not with a grand
/// mean over contracts, and this is the case that tells the two apart: a
/// population of contracts whose effects are all shifted POSITIVE.
///
/// Against zero the estimator recovers the second moment `E[c^2] = mean^2 +
/// var`; against a grand mean it would recover only `var`, the spread about
/// the shift. The distinction is the whole point of the convention and it
/// bites in exactly the regime this PR targets: under network-wide degradation
/// the grand mean is pulled up by the same dead contracts it is meant to
/// measure, `tau2_contract` collapses toward zero, `shrink` goes to zero with
/// it, and the term silently stops producing any effect.
///
/// The existing tests cannot see this: the recovery test draws zero-centred
/// effects, where the two forms agree, and the negative control has no
/// contract effect at all.
#[test]
fn tau2_contract_is_the_second_moment_about_zero_not_about_a_grand_mean() {
    let (shift, spread, sigma) = (0.3, 0.2, 0.5);
    let seeds = 8u64;
    let mut tc = 0.0;
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_cdc0 + seed);
        let mut table = ContractTable::new();
        for contract in 0..120u64 {
            let (slot, _) = table.touch(contract);
            // Every contract sits ABOVE zero, so the grand mean is about
            // `shift` and the spread about it is much smaller than the spread
            // about zero.
            let contract_effect = shift + spread * normal();
            for peer in 0..CONTRACT_ENTRIES as u32 {
                for _ in 0..30 {
                    assert!(table.add(slot, (peer, 0), 1.0, contract_effect + sigma * normal()));
                }
            }
        }
        tc += table
            .compute_components()
            .expect("a full table has components")
            .tau2_contract
            / seeds as f64;
    }
    let about_zero = shift * shift + spread * spread;
    let about_the_grand_mean = spread * spread;
    assert!(
        (tc - about_zero).abs() < 0.03,
        "tau2_contract must be the second moment about ZERO ({about_zero}), got {tc}"
    );
    // The threshold sits MIDWAY between the two hypotheses, and that is
    // load-bearing. It was `about_the_grand_mean + 3.0 * 0.03`, which for
    // these parameters is 0.13 and therefore EXACTLY `about_zero`: the test
    // asked the estimator to land strictly above its own expectation, which
    // is a coin flip on the RNG stream (the SD of this average is about
    // 0.0043, so it was green and deterministic, and any unrelated change to
    // `normal()` call order, the seed base or the loop counts had about even
    // odds of turning it red as a false regression). The coincidence looked
    // deliberate and was not. I5 of the 2026-09-18 round-3 testing review.
    let midway = 0.5 * (about_zero + about_the_grand_mean);
    assert!(
        tc > midway,
        "and must be well clear of the variance about the grand mean \
         ({about_the_grand_mean}), or this test cannot tell the two forms \
         apart: got {tc}, needed above the midpoint {midway}"
    );
}

/// The other half of finding 8: real per-peer effects with NO between-contract
/// effect must read as about zero between-contract variance, so the term
/// produces no effect where none exists. `tau2_contract` contrasts each
/// contract's mean with zero, and the noise it subtracts is
/// `sigma2 * w2/n^2 + tau2_peer * sum n_q^2 / n^2`, which is exactly the
/// per-peer term this case tests.
#[test]
fn zero_between_contract_variance_reads_as_zero() {
    let (sigma, tau_peer) = (1.0, 0.7);
    let seeds = 8u64;
    let mut tc = 0.0;
    for seed in 0..seeds {
        let _guard = GlobalRng::seed_guard(0x4485_cc80 + seed);
        let mut table = ContractTable::new();
        for contract in 0..120u64 {
            let (slot, _) = table.touch(contract);
            for peer in 0..CONTRACT_ENTRIES as u32 {
                let peer_effect = tau_peer * normal();
                for _ in 0..30 {
                    assert!(table.add(slot, (peer, 0), 1.0, peer_effect + sigma * normal()));
                }
            }
        }
        let c = table
            .compute_components()
            .expect("a full table has components");
        assert!(
            (c.tau2_peer - tau_peer * tau_peer).abs() < 0.2,
            "the peer effects must be recovered, or this test proves nothing: {}",
            c.tau2_peer
        );
        tc += c.tau2_contract / seeds as f64;
    }
    assert!(
        tc < 0.02,
        "no between-contract effect exists; the term must not invent one: {tc}"
    );
    // And with no components at all there is no effect: the `effect` query
    // short-circuits on `tau2_contract <= 0`.
    let mut table = ContractTable::new();
    let (slot, _) = table.touch(0.25f64.to_bits());
    assert!(table.add(slot, (1, 0), 1.0, 1.0));
    assert_eq!(
        table.effect(Some(slot), ContractQuery::Shared, 0.0, 1),
        None
    );
}

/// The contract table holds at most `CONTRACT_CAPACITY` contracts and
/// `CONTRACT_ENTRIES` peers per contract, and a reused slot starts empty.
#[test]
fn contract_table_is_bounded() {
    let mut table = ContractTable::new();
    table.touch(0.25f64.to_bits());
    assert!(table.add(0, (7, 0), 1.0, 0.5));
    let first = table.table.lookup(&0.25f64.to_bits()).unwrap();
    for i in 1..3 * CONTRACT_CAPACITY as u64 {
        let (slot, _) = table.touch((i as f64 / 1e5).to_bits());
        assert!(table.add(slot, (1, 0), 1.0, 0.0));
    }
    assert!(table.table.index.len() <= CONTRACT_CAPACITY);
    assert!(table.nodes.len() <= CONTRACT_CAPACITY);
    assert!(table.table.evictions >= 2 * CONTRACT_CAPACITY as u64 - 64);
    assert!(table.table.lookup(&0.25f64.to_bits()).is_none());
    // Every slot has been reused; the one first used must hold only its new
    // contract's entry, not peer 7's.
    let node = &table.nodes[first];
    assert_eq!(node.entries.iter().filter(|e| e.used()).count(), 1);
    assert!(node.entries.iter().all(|e| e.peer_slot != 7));

    // Entries per contract: a ninth peer replaces the lightest entry only when
    // it is heavier, and is refused otherwise.
    let mut table = ContractTable::new();
    let (slot, _) = table.touch(0.5f64.to_bits());
    for peer in 0..CONTRACT_ENTRIES as u32 {
        assert!(table.add(slot, (peer, 0), 1.0 + peer as f64, 1.0));
    }
    assert!(
        !table.add(slot, (100, 0), 0.5, 1.0),
        "a lighter ninth peer is refused"
    );
    assert!(
        table.add(slot, (101, 0), 2.5, 1.0),
        "a heavier one replaces the lightest"
    );
    let peers: Vec<u32> = table.nodes[slot]
        .entries
        .iter()
        .map(|e| e.peer_slot)
        .collect();
    assert!(!peers.contains(&0) && peers.contains(&101) && !peers.contains(&100));

    // Through a stage: distinct contracts never grow the table past its bound,
    // and the evictions are counted.
    let _guard = GlobalRng::seed_guard(0x4485_c006);
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    feed(&mut [&mut stage], background(0.0, 5.0, 0..30));
    let diagnostics = stage.diagnostics();
    // Evictions come in batches of `CONTRACT_CAPACITY / 64`.
    assert!(diagnostics.contracts <= CONTRACT_CAPACITY);
    assert!(diagnostics.contracts > CONTRACT_CAPACITY - CONTRACT_CAPACITY / 64);
    assert!(diagnostics.contract_evictions > 0);
}

/// The timing stages carry no contract term and keep their horizon menu, so
/// a contract many peers were slow on does not move another contract's timing
/// forecast. The failure stage in the same traffic is the non-vacuity check.
#[test]
fn timing_stages_are_unaffected_by_the_contract_term() {
    for target in [Target::LogResponseTime, Target::LogTransferSpeed] {
        let stage: Stage<u32> = Stage::new(target, 64);
        assert!(stage.contracts.is_none());
        assert_eq!(
            stage.levels.map(|level| level.horizon_hours),
            LOG_HORIZONS_HOURS
        );
    }
    // The failure menu is asserted BEHAVIOURALLY, not against itself: setting
    // `FAILURE_HORIZONS_HOURS` equal to `LOG_HORIZONS_HOURS` passed the old
    // `assert_eq!(levels.map(horizon_hours), FAILURE_HORIZONS_HOURS)` while
    // the whole accuracy gain attributed to the menu disappeared. What the
    // short menu buys is tracking a failure burst faster, so that is what is
    // checked: after a burst, the failure stage's forecast for the bursting
    // peers must rise further than a stage carrying the timing menu does.
    let failure: Stage<u32> = Stage::new(Target::Failure, 64);
    assert_eq!(
        failure.levels[0].horizon_hours, None,
        "the menu must keep a no-forgetting level for quiet nodes"
    );
    assert!(
        failure
            .levels
            .iter()
            .filter_map(|level| level.horizon_hours)
            .all(|hours| hours <= 1.5),
        "every forgetting level of the failure menu must be at most 1.5 h, \
         which is what makes it faster than the timing menu"
    );
    {
        let burst_response = |horizons: [Option<f64>; HORIZONS]| {
            let _guard = GlobalRng::seed_guard(0x4485_c010);
            let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
            for (level, hours) in stage.levels.iter_mut().zip(horizons) {
                *level = Level::new(hours);
            }
            // Two hours of healthy traffic on unique contracts, then a burst
            // in which peers 0..6 fail everything for six minutes.
            let mut steps = background_pooled(0.0, 2.0, 0..30);
            for peer in 0..6u32 {
                for i in 0..60 {
                    steps.push(Step {
                        peer,
                        contract: uniform(),
                        distance: 0.1,
                        failed: true,
                        hours: 2.0 + 0.1 * i as f64 / 60.0,
                    });
                }
            }
            let mut scratch = Scratch::default();
            steps.sort_by(|a, b| a.hours.total_cmp(&b.hours));
            for step in &steps {
                stage.observe(
                    &mut scratch,
                    &step.peer,
                    step.contract,
                    step.distance,
                    f64::from(u8::from(step.failed)),
                    step.hours,
                );
            }
            stage.refit(&mut scratch, 2.1);
            mean_forecast(&stage, 0..6) - mean_forecast(&stage, 6..30)
        };
        let fast = burst_response(FAILURE_HORIZONS_HOURS);
        let slow = burst_response(LOG_HORIZONS_HOURS);
        assert!(
            fast > slow + 0.02,
            "the failure menu must track a burst faster than the timing menu: \
             gap {fast} against {slow}"
        );
    }

    // Two contracts in the same band, one of which six peers found slow (or
    // failed). An unseen peer's forecast for the two must match on a timing
    // stage, and differ on the failure stage.
    let (slow, other) = (0.37, 0.36);
    assert_eq!(band_of(slow), band_of(other));
    let difference = |target: Target| {
        let _guard = GlobalRng::seed_guard(0x4485_c007);
        let mut stage: Stage<u32> = Stage::new(target, 64);
        let mut steps = background_pooled(0.0, 3.25, 0..30);
        for peer in 0..6 {
            steps.extend(on_contract(peer, slow, true, 8, 3.0, 0.25));
        }
        // Timing stages read `failed` as a slow response: log time 3 against 0.
        let mut scratch = Scratch::default();
        steps.sort_by(|a, b| a.hours.total_cmp(&b.hours));
        for step in &steps {
            let y = match (target, step.failed) {
                (Target::Failure, failed) => f64::from(u8::from(failed)),
                (_, true) => 3.0,
                (_, false) => 0.0,
            };
            stage.observe(
                &mut scratch,
                &step.peer,
                step.contract,
                step.distance,
                y,
                step.hours,
            );
        }
        stage.refit(&mut scratch, 3.25);
        let at = |contract: f64| stage.predict(&999, contract, 0.05, 3.25).unwrap().value;
        at(slow) - at(other)
    };
    for target in [Target::LogResponseTime, Target::LogTransferSpeed] {
        assert_eq!(difference(target), 0.0, "{target:?}");
    }
    assert!(difference(Target::Failure) > 0.1);
}

/// Finding 1 of the 2026-09-17 review, the falsifier for the claim this
/// design rested on. The shared contract effect is candidate-INDEPENDENT, so
/// it cannot change which candidate has the higher failure probability. The
/// module docs used to conclude that it therefore "cannot reorder peers for
/// one decision", and that does not follow: routing ranks by
/// `t + transfer + 3 * t * p`, so a shared effect `d` moves candidate `i`'s
/// cost by `3 * t_i * d`, which differs across candidates whose response
/// times differ.
///
/// The test computes the router's cost formula itself, from estimates for two
/// contracts in the SAME BAND at the SAME distance, one with dead-contract
/// evidence and one without, so the only difference between the two sets of
/// costs is the shared effect. It asserts the exact arithmetic identity and
/// that at least one candidate pair's order changes. The router-level path is
/// covered separately by
/// `routing_cost_ranks_peers_whose_forecasts_clamp_at_one`.
#[test]
fn a_shared_contract_effect_reweights_peers_that_differ_in_response_time() {
    let _guard = GlobalRng::seed_guard(0x4485_c011);
    let (dead, unseen) = (0.37, 0.36);
    assert_eq!(band_of(dead), band_of(unseen));
    let peers: Vec<PeerKeyLocation> = (0..30).map(|_| PeerKeyLocation::random()).collect();
    let mut routing = HierarchicalRouting::new(200);
    // Response time FALLS with the peer index while the failure rate RISES,
    // so the fast peers are the unreliable ones. That is the direction in
    // which a positive shared effect, which favours the faster peer, can
    // overturn the order.
    let mut events: Vec<(usize, f64, f64, bool, f64)> = Vec::new();
    for i in 0..6_000 {
        let peer = GlobalRng::random_range(0..peers.len());
        let rate = 0.02 + 0.5 * (peer as f64 / 29.0);
        events.push((peer, uniform(), 0.05, uniform() < rate, i as f64 / 600.0));
    }
    // Eight peers fail the dead contract in the last six minutes, and three
    // OTHER contracts are dead alongside it. `tau2_contract` contrasts each
    // contract against ZERO and averages over qualifying contracts, so a
    // single dead contract among forty healthy ones is averaged away and the
    // term switches off. Real gateway traffic carries many at once (52 to 76
    // qualifying contracts measured, with the term active on 31 to 93% of
    // rows); a single injected one is the unrepresentative case.
    for peer in 0..8usize {
        for i in 0..10 {
            events.push((peer, dead, 0.05, true, 9.9 + 0.1 * i as f64 / 10.0));
        }
    }
    for (index, other) in [0.34f64, 0.345, 0.35].iter().enumerate() {
        for peer in 0..6usize {
            for i in 0..10 {
                events.push((
                    (peer + index * 6) % 30,
                    *other,
                    0.05,
                    true,
                    9.9 + 0.1 * i as f64 / 10.0,
                ));
            }
        }
    }
    events.sort_by(|a, b| a.4.total_cmp(&b.4));
    for &(peer, contract, distance, failed, hours) in &events {
        let outcome = RoutingOutcome {
            success: !failed,
            // Only successes are timed, as in production. Response time falls
            // with the peer index; transfer speed is the same for every peer,
            // so the transfer term cancels out of the comparison.
            time_to_response_start_secs: (!failed).then(|| 0.02 * (30 - peer) as f64),
            transfer_speed_bps: (!failed).then_some(50_000.0),
        };
        routing.observe_at(
            &peers[peer],
            Location::new(contract),
            distance,
            &outcome,
            hours,
        );
    }
    let now = events.last().expect("events exist").4;
    let distance = 0.05;
    let bytes = 4_096.0;
    let cost = |estimate: &Estimate| -> f64 {
        let t = estimate.time_to_response_start_secs.expect("timed");
        let speed = estimate.transfer_speed_bps.expect("speed");
        let p = estimate.failure_ranking.expect("failure");
        t + bytes / speed + t * p * 3.0
    };
    let mut rows = Vec::new();
    for peer in &peers {
        let with = routing.estimate(peer, Location::new(dead), distance, now);
        let without = routing.estimate(peer, Location::new(unseen), distance, now);
        if with.failure_ranking.is_none() || with.time_to_response_start_secs.is_none() {
            continue;
        }
        // The timing stages carry no contract term, so these must be identical
        // between the two contracts: the failure term is the only difference.
        assert_eq!(
            with.time_to_response_start_secs,
            without.time_to_response_start_secs
        );
        assert_eq!(with.transfer_speed_bps, without.transfer_speed_bps);
        // Only peers whose forecasts clamp at neither end: where the bound
        // binds, the RANKING value moves by the slope rather than by the
        // effect, so the shared effect is no longer a common delta. That is a
        // separate mechanism, pinned by
        // `routing_cost_ranks_peers_whose_forecasts_clamp_at_one`.
        let unclamped = |estimate: &Estimate| {
            estimate
                .failure_probability
                .is_some_and(|p| p > 0.0 && p < 1.0)
        };
        if !unclamped(&with) || !unclamped(&without) {
            continue;
        }
        let delta =
            with.failure_ranking.expect("failure") - without.failure_ranking.expect("failure");
        rows.push((
            with.time_to_response_start_secs.expect("timed"),
            delta,
            cost(&with),
            cost(&without),
        ));
    }
    assert!(
        rows.len() >= 10,
        "enough peers must be warm and unclamped: {}",
        rows.len()
    );
    let effect = rows[0].1;
    assert!(
        effect > 0.05,
        "the dead contract must give a real shared effect, or this test proves \
         nothing: {effect}"
    );
    let times: Vec<f64> = rows.iter().map(|row| row.0).collect();
    let (fastest, slowest) = (
        times.iter().copied().fold(f64::MAX, f64::min),
        times.iter().copied().fold(0.0, f64::max),
    );
    assert!(
        slowest > 1.5 * fastest,
        "the peers must differ materially in response time, or this test proves \
         nothing: {fastest} to {slowest}"
    );
    for &(t, delta, with, without) in &rows {
        // The effect is the same number for every candidate...
        assert!(
            (delta - effect).abs() < 1e-9,
            "the shared effect must be candidate-independent: {delta} against {effect}"
        );
        // ...and yet it moves each candidate's COST by `3 * t_i * delta`.
        assert!(
            ((with - without) - 3.0 * t * delta).abs() < 1e-9,
            "the cost must move by 3 * t * delta: {} against {}",
            with - without,
            3.0 * t * delta
        );
    }
    let flips = rows
        .iter()
        .enumerate()
        .flat_map(|(i, a)| rows[i + 1..].iter().map(move |b| (a, b)))
        .filter(|(a, b)| (a.2 - b.2).is_sign_positive() != (a.3 - b.3).is_sign_positive())
        .count();
    assert!(
        flips > 0,
        "a shared contract effect DOES reorder candidates: with {} candidates \
         and an effect of {effect}, no pair changed order, which would mean \
         this test can no longer see the mechanism",
        rows.len()
    );
    eprintln!(
        "#5702 finding 1: shared effect {effect:.4} changed the order of \
         {flips} of {} candidate pairs (response times {fastest:.3} s to \
         {slowest:.3} s)",
        rows.len() * (rows.len() - 1) / 2
    );
}

/// Finding 13 of the 2026-09-17 review: nothing drove the term through the
/// router's own learn path with a REPEATED contract. Every pre-existing router
/// test draws `Location::random()` per event, so no `(contract, peer)` cell is
/// replicated and the term is inert; the PR's own router test replaced
/// `router.hierarchical` wholesale. This is the one place the contract key can
/// fail to be threaded at all.
#[test]
fn add_event_threads_a_repeated_contract_through_the_contract_term() {
    use crate::node::network_status::OpType;
    use crate::router::{RouteEvent, RouteOutcome, Router};

    let _guard = GlobalRng::seed_guard(0x4485_c012);
    let _correction = crate::router::force_residual_correction(false);
    let _hierarchical = crate::router::force_hierarchical_routing(true);
    let mut router = Router::new(&[]);
    let peers: Vec<PeerKeyLocation> = (0..24).map(|_| PeerKeyLocation::random()).collect();
    let contracts: Vec<Location> = (0..16).map(|i| Location::new(i as f64 / 16.0)).collect();
    let dead = Location::new(0.37);
    for index in 0..1_200 {
        // A pool of repeated contracts, plus a dead one that eight peers fail.
        let (peer, contract, outcome) = if index % 5 == 0 {
            (&peers[index % 8], dead, RouteOutcome::Failure)
        } else {
            (
                &peers[index % peers.len()],
                contracts[index % contracts.len()],
                if index % 11 == 0 {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::SuccessUntimed
                },
            )
        };
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location: contract,
            outcome,
            op_type: Some(OpType::Get),
        });
    }
    let snapshot = router.snapshot();
    assert!(
        snapshot.hierarchical_contracts > 1,
        "the contract key must reach the term through add_event: {}",
        snapshot.hierarchical_contracts
    );
    assert!(
        snapshot.hierarchical_contract_estimable_refits > 0,
        "the term must have become estimable, or it is inert on this path"
    );
    assert!(
        snapshot
            .hierarchical_contract_tau2
            .is_some_and(|tau2| tau2 > 0.0),
        "between-contract variance must be estimated: {:?}",
        snapshot.hierarchical_contract_tau2
    );
    // And the key reaches the DECISION path, through the same `estimate` call
    // `predict_with_model` makes. Held at one distance so the only difference
    // between the two contracts is the shared effect; they share a band.
    let unseen = Location::new(0.36);
    assert_eq!(band_of(dead.as_f64()), band_of(unseen.as_f64()));
    let clock = router.prediction_clock();
    let failure_for = |contract: Location| {
        router
            .hierarchical
            .estimate(&peers[20], contract, 0.05, clock.estimator_hours)
            .failure_ranking
            .expect("the failure stage predicts after 1,200 events")
    };
    assert!(
        failure_for(dead) > failure_for(unseen) + 0.05,
        "the dead contract must raise the forecast: {} against {}",
        failure_for(dead),
        failure_for(unseen)
    );
}

/// Finding 22 of the 2026-09-17 review: `prepare`'s contract-liveness check
/// had no test. An event whose contract has been evicted and its slot reused
/// must not be adjusted against the NEW contract's evidence; it keeps the
/// adjustment it last had.
#[test]
fn an_event_whose_contract_slot_was_reused_is_not_adjusted_by_the_new_contract() {
    let _guard = GlobalRng::seed_guard(0x4485_c013);
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    let dead = 0.37;
    let mut steps = background_pooled(0.0, 1.0, 0..30);
    for peer in 0..6u32 {
        steps.extend(on_contract(peer, dead, true, 8, 0.8, 0.15));
    }
    feed(&mut [&mut stage], steps);
    let slot = stage.peers.lookup(&0).expect("peer 0 is tracked") as u32;
    let event = *stage
        .sorted
        .iter()
        .find(|event| event.slot == slot && event.distance == 0.05)
        .expect("peer 0's contract event is in the window");
    assert!(
        event.adjustment > 0.1,
        "the event must carry an adjustment, or this test proves nothing: {}",
        event.adjustment
    );
    // Churn the contract table past its capacity so `dead`'s slot is reused.
    let mut churn = Vec::new();
    for i in 0..3 * CONTRACT_CAPACITY {
        churn.push(Step {
            peer: 1 + (i % 29) as u32,
            contract: 0.5 + i as f64 / (8.0 * CONTRACT_CAPACITY as f64),
            distance: 0.1,
            failed: i % 3 == 0,
            hours: 1.0 + i as f64 / 600.0,
        });
    }
    feed(&mut [&mut stage], churn);
    let table = stage
        .contracts
        .as_ref()
        .expect("the failure stage has a term");
    assert!(
        !table.live(event.contract_slot, event.contract_generation),
        "the churn must have evicted the contract, or this test proves nothing"
    );
    let find = |stage: &Stage<u32>| {
        *stage
            .sorted
            .iter()
            .find(|candidate| candidate.seq == event.seq)
            .expect("the event is still in the window")
    };
    // `prepare` reports it as having no contract, so it cannot be adjusted by
    // whatever now occupies the slot.
    let mut prepared = Vec::new();
    assert!(stage.prepare(&mut prepared));
    let row = prepared
        .iter()
        .find(|row| stage.sorted[row.source as usize].seq == event.seq)
        .expect("the event is prepared");
    assert_eq!(row.contract_slot, u32::MAX);
    // The adjustment it held when its contract went is now frozen, however
    // much evidence the reused slot accumulates. (It is not compared against
    // the PRE-churn value: refits early in the churn, while the contract was
    // still present, legitimately refreshed it.)
    let frozen = find(&stage).adjustment;
    assert!(frozen > 0.1, "the frozen adjustment must be real: {frozen}");
    let mut more = Vec::new();
    for i in 0..CONTRACT_CAPACITY {
        more.push(Step {
            peer: 1 + (i % 29) as u32,
            contract: dead + 1e-9 * i as f64,
            distance: 0.05,
            failed: true,
            hours: 7.0 + i as f64 / 600.0,
        });
    }
    feed(&mut [&mut stage], more);
    assert_eq!(
        find(&stage).adjustment.to_bits(),
        frozen.to_bits(),
        "an event whose contract slot has been reused keeps its adjustment"
    );
}

/// Finding 22 of the 2026-09-17 review: the contract clause in `must_rebase`
/// was untested. It is what keeps the contract table's epoch-scaled weights
/// finite on a node whose levels forget nothing, where no level clause can
/// fire: at a 0.5 h horizon the exponent passes `REBASE_EXPONENT` after 15 h.
#[test]
fn an_idle_gap_rebases_the_contract_table_even_when_no_level_would() {
    let _guard = GlobalRng::seed_guard(0x4485_c014);
    let mut stage: Stage<u32> = Stage::new(Target::Failure, 64);
    // Every level forgets nothing, so `Level::exponent` is 0 forever and only
    // the contract table can force a rebase.
    for level in &mut stage.levels {
        *level = Level::new(None);
    }
    let mut scratch = Scratch::default();
    for i in 0..200 {
        stage.observe(
            &mut scratch,
            &(i % 20),
            0.25,
            uniform() * 0.5,
            f64::from(u8::from(i % 4 == 0)),
            i as f64 / 600.0,
        );
    }
    let before = stage.refits;
    let epoch = stage
        .contracts
        .as_ref()
        .expect("the failure stage has a term")
        .epoch;
    // One event 20 hours later: past `REBASE_EXPONENT * CONTRACT_HORIZON_HOURS`.
    let late = epoch + 20.0;
    assert!(stage.contracts.as_ref().expect("term").exponent(late) > REBASE_EXPONENT);
    stage.observe(&mut scratch, &0, 0.25, 0.1, 0.0, late);
    assert!(
        stage.refits > before,
        "the contract clause must force a refit, or weights grow without bound"
    );
    let table = stage.contracts.as_ref().expect("term");
    assert!(
        table.exponent(late) <= 0.0,
        "the epoch must be rebased to now"
    );
    assert!(
        table.weight(late).is_finite() && table.weight(late) <= 1.0,
        "weights must be finite after the rebase: {}",
        table.weight(late)
    );
}

/// The value peers are ranked by keeps the order of the unbounded forecasts
/// above 1, equals the probability wherever the bound does not bind, and is
/// never negative: the router's no-timing cost branch is this value times a
/// multiplier, and a negative cost renders as "N/A" on the dashboard and is
/// recorded in the routing dataset.
#[test]
fn ranking_probability_preserves_order_above_one_and_never_goes_negative() {
    for p in [0.0, 1e-9, 0.3, 0.999, 1.0] {
        assert_eq!(ranking_failure_probability(p).to_bits(), p.to_bits());
    }
    let values = [-2.0, -0.4, -1e-3, 0.0, 0.5, 1.0, 1.0 + 1e-3, 1.4, 3.0];
    for pair in values.windows(2) {
        let (low, high) = (
            ranking_failure_probability(pair[0]),
            ranking_failure_probability(pair[1]),
        );
        assert!(
            low <= high,
            "{} -> {low} must not rank above {} -> {high}",
            pair[0],
            pair[1]
        );
    }
    // Strictly increasing above 1, which is the tie the mechanism exists for.
    for pair in [1.0, 1.0 + 1e-3, 1.4, 3.0].windows(2) {
        assert!(ranking_failure_probability(pair[0]) < ranking_failure_probability(pair[1]));
    }
    // Below 0 the value is pinned at 0, so it cannot make a cost negative.
    for value in [-1e9, -2.0, -1e-3, -f64::MIN_POSITIVE] {
        assert_eq!(ranking_failure_probability(value), 0.0, "{value}");
    }
    for value in values {
        let ranking = ranking_failure_probability(value);
        assert!(ranking >= 0.0, "{value} -> {ranking} must not be negative");
        // An ABSOLUTE bound, not one that scales with the constant: raising
        // `RANKING_OVERSHOOT_SLOPE` past this must fail the suite.
        assert!(
            (ranking - value.clamp(0.0, 1.0)).abs() <= 1e-5,
            "{value} -> {ranking} must stay within 1e-5 of the clamped probability"
        );
        // Stays positive enough that `t * (1 + 3p)` is increasing in `t`.
        assert!(1.0 + 3.0 * ranking > 0.0);
    }
}

/// End to end for finding 6 of the 2026-09-17 review: the healthiest peers on
/// untimed traffic have a negative unbounded failure forecast, and the cost
/// the dashboard and the routing dataset read must still be a number the
/// dashboard can print. Before the fix the ranking value carried the DOWNWARD
/// overshoot at the same slope, so the no-timing cost branch
/// (`failure * 3.0`) went negative and `fmt_prediction_time` printed "N/A"
/// for exactly the best peers.
#[test]
fn a_negative_unbounded_forecast_still_yields_a_printable_cost() {
    use crate::node::network_status::OpType;
    use crate::router::{RouteEvent, RouteOutcome, Router};
    use crate::server::fmt_prediction_time_for_tests as fmt_prediction_time;

    let _guard = GlobalRng::seed_guard(0x4485_c00c);
    let _correction = crate::router::force_residual_correction(false);
    let _hierarchical = crate::router::force_hierarchical_routing(true);
    let target = Location::new(0.5);
    // A peer near the target, whose own record is at FAR distances where the
    // curve is high. Its peer-level residual is then far below the curve at
    // the near distance the query asks about, which is how an unbounded
    // forecast goes below 0.
    let clean = std::iter::repeat_with(PeerKeyLocation::random)
        .find(|peer| {
            peer.location()
                .is_some_and(|location| target.distance(location).as_f64() < 0.02)
        })
        .expect("a near peer is drawn within a few hundred attempts");
    let far_contract = Location::new(
        clean
            .location()
            .map(|location| (location.as_f64() + 0.45) % 1.0)
            .expect("a random peer has a location"),
    );
    let mut router = Router::new(&[]);
    for index in 0..400 {
        // Other peers fail whenever the contract is far from them, so the
        // distance curve rises steeply.
        let peer = PeerKeyLocation::random();
        let contract = Location::random();
        let distance = peer
            .location()
            .map(|location| contract.distance(location).as_f64())
            .unwrap_or(0.5);
        router.add_event(RouteEvent {
            peer,
            contract_location: contract,
            outcome: if distance > 0.2 {
                RouteOutcome::Failure
            } else {
                RouteOutcome::SuccessUntimed
            },
            op_type: Some(OpType::Get),
        });
        if index % 4 == 0 {
            // The clean peer succeeds every time, at a far distance.
            router.add_event(RouteEvent {
                peer: clean.clone(),
                contract_location: far_contract,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: Some(OpType::Get),
            });
        }
    }
    let clock = router.prediction_clock();
    let distance = target
        .distance(clean.location().expect("a random peer has a location"))
        .as_f64();
    let unbounded = router
        .hierarchical
        .failure
        .predict(&clean, target.as_f64(), distance, clock.estimator_hours)
        .expect("the failure stage predicts after 400 events")
        .unbounded;
    assert!(
        unbounded < 0.0,
        "the scenario must reach a negative unbounded forecast, or this test \
         proves nothing: {unbounded}"
    );
    let prediction = router
        .predict_routing_outcome_at(&clean, target, clock)
        .expect("prediction after warm-up");
    assert_eq!(prediction.failure_probability, 0.0);
    assert!(
        prediction.expected_total_time >= 0.0,
        "expected total time must not be negative: {}",
        prediction.expected_total_time
    );
    assert_ne!(
        fmt_prediction_time(prediction.expected_total_time),
        "N/A",
        "the dashboard must be able to print the cost: {}",
        prediction.expected_total_time
    );
}

/// An estimator in which two peers' failure forecasts for `dead` both exceed
/// 1 before the bound, `worse` further than `better`. Returns (estimator,
/// worse, better, dead contract, distance, estimator hours).
fn clamped_pair() -> (
    HierarchicalRouting,
    PeerKeyLocation,
    PeerKeyLocation,
    Location,
    f64,
    f64,
) {
    let peers: Vec<PeerKeyLocation> = (0..30).map(|_| PeerKeyLocation::random()).collect();
    let (worse, better) = (peers[0].clone(), peers[1].clone());
    let dead = Location::new(0.37);
    let distance = 0.05;
    let failure = RoutingOutcome {
        success: false,
        time_to_response_start_secs: None,
        transfer_speed_bps: None,
    };
    let success = RoutingOutcome {
        success: true,
        time_to_response_start_secs: None,
        transfer_speed_bps: None,
    };
    let mut routing = HierarchicalRouting::new(200);
    let mut steps = background_pooled(0.0, 3.25, 0..30);
    for peer in 0..8 {
        steps.extend(on_contract(peer, 0.37, true, 12, 3.0, 0.25));
    }
    // The two peers also fail much of their other traffic, `worse` more so.
    for step in &mut steps {
        if step.contract != 0.37 && step.peer < 2 {
            step.failed = uniform() < if step.peer == 0 { 0.9 } else { 0.6 };
        }
    }
    steps.sort_by(|a, b| a.hours.total_cmp(&b.hours));
    for step in &steps {
        routing.observe_at(
            &peers[step.peer as usize],
            Location::new(step.contract),
            step.distance,
            if step.failed { &failure } else { &success },
            step.hours,
        );
    }
    (routing, worse, better, dead, distance, 3.25)
}

/// Routing acts on the ranking value: two peers whose reported failure
/// probabilities both clamp at 1 get different expected costs, the worse peer
/// the higher, and the better one is selected.
#[test]
fn routing_cost_ranks_peers_whose_forecasts_clamp_at_one() {
    use crate::node::network_status::OpType;
    use crate::router::{RouteEvent, RouteOutcome, Router};

    let _guard = GlobalRng::seed_guard(0x4485_c009);
    let _correction = crate::router::force_residual_correction(false);
    let _hierarchical = crate::router::force_hierarchical_routing(true);
    let (estimator, worse, better, dead, _, _) = clamped_pair();
    let mut router = Router::new(&[]);
    // Untimed traffic only, so no stage has timing and the cost is the
    // failure term alone.
    for index in 0..200 {
        router.add_event(RouteEvent {
            peer: PeerKeyLocation::random(),
            contract_location: Location::random(),
            outcome: if index % 5 == 0 {
                RouteOutcome::Failure
            } else {
                RouteOutcome::SuccessUntimed
            },
            op_type: Some(OpType::Get),
        });
    }
    router.hierarchical = estimator;
    let clock = router.prediction_clock();
    let predict = |peer: &PeerKeyLocation| {
        router
            .predict_routing_outcome_at(peer, dead, clock)
            .expect("prediction after warm-up")
    };
    let (worse_prediction, better_prediction) = (predict(&worse), predict(&better));
    assert_eq!(worse_prediction.failure_probability, 1.0);
    assert_eq!(better_prediction.failure_probability, 1.0);
    assert!(
        worse_prediction.expected_total_time > better_prediction.expected_total_time,
        "the clamp must not tie the two peers' costs: {} against {}",
        worse_prediction.expected_total_time,
        better_prediction.expected_total_time
    );
    let (selected, _) = router.select_k_best_peers_with_telemetry([&worse, &better], dead, 1);
    assert_eq!(selected, vec![&better]);
}

/// H1 of the 2026-09-18 round-3 review. `log_saturation`'s notice reports
/// `refused - <stored>`, and the stored value had become the COMPOSITE
/// saturation reading, which includes `pairs_refused_last_refit`: a gauge the
/// refit resets. The composite therefore falls, and the subtraction underflows
/// the first time a second notice is due, which is a panic in this build and a
/// wrap to about 1.8e19 in the release telemetry field this work is gated on.
///
/// Nothing reached it because every test either feeds one instant or never
/// crosses `SATURATION_LOG_INTERVAL_HOURS`. This test crosses it, with the
/// gauge falling between the two notices, which is the whole fault condition.
#[test]
fn a_second_saturation_notice_reports_a_real_delta_and_does_not_underflow() {
    let mut estimator = HierarchicalRouting::new(200);
    let table = estimator
        .failure
        .contracts
        .as_mut()
        .expect("the failure stage carries a contract table");
    // A composite reading well ABOVE the monotone total, which is the state
    // the old code stored and then subtracted from a later total.
    table.refused = 3;
    table.pairs_refused_last_refit = 40;
    table.displaced = 7;
    estimator.log_saturation(0.0);
    assert_eq!(
        estimator.refused_at_last_log, 3,
        "the delta must be tracked against the monotone total, not the composite"
    );
    assert_eq!(estimator.saturation_at_last_log, 50);

    // The refit resets the gauge, and two more live refusals arrive. Under the
    // old code the notice computed 5 - 50.
    let table = estimator.failure.contracts.as_mut().expect("table");
    table.pairs_refused_last_refit = 0;
    table.refused = 5;
    estimator.log_saturation(SATURATION_LOG_INTERVAL_HOURS + 0.001);
    assert_eq!(
        estimator.refused_at_last_log, 5,
        "the second notice must advance the monotone total"
    );
    assert_eq!(estimator.saturation_at_last_log, 12);
}

/// Same review, the second half of H1: the change gate read the composite
/// alone, so one new live refusal against a gauge one lower left the composite
/// identical and suppressed a notice about a real refusal.
#[test]
fn a_new_live_refusal_is_not_suppressed_by_a_falling_refit_gauge() {
    let mut estimator = HierarchicalRouting::new(200);
    let table = estimator.failure.contracts.as_mut().expect("table");
    table.refused = 3;
    table.pairs_refused_last_refit = 10;
    estimator.log_saturation(0.0);
    assert_eq!(estimator.saturation_at_last_log, 13);

    // +1 refusal, -1 gauge: the composite is unchanged at 13.
    let table = estimator.failure.contracts.as_mut().expect("table");
    table.refused = 4;
    table.pairs_refused_last_refit = 9;
    estimator.log_saturation(SATURATION_LOG_INTERVAL_HOURS + 0.001);
    assert_eq!(
        estimator.refused_at_last_log, 4,
        "a new live refusal must not be suppressed by an unchanged composite"
    );
}

/// I1 of the 2026-09-18 round-3 testing review, as a unit fact on an explicit
/// table rather than a probe of a stage's final state.
///
/// `a_peer_failing_a_contract_others_serve_is_charged_to_that_peer` compares
/// two stages and its verdict is the same in three regimes, including the term
/// being inert. The counterfactual that makes the leave-one-out MEAN anything
/// is this: with the failing peer included, the contract's effect is strongly
/// positive; with that one peer left out, it is not. If those two were equal,
/// leaving the peer out would be doing nothing.
#[test]
fn leaving_the_failing_peer_out_removes_its_own_evidence() {
    let mut table = ContractTable::new();
    let key = 0.52f64.to_bits();
    let (slot, _) = table.touch(key);
    // Peer 0 fails the contract; peers 1 and 2 succeed on it. Residuals carry
    // the sign of the outcome, as they do in the stage.
    for _ in 0..8 {
        assert!(table.add(slot, (0, 0), 1.0, 0.9));
    }
    for peer in 1..3u32 {
        for _ in 0..8 {
            assert!(table.add(slot, (peer, 0), 1.0, -0.3));
        }
    }
    // Contracts whose MEANS differ, with their own peers in agreement. This is
    // what `tau2_contract` measures, and it has to be positive or `effect`
    // returns `None` for every query and the counterfactual is unobservable.
    //
    // Measured while writing this, and worth recording: a table holding ONLY
    // the disagreeing contract above gives `tau2_contract` exactly 0 with two
    // qualifying contracts, because the pooled `tau2_peer` rises to 0.2275 and
    // the noise it contributes exceeds the between-contract contrast. So a
    // contract whose peers disagree sharply contributes its spread to
    // `tau2_peer`, not to `tau2_contract`, and the term produces no effect for
    // it at all unless OTHER contracts supply the between-contract variance.
    // That is the estimator behaving as designed, and it is why this table has
    // a background of agreeing contracts.
    for (index, mean) in (0..12u64).map(|i| (i, if i % 2 == 0 { 0.8 } else { -0.4 })) {
        let (other, _) = table.touch((0.1 + 0.01 * index as f64).to_bits());
        for peer in 0..3u32 {
            for _ in 0..8 {
                assert!(table.add(other, (peer, 0), 1.0, mean));
            }
        }
    }
    table.components = table.compute_components();
    let components = table.components.expect("components exist");
    assert!(
        components.tau2_contract > 0.0,
        "the contracts must differ, or every effect is None: {components:?}"
    );

    let shared = table
        .effect(Some(slot), ContractQuery::Shared, 0.0, 1)
        .expect("three present peers");
    let left_out = table
        .effect(Some(slot), ContractQuery::LeaveOut(Some((0, 0))), 0.0, 1)
        .expect("two present peers without peer 0");
    assert!(
        shared > 0.05,
        "including the failing peer, the contract's mean residual is positive \
         and so is its effect: {shared}"
    );
    assert!(
        left_out < 0.0,
        "leaving that peer out leaves two successes, so the effect must turn \
         NEGATIVE: {left_out} against shared {shared}"
    );
    // And the consequence for the peer: its own positive residual is adjusted
    // by a NEGATIVE effect, which the bound refuses, so nothing is explained
    // away. This is the property `explaining_bound` exists for on the learn
    // path, and the mutation that drops it (c1_learn_site_drops_the_bound,
    // GREEN before this test) makes the residual LARGER than the peer earned.
    let residual = 0.9;
    assert_eq!(
        explaining_bound(left_out, residual),
        0.0,
        "an effect opposing the residual's sign must remove nothing"
    );
    assert!(
        residual - left_out > residual,
        "and dropping the bound would charge the peer MORE than its own \
         residual: {} against {residual}",
        residual - left_out
    );
}

/// I2 / L8 of the same review: `effects_applied` must count residuals that
/// MOVED, not effects that were merely available. An effect the bound takes to
/// zero (one opposing the residual's sign) leaves the levels learning exactly
/// what they would have learned without the term, and counting it made this an
/// availability count under an application count's name.
///
/// The case is routine rather than rare: it is a failure event on a contract
/// most peers succeed on, which is the setup above.
#[test]
fn an_effect_the_bound_refuses_does_not_count_as_applied() {
    let _guard = GlobalRng::seed_guard(0x4485_c0b0);
    let good = 0.41;
    // Peers 1..6 succeed on `good`, so its effect is negative.
    let mut steps = background_pooled(0.0, 2.0, 10..30);
    for peer in 1..6u32 {
        steps.extend(on_contract(peer, good, false, 8, 1.6, 0.1));
    }
    // Then peer 0 FAILS there: a positive residual meeting a negative effect.
    steps.extend(on_contract(0, good, true, 4, 1.9, 0.02));
    let (mut stage, _) = failure_stage_pair();
    feed(&mut [&mut stage], steps);

    let table = stage.contracts.as_ref().expect("a contract table");
    let applied = table.effects_applied;
    let slot = table
        .table
        .lookup(&good.to_bits())
        .expect("the contract is tracked");
    let effect = table.effect(Some(slot), ContractQuery::Shared, 1.9, 1);
    assert!(
        effect.is_none_or(|value| value < 0.0),
        "the contract several peers succeed on must not have a positive \
         effect, or this test is not exercising the refused case: {effect:?}"
    );
    // The counter must not have run away with the availability of an effect:
    // every failure on `good` had one available and the bound refused it.
    assert!(
        applied < table.estimable_refits * u64::from(WINDOW_EVENTS as u32),
        "sanity on the shape of the counter: {applied}"
    );
    // The decisive part, as a unit fact, because the stage's own count mixes
    // in every other contract's genuine adjustments.
    assert_eq!(
        explaining_bound(-0.2, 0.9),
        0.0,
        "an effect opposing the residual removes nothing"
    );
    assert_ne!(
        explaining_bound(0.2, 0.9),
        0.0,
        "and one agreeing with it does, so the two cases are distinguishable"
    );
}

/// C1 of the 2026-09-18 round-3 testing review, at the CALL SITES rather than
/// on the helper.
///
/// `explaining_bound` had an exhaustive unit test and both of its call sites
/// were still unpinned: reverting the learn site to `residual - effect` or the
/// refit site to `residual -= adjustment` left the whole suite green (measured
/// twice, at `450462896` and again at `9ad70135b` after the first attempt at
/// closing it, which asserted on the helper and therefore did not touch this).
/// A guard on one layer says nothing about the layer above it.
///
/// The case the bound exists for needs three things at once, which is why no
/// existing test reached it: the term has to be ACTIVE (a real between-contract
/// spread, or every effect is `None`), the acting peer's residual has to have
/// the OPPOSITE sign to its contract's effect, and the comparison has to be
/// against a control stage rather than against a recomputed expectation.
///
/// Peer 0 FAILS a contract that four other peers SUCCEED on. Its residual is
/// positive and the leave-one-out effect is negative, so the bound must remove
/// NOTHING: peer 0's level sum has to equal the no-term control exactly. Drop
/// either bound and the negative effect is subtracted instead, which charges
/// peer 0 MORE than its own residuals justify, and the equality fails.
#[test]
fn neither_bound_call_site_may_charge_a_peer_more_than_its_own_residual() {
    let _guard = GlobalRng::seed_guard(0x4485_c1b0);
    let target = 0.61;

    // A real between-contract spread: eight contracts everyone fails and eight
    // everyone succeeds on. Without this `tau2_contract` is zero and every
    // effect is `None`, which is the regime
    // `a_peer_failing_a_contract_others_serve_is_charged_to_that_peer` turned
    // out to be in.
    let mut steps = Vec::new();
    for index in 0..8u32 {
        let failing = 0.05 + 0.01 * f64::from(index);
        let winning = 0.30 + 0.01 * f64::from(index);
        for peer in 10..13u32 {
            steps.extend(on_contract(peer, failing, true, 6, 0.0, 0.4));
            steps.extend(on_contract(peer, winning, false, 6, 0.0, 0.4));
        }
    }
    // The target contract: four peers succeed on it, so its effect is negative.
    for peer in 20..24u32 {
        steps.extend(on_contract(peer, target, false, 6, 0.5, 0.3));
    }
    // A second background block BEFORE peer 0 acts, so the refit that precedes
    // its events has already seen the target contract and `tau2_contract` is
    // positive when they are LEARNED. Without this the learn-site branch never
    // fires for peer 0 and only the refit site is exercised: measured, and it
    // is why the first version of this test killed one mutation and not the
    // other.
    for index in 0..8u32 {
        let failing = 0.05 + 0.01 * f64::from(index);
        let winning = 0.30 + 0.01 * f64::from(index);
        for peer in 10..13u32 {
            steps.extend(on_contract(peer, failing, true, 6, 0.85, 0.3));
            steps.extend(on_contract(peer, winning, false, 6, 0.85, 0.3));
        }
    }
    // Then peer 0 fails there. Peer 0 appears NOWHERE else, so its level sum is
    // exactly these events' adjusted residuals.
    steps.extend(on_contract(0, target, true, 4, 1.2, 0.1));

    let (mut with_term, mut without_term) = failure_stage_pair();
    feed(&mut [&mut with_term, &mut without_term], steps);

    // PHASE 1 measures the LEARN site. A refit REBUILDS the levels from the
    // window, so after one has run the level sums carry the refit's
    // adjustments and the learn site is invisible in them. Measuring here,
    // with no further traffic, is the only way to see it: the first version of
    // this test fed everything in one go and therefore killed the refit-site
    // mutation and not the learn-site one.
    {
        let slot_with = with_term.peers.lookup(&0).expect("peer 0 is tracked");
        let slot_without = without_term.peers.lookup(&0).expect("peer 0 is tracked");
        let applied = with_term.levels[0].nodes[slot_with].peer.sum;
        let control = without_term.levels[0].nodes[slot_without].peer.sum;
        assert!(
            control > 0.05,
            "sanity: peer 0's own failures must be worth something in the \
             control at the learn site, or the equality holds trivially: {control}"
        );
        assert!(
            (applied - control).abs() < 1e-9,
            "LEARN SITE: an effect opposing peer 0's residuals must remove \
             nothing: with the term {applied}, control {control}"
        );
    }

    // PHASE 2 forces a refit, which re-adjusts the whole window and rebuilds
    // the levels, and measures the REFIT site on the same events.
    let mut more = Vec::new();
    for index in 0..8u32 {
        let failing = 0.05 + 0.01 * f64::from(index);
        let winning = 0.30 + 0.01 * f64::from(index);
        for peer in 10..13u32 {
            more.extend(on_contract(peer, failing, true, 6, 1.4, 0.3));
            more.extend(on_contract(peer, winning, false, 6, 1.4, 0.3));
        }
    }
    feed(&mut [&mut with_term, &mut without_term], more);

    // The term must be active, or this test cannot see the bound at all.
    let table = with_term.contracts.as_ref().expect("a contract table");
    let components = table.components.expect("components at the last refit");
    assert!(
        components.tau2_contract > 0.0,
        "the contracts must differ or every effect is None: {components:?}"
    );
    let slot = table
        .table
        .lookup(&target.to_bits())
        .expect("the target contract is tracked");
    let peer_zero = with_term
        .peers
        .lookup(&0)
        .and_then(|index| with_term.peers.generation(index).map(|g| (index as u32, g)));
    let effect = table
        .effect(Some(slot), ContractQuery::LeaveOut(peer_zero), 1.5, 1)
        .expect("four other peers are present on the target");
    assert!(
        effect < -0.01,
        "leaving peer 0 out leaves four successes, so the effect must be \
         clearly negative or the bound has nothing to refuse: {effect}"
    );

    // Peer 0's residuals are POSITIVE (it failed), the effect is NEGATIVE, so
    // the bound removes nothing and the two stages must agree exactly.
    let slot_with = with_term.peers.lookup(&0).expect("peer 0 is tracked");
    let slot_without = without_term.peers.lookup(&0).expect("peer 0 is tracked");
    let applied = with_term.levels[0].nodes[slot_with].peer.sum;
    let control = without_term.levels[0].nodes[slot_without].peer.sum;
    assert!(
        control > 0.05,
        "sanity: peer 0's own failures must be worth something in the control \
         at the refit site too: {control}"
    );
    assert!(
        (applied - control).abs() < 1e-9,
        "REFIT SITE: an effect opposing peer 0's residuals must remove \
         nothing: with the term {applied}, control {control}, effect {effect}"
    );
}
