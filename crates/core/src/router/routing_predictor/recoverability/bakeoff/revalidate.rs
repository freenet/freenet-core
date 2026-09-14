//! Re-validation of the bake-off winner after the statistical review of the
//! production port (PR #5655), on the full scenario set plus a
//! production-like traffic shape. Everything here was fixed before running.
//!
//! # What the review found, and what is implemented here
//!
//! 1. **Curve shrinkage `tau2`** was an unweighted mean over PAV blocks, so
//!    small outcome-selected end blocks dominated. [`Curve::new_anova`] uses
//!    the one-way random-effects method of moments for unequal block sizes.
//! 2. **Variance components** were biased because a cell's mean is part of its
//!    peer's mean. [`HierC`] estimates them from leave-one-out contrasts.
//! 3. **Decayed moments** used `sum w` where the Kish effective count belongs.
//!    [`HierC`] stores `W2 = sum w^2` per node: noise `sigma2 W2 / n^2`, degrees
//!    of freedom `sum (n - W2/n)`, gate `n^2 / W2 >= 2`.
//!
//! Timing is also scored in SECONDS against `E[seconds]`, and the corrected
//! rows predict the expectation `exp(mu + (sigma2 + v_post) / 2)` rather than
//! the median `exp(mu)`.
//!
//! # Rows
//!
//! Rows 3 and 4 run the bake-off's reference code, with components recomputed
//! for every prediction. Row 5 is the production form (components frozen at
//! each refit, node statistics read live) with the ORIGINAL formulas, so the
//! step from 5 to 6 isolates corrections 1-3 and the step from 4 to 5 isolates
//! the freezing.
//!
//! # Baseline
//!
//! Failure: the legacy blend. Timing in seconds: the legacy stack AS SHIPPED
//! (raw units), which estimates arithmetic means natively. The log-space legacy
//! blend exponentiated is a median and is reported for reference.

use super::*;

/// The production-like modifier: 200 peers, Zipf traffic, home-band locality,
/// 600 events/h over 10 hours.
const PROD_PEERS: usize = 200;
const PROD_EVENTS: usize = 6_000;
const PROD_EVENTS_PER_HOUR: f64 = 600.0;

const NODE_MIN: f64 = 1e-12;

const REVAL_ROWS: [&str; 9] = [
    "a  legacy blend (log-space, exp = median)",
    "a' legacy as shipped (raw units)",
    "b  global curve alone",
    "3  H* EB with attribute (winner, e26a92c1d)",
    "4  H* EB no attribute (PR #5655 shape)",
    "5  no attr, frozen comps, original formulas",
    "6  no attr + corrections 1-3, median timing",
    "7  no attr + corrections 1-3 + E[] timing",
    "8  7 + LOO descent (guarded)",
];

const REVAL_RANK_ROWS: [(usize, &str); 8] = [
    (0, "a  legacy blend"),
    (2, "b  global curve alone"),
    (3, "3  H* EB with attribute"),
    (4, "4  H* EB no attribute"),
    (5, "5  frozen, original formulas"),
    (6, "6/7 corrections 1-3"),
    (8, "8  + LOO descent"),
    (usize::MAX, "   nearest peer (no model)"),
];

fn prod(mut spec: Spec, name: &'static str) -> Spec {
    spec.name = name;
    spec.peers = PROD_PEERS;
    spec.zipf = true;
    spec.events = PROD_EVENTS;
    spec.events_per_hour = PROD_EVENTS_PER_HOUR;
    spec.home_band = !spec.ops;
    spec
}

/// The production-like scenarios, fixed a priori.
fn prod_scenarios() -> Vec<Spec> {
    let mixed = Spec {
        marginal_sd: 0.08,
        attribute_step: 0.06,
        pairs: Pairs::Natural,
        pair_effect: 0.40,
        ..Spec::failure("")
    };
    let timing_mixed = Spec {
        marginal_sd: 0.4,
        attribute_step: 0.25,
        pairs: Pairs::Natural,
        pair_effect: 0.8,
        ..Spec::timing("")
    };
    vec![
        prod(Spec::failure(""), "p.dist"),
        prod(
            Spec {
                marginal_sd: 0.08,
                ..Spec::failure("")
            },
            "p.peer",
        ),
        prod(mixed, "p.mixed"),
        prod(
            Spec {
                drift_effect: 0.25,
                ..Spec::failure("")
            },
            "p.drift",
        ),
        prod(
            Spec {
                base: Base::Rare,
                marginal_sd: 0.01,
                attribute_step: 0.01,
                pairs: Pairs::Natural,
                pair_effect: 0.15,
                ..Spec::failure("")
            },
            "p.rare-mixed",
        ),
        prod(noise("", 0.0, Labeling::All), "p.ops-clean"),
        prod(noise("", 0.0, Labeling::Untrained), "p.ops-untrained"),
        prod(
            noise_with("", 0.05, Labeling::All, AbsentLayout::Uniform),
            "p.abs5-naive-uni",
        ),
        prod(
            noise_with("", 0.05, Labeling::Untrained, AbsentLayout::Uniform),
            "p.abs5-untrained-uni",
        ),
        prod(
            noise_with("", 0.20, Labeling::Untrained, AbsentLayout::Uniform),
            "p.abs20-untrained-uni",
        ),
        prod(Spec::timing(""), "pt.dist"),
        prod(
            Spec {
                marginal_sd: 0.4,
                ..Spec::timing("")
            },
            "pt.peer",
        ),
        prod(timing_mixed, "pt.mixed"),
        prod(
            Spec {
                drift_effect: 0.7,
                ..Spec::timing("")
            },
            "pt.drift",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Hierarchy root -> peer -> (peer, band), production form
// ---------------------------------------------------------------------------

/// Exponentially-forgotten moments with the squared-weight sum.
#[derive(Default, Clone, Copy)]
struct Mom {
    n: f64,
    w2: f64,
    sum: f64,
    sumsq: f64,
    t: f64,
}

impl Mom {
    fn at(&self, now: f64, decay: Option<f64>) -> Mom {
        let f = decay.map_or(1.0, |h| (-(now - self.t).max(0.0) / h).exp());
        Mom {
            n: self.n * f,
            w2: self.w2 * f * f,
            sum: self.sum * f,
            sumsq: self.sumsq * f,
            t: now,
        }
    }

    fn add(&mut self, x: f64, now: f64, decay: Option<f64>) {
        let mut d = self.at(now, decay);
        d.n += 1.0;
        d.w2 += 1.0;
        d.sum += x;
        d.sumsq += x * x;
        *self = d;
    }

    fn mean(&self) -> f64 {
        self.sum / self.n
    }
}

/// Frozen at refit.
#[derive(Clone, Copy, Default)]
struct Comps {
    sigma2: f64,
    tau2_cell: f64,
    tau2_peer: f64,
    /// `sum_c n_c^2` and `sum_p N_p^2` over the whole hierarchy.
    s_cells: f64,
    s_peers: f64,
    peers_with_data: usize,
}

struct HierC {
    decay: Option<f64>,
    corrected: bool,
    root: Mom,
    peers: HashMap<usize, Mom>,
    cells: HashMap<(usize, usize), Mom>,
    comps: Option<Comps>,
}

fn band_of(contract: f64) -> usize {
    ((contract * HIER_BANDS as f64).floor() as usize).min(HIER_BANDS - 1)
}

impl HierC {
    fn new(decay: Option<f64>, corrected: bool) -> HierC {
        HierC {
            decay,
            corrected,
            root: Mom::default(),
            peers: HashMap::new(),
            cells: HashMap::new(),
            comps: None,
        }
    }

    fn add(&mut self, peer: usize, contract: f64, t: f64, r: f64) {
        self.root.add(r, t, self.decay);
        self.peers.entry(peer).or_default().add(r, t, self.decay);
        self.cells
            .entry((peer, band_of(contract)))
            .or_default()
            .add(r, t, self.decay);
    }

    fn gated(&self, m: &Mom) -> bool {
        if self.corrected {
            m.w2 > 0.0 && m.n * m.n / m.w2 >= 2.0
        } else {
            m.n >= 2.0
        }
    }

    /// Recompute variance components at `now`. `O(nodes)`.
    fn refresh(&mut self, now: f64) {
        let d = self.decay;
        let root = self.root.at(now, d);
        let peers: HashMap<usize, Mom> =
            self.peers.iter().map(|(&k, v)| (k, v.at(now, d))).collect();
        let cells: HashMap<(usize, usize), Mom> =
            self.cells.iter().map(|(&k, v)| (k, v.at(now, d))).collect();

        let (mut ss, mut df) = (0.0, 0.0);
        for m in cells.values() {
            if self.gated(m) {
                ss += (m.sumsq - m.sum * m.sum / m.n).max(0.0);
                df += if self.corrected {
                    m.n - m.w2 / m.n
                } else {
                    m.n - 1.0
                };
            }
        }
        if df < 2.0 {
            self.comps = None;
            return;
        }
        let sigma2 = (ss / df).max(1e-9);

        let mut peer_sq: HashMap<usize, f64> = HashMap::new();
        for (&(p, _), m) in &cells {
            *peer_sq.entry(p).or_default() += m.n * m.n;
        }
        let s_cells: f64 = peer_sq.values().sum();
        let s_peers: f64 = peers.values().map(|m| m.n * m.n).sum();

        let tau2_cell = if self.corrected {
            let (mut num, mut den) = (0.0, 0.0);
            for (&(p, _), c) in &cells {
                let parent = peers[&p];
                let rest_n = parent.n - c.n;
                if !self.gated(c) || rest_n <= NODE_MIN {
                    continue;
                }
                let rest_mean = (parent.sum - c.sum) / rest_n;
                let rest_w2 = (parent.w2 - c.w2).max(0.0);
                num += (c.mean() - rest_mean).powi(2)
                    - sigma2 * (c.w2 / (c.n * c.n) + rest_w2 / (rest_n * rest_n));
                den += 1.0 + (peer_sq[&p] - c.n * c.n).max(0.0) / (rest_n * rest_n);
            }
            if den > 0.0 { (num / den).max(0.0) } else { 0.0 }
        } else {
            let (mut acc, mut count) = (0.0, 0.0);
            for (&(p, _), c) in &cells {
                let parent = peers[&p];
                if c.n >= 2.0 {
                    acc += (c.mean() - parent.mean()).powi(2)
                        - sigma2 * (1.0 / c.n - 1.0 / parent.n).max(0.0);
                    count += 1.0;
                }
            }
            if count > 0.0 {
                (acc / count).max(0.0)
            } else {
                0.0
            }
        };

        let tau2_peer = if self.corrected {
            let (mut num, mut den) = (0.0, 0.0);
            for (&p, m) in &peers {
                let rest_n = root.n - m.n;
                if !self.gated(m) || rest_n <= NODE_MIN {
                    continue;
                }
                let rest_mean = (root.sum - m.sum) / rest_n;
                let rest_w2 = (root.w2 - m.w2).max(0.0);
                let sp = peer_sq.get(&p).copied().unwrap_or(0.0);
                num += (m.mean() - rest_mean).powi(2)
                    - tau2_cell * (sp / (m.n * m.n) + (s_cells - sp).max(0.0) / (rest_n * rest_n))
                    - sigma2 * (m.w2 / (m.n * m.n) + rest_w2 / (rest_n * rest_n));
                den += 1.0 + (s_peers - m.n * m.n).max(0.0) / (rest_n * rest_n);
            }
            if den > 0.0 { (num / den).max(0.0) } else { 0.0 }
        } else {
            // The reference: peer means against the root mean.
            let base = root.mean();
            let (mut acc, mut count) = (0.0, 0.0);
            for (&p, m) in &peers {
                if m.n < 2.0 {
                    continue;
                }
                let sp = peer_sq.get(&p).copied().unwrap_or(0.0);
                acc += (m.mean() - base).powi(2) - (tau2_cell * sp / (m.n * m.n) + sigma2 / m.n);
                count += 1.0;
            }
            if count > 0.0 {
                (acc / count).max(0.0)
            } else {
                0.0
            }
        };

        self.comps = Some(Comps {
            sigma2,
            tau2_cell,
            tau2_peer,
            s_cells,
            s_peers,
            peers_with_data: peers.values().filter(|m| m.n > NODE_MIN).count(),
        });
    }

    /// Posterior `(mean, variance, sigma2)` of the residual. Node statistics
    /// are read live; components are the frozen ones.
    fn predict(&self, now: f64, peer: usize, contract: f64, loo: bool) -> (f64, f64, f64) {
        let Some(c) = self.comps else {
            return (0.0, 0.0, 0.0);
        };
        let d = self.decay;
        let step = |(mu, v): (f64, f64), node: Option<(f64, f64)>, tau2: f64| match node {
            Some((mean, noise)) if tau2 > 0.0 => {
                let prior = tau2 + v;
                let b = prior / (prior + noise);
                (mu + b * (mean - mu), b * noise)
            }
            _ => (mu, v + tau2),
        };
        let root = self.root.at(now, d);
        let pm = self.peers.get(&peer).map(|m| m.at(now, d));
        let band = band_of(contract);
        let cm = self.cells.get(&(peer, band)).map(|m| m.at(now, d));
        let sp: f64 = (0..HIER_BANDS)
            .filter_map(|b| self.cells.get(&(peer, b)))
            .map(|m| {
                let m = m.at(now, d);
                m.n * m.n
            })
            .sum();
        let noise_of = |w2: f64, n: f64| {
            if self.corrected {
                c.sigma2 * w2 / (n * n)
            } else {
                c.sigma2 / n
            }
        };

        // Root, leaving the queried peer out when enough other peers exist.
        let pn = pm.map_or(0.0, |m| m.n);
        let other_peers = c.peers_with_data - usize::from(pn > NODE_MIN);
        let (rn, rs, rw2, rs_peers, rs_cells) = if loo && pm.is_some() && other_peers >= 2 {
            let m = pm.expect("checked");
            (
                root.n - m.n,
                root.sum - m.sum,
                (root.w2 - m.w2).max(0.0),
                (c.s_peers - m.n * m.n).max(0.0),
                (c.s_cells - sp).max(0.0),
            )
        } else {
            (root.n, root.sum, root.w2, c.s_peers, c.s_cells)
        };
        let mut state = (0.0, 0.0);
        if rn > NODE_MIN {
            let mean = rs / rn;
            let noise = noise_of(rw2, rn)
                + c.tau2_peer * rs_peers / (rn * rn)
                + c.tau2_cell * rs_cells / (rn * rn);
            state = step(state, Some((mean, noise)), (mean * mean - noise).max(0.0));
        }

        // Peer, leaving the queried cell out under LOO.
        let peer_node = pm.and_then(|m| {
            let (n, sum, w2, sq) = match (loo, cm) {
                (true, Some(cell)) => (
                    m.n - cell.n,
                    m.sum - cell.sum,
                    (m.w2 - cell.w2).max(0.0),
                    (sp - cell.n * cell.n).max(0.0),
                ),
                _ => (m.n, m.sum, m.w2, sp),
            };
            (n > NODE_MIN).then(|| (sum / n, c.tau2_cell * sq / (n * n) + noise_of(w2, n)))
        });
        state = step(state, peer_node, c.tau2_peer);

        let cell_node = cm
            .filter(|m| m.n > NODE_MIN)
            .map(|m| (m.mean(), noise_of(m.w2, m.n)));
        state = step(state, cell_node, c.tau2_cell);
        (state.0, state.1, c.sigma2)
    }
}

/// One `HierC` per horizon, rebuilt at each refit, with a prequential horizon
/// selector per descent mode.
struct ReC {
    hiers: Vec<HierC>,
    /// Decayed loss per horizon for `[standard, loo]` descent.
    loss: [Vec<(f64, f64)>; 2],
}

impl ReC {
    fn new(corrected: bool) -> ReC {
        ReC {
            hiers: HORIZONS.iter().map(|&h| HierC::new(h, corrected)).collect(),
            loss: [
                vec![(0.0, 0.0); HORIZONS.len()],
                vec![(0.0, 0.0); HORIZONS.len()],
            ],
        }
    }

    fn rebuild(&mut self, raw: &[Raw], now: f64, prior: impl Fn(&Raw) -> Option<f64>) {
        for (hier, &h) in self.hiers.iter_mut().zip(HORIZONS.iter()) {
            *hier = HierC::new(h, hier.corrected);
        }
        for r in raw {
            if let Some(g) = prior(r) {
                for hier in &mut self.hiers {
                    hier.add(r.peer, r.contract, r.time, r.y - g);
                }
            }
        }
        for hier in &mut self.hiers {
            hier.refresh(now);
        }
    }

    fn add(&mut self, r: &Raw, prior: f64) {
        for hier in &mut self.hiers {
            hier.add(r.peer, r.contract, r.time, r.y - prior);
        }
    }

    fn selected(&self, now: f64, mode: usize) -> usize {
        let decayed =
            |(sum, t): (f64, f64)| sum * (-(now - t).max(0.0) / SELECTOR_FORGETTING_HOURS).exp();
        let mut best = 0;
        for i in 1..HORIZONS.len() {
            if decayed(self.loss[mode][i]) < decayed(self.loss[mode][best]) {
                best = i;
            }
        }
        best
    }

    fn predictions(&self, now: f64, peer: usize, contract: f64, loo: bool) -> Vec<(f64, f64, f64)> {
        self.hiers
            .iter()
            .map(|h| h.predict(now, peer, contract, loo))
            .collect()
    }

    fn score(&mut self, mode: usize, predictions: &[f64], actual_residual: f64, now: f64) {
        for (loss, prediction) in self.loss[mode].iter_mut().zip(predictions) {
            let factor = (-(now - loss.1).max(0.0) / SELECTOR_FORGETTING_HOURS).exp();
            *loss = (
                loss.0 * factor + (prediction - actual_residual).powi(2),
                now,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RevalResult {
    /// `[subset][row]`: log-space for timing (as the bake-off scored it).
    mse_log: Vec<Vec<f64>>,
    /// `[subset][row]`: natural units (failure probability; timing seconds).
    mse_nat: Vec<Vec<f64>>,
    counts: [usize; SUBSETS.len()],
    ranking: [[f64; 6]; REVAL_RANK_ROWS.len()],
    decisions: usize,
    /// End-of-run components of the selected-horizon hierarchy:
    /// `[sigma2, tau2_cell, tau2_peer]` for original (row 5) then corrected.
    components: [[f64; 3]; 2],
}

struct PendingC {
    peer_index: usize,
    distance: f64,
    time: f64,
    y: f64,
    global: Option<f64>,
    shrunk: Option<(f64, Vec<f64>)>,
    noattr: Option<(f64, Vec<f64>)>,
    frozen: Option<(f64, Vec<f64>)>,
    corrected: Option<(f64, Vec<f64>, Vec<f64>)>,
}

fn run_revalidate(spec: Spec, seed: u64) -> RevalResult {
    let _guard = GlobalRng::seed_guard(seed);
    let world = World::new(spec);
    let rows = REVAL_ROWS.len();
    let timing = spec.target == Target::Timing;

    let mut iso = IsotonicEstimator::new(Vec::new(), EstimatorType::Positive);
    let mut iso_raw = IsotonicEstimator::new_with_mode(
        Vec::new(),
        EstimatorType::Positive,
        AdjustmentMode::Multiplicative,
    );
    let mut legacy_abs = PredictionStage::new(10_000);
    let mut legacy_abs_raw = PredictionStage::new(10_000);
    let mut peer_ids: HashMap<usize, u64> = HashMap::new();
    let mut peer_events = vec![0usize; spec.peers];
    let (weight_ramp, clamp_unit) = if timing {
        (TIMING_WEIGHT_RAMP_EVENTS, false)
    } else {
        (FAILURE_WEIGHT_RAMP_EVENTS, true)
    };
    let finish = |v: f64| if clamp_unit { v.clamp(0.0, 1.0) } else { v };
    let attr_config = HierConfig {
        root: true,
        attribute: true,
        band_by: BandBy::Contract,
        decay_hours: None,
    };
    let noattr_config = HierConfig {
        attribute: false,
        ..attr_config
    };
    let mut shrunk_long = Curve::new(None, true);
    let mut anova_long = Curve::new_anova();
    let mut re_attr = Reanchored::new(attr_config, &HORIZONS);
    let mut re_noattr = Reanchored::new(noattr_config, &HORIZONS);
    let mut re_frozen = ReC::new(false);
    let mut re_corrected = ReC::new(true);
    let mut raw: Vec<Raw> = Vec::new();
    let mut learned_since_rebuild = 0usize;

    let mut err_log = vec![vec![0.0; rows]; SUBSETS.len()];
    let mut err_nat = vec![vec![0.0; rows]; SUBSETS.len()];
    let mut counts = [0usize; SUBSETS.len()];
    let mut ranking = [[0.0f64; 6]; REVAL_RANK_ROWS.len()];
    let mut decisions = 0usize;
    let mut pending: Vec<PendingC> = Vec::new();
    let hours = |index: usize| index as f64 / spec.events_per_hour;
    let mut index = 0usize;

    while index < spec.events {
        let op = world.next_op(index);
        let contract_value = op.contract;
        let contract = Location::try_from(contract_value).expect("contract within ring");

        if spec.ops && !op.absent && index >= WARMUP_EVENTS {
            let time = hours(index);
            let candidates = world.nearest_peers(contract_value);
            let snaps_attr = re_attr.snapshots(time);
            let sel_attr = re_attr.selected(time);
            let snaps_noattr = re_noattr.snapshots(time);
            let sel_noattr = re_noattr.selected(time);
            let sel_frozen = re_frozen.selected(time, 0);
            let sel_corr = re_corrected.selected(time, 0);
            let sel_loo = re_corrected.selected(time, 1);
            let mut truths = Vec::new();
            let mut scores: Vec<Vec<f64>> = vec![Vec::new(); REVAL_RANK_ROWS.len()];
            let mut complete = true;
            for (position, &cand) in candidates.iter().enumerate() {
                let peer = &world.peers[cand];
                let distance = contract
                    .distance(peer.location().expect("peer has a location"))
                    .as_f64();
                let attribute = world.attribute[cand];
                truths.push(world.truth(index, cand, contract_value, distance));
                let (Some(global), Some(peer_adjusted)) = (
                    iso.estimate_global(peer, contract).ok().map(finish),
                    iso.estimate_retrieval_time(peer, contract).ok().map(finish),
                ) else {
                    complete = false;
                    break;
                };
                let next_id = peer_ids.len() as u64;
                let observation = RoutingObservation {
                    peer_id: *peer_ids.get(&cand).unwrap_or(&next_id) as f64,
                    contract_location: contract_value,
                    distance,
                    time,
                };
                let legacy = match legacy_abs.predict(&observation) {
                    Some(v) if v.is_finite() => {
                        let w = (legacy_abs.len() as f64 / weight_ramp).min(MAX_RENEGADE_WEIGHT);
                        finish(peer_adjusted * (1.0 - w) + v.clamp(0.0, 1.0) * w)
                    }
                    _ => peer_adjusted,
                };
                let old = |curve: &Curve, re: &Reanchored, snaps: &[Option<Snapshot>], h: usize| {
                    curve.value(distance).map(finish).map_or(global, |g| {
                        finish(
                            g + re.hiers[h]
                                .predict(
                                    snaps[h].as_ref(),
                                    cand,
                                    attribute,
                                    contract_value,
                                    distance,
                                )
                                .0,
                        )
                    })
                };
                let new = |curve: &Curve, re: &ReC, h: usize, loo: bool| {
                    curve.value(distance).map(finish).map_or(global, |g| {
                        finish(g + re.hiers[h].predict(time, cand, contract_value, loo).0)
                    })
                };
                for (slot, &(row, _)) in REVAL_RANK_ROWS.iter().enumerate() {
                    scores[slot].push(match row {
                        0 => legacy,
                        2 => global,
                        3 => old(&shrunk_long, &re_attr, &snaps_attr, sel_attr),
                        4 => old(&shrunk_long, &re_noattr, &snaps_noattr, sel_noattr),
                        5 => new(&shrunk_long, &re_frozen, sel_frozen, false),
                        6 => new(&anova_long, &re_corrected, sel_corr, false),
                        8 => new(&anova_long, &re_corrected, sel_loo, true),
                        _ => position as f64,
                    });
                }
            }
            if complete && candidates.len() >= 3 {
                let argmin = |values: &[f64]| {
                    let mut best = 0;
                    for (i, v) in values.iter().enumerate() {
                        if *v < values[best] {
                            best = i;
                        }
                    }
                    best
                };
                let best10 = argmin(&truths);
                let best3 = argmin(&truths[..3]);
                let targeted = candidates
                    .iter()
                    .any(|&cand| world.in_pair(cand, contract_value));
                for (slot, row_scores) in scores.iter().enumerate() {
                    let chosen10 = argmin(row_scores);
                    let chosen3 = argmin(&row_scores[..3]);
                    ranking[slot][0] += f64::from(u8::from(chosen10 == best10));
                    ranking[slot][1] += truths[chosen10] - truths[best10];
                    ranking[slot][2] += f64::from(u8::from(chosen3 == best3));
                    ranking[slot][3] += truths[chosen3] - truths[best3];
                    if targeted {
                        ranking[slot][4] += f64::from(u8::from(chosen10 == best10));
                        ranking[slot][5] += truths[chosen10] - truths[best10];
                    }
                }
                decisions += 1;
            }
        }

        pending.clear();
        let mut succeeded = false;
        for &peer_index in &op.candidates {
            if index >= spec.events {
                break;
            }
            let peer = &world.peers[peer_index];
            let distance = contract
                .distance(peer.location().expect("peer has a location"))
                .as_f64();
            let time = hours(index);
            let attribute = world.attribute[peer_index];
            let truth = world.truth(index, peer_index, contract_value, distance);
            let y = if op.absent { 1.0 } else { world.outcome(truth) };

            let global = iso.estimate_global(peer, contract).ok().map(finish);
            let peer_adjusted = iso.estimate_retrieval_time(peer, contract).ok().map(finish);
            let mut pend = PendingC {
                peer_index,
                distance,
                time,
                y,
                global,
                shrunk: None,
                noattr: None,
                frozen: None,
                corrected: None,
            };

            if let (Some(global), Some(peer_adjusted)) = (global, peer_adjusted) {
                let next_id = peer_ids.len() as u64;
                let observation = RoutingObservation {
                    peer_id: *peer_ids.get(&peer_index).unwrap_or(&next_id) as f64,
                    contract_location: contract_value,
                    distance,
                    time,
                };
                let weight = |len: usize| (len as f64 / weight_ramp).min(MAX_RENEGADE_WEIGHT);
                // Log-space predictions (failure: probability) plus, per row, the
                // log-space adjustment that turns a median into an expectation.
                let mut prediction = vec![global; rows];
                let mut expectation = vec![0.0; rows];
                prediction[0] = match legacy_abs.predict(&observation) {
                    Some(v) if v.is_finite() && (clamp_unit || v >= 0.0) => {
                        let w = weight(legacy_abs.len());
                        let v = if clamp_unit { v.clamp(0.0, 1.0) } else { v };
                        finish(peer_adjusted * (1.0 - w) + v * w)
                    }
                    _ => peer_adjusted,
                };
                prediction[1] = if timing {
                    match iso_raw.estimate_retrieval_time(peer, contract).ok() {
                        Some(base_ms) => {
                            let blended = match legacy_abs_raw.predict(&observation) {
                                Some(v) if v.is_finite() && v >= 0.0 => {
                                    let w = weight(legacy_abs_raw.len());
                                    base_ms * (1.0 - w) + v * w
                                }
                                _ => base_ms,
                            };
                            blended.max(1.0).ln()
                        }
                        None => prediction[0],
                    }
                } else {
                    prediction[0]
                };

                let old_rows = |curve: &Curve, re: &Reanchored| -> Option<(f64, Vec<f64>, usize)> {
                    curve.value(distance).map(finish).map(|g| {
                        let snaps = re.snapshots(time);
                        let per: Vec<f64> = (0..HORIZONS.len())
                            .map(|h| {
                                re.hiers[h]
                                    .predict(
                                        snaps[h].as_ref(),
                                        peer_index,
                                        attribute,
                                        contract_value,
                                        distance,
                                    )
                                    .0
                            })
                            .collect();
                        (g, per, re.selected(time))
                    })
                };
                match old_rows(&shrunk_long, &re_attr) {
                    Some((g, per, sel)) => {
                        prediction[3] = finish(g + per[sel]);
                        pend.shrunk = Some((g, per));
                    }
                    None => prediction[3] = global,
                }
                match old_rows(&shrunk_long, &re_noattr) {
                    Some((g, per, sel)) => {
                        prediction[4] = finish(g + per[sel]);
                        pend.noattr = Some((g, per));
                    }
                    None => prediction[4] = global,
                }
                match shrunk_long.value(distance).map(finish) {
                    Some(g) => {
                        let per = re_frozen.predictions(time, peer_index, contract_value, false);
                        let sel = re_frozen.selected(time, 0);
                        prediction[5] = finish(g + per[sel].0);
                        pend.frozen = Some((g, per.iter().map(|p| p.0).collect()));
                    }
                    None => prediction[5] = global,
                }
                match anova_long.value(distance).map(finish) {
                    Some(g) => {
                        let std = re_corrected.predictions(time, peer_index, contract_value, false);
                        let loo = re_corrected.predictions(time, peer_index, contract_value, true);
                        let s = std[re_corrected.selected(time, 0)];
                        let l = loo[re_corrected.selected(time, 1)];
                        prediction[6] = finish(g + s.0);
                        prediction[7] = finish(g + s.0);
                        expectation[7] = (s.2 + s.1) / 2.0;
                        prediction[8] = finish(g + l.0);
                        expectation[8] = (l.2 + l.1) / 2.0;
                        pend.corrected = Some((
                            g,
                            std.iter().map(|p| p.0).collect(),
                            loo.iter().map(|p| p.0).collect(),
                        ));
                    }
                    None => {
                        for row in 6..=8 {
                            prediction[row] = global;
                        }
                    }
                }

                if index >= WARMUP_EVENTS && !op.absent {
                    let in_subset = [
                        true,
                        world.in_pair(peer_index, contract_value),
                        peer_events[peer_index] < COLD_START_EVENTS,
                        world.drift_changed(peer_index) && index >= spec.events / 2,
                        world.near_absent(contract_value),
                    ];
                    let truth_nat = if timing {
                        (truth + TIMING_NOISE_SD * TIMING_NOISE_SD / 2.0).exp() / 1000.0
                    } else {
                        truth
                    };
                    for (subset, &active) in in_subset.iter().enumerate() {
                        if !active {
                            continue;
                        }
                        counts[subset] += 1;
                        for row in 0..rows {
                            err_log[subset][row] += (prediction[row] - truth).powi(2);
                            let natural = if timing {
                                (prediction[row] + expectation[row]).exp() / 1000.0
                            } else {
                                prediction[row]
                            };
                            err_nat[subset][row] += (natural - truth_nat).powi(2);
                        }
                    }
                }
            }

            pending.push(pend);
            peer_events[peer_index] += 1;
            index += 1;
            if y == 0.0 && spec.target == Target::Failure {
                succeeded = true;
                break;
            }
        }

        let learn = match spec.labeling {
            Labeling::All => true,
            Labeling::Delayed => {
                !op.absent
                    && (succeeded || GlobalRng::random_range(0.0..1.0) < CONFIRMED_EXHAUSTED_SHARE)
            }
            Labeling::Untrained => succeeded,
        };
        if !learn {
            continue;
        }
        for pend in &pending {
            let (peer_index, distance, time, y, global) = (
                pend.peer_index,
                pend.distance,
                pend.time,
                pend.y,
                pend.global,
            );
            let peer = &world.peers[peer_index];
            let attribute = world.attribute[peer_index];
            if global.is_some() {
                let id = {
                    let next = peer_ids.len() as u64;
                    *peer_ids.entry(peer_index).or_insert(next)
                };
                let observation = RoutingObservation {
                    peer_id: id as f64,
                    contract_location: contract_value,
                    distance,
                    time,
                };
                legacy_abs.add(observation.clone(), y);
                if legacy_abs.should_train() {
                    legacy_abs.train();
                }
                if timing {
                    legacy_abs_raw.add(observation, y.exp());
                    if legacy_abs_raw.should_train() {
                        legacy_abs_raw.train();
                    }
                }
                let event = Raw {
                    peer: peer_index,
                    attribute,
                    contract: contract_value,
                    distance,
                    time,
                    y,
                };
                if let Some((g, per)) = &pend.shrunk {
                    re_attr.score(per, y - g, time);
                }
                if let Some((g, per)) = &pend.noattr {
                    re_noattr.score(per, y - g, time);
                }
                if let Some((g, per)) = &pend.frozen {
                    re_frozen.score(0, per, y - g, time);
                }
                if let Some((g, std, loo)) = &pend.corrected {
                    re_corrected.score(0, std, y - g, time);
                    re_corrected.score(1, loo, y - g, time);
                }
                raw.push(event);
                if let Some(g) = shrunk_long.value(distance).map(finish) {
                    re_attr.add(&event, g);
                    re_noattr.add(&event, g);
                    re_frozen.add(&event, g);
                }
                if let Some(g) = anova_long.value(distance).map(finish) {
                    re_corrected.add(&event, g);
                }
                learned_since_rebuild += 1;
            }
            iso.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: y,
            });
            if timing {
                iso_raw.add_event(IsotonicEvent {
                    peer: peer.clone(),
                    contract_location: contract,
                    result: y.exp(),
                });
            }
            if global.is_some() && (raw.len() < 100 || learned_since_rebuild >= REBUILD_EVERY) {
                let now = raw.last().map_or(0.0, |r| r.time);
                shrunk_long.refit(&raw, now);
                anova_long.refit(&raw, now);
                re_attr.rebuild(&raw, |r| shrunk_long.value(r.distance).map(finish));
                re_noattr.rebuild(&raw, |r| shrunk_long.value(r.distance).map(finish));
                re_frozen.rebuild(&raw, now, |r| shrunk_long.value(r.distance).map(finish));
                re_corrected.rebuild(&raw, now, |r| anova_long.value(r.distance).map(finish));
                learned_since_rebuild = 0;
            }
        }
    }

    let end = hours(spec.events);
    let comps_of = |re: &ReC| {
        let h = &re.hiers[re.selected(end, 0)];
        h.comps
            .map_or([f64::NAN; 3], |c| [c.sigma2, c.tau2_cell, c.tau2_peer])
    };
    let normalise = |e: Vec<Vec<f64>>| -> Vec<Vec<f64>> {
        e.into_iter()
            .zip(counts)
            .map(|(row, n)| {
                row.into_iter()
                    .map(|v| if n == 0 { f64::NAN } else { v / n as f64 })
                    .collect()
            })
            .collect()
    };
    RevalResult {
        mse_log: normalise(err_log),
        mse_nat: normalise(err_nat),
        counts,
        ranking,
        decisions,
        components: [comps_of(&re_frozen), comps_of(&re_corrected)],
    }
}

/// Natural-units baseline row: legacy blend for failure, legacy as shipped
/// (raw units) for timing.
fn baseline_row(spec: &Spec) -> usize {
    if spec.target == Target::Timing { 1 } else { 0 }
}

fn revalidate_report(seeds: &[u64], title: &str) {
    let mut specs = scenarios();
    let base_count = specs.len();
    specs.extend(prod_scenarios());
    let rows = REVAL_ROWS.len();
    let threads = std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .clamp(1, 8);

    // Cross-check: this loop reproduces the bake-off's stream and rows.
    {
        let _ = PeerKeyLocation::random();
        for name in ["f.mixed", "f.abs5-delayed", "t.mixed"] {
            let spec = *specs.iter().find(|s| s.name == name).expect("scenario");
            let a = run_revalidate(spec, seeds[0]);
            let b = run_bakeoff(spec, seeds[0]);
            for (mine, theirs) in [(0, 0), (1, 1), (2, 2), (3, 27)] {
                assert!(
                    (a.mse_log[0][mine] - b.mse[0][theirs]).abs() < 1e-12,
                    "{name}: row {mine} diverged from bake-off row {theirs}: {} vs {}",
                    a.mse_log[0][mine],
                    b.mse[0][theirs]
                );
            }
        }
    }

    let mut results: Vec<Vec<RevalResult>> = vec![Vec::new(); specs.len()];
    for chunk in (0..specs.len()).collect::<Vec<_>>().chunks(threads) {
        let finished: Vec<(usize, Vec<RevalResult>)> = std::thread::scope(|scope| {
            let handles: Vec<_> = chunk
                .iter()
                .map(|&s| {
                    let spec = specs[s];
                    scope.spawn(move || {
                        let _ = PeerKeyLocation::random();
                        (
                            s,
                            seeds
                                .iter()
                                .map(|&seed| run_revalidate(spec, seed))
                                .collect(),
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("scenario thread panicked"))
                .collect()
        });
        for (s, runs) in finished {
            results[s] = runs;
        }
    }

    let seed_mean = |s: usize, sub: usize, row: usize, natural: bool| {
        let v: Vec<f64> = results[s]
            .iter()
            .filter(|r| r.counts[sub] > 0)
            .map(|r| {
                if natural {
                    r.mse_nat[sub][row]
                } else {
                    r.mse_log[sub][row]
                }
            })
            .collect();
        if v.is_empty() { f64::NAN } else { mean(&v) }
    };

    let mut out = format!(
        "\n#4485 RE-VALIDATION after the PR #5655 statistical review ({title} {seeds:x?})\n\
         natural units: failure probability; timing SECONDS vs E[seconds]. Baseline: legacy \
         blend (failure), legacy as shipped (timing). p.* / pt.* = production-like \
         (200 peers, Zipf, 90% home band, 600 ev/h, 6000 events).\n"
    );
    out.push_str(&format!("{:<44}", "natural-units mse ratio vs baseline"));
    for spec in &specs {
        out.push_str(&format!("{:>16}", spec.name));
    }
    out.push_str("   worst (scenario)   worst-prod   seed-worst\n");
    let mut worst_rows = Vec::new();
    for row in 0..rows {
        out.push_str(&format!("{:<44}", REVAL_ROWS[row]));
        let mut worst = (0.0f64, "");
        let mut worst_prod = 0.0f64;
        let mut seed_worst = 0.0f64;
        for (s, spec) in specs.iter().enumerate() {
            let b = baseline_row(spec);
            let ratio = seed_mean(s, 0, row, true) / seed_mean(s, 0, b, true);
            out.push_str(&format!("{ratio:>16.3}"));
            if ratio > worst.0 {
                worst = (ratio, spec.name);
            }
            if s >= base_count {
                worst_prod = worst_prod.max(ratio);
            }
            for r in &results[s] {
                seed_worst = seed_worst.max(r.mse_nat[0][row] / r.mse_nat[0][b]);
            }
        }
        out.push_str(&format!(
            "   {:.3} ({})   {worst_prod:.3}   {seed_worst:.3}\n",
            worst.0, worst.1
        ));
        worst_rows.push((row, worst, worst_prod));
    }

    out.push_str("\n== absolute natural-units mse (mean over seeds), selected scenarios\n");
    let shown: Vec<usize> = specs
        .iter()
        .enumerate()
        .filter(|(i, s)| {
            *i >= base_count
                || [
                    "f.dist",
                    "f.mixed",
                    "f.drift",
                    "f.rare",
                    "f.ops-untrained",
                    "f.abs20-untrained-uni",
                    "t.dist",
                    "t.peer",
                    "t.mixed",
                    "t.drift",
                    "t.curve-drift*",
                ]
                .contains(&s.name)
        })
        .map(|(i, _)| i)
        .collect();
    out.push_str(&format!("{:<44}", "estimator"));
    for &s in &shown {
        out.push_str(&format!("{:>22}", specs[s].name));
    }
    out.push('\n');
    for row in 0..rows {
        out.push_str(&format!("{:<44}", REVAL_ROWS[row]));
        for &s in &shown {
            out.push_str(&format!("{:>22.6}", seed_mean(s, 0, row, true)));
        }
        out.push('\n');
    }
    out.push_str(&format!(
        "{:<44}",
        "subset (targeted / cold / drifted) counts"
    ));
    for &s in &shown {
        let c: Vec<f64> = (1..4)
            .map(|sub| {
                mean(
                    &results[s]
                        .iter()
                        .map(|r| r.counts[sub] as f64)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        out.push_str(&format!(
            "{:>22}",
            format!("{:.0}/{:.0}/{:.0}", c[0], c[1], c[2])
        ));
    }
    out.push('\n');
    out.push_str(
        "\n== subset natural ratios vs baseline (targeted / cold / drifted), selected scenarios\n",
    );
    for row in 0..rows {
        out.push_str(&format!("{:<44}", REVAL_ROWS[row]));
        for &s in &shown {
            let b = baseline_row(&specs[s]);
            let cell: Vec<String> = (1..4)
                .map(|sub| {
                    let r = seed_mean(s, sub, row, true) / seed_mean(s, sub, b, true);
                    if r.is_finite() {
                        format!("{r:.2}")
                    } else {
                        "-".into()
                    }
                })
                .collect();
            out.push_str(&format!("{:>22}", cell.join("/")));
        }
        out.push('\n');
    }

    out.push_str("\n== RANKING best@10 / regret@10 / best@3 (ops scenarios)\n");
    let rank_specs: Vec<usize> = specs
        .iter()
        .enumerate()
        .filter(|(_, s)| s.ops)
        .map(|(i, _)| i)
        .collect();
    out.push_str(&format!("{:<34}", "estimator"));
    for &s in &rank_specs {
        out.push_str(&format!("{:>24}", specs[s].name));
    }
    out.push('\n');
    for (slot, &(_, label)) in REVAL_RANK_ROWS.iter().enumerate() {
        out.push_str(&format!("{label:<34}"));
        for &s in &rank_specs {
            let per = |k: usize| {
                mean(
                    &results[s]
                        .iter()
                        .map(|r| r.ranking[slot][k] / r.decisions.max(1) as f64)
                        .collect::<Vec<_>>(),
                )
            };
            out.push_str(&format!(
                "{:>24}",
                format!("{:.3}/{:.4}/{:.3}", per(0), per(1), per(2))
            ));
        }
        out.push('\n');
    }

    out.push_str(
        "\n== end-of-run variance components of the selected horizon, [sigma2, tau2_cell, \
         tau2_peer]: original formulas (row 5) vs corrected (rows 6-8), production-like scenarios\n",
    );
    for s in base_count..specs.len() {
        let avg = |which: usize, k: usize| {
            mean(
                &results[s]
                    .iter()
                    .map(|r| r.components[which][k])
                    .collect::<Vec<_>>(),
            )
        };
        out.push_str(&format!(
            "  {:<24} original [{:.4}, {:.5}, {:.5}]   corrected [{:.4}, {:.5}, {:.5}]\n",
            specs[s].name,
            avg(0, 0),
            avg(0, 1),
            avg(0, 2),
            avg(1, 0),
            avg(1, 1),
            avg(1, 2)
        ));
    }

    out.push_str(&format!(
        "\n== VERDICT per estimator (natural units, material = > {MATERIAL_RATIO})\n"
    ));
    for (row, worst, worst_prod) in worst_rows {
        out.push_str(&format!(
            "{:<44} worst {:.3} ({}), worst production-like {:.3}: {}\n",
            REVAL_ROWS[row],
            worst.0,
            worst.1,
            worst_prod,
            if worst.0 > MATERIAL_RATIO {
                "FAILS"
            } else {
                "passes"
            }
        ));
    }

    // Log-space view for continuity with the bake-off's earlier tables.
    out.push_str("\n== log-space (bake-off scoring) worst ratio vs log-space legacy blend\n");
    for row in 0..rows {
        let mut worst = (0.0f64, "");
        for (s, spec) in specs.iter().enumerate() {
            let ratio = seed_mean(s, 0, row, false) / seed_mean(s, 0, 0, false);
            if ratio > worst.0 {
                worst = (ratio, spec.name);
            }
        }
        out.push_str(&format!(
            "{:<44} {:.3} ({})\n",
            REVAL_ROWS[row], worst.0, worst.1
        ));
    }
    eprintln!("{out}");
}

#[test]
fn revalidate_after_statistical_review() {
    revalidate_report(&SEEDS, "original seeds");
}

#[test]
fn revalidate_after_statistical_review_confirmation_seeds() {
    revalidate_report(&CONFIRMATION_SEEDS, "CONFIRMATION seeds");
}
