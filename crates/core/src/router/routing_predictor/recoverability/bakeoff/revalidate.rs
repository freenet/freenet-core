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
//!
//! # Follow-up 4 (fixed before running)
//!
//! - **7b, live root sums.** Row 7 froze `sum n^2` at refit and divided by live
//!   decayed `n^2`. 7b keeps `sum n^2` live, decayed to query time as
//!   production does. `live_root_sums_match_frozen_at_refit_and_diverge_after`
//!   pins that the live sums equal the frozen ones at refit and diverge
//!   afterwards; it was mutation-tested.
//! - **7c, the LOO cancellation guard.** The rest-of-peer sums are built from
//!   the other cells directly, and `tau2_peer` skips peers whose rest is below
//!   `1e-6 * root.n`.
//! - **Production cadence.** Window 10k, refit every `max(50, window/100)`
//!   learned events once full. The `*-long` scenarios (15k events) engage it.
//!   The quiet `q*` scenarios (6 events/h) put refits ~8h apart, where frozen
//!   and live root sums differ most.
//! - **Transfer speed.** Log speed falls with distance; contracts come from a
//!   fixed pool of 64, so identical distances repeat. It is scored as routing
//!   uses it: the expected transfer time of `PAYLOAD_BYTES`, in seconds. The
//!   curve orders ties by the target's PAV direction (`Curve::descending`).
//!
//! # Follow-up 5: legacy re-scored with the fixed `IsotonicEstimator`
//!
//! From commit faf1db7ab (cherry-picked PR #5662), the legacy rows (`a`, `a'`,
//! `b`) run on an isotonic estimator whose rolling window is rebuilt exactly,
//! instead of being patched with `pav_regression`'s `remove_points`, which
//! corrupted the curve between refits. Every row from 5 on reads its prior from
//! `Curve`, a batch fit that was never affected; their MSE is bit-identical
//! before and after. Only the baseline moved.
//!
//! 7c against FIXED legacy: worst 1.041 (`pt.drift`) on the original seeds,
//! against 1.002 before; 0.971 (`pt.drift`) on the confirmation seeds, 0.971
//! before. Legacy's own MSE fell by up to 26% (`p.rare-mixed`), 1-6% in most
//! failure and timing groups, and was flat for speed. 7c's geometric-mean
//! ratio moved by 0.00-0.03 in every group.

use super::*;

/// The production-like modifier: 200 peers, Zipf traffic, home-band locality,
/// 600 events/h over 10 hours.
const PROD_PEERS: usize = 200;
const PROD_EVENTS: usize = 6_000;
const PROD_EVENTS_PER_HOUR: f64 = 600.0;

const NODE_MIN: f64 = 1e-12;

const REVAL_ROWS: [&str; 11] = [
    "a  legacy blend (log-space, exp = median)",
    "a' legacy as shipped (raw units)",
    "b  global curve alone",
    "3  H* EB with attribute (winner, e26a92c1d)",
    "4  H* EB no attribute (PR #5655 shape)",
    "5  no attr, frozen comps, original formulas",
    "6  no attr + corrections 1-3, median timing",
    "7  no attr + corrections 1-3 + E[] timing",
    "8  7 + LOO descent (guarded)",
    "7b 7 with LIVE root squared-count sums",
    "7c 7b + LOO cancellation guard",
];
const ROW_7: usize = 7;
const ROW_7B: usize = 9;
const ROW_7C: usize = 10;

/// Payload for scoring transfer speed as routing uses it: expected transfer
/// time `S / v`.
const PAYLOAD_BYTES: f64 = 1.0e6;

/// Production cadence: once the window is full, refit every
/// `max(REBUILD_EVERY, LONG_WINDOW / 100)` learned events.
fn refit_every(windowed: usize) -> usize {
    if windowed >= LONG_WINDOW {
        (LONG_WINDOW / 100).max(REBUILD_EVERY)
    } else {
        REBUILD_EVERY
    }
}

const REVAL_RANK_ROWS: [(usize, &str); 10] = [
    (0, "a  legacy blend"),
    (2, "b  global curve alone"),
    (3, "3  H* EB with attribute"),
    (4, "4  H* EB no attribute"),
    (5, "5  frozen, original formulas"),
    (6, "6/7 corrections 1-3"),
    (8, "8  + LOO descent"),
    (ROW_7B, "7b live root sums"),
    (ROW_7C, "7c 7b + LOO guard"),
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
        // Follow-up 4 (fixed before running): long runs, so the 10k window
        // fills and production's slower refit cadence engages, and speed.
        long(prod(
            Spec {
                drift_effect: 0.25,
                ..Spec::failure("")
            },
            "p.drift-long",
        )),
        long(prod(
            Spec {
                drift_effect: 0.7,
                ..Spec::timing("")
            },
            "pt.drift-long",
        )),
        prod(Spec::speed(""), "ps.dist"),
        prod(speed_mixed(), "ps.mixed"),
        prod(
            Spec {
                drift_effect: -0.7,
                ..Spec::speed("")
            },
            "ps.drift",
        ),
        long(prod(
            Spec {
                drift_effect: -0.7,
                ..Spec::speed("")
            },
            "ps.drift-long",
        )),
    ]
}

/// Events for the long production-like runs: past the 10k window.
const LONG_EVENTS: usize = 15_000;

fn long(mut spec: Spec) -> Spec {
    spec.events = LONG_EVENTS;
    spec
}

fn speed_mixed() -> Spec {
    Spec {
        marginal_sd: 0.4,
        attribute_step: -0.25,
        pairs: Pairs::Natural,
        pair_effect: -0.8,
        ..Spec::speed("")
    }
}

/// Quiet-node shape (fixed before the follow-up-4 rerun): the base traffic at
/// 6 events/h, so a refit every 50 events is ~8h apart, several multiples of
/// the 1.5h horizon. This is where frozen root sums and live ones diverge; at
/// the other shapes a refit is at most ~50 minutes old.
const QUIET_EVENTS_PER_HOUR: f64 = 6.0;

fn quiet(mut spec: Spec, name: &'static str) -> Spec {
    spec.name = name;
    spec.events_per_hour = QUIET_EVENTS_PER_HOUR;
    spec
}

fn quiet_scenarios() -> Vec<Spec> {
    vec![
        quiet(
            Spec {
                drift_effect: 0.25,
                ..Spec::failure("")
            },
            "q.drift",
        ),
        quiet(
            Spec {
                marginal_sd: 0.08,
                curve_shift: 0.10,
                ..Spec::failure("")
            },
            "q.curve-drift",
        ),
        quiet(
            Spec {
                drift_effect: 0.7,
                ..Spec::timing("")
            },
            "qt.drift",
        ),
        quiet(
            Spec {
                marginal_sd: 0.4,
                curve_shift: 0.5,
                ..Spec::timing("")
            },
            "qt.curve-drift",
        ),
        quiet(
            Spec {
                drift_effect: -0.7,
                ..Spec::speed("")
            },
            "qs.drift",
        ),
    ]
}

/// Transfer-speed scenarios at the bake-off's base traffic shape.
fn speed_scenarios() -> Vec<Spec> {
    vec![
        Spec::speed("s.dist"),
        Spec {
            marginal_sd: 0.4,
            ..Spec::speed("s.peer")
        },
        Spec {
            name: "s.mixed",
            ..speed_mixed()
        },
        Spec {
            drift_effect: -0.7,
            ..Spec::speed("s.drift")
        },
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
    /// Row 7c: rest-of-peer sums built from the other cells directly, and
    /// `tau2_peer` skipped when the rest is below `1e-6 * root.n`.
    guard: bool,
    /// Live decayed `sum_c n_c^2` and `sum_p N_p^2`, as `(value, time)`; each
    /// squared count decays at twice the rate.
    sq_cells: (f64, f64),
    sq_peers: (f64, f64),
    root: Mom,
    peers: HashMap<usize, Mom>,
    cells: HashMap<(usize, usize), Mom>,
    comps: Option<Comps>,
}

fn band_of(contract: f64) -> usize {
    ((contract * HIER_BANDS as f64).floor() as usize).min(HIER_BANDS - 1)
}

impl HierC {
    fn new(decay: Option<f64>, corrected: bool, guard: bool) -> HierC {
        HierC {
            decay,
            corrected,
            guard,
            sq_cells: (0.0, 0.0),
            sq_peers: (0.0, 0.0),
            root: Mom::default(),
            peers: HashMap::new(),
            cells: HashMap::new(),
            comps: None,
        }
    }

    fn squared_at(&self, (value, t0): (f64, f64), now: f64) -> f64 {
        value
            * self
                .decay
                .map_or(1.0, |h| (-2.0 * (now - t0).max(0.0) / h).exp())
    }

    fn add(&mut self, peer: usize, contract: f64, t: f64, r: f64) {
        let d = self.decay;
        let key = (peer, band_of(contract));
        let cell_n = self.cells.get(&key).map_or(0.0, |m| m.at(t, d).n);
        let peer_n = self.peers.get(&peer).map_or(0.0, |m| m.at(t, d).n);
        self.sq_cells = (self.squared_at(self.sq_cells, t) + 2.0 * cell_n + 1.0, t);
        self.sq_peers = (self.squared_at(self.sq_peers, t) + 2.0 * peer_n + 1.0, t);
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

        let tau2_cell = if self.corrected && self.guard {
            let mut by_peer: HashMap<usize, Vec<Mom>> = HashMap::new();
            for (&(p, _), m) in &cells {
                by_peer.entry(p).or_default().push(*m);
            }
            let (mut num, mut den) = (0.0, 0.0);
            for list in by_peer.values() {
                for (i, c) in list.iter().enumerate() {
                    let (mut rest_n, mut rest_sum, mut rest_w2, mut rest_sq) = (0.0, 0.0, 0.0, 0.0);
                    for (j, other) in list.iter().enumerate() {
                        if j != i {
                            rest_n += other.n;
                            rest_sum += other.sum;
                            rest_w2 += other.w2;
                            rest_sq += other.n * other.n;
                        }
                    }
                    if !self.gated(c) || rest_n <= NODE_MIN {
                        continue;
                    }
                    num += (c.mean() - rest_sum / rest_n).powi(2)
                        - sigma2 * (c.w2 / (c.n * c.n) + rest_w2 / (rest_n * rest_n));
                    den += 1.0 + rest_sq / (rest_n * rest_n);
                }
            }
            if den > 0.0 { (num / den).max(0.0) } else { 0.0 }
        } else if self.corrected {
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
                let floor = if self.guard { 1e-6 * root.n } else { NODE_MIN };
                if !self.gated(m) || rest_n <= floor {
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
    fn predict(
        &self,
        now: f64,
        peer: usize,
        contract: f64,
        loo: bool,
        live: bool,
    ) -> (f64, f64, f64) {
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
        } else if live {
            (
                root.n,
                root.sum,
                root.w2,
                self.squared_at(self.sq_peers, now),
                self.squared_at(self.sq_cells, now),
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
    /// Decayed loss per horizon for `[standard, loo, live]` prediction.
    loss: [Vec<(f64, f64)>; 3],
}

const MODE_STANDARD: usize = 0;
const MODE_LOO: usize = 1;
const MODE_LIVE: usize = 2;

impl ReC {
    fn new(corrected: bool, guard: bool) -> ReC {
        ReC {
            hiers: HORIZONS
                .iter()
                .map(|&h| HierC::new(h, corrected, guard))
                .collect(),
            loss: [
                vec![(0.0, 0.0); HORIZONS.len()],
                vec![(0.0, 0.0); HORIZONS.len()],
                vec![(0.0, 0.0); HORIZONS.len()],
            ],
        }
    }

    fn rebuild(&mut self, raw: &[Raw], now: f64, prior: impl Fn(&Raw) -> Option<f64>) {
        for (hier, &h) in self.hiers.iter_mut().zip(HORIZONS.iter()) {
            *hier = HierC::new(h, hier.corrected, hier.guard);
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

    fn predictions(
        &self,
        now: f64,
        peer: usize,
        contract: f64,
        mode: usize,
    ) -> Vec<(f64, f64, f64)> {
        self.hiers
            .iter()
            .map(|h| h.predict(now, peer, contract, mode == MODE_LOO, mode == MODE_LIVE))
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
    /// `[subset][row]`: log-space for timing and speed.
    mse_log: Vec<Vec<f64>>,
    /// `[subset][row]`: natural units. Failure: probability. Timing: seconds
    /// vs `E[seconds]`. Speed: expected transfer time of `PAYLOAD_BYTES` in
    /// seconds vs `S * E[1/v]`.
    mse_nat: Vec<Vec<f64>>,
    counts: [usize; SUBSETS.len()],
    ranking: [[f64; 6]; REVAL_RANK_ROWS.len()],
    decisions: usize,
    /// End-of-run components of the selected-horizon hierarchy:
    /// `[sigma2, tau2_cell, tau2_peer]` for original (row 5) then corrected.
    components: [[f64; 3]; 2],
    /// Speed: scored events where legacy's blended raw speed was not positive
    /// (production falls back to "sorts last"; here the global curve is used).
    legacy_speed_fallbacks: usize,
    /// Over scored events, `(sum |p_7b - p_7|, max, sum |p_7c - p_7b|, max)` on
    /// the model scale (probability, or log units), so a "no difference" result
    /// can be told apart from a harness that never exercises the difference.
    deltas: [f64; 4],
    /// Refits after the window filled (production's slower cadence engaged).
    slow_cadence_refits: usize,
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
    /// `(prior, standard, loo, live)` residual predictions per horizon.
    corrected: Option<(f64, Vec<f64>, Vec<f64>, Vec<f64>)>,
    /// `(prior, live)` for the guarded hierarchy.
    guarded: Option<(f64, Vec<f64>)>,
}

/// `lean` skips rows 3 and 4 (the reference code's per-prediction component
/// recomputation, the expensive part), reporting them as NaN.
fn run_revalidate(spec: Spec, seed: u64, lean: bool) -> RevalResult {
    let _guard = GlobalRng::seed_guard(seed);
    let world = World::new(spec);
    let rows = REVAL_ROWS.len();
    let log_target = spec.target != Target::Failure;
    let speed = spec.target == Target::Speed;
    let direction = if speed {
        EstimatorType::Negative
    } else {
        EstimatorType::Positive
    };

    let mut iso = IsotonicEstimator::new(Vec::new(), direction);
    // As shipped: response time multiplicative on raw seconds (here ms),
    // transfer speed additive on raw bytes/s, both from the router.
    let mut iso_raw = IsotonicEstimator::new_with_mode(
        Vec::new(),
        direction,
        if speed {
            AdjustmentMode::Additive
        } else {
            AdjustmentMode::Multiplicative
        },
    );
    let mut legacy_abs = PredictionStage::new(10_000);
    let mut legacy_abs_raw = PredictionStage::new(10_000);
    let mut peer_ids: HashMap<usize, u64> = HashMap::new();
    let mut peer_events = vec![0usize; spec.peers];
    let (weight_ramp, clamp_unit) = if log_target {
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
    let mut shrunk_long = Curve::new(None, true).with_direction(speed);
    let mut anova_long = Curve::new_anova().with_direction(speed);
    let mut re_attr = Reanchored::new(attr_config, &HORIZONS);
    let mut re_noattr = Reanchored::new(noattr_config, &HORIZONS);
    let mut re_frozen = ReC::new(false, false);
    let mut re_corrected = ReC::new(true, false);
    let mut re_guarded = ReC::new(true, true);
    let mut raw: Vec<Raw> = Vec::new();
    let mut learned_since_rebuild = 0usize;
    let mut slow_cadence_refits = 0usize;
    let mut legacy_speed_fallbacks = 0usize;
    let mut deltas = [0.0f64; 4];

    let mut err_log = vec![vec![0.0; rows]; SUBSETS.len()];
    let mut err_nat = vec![vec![0.0; rows]; SUBSETS.len()];
    let mut counts = [0usize; SUBSETS.len()];
    let mut ranking = [[0.0f64; 6]; REVAL_RANK_ROWS.len()];
    let mut decisions = 0usize;
    let mut pending: Vec<PendingC> = Vec::new();
    let hours = |index: usize| index as f64 / spec.events_per_hour;
    // Log-space prediction plus expectation adjustment -> natural units.
    let natural = |log_value: f64, adjust: f64| match spec.target {
        Target::Failure => log_value,
        Target::Timing => (log_value + adjust).exp() / 1000.0,
        Target::Speed => PAYLOAD_BYTES * (-log_value + adjust).exp(),
    };
    let half_noise = TIMING_NOISE_SD * TIMING_NOISE_SD / 2.0;
    let mut index = 0usize;

    while index < spec.events {
        let op = world.next_op(index);
        let contract_value = op.contract;
        let contract = Location::try_from(contract_value).expect("contract within ring");

        if spec.ops && !op.absent && index >= WARMUP_EVENTS {
            let time = hours(index);
            let candidates = world.nearest_peers(contract_value);
            let snaps_attr = if lean {
                Vec::new()
            } else {
                re_attr.snapshots(time)
            };
            let sel_attr = re_attr.selected(time);
            let snaps_noattr = if lean {
                Vec::new()
            } else {
                re_noattr.snapshots(time)
            };
            let sel_noattr = re_noattr.selected(time);
            let sel_frozen = re_frozen.selected(time, MODE_STANDARD);
            let sel_corr = re_corrected.selected(time, MODE_STANDARD);
            let sel_loo = re_corrected.selected(time, MODE_LOO);
            let sel_live = re_corrected.selected(time, MODE_LIVE);
            let sel_guard = re_guarded.selected(time, MODE_LIVE);
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
                    if lean {
                        return f64::NAN;
                    }
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
                let new = |curve: &Curve, re: &ReC, h: usize, mode: usize| {
                    curve.value(distance).map(finish).map_or(global, |g| {
                        finish(
                            g + re.hiers[h]
                                .predict(
                                    time,
                                    cand,
                                    contract_value,
                                    mode == MODE_LOO,
                                    mode == MODE_LIVE,
                                )
                                .0,
                        )
                    })
                };
                for (slot, &(row, _)) in REVAL_RANK_ROWS.iter().enumerate() {
                    scores[slot].push(match row {
                        0 => legacy,
                        2 => global,
                        3 => old(&shrunk_long, &re_attr, &snaps_attr, sel_attr),
                        4 => old(&shrunk_long, &re_noattr, &snaps_noattr, sel_noattr),
                        5 => new(&shrunk_long, &re_frozen, sel_frozen, MODE_STANDARD),
                        6 => new(&anova_long, &re_corrected, sel_corr, MODE_STANDARD),
                        8 => new(&anova_long, &re_corrected, sel_loo, MODE_LOO),
                        ROW_7B => new(&anova_long, &re_corrected, sel_live, MODE_LIVE),
                        ROW_7C => new(&anova_long, &re_guarded, sel_guard, MODE_LIVE),
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
                guarded: None,
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
                let mut fallback = false;
                prediction[1] = if log_target {
                    match iso_raw.estimate_retrieval_time(peer, contract).ok() {
                        Some(base_raw) => {
                            let blended = match legacy_abs_raw.predict(&observation) {
                                Some(v) if v.is_finite() && v >= 0.0 => {
                                    let w = weight(legacy_abs_raw.len());
                                    base_raw * (1.0 - w) + v * w
                                }
                                _ => base_raw,
                            };
                            if speed {
                                if blended > 0.0 {
                                    blended.ln()
                                } else {
                                    fallback = true;
                                    global
                                }
                            } else {
                                blended.max(1.0).ln()
                            }
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
                if lean {
                    prediction[3] = f64::NAN;
                    prediction[4] = f64::NAN;
                } else {
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
                }
                match shrunk_long.value(distance).map(finish) {
                    Some(g) => {
                        let per =
                            re_frozen.predictions(time, peer_index, contract_value, MODE_STANDARD);
                        let sel = re_frozen.selected(time, MODE_STANDARD);
                        prediction[5] = finish(g + per[sel].0);
                        pend.frozen = Some((g, per.iter().map(|p| p.0).collect()));
                    }
                    None => prediction[5] = global,
                }
                match anova_long.value(distance).map(finish) {
                    Some(g) => {
                        let std = re_corrected.predictions(
                            time,
                            peer_index,
                            contract_value,
                            MODE_STANDARD,
                        );
                        let loo =
                            re_corrected.predictions(time, peer_index, contract_value, MODE_LOO);
                        let live =
                            re_corrected.predictions(time, peer_index, contract_value, MODE_LIVE);
                        let guarded =
                            re_guarded.predictions(time, peer_index, contract_value, MODE_LIVE);
                        let s = std[re_corrected.selected(time, MODE_STANDARD)];
                        let l = loo[re_corrected.selected(time, MODE_LOO)];
                        let b = live[re_corrected.selected(time, MODE_LIVE)];
                        let c = guarded[re_guarded.selected(time, MODE_LIVE)];
                        prediction[6] = finish(g + s.0);
                        prediction[ROW_7] = finish(g + s.0);
                        expectation[ROW_7] = (s.2 + s.1) / 2.0;
                        prediction[8] = finish(g + l.0);
                        expectation[8] = (l.2 + l.1) / 2.0;
                        prediction[ROW_7B] = finish(g + b.0);
                        expectation[ROW_7B] = (b.2 + b.1) / 2.0;
                        prediction[ROW_7C] = finish(g + c.0);
                        expectation[ROW_7C] = (c.2 + c.1) / 2.0;
                        let firsts =
                            |v: &[(f64, f64, f64)]| v.iter().map(|p| p.0).collect::<Vec<_>>();
                        pend.corrected = Some((g, firsts(&std), firsts(&loo), firsts(&live)));
                        pend.guarded = Some((g, firsts(&guarded)));
                    }
                    None => {
                        for row in [6, ROW_7, 8, ROW_7B, ROW_7C] {
                            prediction[row] = global;
                        }
                    }
                }

                if index >= WARMUP_EVENTS && !op.absent {
                    legacy_speed_fallbacks += usize::from(fallback);
                    let d_live = (prediction[ROW_7B] - prediction[ROW_7]).abs();
                    let d_guard = (prediction[ROW_7C] - prediction[ROW_7B]).abs();
                    deltas[0] += d_live;
                    deltas[1] = deltas[1].max(d_live);
                    deltas[2] += d_guard;
                    deltas[3] = deltas[3].max(d_guard);
                    let in_subset = [
                        true,
                        world.in_pair(peer_index, contract_value),
                        peer_events[peer_index] < COLD_START_EVENTS,
                        world.drift_changed(peer_index) && index >= spec.events / 2,
                        world.near_absent(contract_value),
                    ];
                    let truth_nat = natural(truth, half_noise);
                    for (subset, &active) in in_subset.iter().enumerate() {
                        if !active {
                            continue;
                        }
                        counts[subset] += 1;
                        for row in 0..rows {
                            err_log[subset][row] += (prediction[row] - truth).powi(2);
                            err_nat[subset][row] +=
                                (natural(prediction[row], expectation[row]) - truth_nat).powi(2);
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
                if log_target {
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
                    re_frozen.score(MODE_STANDARD, per, y - g, time);
                }
                if let Some((g, std, loo, live)) = &pend.corrected {
                    re_corrected.score(MODE_STANDARD, std, y - g, time);
                    re_corrected.score(MODE_LOO, loo, y - g, time);
                    re_corrected.score(MODE_LIVE, live, y - g, time);
                }
                if let Some((g, live)) = &pend.guarded {
                    re_guarded.score(MODE_LIVE, live, y - g, time);
                }
                raw.push(event);
                if let Some(g) = shrunk_long.value(distance).map(finish) {
                    if !lean {
                        re_attr.add(&event, g);
                        re_noattr.add(&event, g);
                    }
                    re_frozen.add(&event, g);
                }
                if let Some(g) = anova_long.value(distance).map(finish) {
                    re_corrected.add(&event, g);
                    re_guarded.add(&event, g);
                }
                learned_since_rebuild += 1;
            }
            iso.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: y,
            });
            if log_target {
                iso_raw.add_event(IsotonicEvent {
                    peer: peer.clone(),
                    contract_location: contract,
                    result: y.exp(),
                });
            }
            let cadence = refit_every(raw.len());
            if global.is_some() && (raw.len() < 100 || learned_since_rebuild >= cadence) {
                if cadence > REBUILD_EVERY {
                    slow_cadence_refits += 1;
                }
                // Hierarchies see the same window as the curves.
                let window = &raw[raw.len().saturating_sub(LONG_WINDOW)..];
                let now = raw.last().map_or(0.0, |r| r.time);
                shrunk_long.refit(&raw, now);
                anova_long.refit(&raw, now);
                if !lean {
                    re_attr.rebuild(window, |r| shrunk_long.value(r.distance).map(finish));
                    re_noattr.rebuild(window, |r| shrunk_long.value(r.distance).map(finish));
                }
                re_frozen.rebuild(window, now, |r| shrunk_long.value(r.distance).map(finish));
                re_corrected.rebuild(window, now, |r| anova_long.value(r.distance).map(finish));
                re_guarded.rebuild(window, now, |r| anova_long.value(r.distance).map(finish));
                learned_since_rebuild = 0;
            }
        }
    }

    let end = hours(spec.events);
    let comps_of = |re: &ReC| {
        let h = &re.hiers[re.selected(end, MODE_STANDARD)];
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
        legacy_speed_fallbacks,
        deltas,
        slow_cadence_refits,
    }
}

/// Natural-units baseline row: legacy blend for failure, legacy as shipped
/// (raw units) for timing and speed.
fn baseline_row(spec: &Spec) -> usize {
    if spec.target == Target::Failure { 0 } else { 1 }
}

fn revalidate_report(seeds: &[u64], title: &str, lean: bool) {
    let mut specs = scenarios();
    specs.extend(speed_scenarios());
    specs.extend(quiet_scenarios());
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
            let a = run_revalidate(spec, seeds[0], false);
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
                                .map(|&seed| run_revalidate(spec, seed, lean))
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
    let ratio = |s: usize, sub: usize, row: usize| {
        seed_mean(s, sub, row, true) / seed_mean(s, sub, baseline_row(&specs[s]), true)
    };

    let mut out = format!(
        "\n#4485 RE-VALIDATION ({title} {seeds:x?}{})\n\
         natural units: failure probability; timing SECONDS vs E[seconds]; speed = expected \
         transfer time of {PAYLOAD_BYTES} bytes in SECONDS vs S*E[1/v]. Baseline: legacy blend \
         (failure), legacy as shipped (timing, speed). p.* / pt.* / ps.* = production-like \
         (200 peers, Zipf, 90% home band, 600 ev/h; *-long = {LONG_EVENTS} events, window fills).\n",
        if lean { ", lean: rows 3-4 skipped" } else { "" }
    );
    out.push_str(&format!("{:<44}", "natural-units mse ratio vs baseline"));
    for spec in &specs {
        out.push_str(&format!("{:>16}", spec.name));
    }
    out.push_str("   worst (scenario)   worst-prod   worst-speed   seed-worst\n");
    let mut worst_rows = Vec::new();
    for row in 0..rows {
        out.push_str(&format!("{:<44}", REVAL_ROWS[row]));
        let mut worst = (0.0f64, "");
        let (mut worst_prod, mut worst_speed, mut seed_worst) = (0.0f64, 0.0f64, 0.0f64);
        for (s, spec) in specs.iter().enumerate() {
            let r = ratio(s, 0, row);
            out.push_str(&format!("{r:>16.3}"));
            if r > worst.0 {
                worst = (r, spec.name);
            }
            if s >= base_count {
                worst_prod = worst_prod.max(r);
            }
            if spec.target == Target::Speed {
                worst_speed = worst_speed.max(r);
            }
            let b = baseline_row(spec);
            for run in &results[s] {
                seed_worst = seed_worst.max(run.mse_nat[0][row] / run.mse_nat[0][b]);
            }
        }
        out.push_str(&format!(
            "   {:.3} ({})   {worst_prod:.3}   {worst_speed:.3}   {seed_worst:.3}\n",
            worst.0, worst.1
        ));
        worst_rows.push((row, worst, worst_prod, worst_speed));
    }

    // The rows this follow-up is about, one line per scenario.
    out.push_str(
        "\n== rows 7 / 7b / 7c per scenario: natural ratio vs baseline, then subset ratios \
         (targeted / cold / drifted) for 7c, then natural mse of baseline and 7c\n",
    );
    for (s, spec) in specs.iter().enumerate() {
        let subsets: Vec<String> = (1..4)
            .map(|sub| {
                let r = ratio(s, sub, ROW_7C);
                if r.is_finite() {
                    format!("{r:.2}")
                } else {
                    "-".into()
                }
            })
            .collect();
        out.push_str(&format!(
            "  {:<24} 7 {:.3}  7b {:.3}  7c {:.3}   7c subsets {:<16}  baseline {:.6}  7c {:.6}\n",
            spec.name,
            ratio(s, 0, ROW_7),
            ratio(s, 0, ROW_7B),
            ratio(s, 0, ROW_7C),
            subsets.join("/"),
            seed_mean(s, 0, baseline_row(spec), true),
            seed_mean(s, 0, ROW_7C, true),
        ));
    }

    out.push_str(
        "\n== prediction differences on the model scale, mean / max over scored events and seeds: \
         |7b - 7| (live root sums), |7c - 7b| (LOO guard)\n",
    );
    for (s, spec) in specs.iter().enumerate() {
        let scored: f64 = results[s].iter().map(|r| r.counts[0] as f64).sum();
        let sum = |k: usize| results[s].iter().map(|r| r.deltas[k]).sum::<f64>();
        let max = |k: usize| results[s].iter().map(|r| r.deltas[k]).fold(0.0, f64::max);
        out.push_str(&format!(
            "  {:<24} live {:.2e} / {:.2e}   guard {:.2e} / {:.2e}\n",
            spec.name,
            sum(0) / scored.max(1.0),
            max(1),
            sum(2) / scored.max(1.0),
            max(3)
        ));
    }

    out.push_str("\n== transfer speed (natural = expected transfer time, seconds), all rows\n");
    for (s, spec) in specs.iter().enumerate() {
        if spec.target != Target::Speed {
            continue;
        }
        out.push_str(&format!("-- {}: ", spec.name));
        for row in 0..rows {
            out.push_str(&format!(
                "[{}] {:.3} (mse {:.4})  ",
                &REVAL_ROWS[row][..2],
                ratio(s, 0, row),
                seed_mean(s, 0, row, true)
            ));
        }
        let fallbacks: usize = results[s].iter().map(|r| r.legacy_speed_fallbacks).sum();
        out.push_str(&format!(
            "legacy non-positive speed fallbacks: {fallbacks}\n"
        ));
    }

    out.push_str("\n== slow-cadence refits (window full) per run, long scenarios\n");
    for (s, spec) in specs.iter().enumerate() {
        if spec.events > LONG_WINDOW {
            let n: Vec<usize> = results[s].iter().map(|r| r.slow_cadence_refits).collect();
            out.push_str(&format!("  {}: {n:?}\n", spec.name));
        }
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
    for (row, worst, worst_prod, worst_speed) in worst_rows {
        out.push_str(&format!(
            "{:<44} worst {:.3} ({}), production-like {:.3}, speed {:.3}: {}\n",
            REVAL_ROWS[row],
            worst.0,
            worst.1,
            worst_prod,
            worst_speed,
            if !worst.0.is_finite() || worst.0 == 0.0 {
                "skipped"
            } else if worst.0 > MATERIAL_RATIO {
                "FAILS"
            } else {
                "passes"
            }
        ));
    }
    eprintln!("{out}");
}

/// The live squared-count sums must equal the frozen ones at the moment of a
/// refit, and the root step must then diverge between the two as time passes
/// with a non-zero root residual mean. Guards against 7b being vacuously equal
/// to 7 because the live path is miswired.
#[test]
fn live_root_sums_match_frozen_at_refit_and_diverge_after() {
    let mut hier = HierC::new(Some(1.5), true, false);
    let mut t = 0.0;
    for i in 0..400usize {
        t += 0.02;
        let peer = i % 7;
        let contract = ((i * 37) % 64) as f64 / 64.0;
        // Non-zero root mean, peer and cell structure, deterministic.
        let r = 0.3 + 0.05 * peer as f64 + if (i * 13) % 5 == 0 { 0.2 } else { -0.05 };
        hier.add(peer, contract, t, r);
    }
    hier.refresh(t);
    let comps = hier.comps.expect("components");
    let live_cells = hier.squared_at(hier.sq_cells, t);
    let live_peers = hier.squared_at(hier.sq_peers, t);
    assert!(
        (live_cells - comps.s_cells).abs() <= 1e-9 * comps.s_cells,
        "sum n_c^2: live {live_cells} vs frozen {}",
        comps.s_cells
    );
    assert!(
        (live_peers - comps.s_peers).abs() <= 1e-9 * comps.s_peers,
        "sum N_p^2: live {live_peers} vs frozen {}",
        comps.s_peers
    );
    // A peer with no node: its prediction is the root step alone, which is
    // where the root noise term decides the answer. (Peer 3 has its own node,
    // which dominates and hides most of the difference.)
    let unseen = 99;
    let at_refit = (
        hier.predict(t, unseen, 0.4, false, false).0,
        hier.predict(t, unseen, 0.4, false, true).0,
    );
    assert!((at_refit.0 - at_refit.1).abs() < 1e-12, "{at_refit:?}");
    let later = t + 2.5;
    let (frozen, live) = (
        hier.predict(later, unseen, 0.4, false, false).0,
        hier.predict(later, unseen, 0.4, false, true).0,
    );
    let (seen_frozen, seen_live) = (
        hier.predict(later, 3, 0.4, false, false).0,
        hier.predict(later, 3, 0.4, false, true).0,
    );
    assert!(
        (frozen - live).abs() > 1e-3,
        "frozen {frozen} vs live {live} 2.5h after a refit"
    );
    eprintln!(
        "root sums, 2.5h after refit: unseen peer frozen {frozen:.4} vs live {live:.4}; \
         seen peer frozen {seen_frozen:.4} vs live {seen_live:.4}"
    );
}

#[test]
fn revalidate_after_statistical_review() {
    revalidate_report(&SEEDS, "original seeds", false);
}

#[test]
fn revalidate_after_statistical_review_confirmation_seeds() {
    revalidate_report(&CONFIRMATION_SEEDS, "CONFIRMATION seeds", false);
}

/// Follow-up 4: live root sums (7b), LOO cancellation guard (7c), production
/// refit cadence, transfer speed. Lean: rows 3-4 skipped.
#[test]
fn revalidate_live_sums_guard_and_speed() {
    revalidate_report(&SEEDS, "original seeds", true);
}

#[test]
fn revalidate_live_sums_guard_and_speed_confirmation_seeds() {
    revalidate_report(&CONFIRMATION_SEEDS, "CONFIRMATION seeds", true);
}
