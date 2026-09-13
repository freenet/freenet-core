//! #4485 estimator bake-off over a broadened, a-priori-fixed scenario set.
//!
//! Research harness, not a gate. Every estimator sees the SAME event stream,
//! predicts before the event is learned (prequential), and — except the two
//! legacy rows — is composed as `global + r_hat` on the target's own scale:
//! failure probability additively (clamped to `[0, 1]`), response time in
//! log-milliseconds additively (i.e. multiplicatively in seconds). There is no
//! fixed blend weight anywhere outside the legacy rows.
//!
//! # What was fixed BEFORE any run, and not revisited
//!
//! Scenario parameters (`scenarios()`), the estimator roster
//! (`ROW_LABELS`), the band count (8), the decay horizon (24h, the same scale
//! the Gower time feature already saturates at), the cold-start threshold (10
//! prior events), the baseline (the legacy blend; for timing, the legacy blend
//! computed in log space, which is the STRONGER of the two legacy variants in
//! principle because it cannot pay the arithmetic-vs-geometric-mean bias), and
//! the materiality threshold for "worse than legacy" (`MATERIAL_RATIO`).
//!
//! Scoring is mean squared error against the generating truth: `p*` for
//! failure, `mu* = E[ln ms]` for timing. Label-noise scenarios (`f.abs*`,
//! `f.ops-*`) score existing-contract attempts only.
//!
//! # Run 1 (a priori roster): nothing passed
//!
//! Every estimator was materially worse than legacy somewhere. Three failure
//! classes, each diagnosed from the tables rather than tuned away:
//!
//! - **Global-curve variance.** The 500-event isotonic curve is noisy (on
//!   `f.rare` a PAV block resting on five events reads one failure as 20%).
//!   Legacy survives partly because its 50% renegade blend dilutes that noise;
//!   every no-blend estimator composes 100% on it.
//! - **Stale residuals.** Residuals were stored against the curve that existed
//!   when each event was recorded, so the correction models a curve that no
//!   longer exists.
//! - **Adaptation.** Legacy's per-peer EWMA (alpha 0.1) tracks timing drift
//!   within ~10 events; a lifetime or 24h-decayed hierarchy cannot.
//!
//! # Run 2/3 (post hoc, marked `*`): fixes for exactly those three
//!
//! Rows and scenarios marked `*` were added AFTER run 1. To limit
//! forking-paths risk they were checked on `CONFIRMATION_SEEDS`, which were
//! never used for diagnosis, and against two adversarial scenarios
//! (`*curve-drift`) designed to penalise the long-window prior. The fixes are
//! a long-window curve with empirical-Bayes shrinkage of its PAV blocks,
//! residuals re-anchored to the current curve at every refit, a root level
//! that absorbs curve lag, and a forgetting horizon chosen online by
//! prequential loss.
//!
//! Only `*d H* on EB-shrunk long curve` met the criterion on both seed sets,
//! and only just: worst overall ratio 1.096 (original) and 1.098
//! (confirmation) against a 1.10 threshold, both in timing drift. It was
//! materially better in 21 of 24 scenarios. It does NOT beat legacy on
//! narrow peer x contract pairs (targeted subset 1.0-1.6x, small samples).
//! Adding kNN on top recovers those pairs but fails the worst case (1.25-1.29,
//! `t.drift`). A joint curve+hierarchy horizon was worse under curve drift
//! (1.57) than letting the root level absorb it.
//!
//! # Follow-up: uniform absent keys, hot key, ranking (fixed before running)
//!
//! Naive labeling does NOT wash out when absent keys are uniform. MSE rises
//! 1.5-1.8x (5% absent) and 6-9x (20%) over the clean level, about as much as
//! with clustered keys. Ranking (`RANKING` table, lowest-predicted among the
//! 10 nearest peers matching the lowest-`p*` peer) also falls. Naive vs
//! delayed at the same absent share, which holds the event budget equal:
//! best@10 is 7-11 points lower under naive, uniform or clustered. A 1% hot
//! key is negligible either way. Caveat: absent ops consume attempts from the
//! fixed event budget, so every noisy scenario also learns from fewer
//! existing-contract events. Compare naive with delayed at equal share, not
//! with clean.

use super::*;

/// Events per run. Same production-sized budget as the rest of the harness.
const EVENTS: usize = RECOVERY_BUDGET_EVENTS;
const PEERS_UNIFORM: usize = 30;
const PEERS_LONG_TAIL: usize = 80;
/// Zipf exponent over peer rank for long-tail traffic.
const ZIPF_EXPONENT: f64 = 1.0;
/// Lognormal noise of a single response time, in natural-log units.
const TIMING_NOISE_SD: f64 = 0.5;
const ATTRIBUTE_BUCKETS: usize = 3;
/// A peer with fewer prior events than this is "cold" for the subset metric.
const COLD_START_EVENTS: usize = 10;
/// Half-width of a naturally-sampled bad contract band.
const NATURAL_BAND_HALF_WIDTH: f64 = 0.05;
const HIER_BANDS: usize = 8;
/// Forgetting horizon for the decayed hierarchical variants, in harness hours
/// (60 events per hour, so ~1440 events).
const DECAY_HOURS: f64 = 24.0;
/// An estimator whose mse exceeds legacy's by more than this factor is
/// "materially worse". Fixed before any run.
const MATERIAL_RATIO: f64 = 1.10;
/// Label-noise scenarios (all fixed a priori).
const MAX_ATTEMPTS: usize = 3;
const OP_CANDIDATES: usize = 6;
const ABSENT_CLUSTERS: usize = 3;
const ABSENT_HALF_WIDTH: f64 = 0.02;
/// Existing-contract attempts within this ring distance of an absent cluster
/// centre form the `near-abs` subset.
const NEAR_ABSENT: f64 = 0.05;
const CONFIRMED_EXHAUSTED_SHARE: f64 = 0.5;

// Post-hoc additions after run 1 (see the module docs' "Run 2" section).
/// Learned events between refits of the long-window curve and rebuilds of the
/// re-anchored hierarchies: the isotonic estimator's own refit cadence at
/// saturation (10% of its 500-point window).
const REBUILD_EVERY: usize = 50;
/// Cap on the long-window curve's history: the renegade stages' own cap.
const LONG_WINDOW: usize = 10_000;
/// Forgetting horizons the re-anchored hierarchy selects between online by
/// prequential loss; powers of four down from the a-priori 24h.
const HORIZONS: [Option<f64>; 4] = [None, Some(24.0), Some(6.0), Some(1.5)];
/// Forgetting of the horizon selector's accumulated loss.
const SELECTOR_FORGETTING_HOURS: f64 = DECAY_HOURS;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Target {
    Failure,
    Timing,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Base {
    /// 5% at distance 0 rising to 40% at 0.5 (NotFound counted as failure).
    Realistic,
    /// 1% rising to 2%.
    Rare,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Pairs {
    None,
    /// The original harness's three targeted pairs, every 12th event.
    Oversampled,
    /// 20% of peers (index % 5 == 2) have one bad band of half-width 0.05 at a
    /// random contract location; traffic to it is whatever uniform contract
    /// sampling gives it.
    Natural,
}

#[derive(Clone, Copy, Debug)]
struct Spec {
    name: &'static str,
    target: Target,
    base: Base,
    peers: usize,
    zipf: bool,
    /// Standard deviation of a per-peer offset (probability or ln units).
    marginal_sd: f64,
    /// Effect per attribute bucket (bucket 0, 1, 2 -> 0, 1x, 2x).
    attribute_step: f64,
    pairs: Pairs,
    pair_effect: f64,
    /// `> 0`: peers %4==0 carry this until mid-run, then recover, while peers
    /// %4==1 acquire it.
    drift_effect: f64,
    /// Operation mode: each op picks a contract, ranks a random candidate
    /// sample of `OP_CANDIDATES` peers by distance, and attempts the closest
    /// up to `MAX_ATTEMPTS` times, stopping at the first success. Otherwise
    /// each event is one independent attempt with a traffic-weighted peer.
    ops: bool,
    /// Share of ops targeting a contract that does not exist (every attempt
    /// fails). Absent keys cluster near `ABSENT_CLUSTERS` locations.
    absent_share: f64,
    absent_layout: AbsentLayout,
    labeling: Labeling,
    /// `> 0`: the GLOBAL distance curve shifts up by this from mid-run on,
    /// for every peer. Added after run 1 as an adversarial check on the
    /// post-hoc long-window prior, which it should penalise.
    curve_shift: f64,
}

/// Where absent-contract keys fall (follow-up run, fixed before running).
#[derive(Clone, Copy, PartialEq, Debug)]
enum AbsentLayout {
    /// Near `ABSENT_CLUSTERS` locations, half-width `ABSENT_HALF_WIDTH`.
    Clustered,
    /// Uniform over the ring: the "blameless NotFounds wash out" assumption.
    Uniform,
    /// Every absent op polls exactly one key.
    HotKey,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Labeling {
    /// Every attempt is trained, absent-contract failures included.
    All,
    /// Absent ops are never trained; an exhausted op on an EXISTING contract
    /// is trained (all its attempts) only with probability
    /// `CONFIRMED_EXHAUSTED_SHARE`; ops that end in success are always trained.
    Delayed,
}

impl Spec {
    const fn failure(name: &'static str) -> Spec {
        Spec {
            name,
            target: Target::Failure,
            base: Base::Realistic,
            peers: PEERS_UNIFORM,
            zipf: false,
            marginal_sd: 0.0,
            attribute_step: 0.0,
            pairs: Pairs::None,
            pair_effect: 0.0,
            drift_effect: 0.0,
            ops: false,
            absent_share: 0.0,
            absent_layout: AbsentLayout::Clustered,
            labeling: Labeling::All,
            curve_shift: 0.0,
        }
    }

    const fn timing(name: &'static str) -> Spec {
        Spec {
            target: Target::Timing,
            ..Spec::failure(name)
        }
    }
}

fn noise(name: &'static str, absent_share: f64, labeling: Labeling) -> Spec {
    noise_with(name, absent_share, labeling, AbsentLayout::Clustered)
}

fn noise_with(
    name: &'static str,
    absent_share: f64,
    labeling: Labeling,
    absent_layout: AbsentLayout,
) -> Spec {
    Spec {
        absent_layout,
        marginal_sd: 0.08,
        attribute_step: 0.06,
        pairs: Pairs::Natural,
        pair_effect: 0.40,
        ops: true,
        absent_share,
        labeling,
        ..Spec::failure(name)
    }
}

/// The scenario set, fixed a priori.
fn scenarios() -> Vec<Spec> {
    vec![
        Spec::failure("f.dist"),
        Spec {
            marginal_sd: 0.08,
            ..Spec::failure("f.peer")
        },
        Spec {
            pairs: Pairs::Oversampled,
            pair_effect: 0.45,
            ..Spec::failure("f.pair-over")
        },
        Spec {
            pairs: Pairs::Natural,
            pair_effect: 0.40,
            ..Spec::failure("f.pair-nat")
        },
        Spec {
            marginal_sd: 0.08,
            attribute_step: 0.06,
            pairs: Pairs::Natural,
            pair_effect: 0.40,
            ..Spec::failure("f.mixed")
        },
        Spec {
            peers: PEERS_LONG_TAIL,
            zipf: true,
            marginal_sd: 0.08,
            attribute_step: 0.06,
            pairs: Pairs::Natural,
            pair_effect: 0.40,
            ..Spec::failure("f.longtail")
        },
        Spec {
            drift_effect: 0.25,
            ..Spec::failure("f.drift")
        },
        Spec {
            base: Base::Rare,
            ..Spec::failure("f.rare")
        },
        Spec {
            base: Base::Rare,
            marginal_sd: 0.01,
            attribute_step: 0.01,
            pairs: Pairs::Natural,
            pair_effect: 0.15,
            ..Spec::failure("f.rare-mixed")
        },
        // Label noise: the f.mixed structure under operation-mode routing.
        noise("f.ops-clean", 0.0, Labeling::All),
        noise("f.abs5-naive", 0.05, Labeling::All),
        noise("f.abs20-naive", 0.20, Labeling::All),
        noise("f.ops-delayed", 0.0, Labeling::Delayed),
        noise("f.abs5-delayed", 0.05, Labeling::Delayed),
        noise("f.abs20-delayed", 0.20, Labeling::Delayed),
        // Follow-up (fixed before running): uniform absent keys and a hot key.
        noise_with(
            "f.abs5-naive-uni",
            0.05,
            Labeling::All,
            AbsentLayout::Uniform,
        ),
        noise_with(
            "f.abs20-naive-uni",
            0.20,
            Labeling::All,
            AbsentLayout::Uniform,
        ),
        noise_with(
            "f.abs5-delayed-uni",
            0.05,
            Labeling::Delayed,
            AbsentLayout::Uniform,
        ),
        noise_with(
            "f.abs20-delayed-uni",
            0.20,
            Labeling::Delayed,
            AbsentLayout::Uniform,
        ),
        noise_with("f.hotkey1-naive", 0.01, Labeling::All, AbsentLayout::HotKey),
        noise_with(
            "f.hotkey1-delayed",
            0.01,
            Labeling::Delayed,
            AbsentLayout::HotKey,
        ),
        Spec::timing("t.dist"),
        Spec {
            marginal_sd: 0.4,
            ..Spec::timing("t.peer")
        },
        Spec {
            pairs: Pairs::Oversampled,
            pair_effect: 0.8,
            ..Spec::timing("t.pair-over")
        },
        Spec {
            pairs: Pairs::Natural,
            pair_effect: 0.8,
            ..Spec::timing("t.pair-nat")
        },
        Spec {
            marginal_sd: 0.4,
            attribute_step: 0.25,
            pairs: Pairs::Natural,
            pair_effect: 0.8,
            ..Spec::timing("t.mixed")
        },
        Spec {
            peers: PEERS_LONG_TAIL,
            zipf: true,
            marginal_sd: 0.4,
            attribute_step: 0.25,
            pairs: Pairs::Natural,
            pair_effect: 0.8,
            ..Spec::timing("t.longtail")
        },
        Spec {
            drift_effect: 0.7,
            ..Spec::timing("t.drift")
        },
        // Post-hoc (after run 1): global-curve drift.
        Spec {
            marginal_sd: 0.08,
            curve_shift: 0.10,
            ..Spec::failure("f.curve-drift*")
        },
        Spec {
            marginal_sd: 0.4,
            curve_shift: 0.5,
            ..Spec::timing("t.curve-drift*")
        },
    ]
}

fn standard_normal() -> f64 {
    let u1: f64 = 1.0 - GlobalRng::random_range(0.0..1.0);
    let u2: f64 = GlobalRng::random_range(0.0..1.0);
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

struct World {
    spec: Spec,
    peers: Vec<PeerKeyLocation>,
    locations: Vec<f64>,
    marginal: Vec<f64>,
    attribute: Vec<usize>,
    bad_band: Vec<Option<f64>>,
    zipf_cdf: Vec<f64>,
    absent_centres: Vec<f64>,
}

/// One routing operation: a contract and the peers it will try, in order.
struct Op {
    contract: f64,
    absent: bool,
    candidates: Vec<usize>,
}

impl World {
    fn new(spec: Spec) -> World {
        let peers: Vec<PeerKeyLocation> =
            (0..spec.peers).map(|_| PeerKeyLocation::random()).collect();
        let locations = peers
            .iter()
            .map(|p| {
                p.location()
                    .expect("generated peers carry a location")
                    .as_f64()
            })
            .collect();
        let marginal = (0..spec.peers)
            .map(|_| spec.marginal_sd * standard_normal())
            .collect();
        let attribute = (0..spec.peers)
            .map(|_| GlobalRng::random_range(0..ATTRIBUTE_BUCKETS))
            .collect();
        let bad_band = (0..spec.peers)
            .map(|i| {
                (spec.pairs == Pairs::Natural && i % 5 == 2)
                    .then(|| GlobalRng::random_range(0.0..1.0))
            })
            .collect();
        let weights: Vec<f64> = (0..spec.peers)
            .map(|rank| {
                if spec.zipf {
                    1.0 / ((rank + 1) as f64).powf(ZIPF_EXPONENT)
                } else {
                    1.0
                }
            })
            .collect();
        let total: f64 = weights.iter().sum();
        let mut acc = 0.0;
        let zipf_cdf = weights
            .iter()
            .map(|w| {
                acc += w / total;
                acc
            })
            .collect();
        let absent_centres = if spec.ops {
            (0..ABSENT_CLUSTERS)
                .map(|_| GlobalRng::random_range(0.0..1.0))
                .collect()
        } else {
            Vec::new()
        };
        World {
            spec,
            peers,
            locations,
            marginal,
            attribute,
            bad_band,
            zipf_cdf,
            absent_centres,
        }
    }

    fn draw_peer(&self) -> usize {
        let u: f64 = GlobalRng::random_range(0.0..1.0);
        self.zipf_cdf
            .iter()
            .position(|&c| u < c)
            .unwrap_or(self.spec.peers - 1)
    }

    fn next_op(&self, index: usize) -> Op {
        if !self.spec.ops {
            let (peer, contract) = self.draw(index);
            return Op {
                contract,
                absent: false,
                candidates: vec![peer],
            };
        }
        let absent = GlobalRng::random_range(0.0..1.0) < self.spec.absent_share;
        let contract = if absent {
            match self.spec.absent_layout {
                AbsentLayout::Clustered => {
                    let centre = self.absent_centres[GlobalRng::random_range(0..ABSENT_CLUSTERS)];
                    (centre + GlobalRng::random_range(-ABSENT_HALF_WIDTH..ABSENT_HALF_WIDTH))
                        .rem_euclid(1.0)
                }
                AbsentLayout::Uniform => GlobalRng::random_range(0.0..1.0),
                AbsentLayout::HotKey => self.absent_centres[0],
            }
        } else {
            GlobalRng::random_range(0.0..1.0)
        };
        let mut sample: Vec<usize> = Vec::with_capacity(OP_CANDIDATES);
        for _ in 0..OP_CANDIDATES * 4 {
            if sample.len() == OP_CANDIDATES {
                break;
            }
            let peer = self.draw_peer();
            if !sample.contains(&peer) {
                sample.push(peer);
            }
        }
        sample.sort_by(|&a, &b| {
            ring_distance(self.locations[a], contract)
                .total_cmp(&ring_distance(self.locations[b], contract))
        });
        sample.truncate(MAX_ATTEMPTS);
        Op {
            contract,
            absent,
            candidates: sample,
        }
    }

    fn near_absent(&self, contract: f64) -> bool {
        let centres = match self.spec.absent_layout {
            AbsentLayout::Clustered => &self.absent_centres[..],
            AbsentLayout::Uniform => &[][..],
            AbsentLayout::HotKey => &self.absent_centres[..1],
        };
        centres
            .iter()
            .any(|&c| ring_distance(contract, c) < NEAR_ABSENT)
    }

    /// Up to `RANK_CANDIDATES` peers nearest `contract`, nearest first.
    fn nearest_peers(&self, contract: f64) -> Vec<usize> {
        let mut all: Vec<usize> = (0..self.spec.peers).collect();
        all.sort_by(|&a, &b| {
            ring_distance(self.locations[a], contract)
                .total_cmp(&ring_distance(self.locations[b], contract))
        });
        all.truncate(RANK_CANDIDATES);
        all
    }

    fn draw(&self, index: usize) -> (usize, f64) {
        if self.spec.pairs == Pairs::Oversampled && index % 12 == 0 {
            let (peer, offset) = TARGETED[(index / 12) % TARGETED.len()];
            let centre = (self.locations[peer] + offset).rem_euclid(1.0);
            let jitter = GlobalRng::random_range(-BAND..BAND);
            return (peer, (centre + jitter).rem_euclid(1.0));
        }
        let peer = self.draw_peer();
        (peer, GlobalRng::random_range(0.0..1.0))
    }

    fn in_pair(&self, peer: usize, contract: f64) -> bool {
        match self.spec.pairs {
            Pairs::None => false,
            Pairs::Oversampled => TARGETED.iter().any(|&(target, offset)| {
                target == peer
                    && ring_distance(contract, (self.locations[target] + offset).rem_euclid(1.0))
                        < BAND
            }),
            Pairs::Natural => self.bad_band[peer]
                .is_some_and(|centre| ring_distance(contract, centre) < NATURAL_BAND_HALF_WIDTH),
        }
    }

    fn drift_changed(&self, peer: usize) -> bool {
        self.spec.drift_effect > 0.0 && peer % 4 <= 1
    }

    /// Generating truth: `p*` (failure) or `mu* = E[ln ms]` (timing).
    fn truth(&self, index: usize, peer: usize, contract: f64, distance: f64) -> f64 {
        let spec = &self.spec;
        let mut effect = self.marginal[peer] + spec.attribute_step * self.attribute[peer] as f64;
        if self.in_pair(peer, contract) {
            effect += spec.pair_effect;
        }
        if spec.drift_effect > 0.0 {
            let late = index >= EVENTS / 2;
            let bad = (!late && peer % 4 == 0) || (late && peer % 4 == 1);
            if bad {
                effect += spec.drift_effect;
            }
        }
        if index >= EVENTS / 2 {
            effect += spec.curve_shift;
        }
        let shape = (2.0 * distance).sqrt();
        match spec.target {
            Target::Failure => {
                let base = match spec.base {
                    Base::Realistic => 0.05 + 0.35 * shape,
                    Base::Rare => 0.01 + 0.01 * shape,
                };
                (base + effect).clamp(0.001, 0.999)
            }
            Target::Timing => 200f64.ln() + 1.5 * shape + effect,
        }
    }

    fn outcome(&self, truth: f64) -> f64 {
        match self.spec.target {
            Target::Failure => {
                if GlobalRng::random_range(0.0..1.0) < truth {
                    1.0
                } else {
                    0.0
                }
            }
            Target::Timing => truth + TIMING_NOISE_SD * standard_normal(),
        }
    }
}

// ---------------------------------------------------------------------------
// Hierarchical empirical Bayes: [attribute] -> peer -> [peer x band]
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum BandBy {
    None,
    Contract,
    Distance,
}

#[derive(Clone, Copy, Debug)]
struct HierConfig {
    /// A root level: the mean residual of ALL traffic, shrunk toward zero.
    /// Absorbs a lagging or noisy global curve.
    root: bool,
    attribute: bool,
    band_by: BandBy,
    decay_hours: Option<f64>,
}

/// Exponentially-forgotten sufficient statistics.
#[derive(Default, Clone, Copy)]
struct Decayed {
    n: f64,
    sum: f64,
    sumsq: f64,
    t: f64,
}

impl Decayed {
    fn at(&self, now: f64, decay: Option<f64>) -> Decayed {
        let factor = decay.map_or(1.0, |h| (-(now - self.t).max(0.0) / h).exp());
        Decayed {
            n: self.n * factor,
            sum: self.sum * factor,
            sumsq: self.sumsq * factor,
            t: now,
        }
    }

    fn add(&mut self, x: f64, now: f64, decay: Option<f64>) {
        let mut d = self.at(now, decay);
        d.n += 1.0;
        d.sum += x;
        d.sumsq += x * x;
        *self = d;
    }

    fn mean(&self) -> f64 {
        self.sum / self.n
    }
}

/// A node's `(n, mean, noise variance of the mean excluding its own effect)`.
#[derive(Clone, Copy)]
struct Node {
    n: f64,
    mean: f64,
    noise: f64,
}

struct Snapshot {
    tau2_root: f64,
    root: Option<Node>,
    sigma2: f64,
    tau2_cell: f64,
    tau2_peer: f64,
    tau2_attr: f64,
    attrs: HashMap<usize, Node>,
    peers: HashMap<usize, Node>,
    cells: HashMap<(usize, usize), Node>,
}

struct Hier {
    config: HierConfig,
    root: Decayed,
    attrs: HashMap<usize, Decayed>,
    peers: HashMap<usize, Decayed>,
    cells: HashMap<(usize, usize), Decayed>,
    peer_attr: HashMap<usize, usize>,
}

impl Hier {
    fn new(config: HierConfig) -> Hier {
        Hier {
            config,
            root: Decayed::default(),
            attrs: HashMap::new(),
            peers: HashMap::new(),
            cells: HashMap::new(),
            peer_attr: HashMap::new(),
        }
    }

    fn band(&self, contract: f64, distance: f64) -> usize {
        let unit = match self.config.band_by {
            BandBy::None => return 0,
            BandBy::Contract => contract,
            BandBy::Distance => distance * 2.0,
        };
        ((unit * HIER_BANDS as f64).floor() as usize).min(HIER_BANDS - 1)
    }

    fn add(&mut self, peer: usize, attribute: usize, contract: f64, distance: f64, t: f64, r: f64) {
        let decay = self.config.decay_hours;
        let band = self.band(contract, distance);
        self.root.add(r, t, decay);
        self.peers.entry(peer).or_default().add(r, t, decay);
        self.peer_attr.insert(peer, attribute);
        if self.config.attribute {
            self.attrs.entry(attribute).or_default().add(r, t, decay);
        }
        if self.config.band_by != BandBy::None {
            self.cells.entry((peer, band)).or_default().add(r, t, decay);
        }
    }

    /// Method-of-moments variance components, computed once per prediction
    /// round. `O(nodes)`.
    fn snapshot(&self, now: f64) -> Option<Snapshot> {
        let decay = self.config.decay_hours;
        let bands = self.config.band_by != BandBy::None;
        let peers: HashMap<usize, Decayed> = self
            .peers
            .iter()
            .map(|(&k, v)| (k, v.at(now, decay)))
            .collect();
        let cells: HashMap<(usize, usize), Decayed> = self
            .cells
            .iter()
            .map(|(&k, v)| (k, v.at(now, decay)))
            .collect();
        let attrs: HashMap<usize, Decayed> = self
            .attrs
            .iter()
            .map(|(&k, v)| (k, v.at(now, decay)))
            .collect();

        // sigma^2: pooled variance within the finest level.
        let finest: Vec<&Decayed> = if bands {
            cells.values().collect()
        } else {
            peers.values().collect()
        };
        let (mut ss, mut df) = (0.0, 0.0);
        for node in finest {
            if node.n >= 2.0 {
                ss += (node.sumsq - node.sum * node.sum / node.n).max(0.0);
                df += node.n - 1.0;
            }
        }
        if df < 2.0 {
            return None;
        }
        let sigma2 = (ss / df).max(1e-9);
        let root = self.root.at(now, decay);
        let base = if self.config.root { root.mean() } else { 0.0 };

        // Sum of squared child counts, for the variance a parent mean inherits
        // from its children's effects.
        let mut peer_sq_cells: HashMap<usize, f64> = HashMap::new();
        let mut attr_sq_cells: HashMap<usize, f64> = HashMap::new();
        for (&(peer, _), cell) in &cells {
            *peer_sq_cells.entry(peer).or_default() += cell.n * cell.n;
            *attr_sq_cells.entry(self.peer_attr[&peer]).or_default() += cell.n * cell.n;
        }
        let mut attr_sq_peers: HashMap<usize, f64> = HashMap::new();
        for (&peer, node) in &peers {
            *attr_sq_peers.entry(self.peer_attr[&peer]).or_default() += node.n * node.n;
        }

        let tau2_cell = if bands {
            let (mut acc, mut count) = (0.0, 0.0);
            for (&(peer, _), cell) in &cells {
                let parent = peers[&peer];
                if cell.n >= 2.0 {
                    acc += (cell.mean() - parent.mean()).powi(2)
                        - sigma2 * (1.0 / cell.n - 1.0 / parent.n).max(0.0);
                    count += 1.0;
                }
            }
            if count > 0.0 {
                (acc / count).max(0.0)
            } else {
                0.0
            }
        } else {
            0.0
        };

        let peer_noise = |peer: usize, node: &Decayed| -> f64 {
            let share = peer_sq_cells.get(&peer).copied().unwrap_or(0.0) / (node.n * node.n);
            tau2_cell * share + sigma2 / node.n
        };

        let tau2_peer = {
            let (mut acc, mut count) = (0.0, 0.0);
            for (&peer, node) in &peers {
                if node.n < 2.0 {
                    continue;
                }
                let parent = if self.config.attribute {
                    attrs[&self.peer_attr[&peer]].mean()
                } else {
                    base
                };
                acc += (node.mean() - parent).powi(2) - peer_noise(peer, node);
                count += 1.0;
            }
            if count > 0.0 {
                (acc / count).max(0.0)
            } else {
                0.0
            }
        };

        let attr_noise = |attr: usize, node: &Decayed| -> f64 {
            let n2 = node.n * node.n;
            tau2_peer * attr_sq_peers.get(&attr).copied().unwrap_or(0.0) / n2
                + tau2_cell * attr_sq_cells.get(&attr).copied().unwrap_or(0.0) / n2
                + sigma2 / node.n
        };

        let tau2_attr = if self.config.attribute {
            let (mut acc, mut count) = (0.0, 0.0);
            for (&attr, node) in &attrs {
                if node.n >= 2.0 {
                    acc += (node.mean() - base).powi(2) - attr_noise(attr, node);
                    count += 1.0;
                }
            }
            if count > 0.0 {
                (acc / count).max(0.0)
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Root: the grand mean inherits the variance of every level's effects.
        let (root_node, tau2_root) = if self.config.root && root.n >= 2.0 {
            let n2 = root.n * root.n;
            let sq =
                |counts: &mut dyn Iterator<Item = f64>| counts.map(|n| n * n).sum::<f64>() / n2;
            let mut noise = sigma2 / root.n
                + tau2_peer * sq(&mut peers.values().map(|v| v.n))
                + tau2_cell * sq(&mut cells.values().map(|v| v.n));
            if self.config.attribute {
                noise += tau2_attr * sq(&mut attrs.values().map(|v| v.n));
            }
            (
                Some(Node {
                    n: root.n,
                    mean: root.mean(),
                    noise,
                }),
                (root.mean().powi(2) - noise).max(0.0),
            )
        } else {
            (None, 0.0)
        };

        Some(Snapshot {
            tau2_root,
            root: root_node,
            sigma2,
            tau2_cell,
            tau2_peer,
            tau2_attr,
            attrs: attrs
                .iter()
                .map(|(&k, v)| {
                    (
                        k,
                        Node {
                            n: v.n,
                            mean: v.mean(),
                            noise: attr_noise(k, v),
                        },
                    )
                })
                .collect(),
            peers: peers
                .iter()
                .map(|(&k, v)| {
                    (
                        k,
                        Node {
                            n: v.n,
                            mean: v.mean(),
                            noise: peer_noise(k, v),
                        },
                    )
                })
                .collect(),
            cells: cells
                .iter()
                .map(|(&k, v)| {
                    (
                        k,
                        Node {
                            n: v.n,
                            mean: v.mean(),
                            noise: sigma2 / v.n,
                        },
                    )
                })
                .collect(),
        })
    }

    /// Posterior `(mean, variance)` of the residual at this query, descending
    /// the hierarchy with a normal-normal update at each level:
    /// `P = tau2 + v_parent`, `B = P / (P + noise)`, `mu += B (ybar - mu)`,
    /// `v = B * noise`. A level with no node, or `tau2 = 0`, passes through.
    fn predict(
        &self,
        snap: Option<&Snapshot>,
        peer: usize,
        attribute: usize,
        contract: f64,
        distance: f64,
    ) -> (f64, f64) {
        let Some(snap) = snap else {
            return (0.0, 0.0);
        };
        let step = |(mu, v): (f64, f64), node: Option<&Node>, tau2: f64| -> (f64, f64) {
            match node {
                Some(node) if tau2 > 0.0 && node.n > 0.0 => {
                    let prior = tau2 + v;
                    let b = prior / (prior + node.noise);
                    (mu + b * (node.mean - mu), b * node.noise)
                }
                _ => (mu, v + tau2),
            }
        };
        let mut state = (0.0, 0.0);
        if self.config.root {
            state = step(state, snap.root.as_ref(), snap.tau2_root);
        }
        if self.config.attribute {
            state = step(state, snap.attrs.get(&attribute), snap.tau2_attr);
        }
        state = step(state, snap.peers.get(&peer), snap.tau2_peer);
        if self.config.band_by != BandBy::None {
            state = step(
                state,
                snap.cells.get(&(peer, self.band(contract, distance))),
                snap.tau2_cell,
            );
        }
        state
    }
}

// ---------------------------------------------------------------------------
// Post-hoc (run 2): long-window prior, re-anchored residuals, online horizon
// ---------------------------------------------------------------------------

/// One learned event, kept raw so residuals can be recomputed against the
/// CURRENT curve rather than the curve that existed when it was recorded.
#[derive(Clone, Copy)]
struct Raw {
    peer: usize,
    attribute: usize,
    contract: f64,
    distance: f64,
    time: f64,
    y: f64,
}

/// Isotonic distance curve over up to `LONG_WINDOW` learned events, refit in
/// batch on `REBUILD_EVERY` cadence. Same PAV as the router's estimator; only
/// the window differs (500 there).
struct LongCurve {
    fit: Option<pav_regression::IsotonicRegression<f64>>,
}

impl LongCurve {
    fn refit(&mut self, raw: &[Raw]) {
        let start = raw.len().saturating_sub(LONG_WINDOW);
        let points: Vec<pav_regression::Point<f64>> = raw[start..]
            .iter()
            .map(|r| pav_regression::Point::new(r.distance, r.y))
            .collect();
        if points.len() >= 5 {
            self.fit = pav_regression::IsotonicRegression::new_ascending(&points).ok();
        }
    }

    fn value(&self, distance: f64) -> Option<f64> {
        self.fit
            .as_ref()
            .and_then(|f| f.interpolate(distance))
            .map(|v| v.max(0.0))
    }
}

/// A hierarchy over residuals of a named prior, rebuilt from raw events on
/// every prior refit, with one copy per forgetting horizon and the horizon
/// chosen by decayed prequential squared loss.
struct Reanchored {
    config: HierConfig,
    hiers: Vec<Hier>,
    /// Decayed squared loss per horizon, as `(sum, t)`.
    loss: Vec<(f64, f64)>,
    horizons: Vec<Option<f64>>,
}

impl Reanchored {
    fn new(config: HierConfig, horizons: &[Option<f64>]) -> Reanchored {
        Reanchored {
            config,
            hiers: horizons
                .iter()
                .map(|&h| {
                    Hier::new(HierConfig {
                        decay_hours: h,
                        ..config
                    })
                })
                .collect(),
            loss: vec![(0.0, 0.0); horizons.len()],
            horizons: horizons.to_vec(),
        }
    }

    fn rebuild(&mut self, raw: &[Raw], prior: impl Fn(&Raw) -> Option<f64>) {
        for (hier, &h) in self.hiers.iter_mut().zip(&self.horizons) {
            *hier = Hier::new(HierConfig {
                decay_hours: h,
                ..self.config
            });
        }
        for r in raw {
            if let Some(g) = prior(r) {
                self.add(r, g);
            }
        }
    }

    fn add(&mut self, r: &Raw, prior: f64) {
        for hier in &mut self.hiers {
            hier.add(
                r.peer,
                r.attribute,
                r.contract,
                r.distance,
                r.time,
                r.y - prior,
            );
        }
    }

    fn snapshots(&self, now: f64) -> Vec<Option<Snapshot>> {
        self.hiers.iter().map(|h| h.snapshot(now)).collect()
    }

    /// Strict argmin, seeded at the no-forgetting horizon so ties keep it.
    fn selected(&self, now: f64) -> usize {
        let decayed =
            |(sum, t): (f64, f64)| sum * (-(now - t).max(0.0) / SELECTOR_FORGETTING_HOURS).exp();
        let mut best = 0;
        for i in 1..self.loss.len() {
            if decayed(self.loss[i]) < decayed(self.loss[best]) {
                best = i;
            }
        }
        best
    }

    fn score(&mut self, predictions: &[f64], actual_residual: f64, now: f64) {
        for (loss, prediction) in self.loss.iter_mut().zip(predictions) {
            let factor = (-(now - loss.1).max(0.0) / SELECTOR_FORGETTING_HOURS).exp();
            *loss = (
                loss.0 * factor + (prediction - actual_residual).powi(2),
                now,
            );
        }
    }
}

/// Isotonic curve with optional exponential forgetting (weighted PAV) and
/// optional empirical-Bayes shrinkage of its blocks toward the pooled mean.
///
/// Shrinkage: PAV blocks at the ends of the distance range can rest on a
/// handful of events, and on binary outcomes at a 1-2% rate one failure in a
/// five-event block reads as 20%. Each block mean is shrunk by
/// `B = tau2 / (tau2 + s2 / w_block)` toward the weighted grand mean, with
/// `s2` the pooled within-block variance and `tau2` the between-block variance
/// by method of moments, then PAV is re-run over the shrunk blocks so the
/// result stays monotone.
struct Curve {
    horizon: Option<f64>,
    shrink: bool,
    fit: Option<pav_regression::IsotonicRegression<f64>>,
}

impl Curve {
    fn new(horizon: Option<f64>, shrink: bool) -> Curve {
        Curve {
            horizon,
            shrink,
            fit: None,
        }
    }

    fn refit(&mut self, raw: &[Raw], now: f64) {
        use pav_regression::{IsotonicRegression, Point};
        let start = raw.len().saturating_sub(LONG_WINDOW);
        let window = &raw[start..];
        if window.len() < 5 {
            return;
        }
        let weight = |r: &Raw| {
            self.horizon
                .map_or(1.0, |h| (-(now - r.time).max(0.0) / h).exp())
                .max(1e-9)
        };
        let points: Vec<Point<f64>> = window
            .iter()
            .map(|r| Point::new_with_weight(r.distance, r.y, weight(r)))
            .collect();
        let Ok(fit) = IsotonicRegression::new_ascending(&points) else {
            return;
        };
        if !self.shrink {
            self.fit = Some(fit);
            return;
        }
        let blocks = fit.get_points_sorted();
        let total: f64 = blocks.iter().map(|b| b.weight()).sum();
        if blocks.len() < 2 || total <= 0.0 {
            self.fit = Some(fit);
            return;
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
        self.fit = IsotonicRegression::new_ascending(&shrunk)
            .ok()
            .or(Some(fit));
    }

    fn value(&self, distance: f64) -> Option<f64> {
        self.fit
            .as_ref()
            .and_then(|f| f.interpolate(distance))
            .map(|v| v.max(0.0))
    }
}

/// ONE forgetting horizon for the whole model: per candidate horizon, an
/// EB-shrunk weighted-PAV curve and a hierarchy re-anchored to it that forgets
/// at the same rate; the horizon is chosen by decayed prequential loss on the
/// composed prediction.
struct Joint {
    config: HierConfig,
    members: Vec<(Curve, Hier)>,
    loss: Vec<(f64, f64)>,
}

impl Joint {
    fn new(config: HierConfig) -> Joint {
        Joint {
            config,
            members: HORIZONS
                .iter()
                .map(|&h| {
                    (
                        Curve::new(h, true),
                        Hier::new(HierConfig {
                            decay_hours: h,
                            ..config
                        }),
                    )
                })
                .collect(),
            loss: vec![(0.0, 0.0); HORIZONS.len()],
        }
    }

    fn rebuild(&mut self, raw: &[Raw], now: f64, finish: impl Fn(f64) -> f64) {
        for (member, &h) in self.members.iter_mut().zip(HORIZONS.iter()) {
            member.0.refit(raw, now);
            member.1 = Hier::new(HierConfig {
                decay_hours: h,
                ..self.config
            });
            for r in raw {
                if let Some(g) = member.0.value(r.distance).map(&finish) {
                    member
                        .1
                        .add(r.peer, r.attribute, r.contract, r.distance, r.time, r.y - g);
                }
            }
        }
    }

    fn add(&mut self, r: &Raw, finish: impl Fn(f64) -> f64) {
        for member in &mut self.members {
            if let Some(g) = member.0.value(r.distance).map(&finish) {
                member
                    .1
                    .add(r.peer, r.attribute, r.contract, r.distance, r.time, r.y - g);
            }
        }
    }

    /// Composed prediction per horizon (before the caller's clamp).
    fn predict(
        &self,
        now: f64,
        peer: usize,
        attribute: usize,
        contract: f64,
        distance: f64,
        finish: impl Fn(f64) -> f64,
    ) -> Vec<Option<f64>> {
        self.members
            .iter()
            .map(|(curve, hier)| {
                curve.value(distance).map(&finish).map(|g| {
                    let snap = hier.snapshot(now);
                    g + hier
                        .predict(snap.as_ref(), peer, attribute, contract, distance)
                        .0
                })
            })
            .collect()
    }

    fn selected(&self, now: f64, available: &[Option<f64>]) -> Option<usize> {
        let decayed =
            |(sum, t): (f64, f64)| sum * (-(now - t).max(0.0) / SELECTOR_FORGETTING_HOURS).exp();
        let mut best: Option<usize> = None;
        for (i, value) in available.iter().enumerate() {
            if value.is_none() {
                continue;
            }
            match best {
                Some(b) if decayed(self.loss[i]) >= decayed(self.loss[b]) => {}
                _ => best = Some(i),
            }
        }
        best
    }

    fn score(&mut self, predictions: &[Option<f64>], y: f64, now: f64) {
        for (loss, prediction) in self.loss.iter_mut().zip(predictions) {
            let factor = (-(now - loss.1).max(0.0) / SELECTOR_FORGETTING_HOURS).exp();
            // A horizon with no curve yet accumulates no loss. Harmless here
            // only because every member's curve is refit from the same events
            // at the same moment, so all become available together.
            let err = prediction.map_or(0.0, |p| (p - y).powi(2));
            *loss = (loss.0 * factor + err, now);
        }
    }
}

// ---------------------------------------------------------------------------
// Estimator roster
// ---------------------------------------------------------------------------

const ROW_LABELS: [&str; 29] = [
    "a  legacy blend (BASELINE)",
    "a' legacy as shipped (raw units)",
    "b  global curve alone",
    "-  peer-adjusted (global + EWMA)",
    "c  kNN gower, renegade k",
    "c  kNN gower, renegade k +lam",
    "c  kNN gower, LOO k",
    "c  kNN gower, LOO k +lam",
    "c  kNN peer-first, LOO k +lam",
    "d  H peer",
    "d  H peer>8 contract bands",
    "d  H peer>8 distance bands",
    "d  H attr>peer>8 contract",
    "d  H peer>8 contract, decay",
    "e1 H(contract) + kNN rr +lam",
    "e2 kNN shrunk toward H(contract)",
    "e3 precision-wtd H(contract),kNN",
    "e1'H(attr,decay) + kNN rr +lam",
    "f  renegade learned metric",
    "f  renegade learned metric +lam",
    // Post-hoc (run 2), marked `*`.
    "*b' long-window curve alone",
    "*d H(contract) re-anchored, 500 curve",
    "*d H(contract) re-anchored, long curve",
    "*d H* root>attr>peer>8c, long, horizon",
    "*e1 H* + kNN rr +lam",
    "*e3 precision-wtd H*, kNN",
    "*b'' EB-shrunk long curve alone",
    "*d H* on EB-shrunk long curve",
    "*d H** joint horizon, EB curve",
];
const BASELINE: usize = 0;

const HIER_CONFIGS: [HierConfig; 6] = [
    HierConfig {
        root: false,
        attribute: false,
        band_by: BandBy::None,
        decay_hours: None,
    },
    HierConfig {
        root: false,
        attribute: false,
        band_by: BandBy::Contract,
        decay_hours: None,
    },
    HierConfig {
        root: false,
        attribute: false,
        band_by: BandBy::Distance,
        decay_hours: None,
    },
    HierConfig {
        root: false,
        attribute: true,
        band_by: BandBy::Contract,
        decay_hours: None,
    },
    HierConfig {
        root: false,
        attribute: false,
        band_by: BandBy::Contract,
        decay_hours: Some(DECAY_HOURS),
    },
    // Used only as the prior of e1'.
    HierConfig {
        root: false,
        attribute: true,
        band_by: BandBy::Contract,
        decay_hours: Some(DECAY_HOURS),
    },
];

/// Subsets scored separately.
const SUBSETS: [&str; 5] = ["all", "targeted", "cold", "drifted", "near-abs"];

/// Simulated routing decision (ops scenarios, existing contracts, after
/// warm-up): pick the candidate with the lowest predicted failure among the
/// `RANK_CANDIDATES` peers nearest the key, and compare with the truly best
/// by `p*`. Ties go to the nearer peer, for truth and estimate alike.
const RANK_CANDIDATES: usize = 10;
const RANK_ROWS: [&str; 8] = [
    "a  legacy blend (BASELINE)",
    "b  global curve alone",
    "-  peer-adjusted (global + EWMA)",
    "c  kNN gower, LOO k +lam",
    "d  H peer>8 contract bands",
    "*d H* root>attr>peer>8c, long, horizon",
    "*d H* on EB-shrunk long curve",
    "   nearest peer (no model)",
];

#[derive(Clone)]
struct RunResult {
    /// `[subset][row]` mse; NaN where the subset is empty.
    mse: Vec<Vec<f64>>,
    counts: [usize; SUBSETS.len()],
    /// Estimated variance components of H(contract) at the end of the run,
    /// `[sigma2, tau2_cell, tau2_peer]`.
    components: [f64; 3],
    /// Per `RANK_ROWS`: `[best@10 hits, regret@10 sum, best@3 hits, regret@3 sum]`.
    ranking: [[f64; 4]; RANK_ROWS.len()],
    decisions: usize,
}

/// Inverse-distance mean/variance over `(distance, value)` pairs, renegade's
/// weighting with its exact-match short-circuit.
fn idw(pairs: &[(f64, f64)]) -> Option<(f64, f64, f64)> {
    inverse_distance_stats(pairs, pairs.len())
}

fn neighbours(
    history: &[ResidualPoint],
    query: &ResidualPoint,
    weights: [f64; 4],
    keep: usize,
) -> Vec<(f64, usize)> {
    let mut all: Vec<(f64, usize)> = history
        .iter()
        .enumerate()
        .map(|(i, p)| (fixed_distance(weights, query, p), i))
        .collect();
    let keep = keep.min(all.len());
    if keep == 0 {
        return Vec::new();
    }
    if keep < all.len() {
        all.select_nth_unstable_by(keep - 1, |a, b| a.0.total_cmp(&b.0));
        all.truncate(keep);
    }
    all.sort_by(|a, b| a.0.total_cmp(&b.0));
    all
}

/// An attempt awaiting its op's label decision.
struct Pending {
    peer_index: usize,
    distance: f64,
    time: f64,
    y: f64,
    /// The 500-window global curve at predict time.
    global: Option<f64>,
    /// The long curve at predict time and each horizon's residual prediction,
    /// for the horizon selector's prequential loss.
    star: Option<(f64, Vec<f64>)>,
    /// The same for the EB-shrunk long curve's hierarchy.
    shrunk: Option<(f64, Vec<f64>)>,
    /// Composed prediction per joint horizon.
    joint: Vec<Option<f64>>,
}

fn run_bakeoff(spec: Spec, seed: u64) -> RunResult {
    let _guard = GlobalRng::seed_guard(seed);
    let world = World::new(spec);
    let rows = ROW_LABELS.len();
    let timing = spec.target == Target::Timing;

    let mut iso = IsotonicEstimator::new(Vec::new(), EstimatorType::Positive);
    // Timing only: the router's actual construction, on milliseconds.
    let mut iso_raw = IsotonicEstimator::new_with_mode(
        Vec::new(),
        EstimatorType::Positive,
        AdjustmentMode::Multiplicative,
    );
    let mut legacy_abs = PredictionStage::new(10_000);
    let mut legacy_abs_raw = PredictionStage::new(10_000);
    let mut renegade_res = PredictionStage::new(10_000);
    let mut hiers: Vec<Hier> = HIER_CONFIGS.iter().map(|&c| Hier::new(c)).collect();
    let mut history: Vec<ResidualPoint> = Vec::new();
    let mut history_attr: Vec<usize> = Vec::new();
    let mut peer_ids: HashMap<usize, u64> = HashMap::new();
    let mut peer_events = vec![0usize; spec.peers];
    let mut loo_k = [DEFAULT_K; 2];
    let mut loo_trained_at = 0usize;
    let selector = residual::ShrinkageSelector::new();
    let lam = |n: f64, var: f64, mean: f64, prior: f64| selector.lambda(n, var, mean, prior);

    let mut err = vec![vec![0.0; rows]; SUBSETS.len()];
    let mut counts = [0usize; SUBSETS.len()];
    let (weight_ramp, clamp_unit) = if timing {
        (TIMING_WEIGHT_RAMP_EVENTS, false)
    } else {
        (FAILURE_WEIGHT_RAMP_EVENTS, true)
    };
    let finish = |v: f64| if clamp_unit { v.clamp(0.0, 1.0) } else { v };

    // Attempts of the current op, learned only once the op ends and its label
    // policy has decided: (peer, contract, distance, time, y, global at predict).
    let mut pending: Vec<Pending> = Vec::new();
    let mut raw: Vec<Raw> = Vec::new();
    let mut history_y: Vec<f64> = Vec::new();
    let mut long = LongCurve { fit: None };
    let contract_config = HIER_CONFIGS[1];
    let star_config = HierConfig {
        root: true,
        attribute: true,
        band_by: BandBy::Contract,
        decay_hours: None,
    };
    let mut re_window = Reanchored::new(contract_config, &[None]);
    let mut re_long = Reanchored::new(contract_config, &[None]);
    let mut re_star = Reanchored::new(star_config, &HORIZONS);
    let mut shrunk_long = Curve::new(None, true);
    let mut re_star_shrunk = Reanchored::new(star_config, &HORIZONS);
    let mut joint = Joint::new(star_config);
    let mut learned_since_rebuild = 0usize;
    let mut index = 0usize;
    let mut ranking = [[0.0f64; 4]; RANK_ROWS.len()];
    let mut decisions = 0usize;
    while index < EVENTS {
        let op = world.next_op(index);
        let contract_value = op.contract;
        let contract = Location::try_from(contract_value).expect("contract within ring");

        // Ranking decision, before any attempt of this op is learned. Reads
        // models only; draws no randomness, so the event stream is unchanged.
        if spec.ops && !op.absent && index >= WARMUP_EVENTS {
            let time = index as f64 / 60.0;
            let candidates = world.nearest_peers(contract_value);
            let snap_contract = hiers[1].snapshot(time);
            let snaps_star = re_star.snapshots(time);
            let star_selected = re_star.selected(time);
            let snaps_shrunk = re_star_shrunk.snapshots(time);
            let shrunk_selected = re_star_shrunk.selected(time);
            let mut truths = Vec::new();
            let mut scores: Vec<Vec<f64>> = vec![Vec::new(); RANK_ROWS.len()];
            let mut complete = true;
            for (rank_position, &cand) in candidates.iter().enumerate() {
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
                let query = ResidualPoint {
                    peer_id: *peer_ids.get(&cand).unwrap_or(&next_id) as f64,
                    peer_index: cand,
                    contract: contract_value,
                    distance,
                    time,
                    residual: 0.0,
                };
                let observation = RoutingObservation {
                    peer_id: query.peer_id,
                    contract_location: contract_value,
                    distance,
                    time,
                };
                scores[0].push(match legacy_abs.predict(&observation) {
                    Some(v) if v.is_finite() => {
                        let w = (legacy_abs.len() as f64 / FAILURE_WEIGHT_RAMP_EVENTS)
                            .min(MAX_RENEGADE_WEIGHT);
                        finish(peer_adjusted * (1.0 - w) + v.clamp(0.0, 1.0) * w)
                    }
                    _ => peer_adjusted,
                });
                scores[1].push(global);
                scores[2].push(peer_adjusted);
                let knn: Vec<(f64, f64)> = neighbours(&history, &query, GOWER, loo_k[0])
                    .iter()
                    .map(|&(d, i)| (d, history[i].residual))
                    .collect();
                scores[3].push(finish(
                    global
                        + match idw(&knn) {
                            Some((m, v, n)) => lam(n, v, m, 0.0) * m,
                            None => 0.0,
                        },
                ));
                scores[4].push(finish(
                    global
                        + hiers[1]
                            .predict(
                                snap_contract.as_ref(),
                                cand,
                                attribute,
                                contract_value,
                                distance,
                            )
                            .0,
                ));
                scores[5].push(match long.value(distance).map(finish) {
                    Some(prior) => finish(
                        prior
                            + re_star.hiers[star_selected]
                                .predict(
                                    snaps_star[star_selected].as_ref(),
                                    cand,
                                    attribute,
                                    contract_value,
                                    distance,
                                )
                                .0,
                    ),
                    None => global,
                });
                scores[6].push(match shrunk_long.value(distance).map(finish) {
                    Some(prior) => finish(
                        prior
                            + re_star_shrunk.hiers[shrunk_selected]
                                .predict(
                                    snaps_shrunk[shrunk_selected].as_ref(),
                                    cand,
                                    attribute,
                                    contract_value,
                                    distance,
                                )
                                .0,
                    ),
                    None => global,
                });
                scores[7].push(rank_position as f64);
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
                for (row, row_scores) in scores.iter().enumerate() {
                    let chosen10 = argmin(row_scores);
                    let chosen3 = argmin(&row_scores[..3]);
                    ranking[row][0] += f64::from(u8::from(chosen10 == best10));
                    ranking[row][1] += truths[chosen10] - truths[best10];
                    ranking[row][2] += f64::from(u8::from(chosen3 == best3));
                    ranking[row][3] += truths[chosen3] - truths[best3];
                }
                decisions += 1;
            }
        }

        pending.clear();
        let mut succeeded = false;
        for &peer_index in &op.candidates {
            if index >= EVENTS {
                break;
            }
            let peer = &world.peers[peer_index];
            let distance = contract
                .distance(peer.location().expect("peer has a location"))
                .as_f64();
            let time = index as f64 / 60.0;
            let attribute = world.attribute[peer_index];
            let truth = world.truth(index, peer_index, contract_value, distance);
            let y = if op.absent { 1.0 } else { world.outcome(truth) };

            let global = iso.estimate_global(peer, contract).ok().map(finish);
            let peer_adjusted = iso.estimate_retrieval_time(peer, contract).ok().map(finish);

            let mut star_pending: Option<(f64, Vec<f64>)> = None;
            let mut shrunk_pending: Option<(f64, Vec<f64>)> = None;
            let mut joint_pending: Vec<Option<f64>> = Vec::new();
            if let (Some(global), Some(peer_adjusted)) = (global, peer_adjusted) {
                let next_id = peer_ids.len() as u64;
                let query = ResidualPoint {
                    peer_id: *peer_ids.get(&peer_index).unwrap_or(&next_id) as f64,
                    peer_index,
                    contract: contract_value,
                    distance,
                    time,
                    residual: 0.0,
                };
                let observation = RoutingObservation {
                    peer_id: query.peer_id,
                    contract_location: contract_value,
                    distance,
                    time,
                };
                let weight = |len: usize| (len as f64 / weight_ramp).min(MAX_RENEGADE_WEIGHT);

                let mut prediction = vec![global; rows];
                // a: legacy blend on the target's scale (log ms for timing).
                prediction[0] = match legacy_abs.predict(&observation) {
                    Some(v) if v.is_finite() && (clamp_unit || v >= 0.0) => {
                        let w = weight(legacy_abs.len());
                        let v = if clamp_unit { v.clamp(0.0, 1.0) } else { v };
                        finish(peer_adjusted * (1.0 - w) + v * w)
                    }
                    _ => peer_adjusted,
                };
                // a': as shipped. Failure: identical. Timing: raw milliseconds.
                prediction[1] = if timing {
                    let base_ms = iso_raw.estimate_retrieval_time(peer, contract).ok();
                    match base_ms {
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
                prediction[3] = peer_adjusted;

                // c: fixed-metric kNN over residuals.
                let keep = renegade_res.cached_k.max(loo_k[0]).max(loo_k[1]).max(1);
                let gower = neighbours(&history, &query, GOWER, keep);
                let stats_at = |list: &[(f64, usize)], k: usize| -> Option<(f64, f64, f64)> {
                    let pairs: Vec<(f64, f64)> = list
                        .iter()
                        .take(k)
                        .map(|&(d, i)| (d, history[i].residual))
                        .collect();
                    idw(&pairs)
                };
                let knn = |stats: Option<(f64, f64, f64)>, shrink: bool| -> f64 {
                    match stats {
                        Some((m, v, n)) => {
                            if shrink {
                                lam(n, v, m, 0.0) * m
                            } else {
                                m
                            }
                        }
                        None => 0.0,
                    }
                };
                let g_ren = stats_at(&gower, renegade_res.cached_k);
                let g_loo = stats_at(&gower, loo_k[0]);
                let peer_first = neighbours(&history, &query, PEER_FIRST, loo_k[1]);
                let p_loo = stats_at(&peer_first, loo_k[1]);
                let mut r_hat = vec![0.0; rows];
                r_hat[4] = knn(g_ren, false);
                r_hat[5] = knn(g_ren, true);
                r_hat[6] = knn(g_loo, false);
                r_hat[7] = knn(g_loo, true);
                r_hat[8] = knn(p_loo, true);

                // d: hierarchies.
                let snaps: Vec<Option<Snapshot>> = hiers.iter().map(|h| h.snapshot(time)).collect();
                let h_pred: Vec<(f64, f64)> = hiers
                    .iter()
                    .zip(&snaps)
                    .map(|(h, s)| {
                        h.predict(s.as_ref(), peer_index, attribute, contract_value, distance)
                    })
                    .collect();
                for (row, h) in (9..=13).zip(&h_pred) {
                    r_hat[row] = h.0;
                }

                // e: combinations on H(contract) = hiers[1], and on hiers[5].
                let rr = |h_index: usize| -> f64 {
                    let (h_mean, _) = h_pred[h_index];
                    let pairs: Vec<(f64, f64)> = gower
                        .iter()
                        .take(loo_k[0])
                        .map(|&(d, i)| {
                            let p = &history[i];
                            let h_i = hiers[h_index]
                                .predict(
                                    snaps[h_index].as_ref(),
                                    p.peer_index,
                                    history_attr[i],
                                    p.contract,
                                    p.distance,
                                )
                                .0;
                            (d, p.residual - h_i)
                        })
                        .collect();
                    match idw(&pairs) {
                        Some((m, v, n)) => h_mean + lam(n, v, m, 0.0) * m,
                        None => h_mean,
                    }
                };
                r_hat[14] = rr(1);
                r_hat[15] = match g_loo {
                    Some((m, v, n)) => h_pred[1].0 + lam(n, v, m, h_pred[1].0) * (m - h_pred[1].0),
                    None => h_pred[1].0,
                };
                r_hat[16] = match (g_loo, snaps[1].as_ref()) {
                    (Some((m, _, n)), Some(snap)) if h_pred[1].1 > 0.0 => {
                        let w_h = 1.0 / h_pred[1].1;
                        let w_k = n / snap.sigma2;
                        (h_pred[1].0 * w_h + m * w_k) / (w_h + w_k)
                    }
                    _ => h_pred[1].0,
                };
                r_hat[17] = rr(5);

                // f: renegade's learned metric.
                if let Some(e) = renegade_res.predict_native(&observation) {
                    r_hat[18] = e.residual;
                    r_hat[19] = lam(e.n_eff, e.variance, e.residual, 0.0) * e.residual;
                }

                for row in 4..20 {
                    prediction[row] = finish(global + r_hat[row]);
                }

                // Post-hoc rows.
                let long_prior = long.value(distance).map(finish);
                let long_at = |d: f64| long.value(d).map(finish);
                let predict_one = |re: &Reanchored, snaps: &[Option<Snapshot>], h: usize| {
                    re.hiers[h].predict(
                        snaps[h].as_ref(),
                        peer_index,
                        attribute,
                        contract_value,
                        distance,
                    )
                };
                let snaps_window = re_window.snapshots(time);
                prediction[21] = finish(global + predict_one(&re_window, &snaps_window, 0).0);
                if let Some(long_prior) = long_prior {
                    prediction[20] = long_prior;
                    let snaps_long = re_long.snapshots(time);
                    prediction[22] = finish(long_prior + predict_one(&re_long, &snaps_long, 0).0);
                    let snaps_star = re_star.snapshots(time);
                    let star: Vec<(f64, f64)> = (0..HORIZONS.len())
                        .map(|h| predict_one(&re_star, &snaps_star, h))
                        .collect();
                    let selected = re_star.selected(time);
                    let (star_mean, star_var) = star[selected];
                    prediction[23] = finish(long_prior + star_mean);
                    // kNN over residuals of the long curve, re-anchored at query.
                    let long_residuals: Vec<(f64, usize, f64)> = gower
                        .iter()
                        .take(loo_k[0])
                        .filter_map(|&(d, i)| {
                            long_at(history[i].distance).map(|g| (d, i, history_y[i] - g))
                        })
                        .collect();
                    let rr_pairs: Vec<(f64, f64)> = long_residuals
                        .iter()
                        .map(|&(d, i, r)| {
                            let p = &history[i];
                            let h_i = re_star.hiers[selected]
                                .predict(
                                    snaps_star[selected].as_ref(),
                                    p.peer_index,
                                    history_attr[i],
                                    p.contract,
                                    p.distance,
                                )
                                .0;
                            (d, r - h_i)
                        })
                        .collect();
                    prediction[24] = finish(
                        long_prior
                            + star_mean
                            + match idw(&rr_pairs) {
                                Some((m, v, n)) => lam(n, v, m, 0.0) * m,
                                None => 0.0,
                            },
                    );
                    let plain: Vec<(f64, f64)> =
                        long_residuals.iter().map(|&(d, _, r)| (d, r)).collect();
                    prediction[25] = match (idw(&plain), snaps_star[selected].as_ref()) {
                        (Some((m, _, n)), Some(snap)) if star_var > 0.0 => {
                            let w_h = 1.0 / star_var;
                            let w_k = n / snap.sigma2;
                            finish(long_prior + (star_mean * w_h + m * w_k) / (w_h + w_k))
                        }
                        _ => prediction[23],
                    };
                    star_pending = Some((long_prior, star.iter().map(|s| s.0).collect()));
                } else {
                    for row in [20, 22, 23, 24, 25] {
                        prediction[row] = global;
                    }
                }

                match shrunk_long.value(distance).map(finish) {
                    Some(prior) => {
                        prediction[26] = prior;
                        let snaps = re_star_shrunk.snapshots(time);
                        let per_horizon: Vec<f64> = (0..HORIZONS.len())
                            .map(|h| predict_one(&re_star_shrunk, &snaps, h).0)
                            .collect();
                        let selected = re_star_shrunk.selected(time);
                        prediction[27] = finish(prior + per_horizon[selected]);
                        shrunk_pending = Some((prior, per_horizon));
                    }
                    None => {
                        prediction[26] = global;
                        prediction[27] = global;
                    }
                }
                joint_pending = joint.predict(
                    time,
                    peer_index,
                    attribute,
                    contract_value,
                    distance,
                    finish,
                );
                prediction[28] = match joint.selected(time, &joint_pending) {
                    Some(h) => finish(joint_pending[h].expect("selected horizon has a curve")),
                    None => global,
                };

                // Ground truth exists only for existing contracts.
                if index >= WARMUP_EVENTS && !op.absent {
                    let in_subset = [
                        true,
                        world.in_pair(peer_index, contract_value),
                        peer_events[peer_index] < COLD_START_EVENTS,
                        world.drift_changed(peer_index) && index >= EVENTS / 2,
                        world.near_absent(contract_value),
                    ];
                    for (subset, &active) in in_subset.iter().enumerate() {
                        if !active {
                            continue;
                        }
                        counts[subset] += 1;
                        for (row, value) in prediction.iter().enumerate() {
                            err[subset][row] += (value - truth).powi(2);
                        }
                    }
                }
            }

            pending.push(Pending {
                peer_index,
                distance,
                time,
                y,
                global,
                star: star_pending,
                shrunk: shrunk_pending,
                joint: joint_pending,
            });
            peer_events[peer_index] += 1;
            index += 1;
            if y == 0.0 && spec.target == Target::Failure {
                succeeded = true;
                break;
            }
        }

        // Label policy decides, at op end, whether its attempts are learned.
        let learn = match spec.labeling {
            Labeling::All => true,
            Labeling::Delayed => {
                !op.absent
                    && (succeeded || GlobalRng::random_range(0.0..1.0) < CONFIRMED_EXHAUSTED_SHARE)
            }
        };
        if !learn {
            continue;
        }
        for pend in &pending {
            let Pending {
                peer_index,
                distance,
                time,
                y,
                global,
                ..
            } = *pend;
            let peer = &world.peers[peer_index];
            let attribute = world.attribute[peer_index];
            if let Some(global) = global {
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
                    legacy_abs_raw.add(observation.clone(), y.exp());
                    if legacy_abs_raw.should_train() {
                        legacy_abs_raw.train();
                    }
                }
                let residual = y - global;
                renegade_res.add(observation, residual);
                if renegade_res.should_train() {
                    renegade_res.train();
                }
                for hier in &mut hiers {
                    hier.add(
                        peer_index,
                        attribute,
                        contract_value,
                        distance,
                        time,
                        residual,
                    );
                }
                history.push(ResidualPoint {
                    peer_id: id as f64,
                    peer_index,
                    contract: contract_value,
                    distance,
                    time,
                    residual,
                });
                history_attr.push(attribute);
                history_y.push(y);
                let event = Raw {
                    peer: peer_index,
                    attribute,
                    contract: contract_value,
                    distance,
                    time,
                    y,
                };
                if let Some((star_prior, star_predictions)) = &pend.star {
                    re_star.score(star_predictions, y - star_prior, time);
                }
                if let Some((prior, predictions)) = &pend.shrunk {
                    re_star_shrunk.score(predictions, y - prior, time);
                }
                if !pend.joint.is_empty() {
                    joint.score(&pend.joint, y, time);
                }
                raw.push(event);
                re_window.add(&event, global);
                if let Some(g) = long.value(distance).map(finish) {
                    re_long.add(&event, g);
                    re_star.add(&event, g);
                }
                if let Some(g) = shrunk_long.value(distance).map(finish) {
                    re_star_shrunk.add(&event, g);
                }
                joint.add(&event, finish);
                learned_since_rebuild += 1;
                let n = history.len();
                if n >= MIN_OBSERVATIONS_FOR_TRAINING
                    && (loo_trained_at == 0 || n >= loo_trained_at + loo_trained_at / 2)
                {
                    loo_k = [
                        loo_select_k(&history, GOWER),
                        loo_select_k(&history, PEER_FIRST),
                    ];
                    loo_trained_at = n;
                }
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
                long.refit(&raw);
                re_window.rebuild(&raw, |r| {
                    let location = Location::try_from(r.contract).expect("contract within ring");
                    iso.estimate_global(&world.peers[r.peer], location)
                        .ok()
                        .map(finish)
                });
                re_long.rebuild(&raw, |r| long.value(r.distance).map(finish));
                re_star.rebuild(&raw, |r| long.value(r.distance).map(finish));
                let now = raw.last().map_or(0.0, |r| r.time);
                shrunk_long.refit(&raw, now);
                re_star_shrunk.rebuild(&raw, |r| shrunk_long.value(r.distance).map(finish));
                joint.rebuild(&raw, now, finish);
                learned_since_rebuild = 0;
            }
        }
    }

    let components = hiers[1]
        .snapshot(EVENTS as f64 / 60.0)
        .map_or([f64::NAN; 3], |s| [s.sigma2, s.tau2_cell, s.tau2_peer]);
    RunResult {
        mse: err
            .into_iter()
            .zip(counts)
            .map(|(row, n)| {
                row.into_iter()
                    .map(|e| if n == 0 { f64::NAN } else { e / n as f64 })
                    .collect()
            })
            .collect(),
        counts,
        components,
        ranking,
        decisions,
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

/// Fresh seeds, never used while diagnosing run 1, to confirm the post-hoc
/// rows are not fitted to the original five draws.
const CONFIRMATION_SEEDS: [u64; 5] = [
    0x4485_b001,
    0x4485_b002,
    0x4485_b003,
    0x4485_b004,
    0x4485_b005,
];

/// The bake-off on the original seeds. Diagnostic tables only; asserts are
/// sanity checks.
#[test]
fn estimator_bakeoff_across_realistic_scenarios() {
    bakeoff_report(&SEEDS, "original seeds");
}

/// The same bake-off on fresh seeds.
#[test]
fn estimator_bakeoff_confirmation_seeds() {
    bakeoff_report(&CONFIRMATION_SEEDS, "CONFIRMATION seeds");
}

fn bakeoff_report(seeds: &[u64], title: &str) {
    let specs = scenarios();
    let rows = ROW_LABELS.len();
    let threads = std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .clamp(1, 8);

    // results[scenario][seed]
    let mut results: Vec<Vec<RunResult>> = vec![Vec::new(); specs.len()];
    for chunk in (0..specs.len()).collect::<Vec<_>>().chunks(threads) {
        let finished: Vec<(usize, Vec<RunResult>)> = std::thread::scope(|scope| {
            let handles: Vec<_> = chunk
                .iter()
                .map(|&s| {
                    let spec = specs[s];
                    scope.spawn(move || {
                        // Warm the per-thread key cache so the first seeded run
                        // sees the same stream as later ones.
                        let _ = PeerKeyLocation::random();
                        (
                            s,
                            seeds.iter().map(|&seed| run_bakeoff(spec, seed)).collect(),
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

    let mut out = String::from(
        "\n#4485 ESTIMATOR BAKE-OFF (mse vs truth; failure: p*, timing: ln-ms mu*; \
         mean over seeds; ratio = row / legacy on the same subset)\n",
    );
    out.push_str(&format!("seed set: {title} {seeds:x?}\n"));
    // ratio[scenario][subset][row] of seed-mean mse.
    let mut ratios: Vec<Vec<Vec<f64>>> = Vec::new();
    // Per-seed worst overall ratio, per row.
    let mut worst_seed_ratio = vec![0.0f64; rows];

    for (s, spec) in specs.iter().enumerate() {
        let runs = &results[s];
        for run in runs {
            for row in 0..rows {
                assert!(
                    run.mse[0][row].is_finite(),
                    "{} {} not finite",
                    spec.name,
                    ROW_LABELS[row]
                );
                worst_seed_ratio[row] =
                    worst_seed_ratio[row].max(run.mse[0][row] / run.mse[0][BASELINE]);
            }
        }
        let counts: Vec<f64> = (0..SUBSETS.len())
            .map(|sub| {
                mean(
                    &runs
                        .iter()
                        .map(|r| r.counts[sub] as f64)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        let comps: Vec<f64> = (0..3)
            .map(|c| mean(&runs.iter().map(|r| r.components[c]).collect::<Vec<_>>()))
            .collect();
        out.push_str(&format!(
            "\n== {} ({:?}) — mean scored events: all {:.0}, targeted {:.0}, cold {:.0}, \
             drifted {:.0}; H(contract) end components sigma2 {:.4} tau2_cell {:.4} \
             tau2_peer {:.4}\n",
            spec.name,
            spec.target,
            counts[0],
            counts[1],
            counts[2],
            counts[3],
            comps[0],
            comps[1],
            comps[2]
        ));
        let active: Vec<usize> = (0..SUBSETS.len())
            .filter(|&sub| sub == 0 || counts[sub] >= 1.0)
            .collect();
        out.push_str(&format!("{:<34}", "estimator"));
        for &sub in &active {
            out.push_str(&format!("{:>11}{:>7}", SUBSETS[sub], "ratio"));
        }
        out.push_str("   all [min-max over seeds]\n");
        let mut scenario_ratios = vec![vec![f64::NAN; rows]; SUBSETS.len()];
        for row in 0..rows {
            out.push_str(&format!("{:<34}", ROW_LABELS[row]));
            for &sub in &active {
                // Mean over seeds where the subset was non-empty.
                let seed_values: Vec<(f64, f64)> = runs
                    .iter()
                    .filter(|r| r.counts[sub] > 0)
                    .map(|r| (r.mse[sub][row], r.mse[sub][BASELINE]))
                    .collect();
                let m = mean(&seed_values.iter().map(|v| v.0).collect::<Vec<_>>());
                let b = mean(&seed_values.iter().map(|v| v.1).collect::<Vec<_>>());
                let ratio = m / b;
                scenario_ratios[sub][row] = ratio;
                out.push_str(&format!("{m:>11.5}{ratio:>7.2}"));
            }
            let all: Vec<f64> = runs.iter().map(|r| r.mse[0][row]).collect();
            let min = all.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = all.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            out.push_str(&format!("   [{min:.5}-{max:.5}]\n"));
        }
        ratios.push(scenario_ratios);
    }

    // Summary: overall ratio matrix and worst cases.
    out.push_str("\n== OVERALL mse ratio vs legacy, scenario x estimator\n");
    out.push_str(&format!("{:<34}", "estimator"));
    for spec in &specs {
        out.push_str(&format!("{:>13}", spec.name));
    }
    out.push_str(&format!(
        "{:>9}{:>9}{:>9}\n",
        "worst", "sub-wst", "seed-wst"
    ));
    let mut verdicts = Vec::new();
    for row in 0..rows {
        out.push_str(&format!("{:<34}", ROW_LABELS[row]));
        let mut worst = (0.0f64, "");
        let mut worst_sub = (0.0f64, String::new());
        for (s, spec) in specs.iter().enumerate() {
            let ratio = ratios[s][0][row];
            out.push_str(&format!("{ratio:>13.3}"));
            if ratio > worst.0 {
                worst = (ratio, spec.name);
            }
            for sub in 1..SUBSETS.len() {
                let r = ratios[s][sub][row];
                if r.is_finite() && r > worst_sub.0 {
                    worst_sub = (r, format!("{}:{}", spec.name, SUBSETS[sub]));
                }
            }
        }
        out.push_str(&format!(
            "{:>9.3}{:>9.3}{:>9.3}\n",
            worst.0, worst_sub.0, worst_seed_ratio[row]
        ));
        verdicts.push((row, worst, worst_sub));
    }
    out.push_str(&format!(
        "\n== WORST RATIO vs legacy per estimator (material = > {MATERIAL_RATIO})\n"
    ));
    for (row, worst, worst_sub) in verdicts {
        let improved = specs
            .iter()
            .enumerate()
            .filter(|(s, _)| ratios[*s][0][row] < 1.0 / MATERIAL_RATIO)
            .count();
        out.push_str(&format!(
            "{:<34} overall worst {:.3} ({}), subset worst {:.3} ({}), {}; \
             materially better overall in {improved}/{} scenarios\n",
            ROW_LABELS[row],
            worst.0,
            worst.1,
            worst_sub.0,
            worst_sub.1,
            if worst.0 > MATERIAL_RATIO {
                "FAILS overall"
            } else {
                "passes overall"
            },
            specs.len()
        ));
    }

    // Label noise: degradation vs the clean ops scenario, rank, and worst
    // ratio vs legacy within each labeling policy.
    let index_of = |name: &str| {
        specs
            .iter()
            .position(|s| s.name == name)
            .expect("noise scenario present")
    };
    let clean = index_of("f.ops-clean");
    let groups: [(&str, [&str; 3]); 5] = [
        (
            "(a) naive, clustered keys",
            ["f.ops-clean", "f.abs5-naive", "f.abs20-naive"],
        ),
        (
            "(b) delayed, clustered keys",
            ["f.ops-delayed", "f.abs5-delayed", "f.abs20-delayed"],
        ),
        (
            "(c) naive, UNIFORM keys",
            ["f.ops-clean", "f.abs5-naive-uni", "f.abs20-naive-uni"],
        ),
        (
            "(d) delayed, UNIFORM keys",
            ["f.ops-delayed", "f.abs5-delayed-uni", "f.abs20-delayed-uni"],
        ),
        (
            "(e) hot key (1% of ops poll one absent key)",
            ["f.ops-clean", "f.hotkey1-naive", "f.hotkey1-delayed"],
        ),
    ];
    let seed_mean =
        |s: usize, row: usize| mean(&results[s].iter().map(|r| r.mse[0][row]).collect::<Vec<_>>());
    let rank = |s: usize, row: usize| {
        let me = seed_mean(s, row);
        1 + (0..rows).filter(|&other| seed_mean(s, other) < me).count()
    };
    out.push_str(
        "\n== LABEL NOISE: per estimator, for each scenario: mse / own mse in f.ops-clean \
         (degradation), rank among all rows (1 = best), ratio vs legacy; then worst ratio \
         vs legacy within the policy group (incl. clean)\n",
    );
    for (group, names) in groups {
        out.push_str(&format!("-- {group}: {names:?}\n"));
        for row in 0..rows {
            out.push_str(&format!("{:<34}", ROW_LABELS[row]));
            let mut worst = ratios[clean][0][row];
            for name in names {
                let s = index_of(name);
                worst = worst.max(ratios[s][0][row]);
                out.push_str(&format!(
                    "  deg {:>5.2} rank {:>2} ratio {:>5.2} |",
                    seed_mean(s, row) / seed_mean(clean, row),
                    rank(s, row),
                    ratios[s][0][row]
                ));
            }
            out.push_str(&format!("  worst {worst:.3}\n"));
        }
    }

    // Ranking: does the estimator still pick the truly best candidate?
    out.push_str(&format!(
        "\n== RANKING (ops scenarios): best@{RANK_CANDIDATES} = fraction of decisions where the \
         lowest-predicted candidate among the {RANK_CANDIDATES} nearest peers is the lowest-p* \
         one; regret = mean p*(chosen) - p*(best); best@3 over the 3 nearest. Mean over seeds.\n"
    ));
    let rank_specs: Vec<usize> = specs
        .iter()
        .enumerate()
        .filter(|(_, spec)| spec.ops)
        .map(|(i, _)| i)
        .collect();
    out.push_str(&format!(
        "{:<40}",
        "estimator  (best@10 / regret@10 / best@3)"
    ));
    for &s in &rank_specs {
        out.push_str(&format!("{:>22}", specs[s].name));
    }
    out.push('\n');
    for (row, label) in RANK_ROWS.iter().enumerate() {
        out.push_str(&format!("{label:<40}"));
        for &s in &rank_specs {
            let per = |k: usize| {
                mean(
                    &results[s]
                        .iter()
                        .map(|r| r.ranking[row][k] / r.decisions.max(1) as f64)
                        .collect::<Vec<_>>(),
                )
            };
            out.push_str(&format!("   {:.3}/{:.4}/{:.3}", per(0), per(1), per(2)));
        }
        out.push('\n');
    }
    out.push_str(&format!("{:<40}", "mean decisions per run"));
    for &s in &rank_specs {
        let d = mean(
            &results[s]
                .iter()
                .map(|r| r.decisions as f64)
                .collect::<Vec<_>>(),
        );
        out.push_str(&format!("{d:>22.0}"));
    }
    out.push('\n');
    eprintln!("{out}");
}
