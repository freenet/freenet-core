//! Synthetic routing scenarios whose generating failure probability is
//! known exactly, so an estimator's error can be measured rather than
//! guessed. Used by the router's head-to-head tests (#4485).
//!
//! Moved here from the removed Renegade module, where it was written to
//! test the residual correction; the scenarios themselves are unchanged.

use crate::config::GlobalRng;
use crate::ring::PeerKeyLocation;

/// Shortest arc distance on the `[0, 1]` ring.
fn ring_distance(a: f64, b: f64) -> f64 {
    let d = (a - b).abs();
    d.min(1.0 - d)
}

/// Events before scoring starts, so the isotonic base has a curve to be
/// corrected and the comparison is not dominated by cold start.
pub(crate) const WARMUP_EVENTS: usize = 300;

/// Sized from production: nova's gateways hold 4,155 and 2,745 failure
/// observations. A mechanism that needs materially more than this cannot
/// work on a real node however elegant it is, so the budget is the
/// assertion, not an implementation detail.
pub(crate) const RECOVERY_BUDGET_EVENTS: usize = 2_000;

const PEER_COUNT: usize = 12;

/// What generated the outcomes. Each isolates one capability.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Model {
    /// `p* = f(distance)` only. The isotonic base should capture this and
    /// the correction should add ~nothing.
    DistanceOnly,
    /// `p* = f(distance) + g(peer)`. The per-peer EWMA should capture it.
    PeerMarginal,
    /// `p* = f(distance) + penalty` on specific (peer, contract) pairs.
    /// Distance and a per-peer offset cannot capture this.
    PeerContract,
}

/// Targeted (peer, contract-offset) pairs for `PeerContract`.
///
/// The offsets deliberately SPAN A RANGE OF DISTANCES. A single peer with a
/// single narrow contract band — as in the abandoned harness on
/// `rescue/dirty-residual-routing-correction` — puts every targeted event at
/// nearly one distance, where the GLOBAL isotonic curve can absorb part of
/// the effect. The test then passes for the wrong reason, or understates the
/// correction's contribution. Spreading the offsets makes the effect
/// genuinely inseparable from distance alone.
const TARGETED: [(usize, f64); 3] = [(0, 0.05), (1, 0.17), (2, 0.31)];

/// Half-width of a targeted contract band.
const BAND: f64 = 0.02;

pub(crate) struct Scenario {
    pub(crate) peers: Vec<PeerKeyLocation>,
}

impl Scenario {
    pub(crate) fn new() -> Self {
        Scenario {
            peers: (0..PEER_COUNT).map(|_| PeerKeyLocation::random()).collect(),
        }
    }

    fn peer_location(&self, index: usize) -> f64 {
        self.peers[index]
            .location()
            .expect("generated peers carry a location")
            .as_f64()
    }

    /// Centre of the targeted band for a targeted peer, placed at a fixed
    /// ring offset from that peer so its distance is controlled.
    fn band_centre(&self, peer_index: usize, offset: f64) -> f64 {
        (self.peer_location(peer_index) + offset).rem_euclid(1.0)
    }

    pub(crate) fn is_targeted(&self, peer_index: usize, contract: f64) -> bool {
        TARGETED.iter().any(|&(target, offset)| {
            target == peer_index && ring_distance(contract, self.band_centre(target, offset)) < BAND
        })
    }

    /// The generating probability. Known exactly, which is the whole point.
    pub(crate) fn true_probability(
        &self,
        model: Model,
        peer_index: usize,
        contract: f64,
        distance: f64,
    ) -> f64 {
        let base = match model {
            // Concave rather than linear, so the monotone isotonic base is
            // not trivially perfect and the test says something about fit.
            // Listed exhaustively so a new model must decide its own base
            // rather than silently inheriting this one.
            Model::DistanceOnly | Model::PeerMarginal | Model::PeerContract => {
                0.03 + 0.45 * distance.sqrt()
            }
        };
        let extra = match model {
            Model::DistanceOnly => 0.0,
            // A per-peer offset the EWMA can absorb.
            Model::PeerMarginal => {
                if peer_index % 4 == 0 {
                    0.30
                } else {
                    0.0
                }
            }
            Model::PeerContract => {
                if self.is_targeted(peer_index, contract) {
                    0.55
                } else {
                    0.0
                }
            }
        };
        (base + extra).clamp(0.01, 0.99)
    }

    /// Draw the next event, biased so targeted pairs are sampled often
    /// enough to be learnable but stay a small minority of traffic.
    pub(crate) fn draw(&self, model: Model, index: usize) -> (usize, f64) {
        let targeted_turn = model == Model::PeerContract && index % 12 == 0;
        if targeted_turn {
            let (peer_index, offset) = TARGETED[(index / 12) % TARGETED.len()];
            let centre = self.band_centre(peer_index, offset);
            let jitter = GlobalRng::random_range(-BAND..BAND);
            (peer_index, (centre + jitter).rem_euclid(1.0))
        } else {
            (
                GlobalRng::random_range(0..PEER_COUNT),
                GlobalRng::random_range(0.0..1.0),
            )
        }
    }
}

/// Average a metric over seeds, so a threshold is not riding on one draw.
pub(crate) const SEEDS: [u64; 5] = [
    0x4485_0001,
    0x4485_0002,
    0x4485_0003,
    0x4485_0004,
    0x4485_0005,
];
