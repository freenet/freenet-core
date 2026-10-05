//! The per-peer page, served at `/peer/{address}`: what this node has learned
//! about one connected peer, what it does with that, and how the peer compares
//! with the node's other peers. Everything network-wide is on `/routing`.
//!
//! The dashboard's auto-refresh replaces `<main>` every few seconds, so every
//! chart here is server-rendered SVG whose only interaction is `<title>`
//! tooltips. Charts that carry text along an axis are rendered twice, at a
//! desktop and a phone width, and CSS shows the one that fits.

use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::charts::{
    LogAxis, downsample, faint_dots, fmt_speed, fmt_time, fmt_trim, interpolate, responsive,
    tooltip,
};
use super::estimator::is_floored_speed;
use super::*;
use crate::node::network_status::PeerSnapshot;
use crate::router::{
    PeerOffset, PeerRoutingSnapshot, PeerWindow, Router, RouterSnapshotInfo, RoutingCurve,
};

/// Share of a peer's own record the estimate must adopt before the page treats
/// what it learned as known rather than still being learned.
///
/// Taken from the estimator itself: [`PeerOffset::weight`] is how far the
/// estimate moved from distance alone toward the peer's own average. Below one
/// half, distance alone still outweighs the peer's record, so "your node
/// predicts its results from distance alone" is the more accurate sentence.
const KNOWN_WEIGHT: f64 = 0.5;

/// A response-time or transfer-speed factor this close to 1 reads as "about
/// the same as distance alone predicts" rather than a one-decimal ratio.
const SAME_FACTOR: f64 = 1.05;

/// The avoided-peer reading needs at least this many decisions the peer was
/// eligible for, so a handful of unlucky draws cannot trigger it.
const AVOIDED_MIN_ELIGIBLE: u64 = 20;

/// ... and the peer must have been chosen less than this share of what an even
/// split of those decisions would have given it ([`PeerSelection::even_share`]).
/// Closer peers win more often than farther ones, so an even split is not the
/// expectation; half of it is a deliberately low bar for "most went elsewhere".
///
/// [`PeerSelection::even_share`]: crate::router::PeerSelection::even_share
const AVOIDED_SHARE_OF_EVEN: f64 = 0.5;

/// ... and the node must have learned that it is worse than distance alone
/// predicts by at least one of these, with [`KNOWN_WEIGHT`] behind it: one
/// percentage point more likely to fail, or replies 1.25 times slower.
const AVOIDED_FAILURE_POINTS: f64 = 0.01;
const AVOIDED_SLOWER_FACTOR: f64 = 1.25;

/// Most of this peer's own dots drawn on one chart; more are thinned evenly.
const MAX_PEER_POINTS: usize = 150;

/// The router's failure window, which the reliability tile and the request
/// tabs count (`IsotonicEstimator`'s `MAX_REGRESSION_POINTS`).
const WINDOW_REQUESTS: usize = 500;

/// Timed replies a timing stage of the routing model needs before it predicts
/// on its own ([`crate::router::MIN_CURVE_POINTS_LOG`]); until then routing
/// uses an early estimate.
const MIN_TIMED_REPLIES: usize = crate::router::MIN_CURVE_POINTS_LOG;

/// The link to `/routing`, worded as the title of the card it lands on.
pub(super) const ROUTING_LINK: &str = "How good are the predictions across all peers?";

/// Prediction/outcome pairs needed before the page states how close this
/// peer's predictions land.
const MIN_PAIRS_TO_JUDGE: usize = 5;

/// What the page reads from the network status snapshot.
pub(super) struct PeerPageInputs<'a> {
    pub peer: &'a PeerSnapshot,
    /// Every connected peer, this one included.
    pub peers: &'a [PeerSnapshot],
    pub own_location: Option<f64>,
    pub version: &'a str,
}

/// What the page reads from the router, taken under its read lock and
/// rendered after the lock is released.
pub(super) struct RouterView {
    snapshot: RouterSnapshotInfo,
    /// `None` when this peer has no key location to look up.
    peer: Option<PeerRoutingSnapshot>,
    /// Every connected peer's learned differences, this one included.
    others: Vec<(String, [Option<PeerOffset>; 3])>,
    /// `(outcomes, failures)` across every peer in the router's failure
    /// window, the population this peer's window counts are a share of.
    window_outcomes: (usize, usize),
    fallback: bool,
}

impl RouterView {
    pub(super) fn gather(inputs: &PeerPageInputs<'_>, router: &Router) -> Self {
        RouterView {
            snapshot: router.snapshot(),
            peer: inputs
                .peer
                .peer_key_location
                .as_ref()
                .map(|key| router.peer_snapshot(key)),
            others: inputs
                .peers
                .iter()
                .filter_map(|peer| {
                    let key = peer.peer_key_location.as_ref()?;
                    Some((peer.address.to_string(), router.peer_offsets(key)))
                })
                .collect(),
            window_outcomes: router.window_outcomes(),
            fallback: crate::router::isotonic_fallback_enabled(),
        }
    }
}

pub fn peer_detail_html(address_str: &str) -> String {
    let snap = network_status::get_snapshot();
    let found = snap.as_ref().and_then(|snap| {
        snap.peers
            .iter()
            .find(|p| p.address.to_string() == address_str)
            .map(|peer| PeerPageInputs {
                peer,
                peers: &snap.peers,
                own_location: snap.own_location,
                version: &snap.version,
            })
    });
    let Some(inputs) = found else {
        return format!(
            include_str!("assets/peer_not_found.html"),
            CSS = CSS,
            PEER_CSS = PEER_CSS,
            JS = JS,
            addr = html_escape(address_str),
        );
    };
    let view = network_status::get_router().map(|lock| RouterView::gather(&inputs, &lock.read()));
    render_peer_page(&inputs, view.as_ref())
}

/// [`peer_detail_html`] from explicit inputs, so tests need no global state.
pub(super) fn render_peer_page(inputs: &PeerPageInputs<'_>, view: Option<&RouterView>) -> String {
    let body = match view {
        Some(view) => {
            let learned = Learned::of(view);
            [
                learned_card(view, &learned),
                compare_card(inputs, view),
                past_requests_card(view, &learned),
                accuracy_card(view),
            ]
            .concat()
        }
        None => r#"<div class="card"><h2>What your node has learned</h2><p class="empty">Routing data is not available yet.</p></div>"#
            .to_string(),
    };
    format!(
        include_str!("assets/peer.html"),
        addr = html_escape(&inputs.peer.address.to_string()),
        CSS = CSS,
        PEER_CSS = PEER_CSS,
        JS = JS,
        version = html_escape(inputs.version),
        header_card = header_card(inputs),
        body = body,
    )
}

// ─── What the node has learned about this peer ──────────────────────────────

/// Whether, and how far, the node still predicts this peer from distance alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Learning {
    /// It has learned enough to use the peer's own record, or there is
    /// nothing to learn yet that would tell this peer from any other.
    No,
    /// No warm stage has adopted any of the peer's record.
    FromDistanceAlone,
    /// Some stage has adopted part of it, less than [`KNOWN_WEIGHT`].
    Mostly,
}

/// The readings the tiles, banners and charts share, computed once.
struct Learned {
    /// The response-time factor against distance alone, when there is enough
    /// evidence behind it to state (see [`known`]).
    response_factor: Option<f64>,
    learning: Learning,
    /// Why the peer reads as avoided, when it does: `(slower, less reliable)`.
    avoided: Option<(bool, bool)>,
}

/// Whether a stage's reading for a peer can be stated: the peer has evidence
/// there, and either the stage adopted at least [`KNOWN_WEIGHT`] of it, or the
/// stage tells no peers apart at all (then its offset is exactly 0 and "as
/// distance alone predicts" is the true reading).
fn known(offset: Option<PeerOffset>, stage_tells_apart: bool) -> bool {
    offset.is_some_and(|o| o.evidence > 0.0 && (o.weight >= KNOWN_WEIGHT || !stage_tells_apart))
}

/// A stage "tells peers apart" once it gives some connected peer's own record
/// any weight. While it gives every peer zero, it has found no difference
/// between peers to learn, which is not the same as lacking evidence.
fn tells_apart(view: &RouterView, stage: usize) -> bool {
    view.others
        .iter()
        .any(|(_, offsets)| offsets[stage].is_some_and(|o| o.weight > 0.0))
}

impl Learned {
    fn of(view: &RouterView) -> Self {
        let nothing = Learned {
            response_factor: None,
            learning: Learning::No,
            avoided: None,
        };
        // No key location: no routing record can be looked up for this peer
        // at all, and nothing will ever arrive. On the emergency fallback
        // routing reads none of this, so no banner claims what it does.
        let Some(peer) = view.peer.as_ref() else {
            return nothing;
        };
        if view.fallback {
            return nothing;
        }
        let [failure, response, _] = peer.offsets;
        let differs = [tells_apart(view, 0), tells_apart(view, 1)];
        let adopted = |o: Option<PeerOffset>| o.is_some_and(|o| o.weight >= KNOWN_WEIGHT);
        let warm = [failure, response]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let learning = if adopted(failure) || adopted(response) {
            Learning::No
        } else if !warm.is_empty() && !differs[0] && !differs[1] {
            // Warm stages that find no difference between any peers: every
            // peer is on the line because they look alike.
            Learning::No
        } else if warm.iter().all(|o| o.weight == 0.0) {
            Learning::FromDistanceAlone
        } else {
            Learning::Mostly
        };
        let response_factor = known(response, differs[1])
            .then(|| response.map(|o| o.offset.exp()))
            .flatten()
            .filter(|factor| factor.is_finite() && *factor > 0.0);
        let slower =
            adopted(response) && response.is_some_and(|o| o.offset.exp() >= AVOIDED_SLOWER_FACTOR);
        let less_reliable =
            adopted(failure) && failure.is_some_and(|o| o.offset >= AVOIDED_FAILURE_POINTS);
        let passed_over = peer.selection.is_some_and(|selection| {
            selection.eligible >= AVOIDED_MIN_ELIGIBLE
                && (selection.chosen as f64) < AVOIDED_SHARE_OF_EVEN * selection.even_share
        });
        Learned {
            response_factor,
            learning,
            avoided: ((slower || less_reliable) && passed_over).then_some((slower, less_reliable)),
        }
    }
}

fn learned_card(view: &RouterView, learned: &Learned) -> String {
    let mut banners = String::new();
    if view.fallback {
        banners.push_str(
            r#"<div class="learning">Routing is on its emergency fallback right now, so it does not use the comparisons with distance alone on this page.</div>"#,
        );
    }
    if let Some((slower, less_reliable)) = learned.avoided {
        let worse = match (slower, less_reliable) {
            (true, true) => "slower and less reliable",
            (true, false) => "slower",
            _ => "less reliable",
        };
        write!(
            banners,
            r#"<div class="learning">Your node has learned this peer is {worse} than distance alone predicts, and recently ranks it first far less often than its share.</div>"#
        )
        .ok();
    } else {
        match learned.learning {
            Learning::FromDistanceAlone => banners.push_str(
                r#"<div class="learning">Still learning about this peer. Until it has handled more requests, your node predicts its results from distance alone.</div>"#,
            ),
            Learning::Mostly => banners.push_str(
                r#"<div class="learning">Still learning about this peer. Until it has handled more requests, your node predicts its results mostly from distance alone.</div>"#,
            ),
            Learning::No if view.peer.is_none() => banners.push_str(
                r#"<div class="learning">Your node has no routing record it can match to this peer.</div>"#,
            ),
            Learning::No => {}
        }
    }
    format!(
        r#"<div class="card">
            <h2>What your node has learned</h2>
            {banners}
            <div class="tiles">{reliability}{response}{chosen}</div>
        </div>"#,
        reliability = reliability_tile(view),
        response = response_tile(view, learned),
        chosen = chosen_tile(view),
    )
}

fn tile(key: &str, value: &str, sub: &str, title: &str) -> String {
    let title = if title.is_empty() {
        String::new()
    } else {
        format!(r#" title="{}""#, html_escape(title))
    };
    format!(
        r#"<div class="tile"{title}><div class="k">{key}</div><div class="v">{value}</div><div class="s">{sub}</div></div>"#
    )
}

/// This peer's outcomes in the router's failure window: the same population
/// the request tabs count, so the tile and the tabs always agree.
fn reliability_tile(view: &RouterView) -> String {
    let (network_total, network_failed) = view.window_outcomes;
    let network = if network_total == 0 {
        String::new()
    } else if network_failed == 0 {
        " Across all your peers, none failed.".to_string()
    } else {
        let one_in = network_total as f64 / network_failed as f64;
        if one_in >= 3.0 {
            format!(" Across all your peers, about 1 in {one_in:.0} fails.")
        } else {
            format!(
                " Across all your peers, {:.0}% fail.",
                100.0 * network_failed as f64 / network_total as f64
            )
        }
    };
    let title = "Requests to this peer among your node's last 500 routed requests to any \
                 peer, including requests it relayed for others. A failed request may have \
                 failed further along the route, not at this peer.";
    let window = format!("of your node's last {WINDOW_REQUESTS} requests (relayed ones included)");
    match view.peer.as_ref().map(|peer| &peer.window) {
        Some(window_counts) if window_counts.outcomes > 0 => tile(
            "Reliability",
            &format!(
                "{} <small>of {}</small>",
                window_counts.outcomes - window_counts.failures,
                window_counts.outcomes
            ),
            &format!("requests to it succeeded, {window}.{network}"),
            title,
        ),
        _ => tile(
            "Reliability",
            "&mdash;",
            &format!("No requests to it {window}.{network}"),
            title,
        ),
    }
}

/// Routing's own response-time prediction near the peer, and, once the node
/// has learned it, how the peer typically differs from distance alone.
fn response_tile(view: &RouterView, learned: &Learned) -> String {
    let peer = view.peer.as_ref();
    let routed = peer
        .and_then(|peer| peer.prediction_at_own_location.as_ref())
        .map(|prediction| prediction.time_to_response_start)
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0);
    let early = peer.is_some_and(|peer| peer.curves[0].early);
    let Some(seconds) = routed else {
        return tile(
            "Response time",
            "&mdash;",
            "Too few replies to judge yet.",
            "",
        );
    };
    let comparison = if view.fallback {
        String::new()
    } else if early {
        format!("; an early estimate until your node has {MIN_TIMED_REPLIES} timed replies")
    } else {
        match learned.response_factor {
            Some(factor) => format!("; it {}", time_comparison(factor, true)),
            None => "; too few replies yet to compare it with distance alone".to_string(),
        }
    };
    tile(
        "Response time",
        &fmt_time_tile(seconds),
        &format!("predicted for contracts near it{comparison}."),
        "",
    )
}

/// Recent routing decisions in which this peer was among the closest
/// candidates scored, and how many ranked it first.
fn chosen_tile(view: &RouterView) -> String {
    let title = "Counted once your node ranks peers by prediction (after its first 50 \
                 requests), including requests it relayed and retries. Both counts halve \
                 every 200 eligible decisions, so they describe the recent past. Eligible: \
                 among the closest peers your node considered for a request.";
    match view.peer.as_ref().and_then(|peer| peer.selection) {
        Some(selection) if selection.eligible > 0 => tile(
            "Chosen",
            &format!(
                "{} <small>of {}</small>",
                selection.chosen, selection.eligible
            ),
            "recent routing decisions it was eligible for (roughly its last 100 to 200) ranked it first.",
            title,
        ),
        _ if !view.snapshot.prediction_active => tile(
            "Chosen",
            "&mdash;",
            "Your node is not ranking peers by prediction yet.",
            title,
        ),
        _ => tile(
            "Chosen",
            "&mdash;",
            "Not among the closest candidates for a recent routing decision.",
            title,
        ),
    }
}

// ─── Compared with your other peers ─────────────────────────────────────────

fn compare_card(inputs: &PeerPageInputs<'_>, view: &RouterView) -> String {
    let this = inputs.peer.address.to_string();
    let dots = |stage: usize| -> (Vec<StripDot>, Option<StripDot>) {
        let apart = tells_apart(view, stage);
        let mut others = Vec::new();
        let mut mine = None;
        for (address, offsets) in &view.others {
            let Some(offset) = offsets[stage] else {
                continue;
            };
            // The same gate as the tiles: a reading too thin to state is drawn
            // hollow, on the line, rather than as a difference.
            let stated = known(Some(offset), apart);
            let (value, text) = match (stage, stated) {
                (0, true) => (offset.offset * 100.0, failure_comparison(offset.offset)),
                (0, false) => (0.0, "too few requests to judge yet".to_string()),
                (_, true) => (
                    offset.offset.exp(),
                    time_comparison(offset.offset.exp(), false),
                ),
                (_, false) => (1.0, "too few replies to judge yet".to_string()),
            };
            let who = if address == &this {
                "this peer".to_string()
            } else {
                html_escape(address)
            };
            let dot = StripDot {
                value,
                title: format!("{who} &middot; {text}"),
                href: (address != &this).then(|| format!("/peer/{}", html_escape(address))),
                hollow: !stated,
            };
            if address == &this {
                mine = Some(dot);
            } else {
                others.push(dot);
            }
        }
        (others, mine)
    };
    let (fail_others, fail_mine) = dots(0);
    let (time_others, time_mine) = dots(1);
    let values = |others: &[StripDot], mine: &Option<StripDot>| -> Vec<f64> {
        others
            .iter()
            .chain(mine.iter())
            .map(|dot| dot.value)
            .collect()
    };

    let failure = if fail_others.is_empty() && fail_mine.is_none() {
        r#"<div class="empty-box">Too few requests across your peers to compare yet.</div>"#
            .to_string()
    } else {
        let values = values(&fail_others, &fail_mine);
        responsive(|width| {
            strip_chart(
                width,
                StripAxis::points(values.iter().copied()),
                &fail_others,
                fail_mine.as_ref(),
                "Chance a request fails compared with distance alone, one dot per peer",
            )
        })
    };
    let time = if time_others.is_empty() && time_mine.is_none() {
        r#"<div class="empty-box">Too few replies across your peers to compare yet.</div>"#
            .to_string()
    } else {
        let values = values(&time_others, &time_mine);
        responsive(|width| {
            strip_chart(
                width,
                StripAxis::ratio(values.iter().copied()),
                &time_others,
                time_mine.as_ref(),
                "Response time compared with distance alone, one dot per peer",
            )
        })
    };
    let matched = view.others.len();
    let connected = inputs.peers.len();
    let whose = if matched == connected {
        format!("one of your {connected} peers")
    } else {
        format!(
            "one of {matched} of your {connected} peers (the other {} cannot be matched to a routing record)",
            connected - matched
        )
    };
    format!(
        r#"<div class="card">
            <h2>Compared with your other peers</h2>
            <p class="caption">Each dot is {whose}, placed by how its results typically differ from what distance alone predicts. A peer with too few requests to judge is drawn hollow, on the line.</p>
            <div class="chart-title">Chance a request fails <span class="unit">(percentage points)</span></div>
            {failure}
            <div class="chart-title">Response time</div>
            {time}
        </div>"#
    )
}

/// One peer on a strip chart, in axis units.
struct StripDot {
    value: f64,
    title: String,
    /// Link to another peer's page; `None` for this peer.
    href: Option<String>,
    /// Too little evidence to state a difference: drawn hollow, on the line.
    hollow: bool,
}

/// A strip chart's horizontal axis.
struct StripAxis {
    lo: f64,
    hi: f64,
    log: bool,
    ticks: Vec<f64>,
    /// Format a tick value.
    label: fn(f64) -> String,
    /// Where "distance alone" sits.
    zero: f64,
    /// What the two ends of the axis mean, when the ticks do not say it.
    ends: Option<(&'static str, &'static str)>,
}

impl StripAxis {
    /// Percentage points, linear, always showing -1 to +2 and widened to whole
    /// steps around whatever the peers need.
    fn points(values: impl Iterator<Item = f64>) -> Self {
        let (mut lo, mut hi) = (-1.0f64, 2.0f64);
        for value in values.filter(|v| v.is_finite()) {
            lo = lo.min(value);
            hi = hi.max(value);
        }
        let span = hi - lo;
        let step = if span <= 4.0 {
            1.0
        } else if span <= 8.0 {
            2.0
        } else if span <= 20.0 {
            5.0
        } else if span <= 40.0 {
            10.0
        } else {
            25.0
        };
        let lo = (lo / step).floor() * step;
        let hi = (hi / step).ceil() * step;
        let ticks = (0..)
            .map(|i| lo + step * i as f64)
            .take_while(|tick| *tick <= hi + 1e-9)
            .collect();
        StripAxis {
            lo,
            hi,
            log: false,
            ticks,
            label: |v| {
                if v.abs() < 1e-9 {
                    "0".to_string()
                } else {
                    format!(
                        "{}{}",
                        if v > 0.0 { "+" } else { "&minus;" },
                        fmt_trim(v.abs())
                    )
                }
            },
            zero: 0.0,
            ends: Some(("fewer failures", "more failures")),
        }
    }

    /// A factor on a log axis, symmetric about 1: four times either way, or
    /// eight when a peer is further out than that.
    fn ratio(values: impl Iterator<Item = f64>) -> Self {
        let widest = values
            .filter(|v| v.is_finite() && *v > 0.0)
            .map(|v| v.ln().abs())
            .fold(0.0f64, f64::max);
        let edge: f64 = if widest > 4f64.ln() + 1e-9 { 8.0 } else { 4.0 };
        let mut ticks = vec![1.0 / edge, 1.0 / 2.0, 1.0, 2.0, edge];
        if edge == 8.0 {
            ticks = vec![1.0 / 8.0, 1.0 / 4.0, 1.0 / 2.0, 1.0, 2.0, 4.0, 8.0];
        }
        StripAxis {
            lo: 1.0 / edge,
            hi: edge,
            log: true,
            ticks,
            label: |v| {
                if (v - 1.0).abs() < 1e-9 {
                    "1&times;".to_string()
                } else if v < 1.0 {
                    format!("{}&times; faster", fmt_trim(1.0 / v))
                } else {
                    format!("{}&times; slower", fmt_trim(v))
                }
            },
            zero: 1.0,
            ends: None,
        }
    }

    fn fraction(&self, value: f64) -> f64 {
        let value = value.clamp(self.lo, self.hi);
        if self.log {
            (value.ln() - self.lo.ln()) / (self.hi.ln() - self.lo.ln())
        } else {
            (value - self.lo) / (self.hi - self.lo)
        }
    }
}

/// One dot per peer along a single axis. The other peers' dots spread
/// vertically in a band; "this peer" sits mid-band, larger and in the accent
/// colour, and its label goes in its own row under the band, where no dot can
/// cover it.
fn strip_chart(
    width: f64,
    axis: StripAxis,
    others: &[StripDot],
    mine: Option<&StripDot>,
    aria: &str,
) -> String {
    let (left, right, top) = (34.0, 40.0, 20.0);
    let band = 15.0;
    let mid = top + 4.0 + band;
    let label_row = mid + band + 14.0;
    let axis_y = label_row + 8.0;
    let h = axis_y + if axis.ends.is_some() { 32.0 } else { 20.0 };
    let x = |value: f64| left + axis.fraction(value) * (width - left - right);
    let mut svg =
        format!(r#"<svg viewBox="0 0 {width} {h}" class="mchart" role="img" aria-label="{aria}">"#);
    write!(
        svg,
        r#"<line x1="{left}" x2="{x2}" y1="{axis_y}" y2="{axis_y}" class="ax"/>"#,
        x2 = width - right,
    )
    .ok();
    let narrow = width < 450.0;
    for (index, tick) in axis.ticks.iter().enumerate() {
        // On a phone, every other tick, always keeping both ends and zero.
        let keep = !narrow
            || index == 0
            || index + 1 == axis.ticks.len()
            || (*tick - axis.zero).abs() < 1e-9
            || index % 2 == 0;
        if keep {
            write!(
                svg,
                r#"<text x="{:.1}" y="{:.1}" text-anchor="middle">{}</text>"#,
                x(*tick),
                axis_y + 14.0,
                (axis.label)(*tick)
            )
            .ok();
        }
    }
    if let Some((low, high)) = axis.ends {
        write!(
            svg,
            r#"<text x="{left}" y="{y:.1}" class="dl">&larr; {low}</text><text x="{:.1}" y="{y:.1}" text-anchor="end" class="dl">{high} &rarr;</text>"#,
            width - right,
            y = axis_y + 28.0,
        )
        .ok();
    }
    let zero_x = x(axis.zero);
    // The line stops above the label row, so "this peer" on the line is not
    // struck through.
    write!(
        svg,
        r#"<line x1="{zero_x:.1}" x2="{zero_x:.1}" y1="{:.1}" y2="{:.1}" class="baseline"/><text x="{zero_x:.1}" y="{:.1}" text-anchor="middle" class="dl">distance alone</text>"#,
        top - 6.0,
        label_row - 10.0,
        top - 10.0,
    )
    .ok();
    for (index, dot) in others.iter().enumerate() {
        // Deterministic vertical jitter (golden-ratio sequence), so overlapping
        // peers spread into a band instead of hiding each other.
        let jitter = ((index as f64 + 1.0) * 0.618_033_988_75).fract() * 2.0 - 1.0;
        let circle = format!(
            r#"<circle cx="{:.1}" cy="{:.1}" r="3.2" class="{}">{}</circle>"#,
            x(dot.value),
            mid + jitter * band,
            if dot.hollow {
                "dot-other hollow"
            } else {
                "dot-other"
            },
            tooltip(&dot.title, narrow)
        );
        match &dot.href {
            Some(href) => write!(svg, r#"<a href="{href}">{circle}</a>"#).ok(),
            None => write!(svg, "{circle}").ok(),
        };
    }
    if let Some(dot) = mine {
        let cx = x(dot.value);
        let label_x = cx.clamp(left + 24.0, width - right - 24.0);
        write!(
            svg,
            r#"<circle cx="{cx:.1}" cy="{mid:.1}" r="6" class="{class}">{title}</circle><line x1="{cx:.1}" x2="{cx:.1}" y1="{:.1}" y2="{:.1}" class="tick-this"/><text x="{label_x:.1}" y="{label_row:.1}" text-anchor="middle" class="this-label">this peer</text>"#,
            mid + 7.0,
            label_row - 9.0,
            class = if dot.hollow { "dot-this hollow" } else { "dot-this" },
            title = tooltip(&dot.title, narrow),
        )
        .ok();
    }
    svg.push_str("</svg>");
    svg
}

// ─── Past requests by ring distance ─────────────────────────────────────────

fn past_requests_card(view: &RouterView, learned: &Learned) -> String {
    let rs = &view.snapshot;
    let empty_window = PeerWindow::default();
    let no_curve = RoutingCurve::default();
    let [time_curve, speed_curve] = match view.peer.as_ref() {
        Some(peer) => [&peer.curves[0], &peer.curves[1]],
        None => [&no_curve, &no_curve],
    };
    let floored = view
        .peer
        .as_ref()
        .and_then(|peer| peer.prediction_at_own_location.as_ref())
        .is_some_and(|prediction| is_floored_speed(prediction.transfer_speed_bps));

    let mut labels = String::new();
    let mut panels = String::new();
    for (index, (name, op)) in [
        ("All", None),
        ("GET", Some("GET")),
        ("PUT", Some("PUT")),
        ("UPDATE", Some("UPDATE")),
        ("SUBSCRIBE", Some("SUBSCRIBE")),
    ]
    .into_iter()
    .enumerate()
    {
        let id = name.to_lowercase();
        let mine: &PeerWindow = match (view.peer.as_ref(), op) {
            (Some(peer), None) => &peer.window,
            (Some(peer), Some(op)) => peer.window_by_op.get(op).unwrap_or(&empty_window),
            (None, _) => &empty_window,
        };
        let (all_times, all_speeds) = match op {
            None => (&rs.response_time_points, &rs.transfer_rate_points),
            Some(op) => rs.per_op_curves.get(op).map_or((&EMPTY, &EMPTY), |curves| {
                (&curves.response_time_points, &curves.transfer_rate_points)
            }),
        };
        let active = if index == 0 { " tab-active" } else { "" };
        let dim = if mine.outcomes == 0 && op.is_some() {
            " tab-dim"
        } else {
            ""
        };
        let badge_title = match op {
            None => format!(
                "This peer's requests, failures included, among your node's last {WINDOW_REQUESTS}."
            ),
            Some(op) => format!(
                "This peer's {op} requests, failures included, among your node's last {WINDOW_REQUESTS} {op} requests. Each operation keeps its own window, so the tabs need not add up to All."
            ),
        };
        write!(
            labels,
            r#"<span class="tab-label{dim}{active}" data-tab="{id}" onclick="switchTab(this)" title="{badge_title}">{name} <span class="tab-count">{count}</span></span>"#,
            count = mine.outcomes,
        )
        .ok();

        // This peer's points are capped per chart, so a peer that fills the
        // router's window does not make every refresh carry hundreds of dots
        // per tab and width.
        let my_times = downsample(&mine.response_times, MAX_PEER_POINTS);
        let my_speeds = downsample(&mine.transfer_speeds, MAX_PEER_POINTS);
        let times = if all_times.is_empty() && my_times.is_empty() {
            r#"<div class="empty-box">No timed replies yet.</div>"#.to_string()
        } else {
            responsive(|width| {
                distance_chart(
                    width,
                    &DistanceChart {
                        kind: Measure::Time,
                        all: all_times,
                        mine: &my_times,
                        line: &time_curve.distance_alone,
                        peer_line: &time_curve.this_peer,
                    },
                )
            })
        };
        let speeds = if my_speeds.is_empty() {
            r#"<div class="empty-box">No data transfers measured through this peer yet.</div>"#
                .to_string()
        } else {
            responsive(|width| {
                distance_chart(
                    width,
                    &DistanceChart {
                        kind: Measure::Speed,
                        all: all_speeds,
                        mine: &my_speeds,
                        line: &speed_curve.distance_alone,
                        peer_line: &speed_curve.this_peer,
                    },
                )
            })
        };
        let floored_note = if floored {
            r#"<p class="caption">Your node currently treats transfers through this peer as unusable and ranks it after every other eligible peer.</p>"#
        } else {
            ""
        };
        let panel_active = if index == 0 { " tab-panel-active" } else { "" };
        write!(
            panels,
            r#"<div class="tab-panel{panel_active}" id="panel-{id}">
                <div class="chart-title">Response time</div>{times}
                <div class="chart-title">Transfer speed</div>{floored_note}{speeds}
            </div>"#,
        )
        .ok();
    }

    let dots = "Each dot is a reply to a request your node sent to a peer.";
    let caption = if view.fallback {
        format!(
            "{dots} Routing is on its emergency fallback; the lines are what it predicts with that."
        )
    } else if time_curve.early {
        format!(
            "{dots} Your node has fewer than {MIN_TIMED_REPLIES} timed replies, so the lines are its early estimate."
        )
    } else {
        match learned.response_factor {
            Some(factor) if !about_same(factor) => format!(
                "{dots} This peer typically replies <b>{}</b> than distance alone predicts.",
                time_ratio(factor),
            ),
            Some(_) => {
                format!(
                    "{dots} This peer typically replies about as fast as distance alone predicts."
                )
            }
            None if view.peer.is_none() => dots.to_string(),
            None => format!(
                "{dots} Too few of its replies yet to say how it differs from what distance alone predicts."
            ),
        }
    };
    let legend = format!(
        r#"<div class="legend-row"><span><i class="sw-dot-peer"></i>this peer</span><span><i class="sw-dot-other"></i>other peers</span>{line}{peer_line}</div>"#,
        line = if time_curve.distance_alone.is_empty() {
            ""
        } else {
            r#"<span><i class="sw-line-net"></i>predicted from distance alone</span>"#
        },
        peer_line = if time_curve.this_peer.is_empty() {
            ""
        } else {
            r#"<span><i class="sw-line-peer"></i>predicted for this peer</span>"#
        },
    );
    format!(
        r#"<div class="card">
            <h2>Past requests by ring distance</h2>
            <p class="caption">{caption}</p>
            {legend}
            <div class="tab-group">
                <div class="tab-bar">{labels}</div>
                {panels}
            </div>
        </div>"#
    )
}

static EMPTY: Vec<(f64, f64)> = Vec::new();

#[derive(Clone, Copy, PartialEq, Eq)]
enum Measure {
    /// Seconds to the first reply. Higher is slower.
    Time,
    /// Bytes per second. Higher is faster.
    Speed,
}

impl Measure {
    fn format(self, value: f64) -> String {
        match self {
            Measure::Time => fmt_time(value),
            Measure::Speed => fmt_speed(value),
        }
    }
}

struct DistanceChart<'a> {
    kind: Measure,
    /// Every peer's recent observations, `(distance, value)`.
    all: &'a [(f64, f64)],
    /// This peer's.
    mine: &'a [(f64, f64)],
    /// What routing predicts for a peer with no record, `(distance, value)`.
    line: &'a [(f64, f64)],
    /// What routing predicts for this peer; empty when it is the same.
    peer_line: &'a [(f64, f64)],
}

/// Observations against ring distance on a log scale, with routing's
/// prediction for a peer with no record and for this peer.
///
/// At phone width the lines carry no end labels (the legend names them, and a
/// stack of wrapped labels did not fit) and the dots carry no tooltips,
/// which a touch screen cannot show anyway.
fn distance_chart(width: f64, chart: &DistanceChart<'_>) -> String {
    let narrow = width < 450.0;
    let (h, left, right, top, bottom) =
        (230.0, 50.0, if narrow { 12.0 } else { 100.0 }, 10.0, 34.0);
    let values = chart
        .all
        .iter()
        .chain(chart.mine)
        .chain(chart.line)
        .chain(chart.peer_line)
        .map(|&(_, v)| v);
    let axis = LogAxis::covering(values);
    let plot_w = width - left - right;
    let x = |d: f64| left + (d.clamp(0.0, 0.5) / 0.5) * plot_w;
    let y = |v: f64| top + (1.0 - axis.fraction(v)) * (h - top - bottom);
    let mut svg = format!(
        r#"<svg viewBox="0 0 {width} {h}" class="mchart" role="img" aria-label="{} against ring distance">"#,
        match chart.kind {
            Measure::Time => "Response time",
            Measure::Speed => "Transfer speed",
        }
    );
    for &value in &axis.ticks {
        write!(
            svg,
            r#"<line x1="{left}" x2="{:.1}" y1="{yy:.1}" y2="{yy:.1}" class="grid"/><text x="{:.1}" y="{:.1}" text-anchor="end">{}</text>"#,
            width - right,
            left - 6.0,
            y(value) + 3.0,
            chart.kind.format(value),
            yy = y(value),
        )
        .ok();
    }
    write!(
        svg,
        r#"<line x1="{left}" x2="{:.1}" y1="{yy:.1}" y2="{yy:.1}" class="ax"/>"#,
        width - right,
        yy = h - bottom
    )
    .ok();
    for tick in 0..=5 {
        let d = tick as f64 / 10.0;
        write!(
            svg,
            r#"<text x="{:.1}" y="{:.1}" text-anchor="middle">{d:.1}</text>"#,
            x(d),
            h - bottom + 14.0
        )
        .ok();
    }
    write!(
        svg,
        r#"<text x="{:.1}" y="{:.1}" text-anchor="middle">{}</text>"#,
        left + plot_w / 2.0,
        h - 4.0,
        if narrow {
            "ring distance, peer to contract"
        } else {
            "ring distance between peer and contract (0 = same spot, 0.5 = opposite side)"
        }
    )
    .ok();
    svg.push_str(&faint_dots(
        chart
            .all
            .iter()
            .filter(|(d, v)| d.is_finite() && v.is_finite() && *v > 0.0)
            .map(|&(d, v)| (x(d), y(v))),
    ));
    let path = |points: &[(f64, f64)]| -> String {
        points
            .iter()
            .filter(|(d, v)| d.is_finite() && v.is_finite() && *v > 0.0)
            .enumerate()
            .map(|(i, &(d, v))| {
                format!("{}{:.1},{:.1}", if i == 0 { "M" } else { " L" }, x(d), y(v))
            })
            .collect()
    };
    let mut end_labels = Vec::new();
    if let Some(&(_, end)) = chart.line.last() {
        write!(svg, r#"<path d="{}" class="line-net"/>"#, path(chart.line)).ok();
        end_labels.push((y(end), "distance alone"));
    }
    if let Some(&(_, end)) = chart.peer_line.last() {
        write!(
            svg,
            r#"<path d="{}" class="line-peer"/>"#,
            path(chart.peer_line)
        )
        .ok();
        end_labels.push((y(end), "this peer"));
    }
    if !narrow {
        // Keep the two end labels at least one line apart.
        end_labels.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut next_free = f64::NEG_INFINITY;
        for (label_y, text) in &end_labels {
            let at = label_y.max(next_free);
            write!(
                svg,
                r#"<text x="{:.1}" y="{:.1}" class="dl">{text}</text>"#,
                width - right + 6.0,
                at + 4.0
            )
            .ok();
            next_free = at + 13.0;
        }
    }
    for &(d, v) in chart.mine {
        if !(d.is_finite() && v.is_finite() && v > 0.0) {
            continue;
        }
        let title = if narrow {
            String::new()
        } else {
            let predicted = interpolate(chart.line, d)
                .map(|p| format!("; distance alone {}", chart.kind.format(p)))
                .unwrap_or_default();
            format!(
                "<title>{} at {d:.2}{predicted}</title>",
                chart.kind.format(v)
            )
        };
        write!(
            svg,
            r#"<circle cx="{:.1}" cy="{:.1}" r="4.5" class="dot-this">{title}</circle>"#,
            x(d),
            y(v),
        )
        .ok();
    }
    svg.push_str("</svg>");
    svg
}

// ─── How good are the predictions? ──────────────────────────────────────────

fn accuracy_card(view: &RouterView) -> String {
    let all = &view.snapshot.hierarchical_response_time_pairs;
    let mine = view
        .peer
        .as_ref()
        .map_or(&EMPTY, |peer| &peer.response_time_pairs);
    let across =
        typical_miss(all).map(|miss| format!("across all peers, within <b>&times;{miss:.1}</b>"));
    let verdict = match (
        typical_miss(mine).filter(|_| mine.len() >= MIN_PAIRS_TO_JUDGE),
        across,
    ) {
        (Some(miss), Some(across)) => format!(
            "For this peer, replies typically land within <b>&times;{miss:.1}</b> of the prediction (faster or slower); {across}."
        ),
        (None, Some(across)) => {
            format!("Too few requests through this peer to judge its predictions yet; {across}.")
        }
        _ => "Too few requests to judge the predictions yet.".to_string(),
    };
    let fallback = if view.fallback {
        "<p>Routing is on its emergency fallback: these are the main model's predictions, which it is not using right now.</p>"
    } else {
        ""
    };
    let chart = if all.is_empty() && mine.is_empty() {
        r#"<div class="empty-box">No predictions compared with replies yet.</div>"#.to_string()
    } else {
        accuracy_chart(all, &downsample(mine, MAX_PEER_POINTS))
    };
    format!(
        r#"<div class="card">
            <h2>How good are the predictions?</h2>
            <div class="acc-row">
                <div>{chart}</div>
                <div class="acc-facts">
                    {fallback}
                    <p>Before each request your node predicts how long each eligible peer will take to reply, and sends it to the best one. Each dot compares a prediction with what actually happened.</p>
                    <p>{verdict}</p>
                </div>
            </div>
        </div>
        <p class="more"><a href="/routing">{ROUTING_LINK} &rarr;</a></p>"#
    )
}

/// The median of `max(predicted / actual, actual / predicted)`: the factor
/// within which half the replies landed. `None` without a usable pair.
fn typical_miss(pairs: &[(f64, f64)]) -> Option<f64> {
    let mut misses: Vec<f64> = pairs
        .iter()
        .filter(|(p, a)| p.is_finite() && a.is_finite() && *p > 0.0 && *a > 0.0)
        .map(|(p, a)| (p / a).max(a / p))
        .collect();
    if misses.is_empty() {
        return None;
    }
    misses.sort_by(f64::total_cmp);
    Some(misses[misses.len() / 2])
}

fn accuracy_chart(all: &[(f64, f64)], mine: &[(f64, f64)]) -> String {
    let (w, h, left, right, top, bottom) = (300.0, 270.0, 58.0, 10.0, 10.0, 34.0);
    let axis = LogAxis::covering(all.iter().chain(mine).flat_map(|&(p, a)| [p, a]));
    let x = |v: f64| left + axis.fraction(v) * (w - left - right);
    let y = |v: f64| top + (1.0 - axis.fraction(v)) * (h - top - bottom);
    let mut svg = format!(
        r#"<svg viewBox="0 0 {w} {h}" class="mchart acc-chart" role="img" aria-label="Predicted against actual response time">"#
    );
    for &v in &axis.ticks {
        write!(
            svg,
            r#"<line x1="{left}" x2="{:.1}" y1="{yy:.1}" y2="{yy:.1}" class="grid"/><line x1="{xx:.1}" x2="{xx:.1}" y1="{top}" y2="{:.1}" class="grid"/><text x="{:.1}" y="{:.1}" text-anchor="end">{t}</text><text x="{xx:.1}" y="{:.1}" text-anchor="middle">{t}</text>"#,
            w - right,
            h - bottom,
            left - 6.0,
            y(v) + 3.0,
            h - bottom + 14.0,
            yy = y(v),
            xx = x(v),
            t = fmt_time(v),
        )
        .ok();
    }
    let (min, max) = (10f64.powf(axis.lo), 10f64.powf(axis.hi));
    write!(
        svg,
        r#"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" class="baseline"/>
        <text x="{:.1}" y="{:.1}">slower than predicted</text>
        <text x="{:.1}" y="{:.1}" text-anchor="end">faster than predicted</text>
        <text x="{:.1}" y="{:.1}" text-anchor="middle">predicted</text>
        <text x="12" y="{mid:.1}" text-anchor="middle" transform="rotate(-90 12 {mid:.1})">actual</text>"#,
        x(min),
        y(min),
        x(max),
        y(max),
        left + 4.0,
        top + 10.0,
        w - right - 4.0,
        h - bottom - 6.0,
        left + (w - left - right) / 2.0,
        h - 4.0,
        mid = (top + h - bottom) / 2.0,
    )
    .ok();
    svg.push_str(&faint_dots(
        all.iter()
            .filter(|(p, a)| p.is_finite() && a.is_finite() && *p > 0.0 && *a > 0.0)
            .map(|&(p, a)| (x(p), y(a))),
    ));
    for &(p, a) in mine {
        if p > 0.0 && a > 0.0 && p.is_finite() && a.is_finite() {
            write!(
                svg,
                r#"<circle cx="{:.1}" cy="{:.1}" r="4.5" class="dot-this"><title>this peer &middot; predicted {}, took {}</title></circle>"#,
                x(p),
                y(a),
                fmt_time(p),
                fmt_time(a)
            )
            .ok();
        }
    }
    // Last, so this peer's dots cannot hide it; the halo keeps it legible
    // over them.
    write!(
        svg,
        r#"<text x="{:.1}" y="{:.1}" text-anchor="end" class="dl halo">exact</text>"#,
        x(max) - 18.0,
        y(max) + 14.0,
    )
    .ok();
    svg.push_str("</svg>");
    svg
}

// ─── Header ─────────────────────────────────────────────────────────────────

fn header_card(inputs: &PeerPageInputs<'_>) -> String {
    let peer = inputs.peer;
    let addr = html_escape(&peer.address.to_string());
    let location = peer
        .location
        .map_or_else(|| "&mdash;".to_string(), |l| format!("{l:.4}"));
    let gateway = if peer.is_gateway {
        "<span><b>Gateway</b></span>"
    } else {
        ""
    };
    format!(
        r#"<div class="card">
            <div class="peer-head">
                <div class="who">
                    <div class="addr"><code>{addr}</code><button type="button" class="copy-btn-inline" data-addr="{addr}" onclick="copyToClipboard(this.getAttribute('data-addr')).then(function(){{showToast('Address copied')}})" title="Copy address">&#x2398;</button></div>
                    <div class="facts">
                        <span title="This peer's position on the ring of all peers (0 to 1). Your node sends requests to peers near the contract they concern.">Location <b>{location}</b></span>
                        <span>Connected <b>{connected}</b></span>
                        <span>Sent <b>{sent}</b></span>
                        <span>Received <b>{received}</b></span>
                        {gateway}
                    </div>
                </div>
                {ring}
            </div>
        </div>"#,
        connected = format_duration(peer.connected_secs),
        sent = format_bytes(peer.bytes_sent),
        received = format_bytes(peer.bytes_received),
        ring = mini_ring(inputs.own_location, peer.location),
    )
}

/// The ring with this node and this peer on it.
fn mini_ring(own: Option<f64>, peer: Option<f64>) -> String {
    let Some(peer) = peer else {
        return String::new();
    };
    let (cx, cy, r) = (120.0, 50.0, 28.0);
    let at = |location: f64| {
        let angle = location * std::f64::consts::TAU;
        (cx + r * angle.sin(), cy - r * angle.cos())
    };
    // Labels sit outside the ring, on the side the dot is on.
    let label = |(x, y): (f64, f64), text: &str, dy: f64| {
        let right = x >= cx;
        format!(
            r#"<text x="{:.1}" y="{:.1}" text-anchor="{}">{text}</text>"#,
            if right { x + 9.0 } else { x - 9.0 },
            y + dy,
            if right { "start" } else { "end" }
        )
    };
    let mut svg = format!(
        r#"<svg viewBox="0 0 240 100" class="mini-ring" role="img" aria-label="Ring position of you and this peer"><circle cx="{cx}" cy="{cy}" r="{r}" class="ring"/>"#
    );
    let peer_at = at(peer);
    if let Some(own) = own {
        let own_at = at(own);
        // Nudge the labels apart when the two dots sit close together.
        let close = (own_at.1 - peer_at.1).abs() < 16.0 && (own_at.0 >= cx) == (peer_at.0 >= cx);
        let own_dy = if close && own_at.1 <= peer_at.1 {
            -5.0
        } else {
            4.0
        };
        let peer_dy = if close && own_at.1 > peer_at.1 {
            -5.0
        } else if close {
            16.0
        } else {
            4.0
        };
        write!(
            svg,
            r#"<circle cx="{:.1}" cy="{:.1}" r="4" class="ring-you"/>{}"#,
            own_at.0,
            own_at.1,
            label(own_at, "you", own_dy)
        )
        .ok();
        svg.push_str(&label(peer_at, "this peer", peer_dy));
    } else {
        svg.push_str(&label(peer_at, "this peer", 4.0));
    }
    write!(
        svg,
        r#"<circle cx="{:.1}" cy="{:.1}" r="5" class="dot-this"/></svg>"#,
        peer_at.0, peer_at.1
    )
    .ok();
    svg
}

// ─── Formatting ─────────────────────────────────────────────────────────────

fn about_same(factor: f64) -> bool {
    (1.0 / SAME_FACTOR..=SAME_FACTOR).contains(&factor)
}

/// `1.4× faster` / `2.6× slower` for a response-time factor.
fn time_ratio(factor: f64) -> String {
    if factor < 1.0 {
        format!("{}&times; faster", fmt_factor(1.0 / factor))
    } else {
        format!("{}&times; slower", fmt_factor(factor))
    }
}

fn fmt_factor(factor: f64) -> String {
    if factor >= 10.0 {
        format!("{factor:.0}")
    } else {
        format!("{factor:.1}")
    }
}

/// "1.4× faster than distance alone predicts", or "about as fast as distance
/// alone predicts". `emphasis` wraps the ratio for the tile.
fn time_comparison(factor: f64, emphasis: bool) -> String {
    if about_same(factor) {
        return "about as fast as distance alone predicts".to_string();
    }
    let ratio = time_ratio(factor);
    if emphasis {
        format!(r#"<span class="cmp">{ratio}</span> than distance alone predicts"#)
    } else {
        format!("{ratio} than distance alone predicts")
    }
}

/// "+1.2 points more likely to fail than distance alone predicts".
fn failure_comparison(offset: f64) -> String {
    let points = offset * 100.0;
    if points.abs() < 0.05 {
        "as likely to fail as distance alone predicts".to_string()
    } else if points > 0.0 {
        format!("+{points:.1} points more likely to fail than distance alone predicts")
    } else {
        format!(
            "&minus;{:.1} points less likely to fail than distance alone predicts",
            -points
        )
    }
}

/// A duration for a tile: the number, then its unit in small type.
fn fmt_time_tile(seconds: f64) -> String {
    if seconds < 1.0 {
        format!("{:.0} <small>ms</small>", seconds * 1e3)
    } else if seconds < 10.0 {
        format!("{seconds:.1} <small>s</small>")
    } else {
        format!("{seconds:.0} <small>s</small>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::network_status::OpType;
    use crate::ring::{Location, PeerKeyLocation};
    use crate::router::{PeerSelection, RouteEvent, RouteOutcome};
    use std::time::Duration;

    /// Twelve peers, fifty rounds each (600 events, so the router's 500-event
    /// windows have rolled), through `add_event` on a frozen clock:
    /// `peers[0]` replies four times slower and fails one request in three,
    /// `peers[3]` never transfers a payload, the rest are ordinary. One round
    /// in four is a PUT.
    fn trained_router() -> (Router, Vec<PeerKeyLocation>) {
        let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
            crate::util::time_source::SharedMockTimeSource::new(),
        ));
        let peers: Vec<PeerKeyLocation> = (0..12).map(|_| PeerKeyLocation::random()).collect();
        for round in 0..50u64 {
            for (index, peer) in peers.iter().enumerate() {
                let jitter_ms = 100 + (index as u64 * 7 + round * 13) % 50;
                let failed = (index == 0 && round % 3 == 0) || (round + index as u64) % 37 == 0;
                let outcome = if failed {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_millis(if index == 0 {
                            jitter_ms * 4
                        } else {
                            jitter_ms
                        }),
                        payload_size: if index == 3 { 0 } else { 20_000 },
                        payload_transfer_time: if index == 3 {
                            Duration::ZERO
                        } else {
                            Duration::from_millis(40 + jitter_ms / 4)
                        },
                    }
                };
                router.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: Location::random(),
                    outcome,
                    op_type: Some(if round % 4 == 0 {
                        OpType::Put
                    } else {
                        OpType::Get
                    }),
                });
            }
        }
        (router, peers)
    }

    fn snapshot_of(key: &PeerKeyLocation) -> PeerSnapshot {
        PeerSnapshot {
            address: key.socket_addr().expect("random peers have an address"),
            is_gateway: false,
            location: key.location().map(|l| l.as_f64()),
            connected_secs: 457,
            peer_key_location: Some(key.clone()),
            bytes_sent: 376_700,
            bytes_received: 348_500,
        }
    }

    fn snapshots(peers: &[PeerKeyLocation]) -> Vec<PeerSnapshot> {
        peers.iter().map(snapshot_of).collect()
    }

    fn inputs<'a>(snaps: &'a [PeerSnapshot], index: usize) -> PeerPageInputs<'a> {
        PeerPageInputs {
            peer: &snaps[index],
            peers: snaps,
            own_location: Some(0.118),
            version: "0.2.141",
        }
    }

    fn render(router: &Router, snaps: &[PeerSnapshot], index: usize) -> String {
        let inputs = inputs(snaps, index);
        let view = RouterView::gather(&inputs, router);
        render_peer_page(&inputs, Some(&view))
    }

    /// The page body as a reader sees it: the embedded stylesheet and script
    /// are not page text.
    fn visible(html: &str) -> String {
        let mut text = html.to_string();
        for (open, close) in [("<style>", "</style>"), ("<script>", "</script>")] {
            while let Some(start) = text.find(open) {
                let end = text[start..]
                    .find(close)
                    .map_or(text.len(), |e| start + e + close.len());
                text.replace_range(start..end, "");
            }
        }
        text
    }

    /// A tab's badge, as rendered.
    fn tab_count(html: &str, tab: &str) -> usize {
        let start = html.find(&format!(r#"data-tab="{tab}""#)).unwrap();
        let badge = &html[start..];
        let open = badge.find(r#"<span class="tab-count">"#).unwrap() + 24;
        let close = open + badge[open..].find('<').unwrap();
        badge[open..close].parse().unwrap()
    }

    /// The reliability tile's `(succeeded, of)`, as rendered; `None` for "—".
    fn reliability(html: &str) -> Option<(usize, usize)> {
        let start = html
            .find(r#"<div class="k">Reliability</div><div class="v">"#)
            .unwrap()
            + 47;
        let value = &html[start..start + html[start..].find("</div>").unwrap()];
        let (succeeded, rest) = value.split_once(" <small>of ")?;
        Some((
            succeeded.parse().unwrap(),
            rest.trim_end_matches("</small>").parse().unwrap(),
        ))
    }

    const FROM_DISTANCE: &str =
        "Until it has handled more requests, your node predicts its results from distance alone.";
    const STILL_LEARNING: &str = "Still learning about this peer.";
    const AVOIDED: &str = "recently ranks it first far less often than its share";
    const FALLBACK: &str = "Routing is on its emergency fallback";

    #[test]
    fn a_typical_peer_shows_every_section_with_no_banner() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let html = render(&router, &snaps, 5);
        for needle in [
            "What your node has learned",
            "requests to it succeeded, of your node's last 500 requests (relayed ones included).",
            "Across all your peers, about 1 in",
            "predicted for contracts near it; it",
            "distance alone predicts",
            "Compared with your other peers",
            "Each dot is one of your 12 peers",
            "&larr; fewer failures",
            "more failures &rarr;",
            "Past requests by ring distance",
            "This peer typically replies",
            "ring distance between peer and contract (0 = same spot, 0.5 = opposite side)",
            r#"class="line-net""#,
            "How good are the predictions?",
            "replies typically land within",
            r#"href="/routing""#,
            "How good are the predictions across all peers?",
        ] {
            assert!(html.contains(needle), "missing {needle:?}");
        }
        assert!(!html.contains(STILL_LEARNING) && !html.contains(AVOIDED));
        assert!(
            !html.contains("replies counted"),
            "no effective sample size as a count"
        );
        assert!(
            html.matches(r#"class="w-wide""#).count()
                == html.matches(r#"class="w-narrow""#).count(),
            "every chart with axis text has a phone variant"
        );
    }

    /// The reliability tile and the All tab count one population, the
    /// router's window: parsed from the page itself for every peer, so a tile
    /// that read any other source would disagree here.
    #[test]
    fn the_reliability_tile_and_the_all_tab_count_the_same_requests() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let mut with_failures = 0;
        for index in 0..peers.len() {
            let html = render(&router, &snaps, index);
            let (succeeded, of) = reliability(&html).expect("every trained peer has requests");
            assert_eq!(of, tab_count(&html, "all"), "peer {index}");
            if succeeded < of {
                with_failures += 1;
            }
        }
        assert!(with_failures >= 2, "the failures reach the tile");
    }

    /// Each operation keeps its own window. After 600 events the All window
    /// holds the last 500 while the GET and PUT windows still hold every one
    /// of their own, so the operation tabs add up to more than All.
    #[test]
    fn operation_tabs_count_their_own_windows() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let html = render(&router, &snaps, 5);
        let (all, get, put) = (
            tab_count(&html, "all"),
            tab_count(&html, "get"),
            tab_count(&html, "put"),
        );
        assert_eq!((get, put), (37, 13), "every GET and PUT the peer had");
        assert!(
            all < get + put,
            "the All window has rolled: {all} vs {get} + {put}"
        );
        assert!(
            html.contains(
                "Each operation keeps its own window, so the tabs need not add up to All."
            )
        );
    }

    #[test]
    fn a_peer_the_router_has_never_seen_is_still_being_learned() {
        let (router, peers) = trained_router();
        let mut snaps = snapshots(&peers);
        snaps.push(snapshot_of(&PeerKeyLocation::random()));
        let html = render(&router, &snaps, 12);
        assert!(
            html.contains(FROM_DISTANCE),
            "no record at all: from distance alone"
        );
        assert_eq!(reliability(&html), None);
        assert!(html.contains("No requests to it of your node's last 500 requests"));
        assert_eq!(tab_count(&html, "all"), 0);
        assert!(
            html.contains("too few replies yet to compare it with distance alone."),
            "routing predicts it from distance alone, and the tile says it cannot compare yet"
        );
        assert!(html.contains("Not among the closest candidates for a recent routing decision."));
        assert!(html.contains("Too few requests through this peer to judge its predictions yet"));
        assert!(html.contains("Each dot is one of your 13 peers"));
        assert!(
            html.contains(r#"class="dot-this hollow""#),
            "with no record it is drawn hollow on the line"
        );
    }

    /// A connected peer with no key location cannot be looked up in the
    /// router at all: it gets a neutral state, not a permanent "still
    /// learning", and the comparison says how many peers it covers.
    #[test]
    fn a_peer_without_a_key_location_is_neutral_not_still_learning() {
        let (router, peers) = trained_router();
        let mut snaps = snapshots(&peers);
        let mut keyless = snapshot_of(&PeerKeyLocation::random());
        keyless.peer_key_location = None;
        snaps.push(keyless);
        let html = render(&router, &snaps, 12);
        assert!(!html.contains(STILL_LEARNING));
        assert!(html.contains("Your node has no routing record it can match to this peer."));
        assert!(html.contains("Each dot is one of 12 of your 13 peers (the other 1 cannot be matched to a routing record)"));
    }

    #[test]
    fn a_slow_failing_peer_that_routing_passes_over_reads_as_avoided() {
        let _seed = crate::config::GlobalRng::seed_guard(0x5794_a001);
        let (router, peers) = trained_router();
        for round in 0..40 {
            let window: Vec<&PeerKeyLocation> = [0, 1 + round % 11, 1 + (round + 3) % 11]
                .iter()
                .map(|&i| &peers[i])
                .collect();
            router.select_k_best_peers_capturing(
                window.into_iter(),
                Location::random(),
                1,
                false,
                true,
            );
        }
        let selection = router.peer_snapshot(&peers[0]).selection.unwrap();
        assert_eq!((selection.eligible, selection.chosen), (40, 0));
        let snaps = snapshots(&peers);
        let html = render(&router, &snaps, 0);
        assert!(
            html.contains("Your node has learned this peer is slower and less reliable than distance alone predicts, and recently ranks it first far less often than its share."),
            "the banner names both conditions that fired"
        );
        assert!(!html.contains(STILL_LEARNING));
        assert!(html.contains("0 <small>of 40</small>"));
        assert!(html.contains("recent routing decisions it was eligible for"));
    }

    #[test]
    fn a_peer_without_transfers_shows_the_empty_transfer_state() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let all_panel = |html: &str| -> String {
            let start = html.find(r#"id="panel-all""#).unwrap();
            let end = html.find(r#"id="panel-get""#).unwrap();
            html[start..end].to_string()
        };
        let without = render(&router, &snaps, 3);
        assert!(
            all_panel(&without).contains("No data transfers measured through this peer yet."),
            "a peer with no transfers gets the empty state"
        );
        let with = render(&router, &snaps, 2);
        assert!(
            !all_panel(&with).contains("No data transfers measured through this peer yet."),
            "a peer with transfers gets a chart"
        );
        assert!(all_panel(&with).contains("Transfer speed against ring distance"));
    }

    #[test]
    fn a_floored_transfer_speed_reads_as_unusable() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let inputs = inputs(&snaps, 4);
        let mut view = RouterView::gather(&inputs, &router);
        let note = "Your node currently treats transfers through this peer as unusable";
        assert!(!render_peer_page(&inputs, Some(&view)).contains(note));
        let prediction = view
            .peer
            .as_mut()
            .and_then(|peer| peer.prediction_at_own_location.as_mut())
            .expect("a trained router predicts at the peer's location");
        prediction.transfer_speed_bps = crate::router::DEGENERATE_SPEED_FLOOR_BPS;
        assert!(render_peer_page(&inputs, Some(&view)).contains(note));
    }

    /// On the emergency fallback routing reads none of the comparisons with
    /// distance alone: the page says so on the banner, under the chart and
    /// beside the accuracy chart, and claims nothing about learning.
    #[test]
    fn the_fallback_is_named_wherever_the_page_compares() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let inputs = inputs(&snaps, 0);
        let mut view = RouterView::gather(&inputs, &router);
        let normal = render_peer_page(&inputs, Some(&view));
        assert!(!normal.contains(FALLBACK));
        view.fallback = true;
        let fallback = render_peer_page(&inputs, Some(&view));
        for needle in [
            "Routing is on its emergency fallback right now, so it does not use the comparisons with distance alone on this page.",
            "Routing is on its emergency fallback; the lines are what it predicts with that.",
            "these are the main model's predictions, which it is not using right now.",
            "predicted for contracts near it.</div>",
        ] {
            assert!(fallback.contains(needle), "missing {needle:?}");
        }
        for absent in [AVOIDED, STILL_LEARNING, "typically replies"] {
            assert!(!fallback.contains(absent), "fallback page shows {absent:?}");
        }
    }

    /// One term per concept, and none of the model's internals: a reader of
    /// this page should never meet the estimator's vocabulary.
    #[test]
    fn the_peer_page_uses_none_of_the_models_internal_terms() {
        let (router, peers) = trained_router();
        let mut snaps = snapshots(&peers);
        snaps.push(snapshot_of(&PeerKeyLocation::random()));
        let pages = [
            render(&router, &snaps, 0),
            render(&router, &snaps, 5),
            render(&router, &snaps, 12),
        ];
        for html in &pages {
            let text = visible(html).to_lowercase();
            for banned in [
                "typical peer",
                "renegade",
                "isotonic",
                "hierarchical",
                "κ",
                "&kappa;",
                "brier",
                "skill",
                "ewma",
                "residual",
                "estimator",
                "expected total time",
            ] {
                assert!(!text.contains(banned), "the peer page says {banned:?}");
            }
        }
    }

    /// A peer that fills the router's window must not make every refresh
    /// carry its whole history: each chart caps this peer's dots, and the
    /// phone copies carry no tooltips.
    #[test]
    fn a_peer_filling_the_window_keeps_the_page_small() {
        let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
            crate::util::time_source::SharedMockTimeSource::new(),
        ));
        let busy = PeerKeyLocation::random();
        for round in 0..600u64 {
            router.add_event(RouteEvent {
                peer: busy.clone(),
                contract_location: Location::random(),
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(80 + round % 90),
                    payload_size: 20_000,
                    payload_transfer_time: Duration::from_millis(40 + round % 30),
                },
                op_type: Some(OpType::Get),
            });
        }
        let snaps = vec![snapshot_of(&busy)];
        let html = render(&router, &snaps, 0);
        assert_eq!(
            tab_count(&html, "all"),
            500,
            "the busy peer fills the window"
        );
        let this_dots = html.matches(r#"class="dot-this""#).count();
        // 2 tabs with data (All, GET) x 2 charts x 2 widths x <= 150, plus
        // the accuracy chart's <= 150 and the strips' and ring's few.
        assert!(
            this_dots <= 2 * 2 * 2 * MAX_PEER_POINTS + MAX_PEER_POINTS + 10,
            "{this_dots}"
        );
        let narrow_titles: usize = html
            .split(r#"<div class="w-narrow">"#)
            .skip(1)
            .map(|rest| {
                rest[..rest.find("</svg>").unwrap()]
                    .matches("<title>")
                    .count()
            })
            .sum();
        assert_eq!(narrow_titles, 0, "phone copies carry no tooltips");
        assert!(html.len() < 300_000, "page is {} bytes", html.len());
    }

    #[test]
    fn without_a_router_the_page_says_so() {
        let snaps = [snapshot_of(&PeerKeyLocation::random())];
        let html = render_peer_page(&inputs(&snaps, 0), None);
        assert!(html.contains("Routing data is not available yet."));
        assert!(html.contains("Location <b>"), "the header still renders");
    }

    // ── The readings behind the banners, tiles and strips ──────────────────

    /// A failure-stage reading.
    fn failure(offset: f64, evidence: f64, weight: f64) -> Option<PeerOffset> {
        Some(PeerOffset {
            offset,
            evidence,
            weight,
        })
    }

    /// A timing-stage reading whose typical factor is `factor`.
    fn timing(factor: f64, evidence: f64, weight: f64) -> Option<PeerOffset> {
        failure(factor.ln(), evidence, weight)
    }

    fn view_with(
        mine: [Option<PeerOffset>; 3],
        others: Vec<[Option<PeerOffset>; 3]>,
        selection: Option<PeerSelection>,
    ) -> RouterView {
        let router = Router::new(&[]);
        let mut view = RouterView {
            snapshot: router.snapshot(),
            peer: Some(router.peer_snapshot(&PeerKeyLocation::random())),
            others: others
                .into_iter()
                .enumerate()
                .map(|(i, offsets)| (format!("10.0.0.{i}:1"), offsets))
                .collect(),
            window_outcomes: (0, 0),
            fallback: false,
        };
        let peer = view.peer.as_mut().unwrap();
        peer.offsets = mine;
        peer.selection = selection;
        view.others.push(("this".to_string(), mine));
        view
    }

    #[test]
    fn still_learning_says_how_far_the_node_relies_on_distance_alone() {
        let known_other = [failure(0.02, 30.0, 0.7), timing(1.3, 30.0, 0.7), None];
        // Nothing of this peer's record adopted on any warm stage.
        let none = view_with(
            [failure(0.0, 1.0, 0.0), timing(1.0, 0.0, 0.0), None],
            vec![known_other],
            None,
        );
        assert_eq!(Learned::of(&none).learning, Learning::FromDistanceAlone);
        // Part of it adopted on one stage: "mostly".
        let part = view_with(
            [failure(0.0, 1.0, 0.2), timing(1.0, 1.0, 0.0), None],
            vec![known_other],
            None,
        );
        assert_eq!(Learned::of(&part).learning, Learning::Mostly);
        // Exactly half adopted counts as known.
        let known = view_with(
            [failure(0.0, 9.0, 0.5), None, None],
            vec![known_other],
            None,
        );
        assert_eq!(Learned::of(&known).learning, Learning::No);
        // Warm stages that tell nobody apart: every peer is on the line because
        // they look alike, not because this one is new. No banner.
        let alike = [failure(0.0, 40.0, 0.0), timing(1.0, 40.0, 0.0), None];
        assert_eq!(
            Learned::of(&view_with(alike, vec![alike, alike], None)).learning,
            Learning::No
        );
        // Nothing warm yet: the node predicts every peer from distance alone.
        let cold = view_with([None, None, None], vec![[None, None, None]], None);
        assert_eq!(Learned::of(&cold).learning, Learning::FromDistanceAlone);
        // No key location, or the fallback: no banner.
        let mut keyless = view_with([None, None, None], vec![known_other], None);
        keyless.peer = None;
        assert_eq!(Learned::of(&keyless).learning, Learning::No);
        let mut fallback = view_with(
            [failure(0.0, 1.0, 0.2), None, None],
            vec![known_other],
            None,
        );
        fallback.fallback = true;
        assert_eq!(Learned::of(&fallback).learning, Learning::No);
    }

    /// The stated factor is the learned typical difference, exp(offset): a
    /// peer the node knows well whose replies match distance alone reads as
    /// "about as fast", not faster for having narrowed the uncertainty.
    #[test]
    fn a_well_known_peer_with_no_difference_reads_as_about_the_same() {
        let others = vec![[None, timing(1.3, 30.0, 0.7), None]];
        let learned = Learned::of(&view_with(
            [None, timing(1.0, 40.0, 0.9), None],
            others,
            None,
        ));
        assert_eq!(learned.response_factor, Some(1.0));
        assert!(
            time_comparison(learned.response_factor.unwrap(), false).starts_with("about as fast")
        );
        // Too little adopted: no factor at all, so nothing to contradict.
        let thin = Learned::of(&view_with(
            [None, timing(1.6, 4.0, 0.49), None],
            vec![[None, timing(1.3, 30.0, 0.7), None]],
            None,
        ));
        assert_eq!(thin.response_factor, None);
    }

    #[test]
    fn the_avoided_reading_needs_evidence_a_low_share_and_a_worse_record() {
        let others = vec![[failure(0.0, 30.0, 0.7), timing(1.0, 30.0, 0.7), None]];
        let selection = |eligible, chosen, even_share| {
            Some(PeerSelection {
                eligible,
                chosen,
                even_share,
            })
        };
        let less_reliable = [failure(0.01, 30.0, 0.6), timing(1.0, 30.0, 0.6), None];
        let avoided = |mine, sel| Learned::of(&view_with(mine, others.clone(), sel)).avoided;
        assert_eq!(
            avoided(less_reliable, selection(20, 1, 4.0)),
            Some((false, true)),
            "all three conditions hold, and it says which"
        );
        assert_eq!(
            avoided(less_reliable, selection(19, 0, 4.0)),
            None,
            "too few decisions"
        );
        assert_eq!(
            avoided(less_reliable, selection(20, 2, 4.0)),
            None,
            "chosen at half an even split is not avoided"
        );
        let not_worse = [failure(0.0099, 30.0, 0.6), timing(1.24, 30.0, 0.6), None];
        assert_eq!(
            avoided(not_worse, selection(40, 0, 8.0)),
            None,
            "no worse than distance alone"
        );
        let slower = [failure(0.0, 30.0, 0.6), timing(1.26, 30.0, 0.6), None];
        assert_eq!(
            avoided(slower, selection(40, 0, 8.0)),
            Some((true, false)),
            "slower"
        );
        let unsure = [failure(0.05, 3.0, 0.4), timing(2.0, 3.0, 0.4), None];
        assert_eq!(
            avoided(unsure, selection(40, 0, 8.0)),
            None,
            "a worse record needs half its weight adopted"
        );
    }

    /// The strip and the tile apply one gate: a reading too thin for the tile
    /// to state is drawn hollow, on the line, with a title saying so.
    #[test]
    fn a_thin_reading_is_drawn_hollow_on_the_line() {
        let snaps = [
            snapshot_of(&PeerKeyLocation::random()),
            snapshot_of(&PeerKeyLocation::random()),
        ];
        let inputs = inputs(&snaps, 1);
        let mut view = view_with(
            [failure(0.03, 4.0, 0.3), timing(2.5, 4.0, 0.3), None],
            vec![[failure(0.02, 30.0, 0.7), timing(1.3, 30.0, 0.7), None]],
            None,
        );
        view.others[0].0 = snaps[0].address.to_string();
        view.others.last_mut().unwrap().0 = snaps[1].address.to_string();
        let html = compare_card(&inputs, &view);
        assert!(html.contains("this peer &middot; too few requests to judge yet"));
        assert!(html.contains("this peer &middot; too few replies to judge yet"));
        assert!(
            !html.contains("2.5&times; slower"),
            "the thin factor is not stated"
        );
        assert!(html.contains(r#"class="dot-this hollow""#));
        assert!(html.contains("&larr; fewer failures") && html.contains("more failures &rarr;"));
        let known = Learned::of(&view);
        assert_eq!(
            known.response_factor, None,
            "and the tile states no factor either"
        );
    }

    #[test]
    fn typical_miss_is_the_median_factor_either_way() {
        assert_eq!(typical_miss(&[]), None);
        let pairs = [(1.0, 2.0), (2.0, 1.0), (1.0, 1.0), (1.0, 4.0), (1.0, 1.5)];
        assert_eq!(typical_miss(&pairs), Some(2.0));
        assert_eq!(typical_miss(&[(0.0, 1.0), (1.0, f64::NAN)]), None);
    }

    #[test]
    fn strip_axes_cover_every_peer() {
        let points = StripAxis::points([-0.2, 0.5].into_iter());
        assert_eq!((points.lo, points.hi), (-1.0, 2.0), "the default range");
        let wide = StripAxis::points([0.0, 14.3].into_iter());
        assert!(wide.hi >= 14.3 && wide.ticks.len() <= 6, "{:?}", wide.ticks);
        assert_eq!(StripAxis::ratio([0.5, 3.9].into_iter()).hi, 4.0);
        assert_eq!(StripAxis::ratio([0.1].into_iter()).hi, 8.0);
    }

    #[test]
    fn log_axes_survive_degenerate_inputs() {
        let span = |axis: &LogAxis| axis.hi - axis.lo;
        // Empty, and nothing positive or finite: a sane default decade.
        for values in [vec![], vec![0.0, -3.0, f64::NAN, f64::INFINITY]] {
            let axis = LogAxis::covering(values.into_iter());
            assert!(
                span(&axis) >= 1.0 - 1e-9 && axis.lo.is_finite(),
                "{}..{}",
                axis.lo,
                axis.hi
            );
            assert!(!axis.ticks.is_empty());
        }
        // A single value, and equal min and max: at least one decade around it.
        for values in [vec![0.25], vec![0.25, 0.25, 0.25]] {
            let axis = LogAxis::covering(values.into_iter());
            assert!(span(&axis) >= 1.0 - 1e-9);
            assert!(axis.fraction(0.25) > 0.2 && axis.fraction(0.25) < 0.8);
            assert!(
                axis.ticks.len() >= 2,
                "1-2-5 ticks inside a sub-decade: {:?}",
                axis.ticks
            );
        }
        // Several decades: ticks only at decades, every value on the axis.
        let axis = LogAxis::covering([0.003, 0.04, 2.0, 25.0].into_iter());
        assert_eq!(axis.ticks, vec![0.01, 0.1, 1.0, 10.0]);
        for v in [0.003, 25.0] {
            let f = axis.fraction(v);
            assert!(f > 0.0 && f < 1.0, "{v} at {f}");
        }
        assert_eq!(
            axis.fraction(0.0),
            0.0,
            "a non-positive value clamps to the bottom"
        );
    }

    #[test]
    fn interpolation_handles_the_curve_ends() {
        assert_eq!(interpolate(&[], 0.2), None);
        assert_eq!(interpolate(&[(0.1, 5.0)], 0.4), Some(5.0));
        let curve = [(0.0, 1.0), (0.2, 3.0), (0.2, 3.0), (0.5, 4.0)];
        assert_eq!(interpolate(&curve, -0.1), Some(1.0));
        assert_eq!(interpolate(&curve, 0.1), Some(2.0));
        assert_eq!(interpolate(&curve, 0.2), Some(3.0));
        assert_eq!(interpolate(&curve, 0.9), Some(4.0));
    }

    #[test]
    fn downsampling_keeps_order_and_the_cap() {
        let points: Vec<(f64, f64)> = (0..500).map(|i| (i as f64, 0.0)).collect();
        let thinned = downsample(&points, 150);
        assert_eq!(thinned.len(), 150);
        assert!(thinned.windows(2).all(|w| w[0].0 < w[1].0));
        assert_eq!(thinned[0], points[0]);
        assert_eq!(downsample(&points[..10], 150).len(), 10);
        assert!(downsample(&points, 0).is_empty());
    }

    #[test]
    fn ratios_read_in_the_direction_of_the_measure() {
        assert_eq!(time_ratio(0.7), "1.4&times; faster");
        assert_eq!(time_ratio(2.6), "2.6&times; slower");
        assert!(time_comparison(1.04, false).starts_with("about as fast"));
        assert_eq!(
            failure_comparison(0.0123),
            "+1.2 points more likely to fail than distance alone predicts"
        );
        assert_eq!(fmt_time(0.27), "270 ms");
        assert_eq!(fmt_time(2.4), "2.4 s");
        assert_eq!(fmt_speed(250_000.0), "250 KB/s");
    }
}
