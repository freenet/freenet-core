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
use super::estimator::is_floored_speed;
use super::*;
use crate::node::network_status::PeerSnapshot;
use crate::router::{PeerOffset, PeerRoutingSnapshot, PeerWindow, Router, RouterSnapshotInfo};

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
                learned_card(inputs, view, &learned),
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

/// The readings the tiles, banners and charts share, computed once.
struct Learned {
    /// The response-time factor against distance alone, when there is enough
    /// evidence behind it to state (see [`usable_factor`]).
    response_factor: Option<f64>,
    transfer_factor: Option<f64>,
    still_learning: bool,
    avoided: bool,
}

impl Learned {
    fn of(view: &RouterView) -> Self {
        let offsets = view.peer.as_ref().map_or([None; 3], |peer| peer.offsets);
        let [failure, response, transfer] = offsets;
        // A stage "tells peers apart" once it gives some peer's own record any
        // weight. While it gives every peer zero, it has found no difference
        // between peers to learn, which is not the same as lacking evidence.
        let tells_apart = |stage: usize| {
            view.others
                .iter()
                .any(|(_, offsets)| offsets[stage].is_some_and(|o| o.weight > 0.0))
        };
        let differs = [tells_apart(0), tells_apart(1), tells_apart(2)];
        let known = |offset: Option<PeerOffset>| offset.is_some_and(|o| o.weight >= KNOWN_WEIGHT);
        let warm = failure.is_some() || response.is_some();
        // Still learning: no stage has adopted at least half of this peer's
        // record, AND either some stage tells other peers apart (so the gap is
        // this peer's evidence) or no stage is warm yet (so the node predicts
        // every peer from distance alone). A node whose warm stages find no
        // difference between any peers shows no banner: every peer is on the
        // line because they look alike, not because this one is new.
        let still_learning =
            !known(failure) && !known(response) && (differs[0] || differs[1] || !warm);

        let response_factor = usable_factor(response, differs[1]);
        let transfer_factor = usable_factor(transfer, differs[2]);
        let worse = (known(failure) && failure.is_some_and(|o| o.offset >= AVOIDED_FAILURE_POINTS))
            || (known(response)
                && response.is_some_and(|o| o.offset.exp() >= AVOIDED_SLOWER_FACTOR));
        let avoided = worse
            && view
                .peer
                .as_ref()
                .and_then(|peer| peer.selection)
                .is_some_and(|selection| {
                    selection.eligible >= AVOIDED_MIN_ELIGIBLE
                        && (selection.chosen as f64) < AVOIDED_SHARE_OF_EVEN * selection.even_share
                });
        Learned {
            response_factor,
            transfer_factor,
            still_learning,
            avoided,
        }
    }
}

/// The factor (`exp(offset)`) a timing stage has learned for a peer, when it
/// can be stated: the peer has evidence in that stage, and either the stage
/// has adopted at least [`KNOWN_WEIGHT`] of it or the stage tells no peers
/// apart (then the factor is exactly 1, and "about as fast as distance alone
/// predicts" is the true reading).
fn usable_factor(offset: Option<PeerOffset>, stage_tells_apart: bool) -> Option<f64> {
    let offset = offset?;
    (offset.evidence > 0.0 && (offset.weight >= KNOWN_WEIGHT || !stage_tells_apart))
        .then(|| offset.offset.exp())
        .filter(|factor| factor.is_finite() && *factor > 0.0)
}

fn learned_card(inputs: &PeerPageInputs<'_>, view: &RouterView, learned: &Learned) -> String {
    let mut banners = String::new();
    if view.fallback {
        banners.push_str(
            r#"<div class="learning">Routing is on its emergency fallback right now, so it does not use the comparisons on this page.</div>"#,
        );
    }
    if learned.avoided {
        banners.push_str(
            r#"<div class="learning">Your node now sends most requests this peer is eligible for to other peers instead.</div>"#,
        );
    } else if learned.still_learning {
        banners.push_str(
            r#"<div class="learning">Still learning about this peer. Until it has handled more requests, your node predicts its results from distance alone.</div>"#,
        );
    }
    let peer = view.peer.as_ref();
    format!(
        r#"<div class="card">
            <h2>What your node has learned</h2>
            {banners}
            <div class="tiles">{reliability}{response}{chosen}</div>
        </div>"#,
        reliability = reliability_tile(inputs),
        response = response_tile(peer, learned),
        chosen = chosen_tile(peer),
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

/// Requests this node started whose first hop was this peer, since it
/// connected: the peer-health counts eviction decisions are made on.
fn reliability_tile(inputs: &PeerPageInputs<'_>) -> String {
    let (network_ok, network_failed) = inputs
        .peers
        .iter()
        .filter_map(|peer| peer.route_outcomes)
        .fold((0u64, 0u64), |(ok, failed), (s, f)| (ok + s, failed + f));
    let network_total = network_ok + network_failed;
    let network = if network_total == 0 {
        String::new()
    } else if network_failed == 0 {
        " Across all your peers, none has failed yet.".to_string()
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
    let title = "Requests your node started whose first hop was this peer, since it connected.";
    match inputs.peer.route_outcomes {
        Some((ok, failed)) if ok + failed > 0 => tile(
            "Reliability",
            &format!("{ok} <small>of {}</small>", ok + failed),
            &format!("requests your node made through it succeeded.{network}"),
            title,
        ),
        _ => tile(
            "Reliability",
            "&mdash;",
            &format!("No requests through it yet.{network}"),
            title,
        ),
    }
}

fn response_tile(peer: Option<&PeerRoutingSnapshot>, learned: &Learned) -> String {
    let estimate = peer
        .and_then(|peer| peer.breakdown[1].as_ref())
        .map(|breakdown| breakdown.estimate)
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0);
    match (learned.response_factor, estimate) {
        (Some(factor), Some(seconds)) => tile(
            "Response time",
            &fmt_time_tile(seconds),
            &format!(
                "predicted for contracts near it; {}.",
                time_comparison(factor, true)
            ),
            "",
        ),
        _ => tile(
            "Response time",
            "&mdash;",
            "Too few replies to judge yet.",
            "",
        ),
    }
}

/// Real routing decisions in which this peer was among the closest candidates
/// scored, since the node started, and how many sent the request to it.
fn chosen_tile(peer: Option<&PeerRoutingSnapshot>) -> String {
    let title = "Since your node started. Eligible: among the closest peers your node considered for a request.";
    match peer.and_then(|peer| peer.selection) {
        Some(selection) if selection.eligible > 0 => tile(
            "Chosen",
            &format!(
                "{} <small>of {}</small>",
                selection.chosen, selection.eligible
            ),
            "requests it was eligible for were sent to it.",
            title,
        ),
        _ => tile(
            "Chosen",
            "&mdash;",
            "Not yet eligible for a request.",
            title,
        ),
    }
}

// ─── Compared with your other peers ─────────────────────────────────────────

fn compare_card(inputs: &PeerPageInputs<'_>, view: &RouterView) -> String {
    let this = inputs.peer.address.to_string();
    let count = view.others.len();
    let dots = |stage: usize, value: fn(f64) -> f64| -> (Vec<StripDot>, Option<StripDot>) {
        let mut others = Vec::new();
        let mut mine = None;
        for (address, offsets) in &view.others {
            let Some(offset) = offsets[stage] else {
                continue;
            };
            let dot = StripDot {
                value: value(offset.offset),
                title: String::new(),
                href: (address != &this).then(|| format!("/peer/{}", html_escape(address))),
            };
            let label = |text: String| {
                if address == &this {
                    format!("this peer &middot; {text}")
                } else {
                    format!("{} &middot; {text}", html_escape(address))
                }
            };
            let text = match stage {
                0 => failure_comparison(offset.offset),
                _ => format!(
                    "{} &middot; {:.0} replies counted",
                    time_comparison(offset.offset.exp(), false),
                    offset.evidence
                ),
            };
            let dot = StripDot {
                title: label(text),
                ..dot
            };
            if address == &this {
                mine = Some(dot);
            } else {
                others.push(dot);
            }
        }
        (others, mine)
    };
    let (fail_others, fail_mine) = dots(0, |offset| offset * 100.0);
    let (time_others, time_mine) = dots(1, f64::exp);

    let failure = if fail_others.is_empty() && fail_mine.is_none() {
        r#"<div class="empty-box">Too few requests across your peers to compare yet.</div>"#
            .to_string()
    } else {
        responsive(|width| {
            strip_chart(
                width,
                StripAxis::points(
                    fail_others
                        .iter()
                        .chain(fail_mine.iter())
                        .map(|dot| dot.value),
                ),
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
        responsive(|width| {
            strip_chart(
                width,
                StripAxis::ratio(
                    time_others
                        .iter()
                        .chain(time_mine.iter())
                        .map(|dot| dot.value),
                ),
                &time_others,
                time_mine.as_ref(),
                "Response time compared with distance alone, one dot per peer",
            )
        })
    };
    format!(
        r#"<div class="card">
            <h2>Compared with your other peers</h2>
            <p class="caption">Each dot is one of your {count} peers, placed by how its results differ from what distance alone predicts. A peer with few requests stays on the line until there is evidence either way.</p>
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

fn strip_chart(
    width: f64,
    axis: StripAxis,
    others: &[StripDot],
    mine: Option<&StripDot>,
    aria: &str,
) -> String {
    let (h, left, right, top, bottom) = (84.0, 34.0, 40.0, 20.0, 22.0);
    let mid = top + (h - top - bottom) / 2.0;
    let band = (h - top - bottom) / 2.0 - 5.0;
    let x = |value: f64| left + axis.fraction(value) * (width - left - right);
    let mut svg =
        format!(r#"<svg viewBox="0 0 {width} {h}" class="mchart" role="img" aria-label="{aria}">"#);
    write!(
        svg,
        r#"<line x1="{left}" x2="{x2}" y1="{y}" y2="{y}" class="ax"/>"#,
        x2 = width - right,
        y = h - bottom
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
                h - bottom + 14.0,
                (axis.label)(*tick)
            )
            .ok();
        }
    }
    let zero_x = x(axis.zero);
    write!(
        svg,
        r#"<line x1="{zero_x:.1}" x2="{zero_x:.1}" y1="{:.1}" y2="{:.1}" class="baseline"/><text x="{zero_x:.1}" y="{:.1}" text-anchor="middle" class="dl">distance alone</text>"#,
        top - 6.0,
        h - bottom,
        top - 10.0,
    )
    .ok();
    for (index, dot) in others.iter().enumerate() {
        // Deterministic vertical jitter (golden-ratio sequence), so overlapping
        // peers spread into a band instead of hiding each other.
        let jitter = ((index as f64 + 1.0) * 0.618_033_988_75).fract() * 2.0 - 1.0;
        let circle = format!(
            r#"<circle cx="{:.1}" cy="{:.1}" r="3.2" class="dot-other"><title>{}</title></circle>"#,
            x(dot.value),
            mid + jitter * band,
            dot.title
        );
        match &dot.href {
            Some(href) => write!(svg, r#"<a href="{href}">{circle}</a>"#).ok(),
            None => write!(svg, "{circle}").ok(),
        };
    }
    if let Some(dot) = mine {
        let cx = x(dot.value);
        let right_side = cx < width * 0.75;
        write!(
            svg,
            r#"<circle cx="{cx:.1}" cy="{mid:.1}" r="6" class="dot-this"><title>{title}</title></circle><text x="{lx:.1}" y="{ly:.1}" text-anchor="{anchor}" class="this-label">this peer</text>"#,
            title = dot.title,
            lx = if right_side { cx + 11.0 } else { cx - 11.0 },
            ly = mid + 4.0,
            anchor = if right_side { "start" } else { "end" },
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
    let distance_alone_time = &rs.hierarchical_curves.response_time;
    let distance_alone_speed = &rs.hierarchical_curves.transfer_speed;
    // Lines only from the estimate routing acts on; on the emergency fallback
    // routing uses neither, so neither is drawn.
    let lines = !view.fallback;
    let time_factor = learned
        .response_factor
        .filter(|factor| lines && !about_same(*factor));
    let speed_factor = learned
        .transfer_factor
        .filter(|factor| lines && !about_same(*factor));
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
        write!(
            labels,
            r#"<span class="tab-label{dim}{active}" data-tab="{id}" onclick="switchTab(this)">{name} <span class="tab-count">{count}</span></span>"#,
            count = mine.outcomes,
        )
        .ok();

        let times = if all_times.is_empty() && mine.response_times.is_empty() {
            r#"<div class="empty-box">No timed replies yet.</div>"#.to_string()
        } else {
            responsive(|width| {
                distance_chart(
                    width,
                    &DistanceChart {
                        kind: Measure::Time,
                        all: all_times,
                        mine: &mine.response_times,
                        line: if lines { distance_alone_time } else { &EMPTY },
                        factor: time_factor,
                    },
                )
            })
        };
        let speeds = if mine.transfer_speeds.is_empty() {
            r#"<div class="empty-box">No data transfers measured through this peer yet.</div>"#
                .to_string()
        } else {
            responsive(|width| {
                distance_chart(
                    width,
                    &DistanceChart {
                        kind: Measure::Speed,
                        all: all_speeds,
                        mine: &mine.transfer_speeds,
                        line: if lines { distance_alone_speed } else { &EMPTY },
                        factor: speed_factor,
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

    let caption = match learned.response_factor.filter(|_| lines) {
        Some(factor) if !about_same(factor) => format!(
            "Each dot is a reply to a request your node sent to a peer. This peer replies <b>{}</b> than distance alone predicts, so your node {} it.",
            time_ratio(factor),
            if factor < 1.0 { "favours" } else { "avoids" }
        ),
        Some(_) => "Each dot is a reply to a request your node sent to a peer. This peer replies about as fast as distance alone predicts.".to_string(),
        None => "Each dot is a reply to a request your node sent to a peer. Once this peer has handled more requests, a second line will show how it differs from what distance alone predicts.".to_string(),
    };
    let has_line = lines && !distance_alone_time.is_empty();
    let legend = format!(
        r#"<div class="legend-row"><span><i class="sw-dot-peer"></i>this peer</span><span><i class="sw-dot-other"></i>other peers</span>{line}{peer_line}</div>"#,
        line = if has_line {
            r#"<span><i class="sw-line-net"></i>predicted from distance alone</span>"#
        } else {
            ""
        },
        peer_line = if has_line && time_factor.is_some() {
            r#"<span><i class="sw-line-peer"></i>predicted for this peer</span>"#
        } else {
            ""
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
    /// What routing predicts from distance alone, `(distance, value)`.
    line: &'a [(f64, f64)],
    /// This peer's learned factor on that line, when stated.
    factor: Option<f64>,
}

/// Observations against ring distance on a log scale, with the distance-alone
/// prediction and this peer's line.
fn distance_chart(width: f64, chart: &DistanceChart<'_>) -> String {
    let narrow = width < 450.0;
    let (h, left, right, top, bottom) =
        (230.0, 50.0, if narrow { 72.0 } else { 112.0 }, 10.0, 34.0);
    let peer_line: Vec<(f64, f64)> = chart
        .factor
        .map(|factor| chart.line.iter().map(|&(d, v)| (d, v * factor)).collect())
        .unwrap_or_default();
    let values = chart
        .all
        .iter()
        .chain(chart.mine)
        .chain(chart.line)
        .chain(&peer_line)
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
    for &(d, v) in chart.all {
        if d.is_finite() && v.is_finite() && v > 0.0 {
            write!(
                svg,
                r#"<circle cx="{:.1}" cy="{:.1}" r="1.8" class="dot-faint"/>"#,
                x(d),
                y(v)
            )
            .ok();
        }
    }
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
        end_labels.push((
            y(end),
            if narrow {
                vec!["distance".to_string(), "alone".to_string()]
            } else {
                vec!["distance alone".to_string()]
            },
        ));
    }
    if let (Some(&(_, end)), Some(factor)) = (peer_line.last(), chart.factor) {
        write!(svg, r#"<path d="{}" class="line-peer"/>"#, path(&peer_line)).ok();
        let ratio = match chart.kind {
            Measure::Time => time_ratio(factor),
            Measure::Speed => speed_ratio(factor),
        };
        end_labels.push((y(end), vec!["this peer".to_string(), ratio]));
    }
    // Keep stacked end labels at least one line apart.
    end_labels.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut next_free = f64::NEG_INFINITY;
    for (label_y, lines) in &end_labels {
        let start = label_y.max(next_free);
        for (i, line) in lines.iter().enumerate() {
            write!(
                svg,
                r#"<text x="{:.1}" y="{:.1}" class="dl">{line}</text>"#,
                width - right + 6.0,
                start + 4.0 + 12.0 * i as f64
            )
            .ok();
        }
        next_free = start + 12.0 * lines.len() as f64 + 1.0;
    }
    for &(d, v) in chart.mine {
        if !(d.is_finite() && v.is_finite() && v > 0.0) {
            continue;
        }
        let predicted = interpolate(chart.line, d)
            .map(|p| format!(" (distance alone predicts {})", chart.kind.format(p)))
            .unwrap_or_default();
        write!(
            svg,
            r#"<circle cx="{:.1}" cy="{:.1}" r="4.5" class="dot-this"><title>this peer &middot; {} at ring distance {d:.2}{predicted}</title></circle>"#,
            x(d),
            y(v),
            chart.kind.format(v),
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
    let chart = if all.is_empty() && mine.is_empty() {
        r#"<div class="empty-box">No predictions compared with replies yet.</div>"#.to_string()
    } else {
        accuracy_chart(all, mine)
    };
    format!(
        r#"<div class="card">
            <h2>How good are the predictions?</h2>
            <div class="acc-row">
                <div>{chart}</div>
                <div class="acc-facts">
                    <p>Before each request your node predicts how long each eligible peer will take to reply, and sends it to the best one. Each dot compares a prediction with what actually happened.</p>
                    <p>{verdict}</p>
                </div>
            </div>
        </div>
        <p class="more"><a href="/routing">How your node's routing model is doing overall &rarr;</a></p>"#
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
        <text x="{:.1}" y="{:.1}" text-anchor="end" class="dl">exact</text>
        <text x="{:.1}" y="{:.1}">slower than predicted</text>
        <text x="{:.1}" y="{:.1}" text-anchor="end">faster than predicted</text>
        <text x="{:.1}" y="{:.1}" text-anchor="middle">predicted</text>
        <text x="12" y="{mid:.1}" text-anchor="middle" transform="rotate(-90 12 {mid:.1})">actual</text>"#,
        x(min),
        y(min),
        x(max),
        y(max),
        x(max) - 18.0,
        y(max) + 14.0,
        left + 4.0,
        top + 10.0,
        w - right - 4.0,
        h - bottom - 6.0,
        left + (w - left - right) / 2.0,
        h - 4.0,
        mid = (top + h - bottom) / 2.0,
    )
    .ok();
    for &(p, a) in all {
        if p > 0.0 && a > 0.0 && p.is_finite() && a.is_finite() {
            write!(
                svg,
                r#"<circle cx="{:.1}" cy="{:.1}" r="1.8" class="dot-faint"/>"#,
                x(p),
                y(a)
            )
            .ok();
        }
    }
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
    let (cx, cy, r) = (88.0, 48.0, 34.0);
    let at = |location: f64| {
        let angle = location * std::f64::consts::TAU;
        (cx + r * angle.sin(), cy - r * angle.cos())
    };
    // Labels sit outside the ring, on the side the dot is on.
    let label = |(x, y): (f64, f64), text: &str, dy: f64| {
        let right = x >= cx;
        format!(
            r#"<text x="{:.1}" y="{:.1}" text-anchor="{}">{text}</text>"#,
            if right { x + 8.0 } else { x - 8.0 },
            y + dy,
            if right { "start" } else { "end" }
        )
    };
    let mut svg = format!(
        r#"<svg viewBox="0 0 176 100" class="mini-ring" role="img" aria-label="Ring position of you and this peer"><circle cx="{cx}" cy="{cy}" r="{r}" class="ring"/>"#
    );
    let peer_at = at(peer);
    if let Some(own) = own {
        let own_at = at(own);
        // Nudge the labels apart when the two dots sit close together.
        let close = (own_at.1 - peer_at.1).abs() < 12.0 && (own_at.0 >= cx) == (peer_at.0 >= cx);
        let own_dy = if close && own_at.1 <= peer_at.1 {
            -4.0
        } else {
            3.0
        };
        let peer_dy = if close && own_at.1 > peer_at.1 {
            -4.0
        } else if close {
            12.0
        } else {
            3.0
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
        svg.push_str(&label(peer_at, "this peer", 3.0));
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

/// A chart rendered at a desktop and a phone width; CSS shows the one that fits.
fn responsive(render: impl Fn(f64) -> String) -> String {
    format!(
        r#"<div class="w-wide">{}</div><div class="w-narrow">{}</div>"#,
        render(600.0),
        render(340.0)
    )
}

/// A log axis: its ends in `log10` units, and the values to tick.
struct LogAxis {
    lo: f64,
    hi: f64,
    ticks: Vec<f64>,
}

impl LogAxis {
    /// Covers every positive finite value with a little room either side, at
    /// least one decade wide. Ticks at the decades inside it, or at 1-2-5
    /// steps when fewer than two decades fall inside.
    fn covering(values: impl Iterator<Item = f64>) -> Self {
        let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
        for value in values.filter(|v| v.is_finite() && *v > 0.0) {
            min = min.min(value);
            max = max.max(value);
        }
        if !min.is_finite() {
            (min, max) = (0.1, 1.0);
        }
        let (mut lo, mut hi) = ((min / 1.3).log10(), (max * 1.3).log10());
        if hi - lo < 1.0 {
            let grow = (1.0 - (hi - lo)) / 2.0;
            lo -= grow;
            hi += grow;
        }
        let inside = |v: f64| v.log10() >= lo - 1e-9 && v.log10() <= hi + 1e-9;
        let decades: Vec<f64> = (lo.floor() as i32..=hi.ceil() as i32)
            .map(|d| 10f64.powi(d))
            .filter(|v| inside(*v))
            .collect();
        let ticks = if decades.len() >= 2 {
            decades
        } else {
            (lo.floor() as i32..=hi.ceil() as i32)
                .flat_map(|d| [1.0, 2.0, 5.0].map(|m| m * 10f64.powi(d)))
                .filter(|v| inside(*v))
                .collect()
        };
        LogAxis { lo, hi, ticks }
    }

    /// Position of `value` along the axis, 0 at the low end and 1 at the high.
    fn fraction(&self, value: f64) -> f64 {
        ((value.max(1e-300).log10() - self.lo) / (self.hi - self.lo)).clamp(0.0, 1.0)
    }
}

/// Linear interpolation on a curve sorted by distance.
fn interpolate(curve: &[(f64, f64)], d: f64) -> Option<f64> {
    let first = curve.first()?;
    if d <= first.0 {
        return Some(first.1);
    }
    for pair in curve.windows(2) {
        let ((d0, v0), (d1, v1)) = (pair[0], pair[1]);
        if d <= d1 {
            let t = if d1 > d0 { (d - d0) / (d1 - d0) } else { 0.0 };
            return Some(v0 + t * (v1 - v0));
        }
    }
    curve.last().map(|last| last.1)
}

fn about_same(factor: f64) -> bool {
    (1.0 / SAME_FACTOR..=SAME_FACTOR).contains(&factor)
}

/// A whole number without decimals, otherwise one.
fn fmt_trim(value: f64) -> String {
    if (value - value.round()).abs() < 1e-9 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

/// `1.4× faster` / `2.6× slower` for a response-time factor.
fn time_ratio(factor: f64) -> String {
    if factor < 1.0 {
        format!("{}&times; faster", fmt_factor(1.0 / factor))
    } else {
        format!("{}&times; slower", fmt_factor(factor))
    }
}

/// The same for a transfer-speed factor, where higher is faster.
fn speed_ratio(factor: f64) -> String {
    if factor >= 1.0 {
        format!("{}&times; faster", fmt_factor(factor))
    } else {
        format!("{}&times; slower", fmt_factor(1.0 / factor))
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

/// A duration for an axis or a tooltip.
fn fmt_time(seconds: f64) -> String {
    if seconds < 0.001 {
        format!("{:.0} &micro;s", seconds * 1e6)
    } else if seconds < 1.0 {
        format!("{} ms", fmt_trim_sig(seconds * 1e3))
    } else {
        format!("{} s", fmt_trim_sig(seconds))
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

/// Up to three significant figures, without trailing zeros: 100, 25, 2.5, 0.25.
fn fmt_trim_sig(value: f64) -> String {
    if value >= 100.0 {
        format!("{value:.0}")
    } else if value >= 10.0 {
        fmt_trim(value)
    } else {
        let text = format!("{value:.2}");
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

/// A transfer speed for an axis or a tooltip.
fn fmt_speed(bytes_per_second: f64) -> String {
    let (value, unit) = if bytes_per_second >= 1e9 {
        (bytes_per_second / 1e9, "GB/s")
    } else if bytes_per_second >= 1e6 {
        (bytes_per_second / 1e6, "MB/s")
    } else if bytes_per_second >= 1e3 {
        (bytes_per_second / 1e3, "KB/s")
    } else {
        (bytes_per_second, "B/s")
    };
    format!("{} {unit}", fmt_trim_sig(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::network_status::OpType;
    use crate::ring::{Location, PeerKeyLocation};
    use crate::router::{PeerSelection, RouteEvent, RouteOutcome};
    use std::time::Duration;

    /// Twelve peers, forty rounds each, through `add_event` on a frozen clock:
    /// `peers[0]` replies four times slower and fails one request in three,
    /// `peers[3]` never transfers a payload, the rest are ordinary.
    fn trained_router() -> (Router, Vec<PeerKeyLocation>) {
        let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
            crate::util::time_source::SharedMockTimeSource::new(),
        ));
        let peers: Vec<PeerKeyLocation> = (0..12).map(|_| PeerKeyLocation::random()).collect();
        for round in 0..40u64 {
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

    fn snapshot_of(key: &PeerKeyLocation, route_outcomes: Option<(u64, u64)>) -> PeerSnapshot {
        PeerSnapshot {
            address: key.socket_addr().expect("random peers have an address"),
            is_gateway: false,
            location: key.location().map(|l| l.as_f64()),
            connected_secs: 457,
            peer_key_location: Some(key.clone()),
            bytes_sent: 376_700,
            bytes_received: 348_500,
            route_outcomes,
        }
    }

    fn snapshots(peers: &[PeerKeyLocation]) -> Vec<PeerSnapshot> {
        peers
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let outcomes = if index == 0 { (26, 14) } else { (39, 1) };
                snapshot_of(key, Some(outcomes))
            })
            .collect()
    }

    fn render(router: &Router, snaps: &[PeerSnapshot], index: usize) -> String {
        let inputs = PeerPageInputs {
            peer: &snaps[index],
            peers: snaps,
            own_location: Some(0.118),
            version: "0.2.141",
        };
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

    const STILL_LEARNING: &str = "Still learning about this peer.";
    const AVOIDED: &str =
        "Your node now sends most requests this peer is eligible for to other peers instead.";

    #[test]
    fn a_typical_peer_shows_every_section_with_no_banner() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let html = render(&router, &snaps, 5);
        for needle in [
            "What your node has learned",
            "39 <small>of 40</small>",
            "requests your node made through it succeeded.",
            "Across all your peers, about 1 in",
            "distance alone predicts",
            "Compared with your other peers",
            "Each dot is one of your 12 peers",
            "Past requests by ring distance",
            "ring distance between peer and contract (0 = same spot, 0.5 = opposite side)",
            "How good are the predictions?",
            "replies typically land within",
            r#"href="/routing""#,
        ] {
            assert!(html.contains(needle), "missing {needle:?}");
        }
        assert!(!html.contains(STILL_LEARNING) && !html.contains(AVOIDED));
        assert!(
            html.contains(r#"<span class="tab-label tab-active" data-tab="all" onclick="switchTab(this)">All <span class="tab-count">40</span>"#),
            "the All tab counts this peer's outcomes in the window"
        );
        assert!(
            html.contains(
                r#"data-tab="put" onclick="switchTab(this)">PUT <span class="tab-count">10</span>"#
            ),
            "each op tab counts this peer's outcomes for that op"
        );
        assert!(
            html.matches(r#"class="w-wide""#).count()
                == html.matches(r#"class="w-narrow""#).count(),
            "every chart with axis text has a phone variant"
        );
    }

    #[test]
    fn a_peer_the_router_has_never_seen_is_still_being_learned() {
        let (router, peers) = trained_router();
        let mut snaps = snapshots(&peers);
        snaps.push(snapshot_of(&PeerKeyLocation::random(), Some((2, 0))));
        let html = render(&router, &snaps, 12);
        assert!(
            html.contains(STILL_LEARNING),
            "the still-learning banner shows"
        );
        assert!(html.contains("2 <small>of 2</small>"));
        assert!(
            html.contains(
                r#"<div class="v">&mdash;</div><div class="s">Too few replies to judge yet.</div>"#
            ),
            "the response-time tile has no figure yet"
        );
        assert!(html.contains("Not yet eligible for a request."));
        assert!(html.contains("Too few requests through this peer to judge its predictions yet"));
        assert!(html.contains("Each dot is one of your 13 peers"));
    }

    #[test]
    fn a_slow_failing_peer_that_routing_passes_over_reads_as_avoided() {
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
        let snaps = snapshots(&peers);
        let html = render(&router, &snaps, 0);
        assert!(html.contains(AVOIDED), "the avoided banner shows");
        assert!(!html.contains(STILL_LEARNING));
        assert!(html.contains("than distance alone predicts"));
        assert!(
            html.contains("slower"),
            "the response-time tile names the direction"
        );
        assert!(html.contains("26 <small>of 40</small>"));
        // Chosen: some of the 40 eligible decisions, far fewer than an even split.
        let selection = router.peer_snapshot(&peers[0]).selection.unwrap();
        assert_eq!(selection.eligible, 40);
        assert!(html.contains(&format!("{} <small>of 40</small>", selection.chosen)));
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
        let inputs = PeerPageInputs {
            peer: &snaps[4],
            peers: &snaps,
            own_location: None,
            version: "?",
        };
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

    /// One term per concept, and none of the model's internals: a reader of
    /// this page should never meet the estimator's vocabulary.
    #[test]
    fn the_peer_page_uses_none_of_the_models_internal_terms() {
        let (router, peers) = trained_router();
        let mut snaps = snapshots(&peers);
        snaps.push(snapshot_of(&PeerKeyLocation::random(), None));
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

    #[test]
    fn without_a_router_the_page_says_so() {
        let peer = snapshot_of(&PeerKeyLocation::random(), None);
        let snaps = [peer];
        let inputs = PeerPageInputs {
            peer: &snaps[0],
            peers: &snaps,
            own_location: Some(0.5),
            version: "?",
        };
        let html = render_peer_page(&inputs, None);
        assert!(html.contains("Routing data is not available yet."));
        assert!(html.contains("Location <b>"), "the header still renders");
    }

    // ── The readings behind the banners and tiles ──────────────────────────

    fn offset(offset: f64, evidence: f64, weight: f64) -> Option<PeerOffset> {
        Some(PeerOffset {
            offset,
            evidence,
            weight,
        })
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
            fallback: false,
        };
        let peer = view.peer.as_mut().unwrap();
        peer.offsets = mine;
        peer.selection = selection;
        view.others.push(("this".to_string(), mine));
        view
    }

    #[test]
    fn still_learning_needs_peers_to_differ_or_a_cold_node() {
        let known_other = [offset(0.02, 30.0, 0.7), offset(0.3, 30.0, 0.7), None];
        // Others are told apart, this one has under half its record adopted.
        let learning = view_with(
            [offset(0.0, 1.0, 0.2), offset(0.0, 1.0, 0.49), None],
            vec![known_other],
            None,
        );
        assert!(Learned::of(&learning).still_learning);
        // Exactly half adopted counts as known.
        let known = view_with([offset(0.0, 9.0, 0.5), None, None], vec![known_other], None);
        assert!(!Learned::of(&known).still_learning);
        // Warm stages that tell nobody apart: every peer is on the line because
        // they look alike, not because this one is new. No banner.
        let alike = [offset(0.0, 40.0, 0.0), offset(0.0, 40.0, 0.0), None];
        let flat = view_with(alike, vec![alike, alike], None);
        assert!(!Learned::of(&flat).still_learning);
        // Nothing warm yet: the node predicts every peer from distance alone.
        let cold = view_with([None, None, None], vec![[None, None, None]], None);
        assert!(Learned::of(&cold).still_learning);
    }

    #[test]
    fn the_avoided_reading_needs_evidence_a_low_share_and_a_worse_record() {
        let others = vec![[offset(0.0, 30.0, 0.7), offset(0.0, 30.0, 0.7), None]];
        let selection = |eligible, chosen, even_share| {
            Some(PeerSelection {
                eligible,
                chosen,
                even_share,
            })
        };
        let worse = [offset(0.01, 30.0, 0.6), offset(0.0, 30.0, 0.6), None];
        let avoided = |mine, sel| Learned::of(&view_with(mine, others.clone(), sel)).avoided;
        assert!(
            avoided(worse, selection(20, 1, 4.0)),
            "all three conditions hold"
        );
        assert!(!avoided(worse, selection(19, 0, 4.0)), "too few decisions");
        assert!(
            !avoided(worse, selection(20, 2, 4.0)),
            "chosen at half an even split is not avoided"
        );
        let not_worse = [offset(0.0099, 30.0, 0.6), offset(0.2, 30.0, 0.6), None];
        assert!(
            !avoided(not_worse, selection(40, 0, 8.0)),
            "no worse than distance alone"
        );
        let slower = [
            offset(0.0, 30.0, 0.6),
            offset(1.25f64.ln(), 30.0, 0.6),
            None,
        ];
        assert!(
            avoided(slower, selection(40, 0, 8.0)),
            "1.25x slower is worse"
        );
        let unsure = [offset(0.05, 3.0, 0.4), offset(0.0, 3.0, 0.4), None];
        assert!(
            !avoided(unsure, selection(40, 0, 8.0)),
            "a worse record needs half its weight adopted"
        );
    }

    #[test]
    fn a_timing_factor_is_stated_only_with_evidence_behind_it() {
        assert_eq!(usable_factor(None, true), None);
        assert_eq!(
            usable_factor(offset(0.5, 0.0, 0.0), false),
            None,
            "no replies"
        );
        assert_eq!(
            usable_factor(offset(0.5, 4.0, 0.49), true),
            None,
            "distance alone still outweighs it"
        );
        assert!(
            usable_factor(offset(0.5, 9.0, 0.5), true)
                .is_some_and(|f| (f - 0.5f64.exp()).abs() < 1e-12)
        );
        assert_eq!(
            usable_factor(offset(0.0, 40.0, 0.0), false),
            Some(1.0),
            "peers do not differ"
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
    fn ratios_read_in_the_direction_of_the_measure() {
        assert_eq!(time_ratio(0.7), "1.4&times; faster");
        assert_eq!(time_ratio(2.6), "2.6&times; slower");
        assert_eq!(speed_ratio(2.0), "2.0&times; faster");
        assert_eq!(speed_ratio(0.5), "2.0&times; slower");
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
