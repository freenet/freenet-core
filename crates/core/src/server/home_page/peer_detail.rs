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
    SELECTION_RECENT_DECISIONS,
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

/// What the page says in place of a stage's comparison when that stage tells
/// no peers apart ([`stage_alike`]): a statement about every peer,
/// so it replaces the strip instead of riding on "this peer".
const NO_DIFFERENCE: [&str; 2] = [
    "No measurable difference between your peers' failure rates yet, so your node has not learned any peer to fail more or less often than distance alone predicts.",
    "No measurable difference between your peers' response times yet, so your node has not learned any peer to be faster or slower than distance alone predicts.",
];

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
    /// The peer's key location carries a ring location. Without one, routing
    /// predicts nothing for it (it has a record to look up, but no distance).
    located: bool,
    /// Every connected peer that has a key location, this one included.
    others: Vec<OtherPeer>,
    /// `(outcomes, failures)` across every peer in the router's failure
    /// window, the population this peer's window counts are a share of.
    window_outcomes: (usize, usize),
    fallback: bool,
}

/// One connected peer's readings, for the comparison strips.
struct OtherPeer {
    address: String,
    /// See [`PeerRoutingSnapshot::offsets`].
    offsets: [Option<PeerOffset>; 3],
    /// See [`PeerRoutingSnapshot::expected_response_factor`].
    response_factor: Option<f64>,
}

impl RouterView {
    pub(super) fn gather(inputs: &PeerPageInputs<'_>, router: &Router) -> Self {
        // The no-record line is the same for every peer: computed once.
        let alone = router.response_line_at_zero(None);
        RouterView {
            snapshot: router.snapshot(),
            peer: inputs
                .peer
                .peer_key_location
                .as_ref()
                .map(|key| router.peer_snapshot(key)),
            located: inputs
                .peer
                .peer_key_location
                .as_ref()
                .is_some_and(|key| key.location().is_some()),
            others: inputs
                .peers
                .iter()
                .filter_map(|peer| {
                    let key = peer.peer_key_location.as_ref()?;
                    Some(OtherPeer {
                        address: peer.address.to_string(),
                        offsets: router.peer_offsets(key),
                        response_factor: alone
                            .and_then(|alone| router.expected_response_factor_against(key, alone)),
                    })
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

/// Where routing's prediction for one stage of this peer comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The stage is cold: routing uses the early (isotonic) estimate, which
    /// already corrects per peer, so "distance alone" would be false.
    Early,
    /// Warm, and none of the peer's record adopted.
    DistanceAlone,
    /// Warm, part of it adopted, less than [`KNOWN_WEIGHT`].
    Mostly,
    /// Warm, at least [`KNOWN_WEIGHT`] adopted.
    Known,
}

/// The stages a still-learning banner speaks for: all three, the transfer
/// stage included, because the page draws its lines too.
const STAGE_NAMES: [&str; 3] = ["failures", "response time", "transfer speed"];

/// Whether, and how, the node still predicts this peer without its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Learning {
    /// Some stage has learned enough to use the peer's own record, or the
    /// warm stages find nothing that tells any peer from another.
    No,
    /// Routing is not predicting at all yet (the 50-event gate).
    NotPredicting,
    /// The peer has no ring location, so routing cannot predict for it.
    NoLocation,
    /// Every stage is cold: routing uses its early estimates throughout.
    FirstEstimates,
    /// Per stage `[failure, response time, transfer speed]`, none known.
    Still([Source; 3]),
}

/// "your node predicts its failures from distance alone, and its response
/// time and transfer speed from an early estimate": the stages grouped by
/// where their prediction comes from, in stage order.
fn still_learning_sentence(sources: [Source; 3]) -> String {
    let phrase = |source: Source| match source {
        Source::Early => "from an early estimate",
        Source::DistanceAlone => "from distance alone",
        Source::Mostly => "mostly from distance alone",
        Source::Known => "from its own record",
    };
    if sources.iter().all(|s| *s == sources[0]) {
        return format!("your node predicts its results {}", phrase(sources[0]));
    }
    let mut groups: Vec<(Source, Vec<&str>)> = Vec::new();
    for (source, name) in sources.into_iter().zip(STAGE_NAMES) {
        match groups.iter_mut().find(|(s, _)| *s == source) {
            Some((_, names)) => names.push(name),
            None => groups.push((source, vec![name])),
        }
    }
    let clauses: Vec<String> = groups
        .into_iter()
        .map(|(source, names)| format!("its {} {}", names.join(" and "), phrase(source)))
        .collect();
    let list = match clauses.as_slice() {
        [one] => one.clone(),
        [first, second] => format!("{first}, and {second}"),
        [init @ .., last] => format!("{}, and {last}", init.join(", ")),
        [] => String::new(),
    };
    format!("your node predicts {list}")
}

/// The readings the tiles, banners and charts share, computed once.
struct Learned {
    /// How many times slower (above 1) or faster routing expects this peer to
    /// reply than distance alone predicts, when there is enough evidence
    /// behind it to state (see [`known`]).
    response_factor: Option<f64>,
    /// Whether each of `[failure, response time]` finds every peer alike
    /// (see [`stage_alike`]).
    alike: [bool; 2],
    learning: Learning,
    /// Why the peer reads as avoided, when it does: `(slower, less reliable)`.
    avoided: Option<(bool, bool)>,
}

/// Whether a stage's reading for a peer can be stated: the peer has evidence
/// there and the stage adopted at least [`KNOWN_WEIGHT`] of it. A stage that
/// gives no peer any weight states nothing either.
fn known(offset: Option<PeerOffset>) -> bool {
    offset.is_some_and(|o| o.evidence > 0.0 && o.weight >= KNOWN_WEIGHT)
}

/// A stage finds every connected peer alike when it holds a record of at
/// least one of them (evidence > 0) yet gives no connected peer's record any
/// weight: it measured them and found no spread between peers to learn. A
/// stage with no record of any of them (all fresh, or evicted) also gives
/// every one weight 0, but that is too few replies, not "no difference".
fn stage_alike(view: &RouterView, stage: usize) -> bool {
    let mut measured = false;
    for offset in view.others.iter().filter_map(|other| other.offsets[stage]) {
        if offset.weight > 0.0 {
            return false;
        }
        measured |= offset.evidence > 0.0;
    }
    measured
}

impl Learned {
    fn of(view: &RouterView) -> Self {
        let alike = [stage_alike(view, 0), stage_alike(view, 1)];
        let nothing = Learned {
            response_factor: None,
            alike,
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
        if !view.snapshot.prediction_active {
            return Learned {
                learning: Learning::NotPredicting,
                ..nothing
            };
        }
        if !view.located {
            return Learned {
                learning: Learning::NoLocation,
                ..nothing
            };
        }
        let [failure, response, _] = peer.offsets;
        let sources = peer.offsets.map(|offset| match offset {
            None => Source::Early,
            Some(_) if known(offset) => Source::Known,
            Some(o) if o.weight == 0.0 => Source::DistanceAlone,
            Some(_) => Source::Mostly,
        });
        // Over the WARM stages only: a cold stage (transfer speed, often,
        // since transfers are rare) does not stop the warm ones from having
        // found that every peer looks alike, which more requests from this
        // peer will not change.
        let warm_alike = peer.offsets.iter().any(Option::is_some)
            && (0..3).all(|stage| peer.offsets[stage].is_none() || stage_alike(view, stage));
        let learning = if sources.contains(&Source::Known) {
            Learning::No
        } else if sources.iter().all(|s| *s == Source::Early) {
            Learning::FirstEstimates
        } else if warm_alike {
            // Every warm stage finds no difference between any peers: every
            // peer is on the line because they look alike.
            Learning::No
        } else {
            Learning::Still(sources)
        };
        // Routing's own expectation, the ratio of the two lines the page
        // draws, so the stated factor and the lines cannot disagree.
        let response_factor = known(response)
            .then_some(peer.expected_response_factor)
            .flatten()
            .filter(|factor| factor.is_finite() && *factor > 0.0);
        let slower = response_factor.is_some_and(|factor| factor >= AVOIDED_SLOWER_FACTOR);
        let less_reliable =
            known(failure) && failure.is_some_and(|o| o.offset >= AVOIDED_FAILURE_POINTS);
        let passed_over = peer.selection.is_some_and(|selection| {
            selection.eligible >= AVOIDED_MIN_ELIGIBLE
                && selection.chosen < AVOIDED_SHARE_OF_EVEN * selection.even_share
        });
        Learned {
            response_factor,
            alike,
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
            r#"<div class="learning">Your node expects this peer to be {worse} than distance alone predicts, and recently ranks it first far less often than its share.</div>"#
        )
        .ok();
    } else {
        match learned.learning {
            Learning::NotPredicting => banners.push_str(
                r#"<div class="learning">Your node is not predicting yet: until it has seen 50 requests, it routes by distance alone.</div>"#,
            ),
            Learning::FirstEstimates => banners.push_str(
                r#"<div class="learning">Your node is still building its first estimates for this peer.</div>"#,
            ),
            Learning::NoLocation => banners.push_str(
                r#"<div class="learning">This peer's ring location is not known yet, so your node does not predict its results.</div>"#,
            ),
            Learning::Still(sources) => write!(
                banners,
                r#"<div class="learning">Still learning about this peer. Until it has handled more requests, {}.</div>"#,
                still_learning_sentence(sources)
            )
            .unwrap_or_default(),
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
    // The window's real size until it fills, so a young node is not told
    // about 500 requests it has not made.
    let window = if network_total < WINDOW_REQUESTS {
        format!("of your node's {network_total} requests so far (relayed ones included)")
    } else {
        format!("of your node's last {WINDOW_REQUESTS} requests (relayed ones included)")
    };
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
    let early = peer.is_some_and(|peer| peer.curves[0].early);
    let Some((seconds, alone)) = peer.and_then(|peer| lines_at_own_location(&peer.curves[0]))
    else {
        let drawn = peer.is_some_and(|peer| !peer.curves[0].distance_alone.is_empty());
        let why = if !view.snapshot.prediction_active {
            "Your node is not predicting yet."
        } else if peer.is_none() {
            "Your node cannot match this peer to a routing record."
        } else if !view.located {
            "Its ring location is not known yet, so your node does not predict it."
        } else if drawn {
            // Lines exist but neither has a usable value at distance 0.
            "Routing has no usable prediction at this peer's own location right now."
        } else {
            "Too few timed replies to predict it yet."
        };
        return tile("Response time", "&mdash;", why, "");
    };
    let comparison = if view.fallback {
        String::new()
    } else if early {
        format!("; an early estimate until your node has {MIN_TIMED_REPLIES} timed replies")
    } else {
        match learned.response_factor {
            Some(factor) if about_same(factor) => format!(
                "; your node expects it to reply about as fast as the {} distance alone predicts",
                fmt_time(alone)
            ),
            Some(factor) => format!(
                r#"; your node expects it to reply <span class="cmp">{}</span> than the {} distance alone predicts"#,
                time_ratio(factor),
                fmt_time(alone)
            ),
            None if learned.alike[1] => {
                // Short: the comparison card carries the full sentence.
                "; no measurable difference between peers yet".to_string()
            }
            None => "; too few timed replies yet to compare it with distance alone".to_string(),
        }
    };
    // What the line is depends on which estimate routing is using.
    let source = if view.fallback {
        "from the emergency fallback's estimate with this peer's own correction"
    } else if early {
        "from its early estimate with this peer's own correction"
    } else {
        "in a part of the ring this peer has no specific record in"
    };
    tile(
        "Response time",
        &fmt_time_tile(seconds),
        &format!("predicted for contracts near it{comparison}."),
        &format!(
            "Routing's prediction for a contract at this peer's location, {source}: the \
             start of the lines under Past requests."
        ),
    )
}

/// `(this peer, distance alone)` where the drawn response-time lines are at
/// distance 0: the tile's number and the one it is compared with, so the tile
/// reads exactly off the chart (and `this peer = distance alone × factor`).
/// `None` when either line has no usable point at distance 0 (a non-finite
/// value there is dropped from the line, so its first point may be further
/// out).
fn lines_at_own_location(curve: &RoutingCurve) -> Option<(f64, f64)> {
    let start = |line: &[(f64, f64)]| {
        line.iter()
            .find(|(distance, _)| *distance == 0.0)
            .filter(|(_, seconds)| seconds.is_finite() && *seconds > 0.0)
            .map(|&(_, seconds)| seconds)
    };
    let alone = start(&curve.distance_alone)?;
    let mine = if curve.this_peer.is_empty() {
        alone
    } else {
        start(&curve.this_peer)?
    };
    Some((mine, alone))
}

/// Recent routing decisions in which this peer was among the closest
/// candidates scored, and how many ranked it first.
fn chosen_tile(view: &RouterView) -> String {
    let title = format!(
        "Counted once your node ranks peers by prediction (after its first 50 requests), \
         including requests it relayed and retries. Eligible: among the closest peers your \
         node considered for a request. When the eligible count reaches {recent}, both counts \
         halve, so it stays between {half} and {top} and older decisions count for \
         geometrically less.",
        recent = SELECTION_RECENT_DECISIONS,
        half = SELECTION_RECENT_DECISIONS / 2,
        top = SELECTION_RECENT_DECISIONS - 1,
    );
    let empty = |text: &str| tile("Chosen", "&mdash;", text, &title);
    let Some(peer) = view.peer.as_ref() else {
        return empty("Your node cannot match this peer to a routing record.");
    };
    match peer.selection {
        Some(selection) if selection.eligible > 0 => {
            let sub = if selection.halved {
                "recent routing decisions it was eligible for ranked it first; older ones count for less."
                    .to_string()
            } else {
                format!(
                    "routing decisions it was eligible for ranked it first (all {} so far).",
                    selection.eligible
                )
            };
            tile(
                "Chosen",
                &format!(
                    "{} <small>of {}</small>",
                    fmt_count(selection.chosen),
                    selection.eligible
                ),
                &sub,
                &title,
            )
        }
        _ if !view.snapshot.prediction_active => {
            empty("Your node is not ranking peers by prediction yet.")
        }
        _ if peer.selection_evicted => empty(
            "Your node dropped this peer's counts to make room for others; they restart the next time it is eligible.",
        ),
        _ => empty("Not among the closest candidates for a routing decision yet."),
    }
}

/// A decayed count, which is fractional after a halving, as a whole number.
fn fmt_count(count: f64) -> String {
    format!("{}", count.round().max(0.0) as u64)
}

// ─── Compared with your other peers ─────────────────────────────────────────

fn compare_card(inputs: &PeerPageInputs<'_>, view: &RouterView) -> String {
    // On the fallback routing reads none of these differences, and the
    // weights that gate them describe a model it is not using: say so instead
    // of drawing readings with reasons that are not true.
    if view.fallback {
        return r#"<div class="card">
            <h2>Compared with your other peers</h2>
            <p class="empty">Routing is on its emergency fallback right now and is not using these comparisons with distance alone, so they are not shown.</p>
        </div>"#
            .to_string();
    }
    let this = inputs.peer.address.to_string();
    let dots = |stage: usize| -> (Vec<StripDot>, Option<StripDot>) {
        let mut others = Vec::new();
        let mut mine = None;
        for other in &view.others {
            let address = &other.address;
            let Some(offset) = other.offsets[stage] else {
                continue;
            };
            // The same gate as the tiles: a reading too thin to state is drawn
            // hollow, on the line, rather than as a difference. A response
            // time is placed at routing's expected factor, the tile's number.
            let factor = other.response_factor.filter(|_| known(Some(offset)));
            // Only a reason about THIS dot's peer; a stage that tells nobody
            // apart is not drawn as a strip at all (see below).
            let unstated = if stage == 0 {
                "too few requests to judge yet"
            } else {
                "too few timed replies to judge yet"
            };
            let (value, text, note) = match (stage, known(Some(offset)), factor) {
                (0, true, _) => (
                    offset.offset * 100.0,
                    failure_comparison(offset.offset),
                    None,
                ),
                (0, false, _) => (0.0, unstated.to_string(), Some(unstated)),
                (_, _, Some(factor)) => (
                    factor,
                    format!("expected to reply {}", time_comparison(factor, false)),
                    None,
                ),
                (_, _, None) => (1.0, unstated.to_string(), Some(unstated)),
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
                hollow: note.is_some(),
                note,
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

    // A stage with readings that tells no peers apart: one plain sentence
    // about all of them in the strip's place, not a row of identical dots.
    let no_difference =
        |stage: usize| format!(r#"<p class="chart-note">{}</p>"#, NO_DIFFERENCE[stage]);
    let failure_strip = !(fail_others.is_empty() && fail_mine.is_none()) && !stage_alike(view, 0);
    let time_strip = !(time_others.is_empty() && time_mine.is_none()) && !stage_alike(view, 1);
    let failure = if fail_others.is_empty() && fail_mine.is_none() {
        r#"<div class="empty-box">Too few requests across your peers to compare yet.</div>"#
            .to_string()
    } else if !failure_strip {
        no_difference(0)
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
    } else if !time_strip {
        no_difference(1)
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
    // `others` is drawn from `inputs.peers`, so `matched <= connected`; the
    // subtraction below saturates rather than trust that.
    // The dot caption and the failure unit describe strips, so each says only
    // what the strips actually drawn need.
    let failure_unit = if failure_strip {
        r#" <span class="unit">(percentage points)</span>"#
    } else {
        ""
    };
    let matched = view.others.len();
    let connected = inputs.peers.len();
    let whose = if matched == connected {
        format!("one of your {connected} peers")
    } else {
        format!(
            "one of {matched} of your {connected} peers (the other {} cannot be matched to a routing record)",
            connected.saturating_sub(matched)
        )
    };
    let too_few = match (failure_strip, time_strip) {
        (true, true) => "too few requests (for response time, timed replies)",
        (true, false) => "too few requests",
        (false, true) => "too few timed replies",
        (false, false) => "",
    };
    let caption = if failure_strip || time_strip {
        format!(
            r#"<p class="caption">Each dot is {whose}, placed by how your node expects its results to differ from what distance alone predicts. A peer with {too_few} to judge is drawn hollow, on the line.</p>"#
        )
    } else {
        String::new()
    };
    format!(
        r#"<div class="card">
            <h2>Compared with your other peers</h2>
            {caption}
            <div class="chart-title">Chance a request fails{failure_unit}</div>
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
    /// Why it is hollow, shown beside "this peer" so the reason is visible
    /// without hovering.
    note: Option<&'static str>,
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
    let placed = swarm(
        &others.iter().map(|dot| x(dot.value)).collect::<Vec<_>>(),
        (mid, band),
        (left, width - right),
        OTHER_DOT_R,
    );
    for (dot, (cx, cy)) in others.iter().zip(placed) {
        let circle = format!(
            r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="{OTHER_DOT_R}" class="{}">{}</circle>"#,
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
        let label = match dot.note {
            Some(note) => format!("this peer &middot; {note}"),
            None => "this peer".to_string(),
        };
        // Roughly 6.3 px a character at the label's size; kept inside the
        // chart, centred under the dot where it fits.
        let glyphs = label.replace("&middot;", "·").chars().count();
        let half = 3.2 * glyphs as f64;
        let label_x = cx.clamp(left + half, (width - right - half).max(left + half));
        write!(
            svg,
            r#"<circle cx="{cx:.1}" cy="{mid:.1}" r="6" class="{class}">{title}</circle><line x1="{cx:.1}" x2="{cx:.1}" y1="{:.1}" y2="{:.1}" class="tick-this"/><text x="{label_x:.1}" y="{label_row:.1}" text-anchor="middle" class="this-label">{label}</text>"#,
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

/// Radius of another peer's dot on a strip.
const OTHER_DOT_R: f64 = 2.6;

/// Most dot-widths a strip dot may move sideways from its value.
const SWARM_MAX_SHIFT: i32 = 2;

/// Place dots at the given x positions in a band `mid ± band`, each at the
/// first height (centre outward) where it overlaps no dot already placed, so
/// twenty peers that read alike spread into visible columns instead of one
/// blob. When a column is full a dot moves one dot-width sideways,
/// alternating right and left, at most [`SWARM_MAX_SHIFT`] dot-widths; past
/// that it is drawn at its own value and may overlap. Every dot stays inside
/// `x_range`.
fn swarm(xs: &[f64], (mid, band): (f64, f64), x_range: (f64, f64), r: f64) -> Vec<(f64, f64)> {
    let gap = 2.0 * r + 0.8;
    let rows = (band / gap).floor() as i32;
    let heights: Vec<f64> = std::iter::once(0)
        .chain((1..=rows).flat_map(|k| [k, -k]))
        .map(|k| mid + k as f64 * gap)
        .collect();
    let inside = |cx: f64| cx.clamp(x_range.0, x_range.1.max(x_range.0));
    let mut order: Vec<usize> = (0..xs.len()).collect();
    order.sort_by(|&a, &b| xs[a].total_cmp(&xs[b]));
    let mut placed: Vec<(f64, f64)> = Vec::with_capacity(xs.len());
    let mut out = vec![(0.0, 0.0); xs.len()];
    for index in order {
        let free = |cx: f64, cy: f64, placed: &[(f64, f64)]| {
            placed
                .iter()
                .all(|&(px, py)| (px - cx).hypot(py - cy) >= gap - 1e-9)
        };
        let mut spot = None;
        'search: for shift in 0..=2 * SWARM_MAX_SHIFT {
            // 0, +1, -1, +2, -2 dot-widths.
            let step = (shift + 1) / 2 * if shift % 2 == 0 { -1 } else { 1 };
            let cx = inside(xs[index] + f64::from(step) * gap);
            for &cy in &heights {
                if free(cx, cy, &placed) {
                    spot = Some((cx, cy));
                    break 'search;
                }
            }
        }
        let spot = spot.unwrap_or((inside(xs[index]), mid));
        placed.push(spot);
        out[index] = spot;
    }
    out
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
        let early_note = |early: bool, what: &str| {
            if early && !view.fallback {
                format!(
                    r#"<p class="chart-note">Fewer than {MIN_TIMED_REPLIES} {what} so far, so these lines are routing's early estimate.</p>"#
                )
            } else {
                String::new()
            }
        };
        let time_note = early_note(time_curve.early, "timed replies");
        // Only beside a chart: the empty state already says there is nothing.
        let speed_note = early_note(
            speed_curve.early && !my_speeds.is_empty(),
            "measured transfers",
        );
        let floored_note = if floored {
            r#"<p class="caption">Your node currently treats transfers through this peer as unusable and ranks it after every other eligible peer.</p>"#
        } else {
            ""
        };
        let panel_active = if index == 0 { " tab-panel-active" } else { "" };
        write!(
            panels,
            r#"<div class="tab-panel{panel_active}" id="panel-{id}">
                <div class="chart-title">Response time</div>{times}{time_note}
                <div class="chart-title">Transfer speed</div>{floored_note}{speeds}{speed_note}
            </div>"#,
        )
        .ok();
    }

    let dots = "Each dot is a reply to a request your node sent to a peer.";
    let caption = if view.fallback {
        format!(
            "{dots} Routing is on its emergency fallback; the lines are what it predicts with that."
        )
    } else if !rs.prediction_active {
        format!(
            "{dots} Your node is not predicting yet (it starts after 50 requests), so there are no prediction lines."
        )
    } else if view.peer.is_some() && !view.located {
        format!(
            "{dots} This peer's ring location is not known yet, so routing does not predict it."
        )
    } else if time_curve.early || view.peer.is_none() {
        // The response-time chart says why its lines are an early estimate.
        dots.to_string()
    } else {
        match learned.response_factor {
            Some(factor) if !about_same(factor) => format!(
                "{dots} Your node expects this peer to reply <b>{}</b> than distance alone predicts.",
                time_ratio(factor),
            ),
            Some(_) => format!(
                "{dots} Your node expects this peer to reply about as fast as distance alone predicts."
            ),
            // The comparison card says it once; the caption does not repeat it.
            None if learned.alike[1] => dots.to_string(),
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
            format!("Too few timed replies from this peer to judge its predictions yet; {across}.")
        }
        _ => "Too few timed replies to judge the predictions yet.".to_string(),
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
        <text x="{:.1}" y="{:.1}" class="halo">slower than predicted</text>
        <text x="{:.1}" y="{:.1}" text-anchor="end" class="halo">faster than predicted</text>
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
            "predicted for contracts near it; your node expects it to reply",
            "distance alone predicts",
            "Compared with your other peers",
            "Each dot is one of your 12 peers",
            "&larr; fewer failures",
            "more failures &rarr;",
            "Past requests by ring distance",
            "Your node expects this peer to reply",
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

    /// The factor the page states is the ratio of the two lines it draws at
    /// distance 0, for this peer's tile and for every dot on the strip, so the
    /// caption can never say "about as fast" while the lines part.
    #[test]
    fn stated_factor_is_the_ratio_of_the_drawn_lines() {
        let _fallback_off = crate::router::force_isotonic_fallback(false);
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let mut stated = 0;
        for index in 0..peers.len() {
            let inputs = inputs(&snaps, index);
            let view = RouterView::gather(&inputs, &router);
            let peer = view.peer.as_ref().unwrap();
            let curve = &peer.curves[0];
            assert!(!curve.early, "the trained router's timing stage is warm");
            let line = if curve.this_peer.is_empty() {
                &curve.distance_alone
            } else {
                &curve.this_peer
            };
            assert_eq!((line[0].0, curve.distance_alone[0].0), (0.0, 0.0));
            let ratio = line[0].1 / curve.distance_alone[0].1;
            let expected = peer.expected_response_factor.expect("warm, gate open");
            assert!(
                (expected - ratio).abs() < 1e-12,
                "peer {index}: {expected} vs {ratio}"
            );
            let strip = view
                .others
                .iter()
                .find(|other| other.address == snaps[index].address.to_string())
                .unwrap();
            assert_eq!(
                strip.response_factor,
                Some(expected),
                "the strip uses the same"
            );
            if let Some(factor) = Learned::of(&view).response_factor {
                assert_eq!(factor, expected);
                stated += 1;
            }
        }
        assert!(
            stated > 0,
            "some peer is known well enough to state a factor"
        );
    }

    /// The response tile's number is where the drawn "this peer" line starts,
    /// and the number it is compared with is where the distance-alone line
    /// starts, so tile = distance alone × the stated factor.
    #[test]
    fn the_response_tile_reads_off_the_drawn_lines() {
        let _fallback_off = crate::router::force_isotonic_fallback(false);
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let mut compared = 0;
        for index in 0..peers.len() {
            let inputs = inputs(&snaps, index);
            let view = RouterView::gather(&inputs, &router);
            let peer = view.peer.as_ref().unwrap();
            let (mine, alone) = lines_at_own_location(&peer.curves[0]).expect("lines");
            let line = if peer.curves[0].this_peer.is_empty() {
                &peer.curves[0].distance_alone
            } else {
                &peer.curves[0].this_peer
            };
            assert_eq!(mine, line[0].1);
            assert_eq!(alone, peer.curves[0].distance_alone[0].1);
            let factor = peer.expected_response_factor.unwrap();
            assert!(
                (mine - alone * factor).abs() <= 1e-12 * mine,
                "peer {index}"
            );
            let html = render_peer_page(&inputs, Some(&view));
            assert!(
                html.contains(&fmt_time_tile(mine)),
                "peer {index}: tile shows the line"
            );
            if Learned::of(&view).response_factor.is_some() {
                assert!(
                    html.contains(&format!(
                        "than the {} distance alone predicts",
                        fmt_time(alone)
                    )) || html.contains(&format!(
                        "as fast as the {} distance alone predicts",
                        fmt_time(alone)
                    )),
                    "peer {index}: the comparison names the distance-alone number"
                );
                compared += 1;
            }
        }
        assert!(compared > 0);
    }

    /// The tile reads the lines' points AT distance 0; a line whose value there
    /// was dropped gets the right reason, not "too few timed replies".
    #[test]
    fn the_tile_reads_distance_zero_or_says_it_has_none() {
        let curve = |alone: Vec<(f64, f64)>, mine: Vec<(f64, f64)>| RoutingCurve {
            distance_alone: alone,
            this_peer: mine,
            early: false,
        };
        assert_eq!(
            lines_at_own_location(&curve(vec![(0.0, 0.2), (0.5, 0.4)], vec![(0.0, 0.1)])),
            Some((0.1, 0.2))
        );
        assert_eq!(
            lines_at_own_location(&curve(vec![(0.01, 0.2), (0.5, 0.4)], Vec::new())),
            None,
            "a line starting past 0 has no value at the peer's own location"
        );
        let others = vec![[failure(0.0, 30.0, 0.7), timing(1.0, 30.0, 0.7), None]];
        let mut view = view_with([None, timing(1.0, 30.0, 0.7), None], others, None);
        view.peer.as_mut().unwrap().curves[0] = curve(vec![(0.01, 0.2), (0.5, 0.4)], Vec::new());
        let tile = response_tile(&view, &Learned::of(&view));
        assert!(tile.contains("no usable prediction at this peer"), "{tile}");
        assert!(!tile.contains("Too few timed replies"));
    }

    /// The tile's tooltip says which estimate the line comes from.
    #[test]
    fn the_tile_tooltip_names_the_estimate_in_use() {
        let others = vec![[failure(0.0, 30.0, 0.7), timing(1.0, 30.0, 0.7), None]];
        let mut view = view_with([None, timing(1.0, 30.0, 0.7), None], others, None);
        view.peer.as_mut().unwrap().curves[0] = RoutingCurve {
            distance_alone: vec![(0.0, 0.2), (0.5, 0.4)],
            this_peer: Vec::new(),
            early: false,
        };
        let warm = response_tile(&view, &Learned::of(&view));
        assert!(warm.contains("no specific record in"), "{warm}");
        view.peer.as_mut().unwrap().curves[0].early = true;
        let early = response_tile(&view, &Learned::of(&view));
        assert!(
            early.contains("from its early estimate with this peer"),
            "{early}"
        );
        assert!(!early.contains("no specific record in"));
        view.fallback = true;
        let fallback = response_tile(&view, &Learned::of(&view));
        assert!(fallback.contains("emergency fallback"), "{fallback}");
        assert!(!fallback.contains("no specific record in"));
    }

    /// A peer with a key but no ring location: routing predicts nothing, and
    /// the page says why rather than "too few replies".
    #[test]
    fn a_peer_without_a_ring_location_says_so() {
        let (router, peers) = trained_router();
        let mut snaps = snapshots(&peers);
        let mut unlocated = snapshot_of(&PeerKeyLocation::random());
        unlocated.peer_key_location = Some(PeerKeyLocation::with_unknown_addr(
            PeerKeyLocation::random().pub_key().clone(),
        ));
        unlocated.location = None;
        snaps.push(unlocated);
        let html = render(&router, &snaps, 12);
        for needle in [
            "This peer's ring location is not known yet, so your node does not predict its results.",
            "Its ring location is not known yet, so your node does not predict it.",
            "This peer's ring location is not known yet, so routing does not predict it.",
        ] {
            assert!(html.contains(needle), "missing {needle:?}");
        }
        assert!(
            !html.contains("too few timed replies yet") && !html.contains(r#"class="line-peer""#)
        );
    }

    /// The transfer chart's early note sits beside a chart, never beside the
    /// empty state.
    #[test]
    fn no_early_note_beside_an_empty_transfer_chart() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let inputs = inputs(&snaps, 3);
        let mut view = RouterView::gather(&inputs, &router);
        view.peer.as_mut().unwrap().curves[1].early = true;
        let html = render_peer_page(&inputs, Some(&view));
        assert!(html.contains("No data transfers measured through this peer yet."));
        assert!(!html.contains("measured transfers so far"));
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
            html.contains("too few timed replies yet to compare it with distance alone."),
            "routing predicts it from distance alone, and the tile says it cannot compare yet"
        );
        assert!(html.contains("Not among the closest candidates for a routing decision yet."));
        assert!(html.contains("Too few timed replies from this peer to judge its predictions yet"));
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
        assert_eq!((selection.eligible, selection.chosen), (40, 0.0));
        let snaps = snapshots(&peers);
        let html = render(&router, &snaps, 0);
        assert!(
            html.contains("Your node expects this peer to be slower and less reliable than distance alone predicts, and recently ranks it first far less often than its share."),
            "the banner names both conditions that fired"
        );
        assert!(!html.contains(STILL_LEARNING));
        assert!(html.contains("0 <small>of 40</small>"));
        assert!(
            html.contains("routing decisions it was eligible for ranked it first (all 40 so far).")
        );
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
            "Routing is on its emergency fallback right now and is not using these comparisons with distance alone, so they are not shown.",
        ] {
            assert!(fallback.contains(needle), "missing {needle:?}");
        }
        for absent in [
            AVOIDED,
            STILL_LEARNING,
            "expects this peer to reply",
            // The comparison card states no readings and no false reasons.
            "Each dot is one of",
            "too few timed replies to judge yet",
            "too few requests to judge yet",
            r#"class="dot-other"#,
        ] {
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
        // Routing's expected factor taken as the offset's own factor: these
        // tests are about the gates, not the expectation's arithmetic, which
        // `stated_factor_is_the_ratio_of_the_drawn_lines` pins.
        let factor = |offsets: [Option<PeerOffset>; 3]| offsets[1].map(|o| o.offset.exp());
        let other = |address: String, offsets: [Option<PeerOffset>; 3]| OtherPeer {
            address,
            offsets,
            response_factor: factor(offsets),
        };
        let router = Router::new(&[]);
        let mut view = RouterView {
            snapshot: router.snapshot(),
            peer: Some(router.peer_snapshot(&PeerKeyLocation::random())),
            others: others
                .into_iter()
                .enumerate()
                .map(|(i, offsets)| other(format!("10.0.0.{i}:1"), offsets))
                .collect(),
            window_outcomes: (0, 0),
            located: true,
            fallback: false,
        };
        view.snapshot.prediction_active = true;
        let peer = view.peer.as_mut().unwrap();
        peer.offsets = mine;
        peer.expected_response_factor = factor(mine);
        peer.selection = selection;
        view.others.push(other("this".to_string(), mine));
        view
    }

    #[test]
    fn still_learning_says_how_far_the_node_relies_on_distance_alone() {
        use Source::*;
        let known_other = [
            failure(0.02, 30.0, 0.7),
            timing(1.3, 30.0, 0.7),
            timing(0.8, 30.0, 0.7),
        ];
        let learning = |mine, others: Vec<[Option<PeerOffset>; 3]>| {
            Learned::of(&view_with(mine, others, None)).learning
        };
        // Nothing of this peer's record adopted on any warm stage.
        let none = [
            failure(0.0, 1.0, 0.0),
            timing(1.0, 0.0, 0.0),
            timing(1.0, 0.0, 0.0),
        ];
        assert_eq!(
            learning(none, vec![known_other]),
            Learning::Still([DistanceAlone; 3])
        );
        // Part of it adopted on one stage, and a cold stage: per stage.
        let mixed = [failure(0.0, 1.0, 0.2), timing(1.0, 1.0, 0.0), None];
        assert_eq!(
            learning(mixed, vec![known_other]),
            Learning::Still([Mostly, DistanceAlone, Early])
        );
        // The failure stage warm but the timing stages cold: routing corrects
        // their early estimate per peer, so they are not "distance alone".
        assert_eq!(
            learning([failure(0.0, 1.0, 0.0), None, None], vec![known_other]),
            Learning::Still([DistanceAlone, Early, Early])
        );
        // Exactly half adopted counts as known.
        assert_eq!(
            learning([failure(0.0, 9.0, 0.5), None, None], vec![known_other]),
            Learning::No
        );
        // Every stage warm and telling nobody apart: every peer is on the
        // line because they look alike. No banner.
        let alike = [
            failure(0.0, 40.0, 0.0),
            timing(1.0, 40.0, 0.0),
            timing(1.0, 40.0, 0.0),
        ];
        assert_eq!(learning(alike, vec![alike, alike]), Learning::No);
        // The same with the transfer stage cold (transfers are rare): the
        // warm stages still find every peer alike, so still no banner.
        let alike_cold = [failure(0.0, 40.0, 0.0), timing(1.0, 40.0, 0.0), None];
        assert_eq!(
            learning(alike_cold, vec![alike_cold, alike_cold]),
            Learning::No
        );
        // Nothing warm yet: early estimates throughout.
        assert_eq!(
            learning([None, None, None], vec![[None, None, None]]),
            Learning::FirstEstimates
        );
        // Before the 50-event gate routing predicts nothing at all.
        let mut gated = view_with(mixed, vec![known_other], None);
        gated.snapshot.prediction_active = false;
        assert_eq!(Learned::of(&gated).learning, Learning::NotPredicting);
        // A key with no ring location: routing cannot predict for it.
        let mut unlocated = view_with(mixed, vec![known_other], None);
        unlocated.located = false;
        assert_eq!(Learned::of(&unlocated).learning, Learning::NoLocation);
        // No key location, or the fallback: no banner.
        let mut keyless = view_with([None, None, None], vec![known_other], None);
        keyless.peer = None;
        assert_eq!(Learned::of(&keyless).learning, Learning::No);
        let mut fallback = view_with(mixed, vec![known_other], None);
        fallback.fallback = true;
        assert_eq!(Learned::of(&fallback).learning, Learning::No);
    }

    #[test]
    fn the_still_learning_sentence_groups_stages_by_source() {
        use Source::*;
        assert_eq!(
            still_learning_sentence([Early; 3]),
            "your node predicts its results from an early estimate"
        );
        assert_eq!(
            still_learning_sentence([DistanceAlone, Early, Early]),
            "your node predicts its failures from distance alone, and its response time and transfer speed from an early estimate"
        );
        assert_eq!(
            still_learning_sentence([Mostly, DistanceAlone, Early]),
            "your node predicts its failures mostly from distance alone, its response time from distance alone, and its transfer speed from an early estimate"
        );
    }

    /// The stated factor is routing's expectation, gated by the evidence: a
    /// peer whose expectation matches distance alone reads as "about as
    /// fast", and one with too little adopted states nothing.
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
        let selection = |eligible, chosen: u32, even_share| {
            Some(PeerSelection {
                eligible,
                chosen: f64::from(chosen),
                even_share,
                halved: false,
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
        view.others[0].address = snaps[0].address.to_string();
        view.others.last_mut().unwrap().address = snaps[1].address.to_string();
        let html = compare_card(&inputs, &view);
        assert!(html.contains("this peer &middot; too few requests to judge yet"));
        assert!(html.contains("this peer &middot; too few timed replies to judge yet"));
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

    /// A stage that gives no peer any weight tells nobody apart: plenty of
    /// evidence, but no factor. The tile says there is no measurable
    /// difference rather than "about as fast", and the comparison card says so
    /// once in each strip's place instead of drawing identical dots with the
    /// statement glued to "this peer".
    #[test]
    fn a_stage_that_tells_nobody_apart_states_no_factor() {
        let alike = [failure(0.0, 40.0, 0.0), timing(1.0, 40.0, 0.0), None];
        let view = view_with(alike, vec![alike, alike], None);
        let learned = Learned::of(&view);
        assert_eq!(learned.response_factor, None);
        assert_eq!(learned.alike, [true, true]);
        let snaps = [snapshot_of(&PeerKeyLocation::random())];
        let mut view = view;
        view.others.last_mut().unwrap().address = snaps[0].address.to_string();
        let peer = view.peer.as_mut().unwrap();
        peer.curves[0] = RoutingCurve {
            distance_alone: vec![(0.0, 0.2), (0.5, 0.4)],
            this_peer: Vec::new(),
            early: false,
        };
        let html = render_peer_page(&inputs(&snaps, 0), Some(&view));
        assert!(html.contains(
            "predicted for contracts near it; no measurable difference between peers yet."
        ));
        assert!(!html.contains("about as fast"), "no comparison is claimed");
        // Each full sentence appears once on the page, in the comparison card.
        for sentence in NO_DIFFERENCE {
            assert_eq!(html.matches(sentence).count(), 1, "{sentence:?}");
        }
        assert!(
            html.contains(r#"<p class="caption">Each dot is a reply to a request your node sent to a peer.</p>"#),
            "the Past requests caption adds nothing in this state"
        );
        let card = compare_card(&inputs(&snaps, 0), &view);
        assert!(!card.contains("<svg"), "no strips are drawn");
        assert!(
            !card.contains("Each dot is") && !card.contains("(percentage points)"),
            "no caption or unit for strips that are not there"
        );
        assert!(
            !card.contains("this peer"),
            "nothing about all peers rides on this peer"
        );
        assert!(!html.contains("tell peers apart") && !html.contains("tell its peers apart"));
    }

    /// Each distance chart names its own stage's early estimate: the
    /// transfer-speed stage warms on its own count of transfers.
    #[test]
    fn each_chart_names_its_own_early_estimate() {
        let (router, peers) = trained_router();
        let snaps = snapshots(&peers);
        let inputs = inputs(&snaps, 2);
        let mut view = RouterView::gather(&inputs, &router);
        let note = |what: &str| format!("Fewer than {MIN_TIMED_REPLIES} {what} so far");
        let html = render_peer_page(&inputs, Some(&view));
        assert!(
            !html.contains(&note("timed replies")) && !html.contains(&note("measured transfers"))
        );
        view.peer.as_mut().unwrap().curves[1].early = true;
        let html = render_peer_page(&inputs, Some(&view));
        assert!(
            html.contains(&note("measured transfers")),
            "the speed chart says so"
        );
        assert!(
            !html.contains(&note("timed replies")),
            "the time chart does not"
        );
    }

    /// Below the 50-event gate routing predicts nothing: no lines, and the
    /// page says why. The reliability tile counts the window's real size.
    #[test]
    fn before_the_prediction_gate_the_page_draws_no_lines() {
        let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
            crate::util::time_source::SharedMockTimeSource::new(),
        ));
        let peer = PeerKeyLocation::random();
        for round in 0..40u64 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location: Location::random(),
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(100 + round),
                    payload_size: 20_000,
                    payload_transfer_time: Duration::from_millis(50),
                },
                op_type: Some(OpType::Get),
            });
        }
        let snaps = vec![snapshot_of(&peer)];
        let html = render(&router, &snaps, 0);
        assert!(!html.contains(r#"class="line-net""#) && !html.contains(r#"class="line-peer""#));
        assert!(html.contains("Your node is not predicting yet (it starts after 50 requests)"));
        assert!(html.contains("Your node is not predicting yet: until it has seen 50 requests"));
        assert!(
            html.contains("of your node's 40 requests so far"),
            "not 'last 500' with only 40 in the window"
        );
    }

    /// The chosen tile tells apart a peer it cannot match, one whose counts
    /// were evicted, one never eligible, and counts that have halved.
    #[test]
    fn the_chosen_tile_says_why_it_has_no_counts() {
        let others = vec![[failure(0.0, 30.0, 0.7), timing(1.0, 30.0, 0.7), None]];
        let mine = [failure(0.0, 30.0, 0.7), timing(1.0, 30.0, 0.7), None];
        let never = view_with(mine, others.clone(), None);
        assert!(
            chosen_tile(&never)
                .contains("Not among the closest candidates for a routing decision yet.")
        );
        let mut evicted = view_with(mine, others.clone(), None);
        evicted.peer.as_mut().unwrap().selection_evicted = true;
        assert!(chosen_tile(&evicted).contains("dropped this peer's counts"));
        let mut keyless = view_with(mine, others.clone(), None);
        keyless.peer = None;
        assert!(chosen_tile(&keyless).contains("cannot match this peer"));
        let fresh = view_with(
            mine,
            others.clone(),
            Some(PeerSelection {
                eligible: 13,
                chosen: 3.0,
                even_share: 3.0,
                halved: false,
            }),
        );
        let tile = chosen_tile(&fresh);
        assert!(tile.contains("3 <small>of 13</small>") && tile.contains("(all 13 so far)"));
        let decayed = view_with(
            mine,
            others,
            Some(PeerSelection {
                eligible: 120,
                chosen: 2.5,
                even_share: 30.0,
                halved: true,
            }),
        );
        let tile = chosen_tile(&decayed);
        assert!(tile.contains("older ones count for less") && !tile.contains("so far"));
        assert!(tile.contains(&format!(
            "stays between {} and {}",
            SELECTION_RECENT_DECISIONS / 2,
            SELECTION_RECENT_DECISIONS - 1
        )));
    }

    #[test]
    fn the_still_learning_banners_render() {
        let others = vec![[
            failure(0.02, 30.0, 0.7),
            timing(1.3, 30.0, 0.7),
            timing(0.8, 30.0, 0.7),
        ]];
        let snaps = [snapshot_of(&PeerKeyLocation::random())];
        let mostly = view_with(
            [
                failure(0.0, 1.0, 0.2),
                timing(1.0, 1.0, 0.3),
                timing(1.0, 1.0, 0.1),
            ],
            others.clone(),
            None,
        );
        assert!(render_peer_page(&inputs(&snaps, 0), Some(&mostly)).contains(
            "Still learning about this peer. Until it has handled more requests, your node predicts its results mostly from distance alone."
        ));
        let mixed = view_with([failure(0.0, 1.0, 0.0), None, None], others, None);
        assert!(render_peer_page(&inputs(&snaps, 0), Some(&mixed)).contains(
            "your node predicts its failures from distance alone, and its response time and transfer speed from an early estimate."
        ));
    }

    /// Twenty peers that read alike spread out instead of stacking on one
    /// spot, none overlapping another.
    #[test]
    fn alike_peers_spread_into_a_swarm() {
        let xs = vec![100.0; 20];
        let placed = swarm(&xs, (40.0, 15.0), (34.0, 560.0), OTHER_DOT_R);
        for (i, a) in placed.iter().enumerate() {
            for b in &placed[i + 1..] {
                assert!(
                    (a.0 - b.0).hypot(a.1 - b.1) >= 2.0 * OTHER_DOT_R,
                    "{a:?} overlaps {b:?}"
                );
            }
            assert!((a.1 - 40.0).abs() <= 15.0 + 1e-9, "inside the band");
        }
        let spread: std::collections::HashSet<_> = placed
            .iter()
            .map(|&(x, y)| ((x * 10.0) as i64, (y * 10.0) as i64))
            .collect();
        assert_eq!(spread.len(), 20);
        assert_eq!(swarm(&[], (40.0, 15.0), (34.0, 560.0), OTHER_DOT_R), vec![]);
        // Many alike peers at the edge of the plot: none moves more than the
        // cap from its value, and none leaves the plot.
        let crowd = swarm(&[34.5; 60], (40.0, 15.0), (34.0, 560.0), OTHER_DOT_R);
        let gap = 2.0 * OTHER_DOT_R + 0.8;
        for &(x, _) in &crowd {
            assert!((34.0..=560.0).contains(&x), "{x} is off the plot");
            assert!(
                (x - 34.5).abs() <= f64::from(SWARM_MAX_SHIFT) * gap + 1e-9,
                "{x} moved too far from its value"
            );
        }
    }

    /// Only the stage that tells nobody apart is replaced; the other keeps
    /// its strip, and "this peer" carries only a reason about this peer.
    #[test]
    fn only_the_stage_without_differences_loses_its_strip() {
        let failure_alike = [failure(0.0, 40.0, 0.0), timing(1.4, 40.0, 0.7), None];
        let snaps = [
            snapshot_of(&PeerKeyLocation::random()),
            snapshot_of(&PeerKeyLocation::random()),
        ];
        let mut view = view_with(failure_alike, vec![failure_alike], None);
        view.others[0].address = snaps[0].address.to_string();
        view.others.last_mut().unwrap().address = snaps[1].address.to_string();
        let card = compare_card(&inputs(&snaps, 1), &view);
        assert!(card.contains(NO_DIFFERENCE[0]));
        assert!(!card.contains(NO_DIFFERENCE[1]));
        assert!(
            card.contains("Response time compared with distance alone"),
            "the time strip stays"
        );
        assert!(!card.contains("Chance a request fails compared with distance alone"));
        assert!(card.contains(">this peer</text>"));
        assert!(
            card.contains("Each dot is"),
            "a strip is drawn, so its caption is too"
        );
        assert!(
            card.contains("A peer with too few timed replies to judge is drawn hollow"),
            "the caption names only the drawn strip's reason"
        );
        assert!(
            !card.contains("(percentage points)"),
            "the failure strip is not"
        );
    }

    /// A stage with no record of any connected peer (all fresh or evicted)
    /// gives every one weight 0 too, but that is too few replies, not "no
    /// difference": the strips stay, hollow, with that reason.
    #[test]
    fn peers_without_records_are_too_few_not_alike() {
        let fresh = [failure(0.0, 0.0, 0.0), timing(1.0, 0.0, 0.0), None];
        let snaps = [
            snapshot_of(&PeerKeyLocation::random()),
            snapshot_of(&PeerKeyLocation::random()),
        ];
        let mut view = view_with(fresh, vec![fresh], None);
        view.others[0].address = snaps[0].address.to_string();
        view.others.last_mut().unwrap().address = snaps[1].address.to_string();
        view.peer.as_mut().unwrap().curves[0] = RoutingCurve {
            distance_alone: vec![(0.0, 0.2), (0.5, 0.4)],
            this_peer: Vec::new(),
            early: false,
        };
        let learned = Learned::of(&view);
        assert_eq!(learned.alike, [false, false]);
        let html = render_peer_page(&inputs(&snaps, 1), Some(&view));
        for sentence in NO_DIFFERENCE {
            assert!(!html.contains(sentence), "claims {sentence:?}");
        }
        assert!(!html.contains("no measurable difference"));
        assert!(html.contains("too few timed replies yet to compare it with distance alone"));
        assert!(html.contains("this peer &middot; too few requests to judge yet"));
        assert!(html.contains("this peer &middot; too few timed replies to judge yet"));
        assert!(html.contains("Still learning about this peer"));
        // One peer with a record but no weight makes the stage alike.
        let measured = [failure(0.0, 25.0, 0.0), timing(1.0, 25.0, 0.0), None];
        let mut view = view_with(fresh, vec![measured], None);
        view.others[0].address = snaps[0].address.to_string();
        assert_eq!(Learned::of(&view).alike, [true, true]);
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
