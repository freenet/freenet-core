use std::str::FromStr;

use freenet_stdlib::prelude::ContractInstanceId;

use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::*;
use crate::conformance::property::Severity;
use crate::conformance::status;

// ─── Contract detail page ────────────────────────────────────────────────────

/// One contract, gathered from the three places the dashboard already knows
/// about it.
///
/// The value here is the JOIN, not any new measurement. A contract's
/// subscription state lives in `snap.contracts`, its hosting and eviction
/// state in `snap.hosting.contracts`, and its governance scoring in
/// `snap.governance.contracts` — three different cards, two of which truncate
/// (the eviction table showed 20 rows of up to 2,256) and none of which can be
/// cross-referenced by eye, because they key on different strings: the
/// subscription and hosting views use the full `ContractKey` encoding while
/// governance uses `ContractInstanceId`.
///
/// Everything below is read from the already-materialised snapshot, and that
/// is a deliberate constraint rather than an oversight. The two most valuable
/// fields for this page — the local and downstream subscriber counts that are
/// invariant 3's PRIMARY eviction ranking key — are not in the snapshot at
/// all; they are computed transiently during the eviction sweep. Surfacing
/// them means a live read of `InterestManager`/`HostingCache` on an HTTP
/// request path, which every other dashboard page avoids, and which needs a
/// lock-contention check against hot-path PUT/GET/SUBSCRIBE work first.
/// Tracked as #5372 so the deferral stays visible instead of becoming a silent
/// scope cut; this page says plainly that the counts are missing rather than
/// implying the ranking is visible.
pub fn contract_detail_html(key_str: &str) -> String {
    // `ContractKey`'s `Display` and `ContractInstanceId`'s `Display` produce
    // the same string (see the comment on the `governance` lookup below), so
    // `key_str` — whichever form the operator pasted — is always a valid
    // instance id to query the merge-check status by, when it parses at all.
    // A key from a URL path can be anything, so a parse failure here means
    // "not a real instance id" and must render as "not checked", never panic.
    let merge_id = ContractInstanceId::from_str(key_str).ok();
    // Two separate reads, deliberately not collapsed into one: `is_enabled()`
    // and `view_for()` answer different questions, and can disagree during
    // the checker's very first interval — enabled, but nothing published
    // yet. That state must render as "not checked recently", not as "off",
    // so both flow into the pure renderer below rather than being merged
    // here into a single bool.
    //
    // A VIEW, not the whole snapshot: the checked window holds up to 256
    // records with their findings, and this page needs one of them. Cloning
    // the set on every render made the page's cost grow with the peer's
    // history for no reader benefit.
    contract_detail_html_from(
        &network_status::get_snapshot(),
        key_str,
        status::is_enabled(),
        status::view_for(merge_id.as_ref()).as_ref(),
    )
}

/// The page as a pure function of the snapshot.
///
/// Split out so the rendering can be tested without a running node, which is
/// how every `cards.rs` builder is already shaped (`build_peers_card` takes
/// `&Option<NetworkStatusSnapshot>`). Reading the global directly would have
/// made the interesting cases — a hosted-only contract, one the governance
/// manager has flagged, one the merge-law checker has or hasn't looked at —
/// reachable only by standing up a node and waiting for it to reach that
/// state. `merge_enabled` and `merge_view` are the wrapper's two reads of
/// `conformance::status` (see the doc comment at the call site above for why
/// they stay separate parameters).
pub fn contract_detail_html_from(
    snap: &Option<network_status::NetworkStatusSnapshot>,
    key_str: &str,
    merge_enabled: bool,
    merge_view: Option<&status::MergeCheckView>,
) -> String {
    // Match on the full key OR the instance id, so a link from any card works
    // and so an operator can paste either form.
    let subscribed = snap.as_ref().and_then(|s| {
        s.contracts
            .iter()
            .find(|c| c.key_full == key_str || c.instance_id == key_str)
    });
    let hosted = snap
        .as_ref()
        .and_then(|s| s.hosting.contracts.iter().find(|c| c.key_full == key_str));
    // Governance is keyed by `ContractInstanceId`. That sounds like it needs a
    // separate lookup value, and `ContractSnapshot` carries both `key_full` and
    // `instance_id` as if they differed — but they do not.
    // `impl Display for ContractKey` delegates to `self.instance`, and
    // `ContractKey::id()` returns `&self.instance`, so `key.to_string()` and
    // `key.id().to_string()` produce the SAME string. The requested key is
    // therefore always a valid governance lookup value, including for a
    // hosted-only contract that carries no `instance_id` field of its own.
    //
    // Worth stating because the rustdoc on `ContractSnapshot::instance_id`
    // asserts the opposite ("Distinct from `key_full` which carries the full
    // ContractKey encoding"), which is wrong and cost a wrong turn here.
    let governance = snap.as_ref().and_then(|s| {
        s.governance.contracts.iter().find(|c| {
            c.instance_id == key_str || Some(&c.instance_id) == subscribed.map(|x| &x.instance_id)
        })
    });

    // The merge-law window is a FOURTH place this node knows about a contract, and it
    // outlives the other three. The checked window holds ~32 hours (256 records at two
    // per fifteen-minute tick) while the hosting cache evicts under budget pressure
    // well inside that, and `network_status::get_snapshot()` returning `None` empties
    // all three at once. Without this, the page asserted "This node knows nothing
    // about <key>" while holding a recorded merge-law VIOLATION for it — the strongest
    // statement the checker ever makes, rendered as an absence. `merge_view` was
    // already computed at the call site and thrown away.
    let merge_record_known = merge_view.is_some_and(|v| v.contract.is_some());
    if subscribed.is_none() && hosted.is_none() && governance.is_none() && !merge_record_known {
        return format!(
            include_str!("assets/contract_not_found.html"),
            CSS = CSS,
            PEER_CSS = PEER_CSS,
            JS = JS,
            key = html_escape(key_str),
        );
    }

    let key_enc = html_escape(key_str);
    let short = subscribed
        .map(|c| c.key_short.clone())
        .or_else(|| hosted.map(|c| c.key_short.clone()))
        .unwrap_or_else(|| abbreviate(key_str));

    let identity_card = format!(
        r#"<div class="card">
            <h2>Identity</h2>
            <div><code>{short}</code><button type="button" class="copy-btn-inline" data-addr="{key_enc}" onclick="copyToClipboard(this.getAttribute('data-addr')).then(function(){{showToast('Contract key copied')}})" title="Copy contract key">⎘</button></div>
            <div class="info-grid">
                <div class="info-label">Key</div><div class="info-value"><code class="key-wrap">{key_enc}</code></div>
                {instance_row}
            </div>
        </div>"#,
        short = html_escape(&short),
        key_enc = key_enc,
        instance_row = subscribed
            .map(|c| format!(
                r#"<div class="info-label" title="The 32-byte content-hash portion of the key. Governance scores are keyed by this, not by the full key — which is why the two tables could not be cross-referenced by eye.">Instance id</div><div class="info-value"><code class="key-wrap">{}</code></div>"#,
                html_escape(&c.instance_id)
            ))
            .unwrap_or_default(),
    );

    let subscription_card = match subscribed {
        Some(c) => format!(
            r#"<div class="card">
                <h2>Subscription</h2>
                <div class="info-grid">
                    <div class="info-label">Freshness</div><div class="info-value">{fresh}</div>
                    <div class="info-label" title="Whether real demand pins the contract: a local client subscription or a registered downstream subscriber.">In use</div><div class="info-value">{in_use}</div>
                    <div class="info-label">Subscribed for</div><div class="info-value">{sub}</div>
                    <div class="info-label">Last update</div><div class="info-value">{last}</div>
                </div>
            </div>"#,
            fresh = if c.is_receiving_updates {
                r#"<span class="fresh-pill fresh-ok">receiving updates</span>"#
            } else {
                r#"<span class="fresh-pill fresh-stale">not receiving updates</span>"#
            },
            in_use = if c.in_use { "yes" } else { "no" },
            sub = format_duration(c.subscribed_secs),
            last = c
                .last_updated_secs
                .map(|s| format!("{} ago", format_duration(s)))
                .unwrap_or_else(|| "never".to_string()),
        ),
        None => r#"<div class="card">
                <h2>Subscription</h2>
                <p class="empty">This node holds no subscription for this contract. It may be hosted without one — a hosted copy with no client subscription still joins anti-entropy, so it is not automatically stale.</p>
            </div>"#
            .to_string(),
    };

    let hosting_card = match hosted {
        Some(h) => format!(
            r#"<div class="card">
                <h2>Hosting</h2>
                <div class="info-grid">
                    <div class="info-label">State size</div><div class="info-value">{size}</div>
                    <div class="info-label" title="GET and SUBSCRIBE accesses observed over this entry's residency in the cache. Reset when the entry is evicted and re-admitted.">Reads</div><div class="info-value">{reads}</div>
                    <div class="info-label" title="The eviction recency clock: a per-run sequence, higher is more recent. Reset by a real GET or PUT, and ALSO when the contract loses its last subscriber — the sweep deliberately gives a just-unsubscribed contract a grace period. It is therefore NOT purely a last-read time.">Recency</div><div class="info-value">{recency}</div>
                    <div class="info-label" title="Whether the over-budget sweep would consider this contract at all. A contract pinned by demand — a local client subscription or a registered downstream subscriber — is not a candidate.">Eviction candidate</div><div class="info-value">{eligible}</div>
                </div>
                <p class="empty" style="margin-top:0.75rem">The counts that actually decide eviction — local client subscriptions, then downstream subscribers — outrank recency and are <strong>not shown here</strong>: they are computed during the sweep and are not in the snapshot this page reads. See freenet/freenet-core#5372.</p>
            </div>"#,
            size = format_bytes(h.size_bytes),
            reads = h.read_count,
            eligible = if h.eviction_eligible {
                "yes"
            } else {
                "no — pinned by demand"
            },
            recency = if h.recency_seq == 0 {
                "never".to_string()
            } else {
                h.recency_seq.to_string()
            },
        ),
        None => r#"<div class="card">
                <h2>Hosting</h2>
                <p class="empty">Not in this node's hosting cache.</p>
            </div>"#
            .to_string(),
    };

    let governance_card = match governance {
        Some(g) => {
            let mut history = String::new();
            /* Newest FIRST, and taken from the TAIL. `GovernanceSnapshot`
               documents this vec as "State history (bounded). Newest last", so
               a plain `.take(10)` shows the ten OLDEST transitions and hides
               every recent one — the opposite of what an operator opening this
               page wants, and it degrades as the contract accumulates history.
               The cap also preserves `FirstSeen` and drops older non-anchor
               entries, so the head is not even a stable window. */
            for t in g.history.iter().rev().take(MAX_HISTORY_ROWS) {
                history.push_str(&format!(
                    r#"<tr><td>{from} &rarr; {to}</td><td>{reason}</td><td class="right">{ago} ago</td></tr>"#,
                    from = html_escape(&format!("{:?}", t.from)),
                    to = html_escape(&format!("{:?}", t.to)),
                    reason = html_escape(&format!("{:?}", t.reason)),
                    ago = format_duration(t.secs_ago),
                ));
            }
            format!(
                r#"<div class="card">
                    <h2>Governance</h2>
                    <div class="info-grid">
                        <div class="info-label">State</div><div class="info-value">{state}</div>
                        <div class="info-label">Cost used</div><div class="info-value">{cost:.2}</div>
                        <div class="info-label">Benefit</div><div class="info-value">{benefit:.2}</div>
                        <div class="info-label">log-ratio</div><div class="info-value">{ratio}</div>
                        <div class="info-label">Age</div><div class="info-value">{age}</div>
                    </div>
                    {history_block}
                </div>"#,
                state = html_escape(&format!("{:?}", g.state)),
                cost = g.cost_used,
                benefit = g.benefit_score,
                ratio = g
                    .log_ratio
                    .map(|r| format!("{r:.3}"))
                    .unwrap_or_else(|| "—".to_string()),
                age = format_duration(g.age_secs),
                history_block = if history.is_empty() {
                    String::new()
                } else {
                    format!(
                        r#"<div class="table-wrap" style="margin-top:0.75rem"><table><thead><tr><th>Transition</th><th>Reason</th><th class="right">When</th></tr></thead><tbody>{history}</tbody></table></div>"#
                    )
                },
            )
        }
        None => r#"<div class="card">
                <h2>Governance</h2>
                <p class="empty">Not flagged by the governance manager. Only Borderline / WouldEvict / Evicted / Banned contracts appear in the snapshot — a normal contract being absent here is the expected case, not a gap.</p>
            </div>"#
            .to_string(),
    };

    let merge_card = merge_law_card(merge_enabled, merge_view);

    format!(
        include_str!("assets/contract.html"),
        CSS = CSS,
        PEER_CSS = PEER_CSS,
        JS = JS,
        key_short = html_escape(&short),
        version = env!("CARGO_PKG_VERSION"),
        identity = identity_card,
        subscription = subscription_card,
        hosting = hosting_card,
        merge = merge_card,
        governance = governance_card,
    )
}

/// The "Merge laws" card: what `conformance::status` — the shadow merge-law
/// checker's published snapshot — has established about THIS contract.
///
/// Four states, and getting them right without conflating any two of them is
/// the entire point of the card (see `conformance::status`'s module doc for
/// the production incident this is guarding against: an unjudgeable contract
/// rendering identically to a clean one). In order of how alarming they are
/// NOT:
///
/// 1. Checking isn't running on this node at all (`!merge_enabled`) — no
///    counts are shown, because a zero here would read as "clean" rather
///    than "unmeasured".
/// 2. Checking is running, but this contract is outside the bounded recently-
///    checked window (or the checker hasn't published its first tick yet) —
///    explicitly NOT reported as fine.
/// 3. Checked, no findings for this contract.
/// 4. Checked, with one or more findings, each carrying the property that
///    broke, its severity, and whether enforcement would remove it.
///
/// `without_verdict_last_tick` — focus contracts the checker formed no opinion
/// about in its most recent tick — is surfaced
/// whenever non-zero, independent of which of states 2-4 this particular
/// contract is in: it is node-wide information the operator needs regardless
/// of this contract's own status. It is phrased as a statement about that
/// tick, because that is what it measures; the earlier "on this node"
/// phrasing read as a standing property.
///
/// Its CAUSES are stated the way `shadow::ShadowReport::without_verdict` counts
/// them, which is broader than the wording it first shipped with. "Every case
/// was inconclusive, or related state exceeded its budget" named the two rarest
/// entries on a list that also holds: no contract store on this node, an empty
/// corpus, code that would not resolve, an oracle that would not build, setup
/// that exhausted the time budget before a single case ran, a probe task that
/// died, and `awaiting_samples` — for most of which ZERO cases were tried. On a
/// warming-up peer `awaiting_samples` dominates outright, so the old wording
/// described the least likely cause as though it were the only one.
///
/// Every state that has a snapshot at all — states 2, 3 and 4 — renders WHEN
/// the snapshot was published, including the no-record state, where it is the
/// only age there is. For a contract with a record it also renders when THAT
/// CONTRACT was last checked, a different and usually much older number. The
/// node-wide age is needed because a peer can stop publishing indefinitely
/// while the old snapshot stands (a probe task that panicked, a probe that hangs
/// so no later tick starts, a writer task that is gone), and without it that
/// peer keeps serving a week-old tick as a current result.
fn merge_law_card(merge_enabled: bool, merge_view: Option<&status::MergeCheckView>) -> String {
    if !merge_enabled {
        return r#"<div class="card">
                <h2>Merge laws</h2>
                <p class="empty">Merge-law checking is not enabled on this node — that is the default. No merge-law information is available here, which is not the same as this contract being clean.</p>
            </div>"#
            .to_string();
    }

    // Everything below needs a published snapshot. Without one the checker is
    // on but has not finished its first interval, which must read as "not
    // looked at yet" rather than as "off" or as "clean".
    let Some(view) = merge_view else {
        return r#"<div class="card">
                <h2>Merge laws</h2>
                <p class="empty">This contract has not been checked recently. The checked window is bounded, so this is <strong>not</strong> a statement that the contract is fine — the checker has simply not looked at it lately.</p>
            </div>"#
            .to_string();
    };

    // Node-wide "some contracts could not be judged at all" note, plus how old
    // the tick it describes is.
    let mut note = String::new();
    if view.without_verdict_last_tick > 0 {
        note.push_str(&format!(
            r#"<p class="empty" style="margin-top:0.5rem">In its most recent tick the checker judged {judged} contract(s) and could not reach a verdict on {n} — most often because there was nothing yet to check (no samples collected for them, or no contract code on this node), and sometimes because every case it did try came back inconclusive. Those contracts are neither clean nor flagged; they simply were not judged.</p>"#,
            judged = view.judged_last_tick,
            n = view.without_verdict_last_tick,
        ));
    }
    if view.stale {
        note.push_str(&format!(
            r#"<p class="empty" style="margin-top:0.5rem"><strong>The checker has not published for {ago}.</strong> It runs every 15 minutes when healthy, so everything on this card may be stale — a probe that died, a probe still running (no later tick starts while one is in flight, and a probe that stalls outside contract code has no deadline of its own), or a capture writer that is gone, each leaves the previous result standing with nothing else to say so. The node's log is where to look next.</p>"#,
            ago = format_duration(view.published_secs_ago),
        ));
    }

    let Some(record) = view.contract.as_ref() else {
        // The node-wide tick age is rendered HERE too, not only in the stale note.
        // "Not checked recently" has two very different explanations — a busy checker
        // that has not got round to this contract, and a checker that stopped — and
        // without the age the page cannot tell them apart at all below the staleness
        // threshold, which is where a checker that died fourteen minutes ago sits.
        // There is no per-contract age to show, because there is no record.
        return format!(
            r#"<div class="card">
                <h2>Merge laws</h2>
                <p class="empty">This contract has not been checked recently. The checked window is bounded, so this is <strong>not</strong> a statement that the contract is fine — the checker has simply not looked at it lately.</p>
                <div class="info-grid" style="margin-top:0.75rem">
                    <div class="info-label" title="How long ago the merge-law checker last published a tick on this NODE — about any contract, not this one, which it has no record of. It probes every 15 minutes when healthy.">Last checker tick</div><div class="info-value">{ago} ago</div>
                </div>
                {note}
            </div>"#,
            ago = format_duration(view.published_secs_ago),
            note = note,
        );
    };

    // THIS contract's own case counts, not the node's most recent tick. The
    // fleet-wide number cannot answer "how much looking did this contract
    // get?": a contract with one verdict and 199 inconclusive cases rendered
    // byte-identically to one with 200 verdicts, and a contract last probed
    // forty ticks ago was shown a count from a tick that never touched it.
    //
    // The per-contract age is the record's OWN `checked_at`, not the node's most
    // recent tick. Those are different clocks: the checker probes at most two
    // contracts per fifteen-minute tick against a window holding 256, so a contract
    // judged twenty hours ago sat beside "Last checker tick: 3 minutes" and a sentence
    // saying no violation was found "the last time it was checked". That is the same
    // misattribution already fixed one row up for the COUNTS (see
    // `status::MergeCheckStatus::judged_last_tick`), and both numbers are kept because
    // both are worth knowing — what was wrong was one of them answering the other's
    // question.
    let cases_row = format!(
        r#"<div class="info-label" title="Cases run against THIS contract while it has been in the checked window. A verdict is a case that came back Holds or Violated; an inconclusive case established nothing, so a clean result resting mostly on those is barely a result.">Cases for this contract</div><div class="info-value">{verdicts} reached a verdict, {inconclusive} inconclusive</div>
                    <div class="info-label" title="How long ago the tick that last judged THIS contract published. The checker probes at most two contracts per tick out of a window holding 256, so this can be far older than the node's last tick below.">This contract last checked</div><div class="info-value">{checked} ago</div>
                    <div class="info-label" title="How long ago the merge-law checker last published a tick on this NODE — about any contract, not necessarily this one. It probes every 15 minutes when healthy.">Last checker tick</div><div class="info-value">{ago} ago</div>"#,
        verdicts = record.verdicts,
        inconclusive = record.inconclusive,
        checked = view
            .checked_secs_ago
            .map(format_duration)
            // Unreachable: `checked_secs_ago` is `Some` exactly when `view.contract`
            // is, and this branch is inside `Some(record)`. Rendered as unknown rather
            // than as a zero, because "0 seconds ago" would be a confident claim that
            // this contract was checked just now.
            .unwrap_or_else(|| "an unknown time".to_string()),
        ago = format_duration(view.published_secs_ago),
    );

    if record.findings().is_empty() {
        format!(
            r#"<div class="card">
                <h2>Merge laws</h2>
                <div class="info-grid">
                    <div class="info-label">Result</div><div class="info-value"><span class="fresh-pill fresh-ok">no violation found</span></div>
                    {cases_row}
                </div>
                <p class="empty" style="margin-top:0.75rem">No merge-law violation was found for this contract the last time it was checked.</p>
                {note}
            </div>"#,
            cases_row = cases_row,
            note = note,
        )
    } else {
        let mut rows = String::new();
        for f in record.findings() {
            let (pill_class, label) = match f.severity {
                Severity::Violation => ("fresh-stale", "Violation — cannot converge"),
                Severity::Diagnostic => ("use-idle", "Diagnostic — legal but wasteful"),
            };
            rows.push_str(&format!(
                r#"<tr><td>{property}</td><td><span class="fresh-pill {pill_class}">{label}</span></td><td class="right">{would_remove}</td></tr>"#,
                property = html_escape(f.property),
                would_remove = if f.would_remove { "yes" } else { "no" },
            ));
        }
        format!(
            r#"<div class="card">
                <h2>Merge laws</h2>
                <div class="table-wrap"><table><thead><tr><th>Property</th><th>Severity</th><th class="right">Would remove</th></tr></thead><tbody>{rows}</tbody></table></div>
                <div class="info-grid" style="margin-top:0.75rem">
                    {cases_row}
                </div>
                <p class="empty" style="margin-top:0.75rem"><strong>Violation</strong> means the contract cannot converge: peers holding it disagree and retry indefinitely. <strong>Diagnostic</strong> is legal but wasteful — it never justifies removal on its own.</p>
                {note}
            </div>"#,
            rows = rows,
            cases_row = cases_row,
            note = note,
        )
    }
}

/// Governance transitions shown on the detail page.
///
/// A display truncation, not a safety threshold: the snapshot's history is
/// already bounded on the producing side. Ten is enough to show the shape of a
/// contract's recent trajectory without turning the panel into a log. Taken
/// from the TAIL — see the comment at the call site for why the direction
/// matters more than the count.
const MAX_HISTORY_ROWS: usize = 10;

/// Shorten a key for display when neither card supplied a short form.
fn abbreviate(key: &str) -> String {
    if key.chars().count() <= 12 {
        return key.to_string();
    }
    let head: String = key.chars().take(12).collect();
    format!("{head}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::capture;
    use crate::conformance::status::{CheckedContract, MergeFinding};

    /// The law this page reports when it has one to report.
    const PROPERTY: &str = "state_commutativity";

    /// The wrapper's two reads of the process-global merge status — and the one
    /// write anywhere in production that makes them mean anything.
    ///
    /// Every other test on this page drives [`contract_detail_html_from`], which
    /// takes both values as parameters, so the tested surface is not the
    /// production surface. [`contract_detail_html`] is what the HTTP route calls
    /// and the only caller of `status::is_enabled` and `status::view_for`: a
    /// refactor that dropped either read, or replaced it with a constant, changed
    /// nothing any test could see. The same "the parameterized function is what's
    /// tested, the wrapper is what runs" gap produced three blocking defects
    /// earlier in this change.
    ///
    /// **One test rather than four, deliberately.** `status`'s `ENABLED` is a
    /// `OnceLock` with no reset, so whichever test flips it decides what every
    /// later test in the same process sees — and `cargo test` runs the lib tests
    /// in ONE process while nextest (what CI runs) gives each its own. Split
    /// across tests, the "off" assertions would be deterministic in CI and
    /// order-dependent locally, which is the worst of both. Sequenced inside a
    /// single test, both halves are deterministic under either runner.
    ///
    /// This is therefore the ONLY place in the crate that reaches
    /// `status::mark_enabled`, directly or through [`capture::start`]. The
    /// pre-condition is asserted rather than assumed so that a second enabler
    /// added later fails HERE, with an explanation, instead of quietly making the
    /// "off" half of this test unable to fail.
    ///
    /// It doubles as the coverage for `capture::start`'s `mark_enabled()` call
    /// and for that call's POSITION. `start` must never claim checking is on for
    /// a capture that then failed to start, so the flag is checked after each of
    /// `start`'s two failure paths (no tokio runtime, unusable directory) as well
    /// as after the success path.
    #[tokio::test]
    async fn the_page_reports_what_the_status_global_says_and_capture_start_sets_it() {
        assert!(
            !status::is_enabled(),
            "merge-check status was already enabled before this test enabled it, so \
             something else in this test binary now reaches status::mark_enabled(). \
             The 'checking is off' half of this test can no longer fail; either make \
             that other test stop enabling the process-global, or fold its case in \
             here — do not delete this assertion"
        );

        let checked = ContractInstanceId::new([0x5a; 32]);
        let unchecked = ContractInstanceId::new([0x17; 32]);
        let mut record = CheckedContract::new(checked, 7, 2, tokio::time::Instant::now());
        record.note_finding(MergeFinding {
            contract: checked,
            property: PROPERTY,
            severity: Severity::Violation,
            would_remove: false,
        });
        status::publish([record], 1, 0, tokio::time::Instant::now());

        // No node is running, so `network_status::get_snapshot()` is `None` and
        // this contract has no subscription, no hosted copy and no governance
        // record. The published merge record is the ONLY thing that can keep the
        // page off the "this node knows nothing about it" branch — so rendering
        // anything else is proof the wrapper fetched it.
        let html = contract_detail_html(&checked.to_string());
        assert!(
            !html.contains("Contract Not Found"),
            "the page rendered as 'this node knows nothing about it' for a contract \
             with a published merge-law record, so the wrapper is not reading \
             status::view_for — got:\n{html}"
        );
        assert!(
            html.contains("not enabled on this node"),
            "checking is off in this process and the card did not say so, so the \
             wrapper is not reading status::is_enabled — got:\n{html}"
        );
        assert!(
            !html.contains(PROPERTY),
            "the card reported a finding while checking is off. A count or a finding \
             shown here reads as a measured result rather than as unmeasured, which \
             is the conflation conformance::status exists to prevent — got:\n{html}"
        );

        // …and the lookup uses the REQUESTED key, not merely "some record was
        // published". A contract outside the window still gets the absence page.
        let other = contract_detail_html(&unchecked.to_string());
        assert!(
            other.contains("Contract Not Found"),
            "a contract with no record of any kind rendered a detail page, so the \
             wrapper is not passing the requested key to status::view_for — \
             got:\n{other}"
        );

        // ── capture::start is what turns the flag on, and only once it cannot fail ──

        let root = tempfile::TempDir::new().expect("tempdir");

        // Failure path 1: no tokio runtime, so there is nothing to spawn the
        // writer on and `start` refuses before doing anything else.
        let no_runtime_dir = root.path().join("no-runtime");
        let refused = std::thread::spawn({
            let dir = no_runtime_dir.clone();
            move || capture::start(dir)
        })
        .join()
        .expect("the thread that called capture::start panicked");
        assert!(
            refused.is_err(),
            "capture::start must refuse outside a tokio runtime rather than start a \
             writer that can never run"
        );
        assert!(
            !status::is_enabled(),
            "capture::start marked checking enabled from a thread with no tokio \
             runtime, where it had already refused to start. mark_enabled() must \
             stay AFTER the runtime check — the dashboard would otherwise report a \
             capture that is not running"
        );

        // Failure path 2: inside a runtime, but the directory cannot be created —
        // its parent is a regular file.
        let blocker = root.path().join("not-a-directory");
        std::fs::write(&blocker, b"a file where a directory would have to be").expect("write");
        assert!(
            capture::start(blocker.join("capture")).is_err(),
            "capture::start must fail when its directory cannot be created"
        );
        assert!(
            !status::is_enabled(),
            "capture::start marked checking enabled after create_dir_all failed. \
             mark_enabled() must stay AFTER the fallible directory creation, or the \
             dashboard reports a capture that never started"
        );

        // The success path.
        let _handle = capture::start(root.path().join("capture"))
            .expect("capture must start against a fresh temp directory inside a runtime");
        assert!(
            status::is_enabled(),
            "capture::start did not mark the dashboard status enabled, so every \
             render before the checker's first tick says 'not enabled' for a node \
             that IS checking — absence and success must not look alike"
        );

        // Same contract, same published record, flag now on: the card must switch
        // from "not enabled" to the finding it has been holding all along.
        let enabled_html = contract_detail_html(&checked.to_string());
        assert!(
            !enabled_html.contains("not enabled on this node"),
            "the card still said checking is off after capture::start enabled it, so \
             the wrapper is not reading status::is_enabled — got:\n{enabled_html}"
        );
        assert_eq!(
            enabled_html.matches(PROPERTY).count(),
            1,
            "the card did not render the one finding on the published record, so the \
             wrapper's status::view_for result is not reaching the renderer — \
             got:\n{enabled_html}"
        );
    }
}
