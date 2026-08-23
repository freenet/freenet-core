use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::*;
use crate::conformance::property::Severity;
use crate::conformance::status;
use freenet_stdlib::prelude::ContractInstanceId;
use std::str::FromStr;

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
    // Two separate reads, deliberately not collapsed into one: `is_enabled()`
    // and `snapshot()` answer different questions, and can disagree during
    // the checker's very first interval — enabled, but nothing published
    // yet. That state must render as "not checked recently", not as "off",
    // so both flow into the pure renderer below rather than being merged
    // here into a single bool.
    contract_detail_html_from(
        &network_status::get_snapshot(),
        key_str,
        status::is_enabled(),
        status::snapshot().as_ref(),
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
/// state. `merge_enabled` and `merge_status` are the wrapper's two reads of
/// `conformance::status` (see the doc comment at the call site above for why
/// they stay separate parameters).
pub fn contract_detail_html_from(
    snap: &Option<network_status::NetworkStatusSnapshot>,
    key_str: &str,
    merge_enabled: bool,
    merge_status: Option<&status::MergeCheckStatus>,
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

    if subscribed.is_none() && hosted.is_none() && governance.is_none() {
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

    // `ContractKey`'s `Display` and `ContractInstanceId`'s `Display` produce
    // the same string (see the comment on the `governance` lookup above), so
    // `key_str` — whichever form the operator pasted — is always a valid
    // instance id to query the merge-check status by, when it parses at all.
    // A key from a URL path can be anything, so a parse failure here means
    // "not a real instance id" and must render as "not checked", never panic.
    let merge_id = ContractInstanceId::from_str(key_str).ok();
    let merge_card = merge_law_card(merge_enabled, merge_status, merge_id.as_ref());

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
/// `contracts_without_verdict` — contracts the checker looked at but could
/// reach no verdict on for any case — is surfaced whenever non-zero,
/// independent of which of states 2-4 this particular contract is in: it is
/// fleet-wide information the operator needs regardless of this contract's
/// own status.
fn merge_law_card(
    merge_enabled: bool,
    merge_status: Option<&status::MergeCheckStatus>,
    merge_id: Option<&ContractInstanceId>,
) -> String {
    // Fleet-wide "some contracts could not be judged at all" note. Shown
    // whenever the checker has published a tick and the count is non-zero,
    // regardless of this contract's own state — see the doc comment above.
    let unjudged_note = |s: &status::MergeCheckStatus| -> String {
        if s.contracts_without_verdict == 0 {
            String::new()
        } else {
            format!(
                r#"<p class="empty" style="margin-top:0.5rem">The checker could not reach a verdict on {n} contract(s) on this node — every case it tried was inconclusive, or related state exceeded its budget. Those contracts are neither clean nor flagged; they simply were not judged.</p>"#,
                n = s.contracts_without_verdict,
            )
        }
    };

    if !merge_enabled {
        return r#"<div class="card">
                <h2>Merge laws</h2>
                <p class="empty">Merge-law checking is not enabled on this node — that is the default. No merge-law information is available here, which is not the same as this contract being clean.</p>
            </div>"#
            .to_string();
    }

    let was_checked = match (merge_status, merge_id) {
        (Some(s), Some(id)) => s.was_checked(id),
        _ => false,
    };

    if !was_checked {
        let note = merge_status.map(unjudged_note).unwrap_or_default();
        return format!(
            r#"<div class="card">
                <h2>Merge laws</h2>
                <p class="empty">This contract has not been checked recently. The checked window is bounded, so this is <strong>not</strong> a statement that the contract is fine — the checker has simply not looked at it lately.</p>
                {note}
            </div>"#,
            note = note,
        );
    }

    // `was_checked` only returns `true` when both are `Some` (see the match
    // above), so these cannot fail.
    let merge_status = merge_status.expect("was_checked implies a published status");
    let merge_id = merge_id.expect("was_checked implies a parsed contract id");
    let findings: Vec<&status::MergeFinding> = merge_status.findings_for(merge_id).collect();
    let note = unjudged_note(merge_status);

    if findings.is_empty() {
        format!(
            r#"<div class="card">
                <h2>Merge laws</h2>
                <div class="info-grid">
                    <div class="info-label">Result</div><div class="info-value"><span class="fresh-pill fresh-ok">no violation found</span></div>
                    <div class="info-label" title="How many cases the checker ran, fleet-wide, in its most recent tick — so a clean result here can be judged against how much looking actually happened.">Cases last tick</div><div class="info-value">{cases}</div>
                </div>
                <p class="empty" style="margin-top:0.75rem">No merge-law violation was found for this contract the last time it was checked.</p>
                {note}
            </div>"#,
            cases = merge_status.cases_last_tick,
            note = note,
        )
    } else {
        let mut rows = String::new();
        for f in &findings {
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
                <p class="empty" style="margin-top:0.75rem"><strong>Violation</strong> means the contract cannot converge: peers holding it disagree and retry indefinitely. <strong>Diagnostic</strong> is legal but wasteful — it never justifies removal on its own.</p>
                {note}
            </div>"#,
            rows = rows,
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
