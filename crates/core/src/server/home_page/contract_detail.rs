use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::*;

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
    contract_detail_html_from(&network_status::get_snapshot(), key_str)
}

/// The page as a pure function of the snapshot.
///
/// Split out so the rendering can be tested without a running node, which is
/// how every `cards.rs` builder is already shaped (`build_peers_card` takes
/// `&Option<NetworkStatusSnapshot>`). Reading the global directly would have
/// made the interesting cases — a hosted-only contract, one the governance
/// manager has flagged — reachable only by standing up a node and waiting for
/// it to reach that state.
pub fn contract_detail_html_from(
    snap: &Option<network_status::NetworkStatusSnapshot>,
    key_str: &str,
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
            for t in g.history.iter().take(10) {
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
        governance = governance_card,
    )
}

/// Shorten a key for display when neither card supplied a short form.
fn abbreviate(key: &str) -> String {
    if key.chars().count() <= 12 {
        return key.to_string();
    }
    let head: String = key.chars().take(12).collect();
    format!("{head}…")
}
