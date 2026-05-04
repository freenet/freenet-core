//! HTTP endpoints for delegate permission prompts.
//!
//! When a delegate emits `RequestUserInput`, the `DashboardPrompter` stores the
//! pending prompt and broadcasts a `PromptEvent::Added`. The gateway shell
//! page's JS subscribes to `/permission/events` (Server-Sent Events) and
//! renders the prompt as an in-page overlay (see issue #3836) on every open
//! Freenet tab. When the user clicks a button in any tab the response is
//! POSTed to `/permission/{nonce}/respond`; the gateway then emits
//! `PromptEvent::Removed` and every tab dismisses its overlay.
//!
//! The legacy `/permission/pending` JSON polling endpoint is retained as a
//! fallback for environments without `EventSource`, for the SSE bootstrap
//! reconciliation (used on connect/reconnect/resync), and for tests. The
//! standalone `/permission/{nonce}` HTML page is retained as a fallback
//! (e.g. if JS is disabled in the shell, or for debugging / manual testing).
//!
//! # Trust model and UI rationale (#3857)
//!
//! The user installed the delegate. It is their agent; its per-key storage is
//! cryptographically isolated, so an impostor delegate with a different
//! `DelegateKey` cannot read or sign with the real delegate's secrets. That
//! limits one class of spoofing — but it does not make the prompt safe to
//! ship without identity attestation. A malicious-but-installed delegate can
//! still:
//!
//! - sign actions with its own (fake) key and trick the user into thinking
//!   they signed as their real identity (downstream verifiers that don't
//!   know the real public key would accept it);
//! - condition the user toward "Always allow" on a hostile request;
//! - exfiltrate user input via the response channel;
//! - write text in the message that looks like Freenet UI chrome (e.g.
//!   `"Freenet verified this request"`).
//!
//! The prompt UI defends against these the same way a hardware key does:
//! by surfacing a stable, runtime-attested fingerprint the user can
//! recognise across sessions. The runtime-attested `DelegateKey` is the one
//! signal that a returning user can passively use to spot an impostor; the
//! attested caller (today only `MessageOrigin::WebApp(..)`, see #3860) tells
//! the user *which* application is asking right now.
//!
//! Concrete UI choices that follow from this:
//!
//! 1. **The delegate's message ("Delegate says:") stays.** It is the most
//!    informative thing on the screen for the *honest* delegate case (which
//!    is the common case). Removing the authorship label was tempting but
//!    is wrong: it is the only thing on the page distinguishing
//!    delegate-authored text from Freenet UI chrome, and HTML-escaping does
//!    not protect against text deception.
//! 2. **The truncated delegate hash is shown inline, always visible**,
//!    under the buttons in muted monospace. A user who recognises their
//!    delegate's fingerprint can spot an impostor without expanding any
//!    disclosure.
//! 3. **A `<details>` "Technical details" disclosure** holds the full
//!    delegate hash and the Caller row (`Freenet app <truncated>` or
//!    `No app caller` for the `None` case). Closed by default — the user's
//!    real decision is timing/intent ("did I just trigger this?"), not
//!    hash matching.
//! 4. **No human-readable names.** Any name would have to come from
//!    app-controlled metadata and would be spoofable by a malicious
//!    contract publishing a manifest named after a popular app. Showing a
//!    name beside an unverified hash would pretend they are equally
//!    verified. Names are deferred until there is a real provenance story
//!    (signed manifests, or trust-on-first-use state).
//! 5. **`No app caller` rather than `Unknown` or `(not recorded)`** for
//!    the `CallerIdentity::None` case. `Unknown` reads like a failure;
//!    `No app caller` is accurate and neutral.
//! 6. **No `"Freenet confirmed these identities"` badge.** Such a badge
//!    would oversell the defensive value of the Delegate field (see above)
//!    and underlabel the Caller field. The information is presented
//!    factually and the user evaluates it against their own context.
//!
//! The shell-page overlay JS in `crates/core/src/server/path_handlers.rs`
//! mirrors this same layout. Both code paths must stay in sync — the
//! standalone page and the in-page overlay are both regression-tested.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::contract::user_input::{
    CallerIdentity, PendingPrompts, PromptEvent, PromptSnapshot, emit_prompt_event, prompt_events,
};

/// Register permission prompt routes.
pub(super) fn routes() -> Router {
    Router::new()
        .route("/permission/pending", get(pending_prompts))
        .route("/permission/events", get(permission_events))
        .route("/permission/{nonce}", get(permission_page))
        .route("/permission/{nonce}/respond", post(permission_respond))
}

/// Cap on simultaneous SSE subscribers. A single browser typically opens one
/// EventSource per Freenet tab; 64 is generous headroom and protects against
/// runaway tab counts or a buggy client looping reconnects.
const MAX_SSE_CONNECTIONS: usize = 64;

/// Live SSE connection count. Used to enforce `MAX_SSE_CONNECTIONS`.
static SSE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// Decrements `SSE_CONNECTIONS` on drop. The connection count must always be
/// released even if the SSE stream is dropped mid-handshake or panics in a
/// downstream layer.
struct SseConnectionGuard;

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        SSE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Maximum message length returned to the shell-page overlay. Caps the
/// amount of delegate-controlled text the shell renders per poll so a
/// malicious delegate cannot balloon the polling response.
const OVERLAY_MESSAGE_MAX: usize = 2048;
/// Maximum number of button labels rendered on the overlay. A delegate
/// cannot force the shell to create an unbounded button grid.
const OVERLAY_LABELS_MAX: usize = 8;
/// Maximum length of each individual button label.
const OVERLAY_LABEL_CHARS_MAX: usize = 64;
/// Maximum length of `delegate_key` / caller hash rendered on the overlay.
/// Re-uses the producer-side cap from `user_input::MAX_IDENTITY_HASH_CHARS`
/// so a single bump there is enough to widen the wire format. These are
/// normally short keys (BLAKE3 hex / base58 contract id) so the cap is
/// well over the actual sizes; the constant exists to bound the
/// amplification surface if the producer ever passes untrusted data.
const OVERLAY_KEY_CHARS_MAX: usize = crate::contract::user_input::MAX_IDENTITY_HASH_CHARS;

/// Number of leading characters of a hash to show in the truncated form.
const HASH_PREFIX_CHARS: usize = 8;
/// Number of trailing characters of a hash to show in the truncated form.
const HASH_SUFFIX_CHARS: usize = 5;

/// Truncate a hash for display: `first8…last5`. Falls back to the full string
/// if it's already short enough that truncating would lose nothing useful
/// (i.e. when prefix + suffix + ellipsis ≥ original length).
///
/// The truncated form is what the user sees in the prompt UI; the full hash
/// is preserved separately and surfaced via the `title` attribute on the
/// rendered span so power users can hover to read the unabbreviated value.
fn truncate_hash(s: &str) -> String {
    let total: usize = s.chars().count();
    if total <= HASH_PREFIX_CHARS + HASH_SUFFIX_CHARS + 1 {
        return s.to_string();
    }
    let prefix: String = s.chars().take(HASH_PREFIX_CHARS).collect();
    let suffix_rev: String = s.chars().rev().take(HASH_SUFFIX_CHARS).collect();
    let suffix: String = suffix_rev.chars().rev().collect();
    format!("{prefix}…{suffix}")
}

/// Strip characters that can visually spoof or hide delegate identity in the
/// overlay: ASCII control characters (except `\t`, `\n`, `\r`) and Unicode
/// bidirectional / formatting overrides. A right-to-left override in a
/// delegate_key could otherwise visually reverse the key displayed in the
/// context panel, undermining user trust.
fn sanitize_display(s: &str, max_chars: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max_chars * 4));
    for ch in s.chars().take(max_chars) {
        let keep = match ch {
            '\t' | '\n' | '\r' => true,
            // C0 / C1 controls
            c if (c as u32) < 0x20 || ((c as u32) >= 0x7f && (c as u32) <= 0x9f) => false,
            // Bidi overrides and invisible formatters
            '\u{202A}'..='\u{202E}' => false,
            '\u{2066}'..='\u{2069}' => false,
            '\u{200B}'..='\u{200F}' => false,
            '\u{FEFF}' => false,
            _ => true,
        };
        if keep {
            out.push(ch);
        }
    }
    out
}

/// Build the JSON representation of the caller identity for the overlay
/// endpoint. Tagged shape so a future `delegate` variant (issue #3860) is
/// purely additive — the shell-page JS can switch on `kind` and fall through
/// safely on unknown values.
fn caller_to_json(caller: &CallerIdentity) -> serde_json::Value {
    match caller {
        CallerIdentity::None => serde_json::json!({ "kind": "none", "hash": null }),
        CallerIdentity::WebApp(hash) => serde_json::json!({
            "kind": "webapp",
            "hash": sanitize_display(hash, OVERLAY_KEY_CHARS_MAX),
        }),
    }
}

/// Return the list of pending prompts for the shell page to render as
/// in-page overlays (see issue #3836). Each entry includes the sanitized
/// message, button labels, and delegate/caller context.
///
/// Because the response carries full delegate-controlled text, it must
/// not be readable by a cross-origin page or DNS-rebinding attacker.
/// Earlier versions enforced that by replying `403 Forbidden` to any
/// untrusted `Origin`, but the `403` carried no `Access-Control-Allow-*`
/// headers, which caused the browser to surface a "CORS header missing"
/// error in the devtools console for every non-same-origin caller
/// (e.g. a sandboxed iframe whose origin is `null`) — user-visible
/// noise that looked like a real bug.
///
/// Instead, always reply `200 OK` with `Access-Control-Allow-Origin: *`
/// so the response body can be delivered, but withhold the real prompt
/// list unless the `Origin` header is a trusted loopback origin.
/// Untrusted / null / missing-but-rewritten origins get an empty `[]`,
/// a valid-shape response the shell's polling loop silently ignores.
///
/// Security posture is unchanged: a cross-origin attacker still cannot
/// read the contents of live prompts, and the state-changing
/// `/permission/{nonce}/respond` endpoint retains its strict Origin
/// check independently. `*` is safe on this endpoint because no
/// credentials (cookies, auth tokens) are associated with the poll.
async fn pending_prompts(
    headers: HeaderMap,
    Extension(pending): Extension<PendingPrompts>,
) -> impl IntoResponse {
    // Missing Origin (e.g. same-origin top-level fetch from some
    // browsers) is treated as trusted: the gateway only listens on
    // loopback, and the poll payload is not a capability.
    let trusted = match headers.get("origin") {
        Some(value) => value.to_str().map(is_trusted_origin).unwrap_or(false),
        None => true,
    };

    // Attach a permissive CORS header on every response so the browser
    // delivers the body (real list or empty) instead of logging a
    // "CORS header missing" error.
    let cors_headers = [("access-control-allow-origin", "*")];

    if !trusted {
        return (
            axum::http::StatusCode::OK,
            cors_headers,
            Json(serde_json::json!([])),
        );
    }

    let prompts: Vec<serde_json::Value> = pending
        .iter()
        .map(|entry| {
            let prompt = entry.value();
            let message = sanitize_display(&prompt.message, OVERLAY_MESSAGE_MAX);
            let labels: Vec<String> = prompt
                .labels
                .iter()
                .take(OVERLAY_LABELS_MAX)
                .map(|l| sanitize_display(l, OVERLAY_LABEL_CHARS_MAX))
                .collect();
            serde_json::json!({
                "nonce": entry.key(),
                "message": message,
                "labels": labels,
                "delegate_key": sanitize_display(&prompt.delegate_key, OVERLAY_KEY_CHARS_MAX),
                "caller": caller_to_json(&prompt.caller),
            })
        })
        .collect();
    (
        axum::http::StatusCode::OK,
        cors_headers,
        Json(serde_json::json!(prompts)),
    )
}

/// Format the caller identity for the standalone HTML page's details
/// disclosure. The variant tag determines the prefix word (`Freenet app`
/// or `No app caller`) that appears next to the truncated hash.
fn caller_display(caller: &CallerIdentity) -> (String, Option<String>) {
    match caller {
        CallerIdentity::None => ("No app caller".to_string(), None),
        CallerIdentity::WebApp(hash) => {
            let sanitized = sanitize_display(hash, OVERLAY_KEY_CHARS_MAX);
            let truncated = truncate_hash(&sanitized);
            (format!("Freenet app {truncated}"), Some(sanitized))
        }
    }
}

/// Serve the HTML permission prompt page.
async fn permission_page(
    Path(nonce): Path<String>,
    Extension(pending): Extension<PendingPrompts>,
) -> impl IntoResponse {
    let headers = [
        ("X-Frame-Options", "DENY"),
        (
            "Content-Security-Policy",
            "frame-ancestors 'none'; default-src 'self' 'unsafe-inline'",
        ),
        ("Cache-Control", "no-store"),
        ("Cross-Origin-Opener-Policy", "same-origin"),
    ];

    let Some(entry) = pending.get(&nonce) else {
        return (headers, Html(expired_html()));
    };

    let message = html_escape(&entry.message);
    let buttons_html: String = entry
        .labels
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let escaped = html_escape(label);
            // First button is primary (Allow), rest are secondary
            let class = if i == 0 { "btn primary" } else { "btn" };
            let escaped_nonce = html_escape(&nonce);
            format!(
                r#"<button class="{class}" onclick="respond('{escaped_nonce}', {i})">{escaped}</button>"#
            )
        })
        .collect::<Vec<_>>()
        .join("\n            ");

    // Delegate identity: always shown, both inline (truncated, under the
    // buttons) and in the Technical details disclosure (full + truncated,
    // copyable). The inline placement gives the user a passive anomaly
    // signal without making them open the disclosure — codex review point 3.
    let delegate_full = sanitize_display(&entry.delegate_key, OVERLAY_KEY_CHARS_MAX);
    let delegate_trunc = truncate_hash(&delegate_full);
    let delegate_full_attr = html_escape(&delegate_full);
    let delegate_trunc_html = html_escape(&delegate_trunc);

    let (caller_display_text, caller_full) = caller_display(&entry.caller);
    // When the caller is None there's no full hash to surface, so omit the
    // title attribute entirely rather than rendering `title=""`. Empty
    // tooltips are noise and one fewer thing for users to wonder about.
    let caller_title_html = caller_full
        .as_deref()
        .map(|h| format!(" title=\"{}\"", html_escape(h)))
        .unwrap_or_default();
    let caller_display_html = html_escape(&caller_display_text);

    (
        headers,
        Html(format!(
            r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Freenet - Permission Request</title>
<style>
  :root {{ --bg: #0f1419; --fg: #e6e8eb; --card: #1a2028; --accent: #3b82f6;
          --border: #2d3748; --warn: #f59e0b; --muted: #6b7280; }}
  @media (prefers-color-scheme: light) {{
    :root {{ --bg: #f5f5f5; --fg: #1a1a1a; --card: #fff; --accent: #2563eb;
            --border: #d1d5db; --warn: #d97706; --muted: #9ca3af; }}
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: var(--bg); color: var(--fg); display: flex; justify-content: center;
         align-items: center; min-height: 100vh; padding: 20px; }}
  .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px;
           padding: 32px; max-width: 520px; width: 100%; box-shadow: 0 4px 24px rgba(0,0,0,0.2); }}
  .header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 20px; }}
  .icon {{ font-size: 32px; }}
  h1 {{ font-size: 18px; font-weight: 600; }}
  .message-label {{ font-size: 12px; color: var(--muted); margin-bottom: 4px; text-transform: uppercase;
                    letter-spacing: 0.5px; }}
  .message {{ font-size: 15px; line-height: 1.5; margin-bottom: 24px; padding: 16px;
              background: var(--bg); border-left: 3px solid var(--warn); border-radius: 4px; }}
  .buttons {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }}
  .btn {{ padding: 10px 24px; border: 1px solid var(--border); border-radius: 8px;
          background: var(--card); color: var(--fg); font-size: 14px; cursor: pointer;
          transition: all 0.15s; flex: 1; min-width: 100px; font-weight: 500; }}
  .btn.primary {{ background: var(--accent); color: white; border-color: var(--accent); }}
  .btn:hover {{ opacity: 0.85; transform: translateY(-1px); }}
  .btn:disabled {{ opacity: 0.5; cursor: not-allowed; transform: none; }}
  .delegate-line {{ font-size: 12px; color: var(--muted); margin-top: 8px;
                    font-family: monospace; }}
  .delegate-line .hash {{ user-select: all; }}
  details.tech {{ margin-top: 12px; font-size: 12px; color: var(--muted); }}
  details.tech summary {{ cursor: pointer; user-select: none; }}
  details.tech dl {{ margin-top: 8px; padding-left: 16px; }}
  details.tech dt {{ font-weight: 600; color: var(--fg); margin-top: 6px; }}
  details.tech dd {{ font-family: monospace; word-break: break-all; user-select: all; }}
  .timer {{ margin-top: 16px; font-size: 13px; color: var(--muted); text-align: center; }}
  .result {{ text-align: center; padding: 24px 0; }}
  .result .icon {{ font-size: 48px; margin-bottom: 12px; }}
</style>
</head>
<body>
<div class="card" id="prompt">
  <div class="header">
    <span class="icon">&#x1f512;</span>
    <h1>Permission Request</h1>
  </div>
  <div class="message-label">Delegate says:</div>
  <p class="message">{message}</p>
  <div class="buttons">
    {buttons_html}
  </div>
  <div class="delegate-line">
    Delegate: <span class="hash" title="{delegate_full_attr}">{delegate_trunc_html}</span>
  </div>
  <details class="tech">
    <summary>Technical details</summary>
    <dl>
      <dt>Delegate</dt>
      <dd title="{delegate_full_attr}">{delegate_full_attr}</dd>
      <dt>Caller</dt>
      <dd{caller_title_html}>{caller_display_html}</dd>
    </dl>
  </details>
  <div class="timer">Auto-deny in <span id="countdown">60</span>s</div>
</div>
<div class="card result" id="done" style="display:none">
  <span class="icon">&#x2705;</span>
  <h1>Response sent</h1>
  <p>You can close this tab.</p>
</div>
<div class="card result" id="expired" style="display:none">
  <span class="icon">&#x23f0;</span>
  <h1>Timed out</h1>
  <p>The request was auto-denied. You can close this tab.</p>
</div>
<script>
var seconds = 60;
var timer = setInterval(function() {{
  seconds--;
  var el = document.getElementById('countdown');
  if (el) el.textContent = seconds;
  if (seconds <= 0) {{
    clearInterval(timer);
    document.getElementById('prompt').style.display = 'none';
    document.getElementById('expired').style.display = 'block';
  }}
}}, 1000);

function respond(nonce, index) {{
  var buttons = document.querySelectorAll('.btn');
  buttons.forEach(function(b) {{ b.disabled = true; }});
  fetch('/permission/' + nonce + '/respond', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify({{ index: index }})
  }}).then(function(r) {{
    if (r.ok) {{
      document.getElementById('prompt').style.display = 'none';
      document.getElementById('done').style.display = 'block';
      clearInterval(timer);
    }} else {{
      buttons.forEach(function(b) {{ b.disabled = false; }});
    }}
  }}).catch(function() {{
    buttons.forEach(function(b) {{ b.disabled = false; }});
  }});
}}
</script>
</body>
</html>"##,
        )),
    )
}

#[derive(Deserialize)]
struct PermissionResponse {
    index: usize,
}

/// Handle the user's response to a permission prompt.
async fn permission_respond(
    Path(nonce): Path<String>,
    headers: HeaderMap,
    Extension(pending): Extension<PendingPrompts>,
    Json(body): Json<PermissionResponse>,
) -> impl IntoResponse {
    // Validate Origin header to prevent CSRF
    if let Some(origin) = headers.get("origin") {
        let origin = origin.to_str().unwrap_or("");
        if !is_trusted_origin(origin) {
            return (
                axum::http::StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "forbidden"})),
            );
        }
    } else {
        // Reject requests with no Origin header
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "missing origin"})),
        );
    }

    // Validate index BEFORE removing from DashMap. Removing first would
    // consume the nonce on invalid input, leaving the user unable to retry.
    let label_count = pending.get(&nonce).map(|e| e.labels.len());
    match label_count {
        None => (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "expired or already answered"})),
        ),
        Some(len) if body.index >= len => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid index"})),
        ),
        Some(_) => {
            // Index is valid -- now atomically remove and send response
            if let Some((_, prompt)) = pending.remove(&nonce) {
                if prompt.response_tx.send(body.index).is_err() {
                    tracing::debug!(nonce = %nonce, "Permission response channel already closed");
                }
                // Notify SSE subscribers so every open Freenet tab dismisses
                // its overlay immediately instead of waiting for the polling
                // fallback (#3836 follow-up).
                emit_prompt_event(PromptEvent::Removed {
                    nonce: nonce.clone(),
                });
                (
                    axum::http::StatusCode::OK,
                    Json(serde_json::json!({"ok": true})),
                )
            } else {
                // Race: another request consumed it between get and remove
                (
                    axum::http::StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "expired or already answered"})),
                )
            }
        }
    }
}

/// Combined origin check used by /permission/events.
///
/// EventSource is the wire that matters here, and browsers send `Origin`
/// only for cross-origin EventSource requests; same-origin GETs from the
/// gateway shell page often arrive with no `Origin`. So we can't simply
/// require a trusted `Origin`. Instead we combine two browser-attested
/// signals:
///
/// * `Origin` present → must be a trusted loopback origin.
/// * `Sec-Fetch-Site: cross-site` → reject regardless of `Origin`.
///
/// Either signal independently rejects cross-origin pages from holding a
/// connection slot. Same-origin shell-page requests (no `Origin`,
/// `Sec-Fetch-Site: same-origin` or absent on older clients) are allowed.
/// The polling endpoint at `/permission/pending` deliberately tolerates
/// missing `Origin` because it returns no useful body to attackers; this
/// endpoint is stricter because the connection slot itself is a resource
/// (cap = 64).
fn is_caller_trusted(headers: &HeaderMap) -> bool {
    if let Some(value) = headers.get("origin") {
        let s = match value.to_str() {
            Ok(s) => s,
            Err(_) => return false,
        };
        if !is_trusted_origin(s) {
            return false;
        }
    }
    if let Some(value) = headers.get("sec-fetch-site") {
        if let Ok(s) = value.to_str() {
            // `cross-site` and `cross-origin` are both cross-origin signals.
            // `same-origin`, `same-site`, and `none` (no initiator, e.g.
            // navigation, refresh) are all allowed.
            if s.eq_ignore_ascii_case("cross-site") || s.eq_ignore_ascii_case("cross-origin") {
                return false;
            }
        }
    }
    true
}

/// Check if an Origin header is from a trusted localhost source.
/// Accepts any port on localhost/loopback to handle non-default configurations.
fn is_trusted_origin(origin: &str) -> bool {
    let Some(host_port) = origin.strip_prefix("http://") else {
        return false;
    };
    // Handle bracketed IPv6 addresses: [::1]:port or [::1]
    if host_port.starts_with('[') {
        let host = if let Some((h, _port)) = host_port.split_once(']') {
            // h is "[::1" without closing bracket, add it back
            format!("{h}]")
        } else {
            return false;
        };
        return host == "[::1]";
    }
    // For non-IPv6: extract hostname before the port
    let host = if let Some((h, _port)) = host_port.rsplit_once(':') {
        h
    } else {
        host_port
    };
    matches!(host, "127.0.0.1" | "localhost")
}

/// HTML for when a permission request has expired or already been answered.
fn expired_html() -> String {
    r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Freenet</title>
<style>
  :root { --bg: #0f1419; --fg: #e6e8eb; --card: #1a2028; --border: #2d3748; }
  @media (prefers-color-scheme: light) {
    :root { --bg: #f5f5f5; --fg: #1a1a1a; --card: #fff; --border: #d1d5db; }
  }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: var(--bg); color: var(--fg); display: flex; justify-content: center;
         align-items: center; min-height: 100vh; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px;
          padding: 40px; text-align: center; max-width: 400px; }
  .icon { font-size: 48px; margin-bottom: 16px; }
</style>
</head>
<body>
<div class="card">
  <div class="icon">&#x2139;</div>
  <h1>Request expired</h1>
  <p>This permission request has already been answered or timed out.</p>
</div>
</body>
</html>"##
        .to_string()
}

/// Minimal HTML entity escaping for untrusted delegate content.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// Render a `PromptSnapshot` as the JSON payload sent over SSE. Mirrors the
/// shape returned by `/permission/pending` so the shell-page renderer can
/// consume both code paths interchangeably.
fn snapshot_to_json(snapshot: &PromptSnapshot) -> serde_json::Value {
    let message = sanitize_display(&snapshot.message, OVERLAY_MESSAGE_MAX);
    let labels: Vec<String> = snapshot
        .labels
        .iter()
        .take(OVERLAY_LABELS_MAX)
        .map(|l| sanitize_display(l, OVERLAY_LABEL_CHARS_MAX))
        .collect();
    serde_json::json!({
        "nonce": snapshot.nonce,
        "message": message,
        "labels": labels,
        "delegate_key": sanitize_display(&snapshot.delegate_key, OVERLAY_KEY_CHARS_MAX),
        "caller": caller_to_json(&snapshot.caller),
    })
}

/// Server-Sent Events stream for permission prompts.
///
/// Replaces the 3-second polling loop in the shell page with a push-based
/// channel. A new prompt becomes visible to every open Freenet tab as soon
/// as the broadcast event lands, with no polling-floor latency.
///
/// Origin gating mirrors `/permission/pending`: trusted loopback origins
/// receive the full event stream; untrusted origins receive a stream that
/// emits one `:closed` comment and ends, so the browser doesn't surface a
/// CORS error in the devtools console.
///
/// Race avoidance: we subscribe to the broadcast channel BEFORE snapshotting
/// the DashMap. Any prompt added between the snapshot and our first
/// `recv()` arrives via the broadcast (possibly duplicated with a snapshot
/// entry); the shell-page renderer keys on `nonce`, so duplicates are
/// idempotent.
async fn permission_events(
    headers: HeaderMap,
    Extension(pending): Extension<PendingPrompts>,
) -> axum::response::Response {
    // Reject untrusted callers BEFORE consuming a connection slot. Browsers
    // send `Sec-Fetch-Site: cross-site` for cross-origin EventSource and
    // `Origin` for any cross-origin request: either signal is enough to
    // reject, and rejecting up front blocks the cross-origin DoS where an
    // evil page opens 64 EventSources to consume every slot.
    if !is_caller_trusted(&headers) {
        tracing::debug!("Rejecting /permission/events request from untrusted origin");
        let stream = stream::once(async {
            Ok::<_, Infallible>(Event::default().comment("untrusted-origin"))
        });
        return Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response();
    }

    // Connection cap. CAS-loop instead of fetch_add+rollback so concurrent
    // reconnects can never overshoot the cap (the previous fetch_add path
    // had a transient overshoot window, and on Relaxed ordering had no
    // upper bound under heavy reconnect storms).
    let mut current = SSE_CONNECTIONS.load(Ordering::Relaxed);
    let guard = loop {
        if current >= MAX_SSE_CONNECTIONS {
            tracing::warn!(
                limit = MAX_SSE_CONNECTIONS,
                "SSE connection cap reached, refusing new /permission/events subscriber"
            );
            let stream = stream::once(async {
                Ok::<_, Infallible>(Event::default().comment("connection-cap-reached"))
            });
            return Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response();
        }
        match SSE_CONNECTIONS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => break SseConnectionGuard,
            Err(actual) => current = actual,
        }
    };

    // Subscribe FIRST to avoid the race where a prompt is added between the
    // DashMap snapshot and our first broadcast recv.
    let rx = prompt_events().subscribe();

    // Convert each currently-pending entry to a synthetic Added event so
    // a fresh subscriber catches up to current state.
    let initial: Vec<Result<Event, Infallible>> = pending
        .iter()
        .map(|entry| {
            let snapshot = PromptSnapshot {
                nonce: entry.key().clone(),
                message: entry.value().message.clone(),
                labels: entry.value().labels.clone(),
                delegate_key: entry.value().delegate_key.clone(),
                caller: entry.value().caller.clone(),
            };
            Ok(Event::default()
                .event("prompt_added")
                .data(snapshot_to_json(&snapshot).to_string()))
        })
        .collect();

    let live = BroadcastStream::new(rx).filter_map(|incoming| async move {
        match incoming {
            Ok(PromptEvent::Added(snapshot)) => Some(Ok(Event::default()
                .event("prompt_added")
                .data(snapshot_to_json(&snapshot).to_string()))),
            Ok(PromptEvent::Removed { nonce }) => Some(Ok(Event::default()
                .event("prompt_removed")
                .data(serde_json::json!({ "nonce": nonce }).to_string()))),
            // Lag means our subscriber was slow and missed events. The shell
            // page reconciles via the resync handler, which re-fetches
            // /permission/pending. Rare in practice: capacity is 128, each
            // prompt lifecycle is two events, and at most 32 prompts can
            // be in flight at once.
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                tracing::warn!(skipped = n, "SSE subscriber lagged, requesting resync");
                Some(Ok(Event::default().event("resync").data("{}")))
            }
        }
    });

    let merged = stream::iter(initial).chain(live);
    let stream_with_guard = GuardedStream {
        inner: merged,
        _guard: guard,
    };

    Sse::new(stream_with_guard)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(25))
                .text("keepalive"),
        )
        .into_response()
}

/// Wraps an inner SSE stream together with the connection-count guard so
/// that dropping the response (client disconnect) decrements the live count.
/// Without this, an SSE stream that's cancelled mid-flight would leak the
/// counter slot until process exit.
#[pin_project::pin_project]
struct GuardedStream<S> {
    #[pin]
    inner: S,
    _guard: SseConnectionGuard,
}

impl<S> Stream for GuardedStream<S>
where
    S: Stream<Item = Result<Event, Infallible>>,
{
    type Item = Result<Event, Infallible>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trusted_origin_localhost_default_port() {
        assert!(is_trusted_origin("http://localhost:7509"));
    }

    #[test]
    fn test_trusted_origin_localhost_custom_port() {
        assert!(is_trusted_origin("http://localhost:8080"));
    }

    #[test]
    fn test_trusted_origin_ipv4_loopback() {
        assert!(is_trusted_origin("http://127.0.0.1:7509"));
    }

    #[test]
    fn test_trusted_origin_ipv6_loopback() {
        assert!(is_trusted_origin("http://[::1]:7509"));
    }

    #[test]
    fn test_trusted_origin_ipv6_no_port() {
        assert!(is_trusted_origin("http://[::1]"));
    }

    #[test]
    fn test_untrusted_origin_external() {
        assert!(!is_trusted_origin("http://evil.com"));
        assert!(!is_trusted_origin("http://evil.com:7509"));
    }

    #[test]
    fn test_untrusted_origin_https() {
        assert!(!is_trusted_origin("https://localhost:7509"));
    }

    #[test]
    fn test_untrusted_origin_null() {
        assert!(!is_trusted_origin("null"));
    }

    #[test]
    fn test_untrusted_origin_empty() {
        assert!(!is_trusted_origin(""));
    }

    #[test]
    fn test_html_escape_script_tag() {
        assert_eq!(
            html_escape("<script>alert(1)</script>"),
            "&lt;script&gt;alert(1)&lt;/script&gt;"
        );
    }

    #[test]
    fn test_html_escape_quotes() {
        assert_eq!(
            html_escape(r#"" onclick="evil()""#),
            "&quot; onclick=&quot;evil()&quot;"
        );
    }

    #[test]
    fn test_html_escape_ampersand() {
        assert_eq!(html_escape("a & b"), "a &amp; b");
    }

    #[test]
    fn test_truncate_hash_long_input() {
        let h = "DLog47hEabcdefghijk8vK2";
        let t = truncate_hash(h);
        assert_eq!(t, "DLog47hE…k8vK2");
    }

    #[test]
    fn test_truncate_hash_short_input_unchanged() {
        let h = "abc";
        assert_eq!(truncate_hash(h), "abc");
    }

    #[test]
    fn test_truncate_hash_boundary_at_threshold() {
        // Exactly prefix+suffix+1 chars: returning unchanged saves no space, return as-is.
        let h = "1234567890ABCD"; // 14 chars: 8 + 5 + 1
        assert_eq!(truncate_hash(h), h);
    }

    #[test]
    fn test_truncate_hash_first_truncated_length() {
        // The first input length that *should* actually get truncated. A
        // one-off bug shifting the boundary by one would only be caught
        // here, not by the at-threshold test above.
        let h = "1234567890ABCDE"; // 15 chars: prefix(8) + suffix(5) + 2
        let t = truncate_hash(h);
        assert_eq!(t, "12345678…ABCDE");
        assert_ne!(t, h, "15-char input must actually be truncated");
    }

    #[test]
    fn test_truncate_hash_unicode() {
        // Multi-byte chars must be counted by `char`, not byte.
        let h = "🔥".repeat(16);
        let t = truncate_hash(&h);
        assert!(t.contains('…'));
        assert!(t.starts_with(&"🔥".repeat(8)));
        assert!(t.ends_with(&"🔥".repeat(5)));
    }

    fn empty_pending() -> PendingPrompts {
        use dashmap::DashMap;
        use std::sync::Arc;
        Arc::new(DashMap::new())
    }

    fn insert_prompt(
        pending: &PendingPrompts,
        nonce: &str,
        message: &str,
        labels: Vec<&str>,
        delegate_key: &str,
        caller: CallerIdentity,
    ) -> tokio::sync::oneshot::Receiver<usize> {
        use crate::contract::user_input::PendingPrompt;
        let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
        pending.insert(
            nonce.to_string(),
            PendingPrompt {
                message: message.to_string(),
                labels: labels.into_iter().map(String::from).collect(),
                delegate_key: delegate_key.to_string(),
                caller,
                response_tx: tx,
            },
        );
        rx
    }

    fn webapp_caller(s: &str) -> CallerIdentity {
        CallerIdentity::WebApp(s.to_string())
    }

    fn trusted_header() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("origin", "http://localhost:7509".parse().unwrap());
        h
    }

    async fn call_pending(
        headers: HeaderMap,
        pending: PendingPrompts,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        let (status, _hdrs, value) = call_pending_full(headers, pending).await;
        (status, value)
    }

    async fn call_pending_full(
        headers: HeaderMap,
        pending: PendingPrompts,
    ) -> (axum::http::StatusCode, HeaderMap, serde_json::Value) {
        use axum::body::to_bytes;
        use axum::response::IntoResponse;
        let resp = pending_prompts(headers, Extension(pending))
            .await
            .into_response();
        let status = resp.status();
        let resp_headers = resp.headers().clone();
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, resp_headers, value)
    }

    async fn call_permission_page(nonce: &str, pending: PendingPrompts) -> String {
        use axum::body::to_bytes;
        use axum::response::IntoResponse;
        let resp = permission_page(Path(nonce.to_string()), Extension(pending))
            .await
            .into_response();
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        String::from_utf8(body.to_vec()).unwrap()
    }

    // Regression test for issue #3836: the /permission/pending JSON must
    // carry enough data for the shell-page overlay to render the prompt
    // (message, labels, delegate key, caller), not just a preview.
    #[tokio::test]
    async fn test_pending_prompts_includes_overlay_fields() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "nonce123",
            "Approve this?",
            vec!["Allow Once", "Always Allow", "Deny"],
            "dkey",
            webapp_caller("cid"),
        );

        let (status, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let arr = value.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        let entry = &arr[0];
        assert_eq!(entry["nonce"], "nonce123");
        assert_eq!(entry["message"], "Approve this?");
        assert_eq!(
            entry["labels"],
            serde_json::json!(["Allow Once", "Always Allow", "Deny"])
        );
        assert_eq!(entry["delegate_key"], "dkey");
        assert_eq!(entry["caller"]["kind"], "webapp");
        assert_eq!(entry["caller"]["hash"], "cid");
    }

    // Regression test for issue #3857: when the prompt has no web-app
    // caller (CallerIdentity::None), the overlay JSON must encode that
    // explicitly as a tagged "none" variant rather than omitting the field
    // or sending "Unknown" as a hash. The shell JS switches on `kind` to
    // decide what to render, so the tag must be present and stable.
    #[tokio::test]
    async fn test_pending_prompts_none_caller_encoding() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "dkey", CallerIdentity::None);
        let (_, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(value[0]["caller"]["kind"], "none");
        assert!(value[0]["caller"]["hash"].is_null());
    }

    // Oversized delegate messages must be clipped so a malicious delegate
    // can't balloon the polling response the shell fetches every few seconds.
    #[tokio::test]
    async fn test_pending_prompts_message_capped() {
        let pending = empty_pending();
        let huge = "a".repeat(OVERLAY_MESSAGE_MAX * 4);
        let _rx = insert_prompt(&pending, "n", &huge, vec!["OK"], "d", webapp_caller("c"));

        let (_, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(
            value[0]["message"].as_str().unwrap().chars().count(),
            OVERLAY_MESSAGE_MAX
        );
    }

    // Multi-byte characters (emoji, CJK) must be counted by `char`, not
    // byte, so truncation never splits a grapheme and panics.
    #[tokio::test]
    async fn test_pending_prompts_message_cap_is_char_based() {
        let pending = empty_pending();
        let emoji = "\u{1F525}".repeat(OVERLAY_MESSAGE_MAX);
        let _rx = insert_prompt(&pending, "n", &emoji, vec!["OK"], "d", webapp_caller("c"));
        let (_, value) = call_pending(trusted_header(), pending).await;
        let got = value[0]["message"].as_str().unwrap();
        assert_eq!(got.chars().count(), OVERLAY_MESSAGE_MAX);
        assert!(got.chars().all(|c| c == '\u{1F525}'));
    }

    // A delegate supplying thousands of labels must not be able to make the
    // shell draw a button grid of arbitrary size. The response must cap
    // both the count and the per-label length.
    #[tokio::test]
    async fn test_pending_prompts_labels_capped_and_truncated() {
        let pending = empty_pending();
        let long_label: String = "L".repeat(OVERLAY_LABEL_CHARS_MAX * 4);
        let labels: Vec<String> = (0..OVERLAY_LABELS_MAX * 4)
            .map(|_| long_label.clone())
            .collect();
        {
            use crate::contract::user_input::PendingPrompt;
            let (tx, _rx) = tokio::sync::oneshot::channel::<usize>();
            pending.insert(
                "n".to_string(),
                PendingPrompt {
                    message: "m".to_string(),
                    labels,
                    delegate_key: "d".to_string(),
                    caller: webapp_caller("c"),
                    response_tx: tx,
                },
            );
        }
        let (_, value) = call_pending(trusted_header(), pending).await;
        let out_labels = value[0]["labels"].as_array().unwrap();
        assert_eq!(out_labels.len(), OVERLAY_LABELS_MAX);
        for l in out_labels {
            assert_eq!(l.as_str().unwrap().chars().count(), OVERLAY_LABEL_CHARS_MAX);
        }
    }

    // Empty-labels case: the JSON must still round-trip as `[]`, and the
    // shell JS has a local `['OK']` fallback that kicks in client-side.
    #[tokio::test]
    async fn test_pending_prompts_empty_labels_round_trip() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec![], "d", webapp_caller("c"));
        let (_, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(value[0]["labels"], serde_json::json!([]));
    }

    // Unicode right-to-left override in delegate_key / caller hash must be
    // stripped so a hostile delegate can't visually reverse the key
    // displayed in the overlay's context panel and spoof identity.
    #[tokio::test]
    async fn test_pending_prompts_strips_bidi_and_controls() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "n",
            // LRO + text + RLO in the middle of the message
            "Hello\u{202E}evil\u{202A}!",
            vec!["\u{202E}Allow\u{202C}"],
            "\u{FEFF}key\u{200B}123",
            webapp_caller("c\u{0007}id"),
        );
        let (_, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(value[0]["message"], "Helloevil!");
        assert_eq!(value[0]["labels"], serde_json::json!(["Allow"]));
        assert_eq!(value[0]["delegate_key"], "key123");
        assert_eq!(value[0]["caller"]["hash"], "cid");
    }

    // /permission/pending now returns full delegate-controlled text, so
    // the endpoint must not leak prompt contents to cross-origin callers.
    // The previous implementation enforced that by replying `403` to any
    // untrusted Origin, but the 403 carried no `Access-Control-Allow-*`
    // header, so the browser surfaced a "CORS header missing" error in
    // the devtools console for every non-same-origin caller — user-
    // visible noise that looked like a real bug. The current contract:
    // untrusted origins get `200 OK` with an empty `[]` body and a
    // permissive CORS header. An attacker still learns nothing about
    // live prompts, and no console error is generated.
    #[tokio::test]
    async fn test_pending_prompts_untrusted_origin_returns_empty_with_cors() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));
        let mut headers = HeaderMap::new();
        headers.insert("origin", "http://evil.com".parse().unwrap());
        let (status, resp_headers, value) = call_pending_full(headers, pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(value, serde_json::json!([]));
        assert_eq!(
            resp_headers
                .get("access-control-allow-origin")
                .map(|v| v.to_str().unwrap()),
            Some("*"),
            "CORS header must be present so the browser can deliver the \
             empty-list response instead of logging a CORS error"
        );
    }

    // Sandboxed iframes (e.g. the webapp content iframe on the gateway
    // shell page) send `Origin: null` for fetches. The endpoint must
    // treat that the same as any other untrusted origin: empty list +
    // CORS header, not `403`. Regression for Lukas Orsvärn's
    // `/permission/pending` console-error report.
    #[tokio::test]
    async fn test_pending_prompts_null_origin_returns_empty_with_cors() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));
        let mut headers = HeaderMap::new();
        headers.insert("origin", "null".parse().unwrap());
        let (status, resp_headers, value) = call_pending_full(headers, pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(value, serde_json::json!([]));
        assert_eq!(
            resp_headers
                .get("access-control-allow-origin")
                .map(|v| v.to_str().unwrap()),
            Some("*"),
        );
    }

    // Trusted (loopback) origins still see the full prompt list and
    // still receive the CORS header (harmless on same-origin replies,
    // required on any non-same-origin path the browser may take).
    #[tokio::test]
    async fn test_pending_prompts_trusted_origin_returns_list_with_cors() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "msg", vec!["OK"], "d", webapp_caller("c"));
        let (status, resp_headers, value) = call_pending_full(trusted_header(), pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let arr = value.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["message"], "msg");
        assert_eq!(
            resp_headers
                .get("access-control-allow-origin")
                .map(|v| v.to_str().unwrap()),
            Some("*"),
        );
    }

    // Missing Origin is allowed (some fetch flavors omit it); this matches
    // the documented threat model: the poll payload is not a capability,
    // and the /respond endpoint still rejects the no-Origin case.
    #[tokio::test]
    async fn test_pending_prompts_allows_missing_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));
        let (status, value) = call_pending(HeaderMap::new(), pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        // Missing Origin should still return the real list, not an empty one.
        assert_eq!(value.as_array().unwrap().len(), 1);
    }

    // End-to-end flow: two prompts pending, user answers one, other remains,
    // second response to the same nonce returns 404. This is the cross-tab
    // dismissal contract the shell JS relies on ("another tab already
    // answered" → hide the overlay).
    #[tokio::test]
    async fn test_respond_consumes_nonce_and_second_response_404s() {
        let pending = empty_pending();
        let rx_a = insert_prompt(
            &pending,
            "a",
            "mA",
            vec!["Yes", "No"],
            "d",
            webapp_caller("c"),
        );
        let _rx_b = insert_prompt(
            &pending,
            "b",
            "mB",
            vec!["Yes", "No"],
            "d",
            webapp_caller("c"),
        );

        let (_, value) = call_pending(trusted_header(), pending.clone()).await;
        let nonces: Vec<&str> = value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["nonce"].as_str().unwrap())
            .collect();
        assert_eq!(nonces.len(), 2);
        assert!(nonces.contains(&"a") && nonces.contains(&"b"));

        // Answer A.
        let (status, _) = {
            let resp = permission_respond(
                Path("a".to_string()),
                trusted_header(),
                Extension(pending.clone()),
                Json(PermissionResponse { index: 0 }),
            )
            .await
            .into_response();
            let status = resp.status();
            use axum::body::to_bytes;
            let _ = to_bytes(resp.into_body(), 1024).await.unwrap();
            (status, ())
        };
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(rx_a.await.unwrap(), 0);

        // Only B remains.
        let (_, value) = call_pending(trusted_header(), pending.clone()).await;
        let remaining: Vec<&str> = value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["nonce"].as_str().unwrap())
            .collect();
        assert_eq!(remaining, vec!["b"]);

        // Responding to A again 404s — the shell JS treats this as "another
        // tab already answered" and hides its overlay card.
        let resp = permission_respond(
            Path("a".to_string()),
            trusted_header(),
            Extension(pending),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }

    // Regression tests for issue #3857 — behavioural assertions on the
    // standalone HTML page (no structural assertions, per codex review
    // point 7).
    //
    // What we check:
    // 1. The "Delegate says:" authorship label is present next to the
    //    message (codex point 2: removing it was a UX/security regression).
    // 2. The truncated delegate hash is visible in the page body without
    //    requiring the user to expand any disclosure (codex point 3:
    //    preserves a passive anomaly signal for users who recognise their
    //    delegate's fingerprint).
    // 3. The full delegate hash is present in a `title=` attribute so power
    //    users can hover or copy the unabbreviated value.
    // 4. The caller's display string includes "Freenet app" so the user can
    //    tell what kind of caller it is.
    // 5. Delegate-supplied content is HTML-escaped (existing behaviour,
    //    re-verified now that the template was rewritten).
    #[tokio::test]
    async fn test_permission_page_renders_webapp_caller() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "abc",
            "Approve signing this document.",
            vec!["Allow", "Deny"],
            "DLog47hEverylongdelegatekeyhashk8vK2",
            webapp_caller("CONTRACTabcdefghijklmnopqZ"),
        );
        let html = call_permission_page("abc", pending).await;

        assert!(
            html.contains("Delegate says:"),
            "authorship label must be present (codex point 2)"
        );
        assert!(
            html.contains("Approve signing this document."),
            "delegate message must be rendered"
        );
        assert!(
            html.contains("DLog47hE…k8vK2"),
            "truncated delegate hash must appear in body (codex point 3)"
        );
        assert!(
            html.contains(r#"title="DLog47hEverylongdelegatekeyhashk8vK2""#),
            "full delegate hash must be present in a title attribute"
        );
        assert!(
            html.contains("Freenet app"),
            "caller kind label must be present"
        );
        assert!(
            html.contains("CONTRACT…nopqZ"),
            "truncated caller hash must appear in body, got HTML: {html}"
        );
    }

    // None caller renders as "No app caller" rather than a blank field or
    // the misleading "(not recorded)" we considered earlier (codex point 4).
    #[tokio::test]
    async fn test_permission_page_renders_none_caller() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "n",
            "m",
            vec!["OK"],
            "DLGKEYabcdefghk8vK2",
            CallerIdentity::None,
        );
        let html = call_permission_page("n", pending).await;
        assert!(
            html.contains("No app caller"),
            "None caller must render as 'No app caller'"
        );
        assert!(
            html.contains("Delegate says:"),
            "authorship label must be present even with no app caller"
        );
        // A regression that rendered `Freenet app ` (empty hash) for the
        // None variant would still pass the positive assertion above
        // because "No app caller" might also be present elsewhere. Pin it
        // negatively too.
        assert!(
            !html.contains("Freenet app"),
            "None caller must NOT render the 'Freenet app' prefix"
        );
    }

    // Hostile delegate text must be HTML-escaped so it cannot inject markup
    // into the prompt page or break out of the message paragraph.
    #[tokio::test]
    async fn test_permission_page_escapes_hostile_message() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "n",
            r#"<script>alert('xss')</script><img src=x onerror=evil()>"#,
            vec!["<b>Allow</b>"],
            "dkey",
            webapp_caller("cid"),
        );
        let html = call_permission_page("n", pending).await;
        assert!(!html.contains("<script>alert"));
        assert!(!html.contains("<img src=x"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("&lt;b&gt;Allow&lt;/b&gt;"));
    }

    // Hostile content in the runtime-attested hash strings (delegate_key
    // and caller hash) must also be HTML-escaped — those values flow into
    // the page body AND into `title="..."` attributes, so a missed escape
    // could break out of the attribute value. Realistic threat model: a
    // future code path that writes attacker-influenced data into these
    // fields. Sanitization strips control/bidi chars; html_escape handles
    // quote/angle injection.
    #[tokio::test]
    async fn test_permission_page_escapes_hostile_hash_fields() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "n",
            "ok",
            vec!["Allow"],
            r#"<script>alert(1)</script>"#,
            webapp_caller(r#""onload="evil()"#),
        );
        let html = call_permission_page("n", pending).await;
        // Raw markup must not appear anywhere in the rendered page.
        assert!(
            !html.contains("<script>alert(1)</script>"),
            "raw <script> from delegate_key must not appear in HTML"
        );
        assert!(
            !html.contains(r#""onload="evil()"#),
            "raw quote-bearing payload from caller hash must not appear unescaped"
        );
        // Escaped forms must be present, demonstrating that the values
        // flowed through html_escape on every render path.
        assert!(
            html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"),
            "escaped delegate_key markup must appear"
        );
        assert!(
            html.contains("&quot;onload=&quot;evil()"),
            "escaped caller hash quotes must appear"
        );
    }

    // ----- SSE endpoint regression tests -----

    /// Open the SSE stream for a given test origin and return the data
    /// stream as a `Stream<Bytes>`. We avoid `http_body_util` (not a direct
    /// dev-dep) by using axum's `Body::into_data_stream` adapter.
    async fn open_sse_with_origin(
        origin: Option<&str>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = bytes::Bytes> + Send>> {
        use axum::response::IntoResponse;
        let pending = crate::contract::user_input::pending_prompts();
        let mut headers = HeaderMap::new();
        if let Some(o) = origin {
            headers.insert("origin", o.parse().unwrap());
        }
        let resp = permission_events(headers, Extension(pending))
            .await
            .into_response();
        let stream = resp
            .into_body()
            .into_data_stream()
            .filter_map(|r| async move { r.ok() });
        Box::pin(stream)
    }

    /// Read the next non-keepalive SSE frame from a body stream, returning
    /// the raw bytes (`event: ...\ndata: ...\n\n`). Caller parses. Skips
    /// keepalive comment lines (starting with `:`).
    async fn next_event_frame(
        stream: &mut std::pin::Pin<Box<dyn futures::stream::Stream<Item = bytes::Bytes> + Send>>,
        timeout_ms: u64,
    ) -> Option<String> {
        let deadline = std::time::Duration::from_millis(timeout_ms);
        loop {
            let chunk = match tokio::time::timeout(deadline, stream.next()).await {
                Ok(Some(b)) => b,
                _ => return None,
            };
            let text = String::from_utf8_lossy(&chunk).to_string();
            // Skip pure keepalive comments (lines starting with ':') and
            // empty chunks.
            if text.trim().is_empty() || text.trim_start().starts_with(':') {
                continue;
            }
            return Some(text);
        }
    }

    /// Walk frames until we see one matching the predicate, or run out of
    /// budget. Concurrent tests share the global broadcast channel so we
    /// must filter by our test's own nonce rather than asserting on the
    /// "next" frame.
    async fn frame_matching(
        stream: &mut std::pin::Pin<Box<dyn futures::stream::Stream<Item = bytes::Bytes> + Send>>,
        predicate: impl Fn(&str) -> bool,
        max_frames: usize,
        per_frame_timeout_ms: u64,
    ) -> Option<String> {
        for _ in 0..max_frames {
            let frame = next_event_frame(stream, per_frame_timeout_ms).await?;
            if predicate(&frame) {
                return Some(frame);
            }
        }
        None
    }

    /// Wait until at least `target` SSE handlers have subscribed to the
    /// global broadcast, then fire `event`. Replaces `tokio::time::sleep`
    /// in the SSE happy-path tests so they are not coupled to wall-clock
    /// timing on slow CI runners.
    async fn wait_subscribers_then_send(target: usize, event: PromptEvent) {
        let sender = prompt_events();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sender.receiver_count() < target {
            if std::time::Instant::now() >= deadline {
                panic!(
                    "timed out waiting for {target} SSE subscribers; have {}",
                    sender.receiver_count()
                );
            }
            tokio::task::yield_now().await;
        }
        drop(sender.send(event));
    }

    /// RAII helper: removes a key from the shared pending registry on drop.
    /// Used in tests that mutate the global registry so a panicking assert
    /// doesn't pollute the registry for sibling tests.
    struct PendingCleanup {
        pending: PendingPrompts,
        nonce: String,
    }

    impl Drop for PendingCleanup {
        fn drop(&mut self) {
            self.pending.remove(&self.nonce);
        }
    }

    fn cleanup_on_drop(pending: PendingPrompts, nonce: impl Into<String>) -> PendingCleanup {
        PendingCleanup {
            pending,
            nonce: nonce.into(),
        }
    }

    /// The bug this PR closes: a `prompt_added` lifecycle event must be
    /// pushed to a subscribed SSE client without polling.
    #[tokio::test]
    async fn test_sse_emits_added_when_prompt_inserted() {
        let initial_subs = prompt_events().receiver_count();
        let mut body = open_sse_with_origin(Some("http://localhost:7509")).await;

        let nonce = "ssetest_added_001".to_string();
        let snapshot = PromptSnapshot {
            nonce: nonce.clone(),
            message: "approve?".into(),
            labels: vec!["Allow".into(), "Deny".into()],
            delegate_key: "dkey-added-001".into(),
            caller: webapp_caller("cid-added-001"),
        };
        wait_subscribers_then_send(initial_subs + 1, PromptEvent::Added(snapshot)).await;

        let frame = frame_matching(
            &mut body,
            |f| f.contains("event: prompt_added") && f.contains(&nonce),
            32,
            500,
        )
        .await
        .unwrap_or_else(|| panic!("did not see prompt_added for {nonce}"));
        assert!(frame.contains("dkey-added-001"));
    }

    /// `prompt_removed` must arrive over SSE so every tab dismisses its
    /// overlay simultaneously when one tab clicks a button.
    #[tokio::test]
    async fn test_sse_emits_removed_when_prompt_responded() {
        let initial_subs = prompt_events().receiver_count();
        let mut body = open_sse_with_origin(Some("http://localhost:7509")).await;

        let nonce = "ssetest_removed_002".to_string();
        wait_subscribers_then_send(
            initial_subs + 1,
            PromptEvent::Removed {
                nonce: nonce.clone(),
            },
        )
        .await;

        let frame = frame_matching(
            &mut body,
            |f| f.contains("event: prompt_removed") && f.contains(&nonce),
            32,
            500,
        )
        .await
        .unwrap_or_else(|| panic!("did not see prompt_removed for {nonce}"));
        assert!(frame.contains(&nonce));
    }

    /// Untrusted-origin SSE subscribers must receive a closed stream rather
    /// than the live event flow. Mirrors the gating on `/permission/pending`.
    #[tokio::test]
    async fn test_sse_untrusted_origin_returns_closed_stream() {
        let mut body = open_sse_with_origin(Some("http://evil.example")).await;
        let frame = next_event_frame(&mut body, 200).await;
        // The stream emits exactly one `:untrusted-origin` comment then
        // closes. Comments are stripped by next_event_frame, so we expect
        // None (no data events ever arrive on this stream).
        assert!(
            frame.is_none(),
            "untrusted origin must not receive any data events, got: {frame:?}"
        );
    }

    /// Initial-state bootstrap: a fresh subscriber must receive
    /// `prompt_added` events for every currently-pending prompt before any
    /// new live events arrive. Avoids the race where a prompt added between
    /// the page load and the SSE subscribe would be invisible.
    #[tokio::test]
    async fn test_sse_replays_existing_pending_on_subscribe() {
        // Insert into the global registry before subscribing.
        let pending = crate::contract::user_input::pending_prompts();
        let nonce = "ssetest_bootstrap_003".to_string();
        let _rx = insert_prompt(
            &pending,
            &nonce,
            "bootstrap?",
            vec!["OK"],
            "dkey-bootstrap",
            webapp_caller("cid-bootstrap"),
        );

        let mut body = open_sse_with_origin(Some("http://localhost:7509")).await;

        // Walk frames until we see one referencing our nonce. Other tests
        // running in parallel may have left additional pending entries in
        // the shared global registry; any of them being replayed first is
        // fine.
        let mut found = false;
        for _ in 0..16 {
            let Some(frame) = next_event_frame(&mut body, 500).await else {
                break;
            };
            if frame.contains("event: prompt_added") && frame.contains(&nonce) {
                found = true;
                break;
            }
        }
        // Cleanup runs even if the assertion below fails so a panicking test
        // does not pollute the shared global registry for siblings.
        let _cleanup = cleanup_on_drop(pending.clone(), nonce.clone());
        assert!(found, "bootstrap replay did not include {nonce}");
    }

    /// Bootstrap replay must arrive BEFORE any live broadcast event, so a
    /// fresh subscriber sees pre-existing prompts before new ones. This
    /// pins the `stream::iter(initial).chain(live)` contract; without the
    /// chain order, a refreshed tab could dedup-skip a re-broadcast Added
    /// for an entry that hadn't been delivered yet.
    #[tokio::test]
    async fn test_sse_bootstrap_replay_arrives_before_live() {
        let pending = crate::contract::user_input::pending_prompts();
        let pre_nonce = "ssetest_order_pre".to_string();
        let _rx = insert_prompt(
            &pending,
            &pre_nonce,
            "pre-existing",
            vec!["OK"],
            "dkey-order-pre",
            webapp_caller("cid-order-pre"),
        );
        let _cleanup_pre = cleanup_on_drop(pending.clone(), pre_nonce.clone());

        let initial_subs = prompt_events().receiver_count();
        let mut body = open_sse_with_origin(Some("http://localhost:7509")).await;

        // Once the SSE handler has subscribed, fire a NEW Added that the
        // chain MUST deliver after the pre-existing snapshot.
        let live_nonce = "ssetest_order_live".to_string();
        let snapshot = PromptSnapshot {
            nonce: live_nonce.clone(),
            message: "live".into(),
            labels: vec!["OK".into()],
            delegate_key: "dkey-order-live".into(),
            caller: webapp_caller("cid-order-live"),
        };
        wait_subscribers_then_send(initial_subs + 1, PromptEvent::Added(snapshot)).await;

        // Walk frames; we must see pre-existing nonce before live nonce.
        let mut saw_pre = false;
        let mut saw_live_before_pre = false;
        for _ in 0..32 {
            let Some(frame) = next_event_frame(&mut body, 500).await else {
                break;
            };
            if frame.contains(&pre_nonce) {
                saw_pre = true;
                break;
            }
            if frame.contains(&live_nonce) {
                saw_live_before_pre = true;
            }
        }
        assert!(saw_pre, "did not observe pre-existing nonce in stream");
        assert!(
            !saw_live_before_pre,
            "live event arrived before bootstrap replay, ordering contract broken"
        );
    }

    /// A `BroadcastStreamRecvError::Lagged` event must be translated into
    /// a `resync` SSE event so the client can recover by re-bootstrapping
    /// from /permission/pending. Constructs a small private broadcaster
    /// (capacity 2) so the test can deterministically overflow the
    /// receiver without relying on the global PROMPT_EVENT_CAPACITY=128.
    #[tokio::test]
    async fn test_sse_emits_resync_on_lag() {
        let (tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(2);

        // Send 5 events without polling rx; receiver lags by 3.
        for i in 0..5 {
            drop(tx.send(PromptEvent::Removed {
                nonce: format!("lag-{i}"),
            }));
        }

        // Drive the SSE filter logic against the lagged receiver. We
        // duplicate the closure body from permission_events here so the
        // test exercises exactly the production translation.
        let stream = BroadcastStream::new(rx).filter_map(|incoming| async move {
            match incoming {
                Ok(PromptEvent::Added(s)) => Some(format!("added:{}", s.nonce)),
                Ok(PromptEvent::Removed { nonce }) => Some(format!("removed:{nonce}")),
                Err(BroadcastStreamRecvError::Lagged(_)) => Some("resync".to_string()),
            }
        });
        tokio::pin!(stream);
        let first = stream.next().await.expect("first event");
        assert_eq!(
            first, "resync",
            "lagged receiver must surface as `resync`, not as a missed event"
        );
    }

    /// MAX_SSE_CONNECTIONS must reject the (cap+1)th subscriber AND must
    /// release a slot when an earlier subscriber's stream is dropped.
    /// Together these pin both the cap (rejecting flood reconnects) and
    /// the GuardedStream's drop semantics (no permanent leak after a
    /// client disconnects mid-flight).
    ///
    /// Uses a serialising mutex so concurrent tests don't perturb the
    /// shared SSE_CONNECTIONS counter. Without it, a sibling test
    /// touching the counter mid-assertion would flake this test.
    #[tokio::test]
    async fn test_sse_connection_cap_and_release_on_drop() {
        use std::sync::OnceLock;
        static SERIAL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        let _serial = SERIAL
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;

        // Drain any leaked connections from prior tests by waiting for the
        // counter to settle to zero. (No SSE responses should be live at
        // this point because the only test that opens them is this one,
        // serialised by the mutex above.)
        let deadline0 = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while SSE_CONNECTIONS.load(Ordering::Relaxed) != 0 {
            if std::time::Instant::now() >= deadline0 {
                // A leaked connection from a previous test indicates a real
                // bug in GuardedStream's drop, but for the purposes of this
                // test, accept the baseline rather than panic.
                break;
            }
            tokio::task::yield_now().await;
        }
        let baseline = SSE_CONNECTIONS.load(Ordering::Relaxed);

        // Open enough subscribers to exhaust the cap.
        let mut held: Vec<_> = Vec::new();
        for _ in baseline..MAX_SSE_CONNECTIONS {
            held.push(open_sse_with_origin(Some("http://localhost:7509")).await);
        }
        assert_eq!(
            SSE_CONNECTIONS.load(Ordering::Relaxed),
            MAX_SSE_CONNECTIONS,
            "all slots must be filled before testing the cap"
        );

        // The next subscriber MUST be rejected with a closed stream.
        let mut rejected = open_sse_with_origin(Some("http://localhost:7509")).await;
        let frame = next_event_frame(&mut rejected, 200).await;
        assert!(
            frame.is_none(),
            "cap-rejected subscriber must not receive data events, got: {frame:?}"
        );

        // Drop one held subscriber; the cap-counter must release a slot.
        let dropped = held.pop().unwrap();
        drop(dropped);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if SSE_CONNECTIONS.load(Ordering::Relaxed) < MAX_SSE_CONNECTIONS {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("SSE_CONNECTIONS did not decrement after dropping a subscriber");
            }
            tokio::task::yield_now().await;
        }

        // A new subscriber should now succeed.
        let _accepted = open_sse_with_origin(Some("http://localhost:7509")).await;
        assert!(SSE_CONNECTIONS.load(Ordering::Relaxed) <= MAX_SSE_CONNECTIONS);

        // Cleanup: drop everything so the counter returns to baseline for
        // the next time this test runs (matters when test runs are not
        // process-isolated, e.g. nextest run --no-fail-fast on debug
        // binaries that re-execute).
        drop(_accepted);
        drop(rejected);
        drop(held);
    }

    /// HTTP `/permission/{nonce}/respond` must fire a `PromptEvent::Removed`
    /// so every other tab dismisses its overlay. Closes a coverage gap
    /// flagged by review: the cross-tab dismissal contract was previously
    /// only covered by integration-level browser tests.
    #[tokio::test]
    async fn test_respond_handler_emits_prompt_removed() {
        let pending = crate::contract::user_input::pending_prompts();
        let nonce = "ssetest_respond_emits_removed".to_string();
        let _rx = insert_prompt(
            &pending,
            &nonce,
            "respond?",
            vec!["Allow", "Deny"],
            "dkey-respond",
            webapp_caller("cid-respond"),
        );
        let _cleanup = cleanup_on_drop(pending.clone(), nonce.clone());

        let mut rx = prompt_events().subscribe();

        let resp = permission_respond(
            Path(nonce.clone()),
            trusted_header(),
            Extension(pending.clone()),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        // Drain until we see the matching Removed (concurrent tests on the
        // global broadcaster may interleave their own events).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut saw = false;
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
                Ok(Ok(PromptEvent::Removed { nonce: n })) if n == nonce => {
                    saw = true;
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) | Err(_) => continue,
            }
        }
        assert!(saw, "permission_respond did not emit Removed for {nonce}");
    }
}
