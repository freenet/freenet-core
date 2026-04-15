//! HTTP endpoints for delegate permission prompts.
//!
//! When a delegate emits `RequestUserInput`, the `DashboardPrompter` stores the
//! pending prompt and the gateway shell page's JS detects it via polling the
//! `/permission/pending` endpoint. The shell page renders the prompt as an
//! in-page overlay (see issue #3836) on every open Freenet tab. When the user
//! clicks a button in any tab the response is POSTed to
//! `/permission/{nonce}/respond` and the result flows back to the delegate;
//! other tabs see the nonce disappear on their next poll and hide the overlay.
//!
//! The standalone `/permission/{nonce}` HTML page is retained as a fallback
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

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;

use crate::contract::user_input::{CallerIdentity, PendingPrompts};

/// Register permission prompt routes.
pub(super) fn routes() -> Router {
    Router::new()
        .route("/permission/pending", get(pending_prompts))
        .route("/permission/{nonce}", get(permission_page))
        .route("/permission/{nonce}/respond", post(permission_respond))
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
/// These are normally short keys; the cap bounds the amplification surface if
/// the producer populates them from untrusted data in the future.
const OVERLAY_KEY_CHARS_MAX: usize = 256;

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
}
