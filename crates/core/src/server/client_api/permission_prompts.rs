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

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;

use crate::contract::user_input::PendingPrompts;

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
/// Maximum length of `delegate_key` / `contract_id` rendered on the overlay.
/// These are normally short keys; the cap bounds the amplification surface if
/// the producer populates them from untrusted data in the future.
const OVERLAY_KEY_CHARS_MAX: usize = 256;

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

/// Return the list of pending prompts for the shell page to render as
/// in-page overlays (see issue #3836). Each entry includes the sanitized
/// message, button labels, and delegate/contract context.
///
/// The endpoint is protected by the same Origin check as
/// `/permission/{nonce}/respond`: since this response now carries the full
/// delegate-controlled message (rather than just a 100-char preview), a
/// cross-origin page or rebinding attacker could otherwise scrape live
/// prompts before the user sees them. Only trusted localhost origins pass.
async fn pending_prompts(
    headers: HeaderMap,
    Extension(pending): Extension<PendingPrompts>,
) -> impl IntoResponse {
    if let Some(origin) = headers.get("origin") {
        let origin = origin.to_str().unwrap_or("");
        if !is_trusted_origin(origin) {
            return (
                axum::http::StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "forbidden"})),
            );
        }
    }
    // Requests with no Origin header (e.g. same-origin top-level fetch from
    // some browsers) are allowed: the gateway only listens on loopback, the
    // response is same-origin by policy, and the polled payload is not a
    // capability — answering still requires POSTing to `/respond` with an
    // Origin check that does reject the no-header case.
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
                "contract_id": sanitize_display(&prompt.contract_id, OVERLAY_KEY_CHARS_MAX),
            })
        })
        .collect();
    (axum::http::StatusCode::OK, Json(serde_json::json!(prompts)))
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

    let delegate_key_display = html_escape(&entry.delegate_key);
    let contract_id_display = html_escape(&entry.contract_id);

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
  .context {{ background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
              padding: 12px; margin-bottom: 16px; font-size: 13px; color: var(--muted); }}
  .context dt {{ font-weight: 600; color: var(--fg); }}
  .context dd {{ margin-bottom: 8px; font-family: monospace; font-size: 12px; word-break: break-all; }}
  .message {{ font-size: 15px; line-height: 1.5; margin-bottom: 24px; padding: 16px;
              background: var(--bg); border-left: 3px solid var(--warn); border-radius: 4px; }}
  .message-label {{ font-size: 12px; color: var(--muted); margin-bottom: 4px; text-transform: uppercase;
                    letter-spacing: 0.5px; }}
  .buttons {{ display: flex; gap: 12px; flex-wrap: wrap; }}
  .btn {{ padding: 10px 24px; border: 1px solid var(--border); border-radius: 8px;
          background: var(--card); color: var(--fg); font-size: 14px; cursor: pointer;
          transition: all 0.15s; flex: 1; min-width: 100px; font-weight: 500; }}
  .btn.primary {{ background: var(--accent); color: white; border-color: var(--accent); }}
  .btn:hover {{ opacity: 0.85; transform: translateY(-1px); }}
  .btn:disabled {{ opacity: 0.5; cursor: not-allowed; transform: none; }}
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
  <div class="context">
    <dl>
      <dt>Delegate</dt>
      <dd>{delegate_key_display}</dd>
      <dt>Requesting contract</dt>
      <dd>{contract_id_display}</dd>
    </dl>
  </div>
  <div class="message-label">Delegate says:</div>
  <p class="message">{message}</p>
  <div class="buttons">
    {buttons_html}
  </div>
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
        contract_id: &str,
    ) -> tokio::sync::oneshot::Receiver<usize> {
        use crate::contract::user_input::PendingPrompt;
        let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
        pending.insert(
            nonce.to_string(),
            PendingPrompt {
                message: message.to_string(),
                labels: labels.into_iter().map(String::from).collect(),
                delegate_key: delegate_key.to_string(),
                contract_id: contract_id.to_string(),
                response_tx: tx,
            },
        );
        rx
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
        use axum::body::to_bytes;
        use axum::response::IntoResponse;
        let resp = pending_prompts(headers, Extension(pending))
            .await
            .into_response();
        let status = resp.status();
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, value)
    }

    // Regression test for issue #3836: the /permission/pending JSON must
    // carry enough data for the shell-page overlay to render the prompt
    // (message, labels, delegate key, contract id), not just a preview.
    #[tokio::test]
    async fn test_pending_prompts_includes_overlay_fields() {
        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "nonce123",
            "Approve this?",
            vec!["Allow Once", "Always Allow", "Deny"],
            "dkey",
            "cid",
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
        assert_eq!(entry["contract_id"], "cid");
    }

    // Oversized delegate messages must be clipped so a malicious delegate
    // can't balloon the polling response the shell fetches every few seconds.
    #[tokio::test]
    async fn test_pending_prompts_message_capped() {
        let pending = empty_pending();
        let huge = "a".repeat(OVERLAY_MESSAGE_MAX * 4);
        let _rx = insert_prompt(&pending, "n", &huge, vec!["OK"], "d", "c");

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
        // Each fire emoji is 4 UTF-8 bytes; OVERLAY_MESSAGE_MAX * 4 bytes
        // would be the naive byte budget. Char-based truncation keeps them
        // all intact.
        let emoji = "\u{1F525}".repeat(OVERLAY_MESSAGE_MAX);
        let _rx = insert_prompt(&pending, "n", &emoji, vec!["OK"], "d", "c");
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
                    contract_id: "c".to_string(),
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
        let _rx = insert_prompt(&pending, "n", "m", vec![], "d", "c");
        let (_, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(value[0]["labels"], serde_json::json!([]));
    }

    // Unicode right-to-left override in delegate_key / contract_id must
    // be stripped so a hostile delegate can't visually reverse the key
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
            "c\u{0007}id",
        );
        let (_, value) = call_pending(trusted_header(), pending).await;
        assert_eq!(value[0]["message"], "Helloevil!");
        assert_eq!(value[0]["labels"], serde_json::json!(["Allow"]));
        assert_eq!(value[0]["delegate_key"], "key123");
        assert_eq!(value[0]["contract_id"], "cid");
    }

    // /permission/pending now returns full delegate-controlled text, so
    // the endpoint must reject cross-origin requests (same Origin check
    // as /respond). This guards against DNS rebinding / browser-extension
    // scraping of live prompts.
    #[tokio::test]
    async fn test_pending_prompts_rejects_untrusted_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", "c");
        let mut headers = HeaderMap::new();
        headers.insert("origin", "http://evil.com".parse().unwrap());
        let (status, _) = call_pending(headers, pending).await;
        assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    }

    // Missing Origin is allowed (some fetch flavors omit it); this matches
    // the documented threat model: the poll payload is not a capability,
    // and the /respond endpoint still rejects the no-Origin case.
    #[tokio::test]
    async fn test_pending_prompts_allows_missing_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", "c");
        let (status, _) = call_pending(HeaderMap::new(), pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
    }

    // End-to-end flow: two prompts pending, user answers one, other remains,
    // second response to the same nonce returns 404. This is the cross-tab
    // dismissal contract the shell JS relies on ("another tab already
    // answered" → hide the overlay).
    #[tokio::test]
    async fn test_respond_consumes_nonce_and_second_response_404s() {
        let pending = empty_pending();
        let rx_a = insert_prompt(&pending, "a", "mA", vec!["Yes", "No"], "d", "c");
        let _rx_b = insert_prompt(&pending, "b", "mB", vec!["Yes", "No"], "d", "c");

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
}
