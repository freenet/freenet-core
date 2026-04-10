//! HTTP endpoints for delegate permission prompts.
//!
//! When a delegate emits `RequestUserInput`, the `DashboardPrompter` stores the
//! pending prompt and the gateway shell page's JS detects it via polling. The user
//! clicks a browser notification to open the permission page, responds, and the
//! result flows back to the delegate.

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

/// Return a list of pending prompt nonces (for the shell page to show notifications).
/// Only returns nonces and message previews, not full prompt data.
async fn pending_prompts(Extension(pending): Extension<PendingPrompts>) -> impl IntoResponse {
    let prompts: Vec<serde_json::Value> = pending
        .iter()
        .map(|entry| {
            serde_json::json!({
                "nonce": entry.key(),
                "preview": entry.value().message.chars().take(100).collect::<String>(),
            })
        })
        .collect();
    Json(prompts)
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
            format!(
                r#"<button class="{class}" onclick="respond('{nonce}', {i})">{escaped}</button>"#
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

    if let Some((_, prompt)) = pending.remove(&nonce) {
        if body.index < prompt.labels.len() {
            if prompt.response_tx.send(body.index).is_err() {
                tracing::debug!(nonce = %nonce, "Permission response channel already closed");
            }
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({"ok": true})),
            )
        } else {
            (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid index"})),
            )
        }
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "expired or already answered"})),
        )
    }
}

/// Check if an Origin header is from a trusted source (localhost).
fn is_trusted_origin(origin: &str) -> bool {
    let trusted = [
        "http://127.0.0.1:7509",
        "http://localhost:7509",
        "http://[::1]:7509",
    ];
    trusted.contains(&origin)
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
