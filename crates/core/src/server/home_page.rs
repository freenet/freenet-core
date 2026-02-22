//! Homepage served at `/` when a user navigates to their local Freenet node.

use axum::response::{Html, IntoResponse};

use crate::node::network_status;

/// Handler for `GET /` — returns a self-contained HTML landing page.
pub(super) async fn homepage() -> impl IntoResponse {
    Html(homepage_html())
}

fn status_section() -> (&'static str, String) {
    match network_status::get_snapshot() {
        None => ("Starting up...", String::new()),
        Some(snap) if snap.open_connections > 0 => {
            let n = snap.open_connections;
            let label = if n == 1 { "peer" } else { "peers" };
            (
                "Connected to Freenet",
                format!(
                    r#"<p class="status connected">{n} {label} connected</p>"#
                ),
            )
        }
        Some(snap) if !snap.failures.is_empty() => {
            let mut items = String::new();
            for f in &snap.failures {
                items.push_str(&format!(
                    "<li><code>{}</code>: {}</li>",
                    f.address, f.reason_html
                ));
            }
            (
                "Connecting to network...",
                format!(
                    r#"<div class="diagnostics">
                    <h2>Connection Issues</h2>
                    <ul>{items}</ul>
                    <p class="attempts">Attempted {attempts} connection(s) over {elapsed}s. Retrying...</p>
                </div>"#,
                    attempts = snap.connection_attempts,
                    elapsed = snap.elapsed_secs,
                ),
            )
        }
        Some(snap) if snap.connection_attempts > 0 => (
            "Connecting to network...",
            format!(
                r#"<p class="attempts">Attempted {} connection(s) over {}s. Retrying...</p>"#,
                snap.connection_attempts, snap.elapsed_secs,
            ),
        ),
        _ => ("Connecting to network...", String::new()),
    }
}

fn homepage_html() -> String {
    let (status_title, status_detail) = status_section();

    let is_connected = status_title == "Connected to Freenet";
    let spinner = if is_connected {
        ""
    } else {
        r#"<div class="spinner"></div>"#
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="5">
    <title>Freenet</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: flex-start;
            min-height: 100vh;
            margin: 0;
            background: #f5f5f5;
            padding-top: 8vh;
        }}
        .container {{
            text-align: center;
            padding: 2rem;
            max-width: 600px;
            width: 100%;
        }}
        .logo {{
            width: 80px;
            height: 80px;
            margin-bottom: 1.5rem;
        }}
        h1 {{
            color: #333;
            font-size: 1.5rem;
            margin-bottom: 0.5rem;
        }}
        p {{
            color: #666;
            margin-bottom: 1rem;
            line-height: 1.5;
        }}
        .status.connected {{
            color: #2e7d32;
            font-weight: 500;
        }}
        .spinner {{
            width: 24px;
            height: 24px;
            border: 3px solid #e0e0e0;
            border-top-color: #2196F3;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin: 0 auto 1.5rem;
        }}
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
        section {{
            text-align: left;
            background: #fff;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            margin-top: 1.5rem;
        }}
        section h2 {{
            color: #333;
            font-size: 1.1rem;
            margin: 0 0 0.75rem 0;
        }}
        section ul {{
            padding-left: 0;
            margin: 0;
            list-style: none;
        }}
        section li {{
            margin-bottom: 0.5rem;
            font-size: 0.95rem;
        }}
        section li a {{
            color: #1976D2;
            text-decoration: none;
        }}
        section li a:hover {{
            text-decoration: underline;
        }}
        .note {{
            color: #888;
            font-size: 0.85rem;
            margin-top: 0.25rem;
        }}
        .diagnostics {{
            text-align: left;
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            margin-top: 1.5rem;
        }}
        .diagnostics h2 {{
            color: #856404;
            font-size: 1.1rem;
            margin: 0 0 0.5rem 0;
        }}
        .diagnostics ul {{
            padding-left: 1.2rem;
            margin: 0.5rem 0;
            list-style: disc;
        }}
        .diagnostics li {{
            color: #555;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
        }}
        .attempts {{
            color: #888;
            font-size: 0.85rem;
            margin-top: 0.5rem;
        }}
        code {{
            background: #f0f0f0;
            padding: 0.1rem 0.3rem;
            border-radius: 3px;
            font-size: 0.85em;
        }}
        .troubleshooting {{
            background: #f9f9f9;
        }}
    </style>
</head>
<body>
    <div class="container">
        <img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="logo">
        <h1>{status_title}</h1>
        {spinner}
        {status_detail}

        <section>
            <h2>Freenet Apps</h2>
            <ul>
                <li>
                    <a href="/v1/contract/web/raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv/">River Chat</a>
                    <p class="note">You'll need an invite to join the "Freenet Official" room.</p>
                </li>
            </ul>
        </section>

        <section>
            <h2>Links</h2>
            <ul>
                <li><a href="https://freenet.org">freenet.org</a></li>
                <li><a href="https://matrix.to/#/#freenet-locutus:matrix.org">Freenet Matrix channel</a></li>
                <li><a href="https://github.com/freenet/freenet-core">GitHub</a></li>
            </ul>
        </section>

        <section class="troubleshooting">
            <h2>Troubleshooting</h2>
            <p>If you're having problems, run <code>freenet service report</code> in your terminal and share the code on our <a href="https://matrix.to/#/#freenet-locutus:matrix.org">Matrix channel</a>.</p>
        </section>
    </div>
</body>
</html>"#
    )
}
