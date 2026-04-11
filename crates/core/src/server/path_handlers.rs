//! Handle the `web` part of the bundles.
//!
//! Contract web apps are served inside sandboxed iframes to provide origin isolation.
//! The local API server returns a "shell" page that holds the auth token and
//! proxies WebSocket connections via postMessage, while the contract runs in an
//! `<iframe sandbox="allow-scripts allow-forms allow-popups">`
//! with an opaque origin that cannot access other contracts' data.
//! Popups inherit the sandbox (no `allow-popups-to-escape-sandbox`); external links
//! are opened via the `open_url` shell bridge message to avoid CORS issues. Sandbox content
//! is protected from top-level access via Sec-Fetch-Dest checks in client_api.rs.

use std::path::{Path, PathBuf};

use axum::response::{Html, IntoResponse};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse},
    prelude::*,
};
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};

use crate::client_events::AuthToken;

use super::{
    ApiVersion, ClientConnection, HostCallbackResult,
    app_packaging::{WebApp, WebContractError},
    client_api::HttpClientApiRequest,
    errors::WebSocketApiError,
};
use tracing::{debug, instrument};

#[instrument(level = "debug", skip(request_sender))]
pub(super) async fn contract_home(
    key: String,
    request_sender: HttpClientApiRequest,
    assigned_token: AuthToken,
    api_version: ApiVersion,
    query_string: Option<String>,
) -> Result<impl IntoResponse, WebSocketApiError> {
    debug!(
        "contract_home: Converting string key to ContractInstanceId: {}",
        key
    );
    let instance_id = ContractInstanceId::from_bytes(&key).map_err(|err| {
        debug!("contract_home: Failed to parse contract key: {}", err);
        WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        }
    })?;
    debug!("contract_home: Successfully parsed contract instance id");
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    debug!("contract_home: Sending NewConnection request");
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token: Some((assigned_token.clone(), instance_id)),
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let client_id = if let Some(HostCallbackResult::NewId { id }) = response_recv.recv().await {
        id
    } else {
        return Err(WebSocketApiError::NodeError {
            error_cause: "Couldn't register new client in the node".into(),
        });
    };
    debug!("contract_home: Sending GET request for contract");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(
                ContractRequest::Get {
                    key: instance_id,
                    return_contract_code: true,
                    subscribe: true,
                    blocking_subscribe: false,
                }
                .into(),
            ),
            auth_token: None,
            origin_contract: None,
            api_version: Default::default(),
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    debug!("contract_home: Waiting for GET response");
    let recv_result =
        tokio::time::timeout(std::time::Duration::from_secs(30), response_recv.recv()).await;
    let response = match recv_result {
        Err(_) => {
            return Err(WebSocketApiError::NodeError {
                error_cause: "GET request timed out after 30s".into(),
            });
        }
        Ok(None) => {
            return Err(WebSocketApiError::NodeError {
                error_cause: "GET response channel closed (node may be shutting down)".into(),
            });
        }
        Ok(Some(HostCallbackResult::Result {
            result:
                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    contract,
                    state,
                    ..
                })),
            ..
        })) => match contract {
            Some(contract) => {
                let contract_key = contract.key();
                let path = contract_web_path(contract_key.id());
                let state_bytes = state.as_ref();
                let current_hash = hash_state(state_bytes);
                let hash_path = state_hash_path(contract_key.id());

                let needs_update = match tokio::fs::read(&hash_path).await {
                    Ok(stored_hash_bytes) if stored_hash_bytes.len() == 8 => {
                        let stored_hash = u64::from_be_bytes(stored_hash_bytes.try_into().unwrap());
                        stored_hash != current_hash
                    }
                    _ => true,
                };

                if needs_update {
                    debug!("State changed or not cached, unpacking webapp");
                    let state = State::from(state_bytes);

                    fn err(
                        err: WebContractError,
                        contract: &ContractContainer,
                    ) -> WebSocketApiError {
                        let key = contract.key();
                        tracing::error!("{err}");
                        WebSocketApiError::InvalidParam {
                            error_cause: format!("failed unpacking contract: {key}"),
                        }
                    }

                    // Clear existing cache if any; may not exist yet
                    let _cleanup = tokio::fs::remove_dir_all(&path).await;
                    tokio::fs::create_dir_all(&path).await.map_err(|e| {
                        WebSocketApiError::NodeError {
                            error_cause: format!("Failed to create cache dir: {e}"),
                        }
                    })?;

                    let mut web =
                        WebApp::try_from(state.as_ref()).map_err(|e| err(e, &contract))?;
                    web.unpack(&path).map_err(|e| err(e, &contract))?;

                    // Store new hash
                    tokio::fs::write(&hash_path, current_hash.to_be_bytes())
                        .await
                        .map_err(|e| WebSocketApiError::NodeError {
                            error_cause: format!("Failed to write state hash: {e}"),
                        })?;
                }

                // Return the shell page instead of the contract HTML directly.
                // The shell page wraps the contract in a sandboxed iframe for
                // origin isolation (GHSA-824h-7x5x-wfmf).
                match shell_page(&assigned_token, &key, api_version, query_string) {
                    Ok(b) => b.into_response(),
                    Err(err) => {
                        tracing::error!("Failed to generate shell page: {err}");
                        return Err(WebSocketApiError::NodeError {
                            error_cause: format!("Failed to generate shell page: {err}"),
                        });
                    }
                }
            }
            None => {
                return Err(WebSocketApiError::MissingContract { instance_id });
            }
        },
        Ok(Some(HostCallbackResult::Result {
            result: Err(err), ..
        })) => {
            tracing::error!("error getting contract `{key}`: {err}");
            return Err(WebSocketApiError::AxumError {
                error: err.kind().clone(),
            });
        }
        Ok(other) => {
            tracing::error!("Unexpected node response: {other:?}");
            return Err(WebSocketApiError::NodeError {
                error_cause: format!("Unexpected response from node: {other:?}"),
            });
        }
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(ClientRequest::Disconnect { cause: None }),
            auth_token: None,
            origin_contract: None,
            api_version: Default::default(),
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    Ok(response)
}

#[instrument(level = "debug")]
pub(super) async fn variable_content(
    key: String,
    req_path: String,
    api_version: ApiVersion,
) -> Result<impl IntoResponse, Box<WebSocketApiError>> {
    debug!(
        "variable_content: Processing request for key: {}, path: {}",
        key, req_path
    );
    // compose the correct absolute path
    let instance_id =
        ContractInstanceId::from_bytes(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;
    let base_path = contract_web_path(&instance_id);
    debug!("variable_content: Base path resolved to: {:?}", base_path);

    // Parse the full request path URI to extract the relative path using the v1 helper.
    let req_uri =
        req_path
            .parse::<axum::http::Uri>()
            .map_err(|err| WebSocketApiError::InvalidParam {
                error_cause: format!("Failed to parse request path as URI: {err}"),
            })?;
    debug!("variable_content: Parsed request URI: {:?}", req_uri);

    let relative_path = get_file_path(req_uri)?;
    debug!(
        "variable_content: Extracted relative path: {}",
        relative_path
    );

    let file_path = base_path.join(relative_path);
    debug!("variable_content: Full file path to serve: {:?}", file_path);
    debug!(
        "variable_content: Checking if file exists: {}",
        file_path.exists()
    );

    // For JavaScript files, rewrite root-relative asset paths just like we do for HTML.
    // Dioxus embeds paths like "/./assets/app_bg.wasm" inside the JS bundle, which browsers
    // normalize to "/assets/..." (root-relative), bypassing the contract web prefix.
    if file_path.extension().is_some_and(|ext| ext == "js") {
        let content = tokio::fs::read_to_string(&file_path).await.map_err(|err| {
            WebSocketApiError::NodeError {
                error_cause: format!("{err}"),
            }
        })?;
        let prefix = format!("/{}/contract/web/{key}/", api_version.prefix());
        let rewritten = content
            .replace("\"/./", &format!("\"{prefix}"))
            .replace("'/./", &format!("'{prefix}"));
        return Ok((
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            rewritten,
        )
            .into_response());
    }

    // serve the file
    let mut serve_file = tower_http::services::fs::ServeFile::new(&file_path);
    let fake_req = axum::http::Request::new(axum::body::Body::empty());
    serve_file
        .try_call(fake_req)
        .await
        .map_err(|err| {
            WebSocketApiError::NodeError {
                error_cause: format!("{err}"),
            }
            .into()
        })
        .map(|r| r.into_response())
}

/// Escapes characters that are dangerous inside an HTML attribute value.
fn html_escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Generates the shell page HTML that wraps the contract in a sandboxed iframe.
///
/// The shell page holds the auth token and proxies WebSocket connections via
/// postMessage, providing origin isolation between contracts.
fn shell_page(
    auth_token: &AuthToken,
    contract_key: &str,
    api_version: ApiVersion,
    query_string: Option<String>,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let version_prefix = api_version.prefix();
    let base_path = format!("/{version_prefix}/contract/web/{contract_key}/");

    // Build the iframe src URL: same path with __sandbox=1 plus any original query params
    let mut iframe_params = vec!["__sandbox=1".to_string()];
    if let Some(qs) = &query_string {
        // Forward the original query params (e.g., ?invitation=...) to the iframe
        for param in qs.split('&') {
            if !param.is_empty() && !param.starts_with("__sandbox") {
                iframe_params.push(param.to_string());
            }
        }
    }
    let iframe_src_raw = format!("{}?{}", base_path, iframe_params.join("&"));
    // HTML-escape the iframe src to prevent XSS via crafted query parameters.
    // While browsers typically percent-encode special chars in URLs, we must not
    // rely on that for defense-in-depth.
    let iframe_src = html_escape_attr(&iframe_src_raw);

    // auth_token is base58 (alphanumeric only), safe for unescaped interpolation.
    let auth_token = auth_token.as_str();
    // Use an inline SVG data URI for the default favicon to avoid CORS errors
    // from cross-origin requests. Contracts can override this via the
    // __freenet_shell__ postMessage bridge (type: 'favicon').
    let favicon = format!(
        "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 640 471'>\
         <path d='{}' fill='%23007FFF' fill-rule='evenodd'/></svg>",
        super::home_page::RABBIT_SVG_PATH,
    );
    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Freenet</title>
<link rel="icon" type="image/svg+xml" href="{favicon}">
<style>*{{margin:0;padding:0}}html,body{{width:100%;height:100%;overflow:hidden}}iframe{{width:100%;height:100%;border:none;display:block}}</style>
</head>
<body>
<iframe id="app" sandbox="allow-scripts allow-forms allow-popups" data-src="{iframe_src}"></iframe>
<script>
{SHELL_BRIDGE_JS}
</script>
<script>freenetBridge("{auth_token}");</script>
</body>
</html>"##
    );

    Ok(Html(html))
}

/// Serves the contract's actual HTML content for display inside the sandboxed iframe.
///
/// This is called when the iframe requests `?__sandbox=1`. It reads the cached
/// contract HTML, rewrites asset paths, and injects the WebSocket shim that
/// routes connections through the shell page's postMessage bridge.
///
/// The `sub_path` parameter allows serving pages other than `index.html` for
/// multi-page websites. When `None`, defaults to `index.html`.
#[instrument(level = "debug")]
pub(super) async fn serve_sandbox_content(
    key: String,
    api_version: ApiVersion,
    sub_path: Option<&str>,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let page = sub_path.unwrap_or("index.html");
    debug!("serve_sandbox_content: serving iframe content for key: {key}, page: {page}");
    let instance_id =
        ContractInstanceId::from_bytes(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;
    let path = contract_web_path(&instance_id);
    if !path.exists() {
        return Err(WebSocketApiError::NodeError {
            error_cause: format!("Contract not cached yet: {key}"),
        });
    }
    sandbox_content_body(&path, &key, api_version, page).await
}

/// Reads a contract HTML page, rewrites paths, and injects the WebSocket shim
/// and navigation interceptor.
async fn sandbox_content_body(
    path: &Path,
    contract_key: &str,
    api_version: ApiVersion,
    page: &str,
) -> Result<impl IntoResponse + use<>, WebSocketApiError> {
    // Sanitize the page path to prevent directory traversal
    let normalized = Path::new(page);
    for component in normalized.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(WebSocketApiError::InvalidParam {
                error_cause: "Path traversal not allowed".to_string(),
            });
        }
    }

    let mut web_path = path.join(page);
    // For directory-style paths, look for index.html inside the directory
    if web_path.is_dir() {
        web_path = web_path.join("index.html");
    }
    // Ensure the resolved path is still under the contract's cache directory
    let canonical_base = path
        .canonicalize()
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let canonical_file = web_path
        .canonicalize()
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("Page not found: {page} ({err})"),
        })?;
    if !canonical_file.starts_with(&canonical_base) {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "Path traversal not allowed".to_string(),
        });
    }

    let mut key_file = File::open(&web_path)
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let mut buf = vec![];
    key_file
        .read_to_end(&mut buf)
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let mut body = String::from_utf8(buf).map_err(|err| WebSocketApiError::NodeError {
        error_cause: format!("{err}"),
    })?;

    // Rewrite root-relative asset paths so they resolve under the contract's web prefix.
    // Dioxus generates paths like /./assets/app.js which browsers normalize to /assets/app.js
    // (root-relative). These bypass the /v1/contract/web/{key}/ prefix and 404.
    let version_prefix = api_version.prefix();
    let prefix = format!("/{version_prefix}/contract/web/{contract_key}/");
    body = body.replace("\"/./", &format!("\"{prefix}"));
    body = body.replace("'/./", &format!("'{prefix}"));

    // Inject the WebSocket shim and navigation interceptor before any other scripts.
    // The shim overrides window.WebSocket so that wasm-bindgen routes connections
    // through the shell page's bridge. The interceptor catches <a> clicks and
    // routes them through postMessage for multi-page navigation.
    let injected_scripts =
        format!("<script>{WEBSOCKET_SHIM_JS}</script><script>{NAVIGATION_INTERCEPTOR_JS}</script>");
    if let Some(pos) = body.find("</head>") {
        body.insert_str(pos, &injected_scripts);
    } else if let Some(pos) = body.find("<body") {
        body.insert_str(pos, &injected_scripts);
    } else {
        body = format!("{injected_scripts}{body}");
    }

    Ok(Html(body))
}

/// JavaScript for the shell page's postMessage bridge.
///
/// The bridge listens for WebSocket requests from the sandboxed iframe,
/// creates real WebSocket connections with the auth token injected, and
/// forwards messages in both directions. Only allows connections to the
/// local API server itself (same origin) to prevent the contract from using the
/// bridge as an open proxy to other localhost services.
const SHELL_BRIDGE_JS: &str = r#"
function freenetBridge(authToken) {
  'use strict';
  var LOCAL_API_ORIGIN = location.origin;
  var MAX_CONNECTIONS = 32;
  var iframe = document.getElementById('app');
  var connections = new Map();
  var lastClipboard = 0;

  // Build iframe src from data-src, appending any URL hash for deep
  // linking. Using data-src (not src) in the HTML means the iframe
  // doesn't start loading until we set .src here, so there is exactly
  // one load -- with the hash already in the URL.
  var iframeSrc = iframe.getAttribute('data-src');
  if (location.hash) {
    iframeSrc += location.hash.slice(0, 8192);
  }
  iframe.src = iframeSrc;

  function sendToIframe(msg) {
    iframe.contentWindow.postMessage(msg, '*');
  }

  window.addEventListener('message', function(event) {
    if (event.source !== iframe.contentWindow) return;
    var msg = event.data;
    if (!msg) return;

    // Handle shell-level messages (title, favicon) from iframe
    if (msg.__freenet_shell__) {
      if (msg.type === 'title' && typeof msg.title === 'string') {
        // Truncate to prevent UI spoofing with excessively long titles
        document.title = msg.title.slice(0, 128);
      } else if (msg.type === 'favicon' && typeof msg.href === 'string') {
        // Only allow https: and data: schemes to prevent exfiltration
        try {
          var scheme = msg.href.split(':')[0].toLowerCase();
          if (scheme !== 'https' && scheme !== 'data') return;
        } catch(e) { return; }
        var link = document.querySelector('link[rel="icon"]');
        if (link) link.href = msg.href;
      } else if (msg.type === 'hash' && typeof msg.hash === 'string') {
        // Only allow # fragments — reject anything that could modify path/query.
        // Note: replaceState (not pushState) is intentional — avoids polluting
        // browser history with every in-app route change. This also means
        // replaceState does NOT fire popstate or hashchange, preventing loops.
        var h = msg.hash.slice(0, 8192);
        if (h.length > 0 && h.charAt(0) === '#') {
          history.replaceState(null, '', h);
        }
      } else if (msg.type === 'clipboard' && typeof msg.text === 'string') {
        // Sandboxed iframes can't use navigator.clipboard due to permissions
        // policy. Proxy clipboard writes through the trusted shell instead.
        // Write-only — no readText proxy to prevent exfiltration.
        // Rate-limited to 1 write/sec to prevent clipboard spam from
        // malicious contracts. Requires transient user activation (browser
        // enforced) — works when the iframe sends this in a click handler.
        var now = Date.now();
        if (now - lastClipboard >= 1000) {
          lastClipboard = now;
          try { navigator.clipboard.writeText(msg.text.slice(0, 2048)); } catch(e) {}
        }
      } else if (msg.type === 'navigate' && typeof msg.href === 'string') {
        // In-contract page navigation for multi-page websites. The sandboxed
        // iframe cannot navigate itself (no allow-top-navigation), so it sends
        // navigation requests to the shell which updates iframe.src.
        // Security: only allows paths under the current contract's web prefix
        // to prevent cross-contract navigation or path traversal.
        try {
          var resolved = new URL(msg.href, iframe.src);
          // Only allow same-origin navigation (no cross-site)
          if (resolved.origin !== location.origin) return;
          var cleanPath = resolved.pathname;
          // Extract the contract web prefix from the iframe's data-src
          var dataSrc = iframe.getAttribute('data-src');
          var baseParts = dataSrc.match(/^(\/v[12]\/contract\/web\/[^/]+\/)/);
          if (!baseParts) return;
          var contractPrefix = baseParts[1];
          // Ensure navigation stays within the same contract
          if (cleanPath.indexOf(contractPrefix) !== 0) return;
          // Build new sandbox URL preserving __sandbox=1
          resolved.searchParams.set('__sandbox', '1');
          iframe.src = resolved.pathname + resolved.search;
        } catch(e) {}
      } else if (msg.type === 'open_url' && typeof msg.url === 'string') {
        // Open external URLs in a new tab. Popups from the sandboxed iframe
        // inherit the opaque origin, breaking CORS on target sites. The shell
        // opens the URL instead, giving proper origin. See issue #1499.
        // Security: only allow https: URLs that are NOT localhost/loopback.
        try {
          var u = new URL(msg.url);
          if (u.protocol !== 'https:') return;
          var h = u.hostname.toLowerCase();
          if (h === 'localhost' || h === '127.0.0.1' || h === '[::1]' || h === '0.0.0.0') return;
          window.open(u.href, '_blank', 'noopener,noreferrer');
        } catch(e) {}
      }
      return;
    }

    if (!msg.__freenet_ws__) return;

    switch (msg.type) {
      case 'open': {
        // Limit concurrent connections to prevent resource exhaustion
        if (connections.size >= MAX_CONNECTIONS) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Security: only allow WebSocket connections to the local API server itself.
        // Validate protocol explicitly and compare origin.
        try {
          var u = new URL(msg.url);
          if (u.protocol !== 'ws:' && u.protocol !== 'wss:') {
            sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
            return;
          }
          var httpProto = u.protocol === 'wss:' ? 'https:' : 'http:';
          if (httpProto + '//' + u.host !== LOCAL_API_ORIGIN) {
            sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
            return;
          }
        } catch(e) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Inject auth token into the WebSocket URL
        u.searchParams.set('authToken', authToken);
        var ws = new WebSocket(u.toString(), msg.protocols || undefined);
        ws.binaryType = 'arraybuffer';
        connections.set(msg.id, ws);

        ws.onopen = function() {
          sendToIframe({ __freenet_ws__: true, type: 'open', id: msg.id });
        };
        ws.onmessage = function(e) {
          var transfer = e.data instanceof ArrayBuffer ? [e.data] : [];
          iframe.contentWindow.postMessage({
            __freenet_ws__: true, type: 'message', id: msg.id, data: e.data
          }, '*', transfer);
        };
        ws.onclose = function(e) {
          sendToIframe({
            __freenet_ws__: true, type: 'close', id: msg.id,
            code: e.code, reason: e.reason
          });
          connections.delete(msg.id);
        };
        ws.onerror = function() {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          connections.delete(msg.id);
        };
        break;
      }
      case 'send': {
        var ws = connections.get(msg.id);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(msg.data);
        }
        break;
      }
      case 'close': {
        var ws = connections.get(msg.id);
        if (ws) {
          ws.close(msg.code, msg.reason);
          connections.delete(msg.id);
        }
        break;
      }
    }
  });

  // Forward runtime hash changes (browser back/forward, manual URL edits)
  function forwardHash() {
    if (location.hash) {
      sendToIframe({ __freenet_shell__: true, type: 'hash', hash: location.hash.slice(0, 8192) });
    }
  }
  window.addEventListener('popstate', forwardHash);
  window.addEventListener('hashchange', forwardHash);

  // Permission prompt polling: check for pending delegate permission requests
  // and show browser notifications. The user clicks the notification to open
  // the permission page in a new tab.
  var knownPrompts = {};
  function checkPermissions() {
    fetch('/permission/pending').then(function(r) { return r.json(); }).then(function(prompts) {
      prompts.forEach(function(p) {
        if (knownPrompts[p.nonce]) return;
        knownPrompts[p.nonce] = true;
        if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
          var n = new Notification('Freenet: Permission needed', {
            body: p.preview || 'A delegate is requesting permission.',
            tag: 'freenet-perm-' + p.nonce
          });
          n.onclick = function() { window.open('/permission/' + p.nonce, '_blank'); };
        } else {
          // Fallback: open directly if notifications not available
          window.open('/permission/' + p.nonce, '_blank');
        }
      });
      // Clean up nonces no longer pending
      Object.keys(knownPrompts).forEach(function(k) {
        if (!prompts.some(function(p) { return p.nonce === k; })) delete knownPrompts[k];
      });
    }).catch(function() {});
  }
  // Request notification permission on first load
  if (typeof Notification !== 'undefined' && Notification.permission === 'default') {
    Notification.requestPermission();
  }
  setInterval(checkPermissions, 3000);
}
"#;

/// JavaScript WebSocket shim injected into the sandboxed iframe content.
///
/// Overrides `window.WebSocket` so that `web_sys::WebSocket::new()` (which
/// compiles to `new WebSocket(url)` via wasm-bindgen, resolving from global
/// scope at call time) is intercepted and routed through postMessage to the
/// shell page's bridge.
const WEBSOCKET_SHIM_JS: &str = r#"
(function() {
  'use strict';
  var wsInstances = new Map();
  var idCounter = 0;

  function FreenetWebSocket(url, protocols) {
    this._id = '__fws_' + (++idCounter);
    this.url = url;
    this.readyState = 0;
    this.bufferedAmount = 0;
    this.extensions = '';
    this.protocol = '';
    this.binaryType = 'blob';
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    this._listeners = {};
    wsInstances.set(this._id, this);
    window.parent.postMessage({
      __freenet_ws__: true, type: 'open', id: this._id,
      url: url, protocols: protocols
    }, '*');
  }
  FreenetWebSocket.CONNECTING = 0;
  FreenetWebSocket.OPEN = 1;
  FreenetWebSocket.CLOSING = 2;
  FreenetWebSocket.CLOSED = 3;
  FreenetWebSocket.prototype.send = function(data) {
    if (this.readyState !== 1) throw new DOMException('WebSocket is not open', 'InvalidStateError');
    var transfer = data instanceof ArrayBuffer ? [data] : [];
    window.parent.postMessage({
      __freenet_ws__: true, type: 'send', id: this._id, data: data
    }, '*', transfer);
  };
  FreenetWebSocket.prototype.close = function(code, reason) {
    if (this.readyState >= 2) return;
    this.readyState = 2;
    window.parent.postMessage({
      __freenet_ws__: true, type: 'close', id: this._id, code: code, reason: reason
    }, '*');
  };
  FreenetWebSocket.prototype.addEventListener = function(type, listener) {
    if (!this._listeners[type]) this._listeners[type] = [];
    this._listeners[type].push(listener);
  };
  FreenetWebSocket.prototype.removeEventListener = function(type, listener) {
    if (!this._listeners[type]) return;
    this._listeners[type] = this._listeners[type].filter(function(l) { return l !== listener; });
  };
  FreenetWebSocket.prototype.dispatchEvent = function(event) {
    var handler = this['on' + event.type];
    if (handler) handler.call(this, event);
    var listeners = this._listeners[event.type];
    if (listeners) for (var i = 0; i < listeners.length; i++) listeners[i].call(this, event);
    return true;
  };

  window.addEventListener('message', function(event) {
    // Only accept messages from the parent shell page
    if (event.source !== window.parent) return;
    var msg = event.data;
    if (!msg || !msg.__freenet_ws__) return;
    var ws = wsInstances.get(msg.id);
    if (!ws) return;
    switch (msg.type) {
      case 'open':
        ws.readyState = 1;
        ws.dispatchEvent(new Event('open'));
        break;
      case 'message':
        var data = msg.data;
        if (ws.binaryType === 'blob' && data instanceof ArrayBuffer) data = new Blob([data]);
        ws.dispatchEvent(new MessageEvent('message', { data: data }));
        break;
      case 'close':
        ws.readyState = 3;
        ws.dispatchEvent(new CloseEvent('close', { code: msg.code, reason: msg.reason, wasClean: true }));
        wsInstances.delete(msg.id);
        break;
      case 'error':
        ws.dispatchEvent(new Event('error'));
        break;
    }
  });

  window.WebSocket = FreenetWebSocket;
  if (typeof globalThis !== 'undefined') globalThis.WebSocket = FreenetWebSocket;
})();
"#;

/// JavaScript navigation interceptor injected into sandboxed iframe HTML pages.
///
/// Intercepts clicks on `<a>` elements and sends a postMessage to the shell
/// page, which updates the iframe's `src` to navigate within the contract.
/// This enables multi-page website navigation without weakening the sandbox
/// (no `allow-top-navigation` needed). Only intercepts same-origin relative
/// links; external links are left to the existing `open_url` bridge.
const NAVIGATION_INTERCEPTOR_JS: &str = r#"
(function() {
  'use strict';
  document.addEventListener('click', function(e) {
    var target = e.target;
    // Walk up to find the nearest <a> element (handles clicks on child elements)
    while (target && target.tagName !== 'A') target = target.parentElement;
    if (!target || !target.href) return;
    // Skip links with explicit targets (e.g. target="_blank")
    if (target.target && target.target !== '_self') return;
    // Skip javascript: and mailto: links
    var protocol = target.protocol;
    if (protocol && protocol !== 'http:' && protocol !== 'https:') return;
    // Skip links with download attribute
    if (target.hasAttribute('download')) return;
    // Skip links explicitly marked to bypass interception
    if (target.dataset && target.dataset.freenetNoIntercept) return;
    // For cross-origin links, use the open_url bridge instead
    try {
      if (target.origin !== location.origin) {
        e.preventDefault();
        window.parent.postMessage({
          __freenet_shell__: true, type: 'open_url', url: target.href
        }, '*');
        return;
      }
    } catch(err) {}
    // Same-origin in-contract link: request navigation via shell
    e.preventDefault();
    window.parent.postMessage({
      __freenet_shell__: true, type: 'navigate', href: target.href
    }, '*');
  }, true);

  // Also intercept form-based navigation and programmatic location changes
  // by overriding window.location assignment (history.pushState is already
  // blocked by the sandbox without allow-same-origin).
})();
"#;

/// Extracts the relative file path from a contract web URI.
///
/// Strips the version and contract key prefix (e.g. `/v1/contract/web/{key}/`)
/// and returns the remaining path (e.g. `assets/app.js`).
fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let path_str = uri.path();

    let remainder = if let Some(rem) = path_str.strip_prefix("/v1/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v1/contract/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/") {
        rem
    } else {
        return Err(Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!(
                "URI path '{path_str}' does not start with /v1/contract/ or /v2/contract/"
            ),
        }));
    };

    // remainder contains "{key}/{path}" or just "{key}"
    let file_path = match remainder.split_once('/') {
        Some((_key, path)) => path.to_string(),
        None => "".to_string(),
    };

    Ok(file_path)
}

/// Returns the base directory for webapp cache.
/// Uses XDG cache directory (~/.cache/freenet on Linux) to avoid permission
/// conflicts when multiple users run freenet on the same machine.
fn webapp_cache_dir() -> PathBuf {
    directories::ProjectDirs::from("", "The Freenet Project Inc", "freenet")
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir().join("freenet"))
        .join("webapp_cache")
}

fn contract_web_path(instance_id: &ContractInstanceId) -> PathBuf {
    webapp_cache_dir().join(instance_id.encode())
}

fn hash_state(state: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(state);
    hasher.finish()
}

fn state_hash_path(instance_id: &ContractInstanceId) -> PathBuf {
    webapp_cache_dir().join(format!("{}.hash", instance_id.encode()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Extracts the response body as a UTF-8 string for test assertions.
    async fn response_body(resp: impl IntoResponse) -> String {
        let body = resp.into_response();
        let bytes = axum::body::to_bytes(body.into_body(), 1024 * 1024)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn root_relative_asset_paths_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";
        let html = r#"<!DOCTYPE html>
<html>
    <head>
        <title>Test</title>
    <link rel="preload" as="script" href="/./assets/app.js" crossorigin></head>
    <body><div id="main"></div>
    <script type="module" async src="/./assets/app.js"></script>
    </body>
</html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        let expected_href = format!("href=\"/v1/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected_href),
            "href not rewritten.\nGot: {result}"
        );

        let expected_src = format!("src=\"/v1/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected_src),
            "src not rewritten.\nGot: {result}"
        );

        // Original root-relative paths should be gone
        assert!(
            !result.contains("\"/./assets/"),
            "original /./assets/ paths still present"
        );

        // WebSocket shim should be injected instead of raw auth token
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected"
        );
    }

    #[tokio::test]
    async fn root_relative_asset_paths_rewritten_v2() {
        let dir = tempfile::tempdir().unwrap();
        let key = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";
        let html = r#"<head><link href="/./assets/app.js"></head><body></body>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V2, "index.html")
                .await
                .unwrap(),
        )
        .await;

        let expected = format!("href=\"/v2/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected),
            "V2 href not rewritten.\nGot: {result}"
        );
        assert!(
            !result.contains("\"/./assets/"),
            "original /./assets/ paths still present in V2"
        );
    }

    #[tokio::test]
    async fn single_quoted_paths_also_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html = "<head><script src='/./assets/app.js'></script></head>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        let expected = format!("'/v1/contract/web/{key}/assets/app.js'");
        assert!(
            result.contains(&expected),
            "single-quoted path not rewritten.\nGot: {result}"
        );
    }

    #[tokio::test]
    async fn paths_without_dot_slash_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Paths like "/assets/app.js" (without /.) should NOT be rewritten,
        // only the Dioxus-specific "/./assets/" pattern is targeted.
        let html = r#"<head><link href="/assets/app.css"></head><body></body>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        // The /assets/ path should remain unchanged (no /. prefix)
        assert!(
            result.contains("\"/assets/app.css\""),
            "path without /. was incorrectly rewritten.\nGot: {result}"
        );
    }

    #[tokio::test]
    async fn shell_page_contains_iframe_and_bridge() {
        let token = AuthToken::generate();
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None).unwrap()).await;

        // Shell page must contain sandboxed iframe
        assert!(
            html.contains(r#"sandbox="allow-scripts allow-forms allow-popups"#),
            "iframe sandbox attribute missing"
        );
        // Iframe src must include __sandbox=1
        assert!(
            html.contains("__sandbox=1"),
            "iframe src missing __sandbox=1 param"
        );
        // Bridge script must be present
        assert!(
            html.contains("freenetBridge"),
            "bridge script not found in shell page"
        );
        // Auth token must NOT be exposed as window.__FREENET_AUTH_TOKEN__
        assert!(
            !html.contains("__FREENET_AUTH_TOKEN__"),
            "auth token exposed in global variable (security risk)"
        );
        // Auth token should be passed to the bridge function
        assert!(
            html.contains(&format!("freenetBridge(\"{}\")", token.as_str())),
            "auth token not passed to bridge"
        );
        // Default title and favicon must be present
        assert!(
            html.contains("<title>Freenet</title>"),
            "shell page title mismatch"
        );
        assert!(
            html.contains(r#"<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,"#),
            "favicon should use inline data URI, not external URL"
        );
        assert!(
            !html.contains("freenet.org"),
            "shell page must not reference external origins (CORS)"
        );
        // Shell message handler must be present in bridge JS
        assert!(
            html.contains("__freenet_shell__"),
            "bridge JS must handle shell-level messages (title/favicon)"
        );
        // allow-popups-to-escape-sandbox must NOT be present. It was removed because
        // escaped popups gain localhost:7509 origin, allowing malicious web apps to
        // access other apps' data and bypass permission prompts. External links are
        // now opened via the open_url shell bridge message instead. See #1499.
        assert!(
            !html.contains("allow-popups-to-escape-sandbox"),
            "allow-popups-to-escape-sandbox must not be set (security: #1499)"
        );
        // open_url handler must be present in shell bridge JS for external links
        assert!(
            html.contains("open_url"),
            "shell bridge must handle open_url messages for external links"
        );
    }

    /// Regression test: the iframe must use data-src (not src) so JS can build
    /// the final URL with the hash fragment before triggering the first load.
    /// Previously, src was set in HTML and the hash was sent via postMessage on
    /// the load event, but WASM apps hadn't registered their listener yet.
    /// See: #3747 (comment)
    #[tokio::test]
    async fn shell_page_iframe_uses_data_src_for_deep_linking() {
        let token = AuthToken::generate();
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None).unwrap()).await;

        // The iframe must NOT have a src attribute (which would trigger an
        // immediate load before JS can append the hash fragment).
        assert!(
            !html.contains(
                r#"<iframe id="app" sandbox="allow-scripts allow-forms allow-popups" src="#
            ),
            "iframe must use data-src, not src, to avoid loading before JS appends the hash"
        );
        // The iframe must have data-src with the sandbox URL.
        assert!(
            html.contains("data-src=\"/"),
            "iframe must have data-src attribute for JS to read"
        );
    }

    #[tokio::test]
    async fn shell_page_forwards_query_params_to_iframe() {
        let token = AuthToken::generate();
        let qs = Some("invitation=abc123&room=test".to_string());
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs).unwrap()).await;

        // Query params should be forwarded to iframe src
        assert!(
            html.contains("invitation=abc123"),
            "invitation param not forwarded to iframe"
        );
        assert!(
            html.contains("room=test"),
            "room param not forwarded to iframe"
        );
        // __sandbox=1 must always be first
        assert!(
            html.contains("?__sandbox=1&"),
            "__sandbox=1 not first in iframe params"
        );
    }

    #[tokio::test]
    async fn sandbox_content_injects_ws_shim_not_auth_token() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html = r#"<!DOCTYPE html><html><head></head><body>Hello</body></html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        // WS shim must be injected
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected"
        );
        assert!(
            result.contains("window.WebSocket = FreenetWebSocket"),
            "WebSocket override not set"
        );
        // Auth token must NOT appear in sandbox content
        assert!(
            !result.contains("__FREENET_AUTH_TOKEN__"),
            "auth token leaked into sandbox content"
        );
    }

    #[tokio::test]
    async fn ws_shim_injected_without_head_tag() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // HTML with <body> but no </head> tag
        let html = "<body><div>Hello</div></body>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected when no </head> tag"
        );
        // Shim should appear before <body
        let shim_pos = result.find("FreenetWebSocket").unwrap();
        let body_pos = result.find("<body").unwrap();
        assert!(
            shim_pos < body_pos,
            "shim should be injected before <body> tag"
        );
    }

    #[tokio::test]
    async fn ws_shim_injected_in_minimal_html() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Minimal HTML with no <head> or <body> tags
        let html = "<div>Hello World</div>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected in minimal HTML"
        );
        // Shim should be prepended (appears before the content)
        assert!(
            result.starts_with("<script>"),
            "shim should be prepended to content when no head/body tags"
        );
    }

    #[tokio::test]
    async fn shell_page_strips_sandbox_prefixed_params() {
        let token = AuthToken::generate();
        let qs = Some("__sandbox_extra=evil&invitation=abc&__sandboxFoo=bar".to_string());
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs).unwrap()).await;

        // __sandbox-prefixed params must be stripped
        assert!(
            !html.contains("__sandbox_extra"),
            "__sandbox_extra param should be stripped"
        );
        assert!(
            !html.contains("__sandboxFoo"),
            "__sandboxFoo param should be stripped"
        );
        // Normal params should be forwarded
        assert!(
            html.contains("invitation=abc"),
            "normal param should be forwarded"
        );
    }

    #[tokio::test]
    async fn shell_page_escapes_html_in_query_params() {
        let token = AuthToken::generate();
        let qs = Some("foo=\"><script>alert(1)</script>".to_string());
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs).unwrap()).await;

        // The double quote and angle brackets must be escaped
        assert!(
            !html.contains("\"><script>alert"),
            "unescaped HTML injection in iframe src"
        );
        assert!(
            html.contains("&quot;"),
            "double quote should be HTML-escaped"
        );
    }

    #[test]
    fn bridge_js_contains_origin_check() {
        assert!(
            SHELL_BRIDGE_JS.contains("LOCAL_API_ORIGIN"),
            "bridge JS must validate WebSocket origin"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("u.protocol !== 'ws:'"),
            "bridge JS must explicitly check WebSocket protocol"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("MAX_CONNECTIONS"),
            "bridge JS must limit concurrent connections"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("connections.delete(msg.id)"),
            "bridge JS must clean up connections"
        );
        // Shell message handler must validate types and restrict favicon schemes
        assert!(
            SHELL_BRIDGE_JS.contains("typeof msg.title === 'string'"),
            "bridge JS must type-check title before setting"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("typeof msg.href === 'string'"),
            "bridge JS must type-check favicon href before setting"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("scheme !== 'https' && scheme !== 'data'"),
            "bridge JS must restrict favicon href to https/data schemes"
        );
        // Hash forwarding: iframe→shell must validate # prefix and truncate
        assert!(
            SHELL_BRIDGE_JS.contains("msg.type === 'hash'"),
            "bridge JS must handle hash shell messages"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("h.charAt(0) === '#'"),
            "bridge JS must require # prefix on hash values"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("location.hash.slice(0, 8192)"),
            "bridge JS must truncate hash to 8192 chars"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("history.replaceState"),
            "bridge JS must use replaceState (not pushState) to avoid polluting browser history"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("history.pushState"),
            "bridge JS must not use pushState — hash changes should replace, not push"
        );
        // Initial hash: built into iframe src from data-src for deep linking
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.getAttribute('data-src')"),
            "bridge JS must read base URL from data-src attribute"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.src = iframeSrc"),
            "bridge JS must set iframe src from data-src (single load, no race)"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("iframe.addEventListener('load'"),
            "bridge JS must NOT use load event (race with WASM init; hash is in iframe URL via data-src)"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("slice(0, 1024)"),
            "hash limit must be 8192, not 1024"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("popstate"),
            "bridge JS must forward hash on browser back/forward"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("hashchange"),
            "bridge JS must forward hash on manual URL fragment edits"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("if (location.hash)"),
            "bridge JS must not forward empty hash to iframe"
        );
        // Clipboard proxy: shell writes to clipboard on behalf of sandboxed iframe
        assert!(
            SHELL_BRIDGE_JS.contains("msg.type === 'clipboard'"),
            "bridge JS must handle clipboard shell messages"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("navigator.clipboard.writeText"),
            "bridge JS must proxy clipboard writes through the shell"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("msg.text.slice(0, 2048)"),
            "bridge JS must truncate clipboard text to 2048 chars"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("lastClipboard"),
            "bridge JS must rate-limit clipboard writes"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("clipboard.readText")
                && !SHELL_BRIDGE_JS.contains("clipboard.read("),
            "bridge JS must be clipboard write-only — no read access"
        );
    }

    #[test]
    fn shim_js_validates_message_source() {
        assert!(
            WEBSOCKET_SHIM_JS.contains("event.source !== window.parent"),
            "shim JS must validate message source"
        );
    }

    #[test]
    fn get_path_v1() {
        let req_path = "/v1/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
        let base_dir = PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/",
        );
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        let result = base_dir.join(parsed);
        assert_eq!(
            PathBuf::from(
                "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
            ),
            result
        );
    }

    #[test]
    fn get_path_v2() {
        let req_path = "/v2/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
        let base_dir = PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/",
        );
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        let result = base_dir.join(parsed);
        assert_eq!(
            PathBuf::from(
                "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
            ),
            result
        );
    }

    #[test]
    fn get_path_v2_web() {
        let req_path =
            "/v2/contract/web/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/assets/app.js";
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        assert_eq!(parsed, "assets/app.js");
    }

    #[test]
    fn get_file_path_rejects_unknown_version() {
        let req_path = "/v3/contract/web/somekey/assets/app.js";
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let result = get_file_path(uri);
        assert!(result.is_err(), "expected error for /v3/ prefix");
    }

    #[test]
    fn bridge_js_contains_navigate_handler() {
        // The shell bridge must handle 'navigate' messages for multi-page
        // website navigation within the sandboxed iframe (issue #3833).
        assert!(
            SHELL_BRIDGE_JS.contains("msg.type === 'navigate'"),
            "bridge JS must handle navigate shell messages"
        );
        // Navigate handler must validate the path stays within the contract prefix
        assert!(
            SHELL_BRIDGE_JS.contains("contractPrefix"),
            "navigate handler must extract and check the contract prefix"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("cleanPath.indexOf(contractPrefix) !== 0"),
            "navigate handler must reject paths outside the contract prefix"
        );
        // Navigate handler must add __sandbox=1 to the new URL
        assert!(
            SHELL_BRIDGE_JS.contains("resolved.searchParams.set('__sandbox', '1')"),
            "navigate handler must add __sandbox=1 to navigated URL"
        );
        // Navigate handler must validate same-origin
        assert!(
            SHELL_BRIDGE_JS.contains("resolved.origin !== location.origin"),
            "navigate handler must reject cross-origin navigation"
        );
    }

    #[test]
    fn navigation_interceptor_js_intercepts_clicks() {
        // The navigation interceptor must catch <a> clicks and route them
        // through postMessage for multi-page navigation (issue #3833).
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("document.addEventListener('click'"),
            "interceptor must listen for click events"
        );
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("type: 'navigate'"),
            "interceptor must send navigate messages to shell"
        );
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("__freenet_shell__: true"),
            "interceptor must use __freenet_shell__ namespace"
        );
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("e.preventDefault()"),
            "interceptor must prevent default link behavior"
        );
        // Cross-origin links should use open_url instead of navigate
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("type: 'open_url'"),
            "interceptor must route cross-origin links through open_url"
        );
        // Skip links with target="_blank" etc.
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("target.target"),
            "interceptor must respect target attribute on links"
        );
        // Must walk up DOM to handle clicks on child elements of <a>
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("target.parentElement"),
            "interceptor must walk up DOM to find <a> ancestor"
        );
    }

    #[tokio::test]
    async fn sandbox_content_serves_sub_pages() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Create a sub-page
        let sub_html = r#"<!DOCTYPE html><html><head></head><body><h1>News</h1></body></html>"#;
        std::fs::write(dir.path().join("news.html"), sub_html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "news.html")
                .await
                .unwrap(),
        )
        .await;

        // Sub-page content must be served
        assert!(
            result.contains("<h1>News</h1>"),
            "sub-page content not served"
        );
        // WebSocket shim must be injected
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected in sub-page"
        );
        // Navigation interceptor must be injected
        assert!(
            result.contains("type: 'navigate'"),
            "navigation interceptor not injected in sub-page"
        );
    }

    #[tokio::test]
    async fn sandbox_content_serves_directory_index() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Create a subdirectory with index.html
        std::fs::create_dir(dir.path().join("news")).unwrap();
        let sub_html =
            r#"<!DOCTYPE html><html><head></head><body><h1>News Index</h1></body></html>"#;
        std::fs::write(dir.path().join("news/index.html"), sub_html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "news")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("<h1>News Index</h1>"),
            "directory index.html not served"
        );
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected in directory index"
        );
    }

    #[tokio::test]
    async fn sandbox_content_rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        std::fs::write(dir.path().join("index.html"), "<html></html>").unwrap();

        // Attempting to traverse above the contract directory must fail
        let result =
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "../../../etc/passwd").await;
        assert!(result.is_err(), "path traversal should be rejected");
    }

    #[tokio::test]
    async fn sandbox_content_injects_navigation_interceptor() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html = r#"<!DOCTYPE html><html><head></head><body>Hello</body></html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        // Navigation interceptor must be injected alongside WebSocket shim
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected"
        );
        assert!(
            result.contains("type: 'navigate'"),
            "navigation interceptor not injected"
        );
    }
}
