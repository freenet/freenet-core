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
    // Sanitize the page path to prevent directory traversal and absolute paths.
    // Path::join with an absolute path replaces the base entirely on Unix,
    // so we must reject absolute paths, parent directory components, and root
    // directory components before joining.
    let normalized = Path::new(page);
    for component in normalized.components() {
        if matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::RootDir
        ) {
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

    // Open the canonical path (not the user-supplied path) to prevent TOCTOU
    // attacks where a symlink could be swapped between canonicalize and open.
    let mut key_file =
        File::open(&canonical_file)
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
  var iframeDataSrc = iframe.getAttribute('data-src');
  // Cache the contract web prefix once; used by nav/popstate path validation.
  var contractPrefixMatch = iframeDataSrc.match(/^(\/v[12]\/contract\/web\/[^/]+\/)/);
  var contractPrefix = contractPrefixMatch ? contractPrefixMatch[1] : null;
  var iframeSrc = iframeDataSrc;
  if (location.hash) {
    iframeSrc += location.hash.slice(0, 8192);
  }
  iframe.src = iframeSrc;
  // Seed history state so that back-navigating to the initial entry still
  // has an identifiable __freenet_nav__ record. Using replaceState avoids
  // adding a new entry — we just tag the existing one.
  if (contractPrefix) {
    try {
      history.replaceState(
        { __freenet_nav__: true, iframePath: iframeSrc },
        ''
      );
    } catch(e) {}
  }

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
          // Preserve the existing state object (which may carry our
          // __freenet_nav__ marker) so popstate can still restore the iframe.
          // If the current entry is tagged, also update its iframePath to
          // include the new fragment — otherwise back/forward would restore
          // the iframe without the user's current fragment position.
          var curState = history.state;
          if (curState && curState.__freenet_nav__ === true &&
              typeof curState.iframePath === 'string') {
            var basePath = curState.iframePath.split('#')[0];
            history.replaceState(
              { __freenet_nav__: true, iframePath: basePath + h },
              '',
              h
            );
          } else {
            history.replaceState(history.state, '', h);
          }
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
        // Cap href length to prevent a malicious contract from bloating
        // history.state or the address bar with arbitrarily large URLs.
        if (msg.href.length > 4096) return;
        try {
          var resolved = new URL(msg.href, iframe.src);
          // Only allow same-origin navigation (no cross-site)
          if (resolved.origin !== location.origin) return;
          var cleanPath = resolved.pathname;
          // Ensure navigation stays within the same contract
          if (!contractPrefix || cleanPath.indexOf(contractPrefix) !== 0) return;
          // Close any open WebSocket connections from the previous page to
          // prevent resource leaks. The old iframe document will be destroyed
          // when src changes, orphaning any connection callbacks.
          connections.forEach(function(ws) { try { ws.close(); } catch(e) {} });
          connections.clear();
          // Cap the hash component to match the 8192-byte cap used by the
          // hash-forwarding path; the iframe path is stored in history.state
          // so unbounded hashes would bloat the per-tab history record.
          var cappedHash = resolved.hash ? resolved.hash.slice(0, 8192) : '';
          // Build new sandbox URL preserving __sandbox=1
          resolved.searchParams.set('__sandbox', '1');
          var newIframePath = resolved.pathname + resolved.search + cappedHash;
          iframe.src = newIframePath;
          // Push a history entry so the browser back/forward buttons navigate
          // between visited subpages, and update the address bar to the
          // non-sandbox URL. The sandbox flag is intentionally omitted from the
          // outer URL; the shell always re-adds it when loading the iframe.
          // See issue #3839.
          try {
            history.pushState(
              { __freenet_nav__: true, iframePath: newIframePath },
              '',
              cleanPath + cappedHash
            );
          } catch(e) {}
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
  // popstate fires when the user presses back/forward. If the popped entry
  // carries our __freenet_nav__ marker, restore the iframe to the matching
  // subpage. Otherwise, fall back to forwarding the hash. See issue #3839.
  window.addEventListener('popstate', function(ev) {
    var state = ev.state;
    if (state && state.__freenet_nav__ === true && typeof state.iframePath === 'string') {
      // Security: path must still live under this contract's web prefix.
      // A stale state object from a different contract must not be able to
      // redirect the iframe elsewhere.
      if (contractPrefix && state.iframePath.indexOf(contractPrefix) === 0) {
        // No-op if the iframe is already on the target path (e.g. popstate
        // fired from a bfcache restore where iframe state was retained).
        // This avoids a spurious reload that would tear down live WebSocket
        // connections unnecessarily.
        if (iframe.src.indexOf(state.iframePath) === -1) {
          connections.forEach(function(ws) { try { ws.close(); } catch(e) {} });
          connections.clear();
          iframe.src = state.iframePath;
        }
        return;
      }
    }
    forwardHash();
  });
  window.addEventListener('hashchange', forwardHash);

  // Permission prompt overlay: render a modal in the shell page's DOM
  // (outside the sandboxed iframe) whenever a delegate permission prompt
  // is pending. The shell is trusted and same-origin with the gateway, so
  // the sandboxed contract cannot reach into this DOM. See issue #3836.
  //
  // Every open Freenet tab renders the overlay for every pending prompt.
  // When the user responds in one tab, the gateway removes the nonce from
  // the pending registry; other tabs see it disappear on their next poll
  // and hide their overlays automatically.
  var overlayRoot = null;
  var overlayCards = {}; // nonce -> card element
  var OVERLAY_CSS =
    '#__freenet_perm_overlay{position:fixed;inset:0;z-index:2147483647;' +
    'background:rgba(8,10,14,0.62);backdrop-filter:blur(4px);' +
    '-webkit-backdrop-filter:blur(4px);display:none;align-items:center;' +
    'justify-content:center;padding:20px;overflow:auto;' +
    'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}' +
    '#__freenet_perm_overlay .fn-card{--bg:#0f1419;--fg:#e6e8eb;--card:#1a2028;' +
    '--accent:#3b82f6;--border:#2d3748;--warn:#f59e0b;--muted:#9ca3af;' +
    'background:var(--card);color:var(--fg);border:1px solid var(--border);' +
    'border-radius:14px;padding:28px;max-width:520px;width:100%;margin:12px 0;' +
    'box-shadow:0 12px 40px rgba(0,0,0,0.5);box-sizing:border-box;}' +
    '@media (prefers-color-scheme: light){#__freenet_perm_overlay .fn-card{' +
    '--bg:#f5f5f5;--fg:#1a1a1a;--card:#ffffff;--accent:#2563eb;' +
    '--border:#d1d5db;--warn:#d97706;--muted:#6b7280;' +
    'box-shadow:0 12px 40px rgba(0,0,0,0.18);}}' +
    '#__freenet_perm_overlay .fn-header{display:flex;align-items:center;gap:12px;' +
    'margin-bottom:18px;}' +
    '#__freenet_perm_overlay .fn-icon{font-size:28px;line-height:1;}' +
    '#__freenet_perm_overlay .fn-title{font-size:18px;font-weight:600;margin:0;' +
    'color:var(--fg);}' +
    '#__freenet_perm_overlay .fn-ctx{background:var(--bg);border:1px solid var(--border);' +
    'border-radius:8px;padding:12px 14px;margin-bottom:16px;font-size:12px;' +
    'color:var(--muted);}' +
    '#__freenet_perm_overlay .fn-ctx dt{font-weight:600;color:var(--fg);' +
    'font-family:inherit;margin-top:6px;}' +
    '#__freenet_perm_overlay .fn-ctx dt:first-child{margin-top:0;}' +
    '#__freenet_perm_overlay .fn-ctx dd{margin:2px 0 0 0;font-family:ui-monospace,' +
    'SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;word-break:break-all;}' +
    '#__freenet_perm_overlay .fn-msg-label{font-size:11px;color:var(--muted);' +
    'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;}' +
    '#__freenet_perm_overlay .fn-msg{font-size:15px;line-height:1.5;margin:0 0 22px 0;' +
    'padding:14px 16px;background:var(--bg);border-left:3px solid var(--warn);' +
    'border-radius:4px;white-space:pre-wrap;word-wrap:break-word;color:var(--fg);}' +
    '#__freenet_perm_overlay .fn-btns{display:flex;gap:10px;flex-wrap:wrap;}' +
    '#__freenet_perm_overlay .fn-btn{padding:10px 20px;border-radius:8px;' +
    'font-size:14px;cursor:pointer;flex:1;min-width:100px;font-weight:500;' +
    'border:1px solid var(--border);background:var(--card);color:var(--fg);' +
    'transition:transform 0.12s, opacity 0.12s, filter 0.12s;font-family:inherit;}' +
    '#__freenet_perm_overlay .fn-btn.primary{background:var(--accent);' +
    'color:#fff;border-color:var(--accent);}' +
    '#__freenet_perm_overlay .fn-btn:hover:not(:disabled){transform:translateY(-1px);' +
    'filter:brightness(1.08);}' +
    '#__freenet_perm_overlay .fn-btn:disabled{opacity:0.55;cursor:not-allowed;}' +
    '#__freenet_perm_overlay .fn-timer{margin-top:14px;font-size:12px;' +
    'color:var(--muted);text-align:center;}';
  // Auto-deny duration in seconds, mirroring the standalone /permission/{nonce}
  // fallback page. Tracked client-side only; the server enforces the real
  // timeout and will clear the nonce regardless.
  var OVERLAY_AUTO_DENY_SECONDS = 60;
  function ensureOverlayRoot() {
    if (overlayRoot) return overlayRoot;
    var style = document.createElement('style');
    style.textContent = OVERLAY_CSS;
    document.head.appendChild(style);
    overlayRoot = document.createElement('div');
    overlayRoot.id = '__freenet_perm_overlay';
    overlayRoot.setAttribute('role', 'dialog');
    overlayRoot.setAttribute('aria-modal', 'true');
    overlayRoot.setAttribute('aria-label', 'Delegate permission request');
    document.body.appendChild(overlayRoot);
    // Escape-to-dismiss: routes to the last button in the most-recently-added
    // card, which (by the standard delegate convention Allow Once / Always
    // Allow / Deny) is the Deny button. If the delegate supplied a single
    // label this is a no-op — Escape just does nothing.
    document.addEventListener('keydown', function(e) {
      if (e.key !== 'Escape') return;
      if (!overlayRoot || overlayRoot.style.display === 'none') return;
      var nonces = Object.keys(overlayCards);
      if (nonces.length === 0) return;
      var nonce = nonces[nonces.length - 1];
      var card = overlayCards[nonce];
      var btns = card.querySelectorAll('button');
      if (btns.length < 2) return; // no non-primary option, ignore
      btns[btns.length - 1].click();
      e.preventDefault();
    });
    return overlayRoot;
  }
  function setText(el, text) {
    // textContent avoids any HTML interpretation of delegate-controlled
    // strings. Delegate-provided fields are never parsed as markup.
    el.textContent = text == null ? '' : String(text);
  }
  function createCard(p) {
    var card = document.createElement('div');
    card.className = 'fn-card';
    card.setAttribute('data-nonce', p.nonce);

    var header = document.createElement('div');
    header.className = 'fn-header';
    var icon = document.createElement('span');
    icon.className = 'fn-icon';
    icon.textContent = '\u{1F512}';
    var title = document.createElement('h1');
    title.className = 'fn-title';
    title.textContent = 'Permission Request';
    header.appendChild(icon);
    header.appendChild(title);
    card.appendChild(header);

    var ctx = document.createElement('dl');
    ctx.className = 'fn-ctx';
    var dkLabel = document.createElement('dt');
    dkLabel.textContent = 'Delegate';
    var dkVal = document.createElement('dd');
    setText(dkVal, p.delegate_key || 'Unknown');
    var ciLabel = document.createElement('dt');
    ciLabel.textContent = 'Requesting contract';
    var ciVal = document.createElement('dd');
    setText(ciVal, p.contract_id || 'Unknown');
    ctx.appendChild(dkLabel);
    ctx.appendChild(dkVal);
    ctx.appendChild(ciLabel);
    ctx.appendChild(ciVal);
    card.appendChild(ctx);

    var msgLabel = document.createElement('div');
    msgLabel.className = 'fn-msg-label';
    msgLabel.textContent = 'Delegate says';
    card.appendChild(msgLabel);
    var msg = document.createElement('p');
    msg.className = 'fn-msg';
    setText(msg, p.message || 'A delegate is requesting permission.');
    card.appendChild(msg);

    var buttons = document.createElement('div');
    buttons.className = 'fn-btns';
    var labels = Array.isArray(p.labels) && p.labels.length > 0 ? p.labels : ['OK'];
    labels.forEach(function(label, idx) {
      var b = document.createElement('button');
      b.className = 'fn-btn' + (idx === 0 ? ' primary' : '');
      setText(b, label);
      b.addEventListener('click', function() {
        respondToPrompt(p.nonce, idx, card);
      });
      buttons.appendChild(b);
    });
    card.appendChild(buttons);

    // Countdown mirroring the standalone permission page. The real timeout
    // lives server-side; this is a hint for the user that the prompt won't
    // wait forever. On expiry the next poll drops the card via the
    // reconciliation path, so we don't need a local hide here.
    var timer = document.createElement('div');
    timer.className = 'fn-timer';
    var remaining = OVERLAY_AUTO_DENY_SECONDS;
    timer.textContent = 'Auto-deny in ' + remaining + 's';
    card._fnTimerId = setInterval(function() {
      remaining -= 1;
      if (remaining <= 0) {
        clearInterval(card._fnTimerId);
        timer.textContent = 'Auto-denied';
        return;
      }
      timer.textContent = 'Auto-deny in ' + remaining + 's';
    }, 1000);
    card.appendChild(timer);
    return card;
  }
  function showCard(nonce, card) {
    var root = ensureOverlayRoot();
    root.appendChild(card);
    root.style.display = 'flex';
    overlayCards[nonce] = card;
    // Move keyboard focus to the primary button so Enter/Space answer the
    // prompt without requiring a mouse click.
    var primary = card.querySelector('.fn-btn.primary');
    if (primary && typeof primary.focus === 'function') {
      try { primary.focus(); } catch (e) {}
    }
  }
  function hideCard(nonce) {
    var card = overlayCards[nonce];
    if (!card) return;
    if (card._fnTimerId) {
      clearInterval(card._fnTimerId);
      card._fnTimerId = null;
    }
    if (card.parentNode) card.parentNode.removeChild(card);
    delete overlayCards[nonce];
    if (overlayRoot && Object.keys(overlayCards).length === 0) {
      overlayRoot.style.display = 'none';
    }
  }
  function respondToPrompt(nonce, index, card) {
    var btns = card.querySelectorAll('button');
    btns.forEach(function(b) { b.disabled = true; b.style.opacity = '0.5'; });
    fetch('/permission/' + encodeURIComponent(nonce) + '/respond', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ index: index })
    }).then(function(r) {
      // 404 means another tab already answered (or it auto-denied) — hide
      // the overlay here as well so the user isn't staring at a dead button.
      if (r.ok || r.status === 404) {
        hideCard(nonce);
      } else {
        btns.forEach(function(b) { b.disabled = false; b.style.opacity = '1'; });
      }
    }).catch(function() {
      btns.forEach(function(b) { b.disabled = false; b.style.opacity = '1'; });
    });
  }
  function checkPermissions() {
    // Skip polling when the tab is hidden: saves bandwidth and gateway
    // load when the user has many Freenet tabs open in the background.
    // The tab picks up the prompt again on its next visibility event.
    if (typeof document !== 'undefined' &&
        document.visibilityState === 'hidden' &&
        Object.keys(overlayCards).length === 0) {
      return;
    }
    fetch('/permission/pending').then(function(r) { return r.json(); }).then(function(prompts) {
      if (!Array.isArray(prompts)) return;
      var seen = {};
      prompts.forEach(function(p) {
        if (!p || typeof p.nonce !== 'string') return;
        seen[p.nonce] = true;
        if (overlayCards[p.nonce]) return;
        var card = createCard(p);
        showCard(p.nonce, card);
      });
      // Any card whose nonce is no longer pending was answered elsewhere
      // (another tab, the 60s auto-deny, or delegate cancelled). Remove it
      // so the user isn't clicking a dead button.
      Object.keys(overlayCards).forEach(function(nonce) {
        if (!seen[nonce]) hideCard(nonce);
      });
    }).catch(function() {});
  }
  setInterval(checkPermissions, 3000);
  checkPermissions();
  // Re-check as soon as the tab becomes visible again so a user returning
  // to a background tab sees any prompt that accumulated while they were
  // away without waiting for the next poll tick.
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('visibilitychange', function() {
      if (document.visibilityState === 'visible') checkPermissions();
    });
  }
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

    /// Regression test for issue #3836: permission prompts must render as an
    /// in-page overlay in the shell DOM, NOT via browser Notifications (which
    /// users block, miss, or dismiss accidentally).
    #[tokio::test]
    async fn shell_page_permission_overlay_present_and_safe() {
        let token = AuthToken::generate();
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None).unwrap()).await;

        // Overlay root and accessibility attributes
        assert!(
            html.contains("__freenet_perm_overlay"),
            "permission overlay root element missing from shell JS"
        );
        assert!(
            html.contains("'role', 'dialog'") || html.contains("\"role\", \"dialog\""),
            "overlay must declare role=dialog for a11y"
        );
        assert!(html.contains("aria-modal"), "overlay must set aria-modal");
        // Polls the new pending endpoint and POSTs back with the response.
        assert!(
            html.contains("/permission/pending"),
            "shell JS must poll /permission/pending"
        );
        assert!(
            html.contains("/respond"),
            "shell JS must POST to /permission/{{nonce}}/respond"
        );
        // The 404 branch is the cross-tab dismissal contract: "another tab
        // answered, hide my card".
        assert!(
            html.contains("r.status === 404"),
            "shell JS must treat 404 on respond as 'already answered' and hide the card"
        );
        // All delegate-controlled strings must go through textContent, never
        // innerHTML — guards against a future refactor re-opening XSS into
        // the trusted shell origin.
        assert!(
            html.contains("function setText(el, text)"),
            "setText helper (textContent-only) missing"
        );
        // innerHTML must not appear anywhere in the overlay code path. It
        // can appear elsewhere in the shell JS if needed, but this test
        // enforces that the overlay diff doesn't introduce it.
        let overlay_start = html.find("__freenet_perm_overlay").unwrap();
        let overlay_end = html[overlay_start..]
            .find("setInterval(checkPermissions")
            .unwrap();
        let overlay_slice = &html[overlay_start..overlay_start + overlay_end];
        assert!(
            !overlay_slice.contains("innerHTML"),
            "overlay code path must not use innerHTML (XSS surface)"
        );

        // The old Notification flow must be gone: no requestPermission(),
        // no new Notification(...), no window.open('/permission/').
        assert!(
            !html.contains("Notification.requestPermission"),
            "browser Notification permission request must be removed (#3836)"
        );
        assert!(
            !html.contains("new Notification("),
            "browser Notification construction must be removed (#3836)"
        );
        assert!(
            !html.contains("window.open('/permission/")
                && !html.contains("window.open(\"/permission/"),
            "shell must no longer open /permission/{{nonce}} as a popup (#3836)"
        );
        // Visibility gating keeps background tabs from polling forever.
        assert!(
            html.contains("visibilityState"),
            "overlay polling should be gated on document.visibilityState"
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
    async fn sandbox_content_injects_shims_not_auth_token() {
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
        // Navigation interceptor must be injected alongside WebSocket shim
        assert!(
            result.contains("type: 'navigate'"),
            "navigation interceptor not injected"
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
            "bridge JS must use replaceState for hash updates to avoid polluting browser history"
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
    async fn sandbox_content_rejects_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        std::fs::write(dir.path().join("index.html"), "<html></html>").unwrap();

        // Absolute paths would make Path::join replace the base directory entirely,
        // so they must be rejected by the component check.
        let result = sandbox_content_body(dir.path(), key, ApiVersion::V1, "/etc/passwd").await;
        assert!(result.is_err(), "absolute path should be rejected");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sandbox_content_rejects_symlink_escape() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.html"), "<html>secret</html>").unwrap();

        // Create a symlink inside the contract directory pointing outside it.
        // The canonicalize + starts_with check must catch this even though the
        // component-level ParentDir check would not.
        std::os::unix::fs::symlink(
            outside.path().join("secret.html"),
            dir.path().join("escape.html"),
        )
        .unwrap();

        let result = sandbox_content_body(dir.path(), key, ApiVersion::V1, "escape.html").await;
        assert!(result.is_err(), "symlink escape should be rejected");
    }

    #[test]
    fn bridge_js_navigate_pushes_history_state() {
        // Regression test for #3839: in-contract navigation must push a browser
        // history entry so back/forward works and the address bar updates.
        assert!(
            SHELL_BRIDGE_JS.contains("history.pushState"),
            "navigate handler must push a history entry"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("__freenet_nav__: true"),
            "history state must be tagged with __freenet_nav__ so popstate can recognise it"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("iframePath: newIframePath"),
            "history state must carry the iframe sandbox URL for popstate restore"
        );
        // The pushState URL must be the clean path (without __sandbox=1) so the
        // address bar shows the user-visible subpage URL, not the sandbox flag.
        assert!(
            SHELL_BRIDGE_JS.contains("cleanPath + cappedHash"),
            "pushState URL must be the clean (non-sandbox) path"
        );
    }

    #[test]
    fn bridge_js_popstate_restores_iframe_from_state() {
        // Regression test for #3839: browser back/forward must restore the
        // iframe to the previously-visited subpage by reading history state.
        assert!(
            SHELL_BRIDGE_JS.contains("addEventListener('popstate'"),
            "bridge JS must listen for popstate events"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("state.__freenet_nav__ === true"),
            "popstate handler must check for the __freenet_nav__ marker"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("state.iframePath.indexOf(contractPrefix) === 0"),
            "popstate handler must validate the restored iframe path stays under the contract prefix"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.src = state.iframePath"),
            "popstate handler must restore iframe.src from state"
        );
    }

    #[test]
    fn bridge_js_seeds_initial_history_state() {
        // Regression test for #3839: the initial history entry must carry the
        // __freenet_nav__ marker so that navigating back to the first page
        // still restores the iframe via popstate.
        assert!(
            SHELL_BRIDGE_JS.contains("history.replaceState"),
            "bridge JS must seed history state on load"
        );
        // The replaceState call for hash forwarding must preserve existing
        // state (history.state) rather than passing null, or it would wipe the
        // __freenet_nav__ marker and break back-navigation.
        assert!(
            SHELL_BRIDGE_JS.contains("history.replaceState(history.state"),
            "hash replaceState must preserve the existing state object"
        );
    }

    #[test]
    fn bridge_js_navigate_caps_href_length() {
        // Prevent a malicious contract from bloating history.state / URL by
        // spamming arbitrarily large navigate hrefs.
        assert!(
            SHELL_BRIDGE_JS.contains("msg.href.length > 4096"),
            "navigate handler must cap msg.href length"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("resolved.hash.slice(0, 8192)"),
            "navigate handler must cap the hash component stored in history.state"
        );
    }

    #[test]
    fn bridge_js_hash_update_syncs_nav_state() {
        // When the iframe sends a hash update while sitting on a pushState
        // entry, the stored iframePath must be refreshed to include the new
        // fragment — otherwise back/forward loses the user's fragment.
        assert!(
            SHELL_BRIDGE_JS.contains("curState.__freenet_nav__ === true"),
            "hash handler must detect tagged nav state"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("basePath + h"),
            "hash handler must rewrite iframePath with the new fragment"
        );
    }

    #[test]
    fn bridge_js_popstate_skips_reload_when_iframe_on_target() {
        // bfcache restore can fire popstate while the iframe is already on
        // the target path. Re-assigning iframe.src would tear down live
        // WebSockets for no reason.
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.src.indexOf(state.iframePath) === -1"),
            "popstate handler must skip reload when iframe is already on the target"
        );
    }

    #[test]
    fn bridge_js_cleans_up_websockets_on_navigate() {
        // When navigating to a new page, existing WebSocket connections must be
        // closed to prevent resource leaks from orphaned connections.
        assert!(
            SHELL_BRIDGE_JS.contains("connections.forEach"),
            "navigate handler must close existing WebSocket connections"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("connections.clear()"),
            "navigate handler must clear the connections map"
        );
    }
}
