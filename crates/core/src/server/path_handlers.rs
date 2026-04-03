//! Handle the `web` part of the bundles.
//!
//! Contract web apps are served inside sandboxed iframes to provide origin isolation.
//! The local API server returns a "shell" page that holds the auth token and
//! proxies WebSocket connections via postMessage, while the contract runs in an
//! `<iframe sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox">`
//! with an opaque origin that cannot access other contracts' data.
//! `allow-popups-to-escape-sandbox` lets external links open normally; sandbox content
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
<iframe id="app" sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox" src="{iframe_src}"></iframe>
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
#[instrument(level = "debug")]
pub(super) async fn serve_sandbox_content(
    key: String,
    api_version: ApiVersion,
) -> Result<impl IntoResponse, WebSocketApiError> {
    debug!("serve_sandbox_content: serving iframe content for key: {key}");
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
    sandbox_content_body(&path, &key, api_version).await
}

/// Reads the contract's index.html, rewrites paths, and injects the WebSocket shim.
async fn sandbox_content_body(
    path: &Path,
    contract_key: &str,
    api_version: ApiVersion,
) -> Result<impl IntoResponse + use<>, WebSocketApiError> {
    let web_path = path.join("index.html");
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

    // Inject the WebSocket shim before any other scripts. The shim overrides
    // window.WebSocket so that wasm-bindgen (which calls `new WebSocket(url)` from
    // global scope at call time) routes connections through the shell page's bridge.
    let shim_script = format!("<script>{WEBSOCKET_SHIM_JS}</script>");
    if let Some(pos) = body.find("</head>") {
        body.insert_str(pos, &shim_script);
    } else if let Some(pos) = body.find("<body") {
        body.insert_str(pos, &shim_script);
    } else {
        body = format!("{shim_script}{body}");
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
        // Truncate to 128 chars to match the title length limit.
        var h = msg.hash.slice(0, 128);
        if (h.length > 0 && h.charAt(0) === '#') {
          history.replaceState(null, '', h);
        }
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

  // Forward the shell's hash to the iframe on load so deep links work.
  // Also listen for browser back/forward navigation (popstate) to keep
  // the iframe in sync when the user navigates via browser controls.
  function forwardHash() {
    if (location.hash) {
      sendToIframe({ __freenet_shell__: true, type: 'hash', hash: location.hash });
    }
  }
  iframe.addEventListener('load', forwardHash);
  window.addEventListener('popstate', forwardHash);
  window.addEventListener('hashchange', forwardHash);
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
            sandbox_content_body(dir.path(), key, ApiVersion::V1)
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
            sandbox_content_body(dir.path(), key, ApiVersion::V2)
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
            sandbox_content_body(dir.path(), key, ApiVersion::V1)
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
            sandbox_content_body(dir.path(), key, ApiVersion::V1)
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
        // allow-popups-to-escape-sandbox must be present so that links opened
        // from within the sandbox behave as normal browser tabs (without inheriting
        // the sandbox's null origin, which breaks CORS on the target site). See #3613.
        assert!(
            html.contains("allow-popups-to-escape-sandbox"),
            "allow-popups-to-escape-sandbox must be set so external links work (#3613)"
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
            sandbox_content_body(dir.path(), key, ApiVersion::V1)
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
            sandbox_content_body(dir.path(), key, ApiVersion::V1)
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
            sandbox_content_body(dir.path(), key, ApiVersion::V1)
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
            SHELL_BRIDGE_JS.contains("msg.hash.slice(0, 128)"),
            "bridge JS must truncate hash to 128 chars"
        );
        // Hash forwarding: shell→iframe on load for deep linking
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.addEventListener('load'"),
            "bridge JS must forward hash to iframe on load"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("popstate"),
            "bridge JS must forward hash on browser back/forward"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("hashchange"),
            "bridge JS must forward hash on manual URL fragment edits"
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
}
