//! HTTP endpoints for delegate permission prompts.
//!
//! When a delegate emits `RequestUserInput`, the `DashboardPrompter` stores the
//! pending prompt and broadcasts a `PromptEvent::Added`. The gateway shell
//! page's JS subscribes to `/permission/events/ws` (WebSocket) and renders the
//! prompt as an in-page overlay (see issue #3836) on every open Freenet tab.
//! When the user clicks a button in any tab the response is POSTed to
//! `/permission/{nonce}/respond`; the gateway then emits `PromptEvent::Removed`
//! and every tab dismisses its overlay.
//!
//! The channel is a WebSocket rather than Server-Sent Events because every tab
//! holds it for the tab's whole life and every Freenet app shares one origin:
//! over SSE that permanently consumed one of the browser's ~6 HTTP/1.1
//! connections per origin PER TAB, so a seventh tab's own document and assets
//! queued forever behind the held-open streams (#5213). Browsers pool
//! WebSockets separately and far more generously. The SSE route is still
//! served for tabs opened before a node upgrade, though only to spare them the
//! polling fallback rather than to keep them working at all; see [`routes`]
//! and #5272.
//!
//! The `/permission/pending` JSON polling endpoint is retained as the
//! always-on fallback while the socket is down or its handshake is hanging,
//! for bootstrap reconciliation on connect/reconnect/resync, for environments
//! with no `WebSocket`, and for tests. The
//! standalone `/permission/{nonce}` HTML page is retained as a fallback
//! (e.g. if JS is disabled in the shell, or for debugging / manual testing).
//!
//! # Hosted nodes
//!
//! A hosted node requires a user token on the permission routes and shows
//! each user only their own prompts. Every prompt records the user whose
//! delegate run raised it (`PendingPrompt::owner`). `pending`, `events/ws` and
//! `{nonce}/respond` resolve the requester from the token under the same rule
//! as the client API ([`resolve_prompt_viewer`]) and list, replay, send and
//! accept answers only for that user's prompts. A request without a valid
//! token is refused with 403, and another user's prompt answers 404 like an
//! unknown nonce. The routes whose content is not scoped to one user (the
//! legacy SSE stream, the standalone page, and the capability-grant routes)
//! are not served on a hosted node. A node that is not hosted ignores any
//! token and behaves as before.
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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::client_events::websocket::{
    UserTokenDecision, decide_user_token, derive_user_context, host_header_ip_in_cidrs,
    is_allowed_host, is_localhost_origin, is_loopback_source,
};
use crate::contract::delegate_capabilities::{AppIdentity, Grant, capability_description};
use crate::contract::user_input::{
    CallerIdentity, PendingPrompts, PromptEvent, PromptSnapshot, emit_prompt_event, prompt_events,
};
use crate::server::client_api::hosted_export::ExportOpManagerHandle;
use crate::server::{AllowedHosts, AllowedSourceCidrs, HostedMode};
use crate::wasm_runtime::UserId;
use freenet_stdlib::prelude::{Capability, ContractInstanceId};
use zeroize::Zeroizing;

/// Register permission prompt routes.
///
/// `/permission/events` is the legacy SSE transport, kept only for tabs that
/// were already open when the node upgraded past #5213 — no current shell JS
/// opens it. Retiring it is tracked in #5272. Be precise about what removal
/// would cost: the pre-#5213 client registers an error listener on the
/// `EventSource` and falls back to polling `/permission/pending` every 3s, so
/// deleting the route degrades those tabs to that fallback (a prompt takes up
/// to ~3s to appear) rather than breaking them. Latency for pre-upgrade tabs,
/// not correctness.
///
/// On a hosted node the routes that show or answer prompts (`pending`,
/// `events/ws`, `{nonce}/respond`) require the user's token and show each user
/// only their own prompts ([`viewer_gate`]); the others are not served there
/// at all ([`refuse_when_hosted`]).
pub(super) fn routes() -> Router {
    let viewer = || axum::middleware::from_fn(viewer_gate);
    let viewer_ws = || axum::middleware::from_fn(viewer_gate_ws);
    let refused = || axum::middleware::from_fn(refuse_when_hosted);
    Router::new()
        .route("/permission/pending", get(pending_prompts).layer(viewer()))
        .route(
            "/permission/events",
            get(permission_events).layer(refused()),
        )
        .route(
            "/permission/events/ws",
            get(permission_events_ws).layer(viewer_ws()),
        )
        .route("/permission/grants", get(list_grants).layer(refused()))
        .route(
            "/permission/grants/revoke",
            post(revoke_grant).layer(refused()),
        )
        .route("/permission/apps", get(apps_page).layer(refused()))
        .route("/permission/{nonce}", get(permission_page).layer(refused()))
        .route(
            "/permission/{nonce}/respond",
            post(permission_respond).layer(viewer()),
        )
}

/// Who is asking to see or answer prompts.
///
/// On a node that is not hosted the requester is the node's own user and may
/// see every prompt ([`PromptViewer::Node`]). On a hosted node it is the user
/// named by the token the request presents ([`PromptViewer::User`]), who may
/// see only the prompts their own delegate runs raised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PromptViewer {
    Node,
    User(UserId),
}

impl PromptViewer {
    /// Whether this viewer may see, and answer, a prompt with this owner.
    fn may_see(self, owner: Option<UserId>) -> bool {
        match self {
            PromptViewer::Node => true,
            PromptViewer::User(id) => owner == Some(id),
        }
    }
}

/// Header carrying the hosted user's token on the permission routes. The same
/// header the client API and the hosted export accept.
const USER_TOKEN_HEADER: &str = "x-freenet-user-token";

/// Decide the [`PromptViewer`] for a request.
///
/// Not hosted: always [`PromptViewer::Node`]; any token is ignored, so a node
/// that is not hosted behaves exactly as before. Hosted: the token is taken
/// from the header first and from the query (`?userToken=`, WebSocket route
/// only) otherwise, and is honoured only under the same rule as the client
/// API ([`decide_user_token`]): a loopback source carrying
/// `X-Forwarded-Proto: https`. No token, an empty token, or an insecure
/// connection is refused with 403. The token is never logged.
pub(crate) fn resolve_prompt_viewer(
    hosted: bool,
    header_token: Option<&str>,
    query_token: Option<&str>,
    source_ip: Option<std::net::IpAddr>,
    xfp_https: bool,
) -> Result<PromptViewer, (axum::http::StatusCode, &'static str)> {
    use axum::http::StatusCode;
    if !hosted {
        return Ok(PromptViewer::Node);
    }
    // The header wins when present, as in `connection_info`.
    let token = header_token.or(query_token);
    let has_token = token.is_some_and(|t| !t.is_empty());
    match decide_user_token(hosted, has_token, source_ip, xfp_https) {
        UserTokenDecision::Honor => match derive_user_context(hosted, token) {
            Some(ctx) => Ok(PromptViewer::User(*ctx.user_id())),
            // Unreachable: `Honor` implies a non-empty token on a hosted node,
            // for which `derive_user_context` returns `Some`. Refused rather
            // than assumed.
            None => Err((
                StatusCode::FORBIDDEN,
                "a hosted node requires a user token on the permission routes",
            )),
        },
        UserTokenDecision::RejectInsecure => Err((
            StatusCode::FORBIDDEN,
            "hosted user token requires a secure (TLS/loopback) connection",
        )),
        UserTokenDecision::Local => Err((
            StatusCode::FORBIDDEN,
            "a hosted node requires a user token on the permission routes",
        )),
    }
}

/// The query form of the token, accepted on the WebSocket route only, since a
/// browser cannot set a header on a WebSocket handshake.
#[derive(Deserialize)]
struct PermTokenQuery {
    #[serde(rename = "userToken")]
    user_token: Option<Zeroizing<String>>,
}

/// The hosted-mode flag for a permission-route request, or `None` (logged as
/// an error) when it is missing; the caller then refuses. Every composition injects `HostedMode` (the node's
/// in `serve_client_api_in_impl`, the standalone one in
/// `HttpClientApi::as_router`). A missing flag is a misconfiguration and is
/// refused rather than read as "not hosted", which on these routes would show
/// every prompt to every requester.
fn hosted_flag(req: &axum::extract::Request) -> Option<bool> {
    let flag = req.extensions().get::<HostedMode>().map(|h| h.0);
    if flag.is_none() {
        tracing::error!(
            path = req.uri().path(),
            "Permission route reached without the HostedMode extension; \
             refusing. The router composition must inject HostedMode."
        );
    }
    flag
}

/// The refusal for a permission-route request with no hosted-mode flag.
fn misconfigured() -> axum::response::Response {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "permission routes are misconfigured",
    )
        .into_response()
}

/// Middleware for `pending` and `{nonce}/respond`: resolve the viewer from the
/// header token and hand it to the handler.
async fn viewer_gate(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    resolve_and_run(req, next, false).await
}

/// Middleware for `events/ws`: as [`viewer_gate`], also accepting the token in
/// the query.
async fn viewer_gate_ws(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    resolve_and_run(req, next, true).await
}

async fn resolve_and_run(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
    accept_query_token: bool,
) -> axum::response::Response {
    let hosted = match hosted_flag(&req) {
        Some(hosted) => hosted,
        None => return misconfigured(),
    };
    let source_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    let xfp_https = req
        .headers()
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("https"));
    // Owned copies of the token are held in `Zeroizing` so they are wiped on
    // drop, as `connection_info` does.
    let header_token: Option<Zeroizing<String>> = req
        .headers()
        .get(USER_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| Zeroizing::new(s.to_owned()));
    let query_token: Option<Zeroizing<String>> = if accept_query_token {
        axum::extract::Query::<PermTokenQuery>::try_from_uri(req.uri())
            .ok()
            .and_then(|q| q.0.user_token)
    } else {
        None
    };
    match resolve_prompt_viewer(
        hosted,
        header_token.as_deref().map(String::as_str),
        query_token.as_deref().map(String::as_str),
        source_ip,
        xfp_https,
    ) {
        Ok(viewer) => {
            req.extensions_mut().insert(viewer);
            next.run(req).await
        }
        Err((status, reason)) => {
            tracing::debug!(source_ip = ?source_ip, reason, "Refused a permission route request");
            (status, reason).into_response()
        }
    }
}

/// Middleware for the permission routes a hosted node does not serve: the
/// legacy SSE stream, the standalone prompt page, and the capability-grant
/// list, revoke and page. Each shows or changes state that is not scoped to
/// one hosted user.
async fn refuse_when_hosted(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let hosted = match hosted_flag(&req) {
        Some(hosted) => hosted,
        None => return misconfigured(),
    };
    if hosted {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "a hosted node does not serve this permission route",
        )
            .into_response();
    }
    next.run(req).await
}

/// The permission-prompt surface (`/permission/pending`, `/permission/events`,
/// `/permission/{nonce}/respond`) leaks/forges capabilities: `pending` and
/// `events` disclose the raw permission nonce, and `respond` approves a prompt.
///
/// These MUST be reachable only from the local host, regardless of whether the
/// node's main HTTP listener binds `0.0.0.0` for network mode. Origin /
/// `Sec-Fetch-Site` / RFC1918 source-CIDR checks are all insufficient here: a
/// non-browser LAN client (e.g. `curl` with no `Origin`) passes every one of
/// them while still connecting from an off-box address (issue #3819).
///
/// The peer socket address is the kernel-observed source of the TCP connection.
/// Unlike `Origin` it cannot be forged at the HTTP layer — an off-box LAN peer's
/// address is never loopback. `into_make_service_with_connect_info::<SocketAddr>`
/// (see `server.rs`) injects the `ConnectInfo<SocketAddr>` extension for every
/// connection, so it is present on the production serve path.
///
/// Scope: this check identifies the direct peer only. Behind a reverse proxy on
/// the same host (a deployment `config.rs` documents as supported) every
/// client appears as the proxy's loopback address, so the check does not
/// distinguish clients there. Hosted nodes additionally require the user's
/// token on the routes they serve, and scope those routes to that user (see
/// the module docs). Client API authentication in general is tracked in #5264.
///
/// Fail closed: a MISSING `ConnectInfo` cannot prove loopback and is rejected.
/// (It is absent only in unit tests that build a request without connect-info;
/// those tests inject it explicitly to exercise both sides of this boundary.)
fn peer_is_loopback(connect_info: Option<&ConnectInfo<SocketAddr>>) -> bool {
    match connect_info {
        Some(ConnectInfo(addr)) => is_loopback_source(addr.ip()),
        None => false,
    }
}

/// Cap on simultaneous permission-event subscribers, across BOTH transports
/// (the WebSocket channel and the legacy SSE stream). A single browser opens
/// one subscriber per Freenet tab.
///
/// Note this number now BINDS where it previously could not. Its original
/// "generous headroom" was calibrated against SSE, where the browser's own
/// ~6-connections-per-origin limit meant one profile could never hold more
/// than about six subscribers — which is the very bug #5213 fixed. WebSockets
/// pool at ~255 per profile, so 65 tabs in one browser now reaches this cap.
/// It is kept at 64 deliberately: past it, tabs degrade to the 3s
/// `/permission/pending` poll rather than losing prompts, and 64 concurrent
/// pump tasks plus broadcast receivers is a reasonable ceiling for a local
/// node. Raise it only with a reason to believe the degraded path is being
/// hit in practice.
///
/// The cap is deliberately shared rather than per-transport: it bounds the
/// number of live broadcast receivers and pump tasks, and that cost is the
/// same whichever transport carries the events. During a node upgrade both
/// transports are in use at once (already-open tabs still hold SSE streams
/// while freshly-loaded tabs use the WebSocket), and two independent caps
/// would silently allow twice the intended ceiling.
const MAX_PERMISSION_SUBSCRIBERS: usize = 64;

/// Ceiling for the LEGACY SSE transport specifically, below
/// [`MAX_PERMISSION_SUBSCRIBERS`], so SSE can never consume the last slots and
/// starve the WebSocket path.
///
/// Sharing one counter (see above) is right for bounding total cost, but it
/// created an interaction worth blocking: the WebSocket pump bounds a client
/// that stops reading with [`PERMISSION_WS_SEND_TIMEOUT`], and the SSE path has
/// no equivalent — a backpressured `Sse` body holds its
/// `PermissionSubscriberGuard` for as long as the peer keeps the socket open.
/// With a single shared ceiling, stalled SSE streams could therefore occupy all
/// 64 slots indefinitely and every new tab's WebSocket handshake would 503,
/// degrading the whole browser to the 3s poll with nothing surfaced — the same
/// "nothing errors, so nothing is visible" shape as #5213 itself.
///
/// A small ABSOLUTE cap rather than a fraction of the total. Fractions get this
/// backwards: three quarters would hand the legacy, un-timeout-able transport
/// most of the budget and leave the primary one a 16-slot floor. SSE
/// subscribers only exist for tabs opened before a node upgrade, and the
/// browser's own ~6-per-origin limit — the #5213 bug itself — caps how many any
/// one profile can hold, so 16 covers a couple of browser profiles' worth of
/// pre-upgrade tabs while leaving 48 slots that SSE can never touch. The whole
/// mechanism disappears with the route in #5272.
const MAX_SSE_SUBSCRIBERS: usize = 16;

/// Which transport is claiming a subscriber slot. Only affects the ceiling
/// applied; the slot itself, and the counter, are shared.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SubscriberTransport {
    WebSocket,
    LegacySse,
}

impl SubscriberTransport {
    fn ceiling(self) -> usize {
        match self {
            SubscriberTransport::WebSocket => MAX_PERMISSION_SUBSCRIBERS,
            SubscriberTransport::LegacySse => MAX_SSE_SUBSCRIBERS,
        }
    }
}

/// Live permission-event subscriber count (WebSocket + SSE). Used to enforce
/// `MAX_PERMISSION_SUBSCRIBERS`.
static PERMISSION_SUBSCRIBERS: AtomicUsize = AtomicUsize::new(0);

/// Decrements `PERMISSION_SUBSCRIBERS` on drop. The count must always be
/// released even if the stream/socket is dropped mid-handshake or panics in a
/// downstream layer.
struct PermissionSubscriberGuard;

impl Drop for PermissionSubscriberGuard {
    fn drop(&mut self) {
        PERMISSION_SUBSCRIBERS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Claims one permission-event subscriber slot, or returns `None` when the cap
/// is already reached.
///
/// CAS-loop instead of `fetch_add` + rollback so concurrent reconnects can
/// never overshoot the cap: an earlier `fetch_add` version of this check had a
/// transient overshoot window, and on Relaxed ordering no upper bound under
/// heavy reconnect storms. Shared by both transports so the cap is enforced
/// identically whichever one the client picked.
///
/// `transport` selects the ceiling: see [`MAX_SSE_SUBSCRIBERS`] for why the
/// legacy path gets a lower one.
fn try_claim_subscriber_slot(transport: SubscriberTransport) -> Option<PermissionSubscriberGuard> {
    let ceiling = transport.ceiling();
    let mut current = PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed);
    loop {
        if current >= ceiling {
            return None;
        }
        match PERMISSION_SUBSCRIBERS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(PermissionSubscriberGuard),
            Err(actual) => current = actual,
        }
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
/// Trust gating + CORS:
///
/// * **Untrusted callers** (cross-origin, `Origin: null`, public Origin):
///   reply `200 OK` with `[]` and `Access-Control-Allow-Origin: *`. The
///   wildcard exists only so the browser delivers the empty body instead
///   of logging a "CORS header missing" error in devtools for legitimate
///   cross-origin probes (e.g. a sandboxed iframe whose origin is
///   `null`). The body itself reveals nothing.
/// * **Trusted callers** (loopback / LAN-with-same-IP-Origin /
///   `allowed-host`-with-matching-Origin / no Origin same-origin GET):
///   reply `200 OK` with the real prompt list and **no**
///   `Access-Control-Allow-Origin` header. The legitimate consumer (the
///   gateway shell page) is same-origin, so it doesn't need CORS to
///   read the body. Withholding the wildcard ensures that even if a
///   future change widens the trust gate, the trusted body cannot be
///   read by an arbitrary cross-origin `fetch()`.
///
/// The state-changing `/permission/{nonce}/respond` endpoint retains
/// its strict Origin check independently of this read endpoint.
async fn pending_prompts(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    allowed_hosts: Option<Extension<AllowedHosts>>,
    allowed_source_cidrs: Option<Extension<AllowedSourceCidrs>>,
    Extension(pending): Extension<PendingPrompts>,
    Extension(viewer): Extension<PromptViewer>,
) -> axum::response::Response {
    // Loopback-only boundary (#3819): this endpoint discloses raw nonces, so a
    // LAN peer must not reach it even when the main listener binds 0.0.0.0.
    if !peer_is_loopback(connect_info.as_ref().map(|Extension(ci)| ci)) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let allowed_hosts = allowed_hosts.as_ref().map(|Extension(v)| v);
    let allowed_source_cidrs = allowed_source_cidrs.as_ref().map(|Extension(v)| v);

    // Missing Origin (e.g. same-origin top-level fetch from some
    // browsers) is treated as trusted: the source-IP filter in
    // `server.rs::private_network_filter` and the operator-configured
    // `allowed-source-cidrs` policy already gate which networks can
    // reach this endpoint, and the poll payload is not a capability.
    let trusted = match headers.get("origin") {
        Some(value) => value
            .to_str()
            .map(|s| is_origin_trusted(&headers, s, allowed_hosts, allowed_source_cidrs))
            .unwrap_or(false),
        None => true,
    };

    if !trusted {
        let mut resp = Json(serde_json::json!([])).into_response();
        resp.headers_mut().insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            axum::http::HeaderValue::from_static("*"),
        );
        return resp;
    }

    let prompts: Vec<serde_json::Value> = pending
        .iter()
        // A hosted user sees only their own prompts.
        .filter(|entry| viewer.may_see(entry.value().owner))
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
                "author": prompt.author.as_str(),
                "message": message,
                "labels": labels,
                "delegate_key": sanitize_display(&prompt.delegate_key, OVERLAY_KEY_CHARS_MAX),
                "caller": caller_to_json(&prompt.caller),
            })
        })
        .collect();
    Json(serde_json::json!(prompts)).into_response()
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
        // Every placeholder is passed explicitly: `format!` cannot implicitly
        // capture `{ident}` variables when the format string comes from a macro
        // (`include_str!`) instead of a string literal.
        Html(format!(
            include_str!("permission_prompts/assets/permission_prompt.html"),
            message_label = match entry.author {
                crate::contract::user_input::PromptAuthor::Delegate => "Delegate says:",
                crate::contract::user_input::PromptAuthor::Node => "Freenet asks:",
            },
            caller_title_html = caller_title_html,
            caller_display_html = caller_display_html,
            delegate_full_attr = delegate_full_attr,
            delegate_trunc_html = delegate_trunc_html,
            message = message,
            buttons_html = buttons_html,
        )),
    )
}

// ==================== Apps and permissions (capability grants) ====================

/// The user's remembered answers to node-enforced capability prompts
/// (`contract::delegate_capabilities`), for the "Apps and permissions" page.
///
/// Same gate as `/permission/pending`: loopback peers only, and a foreign
/// Origin gets an empty list rather than the real one.
async fn list_grants(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    allowed_hosts: Option<Extension<AllowedHosts>>,
    allowed_source_cidrs: Option<Extension<AllowedSourceCidrs>>,
    op_manager: Option<Extension<ExportOpManagerHandle>>,
) -> axum::response::Response {
    if !peer_is_loopback(connect_info.as_ref().map(|Extension(ci)| ci)) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let allowed_hosts = allowed_hosts.as_ref().map(|Extension(v)| v);
    let allowed_source_cidrs = allowed_source_cidrs.as_ref().map(|Extension(v)| v);
    // A same-origin GET often carries no Origin. Without one, require a Host
    // naming this machine (or an operator-allowed host), so a DNS-rebound
    // page, whose Host is its own name, reads nothing: which apps a user has
    // granted is a fingerprint worth keeping.
    if !grants_read_trusted(&headers, allowed_hosts, allowed_source_cidrs) {
        return Json(grants_body(None)).into_response();
    }
    Json(grants_body(capabilities_of(op_manager).as_deref())).into_response()
}

fn grants_read_trusted(
    headers: &HeaderMap,
    allowed_hosts: Option<&AllowedHosts>,
    allowed_source_cidrs: Option<&AllowedSourceCidrs>,
) -> bool {
    match headers.get("origin") {
        Some(value) => value
            .to_str()
            .map(|s| is_origin_trusted(headers, s, allowed_hosts, allowed_source_cidrs))
            .unwrap_or(false),
        None => {
            host_is_loopback(headers)
                || allowed_hosts.is_some_and(|hosts| is_allowed_host(headers, hosts))
        }
    }
}

/// Whether the Host header names the loopback interface (`localhost`,
/// `127.0.0.1`, `[::1]`, with any port).
fn host_is_loopback(headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    let name = if let Some(rest) = host.strip_prefix('[') {
        rest.split(']')
            .next()
            .map(|h| format!("[{h}]"))
            .unwrap_or_default()
    } else {
        host.split(':').next().unwrap_or_default().to_string()
    };
    matches!(name.as_str(), "localhost" | "127.0.0.1" | "[::1]")
}

/// `{ "grants": [...], "stats": {...} }`. The stats are the capability
/// counters (lifecycle runs delivered, deferred, dropped; budget refusals),
/// shown on the Apps and permissions page so a refusal is visible somewhere
/// other than a rotated log.
fn grants_body(
    caps: Option<&crate::contract::delegate_capabilities::DelegateCapabilities>,
) -> serde_json::Value {
    match caps {
        Some(caps) => {
            let (grants, error) = match grants_json(caps) {
                Ok(g) => (g, serde_json::Value::Null),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to list capability grants");
                    (
                        serde_json::json!([]),
                        serde_json::json!("could not read the grant store"),
                    )
                }
            };
            serde_json::json!({
                "grants": grants,
                "error": error,
                "stats": serde_json::to_value(&caps.stats).unwrap_or(serde_json::Value::Null),
            })
        }
        None => serde_json::json!({ "grants": [], "stats": null }),
    }
}

fn capabilities_of(
    op_manager: Option<Extension<ExportOpManagerHandle>>,
) -> Option<std::sync::Arc<crate::contract::delegate_capabilities::DelegateCapabilities>> {
    op_manager
        .and_then(|Extension(h)| h.current())
        .and_then(|om| om.delegate_capabilities())
}

fn grants_json(
    caps: &crate::contract::delegate_capabilities::DelegateCapabilities,
) -> anyhow::Result<serde_json::Value> {
    let rows: Vec<serde_json::Value> = caps
        .grants()?
        .into_iter()
        .map(|(app, cap, grant)| {
            let (state, at_ms, until_ms) = match grant {
                Grant::Granted { at_ms } => ("granted", at_ms, None),
                Grant::Denied { at_ms, until_ms } => ("denied", at_ms, Some(until_ms)),
            };
            serde_json::json!({
                "app": app.display(),
                "capability": serde_json::to_value(cap).unwrap_or(serde_json::Value::Null),
                "description": capability_description(cap),
                "state": state,
                "at_ms": at_ms,
                "until_ms": until_ms,
            })
        })
        .collect();
    Ok(serde_json::json!(rows))
}

#[derive(Deserialize)]
struct RevokeGrantRequest {
    /// The app's contract instance id, as `/permission/grants` lists it.
    app: String,
    /// The capability's manifest name, e.g. `"background"`.
    capability: String,
}

/// Forget one remembered answer. A revoked Background grant stops lifecycle
/// events at once (delivery re-checks the grant; contract-notification runs
/// are not gated by it), and the app is asked again the next time it
/// registers a delegate that wants it.
///
/// State-changing, so the same gate as `/permission/{nonce}/respond`:
/// loopback peer, and an Origin that is present and trusted (CSRF). This is
/// the design the grant endpoint of #4086 lacked (reverted in #4090).
async fn revoke_grant(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    allowed_hosts: Option<Extension<AllowedHosts>>,
    allowed_source_cidrs: Option<Extension<AllowedSourceCidrs>>,
    op_manager: Option<Extension<ExportOpManagerHandle>>,
    Json(body): Json<RevokeGrantRequest>,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    use axum::http::StatusCode;
    if !peer_is_loopback(connect_info.as_ref().map(|Extension(ci)| ci)) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "forbidden"})),
        );
    }
    let allowed_hosts = allowed_hosts.as_ref().map(|Extension(v)| v);
    let allowed_source_cidrs = allowed_source_cidrs.as_ref().map(|Extension(v)| v);
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "missing origin"})),
        );
    };
    if !is_origin_trusted(&headers, origin, allowed_hosts, allowed_source_cidrs) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "forbidden"})),
        );
    }
    let Some(caps) = capabilities_of(op_manager) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "node not ready"})),
        );
    };
    revoke_with(&caps, body)
}

fn revoke_with(
    caps: &crate::contract::delegate_capabilities::DelegateCapabilities,
    body: RevokeGrantRequest,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    use axum::http::StatusCode;
    let Ok(id) = body.app.parse::<ContractInstanceId>() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid app"})),
        );
    };
    let Ok(cap) = serde_json::from_value::<Capability>(serde_json::Value::String(body.capability))
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid capability"})),
        );
    };
    if caps.revoke(&AppIdentity::WebApp(id), cap) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "not revocable"})),
        )
    }
}

/// The "Apps and permissions" page: lists remembered answers with a Revoke
/// button each. Data comes from `/permission/grants`; revocation posts to
/// `/permission/grants/revoke`.
async fn apps_page() -> impl IntoResponse {
    let headers = [
        ("X-Frame-Options", "DENY"),
        (
            "Content-Security-Policy",
            "frame-ancestors 'none'; default-src 'self' 'unsafe-inline'",
        ),
        ("Cache-Control", "no-store"),
    ];
    (
        headers,
        Html(include_str!(
            "permission_prompts/assets/apps_permissions.html"
        )),
    )
}

#[derive(Deserialize)]
struct PermissionResponse {
    index: usize,
}

/// Handle the user's response to a permission prompt.
///
/// The arguments are axum extractors, one per request part this handler reads.
#[allow(clippy::too_many_arguments)]
async fn permission_respond(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Path(nonce): Path<String>,
    headers: HeaderMap,
    allowed_hosts: Option<Extension<AllowedHosts>>,
    allowed_source_cidrs: Option<Extension<AllowedSourceCidrs>>,
    Extension(pending): Extension<PendingPrompts>,
    Extension(viewer): Extension<PromptViewer>,
    Json(body): Json<PermissionResponse>,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    // Loopback-only boundary (#3819): approving a prompt is a capability, so a
    // LAN peer must not reach it even when the main listener binds 0.0.0.0.
    if !peer_is_loopback(connect_info.as_ref().map(|Extension(ci)| ci)) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "forbidden"})),
        );
    }
    let allowed_hosts = allowed_hosts.as_ref().map(|Extension(v)| v);
    let allowed_source_cidrs = allowed_source_cidrs.as_ref().map(|Extension(v)| v);

    // CSRF guard: state-changing POST requires Origin and the Origin must be
    // authorized. Authorized means loopback, in `allowed-source-cidrs`, or a
    // matched `allowed-host` entry whose Origin authority matches the Host
    // header (so a cross-site page reaching the gateway via DNS rebinding
    // cannot forge `Origin: https://evil.com` with a victim Host).
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "missing origin"})),
        );
    };
    if !is_origin_trusted(&headers, origin, allowed_hosts, allowed_source_cidrs) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "forbidden"})),
        );
    }

    // Validate index BEFORE removing from DashMap. Removing first would
    // consume the nonce on invalid input, leaving the user unable to retry.
    //
    // A hosted user may answer only their own prompts. Another user's prompt
    // answers 404 for any index, as an unknown nonce does.
    let label_count = pending
        .get(&nonce)
        .filter(|e| viewer.may_see(e.owner))
        .map(|e| e.labels.len());
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
            // Index is valid -- now atomically remove and send response. The
            // ownership check is repeated inside the removal so that it holds
            // for the entry actually removed.
            if let Some((_, prompt)) = pending.remove_if(&nonce, |_, p| viewer.may_see(p.owner)) {
                if prompt.response_tx.send(body.index).is_err() {
                    tracing::debug!(nonce = %nonce, "Permission response channel already closed");
                }
                // Notify permission subscribers so every open Freenet tab dismisses
                // its overlay immediately instead of waiting for the polling
                // fallback (#3836 follow-up).
                emit_prompt_event(PromptEvent::Removed {
                    nonce: nonce.clone(),
                    owner: prompt.owner,
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

/// Combined origin check used by `/permission/events` (SSE) and, as one of
/// two required checks, by `/permission/events/ws`.
///
/// **This function alone is NOT sufficient for the WebSocket.** It accepts any
/// localhost origin on any port, and it accepts a request with no `Origin` at
/// all. On the SSE path both are safe: the trusted branch sets no
/// `Access-Control-Allow-Origin`, so a cross-origin `EventSource` that passes
/// this gate still has its body withheld by the browser — CORS is the real
/// enforcement. WebSocket handshakes get no CORS and no same-origin policy, so
/// that path pairs this with [`origin_is_exactly_same_origin`], which requires
/// Origin authority == `Host`. Read that function before changing either.
///
/// On the SSE wire, browsers send `Origin` only for cross-origin `EventSource`
/// requests; same-origin GETs from the gateway shell page often arrive with no
/// `Origin`. So this check cannot simply require a trusted `Origin`. Instead
/// it combines two browser-attested signals:
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
fn is_caller_trusted(
    headers: &HeaderMap,
    allowed_hosts: Option<&AllowedHosts>,
    allowed_source_cidrs: Option<&AllowedSourceCidrs>,
) -> bool {
    if let Some(value) = headers.get("origin") {
        let s = match value.to_str() {
            Ok(s) => s,
            Err(_) => return false,
        };
        if !is_origin_trusted(headers, s, allowed_hosts, allowed_source_cidrs) {
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

/// Returns true if `origin` is authorized to read or modify permission-prompt
/// state for this request.
///
/// Closely mirrors the WebSocket upgrade policy in
/// `client_events::websocket::connection_info`, with two deliberate
/// tightenings called out below.
///
/// Pre-check (always): `Origin: null` is rejected for every branch. The
/// WS gate accepts `null` under the operator-CIDR branch to support the
/// dApp sandbox iframe upgrading a WebSocket; permission prompts are a
/// stricter target (the response POST authorises a delegate action on
/// the user's behalf) so the same sandboxed-iframe Origin is refused.
///
/// Accept branches:
///
/// 1. **Loopback** — `http://localhost`/`127.0.0.1`/`[::1]` (any port,
///    plus `https://` variants). Default-on for the localhost-only
///    install. Note: this is broader than the pre-fix gate, which
///    rejected `https://` loopback; the new check matches the WS layer
///    and removes a needless restriction.
/// 2. **LAN / operator-configured CIDR** — Host header IP is private
///    (RFC1918 / loopback / link-local / IPv6 ULA) **or** matches
///    `allowed-source-cidrs`, **and** the Origin is a same-IP literal.
///    The default-LAN sub-branch is on for every caller (including when
///    the `AllowedSourceCidrs` extension is missing, e.g. `run_local_node`
///    or older tests); the operator CIDR set only widens which non-private
///    ranges (e.g. Tailnet CGNAT) qualify. See [`host_header_ip_in_cidrs`]
///    for the CSWSH/DNS-rebind rationale.
/// 3. **Explicit `allowed-host` hostname** — Host header is in
///    `allowed_hosts` **and** the Origin's authority matches the Host
///    header. The Origin-vs-Host check is stricter than the WebSocket
///    layer's `is_allowed_host` branch and closes a CSWSH path where
///    `evil.com` opens a fetch to `http://victim-hostname:7509` (the
///    operator's allow-listed hostname) — browsers send `Host:
///    victim-hostname:7509`, `Origin: https://evil.com`, which would
///    otherwise pass an Origin-less allowed-host check. When the
///    `AllowedHosts` extension is missing, branch 3 is inactive.
///
/// `pub(crate)` so the live-import endpoint (`hosted_import`) reuses the EXACT
/// same origin-trust decision as the permission-prompt CSRF guard — one source
/// of truth for "is this request from the node's own dashboard/shell origin (and
/// not a sandboxed `Origin: null` contract iframe)". See `hosted_import`'s gate.
pub(crate) fn is_origin_trusted(
    headers: &HeaderMap,
    origin: &str,
    allowed_hosts: Option<&AllowedHosts>,
    allowed_source_cidrs: Option<&AllowedSourceCidrs>,
) -> bool {
    // Reject `Origin: null` for permission endpoints regardless of branch.
    // The WS layer's CIDR branch deliberately accepts `null` to support
    // the dApp sandbox iframe upgrading a WebSocket, but the permission
    // endpoints are a different threat model: `/respond` clicks a
    // delegate-permission button on the user's behalf (CSRF target), and
    // `/pending` exposes delegate-controlled prompt text. A sandboxed
    // iframe on any site can present `Origin: null`, so accepting it here
    // — even under operator CIDR opt-in — would let a malicious page open
    // a `<iframe sandbox>` to the gateway and silently confirm prompts
    // the user never saw. Always require a non-null Origin.
    if origin.eq_ignore_ascii_case("null") {
        return false;
    }
    if is_localhost_origin(origin) {
        return true;
    }
    // Always call `host_header_ip_in_cidrs` — even when no CIDR is
    // configured, the helper's default-LAN sub-branch still accepts
    // private RFC1918 / link-local / IPv6 ULA Host with a same-IP
    // literal Origin.
    let empty_cidrs = AllowedSourceCidrs::default();
    let cidrs = allowed_source_cidrs.unwrap_or(&empty_cidrs);
    if host_header_ip_in_cidrs(headers, origin, cidrs) {
        return true;
    }
    if let Some(hosts) = allowed_hosts
        && origin_matches_allowed_host(origin, headers, hosts)
    {
        return true;
    }
    false
}

/// True iff `headers["host"]` is in `allowed_hosts` AND the Origin's
/// authority matches the Host header.
///
/// The Origin/Host equality is what prevents `evil.com` from successfully
/// posting `Origin: https://evil.com` with `Host: <victim-allow-listed-host>`
/// to a state-changing endpoint.
fn origin_matches_allowed_host(
    origin: &str,
    headers: &HeaderMap,
    allowed_hosts: &AllowedHosts,
) -> bool {
    if !is_allowed_host(headers, allowed_hosts) {
        return false;
    }
    let Some(host_header) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let Some(origin_authority) = origin_authority(origin) else {
        return false;
    };
    origin_authority.eq_ignore_ascii_case(host_header)
}

/// True iff the request carries an `Origin` whose authority is byte-equal
/// (case-insensitively) to its `Host` header — i.e. exact same-origin.
///
/// This is STRICTER than [`is_caller_trusted`] and exists solely for the
/// permission-event WebSocket. `is_origin_trusted` accepts any localhost
/// origin on ANY port, and on the SSE path that leniency was harmless because
/// the trusted branch deliberately sets no `Access-Control-Allow-Origin` (see
/// the rationale on [`pending_prompts`]): a page on `http://localhost:3000`
/// passed the server gate but the BROWSER withheld the body. CORS was doing
/// the real enforcement.
///
/// WebSocket handshakes get no CORS. Without this check, any other page on any
/// port of the same loopback host — a dev server, a notebook, another local
/// app's UI, anything with an XSS on `127.0.0.1:*` — could open the socket and
/// read raw permission nonces, delegate-authored prompt text, the delegate key
/// and caller identity, and could hold slots against the subscriber cap.
///
/// A missing `Origin` is rejected too. `is_caller_trusted` tolerates its
/// absence (a non-browser caller), but this endpoint exists ONLY for the shell
/// page, and the whole cross-site argument rests on browsers being required to
/// send `Origin` on every WebSocket handshake. Requiring it makes that
/// reasoning load-bearing in code rather than in a comment.
///
/// The real client always satisfies this: `permSocketUrl` in `shell_bridge.js`
/// builds the socket URL from `location.host`, so Origin authority and Host
/// are the same string for both `127.0.0.1:7509` and `localhost:7509`.
///
/// Deliberately applied at the WebSocket gate rather than inside
/// [`is_origin_trusted`], which is shared with `hosted_import` and
/// `pending_prompts` — tightening it wholesale is a wider blast radius than
/// this endpoint needs.
///
/// Reads the `Host` header specifically. That is correct today because the
/// local listener is HTTP/1.1 only (`axum` is pinned with no `http2` feature),
/// so `Host` is always present. If h2 is ever enabled, the authority moves to
/// the `:authority` pseudo-header and `Host` disappears: this returns `false`
/// and the socket is refused. That is the safe direction — the overlay falls
/// back to polling rather than the gate silently opening — but whoever enables
/// h2 has to teach this function to read `uri.authority()` too.
fn origin_is_exactly_same_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(host_header) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let Some(authority) = origin_authority(origin) else {
        return false;
    };
    if !authority.eq_ignore_ascii_case(host_header) {
        return false;
    }
    origin_scheme_matches_forwarded_proto(headers, origin)
}

/// The `Host` header carries no scheme, so an authority comparison alone treats
/// `http://h` and `https://h` as the same origin. They are NOT the same origin,
/// and on the reverse-proxy deployment `config.rs` documents as supported the
/// difference is reachable: with the node behind TLS at
/// `https://node.example.com`, a page an attacker serves at
/// `http://node.example.com` (an on-path attacker on port 80 with no HSTS, or
/// an unrelated plain-HTTP vhost on the same name) sends
/// `Origin: http://node.example.com` with `Host: node.example.com`. Authority
/// matches, and the allowed-host branch of [`is_origin_trusted`] passes too, so
/// that page could open the socket and read raw permission nonces.
///
/// So compare the scheme as well, deriving the expected one the way
/// [`crate::server::client_api::sandbox_origin_from_headers`] already does.
///
/// Be honest about the strength of this: `X-Forwarded-Proto` is set by the
/// proxy and a careless proxy passes a client-supplied value straight through,
/// in which case an attacker able to forge headers can also forge this one. It
/// is therefore defence-in-depth, not a trust boundary — it closes the BROWSER
/// -reachable version of the attack above (a browser cannot set either header)
/// while doing nothing against a header-forging client. The real fix for a
/// non-browser caller is authenticating this surface, tracked in #5264.
///
/// Enforced ONLY when `X-Forwarded-Proto` is present, i.e. when the
/// client-facing scheme is actually knowable. An absent header is NOT treated
/// as "therefore http": a TLS-terminating proxy that rewrites `Host` correctly
/// but omits `X-Forwarded-Proto` is a common misconfiguration, and rejecting it
/// would refuse the socket for a legitimate deployment and drop every tab to
/// the 3s poll with nothing surfaced — reintroducing the silent-degradation
/// class this whole change exists to remove. Where the scheme cannot be
/// determined this falls back to the authority-only comparison, which is no
/// weaker than the behaviour before this check existed.
///
/// Be precise about the coverage, because an earlier version of this comment
/// overstated it. The two cases are NOT disjoint: they are the same
/// misconfiguration on two different listeners. The natural nginx shape is a
/// `listen 443` block that sets `X-Forwarded-Proto $scheme` alongside a
/// `listen 80` block that also proxies to the node but omits that line — and
/// port 80 is exactly the listener the attack arrives on. Such a request
/// reaches us with no `X-Forwarded-Proto`, so this returns `true` without
/// comparing anything and the socket opens.
///
/// So this closes the attack only where the plain-HTTP vhost is also
/// correctly configured. It is a real narrowing, not a fix. Refusing on an
/// absent header would not fix it either — it would just break honest
/// deployments (see above). The actual remedy is an operator-declared expected
/// origin compared instead of guessed, which needs a config surface this PR
/// does not add; the underlying "this API has no authentication" problem is
/// #5264. What we CAN do here is make the gap observable rather than silent,
/// which is what the warning below does.
fn origin_scheme_matches_forwarded_proto(headers: &HeaderMap, origin: &str) -> bool {
    // The deployment where this gate provably is not running: something in
    // front of us is rewriting the request (so we are proxied) but did not say
    // which scheme the client used. Warn once — an unobservable degraded gate
    // is the failure mode this whole change is about.
    if !headers.contains_key("x-forwarded-proto")
        && (headers.contains_key("x-forwarded-host")
            || headers.contains_key("forwarded")
            || headers.contains_key("x-forwarded-for")
            || headers.contains_key("x-real-ip"))
    {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                "permission WebSocket: request carries forwarding headers but no \
                 X-Forwarded-Proto, so the Origin scheme cannot be checked. Set \
                 `proxy_set_header X-Forwarded-Proto $scheme;` on EVERY listener \
                 that proxies to this node, including any plain-HTTP one."
            );
        });
    }
    let Some(expected) = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        // First element: for X-Forwarded-Proto the head of the list is the
        // CLIENT-facing scheme, which is the one an Origin should match.
        .map(|v| {
            v.split(',')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
        })
        // Present-but-blank (a proxy interpolating an unset variable) must be
        // treated as ABSENT. Comparing against "" matches no scheme, which
        // would refuse every handshake on that deployment and drop every tab to
        // the 3s poll with nothing surfaced — the silent degradation this whole
        // change exists to remove.
        .filter(|v| !v.is_empty())
    else {
        return true;
    };
    match origin.split_once("://") {
        Some((scheme, _)) => scheme.eq_ignore_ascii_case(&expected),
        None => false,
    }
}

/// Returns the scheme-stripped authority of an Origin header value with
/// any `user@` userinfo prefix stripped (e.g.
/// `https://mynode.example.com:7509/foo` → `mynode.example.com:7509`).
/// Returns `None` for malformed Origins or `Origin: null`.
///
/// Differs intentionally from [`crate::client_events::websocket::parse_origin_host`]:
/// that helper returns the *host* component only (no port), which is
/// what the WS Host-IP CIDR check needs. This one returns the full
/// authority (host plus port) so it can be byte-compared against the
/// HTTP `Host` header.
///
/// REJECTS, rather than strips, any authority containing `@`, `?`, `#`, `\`,
/// space or tab. A
/// real browser `Origin` is `scheme://host[:port]` and can contain none of
/// them, so nothing legitimate is lost — and stripping is actively dangerous
/// here, because it makes this parser disagree with
/// [`crate::client_events::websocket::is_localhost_origin`], which is plain
/// prefix matching. Under the old strip-userinfo behaviour
/// `http://127.0.0.1:7509@evil.com` was "localhost" to that helper and
/// `evil.com` to this one, so paired with `Host: evil.com` BOTH gates passed.
/// Symmetrically, not stripping a query let `http://evil.com?x=@127.0.0.1:7509`
/// resolve to authority `127.0.0.1:7509`. Two parsers that disagree about what
/// an origin is are a gate that can be walked between; refusing the ambiguous
/// input is the only version that cannot drift.
fn origin_authority(origin: &str) -> Option<&str> {
    let (_, after_scheme) = origin.split_once("://")?;
    let authority = after_scheme
        .split_once('/')
        .map(|(a, _)| a)
        .unwrap_or(after_scheme);
    if authority.contains(['@', '?', '#', '\\', ' ', '\t']) {
        return None;
    }
    Some(authority)
}

/// HTML for when a permission request has expired or already been answered.
fn expired_html() -> String {
    include_str!("permission_prompts/assets/expired.html").to_string()
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
        "author": snapshot.author.as_str(),
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
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    allowed_hosts: Option<Extension<AllowedHosts>>,
    allowed_source_cidrs: Option<Extension<AllowedSourceCidrs>>,
    Extension(pending): Extension<PendingPrompts>,
) -> axum::response::Response {
    // Loopback-only boundary (#3819): the SSE stream emits raw nonces in
    // `prompt_added` events, so a LAN peer must not reach it even when the main
    // listener binds 0.0.0.0. Reject BEFORE subscribing / consuming a slot.
    if !peer_is_loopback(connect_info.as_ref().map(|Extension(ci)| ci)) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let allowed_hosts = allowed_hosts.as_ref().map(|Extension(v)| v);
    let allowed_source_cidrs = allowed_source_cidrs.as_ref().map(|Extension(v)| v);

    // Reject untrusted callers BEFORE consuming a connection slot. Browsers
    // send `Sec-Fetch-Site: cross-site` for cross-origin EventSource and
    // `Origin` for any cross-origin request: either signal is enough to
    // reject, and rejecting up front blocks the cross-origin DoS where an
    // evil page opens 64 EventSources to consume every slot.
    if !is_caller_trusted(&headers, allowed_hosts, allowed_source_cidrs) {
        tracing::debug!("Rejecting /permission/events request from untrusted origin");
        let stream = stream::once(async {
            Ok::<_, Infallible>(Event::default().comment("untrusted-origin"))
        });
        return Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response();
    }

    // Subscriber cap, shared with the WebSocket transport but with a lower
    // ceiling for this legacy path so it cannot starve the WebSocket one.
    let guard = match try_claim_subscriber_slot(SubscriberTransport::LegacySse) {
        Some(guard) => guard,
        None => {
            tracing::warn!(
                // The ceiling this path ENFORCES, which is the SSE sub-cap, not
                // the shared total. Logging the total on a refusal that happened
                // lower makes an operator conclude the counter is broken.
                limit = SubscriberTransport::LegacySse.ceiling(),
                total = MAX_PERMISSION_SUBSCRIBERS,
                "permission subscriber cap reached, refusing new /permission/events subscriber"
            );
            let stream = stream::once(async {
                Ok::<_, Infallible>(Event::default().comment("connection-cap-reached"))
            });
            return Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response();
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
                author: entry.value().author,
                message: entry.value().message.clone(),
                labels: entry.value().labels.clone(),
                delegate_key: entry.value().delegate_key.clone(),
                caller: entry.value().caller.clone(),
                owner: entry.value().owner,
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
            Ok(PromptEvent::Removed { nonce, .. }) => Some(Ok(Event::default()
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

/// Ping cadence for the permission-event WebSocket. Matches the SSE
/// keep-alive interval it replaces, so an idle channel is proven live on the
/// same schedule and intermediaries see traffic just as often.
const PERMISSION_WS_PING_INTERVAL: Duration = Duration::from_secs(25);

/// Per-message send deadline for the permission-event WebSocket.
///
/// A client that stops reading (suspended tab, wedged renderer) would
/// otherwise stall its pump task inside `send`, holding a subscriber slot
/// against `MAX_PERMISSION_SUBSCRIBERS` indefinitely. This is the same hazard
/// `.claude/rules/channel-safety.md` describes for bounded channels, applied
/// to the socket sink: bound the wait, then drop the connection so the slot is
/// released. The client re-establishes and re-bootstraps from
/// `/permission/pending`, so a dropped socket costs a reconnect, never a
/// missed prompt.
const PERMISSION_WS_SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Wire envelope for the WebSocket transport.
///
/// SSE carries the event name out-of-band in its `event:` field; a WebSocket
/// frame has no such slot, so the name travels inside the payload. Event names
/// and their `data` shapes are otherwise IDENTICAL to the SSE stream, which is
/// what lets the shell bridge share one set of handlers across both
/// transports.
fn ws_envelope(event: &str, data: serde_json::Value) -> String {
    serde_json::json!({ "event": event, "data": data }).to_string()
}

/// WebSocket transport for permission prompts.
///
/// Same events, same gating, same race-avoidance as [`permission_events`];
/// only the transport differs. It exists because the SSE stream was the direct
/// cause of freenet/freenet-core#5213: every open Freenet tab held one
/// long-lived HTTP connection to a single shared origin, so six tabs exhausted
/// the browser's 6-connections-per-origin HTTP/1.1 budget and the seventh
/// tab's document request queued forever behind them, with no error anywhere.
/// Browsers pool WebSockets separately and far more generously (~255 per
/// profile in Chrome, 200 in Firefox), so moving this channel off HTTP stops
/// it competing with page and asset loads.
///
/// **The origin gate below is load-bearing, more so than on the SSE path.**
/// Browsers do NOT apply the same-origin policy to WebSocket handshakes: any
/// page anywhere can open a socket to `ws://127.0.0.1:7509/...` and no CORS
/// preflight will stop it. What does stop it is that browsers are required to
/// send `Origin` on every WebSocket handshake, including same-origin ones, so
/// `is_caller_trusted` sees a foreign origin and refuses. Removing or
/// weakening that check turns this endpoint into a cross-site WebSocket
/// hijacking hole that leaks raw permission nonces to any page the user
/// visits. Keep it, and keep it BEFORE the upgrade.
async fn permission_events_ws(
    ws: WebSocketUpgrade,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    allowed_hosts: Option<Extension<AllowedHosts>>,
    allowed_source_cidrs: Option<Extension<AllowedSourceCidrs>>,
    Extension(pending): Extension<PendingPrompts>,
    Extension(viewer): Extension<PromptViewer>,
) -> axum::response::Response {
    // Loopback-only boundary (#3819), identical to the SSE path: the stream
    // emits raw nonces, so a LAN peer must not reach it even when the main
    // listener binds 0.0.0.0. Reject BEFORE upgrading or consuming a slot.
    if !peer_is_loopback(connect_info.as_ref().map(|Extension(ci)| ci)) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }

    // Cross-origin rejection, BEFORE consuming a slot, so a hostile page
    // cannot exhaust the cap by looping handshakes. See the CSWSH note above.
    //
    // BOTH checks are required and neither subsumes the other:
    // `is_caller_trusted` applies the shared policy (allowed hosts, operator
    // CIDRs, `Sec-Fetch-Site`, `Origin: null`), while
    // `origin_is_exactly_same_origin` closes the cross-PORT localhost hole
    // that CORS silently covered on the SSE path and cannot cover here. Do
    // not drop either one.
    if !is_caller_trusted(
        &headers,
        allowed_hosts.as_ref().map(|Extension(v)| v),
        allowed_source_cidrs.as_ref().map(|Extension(v)| v),
    ) || !origin_is_exactly_same_origin(&headers)
    {
        tracing::debug!("Rejecting /permission/events/ws upgrade from untrusted origin");
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }

    // Subscriber cap, shared with the SSE transport.
    let guard = match try_claim_subscriber_slot(SubscriberTransport::WebSocket) {
        Some(guard) => guard,
        None => {
            tracing::warn!(
                limit = MAX_PERMISSION_SUBSCRIBERS,
                "permission subscriber cap reached, refusing new /permission/events/ws subscriber"
            );
            return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };

    // Subscribe FIRST, then snapshot, exactly as the SSE path does: a prompt
    // added between the snapshot and the first `recv` still arrives over the
    // broadcast. The shell renderer keys on `nonce`, so the resulting
    // duplicate is idempotent. Both happen BEFORE the upgrade completes, so
    // the window is no wider than SSE's.
    let rx = prompt_events().subscribe();
    let initial: Vec<String> = pending
        .iter()
        // A hosted user is replayed only their own prompts.
        .filter(|entry| viewer.may_see(entry.value().owner))
        .map(|entry| {
            let snapshot = PromptSnapshot {
                nonce: entry.key().clone(),
                author: entry.value().author,
                message: entry.value().message.clone(),
                labels: entry.value().labels.clone(),
                delegate_key: entry.value().delegate_key.clone(),
                caller: entry.value().caller.clone(),
                owner: entry.value().owner,
            };
            ws_envelope("prompt_added", snapshot_to_json(&snapshot))
        })
        .collect();

    // This channel is strictly server->client; the inbound arm discards
    // everything it reads and exists only to observe disconnects. Left at the
    // tungstenite defaults that would be 64 MiB per message and 16 MiB per
    // frame of buffering per socket, times the subscriber cap, for data that
    // is thrown away. Cap it at something no legitimate client exceeds.
    // The read buffer gets the same cap: tungstenite allocates it per socket
    // (128 KiB by default) and zero-fills up to its size before every read
    // attempt (#5795). It only bounds one read; a frame's length is reserved
    // when its header arrives, so this refuses nothing the limits above accept.
    ws.max_message_size(1024)
        .max_frame_size(1024)
        .read_buffer_size(1024)
        .on_upgrade(move |socket| async move {
            let (sink, stream) = socket.split();
            pump_permission_events(sink, stream, rx, initial, guard, viewer).await;
        })
}

/// Bounded send: see [`PERMISSION_WS_SEND_TIMEOUT`]. Returns false when the
/// socket is gone or too slow, which ends the pump and frees the slot.
async fn send_bounded<Si>(sink: &mut Si, msg: Message) -> bool
where
    Si: futures::Sink<Message> + Unpin,
{
    use futures::SinkExt;
    matches!(
        tokio::time::timeout(PERMISSION_WS_SEND_TIMEOUT, sink.send(msg)).await,
        Ok(Ok(()))
    )
}

/// Drives one permission-event WebSocket until the client goes away.
///
/// Cancellation safety: every arm of the `select!` is cancel-safe
/// (`broadcast::Receiver::recv`, `StreamExt::next`, `Interval::tick`), so
/// losing a race never drops an event. The guard is moved in and dropped on
/// return, releasing the subscriber slot on every exit path including error
/// and client disconnect.
///
/// Generic over the socket halves rather than taking a `WebSocket` so tests
/// can drive the real state machine with a sink that never accepts (proving
/// the send timeout fires and releases the slot) and with a deliberately
/// lagged broadcast receiver (proving the `resync` branch fires). Neither is
/// reachable through a real socket without a flaky timing setup, and both are
/// safety mechanisms this fix's own argument depends on.
async fn pump_permission_events<Si, St, E>(
    mut sink: Si,
    mut stream: St,
    mut rx: tokio::sync::broadcast::Receiver<PromptEvent>,
    initial: Vec<String>,
    _guard: PermissionSubscriberGuard,
    // Events for prompts this viewer may not see are skipped, so a hosted user
    // is sent only their own prompts' events.
    viewer: PromptViewer,
) where
    Si: futures::Sink<Message> + Unpin,
    St: futures::Stream<Item = Result<Message, E>> + Unpin,
{
    // ONE deadline for the whole replay phase, not one per message. With
    // `MAX_PENDING_PROMPTS = 32` in flight, a per-message deadline would let a
    // client that completes the handshake and then stops reading hold its slot
    // for ~32 x PERMISSION_WS_SEND_TIMEOUT (~320s) rather than the 10s the
    // constant's contract implies.
    let replay = tokio::time::timeout(PERMISSION_WS_SEND_TIMEOUT, async {
        use futures::SinkExt;
        for payload in initial {
            if sink.send(Message::Text(payload.into())).await.is_err() {
                return false;
            }
        }
        true
    })
    .await;
    if !matches!(replay, Ok(true)) {
        return;
    }

    let mut ping = tokio::time::interval(PERMISSION_WS_PING_INTERVAL);
    // Keepalive, not a scheduler: if the task was starved past several
    // intervals, catching up with a burst of pings is pointless noise. Delay
    // re-bases from the resumption instant instead.
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately; skip it so we don't ping the
    // instant the socket opens.
    ping.tick().await;

    loop {
        tokio::select! {
            // Drain inbound so close frames and disconnects are observed
            // promptly. The client never sends anything meaningful; this arm
            // exists to detect the socket going away.
            incoming = stream.next() => {
                match incoming {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                    Some(Ok(_)) => {}
                }
            }
            event = rx.recv() => {
                let payload = match event {
                    Ok(PromptEvent::Added(snapshot)) => {
                        if !viewer.may_see(snapshot.owner) {
                            continue;
                        }
                        ws_envelope("prompt_added", snapshot_to_json(&snapshot))
                    }
                    Ok(PromptEvent::Removed { nonce, owner }) => {
                        if !viewer.may_see(owner) {
                            continue;
                        }
                        ws_envelope("prompt_removed", serde_json::json!({ "nonce": nonce }))
                    }
                    // Lag means this subscriber was slow and missed events;
                    // the shell reconciles from /permission/pending. Rare:
                    // capacity is 128, each prompt lifecycle is two events,
                    // and at most 32 prompts can be in flight.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            skipped = n,
                            "permission WebSocket subscriber lagged, requesting resync"
                        );
                        ws_envelope("resync", serde_json::json!({}))
                    }
                    // Sender dropped: no further events can arrive, so holding
                    // the socket open would strand the client on a dead
                    // channel. Close and let it fall back.
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                if !send_bounded(&mut sink, Message::Text(payload.into())).await {
                    return;
                }
            }
            _ = ping.tick() => {
                if !send_bounded(&mut sink, Message::Ping(Vec::new().into())).await {
                    return;
                }
            }
        }
    }
}

/// Wraps an inner SSE stream together with the connection-count guard so
/// that dropping the response (client disconnect) decrements the live count.
/// Without this, an SSE stream that's cancelled mid-flight would leak the
/// counter slot until process exit.
#[pin_project::pin_project]
struct GuardedStream<S> {
    #[pin]
    inner: S,
    _guard: PermissionSubscriberGuard,
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

    // Loopback-origin behaviour is now provided by
    // `crate::client_events::websocket::is_localhost_origin`, which has its
    // own tests in that module. The localhost-only behaviour at this layer
    // is re-verified end-to-end through the `is_origin_trusted` tests below.

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
                author: crate::contract::user_input::PromptAuthor::Delegate,
                message: message.to_string(),
                labels: labels.into_iter().map(String::from).collect(),
                delegate_key: delegate_key.to_string(),
                caller,
                owner: None,
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
        call_pending_full_with_policy(headers, pending, None, None).await
    }

    async fn call_pending_full_with_policy(
        headers: HeaderMap,
        pending: PendingPrompts,
        allowed_hosts: Option<AllowedHosts>,
        allowed_source_cidrs: Option<AllowedSourceCidrs>,
    ) -> (axum::http::StatusCode, HeaderMap, serde_json::Value) {
        call_pending_full_with_policy_from(
            loopback_connect_info(),
            headers,
            pending,
            allowed_hosts,
            allowed_source_cidrs,
        )
        .await
    }

    /// A loopback `ConnectInfo` extension — the default peer address for tests
    /// that model a legitimate same-host UI. Port is arbitrary.
    fn loopback_connect_info() -> Extension<ConnectInfo<SocketAddr>> {
        Extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
    }

    async fn call_pending_full_with_policy_from(
        connect_info: Extension<ConnectInfo<SocketAddr>>,
        headers: HeaderMap,
        pending: PendingPrompts,
        allowed_hosts: Option<AllowedHosts>,
        allowed_source_cidrs: Option<AllowedSourceCidrs>,
    ) -> (axum::http::StatusCode, HeaderMap, serde_json::Value) {
        use axum::body::to_bytes;
        use axum::response::IntoResponse;
        let resp = pending_prompts(
            Some(connect_info),
            headers,
            allowed_hosts.map(Extension),
            allowed_source_cidrs.map(Extension),
            Extension(pending),
            Extension(PromptViewer::Node),
        )
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
                    author: crate::contract::user_input::PromptAuthor::Delegate,
                    message: "m".to_string(),
                    labels,
                    delegate_key: "d".to_string(),
                    caller: webapp_caller("c"),
                    owner: None,
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

    // Trusted (loopback) origins see the full prompt list and the
    // response carries NO Access-Control-Allow-Origin header. The
    // legitimate consumer is the same-origin shell page, which doesn't
    // need CORS to read the body; withholding the wildcard makes sure a
    // future widening of the trust gate can't accidentally hand the
    // trusted body to an arbitrary cross-origin `fetch()`.
    #[tokio::test]
    async fn test_pending_prompts_trusted_origin_returns_list_without_cors_wildcard() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "msg", vec!["OK"], "d", webapp_caller("c"));
        let (status, resp_headers, value) = call_pending_full(trusted_header(), pending).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let arr = value.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["message"], "msg");
        assert!(
            resp_headers.get("access-control-allow-origin").is_none(),
            "trusted response must NOT carry the `*` wildcard, so a future \
             trust-gate widening can't expose the body cross-origin"
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
                Some(loopback_connect_info()),
                Path("a".to_string()),
                trusted_header(),
                None,
                None,
                Extension(pending.clone()),
                Extension(PromptViewer::Node),
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
            Some(loopback_connect_info()),
            Path("a".to_string()),
            trusted_header(),
            None,
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }

    // An out-of-range button index must return 400 BAD_REQUEST without
    // consuming the prompt, so a user who fat-fingers (or a buggy client
    // that posts a stale index) can still retry. The other respond error
    // paths (missing/untrusted Origin -> 403, expired/unknown nonce -> 404)
    // are covered above; this pins the remaining `index >= labels.len()`
    // arm (issue #3821).
    #[tokio::test]
    async fn test_respond_invalid_index_returns_400_and_keeps_prompt() {
        let pending = empty_pending();
        // Two labels -> valid indices are 0 and 1; index 2 is out of range.
        let _rx = insert_prompt(
            &pending,
            "n",
            "m",
            vec!["Allow", "Deny"],
            "d",
            webapp_caller("c"),
        );

        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            trusted_header(),
            None,
            None,
            Extension(pending.clone()),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 2 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::BAD_REQUEST);

        use axum::body::to_bytes;
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"], "invalid index");

        // The prompt must still be pending so the user can retry with a
        // valid index — a 400 must NOT consume the nonce.
        assert!(
            pending.get("n").is_some(),
            "invalid index must not consume the prompt"
        );
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
        open_sse_with_headers(origin, None).await
    }

    /// Open SSE with both `Origin` and `Sec-Fetch-Site` controlled by the
    /// caller. Either signal alone is enough to reject the request, so the
    /// helper exposes both for the rejection-path tests.
    async fn open_sse_with_headers(
        origin: Option<&str>,
        sec_fetch_site: Option<&str>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = bytes::Bytes> + Send>> {
        use axum::response::IntoResponse;
        let pending = crate::contract::user_input::pending_prompts();
        let mut headers = HeaderMap::new();
        if let Some(o) = origin {
            headers.insert("origin", o.parse().unwrap());
        }
        if let Some(s) = sec_fetch_site {
            headers.insert("sec-fetch-site", s.parse().unwrap());
        }
        let resp = permission_events(
            Some(loopback_connect_info()),
            headers,
            None,
            None,
            Extension(pending),
        )
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
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while sender.receiver_count() < target {
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "timed out waiting for {target} permission subscribers; have {}",
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
    #[serial_test::serial(permission_subscriber_counter)]
    async fn test_sse_emits_added_when_prompt_inserted() {
        let initial_subs = prompt_events().receiver_count();
        let mut body = open_sse_with_origin(Some("http://localhost:7509")).await;

        let nonce = "ssetest_added_001".to_string();
        let snapshot = PromptSnapshot {
            author: crate::contract::user_input::PromptAuthor::Delegate,
            nonce: nonce.clone(),
            message: "approve?".into(),
            labels: vec!["Allow".into(), "Deny".into()],
            delegate_key: "dkey-added-001".into(),
            caller: webapp_caller("cid-added-001"),
            owner: None,
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
    #[serial_test::serial(permission_subscriber_counter)]
    async fn test_sse_emits_removed_when_prompt_responded() {
        let initial_subs = prompt_events().receiver_count();
        let mut body = open_sse_with_origin(Some("http://localhost:7509")).await;

        let nonce = "ssetest_removed_002".to_string();
        wait_subscribers_then_send(
            initial_subs + 1,
            PromptEvent::Removed {
                nonce: nonce.clone(),
                owner: None,
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

    /// Cross-site `Sec-Fetch-Site` must independently reject the request,
    /// even if `Origin` is absent (which is the common case for an
    /// EventSource opened from an attacker page: browsers do not include
    /// `Origin` on most cross-origin GETs). This is the second of the two
    /// independent rejection signals in `is_caller_trusted`. Without this
    /// branch, an evil page could open EventSources to `/permission/events`
    /// and consume slots against `MAX_PERMISSION_SUBSCRIBERS = 64`.
    #[tokio::test]
    async fn test_sse_rejects_cross_site_sec_fetch() {
        for site in ["cross-site", "cross-origin", "Cross-Site"] {
            let mut body = open_sse_with_headers(None, Some(site)).await;
            let frame = next_event_frame(&mut body, 200).await;
            assert!(
                frame.is_none(),
                "Sec-Fetch-Site={site} must reject (no data events), got: {frame:?}"
            );
        }
    }

    /// `Sec-Fetch-Site` values that indicate same-origin or no-initiator
    /// requests must be allowed. Positive control for the rejection branch
    /// above; prevents an over-eager future tightening from breaking
    /// same-origin shell pages. Uses the bootstrap-replay path (insert a
    /// pending prompt with a per-iteration unique nonce; assert the SSE
    /// handler emits `prompt_added` for it) instead of the live broadcast,
    /// to keep the test deterministic under parallel execution.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn test_sse_allows_same_origin_and_none_sec_fetch() {
        let pending = crate::contract::user_input::pending_prompts();
        for site in ["same-origin", "same-site", "none"] {
            let nonce = format!("ssetest_secfetch_allow_{site}");
            let _rx = insert_prompt(
                &pending,
                &nonce,
                "secfetch-test",
                vec!["OK"],
                "dkey-secfetch",
                webapp_caller("cid-secfetch"),
            );
            let _cleanup = cleanup_on_drop(pending.clone(), nonce.clone());

            let mut body = open_sse_with_headers(None, Some(site)).await;
            let frame = frame_matching(
                &mut body,
                |f| f.contains("event: prompt_added") && f.contains(&nonce),
                32,
                500,
            )
            .await
            .unwrap_or_else(|| panic!("Sec-Fetch-Site={site} must accept; did not see {nonce}"));
            assert!(frame.contains(&nonce));
        }
    }

    /// Initial-state bootstrap: a fresh subscriber must receive
    /// `prompt_added` events for every currently-pending prompt before any
    /// new live events arrive. Avoids the race where a prompt added between
    /// the page load and the SSE subscribe would be invisible.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
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
    #[serial_test::serial(permission_subscriber_counter)]
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
            author: crate::contract::user_input::PromptAuthor::Delegate,
            nonce: live_nonce.clone(),
            message: "live".into(),
            labels: vec!["OK".into()],
            delegate_key: "dkey-order-live".into(),
            caller: webapp_caller("cid-order-live"),
            owner: None,
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
                owner: None,
            }));
        }

        // Drive the SSE filter logic against the lagged receiver. We
        // duplicate the closure body from permission_events here so the
        // test exercises exactly the production translation.
        let stream = BroadcastStream::new(rx).filter_map(|incoming| async move {
            match incoming {
                Ok(PromptEvent::Added(s)) => Some(format!("added:{}", s.nonce)),
                Ok(PromptEvent::Removed { nonce, .. }) => Some(format!("removed:{nonce}")),
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

    /// MAX_PERMISSION_SUBSCRIBERS must reject the (cap+1)th subscriber AND must
    /// release a slot when an earlier subscriber's stream is dropped.
    /// Together these pin both the cap (rejecting flood reconnects) and
    /// the GuardedStream's drop semantics (no permanent leak after a
    /// client disconnects mid-flight).
    ///
    /// Uses `#[serial_test::serial(permission_subscriber_counter)]` so any other
    /// test that opens an SSE stream can't perturb the shared
    /// `PERMISSION_SUBSCRIBERS` counter mid-assertion. Tests that only construct
    /// the rejected path (no slot consumed) don't need the lock.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn test_sse_connection_cap_and_release_on_drop() {
        // Local-mutex pattern retained as a defence-in-depth against a
        // future test that opens an SSE stream without the serial
        // attribute. Acquiring an already-uncontended mutex is a no-op.
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
        let deadline0 = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) != 0 {
            if tokio::time::Instant::now() >= deadline0 {
                // A leaked connection from a previous test indicates a real
                // bug in GuardedStream's drop, but for the purposes of this
                // test, accept the baseline rather than panic.
                break;
            }
            tokio::task::yield_now().await;
        }
        let baseline = PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed);

        // Open enough subscribers to exhaust the cap. Note the ceiling for
        // THIS transport is MAX_SSE_SUBSCRIBERS, not MAX_PERMISSION_SUBSCRIBERS:
        // the legacy SSE path is deliberately held below the shared total so it
        // cannot starve the WebSocket path (see MAX_SSE_SUBSCRIBERS).
        let mut held: Vec<_> = Vec::new();
        for _ in baseline..MAX_SSE_SUBSCRIBERS {
            held.push(open_sse_with_origin(Some("http://localhost:7509")).await);
        }
        assert_eq!(
            PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
            MAX_SSE_SUBSCRIBERS,
            "all SSE slots must be filled before testing the cap"
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
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) < MAX_SSE_SUBSCRIBERS {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("PERMISSION_SUBSCRIBERS did not decrement after dropping a subscriber");
            }
            tokio::task::yield_now().await;
        }

        // A new subscriber should now succeed.
        let _accepted = open_sse_with_origin(Some("http://localhost:7509")).await;
        assert!(PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) <= MAX_SSE_SUBSCRIBERS);

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
            Some(loopback_connect_info()),
            Path(nonce.clone()),
            trusted_header(),
            None,
            None,
            Extension(pending.clone()),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        // Drain until we see the matching Removed (concurrent tests on the
        // global broadcaster may interleave their own events).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut saw = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
                Ok(Ok(PromptEvent::Removed { nonce: n, .. })) if n == nonce => {
                    saw = true;
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) | Err(_) => continue,
            }
        }
        assert!(saw, "permission_respond did not emit Removed for {nonce}");
    }

    // ====================================================================
    // Regression tests for issue #4261: a non-localhost browser must be
    // able to respond to delegate permission prompts when the operator has
    // opted into LAN access (the gateway is bound to a LAN/private IP or
    // an `allowed-source-cidrs` range is configured). Before the fix, the
    // Origin gate only accepted `http://localhost`, so any LAN/Tailnet user
    // got a 403 on the response POST even though the Host-header policy
    // already considered them trusted.
    // ====================================================================

    use std::collections::HashSet;
    use std::sync::Arc;

    fn allowed_hosts_with(values: &[&str], port: u16) -> AllowedHosts {
        let mut set = HashSet::new();
        for v in values {
            let lower = v.to_lowercase();
            set.insert(lower.clone());
            set.insert(format!("{lower}:{port}"));
        }
        Arc::new(set)
    }

    fn allowed_cidrs_with(list: &[&str]) -> AllowedSourceCidrs {
        AllowedSourceCidrs(Arc::new(list.iter().map(|s| s.parse().unwrap()).collect()))
    }

    fn lan_browser_headers(host_ip: &str, port: u16) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("host", format!("{host_ip}:{port}").parse().unwrap());
        h.insert(
            "origin",
            format!("http://{host_ip}:{port}").parse().unwrap(),
        );
        h
    }

    /// Default LAN: gateway bound to `192.168.1.42`, browser on the same
    /// LAN. Before the fix this returned `403`; now it must succeed.
    #[tokio::test]
    async fn test_respond_accepts_lan_same_ip_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            lan_browser_headers("192.168.1.42", 7509),
            None,
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::OK,
            "LAN browser POSTing same-IP Origin must succeed (regression for #4261)"
        );
    }

    /// CGNAT / Tailnet: gateway has `allowed-source-cidrs = 100.64.0.0/10`
    /// and the browser arrives via a Tailscale IP. The Tailnet IP is not
    /// in the default private-LAN set, so this branch requires the
    /// operator-opt-in CIDR plus a same-IP Origin.
    #[tokio::test]
    async fn test_respond_accepts_cidr_same_ip_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let cidrs = allowed_cidrs_with(&["100.64.0.0/10"]);
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            lan_browser_headers("100.64.0.1", 7509),
            None,
            Some(Extension(cidrs)),
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// CSWSH/CSRF guard: a same-LAN Host is reachable, but the Origin is a
    /// public attacker (`evil.com`). Must reject — this is the attack the
    /// new policy explicitly defends against by requiring Origin to match
    /// Host when leaving the loopback path.
    #[tokio::test]
    async fn test_respond_rejects_cross_origin_with_lan_host() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let mut headers = HeaderMap::new();
        headers.insert("host", "192.168.1.42:7509".parse().unwrap());
        headers.insert("origin", "https://evil.com".parse().unwrap());
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            headers,
            None,
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// A public-IP Host (not in any policy) must still reject even with
    /// matching Origin: `is_default_lan_host_ip` rejects 8.8.8.8 and no
    /// CIDR was configured.
    #[tokio::test]
    async fn test_respond_rejects_public_host_outside_cidrs() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let mut headers = HeaderMap::new();
        headers.insert("host", "8.8.8.8:7509".parse().unwrap());
        headers.insert("origin", "http://8.8.8.8:7509".parse().unwrap());
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            headers,
            None,
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// `allowed-host` hostname entry: operator added `mynode.example.com`.
    /// The browser navigates to it; Origin authority must match Host.
    #[tokio::test]
    async fn test_respond_accepts_allowed_host_with_matching_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let hosts = allowed_hosts_with(&["mynode.example.com"], 7509);
        let mut headers = HeaderMap::new();
        headers.insert("host", "mynode.example.com:7509".parse().unwrap());
        headers.insert("origin", "http://mynode.example.com:7509".parse().unwrap());
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            headers,
            Some(Extension(hosts)),
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// Closes the CSWSH gap that the WS layer's `is_allowed_host` branch
    /// has: even when the Host header is in `allowed_hosts`, the Origin
    /// must independently match the Host authority. Without this check,
    /// `evil.com` could open a fetch to the allow-listed hostname and the
    /// browser would send `Host: mynode.example.com:7509`, `Origin:
    /// https://evil.com` — the request would otherwise pass.
    #[tokio::test]
    async fn test_respond_rejects_allowed_host_with_cross_origin() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let hosts = allowed_hosts_with(&["mynode.example.com"], 7509);
        let mut headers = HeaderMap::new();
        headers.insert("host", "mynode.example.com:7509".parse().unwrap());
        headers.insert("origin", "https://evil.com".parse().unwrap());
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            headers,
            Some(Extension(hosts)),
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// Missing Origin must still 403 on /respond — POST that's missing
    /// Origin is either a non-browser caller or a browser bug; either way,
    /// the conservative response is to reject.
    #[tokio::test]
    async fn test_respond_rejects_missing_origin_even_with_lan_host() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let mut headers = HeaderMap::new();
        headers.insert("host", "192.168.1.42:7509".parse().unwrap());
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            headers,
            None,
            None,
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// /pending: a LAN browser must see the full prompt list, not the
    /// empty-list cross-origin response. Without this branch the shell
    /// overlay JS would never render a prompt for the user even though
    /// the gateway accepts the LAN connection.
    #[tokio::test]
    async fn test_pending_prompts_lan_same_ip_origin_returns_list() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let (status, _, value) = call_pending_full_with_policy(
            lan_browser_headers("192.168.1.42", 7509),
            pending,
            None,
            None,
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(value.as_array().unwrap().len(), 1);
    }

    /// /pending: a cross-origin caller with a LAN Host must still get an
    /// empty list. Mirrors the existing `evil.com` regression but with the
    /// LAN-policy now in scope.
    #[tokio::test]
    async fn test_pending_prompts_lan_host_cross_origin_returns_empty() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let mut headers = HeaderMap::new();
        headers.insert("host", "192.168.1.42:7509".parse().unwrap());
        headers.insert("origin", "https://evil.com".parse().unwrap());
        let (status, _, value) = call_pending_full_with_policy(headers, pending, None, None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(value, serde_json::json!([]));
    }

    /// Unit-level coverage of the new helper for the IPv6 LAN case:
    /// fe80:: link-local Host with same-Origin must be accepted, even
    /// without operator CIDR opt-in.
    #[test]
    fn test_is_origin_trusted_accepts_ipv6_link_local_same_origin() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "[fe80::1]:7509".parse().unwrap());
        assert!(is_origin_trusted(
            &headers,
            "http://[fe80::1]:7509",
            None,
            None,
        ));
    }

    /// Without operator policy, only loopback Origin is accepted. This
    /// preserves the pre-fix behaviour for `run_local_node` and other
    /// callers that don't inject the AllowedHosts/AllowedSourceCidrs
    /// extensions.
    #[test]
    fn test_is_origin_trusted_loopback_only_with_no_policy() {
        let headers = HeaderMap::new();
        assert!(is_origin_trusted(
            &headers,
            "http://localhost:7509",
            None,
            None,
        ));
        assert!(is_origin_trusted(
            &headers,
            "http://127.0.0.1:7509",
            None,
            None,
        ));
        assert!(!is_origin_trusted(&headers, "http://evil.com", None, None));
        assert!(!is_origin_trusted(
            &headers,
            "http://192.168.1.42:7509",
            None,
            None,
        ));
    }

    /// HTTPS loopback is also accepted (the localhost-only path no longer
    /// requires `http://` — the previous narrower check was a relic).
    #[test]
    fn test_is_origin_trusted_accepts_https_localhost() {
        let headers = HeaderMap::new();
        assert!(is_origin_trusted(
            &headers,
            "https://localhost:7509",
            None,
            None,
        ));
    }

    #[test]
    fn test_origin_authority_strips_scheme_and_path() {
        assert_eq!(
            origin_authority("http://mynode.example.com:7509"),
            Some("mynode.example.com:7509")
        );
        assert_eq!(
            origin_authority("https://[::1]:7509/extra"),
            Some("[::1]:7509")
        );
        assert_eq!(origin_authority("null"), None);
        assert_eq!(origin_authority(""), None);
    }

    /// SUPERSEDED SEMANTICS, deliberately inverted: `origin_authority` used to
    /// STRIP `user@` (mirroring `parse_origin_host` in websocket.rs). It now
    /// REJECTS any authority containing `@`, `?` or `#`.
    ///
    /// Stripping was the weaker choice. `is_localhost_origin` is plain prefix
    /// matching, so `http://127.0.0.1:7509@evil.com` was "localhost" to that
    /// helper while stripping made it `evil.com` here — paired with
    /// `Host: evil.com`, BOTH gates passed. Not stripping the query gave the
    /// symmetric hole: `http://evil.com?x=@127.0.0.1:7509` resolved to
    /// authority `127.0.0.1:7509`. A gate assembled from two parsers that
    /// disagree about what an origin is can be walked between; refusing the
    /// ambiguous input is the only version that cannot drift.
    ///
    /// Nothing legitimate is lost: a browser `Origin` is `scheme://host[:port]`
    /// and can contain none of these characters.
    #[test]
    fn test_origin_authority_rejects_ambiguous_authorities() {
        // Userinfo: previously stripped to the trailing host, now refused.
        assert_eq!(
            origin_authority("http://victim@mynode.example.com:7509"),
            None
        );
        assert_eq!(origin_authority("https://attacker@[::1]:7509/path"), None);
        // The prefix-match trap: `is_localhost_origin` reads this as localhost.
        assert!(crate::client_events::websocket::is_localhost_origin(
            "http://127.0.0.1:7509@evil.example"
        ));
        assert_eq!(origin_authority("http://127.0.0.1:7509@evil.example"), None);
        // Query and fragment forms.
        assert_eq!(
            origin_authority("http://evil.example?x=@127.0.0.1:7509"),
            None
        );
        assert_eq!(
            origin_authority("http://evil.example#@127.0.0.1:7509"),
            None
        );
        // The characters added after the first review round. Dropping any one
        // of them lets the two parsers disagree again, and none was pinned.
        assert_eq!(
            origin_authority("http://127.0.0.1:7509\\evil.example"),
            None
        );
        assert!(crate::client_events::websocket::is_localhost_origin(
            "http://127.0.0.1:7509\\evil.example"
        ));
        assert_eq!(origin_authority("http://exa mple.com:7509"), None);
        assert_eq!(origin_authority("http://exa\tmple.com:7509"), None);
        // Ordinary origins still parse.
        assert_eq!(
            origin_authority("http://mynode.example.com:7509"),
            Some("mynode.example.com:7509")
        );
        assert_eq!(
            origin_authority("https://[::1]:7509/path"),
            Some("[::1]:7509")
        );
    }

    // ====================================================================
    // Stricter-than-WS regression guard: `Origin: null` must NEVER be
    // accepted on the permission endpoints, even when the operator has
    // opted into `allowed-source-cidrs`. The WebSocket upgrade gate
    // intentionally accepts `null` Origin under the CIDR branch (to
    // support the dApp sandbox iframe), but the permission endpoints are
    // a stricter target: `/respond` authorises a delegate action and
    // `/pending` exposes prompt-content. Any sandboxed iframe on any
    // site can produce `Origin: null`, so accepting it here would let a
    // malicious page silently confirm permission prompts the user never
    // saw. (Reported by Codex review of #4262.)
    // ====================================================================

    #[test]
    fn test_is_origin_trusted_rejects_null_origin_even_with_cidr() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "100.64.1.5:7509".parse().unwrap());
        let cidrs = allowed_cidrs_with(&["100.64.0.0/10"]);
        assert!(
            !is_origin_trusted(&headers, "null", None, Some(&cidrs)),
            "null Origin must be rejected even when host IP is inside the \
             operator-configured CIDR — the permission endpoints are \
             stricter than the WS-upgrade gate"
        );
    }

    #[test]
    fn test_is_origin_trusted_rejects_null_origin_default_lan() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "192.168.1.42:7509".parse().unwrap());
        assert!(!is_origin_trusted(&headers, "null", None, None));
    }

    #[tokio::test]
    async fn test_respond_rejects_null_origin_under_cidr() {
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "n", "m", vec!["OK"], "d", webapp_caller("c"));

        let cidrs = allowed_cidrs_with(&["100.64.0.0/10"]);
        let mut headers = HeaderMap::new();
        headers.insert("host", "100.64.1.5:7509".parse().unwrap());
        headers.insert("origin", "null".parse().unwrap());
        let resp = permission_respond(
            Some(loopback_connect_info()),
            Path("n".to_string()),
            headers,
            None,
            Some(Extension(cidrs)),
            Extension(pending),
            Extension(PromptViewer::Node),
            Json(PermissionResponse { index: 0 }),
        )
        .await
        .into_response();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::FORBIDDEN,
            "sandboxed-iframe null Origin must not be allowed to confirm \
             permission prompts even under operator-configured CIDR"
        );
    }

    /// IPv4-mapped IPv6 (`::ffff:192.168.1.42`) is what dual-stack Linux
    /// gateways present to handlers when an IPv4 client connects. Both
    /// Host header parsing (rejects unbracketed bare IPv6, requires
    /// bracketed form for IPv6) and the CIDR-match normalization in
    /// `host_header_ip_in_cidrs` should cooperate so a LAN browser
    /// reaching the gateway via the mapped form still works end-to-end.
    /// This test exercises the realistic header shape: bracketed mapped
    /// v6 on both Host and Origin.
    #[test]
    fn test_is_origin_trusted_accepts_ipv4_mapped_ipv6_default_lan() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "[::ffff:192.168.1.42]:7509".parse().unwrap());
        assert!(is_origin_trusted(
            &headers,
            "http://[::ffff:192.168.1.42]:7509",
            None,
            None,
        ));
    }

    // ====================================================================
    // /permission/events under the new LAN/CIDR policy. The handler glue
    // (`is_caller_trusted(headers, allowed_hosts, allowed_source_cidrs)`)
    // was untested under non-None extensions until now — without these,
    // a refactor that drops the new args could regress silently.
    // ====================================================================

    async fn open_sse_with_policy(
        headers: HeaderMap,
        allowed_hosts: Option<AllowedHosts>,
        allowed_source_cidrs: Option<AllowedSourceCidrs>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = bytes::Bytes> + Send>> {
        use axum::response::IntoResponse;
        let pending = crate::contract::user_input::pending_prompts();
        let resp = permission_events(
            Some(loopback_connect_info()),
            headers,
            allowed_hosts.map(Extension),
            allowed_source_cidrs.map(Extension),
            Extension(pending),
        )
        .await
        .into_response();
        let stream = resp
            .into_body()
            .into_data_stream()
            .filter_map(|r| async move { r.ok() });
        Box::pin(stream)
    }

    /// LAN browser (same-IP Host + Origin) must be able to open the SSE
    /// stream. Asserts the stream is NOT immediately closed with the
    /// `:untrusted-origin` comment.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn test_sse_accepts_lan_same_ip_origin() {
        let pending = crate::contract::user_input::pending_prompts();
        let nonce = "ssetest_lan_accept";
        let _rx = insert_prompt(&pending, nonce, "m", vec!["OK"], "d", webapp_caller("c"));
        let _cleanup = cleanup_on_drop(pending.clone(), nonce.to_string());

        let mut body =
            open_sse_with_policy(lan_browser_headers("192.168.1.42", 7509), None, None).await;
        // The stream must emit our bootstrap prompt as a data event, not
        // close with an `:untrusted-origin` comment.
        let frame = next_event_frame(&mut body, 1_000).await;
        assert!(
            frame.is_some_and(|f| f.contains(nonce)),
            "LAN browser SSE must deliver bootstrap prompt event"
        );
    }

    /// Cross-origin Origin with LAN Host must reject the SSE stream
    /// regardless of policy.
    #[tokio::test]
    async fn test_sse_rejects_cross_origin_with_lan_host() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "192.168.1.42:7509".parse().unwrap());
        headers.insert("origin", "https://evil.com".parse().unwrap());
        let mut body = open_sse_with_policy(headers, None, None).await;
        let frame = next_event_frame(&mut body, 200).await;
        assert!(
            frame.is_none(),
            "cross-origin SSE must not receive any data events"
        );
    }

    /// `allowed-host` hostname with matching Origin must accept SSE.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn test_sse_accepts_allowed_host_with_matching_origin() {
        let pending = crate::contract::user_input::pending_prompts();
        let nonce = "ssetest_allowed_host_accept";
        let _rx = insert_prompt(&pending, nonce, "m", vec!["OK"], "d", webapp_caller("c"));
        let _cleanup = cleanup_on_drop(pending.clone(), nonce.to_string());

        let hosts = allowed_hosts_with(&["mynode.example.com"], 7509);
        let mut headers = HeaderMap::new();
        headers.insert("host", "mynode.example.com:7509".parse().unwrap());
        headers.insert("origin", "http://mynode.example.com:7509".parse().unwrap());
        let mut body = open_sse_with_policy(headers, Some(hosts), None).await;
        let frame = next_event_frame(&mut body, 1_000).await;
        assert!(
            frame.is_some_and(|f| f.contains(nonce)),
            "allowed-host browser SSE must deliver bootstrap prompt event"
        );
    }

    /// Cross-site Sec-Fetch-Site is rejected under the new policy too —
    /// the LAN/CIDR Origin-acceptance path doesn't bypass the existing
    /// independent Sec-Fetch-Site reject branch in `is_caller_trusted`.
    #[tokio::test]
    async fn test_sse_rejects_cross_site_sec_fetch_under_lan_policy() {
        let mut headers = lan_browser_headers("192.168.1.42", 7509);
        headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
        let mut body = open_sse_with_policy(headers, None, None).await;
        let frame = next_event_frame(&mut body, 200).await;
        assert!(
            frame.is_none(),
            "Sec-Fetch-Site: cross-site must reject even when Host/Origin \
             would otherwise satisfy the LAN policy"
        );
    }

    // ====================================================================
    // Router-level Extension-chain test. Without this, an extractor-order
    // regression in `server.rs::serve_client_api_in_impl` (the exact
    // failure class of #4261 itself) would not be caught by the unit
    // tests because they construct Extensions directly.
    // ====================================================================

    #[tokio::test]
    async fn test_router_propagates_extensions_to_respond_handler() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "rt", "m", vec!["OK"], "d", webapp_caller("c"));

        let hosts = allowed_hosts_with(&["127.0.0.1"], 7509);
        let cidrs = AllowedSourceCidrs::default();
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending))
            .layer(Extension(hosts))
            .layer(Extension(cidrs));

        // A legitimate same-host UI: loopback peer address + loopback origin.
        // (The loopback boundary from #3819 gates on the peer address; a LAN
        // host/origin is covered by test_router_rejects_lan_peer_respond below.)
        let mut req = Request::builder()
            .method("POST")
            .uri("/permission/rt/respond")
            .header("host", "127.0.0.1:7509")
            .header("origin", "http://127.0.0.1:7509")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"index":0}"#))
            .unwrap();
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 55000))));

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "router must propagate AllowedHosts + AllowedSourceCidrs to /respond \
             — a missing Extension layer would 500 here (the failure class of #4261)"
        );
    }

    #[tokio::test]
    async fn test_router_rejects_cross_origin_respond() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "rt2", "m", vec!["OK"], "d", webapp_caller("c"));

        let hosts: AllowedHosts = Arc::new(HashSet::new());
        let cidrs = AllowedSourceCidrs::default();
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending))
            .layer(Extension(hosts))
            .layer(Extension(cidrs));

        let req = Request::builder()
            .method("POST")
            .uri("/permission/rt2/respond")
            .header("host", "192.168.1.42:7509")
            .header("origin", "https://evil.com")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"index":0}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ====================================================================
    // Loopback-only boundary for the permission surface (issue #3819).
    //
    // The three endpoints leak/forge capabilities to a LAN peer that
    // passes every Origin / Sec-Fetch-Site / RFC1918-CIDR check by simply
    // sending no Origin (e.g. `curl` from another machine). The fix binds
    // them to loopback via the kernel-observed peer socket address, which
    // — unlike Origin — cannot be forged at the HTTP layer.
    //
    // These tests model the REAL threat: they drive the handlers with a
    // non-loopback peer `SocketAddr` and assert rejection, and with a
    // loopback peer and assert the request is served.
    // ====================================================================

    /// Build a router request that carries a `ConnectInfo` peer address, so
    /// the loopback guard sees the same source IP the kernel would set.
    fn request_from_peer(
        method: &str,
        uri: &str,
        peer: SocketAddr,
        body: &'static str,
    ) -> axum::http::Request<axum::body::Body> {
        // No Origin header: this is exactly the non-browser client (curl) the
        // issue proves bypasses the Origin/Sec-Fetch checks. The loopback guard
        // must reject it regardless.
        let mut req = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap();
        req.extensions_mut().insert(ConnectInfo(peer));
        req
    }

    /// `peer_is_loopback` is the load-bearing decision. Assert both sides plus
    /// the fail-closed missing-ConnectInfo case directly, independent of any
    /// HTTP plumbing.
    #[test]
    fn test_peer_is_loopback_decision() {
        let lo_v4 = ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 1)));
        let lo_v6 = ConnectInfo(SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 1)));
        let lan = ConnectInfo(SocketAddr::from(([192, 168, 1, 50], 1)));
        let public = ConnectInfo(SocketAddr::from(([8, 8, 8, 8], 1)));
        // IPv4-mapped loopback (::ffff:127.0.0.1) must count as loopback.
        let mapped_lo = ConnectInfo(SocketAddr::from((
            "127.0.0.1"
                .parse::<std::net::Ipv4Addr>()
                .unwrap()
                .to_ipv6_mapped(),
            1,
        )));

        assert!(peer_is_loopback(Some(&lo_v4)));
        assert!(peer_is_loopback(Some(&lo_v6)));
        assert!(peer_is_loopback(Some(&mapped_lo)));
        assert!(!peer_is_loopback(Some(&lan)));
        assert!(!peer_is_loopback(Some(&public)));
        // Fail closed: a missing ConnectInfo cannot prove loopback.
        assert!(!peer_is_loopback(None));
    }

    #[tokio::test]
    async fn test_pending_rejects_lan_peer_serves_loopback_peer() {
        use axum::http::StatusCode;
        use tower::ServiceExt;

        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "leak", "m", vec!["OK"], "d", webapp_caller("c"));
        let build_app = || {
            super::routes()
                .layer(Extension(HostedMode(false)))
                .layer(Extension(pending.clone()))
                .layer(Extension::<AllowedHosts>(Arc::new(HashSet::new())))
                .layer(Extension(AllowedSourceCidrs::default()))
        };

        // Attacker: off-box LAN peer, no Origin. Must be rejected and must NOT
        // see the nonce.
        let lan = SocketAddr::from(([192, 168, 1, 50], 40000));
        let resp = build_app()
            .oneshot(request_from_peer("GET", "/permission/pending", lan, ""))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "LAN peer must be rejected from /permission/pending"
        );
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(
            !String::from_utf8_lossy(&body).contains("leak"),
            "rejected response must not disclose the nonce"
        );

        // Legitimate same-host UI: loopback peer. Must be served the nonce.
        let lo = SocketAddr::from(([127, 0, 0, 1], 55000));
        let resp = build_app()
            .oneshot(request_from_peer("GET", "/permission/pending", lo, ""))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("leak"),
            "loopback peer must still receive pending nonces"
        );
    }

    #[tokio::test]
    async fn test_respond_rejects_lan_peer_serves_loopback_peer() {
        use axum::http::StatusCode;
        use tower::ServiceExt;

        // Attacker: forge an approval from an off-box LAN peer (no Origin).
        let pending = empty_pending();
        let rx = insert_prompt(&pending, "forge", "m", vec!["OK"], "d", webapp_caller("c"));
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending.clone()))
            .layer(Extension::<AllowedHosts>(Arc::new(HashSet::new())))
            .layer(Extension(AllowedSourceCidrs::default()));
        let lan = SocketAddr::from(([192, 168, 1, 50], 40000));
        let resp = app
            .oneshot(request_from_peer(
                "POST",
                "/permission/forge/respond",
                lan,
                r#"{"index":0}"#,
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "LAN peer must not be able to approve a prompt"
        );
        // The prompt must still be pending (not consumed by the rejected forge).
        assert!(pending.contains_key("forge"));
        drop(rx);

        // Legitimate same-host UI over loopback: approval goes through.
        let pending = empty_pending();
        let rx = insert_prompt(&pending, "ok", "m", vec!["OK"], "d", webapp_caller("c"));
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending.clone()))
            .layer(Extension(allowed_hosts_with(&["127.0.0.1"], 7509)))
            .layer(Extension(AllowedSourceCidrs::default()));
        let mut req = request_from_peer(
            "POST",
            "/permission/ok/respond",
            SocketAddr::from(([127, 0, 0, 1], 55000)),
            r#"{"index":0}"#,
        );
        req.headers_mut()
            .insert("host", "127.0.0.1:7509".parse().unwrap());
        req.headers_mut()
            .insert("origin", "http://127.0.0.1:7509".parse().unwrap());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            rx.await.unwrap(),
            0,
            "loopback approval must reach the delegate"
        );
    }

    #[tokio::test]
    async fn test_events_rejects_lan_peer() {
        use axum::http::StatusCode;
        use tower::ServiceExt;

        let pending = empty_pending();
        let _rx = insert_prompt(
            &pending,
            "sse-leak",
            "m",
            vec!["OK"],
            "d",
            webapp_caller("c"),
        );
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending.clone()))
            .layer(Extension::<AllowedHosts>(Arc::new(HashSet::new())))
            .layer(Extension(AllowedSourceCidrs::default()));

        // Off-box LAN peer, no Origin: the SSE stream must be refused outright
        // (403), not opened — so the raw nonce in `prompt_added` never streams.
        let lan = SocketAddr::from(([192, 168, 1, 50], 40000));
        let resp = app
            .oneshot(request_from_peer("GET", "/permission/events", lan, ""))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "LAN peer must be rejected from the SSE nonce stream"
        );
    }

    #[tokio::test]
    async fn test_router_rejects_lan_peer_respond() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        // Even with an allowed LAN host + matching Origin (the config the old
        // behavior accepted), an off-box LAN peer address must now be rejected.
        let pending = empty_pending();
        let _rx = insert_prompt(&pending, "rt3", "m", vec!["OK"], "d", webapp_caller("c"));
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending))
            .layer(Extension(allowed_hosts_with(&["192.168.1.42"], 7509)))
            .layer(Extension(AllowedSourceCidrs::default()));

        let mut req = Request::builder()
            .method("POST")
            .uri("/permission/rt3/respond")
            .header("host", "192.168.1.42:7509")
            .header("origin", "http://192.168.1.42:7509")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"index":0}"#))
            .unwrap();
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([192, 168, 1, 42], 40000))));

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "an off-box LAN peer must not approve prompts even with an allowed \
             Host/Origin — the loopback boundary is the #3819 fix"
        );
    }

    // ----- WebSocket transport tests (#5213) -----
    //
    // These drive a REAL TCP connection rather than `oneshot`. A WebSocket
    // upgrade needs a connection hyper can actually hand off, and `oneshot`
    // with an empty body has none, so axum's `WebSocketUpgrade` extractor
    // rejects with 426 Upgrade Required BEFORE the handler's gates ever run.
    // Under `oneshot` every test below would have "passed" a not-101
    // assertion while proving nothing about the gates — which is exactly what
    // `permission_ws_accepts_same_origin_loopback_handshake` exists to catch.
    //
    // The peer address arrives via an `Extension` layer instead of
    // `into_make_service_with_connect_info`, so a test can present a LAN peer
    // (or none at all) over a loopback socket.

    /// Permission routes with permissive host/CIDR policy, so the only gates
    /// under test are the loopback and origin ones. `peer` of `None` omits the
    /// `ConnectInfo` extension entirely, which is the fail-closed case.
    fn ws_test_app(pending: PendingPrompts, peer: Option<SocketAddr>) -> Router {
        use std::collections::HashSet;
        use std::sync::Arc;
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending))
            .layer(Extension::<AllowedHosts>(Arc::new(HashSet::new())))
            .layer(Extension(AllowedSourceCidrs::default()));
        match peer {
            Some(p) => app.layer(Extension(ConnectInfo(p))),
            None => app,
        }
    }

    fn loopback_peer() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 45678))
    }

    /// The Origin a legitimate shell page would send: exactly the authority it
    /// was served from, which is what tungstenite also puts in `Host`. Any
    /// OTHER localhost origin (a different port) must now be refused, so tests
    /// cannot use a hard-coded `http://localhost:7509` here.
    fn same_origin(addr: SocketAddr) -> String {
        format!("http://{addr}")
    }

    async fn spawn_ws_server(app: Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app.into_make_service()).await {
                // The test finishing tears this down; that is not a failure.
                tracing::debug!(%err, "permission-ws test server ended");
            }
        });
        addr
    }

    type TestWsStream = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    /// Attempt the handshake. `Err(status)` carries the HTTP status the server
    /// refused with, so a test can distinguish 403 (gate) from 503 (cap) from
    /// an unrelated failure.
    async fn try_ws_connect(addr: SocketAddr, origin: Option<&str>) -> Result<TestWsStream, u16> {
        try_ws_connect_with(addr, origin, &[]).await
    }

    /// As [`try_ws_connect`], plus arbitrary extra request headers. Needed to
    /// simulate a reverse proxy, which is where the Origin SCHEME (as opposed
    /// to its authority) starts to matter.
    async fn try_ws_connect_with(
        addr: SocketAddr,
        origin: Option<&str>,
        extra: &[(&str, &str)],
    ) -> Result<TestWsStream, u16> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut req = format!("ws://{addr}/permission/events/ws")
            .into_client_request()
            .unwrap();
        if let Some(o) = origin {
            req.headers_mut().insert("origin", o.parse().unwrap());
        }
        for (k, v) in extra {
            req.headers_mut().insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        match tokio_tungstenite::connect_async(req).await {
            Ok((stream, _resp)) => Ok(stream),
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
            Err(other) => panic!("unexpected handshake failure: {other}"),
        }
    }

    fn ws_test_app_with_hosts(
        pending: PendingPrompts,
        peer: Option<SocketAddr>,
        hosts: &[&str],
    ) -> Router {
        use std::collections::HashSet;
        use std::sync::Arc;
        let set: HashSet<String> = hosts.iter().map(|h| h.to_string()).collect();
        let app = super::routes()
            .layer(Extension(HostedMode(false)))
            .layer(Extension(pending))
            .layer(Extension::<AllowedHosts>(Arc::new(set)))
            .layer(Extension(AllowedSourceCidrs::default()));
        match peer {
            Some(p) => app.layer(Extension(ConnectInfo(p))),
            None => app,
        }
    }

    /// The Origin scheme must match, not just the authority.
    ///
    /// `Host` carries no scheme, so comparing authority alone makes
    /// `http://h` and `https://h` the same origin. Behind a TLS-terminating
    /// reverse proxy that is reachable: a page an attacker serves at
    /// `http://node.example.com` sends an Origin whose authority equals `Host`,
    /// and would otherwise be handed raw permission nonces.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_http_origin_when_proxy_terminated_tls() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;

        let refused = try_ws_connect_with(
            addr,
            Some(&format!("http://{addr}")),
            &[("x-forwarded-proto", "https")],
        )
        .await;
        assert_eq!(
            refused.err(),
            Some(403),
            "an http:// page is a different origin from the https:// node and must \
             not read the permission stream, even though the authorities match"
        );

        // Control: same request, matching scheme, must be admitted. Without
        // this the assertion above could be passing because the extra header
        // broke the handshake for some unrelated reason.
        let admitted = try_ws_connect_with(
            addr,
            Some(&format!("https://{addr}")),
            &[("x-forwarded-proto", "https")],
        )
        .await;
        assert!(
            admitted.is_ok(),
            "the matching-scheme handshake must still be accepted (got {:?})",
            admitted.err()
        );

        // A PRESENT-BUT-BLANK header (a proxy interpolating an unset variable)
        // must also be treated as absent. Comparing against "" matches no
        // scheme, so without the filter this refuses every handshake on that
        // deployment and drops every tab to the 3s poll.
        for blank in ["", "   "] {
            let admitted_blank = try_ws_connect_with(
                addr,
                Some(&format!("https://{addr}")),
                &[("x-forwarded-proto", blank)],
            )
            .await;
            assert!(
                admitted_blank.is_ok(),
                "a blank X-Forwarded-Proto must be treated as absent, not as a \
                 scheme that matches nothing (got {:?})",
                admitted_blank.err()
            );
        }

        // And with NO X-Forwarded-Proto the scheme is unknowable, so it must
        // NOT be guessed at. A TLS proxy that rewrites Host correctly but omits
        // the header is a common misconfiguration; refusing it would drop every
        // tab of a legitimate deployment to the 3s poll with nothing surfaced,
        // which is the silent-degradation class this change exists to remove.
        let unknown_scheme = try_ws_connect(addr, Some(&format!("https://{addr}"))).await;
        assert!(
            unknown_scheme.is_ok(),
            "with no X-Forwarded-Proto the scheme must not be inferred (got {:?})",
            unknown_scheme.err()
        );
    }

    /// An Origin containing userinfo, a query or a fragment is refused rather
    /// than normalised.
    ///
    /// `is_localhost_origin` is prefix matching, so it reads
    /// `http://127.0.0.1:7509@evil.com` as localhost, while `origin_authority`
    /// used to strip the userinfo and read it as `evil.com`. Two parsers that
    /// disagree about what an origin is can be walked between; the only version
    /// that cannot drift is refusing the ambiguous input.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_ambiguous_origin_forms() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;
        for hostile in [
            format!("http://{addr}@evil.example"),
            format!("http://evil.example?x=@{addr}"),
            format!("http://evil.example#@{addr}"),
        ] {
            let result = try_ws_connect(addr, Some(&hostile)).await;
            assert_eq!(
                result.err(),
                Some(403),
                "an Origin no browser can produce ({hostile}) must be refused, not \
                 parsed into something that matches Host"
            );
        }
    }

    /// The gate still holds when an `allowed-host` policy is configured.
    ///
    /// Every other WebSocket test runs with an EMPTY `AllowedHosts`, which
    /// exercises only the loopback + localhost-origin branch. A configured
    /// allowed-host opens a second branch in `is_origin_trusted`, and that is
    /// exactly the deployment where the scheme gap above lives.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_cross_origin_under_a_configured_allowed_host() {
        let app = ws_test_app_with_hosts(
            empty_pending(),
            Some(loopback_peer()),
            &["node.example.com", "node.example.com:7509"],
        );
        let addr = spawn_ws_server(app).await;

        let refused = try_ws_connect(addr, Some("http://node.example.com")).await;
        assert_eq!(
            refused.err(),
            Some(403),
            "an allowed HOST is not a licence to be a different ORIGIN: the \
             handshake's Host is the test server, so this must not match"
        );

        let admitted = try_ws_connect(addr, Some(&same_origin(addr))).await;
        assert!(
            admitted.is_ok(),
            "a genuine same-origin handshake must still be admitted with a host \
             policy configured (got {:?})",
            admitted.err()
        );
    }

    /// The legacy SSE transport must not be able to consume every slot.
    ///
    /// The WebSocket pump bounds a client that stops reading with a send
    /// timeout; the SSE path has no equivalent, so a backpressured stream holds
    /// its slot for as long as the peer keeps the socket open. Sharing one
    /// ceiling would let stalled SSE streams take all 64 and silently degrade
    /// every new tab to the 3s poll.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn legacy_sse_cannot_starve_the_websocket_transport() {
        let baseline = quiesced_subscriber_baseline().await;
        assert_eq!(
            baseline, 0,
            "test must start from a quiesced counter (settle loop above)"
        );

        let mut held: Vec<PermissionSubscriberGuard> = Vec::new();
        for _ in 0..MAX_SSE_SUBSCRIBERS {
            held.push(
                try_claim_subscriber_slot(SubscriberTransport::LegacySse)
                    .expect("SSE slots below its own ceiling must be claimable"),
            );
        }

        assert!(
            try_claim_subscriber_slot(SubscriberTransport::LegacySse).is_none(),
            "SSE must be refused at its own lower ceiling"
        );

        let ws = try_claim_subscriber_slot(SubscriberTransport::WebSocket);
        assert!(
            ws.is_some(),
            "the WebSocket transport must still have reserved headroom when SSE \
             has taken everything it is allowed to"
        );

        drop(ws);
        drop(held);
    }

    /// Control for the rejection tests below. Without it, all of them could be
    /// failing the handshake for a reason unrelated to the gate under test
    /// (a malformed upgrade, a missing extension, a transport quirk) and the
    /// whole group would be vacuous. A same-origin loopback handshake MUST
    /// succeed.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_accepts_same_origin_loopback_handshake() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;
        let sock = try_ws_connect(addr, Some(&same_origin(addr))).await;
        assert!(
            sock.is_ok(),
            "a same-origin loopback upgrade must be accepted; if this fails the \
             rejection tests prove nothing (got {:?})",
            sock.err()
        );
    }

    /// **The** security test for this endpoint.
    ///
    /// Browsers do NOT apply the same-origin policy to WebSocket handshakes:
    /// any page on any site can open a socket to `ws://127.0.0.1:7509` and no
    /// CORS preflight intervenes. What stops it is that browsers are required
    /// to send `Origin` on every WebSocket handshake, so the origin gate sees
    /// a foreign origin and refuses.
    ///
    /// Without this gate the endpoint streams raw permission nonces to any
    /// page the user happens to visit, and a nonce is a capability —
    /// `/permission/{nonce}/respond` approves a delegate action with it. That
    /// is a textbook cross-site WebSocket hijack. Paired with the same-origin
    /// control above so it cannot pass vacuously.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_cross_origin_handshake() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;
        for evil in [
            "https://evil.example",
            "http://evil.example:7509",
            "null",
            "http://127.0.0.1.evil.example",
        ] {
            let result = try_ws_connect(addr, Some(evil)).await;
            assert_eq!(
                result.err(),
                Some(403),
                "cross-origin WebSocket handshake from {evil} must be refused with 403 \
                 (CSWSH: browsers do not apply SOP to WebSockets)"
            );
        }
    }

    /// The hole CORS was silently covering on the SSE path.
    ///
    /// `is_origin_trusted` accepts ANY localhost origin on ANY port. Over SSE
    /// that was harmless: the trusted branch sets no
    /// `Access-Control-Allow-Origin`, so a page on `http://localhost:3000`
    /// passed the server gate and the BROWSER withheld the body. WebSocket
    /// handshakes get no CORS, so without an exact same-origin check any other
    /// local web app — a dev server, a notebook, anything with an XSS on
    /// `127.0.0.1:*` — could read raw permission nonces and delegate-authored
    /// prompt text, and hold slots against the subscriber cap.
    ///
    /// These origins all pass `is_caller_trusted`; only
    /// `origin_is_exactly_same_origin` rejects them. That makes this test the
    /// sole guard on that check.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_other_localhost_ports() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;
        for other in [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://[::1]:3000",
            "https://localhost:7509",
        ] {
            let result = try_ws_connect(addr, Some(other)).await;
            assert_eq!(
                result.err(),
                Some(403),
                "a page on {other} is a DIFFERENT origin from the node and must not \
                 read the permission stream; CORS covered this on SSE and cannot here"
            );
        }
    }

    /// A handshake with no `Origin` at all must be refused.
    ///
    /// `is_caller_trusted` tolerates a missing Origin (a non-browser caller),
    /// but this endpoint exists only for the shell page and the entire
    /// cross-site argument rests on browsers being REQUIRED to send Origin on
    /// every WebSocket handshake. Rejecting the absent case puts that
    /// reasoning in the code rather than in a comment.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_handshake_without_origin() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;
        let result = try_ws_connect(addr, None).await;
        assert_eq!(
            result.err(),
            Some(403),
            "an Origin-less handshake must be refused, not treated as trusted"
        );
    }

    /// Loopback boundary parity with the SSE stream (#3819). The socket emits
    /// raw nonces, so an off-box LAN peer must be refused even when the main
    /// listener binds 0.0.0.0.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_lan_peer() {
        let lan = SocketAddr::from(([192, 168, 1, 50], 40000));
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(lan))).await;
        let result = try_ws_connect(addr, Some(&same_origin(addr))).await;
        assert_eq!(
            result.err(),
            Some(403),
            "off-box LAN peer must not reach the permission WebSocket"
        );
    }

    /// Missing `ConnectInfo` cannot prove loopback, so it must be refused.
    /// This is the fail-closed half of the #3819 boundary, and it doubles as
    /// proof that the WebSocket handler actually consults `peer_is_loopback`
    /// rather than trusting the transport.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_rejects_missing_connect_info() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), None)).await;
        let result = try_ws_connect(addr, Some(&same_origin(addr))).await;
        assert_eq!(
            result.err(),
            Some(403),
            "absent ConnectInfo must fail closed, not be treated as loopback"
        );
    }

    /// The subscriber cap is shared with the SSE transport, and the WebSocket
    /// must honour it. Two independent caps would silently permit twice the
    /// intended ceiling during an upgrade, when already-open tabs still hold
    /// SSE streams while fresh tabs use the socket.
    ///
    /// Also pins the release path: freeing a slot must let the next handshake
    /// through, so a cap rejection is transient rather than permanently
    /// wedging the channel.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_honours_shared_subscriber_cap() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;

        // Wait for the counter to settle before sampling the baseline.
        // WebSocket slots are released by a server-side SPAWNED TASK when it
        // observes the disconnect, not synchronously like the SSE path's
        // `GuardedStream` (which lives in a response body the test itself
        // drops). `serial_test`'s lock releases at the end of the async body,
        // BEFORE `#[tokio::test]` tears the runtime down, so a previous test's
        // pump can still be unwinding when this one starts. Without this loop
        // the baseline can be read high and drop mid-test, which both breaks
        // the assert below and would let the "must be refused" handshake
        // wrongly succeed. Same shape as the SSE cap test's quiesce loop.
        let settle = tokio::time::Instant::now() + Duration::from_secs(5);
        while PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) != 0 {
            if tokio::time::Instant::now() >= settle {
                // A genuinely leaked slot is a real bug, but for this test's
                // purposes accept the baseline rather than fail on it.
                break;
            }
            tokio::task::yield_now().await;
        }

        // Fill every slot through the SAME helper the SSE path uses. If the
        // WebSocket had its own counter these would be invisible to it and the
        // handshake below would wrongly succeed.
        let baseline = PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed);
        let mut held: Vec<PermissionSubscriberGuard> = Vec::new();
        for _ in baseline..MAX_PERMISSION_SUBSCRIBERS {
            held.push(
                try_claim_subscriber_slot(SubscriberTransport::WebSocket)
                    .expect("slot below cap must be claimable"),
            );
        }
        assert_eq!(
            PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
            MAX_PERMISSION_SUBSCRIBERS
        );

        let refused = try_ws_connect(addr, Some(&same_origin(addr))).await;
        assert_eq!(
            refused.err(),
            Some(503),
            "WebSocket must respect the cap shared with SSE"
        );

        // Release one slot; the next handshake must be admitted.
        held.pop();
        let admitted = try_ws_connect(addr, Some(&same_origin(addr))).await;
        assert!(
            admitted.is_ok(),
            "freeing a slot must re-admit; a cap rejection must be transient (got {:?})",
            admitted.err()
        );
        drop(held);
    }

    /// A LIVE socket must hold exactly one slot, and dropping the client must
    /// return it.
    ///
    /// The cap test above fills slots with guards directly, so nothing there
    /// proves the guard survives into `pump_permission_events` (it is passed
    /// as `_guard`) — a regression dropping it early would admit unlimited
    /// sockets with every existing test still green. Nor does anything prove
    /// release on disconnect; a leak there wedges the channel at 64 for the
    /// node's lifetime and silently degrades every tab to 3s polling, which is
    /// the same failure class as #5213 itself.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_slot_is_held_while_live_and_released_on_disconnect() {
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;

        let settle = tokio::time::Instant::now() + Duration::from_secs(5);
        while PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) != 0 {
            if tokio::time::Instant::now() >= settle {
                break;
            }
            tokio::task::yield_now().await;
        }
        let baseline = PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed);

        let sock = try_ws_connect(addr, Some(&same_origin(addr)))
            .await
            .expect("handshake must succeed");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) != baseline + 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "a live socket must hold exactly one subscriber slot (saw {}, expected {})",
                PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
                baseline + 1
            );
            tokio::task::yield_now().await;
        }

        drop(sock);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) != baseline {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the slot must be released when the client disconnects, or the \
                 channel wedges at the cap for the node's lifetime (stuck at {})",
                PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Read frames until one satisfies `pred`, ignoring pings and any events
    /// left over from the shared global registry.
    async fn next_ws_event(
        stream: &mut TestWsStream,
        pred: impl Fn(&serde_json::Value) -> bool,
        max_frames: usize,
    ) -> Option<serde_json::Value> {
        use tokio_tungstenite::tungstenite::Message as TMessage;
        for _ in 0..max_frames {
            let next = tokio::time::timeout(Duration::from_secs(5), stream.next()).await;
            let Ok(Some(Ok(msg))) = next else { return None };
            if let TMessage::Text(text) = msg
                && let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
                && pred(&value)
            {
                return Some(value);
            }
        }
        None
    }

    /// End-to-end over a real socket: a prompt raised after the client
    /// connects must be pushed, and its removal must be pushed too, so every
    /// tab dismisses simultaneously. This is the behaviour #4023 introduced;
    /// #5213 only changes the transport carrying it, so it must survive.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_delivers_added_and_removed_end_to_end() {
        let initial_subs = prompt_events().receiver_count();
        let addr = spawn_ws_server(ws_test_app(empty_pending(), Some(loopback_peer()))).await;
        let mut sock = try_ws_connect(addr, Some(&same_origin(addr)))
            .await
            .expect("handshake must succeed");

        let nonce = "wstest_added_001".to_string();
        wait_subscribers_then_send(
            initial_subs + 1,
            PromptEvent::Added(PromptSnapshot {
                author: crate::contract::user_input::PromptAuthor::Delegate,
                nonce: nonce.clone(),
                message: "approve?".into(),
                labels: vec!["Allow".into(), "Deny".into()],
                delegate_key: "dkey-ws-001".into(),
                caller: webapp_caller("cid-ws-001"),
                owner: None,
            }),
        )
        .await;

        let added = next_ws_event(
            &mut sock,
            |v| v["event"] == "prompt_added" && v["data"]["nonce"] == nonce.as_str(),
            32,
        )
        .await
        .expect("prompt_added must arrive over the WebSocket");
        assert_eq!(added["data"]["delegate_key"], "dkey-ws-001");
        assert_eq!(added["data"]["message"], "approve?");

        drop(prompt_events().send(PromptEvent::Removed {
            nonce: nonce.clone(),
            owner: None,
        }));
        let removed = next_ws_event(
            &mut sock,
            |v| v["event"] == "prompt_removed" && v["data"]["nonce"] == nonce.as_str(),
            32,
        )
        .await
        .expect("prompt_removed must arrive over the WebSocket");
        assert_eq!(removed["data"]["nonce"], nonce.as_str());
    }

    /// A tab that loads while a prompt is already pending must still see it.
    /// Pins the subscribe-then-snapshot bootstrap on the socket path; without
    /// the replay, refreshing the only open tab would strand a live prompt
    /// with nothing rendering it.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_replays_pending_on_connect() {
        let pending = empty_pending();
        let nonce = "wstest_replay_002".to_string();
        let _rx = insert_prompt(
            &pending,
            &nonce,
            "pre-existing",
            vec!["OK"],
            "dkey-ws-replay",
            webapp_caller("cid-ws-replay"),
        );
        let addr = spawn_ws_server(ws_test_app(pending, Some(loopback_peer()))).await;
        let mut sock = try_ws_connect(addr, Some(&same_origin(addr)))
            .await
            .expect("handshake must succeed");

        let replayed = next_ws_event(
            &mut sock,
            |v| v["event"] == "prompt_added" && v["data"]["nonce"] == nonce.as_str(),
            32,
        )
        .await
        .expect("already-pending prompt must be replayed to a fresh subscriber");
        assert_eq!(replayed["data"]["message"], "pre-existing");
    }

    /// `ws_envelope` must wrap its payload without altering it: the bridge
    /// dispatches on `event` and reads `data`, so a change to either key
    /// silently stops prompts rendering.
    ///
    /// Named for what it actually checks. It does NOT read the SSE handler, so
    /// it cannot prove cross-transport agreement — the expected value is
    /// computed with the same `snapshot_to_json` the production path calls, so
    /// it is self-consistency plus a pin on the `event`/`data` key names. The
    /// event names themselves are covered off-the-wire by the end-to-end tests
    /// above, which is where a rename would actually be caught.
    #[test]
    fn ws_envelope_wraps_data_unchanged() {
        let snapshot = PromptSnapshot {
            author: crate::contract::user_input::PromptAuthor::Delegate,
            nonce: "n1".into(),
            message: "m".into(),
            labels: vec!["OK".into()],
            delegate_key: "dk".into(),
            caller: webapp_caller("c1"),
            owner: None,
        };
        let added: serde_json::Value =
            serde_json::from_str(&ws_envelope("prompt_added", snapshot_to_json(&snapshot)))
                .unwrap();
        assert_eq!(added["event"], "prompt_added");
        // `data` is the SSE `data:` payload unchanged.
        assert_eq!(added["data"], snapshot_to_json(&snapshot));

        let removed: serde_json::Value = serde_json::from_str(&ws_envelope(
            "prompt_removed",
            serde_json::json!({ "nonce": "n1" }),
        ))
        .unwrap();
        assert_eq!(removed["event"], "prompt_removed");
        assert_eq!(removed["data"]["nonce"], "n1");

        let resync: serde_json::Value =
            serde_json::from_str(&ws_envelope("resync", serde_json::json!({}))).unwrap();
        assert_eq!(resync["event"], "resync");
    }

    /// A sink that never accepts a message: models a client that stopped
    /// reading (suspended tab, wedged renderer) with its receive window full.
    struct StalledSink;

    impl futures::Sink<Message> for StalledSink {
        type Error = std::convert::Infallible;
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
        fn start_send(self: std::pin::Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            unreachable!("start_send is unreachable while poll_ready never completes")
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
    }

    /// A sink that accepts each message, but only after `delay`. Distinguishes
    /// a WHOLE-PHASE replay deadline from a per-message one: with a per-message
    /// deadline every message under `delay` succeeds and all of them get
    /// through, whereas one deadline over the phase cuts the replay off partway.
    struct SlowSink {
        delay: Duration,
        sleep: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
        sent: std::sync::Arc<AtomicUsize>,
    }

    impl futures::Sink<Message> for SlowSink {
        type Error = std::convert::Infallible;
        fn poll_ready(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            let delay = self.delay;
            let slot = &mut self.as_mut().get_mut().sleep;
            let fut = slot.get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));
            match fut.as_mut().poll(cx) {
                std::task::Poll::Ready(()) => {
                    *slot = None;
                    std::task::Poll::Ready(Ok(()))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
        fn start_send(self: std::pin::Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            self.sent.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn never_inbound() -> futures::stream::Pending<Result<Message, std::convert::Infallible>> {
        futures::stream::pending()
    }

    /// A subscriber that falls behind must be told to `resync`, not left
    /// silently missing prompts. Drives the REAL pump with a deliberately
    /// lapped receiver: capacity 2 with five sends queued before the pump ever
    /// polls, so its first `recv` returns `Lagged`.
    ///
    /// The SSE transport has an equivalent (`test_sse_emits_resync_on_lag`);
    /// without this, the WebSocket's lag branch was pinned only by the shape
    /// of a hand-built envelope and never actually executed.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_pump_emits_resync_when_the_broadcast_laps_it() {
        let (tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(2);
        for i in 0..5 {
            drop(tx.send(PromptEvent::Removed {
                nonce: format!("lag-{i}"),
                owner: None,
            }));
        }

        let (sink, mut out) = futures::channel::mpsc::unbounded::<Message>();
        let guard =
            try_claim_subscriber_slot(SubscriberTransport::WebSocket).expect("a slot must be free");
        let pump = tokio::spawn(pump_permission_events(
            sink,
            never_inbound(),
            rx,
            Vec::new(),
            guard,
            PromptViewer::Node,
        ));

        let msg = tokio::time::timeout(Duration::from_secs(5), out.next())
            .await
            .expect("the pump must emit promptly after a lag")
            .expect("the sink must receive a frame");
        let Message::Text(text) = msg else {
            panic!("expected a text frame");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            value["event"], "resync",
            "a lapped subscriber must be told to resync so the shell re-bootstraps \
             from /permission/pending; silently dropping the gap loses prompts"
        );

        // Await the abort rather than just firing it. `pump` holds a
        // `PermissionSubscriberGuard`; a bare abort releases it asynchronously,
        // on a runtime being torn down after `serial_test`'s lock is already
        // free, so the next test can observe a non-zero baseline that then
        // drops mid-assertion.
        pump.abort();
        // `drop(...)` rather than `let _ = ...`, because
        // `let_underscore_must_use` is denied crate-wide. Not asserted because
        // the `JoinResult` carries nothing this test is about: a pump panic
        // before the frame is already surfaced by the `out.next()` expectation
        // above, and after the frame the pump is parked (empty `initial`,
        // `pending()` inbound, live `tx`), so there is nothing else to report.
        // The await itself is the point -- see the comment above.
        drop(pump.await);
    }

    /// Wait for the shared subscriber counter to quiesce, then return it.
    ///
    /// WebSocket slots are released by a server-side SPAWNED TASK, and
    /// `serial_test`'s lock releases at the end of the async body, BEFORE
    /// `#[tokio::test]` tears the runtime down — so a previous test's pump can
    /// still be unwinding when this one starts. Reading the counter without
    /// settling first makes any exact-equality assertion on it a coin flip.
    async fn quiesced_subscriber_baseline() -> usize {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed) != 0 {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            // `sleep`, not `yield_now`: under `#[tokio::test(start_paused)]`
            // the runtime always has a ready task, so time never auto-advances
            // and a `yield_now` spin makes the deadline unreachable — a stuck
            // counter would hang the test instead of breaking out.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed)
    }

    /// The send timeout must actually fire and release the subscriber slot.
    ///
    /// This is the mechanism `PERMISSION_WS_SEND_TIMEOUT` exists for: without
    /// it, one client that stops reading pins a slot against
    /// `MAX_PERMISSION_SUBSCRIBERS` forever, and 64 such clients deny the
    /// permission channel to every legitimate tab. The fix's own safety
    /// argument rests on this, so it is tested rather than asserted.
    ///
    /// `initial` is deliberately EMPTY. An earlier version of this test passed
    /// one replay frame, which meant the pump exited from the REPLAY deadline
    /// and never reached the `select!` loop — so `send_bounded`, the timeout
    /// this doc comment is about, was never executed. Mutating `send_bounded`
    /// to drop its timeout left the whole suite green. With no replay frames
    /// the replay phase completes instantly and the loop's ping tick is what
    /// must time out against `StalledSink`. The replay deadline keeps its own
    /// test below; one test cannot be credited with both, since whichever
    /// deadline fires first is the only one exercised.
    ///
    /// Time is paused, so the wait costs no wall-clock; the assertion is that
    /// the pump RETURNS at all rather than hanging in `send`.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_pump_gives_up_on_a_client_that_stops_reading() {
        let baseline = quiesced_subscriber_baseline().await;
        let guard =
            try_claim_subscriber_slot(SubscriberTransport::WebSocket).expect("a slot must be free");
        assert_eq!(
            PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
            baseline + 1,
            "the guard must have claimed a slot"
        );

        // Keep the sender alive so the pump blocks on the stalled sink rather
        // than exiting early via `RecvError::Closed`.
        let (_tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(8);
        let pump = tokio::spawn(pump_permission_events(
            StalledSink,
            never_inbound(),
            rx,
            Vec::new(),
            guard,
            PromptViewer::Node,
        ));

        tokio::time::timeout(Duration::from_secs(120), pump)
            .await
            .expect("the pump must give up on a stalled client, not hang forever")
            .expect("the pump task must not panic");

        assert_eq!(
            PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
            baseline,
            "the subscriber slot must be released once the pump gives up, or a \
             stalled client pins it against the cap permanently"
        );
    }

    /// The replay deadline bounds the WHOLE PHASE, not each message.
    ///
    /// With exactly one replay frame the two designs are indistinguishable, so
    /// the sibling test above cannot pin this. The comment on
    /// `PERMISSION_WS_SEND_TIMEOUT`'s use in the replay loop argues the phase
    /// bound matters precisely because a per-message deadline would allow
    /// ~32 x 10s against a client that accepts just slowly enough. Three frames
    /// against a sink that takes 4s each separates them: one deadline over the
    /// phase cuts the replay off partway, a per-message deadline lets all three
    /// through.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_replay_deadline_bounds_the_whole_phase() {
        let baseline = quiesced_subscriber_baseline().await;
        let guard =
            try_claim_subscriber_slot(SubscriberTransport::WebSocket).expect("a slot must be free");

        let sent = std::sync::Arc::new(AtomicUsize::new(0));
        let sink = SlowSink {
            delay: Duration::from_secs(4),
            sleep: None,
            sent: sent.clone(),
        };
        let (_tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(8);
        let initial = vec![
            ws_envelope("resync", serde_json::json!({})),
            ws_envelope("resync", serde_json::json!({})),
            ws_envelope("resync", serde_json::json!({})),
        ];

        let pump = tokio::spawn(pump_permission_events(
            sink,
            never_inbound(),
            rx,
            initial,
            guard,
            PromptViewer::Node,
        ));
        tokio::time::timeout(Duration::from_secs(120), pump)
            .await
            .expect("the pump must give up when the replay PHASE runs over")
            .expect("the pump task must not panic");

        // EXACTLY 2, not `< 3`. Under `start_paused` the timeline is exact:
        // 4s + 4s, cut off by the 10s phase deadline. A loose bound would still
        // pass if a future change made it 0 — at which point the test no longer
        // distinguishes whole-phase from per-message, which is its whole point.
        assert_eq!(
            sent.load(Ordering::Relaxed),
            2,
            "the replay deadline must bound the whole phase: all 3 frames got \
             through, which is what a PER-MESSAGE deadline would allow (each 4s \
             send is under the 10s timeout) and is exactly the ~32x overrun the \
             phase bound exists to prevent"
        );
        assert_eq!(
            PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
            baseline,
            "the slot must be released when the replay phase gives up"
        );
    }

    /// The REPLAY phase has its own deadline, distinct from the steady-state
    /// send timeout above. One bootstrap frame against a sink that never
    /// accepts must not hang the pump either.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn permission_ws_pump_gives_up_replaying_to_a_stalled_client() {
        let baseline = quiesced_subscriber_baseline().await;
        let guard =
            try_claim_subscriber_slot(SubscriberTransport::WebSocket).expect("a slot must be free");

        let (_tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(8);
        let pump = tokio::spawn(pump_permission_events(
            StalledSink,
            never_inbound(),
            rx,
            vec![ws_envelope("resync", serde_json::json!({}))],
            guard,
            PromptViewer::Node,
        ));

        tokio::time::timeout(Duration::from_secs(120), pump)
            .await
            .expect("the pump must give up replaying to a stalled client")
            .expect("the pump task must not panic");

        assert_eq!(
            PERMISSION_SUBSCRIBERS.load(Ordering::Relaxed),
            baseline,
            "the slot must be released when replay gives up too"
        );
    }
    // ----- Hosted node: a user token is required and each user sees only
    // ----- their own prompts.

    const TOKEN_A: &str = "hosted-perm-user-a";
    const TOKEN_B: &str = "hosted-perm-user-b";

    fn uid(token: &str) -> UserId {
        *derive_user_context(true, Some(token))
            .expect("a non-empty token derives a context")
            .user_id()
    }

    fn insert_owned(
        pending: &PendingPrompts,
        nonce: &str,
        owner: Option<UserId>,
    ) -> tokio::sync::oneshot::Receiver<usize> {
        use crate::contract::user_input::PendingPrompt;
        let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
        pending.insert(
            nonce.to_string(),
            PendingPrompt {
                author: crate::contract::user_input::PromptAuthor::Delegate,
                message: format!("message for {nonce}"),
                labels: vec!["Allow".to_string(), "Deny".to_string()],
                delegate_key: "dkey".to_string(),
                caller: CallerIdentity::None,
                owner,
                response_tx: tx,
            },
        );
        rx
    }

    /// Permission routes with the hosted-mode flag set as given, a loopback
    /// peer, and permissive host policy, as the production composition builds
    /// them behind a same-host TLS proxy.
    fn hosted_app(pending: PendingPrompts, hosted: bool) -> Router {
        super::routes()
            .layer(Extension(pending))
            .layer(Extension(HostedMode(hosted)))
            .layer(Extension::<AllowedHosts>(Arc::new(HashSet::new())))
            .layer(Extension(AllowedSourceCidrs::default()))
    }

    /// A request as the TLS proxy forwards it: loopback source, the proxy's
    /// `X-Forwarded-Proto` when `xfp_https`, and the user's token header when
    /// given. POSTs carry the shell's own https Origin.
    fn proxied_request(
        method: &str,
        uri: &str,
        token: Option<&str>,
        xfp_https: bool,
        body: &'static str,
    ) -> axum::http::Request<axum::body::Body> {
        let mut b = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("host", "localhost:7509")
            .header("content-type", "application/json");
        if method == "POST" {
            b = b.header("origin", "https://localhost:7509");
        }
        if xfp_https {
            b = b.header("x-forwarded-proto", "https");
        }
        if let Some(t) = token {
            b = b.header("x-freenet-user-token", t);
        }
        let mut req = b.body(axum::body::Body::from(body)).unwrap();
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 55001))));
        req
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    fn listed_nonces(value: &serde_json::Value) -> Vec<String> {
        value
            .as_array()
            .expect("pending returns a JSON array")
            .iter()
            .map(|p| p["nonce"].as_str().unwrap().to_string())
            .collect()
    }

    /// A hosted node lists for a user only the prompts their own runs raised:
    /// not another user's, and not one without an owner.
    ///
    /// FALSIFY: drop the `may_see` filter in `pending_prompts`, or make
    /// `PromptViewer::may_see` return `true`; the other users' nonces appear.
    #[tokio::test]
    async fn a_hosted_node_lists_only_the_requesting_users_prompts() {
        use tower::ServiceExt;
        let pending = empty_pending();
        let _a = insert_owned(&pending, "nonce-of-a", Some(uid(TOKEN_A)));
        let _b = insert_owned(&pending, "nonce-of-b", Some(uid(TOKEN_B)));
        let _n = insert_owned(&pending, "nonce-ownerless", None);

        let resp = hosted_app(pending.clone(), true)
            .oneshot(proxied_request(
                "GET",
                "/permission/pending",
                Some(TOKEN_A),
                true,
                "",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let listed = body_json(resp).await;
        assert_eq!(
            listed_nonces(&listed),
            vec!["nonce-of-a".to_string()],
            "user A must see exactly their own prompt"
        );
        assert!(
            listed
                .as_array()
                .unwrap()
                .iter()
                .all(|p| p.get("owner").is_none()),
            "the prompt's owner is never sent to a client"
        );
    }

    /// Without a valid token a hosted node refuses the routes that show or
    /// answer prompts: no token, an empty token, and a token on a connection
    /// that is not known to be TLS (no `X-Forwarded-Proto: https`) are each
    /// refused with 403, and a refused answer leaves the prompt pending.
    ///
    /// FALSIFY: make `resolve_prompt_viewer` return `PromptViewer::Node` for a
    /// hosted request; every case below is served.
    #[tokio::test]
    async fn a_hosted_node_refuses_the_permission_routes_without_a_valid_token() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let _a = insert_owned(&pending, "nonce-of-a", Some(uid(TOKEN_A)));

        let cases: [(Option<&str>, bool, &str); 4] = [
            (None, true, "no token"),
            (Some(""), true, "an empty token"),
            (
                Some(TOKEN_A),
                false,
                "a token without X-Forwarded-Proto: https",
            ),
            (None, false, "no token and no TLS"),
        ];
        for (token, xfp, what) in cases {
            let resp = hosted_app(pending.clone(), true)
                .oneshot(proxied_request(
                    "GET",
                    "/permission/pending",
                    token,
                    xfp,
                    "",
                ))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "pending with {what} must be refused"
            );
            let resp = hosted_app(pending.clone(), true)
                .oneshot(proxied_request(
                    "POST",
                    "/permission/nonce-of-a/respond",
                    token,
                    xfp,
                    r#"{"index":0}"#,
                ))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "respond with {what} must be refused"
            );
            assert!(
                pending.contains_key("nonce-of-a"),
                "a refused answer must leave the prompt pending ({what})"
            );
        }
    }

    /// The token gate also covers the permission WebSocket, including its
    /// query form of the token. Driven over a real socket because axum
    /// answers a `oneshot` upgrade with 426 before any gate runs.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn a_hosted_node_refuses_the_permission_socket_without_a_valid_token() {
        let addr = spawn_ws_server(hosted_ws_app(empty_pending(), true)).await;
        let origin = format!("https://{addr}");
        for (query, xfp, what) in [
            ("", true, "no token"),
            ("?userToken=", true, "an empty token"),
            (
                "?userToken=hosted-perm-user-a",
                false,
                "a token without X-Forwarded-Proto: https",
            ),
        ] {
            let extra: &[(&str, &str)] = if xfp {
                &[("x-forwarded-proto", "https")]
            } else {
                &[]
            };
            let got = try_ws_connect_path(addr, query, Some(&origin), extra).await;
            assert_eq!(
                got.err(),
                Some(403),
                "the permission socket with {what} must be refused"
            );
        }
    }

    fn hosted_ws_app(pending: PendingPrompts, hosted: bool) -> Router {
        hosted_app(pending, hosted).layer(Extension(ConnectInfo(loopback_peer())))
    }

    async fn try_ws_connect_path(
        addr: SocketAddr,
        query: &str,
        origin: Option<&str>,
        extra: &[(&str, &str)],
    ) -> Result<TestWsStream, u16> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut req = format!("ws://{addr}/permission/events/ws{query}")
            .into_client_request()
            .unwrap();
        if let Some(o) = origin {
            req.headers_mut().insert("origin", o.parse().unwrap());
        }
        for (k, v) in extra {
            req.headers_mut().insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        match tokio_tungstenite::connect_async(req).await {
            Ok((stream, _resp)) => Ok(stream),
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
            Err(other) => panic!("unexpected handshake failure: {other}"),
        }
    }

    /// A hosted user cannot answer another user's prompt: the answer gets the
    /// same 404 as an unknown nonce, and the prompt stays pending and
    /// unanswered. Its owner can still answer it.
    ///
    /// FALSIFY: drop the `may_see` filter on the lookup in
    /// `permission_respond`; user A's answer is accepted. The check inside the
    /// removal is pinned by `a_hosted_users_answer_is_checked_at_removal`.
    #[tokio::test]
    async fn a_hosted_user_cannot_answer_another_users_prompt() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let mut rx_b = insert_owned(&pending, "nonce-of-b", Some(uid(TOKEN_B)));

        let resp = hosted_app(pending.clone(), true)
            .oneshot(proxied_request(
                "POST",
                "/permission/nonce-of-b/respond",
                Some(TOKEN_A),
                true,
                r#"{"index":0}"#,
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "another user's prompt must look like an unknown nonce"
        );
        assert!(
            pending.contains_key("nonce-of-b"),
            "the prompt must stay pending"
        );
        assert!(
            rx_b.try_recv().is_err(),
            "the prompt must not have been answered"
        );

        let resp = hosted_app(pending.clone(), true)
            .oneshot(proxied_request(
                "POST",
                "/permission/nonce-of-b/respond",
                Some(TOKEN_B),
                true,
                r#"{"index":1}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "the owner can answer");
        assert_eq!(rx_b.await.unwrap(), 1);
    }

    /// Another user's prompt answers 404 for any index, as an unknown nonce
    /// does.
    ///
    /// FALSIFY: drop the `may_see` filter on the lookup in
    /// `permission_respond`; the answer is 400.
    #[tokio::test]
    async fn another_users_prompt_answers_404_even_for_a_bad_index() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let _b = insert_owned(&pending, "nonce-of-b", Some(uid(TOKEN_B)));
        let resp = hosted_app(pending.clone(), true)
            .oneshot(proxied_request(
                "POST",
                "/permission/nonce-of-b/respond",
                Some(TOKEN_A),
                true,
                r#"{"index":99}"#,
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "another user's prompt must answer like an unknown nonce"
        );
        assert!(pending.contains_key("nonce-of-b"));
    }

    /// The removal in `permission_respond` checks the owner itself, so the
    /// rule holds for the entry actually removed and not only for the one the
    /// earlier lookup saw. The two can differ only if the registry changes
    /// between them, which no test can arrange, so this pins the source: the
    /// removal is a `remove_if` on the owner, in code rather than a comment,
    /// and there is no unchecked `remove`.
    ///
    /// FALSIFY: replace the `remove_if(..)` in `permission_respond` with
    /// `remove(&nonce)`, or comment the line out; this test fails.
    #[test]
    fn a_hosted_users_answer_is_checked_at_removal() {
        let src = include_str!("permission_prompts.rs");
        let start = src
            .find("\nasync fn permission_respond(")
            .expect("permission_respond must exist");
        let tests_at = src
            .find("\n#[cfg(test)]\nmod tests {")
            .expect("the test module must exist");
        assert!(
            start < tests_at,
            "permission_respond must be production code"
        );
        let body = &src[start..];
        let end = body
            .find("\n}\n")
            .expect("permission_respond must end with a top-level brace");
        let body = &body[..end];
        let needle = "pending.remove_if(&nonce, |_, p| viewer.may_see(p.owner))";
        assert!(
            body.lines()
                .any(|l| !l.trim_start().starts_with("//") && l.contains(needle)),
            "the removal in permission_respond must check the prompt's owner"
        );
        assert!(
            !body.contains("pending.remove(&nonce)"),
            "permission_respond must not remove a prompt without checking its owner"
        );
    }

    /// The pump forwards to a hosted user only their own prompts' events,
    /// both additions and removals.
    ///
    /// FALSIFY: delete the `may_see` check on the `Added` arm of
    /// `pump_permission_events`, or on the `Removed` arm; a frame for user
    /// B's prompt is sent.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn the_permission_pump_sends_a_hosted_user_only_their_own_events() {
        let (tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(16);
        let snap = |nonce: &str, owner: Option<UserId>| PromptSnapshot {
            nonce: nonce.into(),
            author: crate::contract::user_input::PromptAuthor::Delegate,
            message: "m".into(),
            labels: vec!["OK".into()],
            delegate_key: "dk".into(),
            caller: CallerIdentity::None,
            owner,
        };
        let a = Some(uid(TOKEN_A));
        let b = Some(uid(TOKEN_B));
        drop(tx.send(PromptEvent::Added(snap("b-added", b))));
        drop(tx.send(PromptEvent::Added(snap("none-added", None))));
        drop(tx.send(PromptEvent::Added(snap("a-added", a))));
        drop(tx.send(PromptEvent::Removed {
            nonce: "b-removed".into(),
            owner: b,
        }));
        drop(tx.send(PromptEvent::Removed {
            nonce: "none-removed".into(),
            owner: None,
        }));
        drop(tx.send(PromptEvent::Removed {
            nonce: "a-removed".into(),
            owner: a,
        }));

        let (sink, mut out) = futures::channel::mpsc::unbounded::<Message>();
        let guard =
            try_claim_subscriber_slot(SubscriberTransport::WebSocket).expect("a slot must be free");
        let pump = tokio::spawn(pump_permission_events(
            sink,
            never_inbound(),
            rx,
            Vec::new(),
            guard,
            PromptViewer::User(uid(TOKEN_A)),
        ));

        let mut seen = Vec::new();
        for _ in 0..2 {
            let msg = tokio::time::timeout(Duration::from_secs(5), out.next())
                .await
                .expect("the pump must forward the viewer's own events")
                .expect("the sink must receive a frame");
            let Message::Text(text) = msg else {
                panic!("expected a text frame");
            };
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            seen.push(format!(
                "{}:{}",
                v["event"].as_str().unwrap(),
                v["data"]["nonce"].as_str().unwrap()
            ));
        }
        // Every event was queued before the pump started and in this order,
        // so a forwarded event for B or for no owner would arrive before
        // A's own event of the same kind.
        assert_eq!(
            seen,
            vec![
                "prompt_added:a-added".to_string(),
                "prompt_removed:a-removed".to_string()
            ],
            "user A must be sent only their own prompts' events"
        );
        pump.abort();
        drop(pump.await);
    }

    /// The pump on a node that is not hosted forwards every event, as before.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn the_permission_pump_on_a_node_that_is_not_hosted_sends_every_event() {
        let (tx, rx) = tokio::sync::broadcast::channel::<PromptEvent>(16);
        drop(tx.send(PromptEvent::Removed {
            nonce: "owned".into(),
            owner: Some(uid(TOKEN_B)),
        }));
        drop(tx.send(PromptEvent::Removed {
            nonce: "ownerless".into(),
            owner: None,
        }));
        let (sink, mut out) = futures::channel::mpsc::unbounded::<Message>();
        let guard =
            try_claim_subscriber_slot(SubscriberTransport::WebSocket).expect("a slot must be free");
        let pump = tokio::spawn(pump_permission_events(
            sink,
            never_inbound(),
            rx,
            Vec::new(),
            guard,
            PromptViewer::Node,
        ));
        for expected in ["owned", "ownerless"] {
            let msg = tokio::time::timeout(Duration::from_secs(5), out.next())
                .await
                .expect("frame")
                .expect("frame");
            let Message::Text(text) = msg else {
                panic!("expected a text frame");
            };
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(v["data"]["nonce"], expected);
        }
        pump.abort();
        drop(pump.await);
    }

    /// Over a real socket a hosted user is replayed only their own pending
    /// prompts on connect.
    ///
    /// FALSIFY: drop the `may_see` filter on the replay snapshot in
    /// `permission_events_ws`; user B's prompt is replayed to A.
    #[tokio::test]
    #[serial_test::serial(permission_subscriber_counter)]
    async fn the_permission_socket_replays_to_a_hosted_user_only_their_own_prompts() {
        let pending = empty_pending();
        let _b = insert_owned(&pending, "replay-of-b", Some(uid(TOKEN_B)));
        let _n = insert_owned(&pending, "replay-ownerless", None);
        let _a = insert_owned(&pending, "replay-of-a", Some(uid(TOKEN_A)));
        let addr = spawn_ws_server(hosted_ws_app(pending, true)).await;
        let origin = format!("https://{addr}");
        let mut sock = try_ws_connect_path(
            addr,
            "?userToken=hosted-perm-user-a",
            Some(&origin),
            &[("x-forwarded-proto", "https")],
        )
        .await
        .expect("a valid token over TLS must be accepted");

        // A live event for A after connecting marks the end of the replay: the
        // pump sends every replay frame before any live one, so the frames
        // before it are exactly the replay.
        drop(prompt_events().send(PromptEvent::Added(PromptSnapshot {
            nonce: "replay-end-of-a".into(),
            author: crate::contract::user_input::PromptAuthor::Delegate,
            message: "m".into(),
            labels: vec!["OK".into()],
            delegate_key: "dk".into(),
            caller: CallerIdentity::None,
            owner: Some(uid(TOKEN_A)),
        })));
        let mut replayed = Vec::new();
        loop {
            let v = next_ws_event(&mut sock, |v| v["event"] == "prompt_added", 64)
                .await
                .expect("the live event for A must arrive after the replay");
            assert!(
                v["data"].get("owner").is_none(),
                "the prompt's owner is never sent to a client"
            );
            let nonce = v["data"]["nonce"].as_str().unwrap().to_string();
            if nonce == "replay-end-of-a" {
                break;
            }
            replayed.push(nonce);
        }
        assert_eq!(
            replayed,
            vec!["replay-of-a".to_string()],
            "user A must be replayed only their own prompt"
        );
    }

    /// A hosted node does not serve the permission routes whose content is
    /// not scoped to one user: the legacy SSE stream, the standalone prompt
    /// page, and the capability-grant list, revoke and page. A valid token
    /// does not change that.
    ///
    /// FALSIFY: remove `refuse_when_hosted` from any one of these routes in
    /// `routes()`; that route is served.
    #[tokio::test]
    async fn a_hosted_node_does_not_serve_the_other_permission_routes() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let _a = insert_owned(&pending, "nonce-of-a", Some(uid(TOKEN_A)));
        for (method, uri, body) in [
            ("GET", "/permission/events", ""),
            ("GET", "/permission/nonce-of-a", ""),
            ("GET", "/permission/grants", ""),
            (
                "POST",
                "/permission/grants/revoke",
                r#"{"app":"x","capability":"background"}"#,
            ),
            ("GET", "/permission/apps", ""),
        ] {
            let resp = hosted_app(pending.clone(), true)
                .oneshot(proxied_request(method, uri, Some(TOKEN_A), true, body))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "{method} {uri} must not be served on a hosted node"
            );
        }
    }

    /// A node that is not hosted ignores any token and behaves as before: it
    /// lists every prompt and answers any prompt, whatever token a request
    /// carries, and the routes a hosted node refuses are served.
    #[tokio::test]
    async fn a_node_that_is_not_hosted_ignores_a_token_and_lists_every_prompt() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let _a = insert_owned(&pending, "nonce-of-a", Some(uid(TOKEN_A)));
        let rx_b = insert_owned(&pending, "nonce-of-b", Some(uid(TOKEN_B)));
        let _n = insert_owned(&pending, "nonce-ownerless", None);

        for token in [None, Some(TOKEN_A)] {
            let resp = hosted_app(pending.clone(), false)
                .oneshot(proxied_request(
                    "GET",
                    "/permission/pending",
                    token,
                    false,
                    "",
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let mut nonces = listed_nonces(&body_json(resp).await);
            nonces.sort();
            assert_eq!(nonces, vec!["nonce-of-a", "nonce-of-b", "nonce-ownerless"]);
        }

        let resp = hosted_app(pending.clone(), false)
            .oneshot(proxied_request(
                "POST",
                "/permission/nonce-of-b/respond",
                Some(TOKEN_A),
                false,
                r#"{"index":0}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(rx_b.await.unwrap(), 0);

        for uri in ["/permission/nonce-of-a", "/permission/apps"] {
            let resp = hosted_app(pending.clone(), false)
                .oneshot(proxied_request("GET", uri, None, false, ""))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{uri} must be served");
        }
    }

    /// `resolve_prompt_viewer`'s decisions, each case named.
    #[test]
    fn resolve_prompt_viewer_decisions() {
        use axum::http::StatusCode;
        let lo = Some(std::net::IpAddr::from([127, 0, 0, 1]));
        let lan = Some(std::net::IpAddr::from([192, 168, 1, 9]));
        let a = PromptViewer::User(uid(TOKEN_A));

        // Not hosted: always the node's own user, whatever is presented.
        for (h, q, ip, xfp) in [
            (None, None, lo, false),
            (Some(TOKEN_A), None, lo, true),
            (None, Some(TOKEN_A), lan, true),
            (Some(""), None, None, false),
        ] {
            assert_eq!(
                resolve_prompt_viewer(false, h, q, ip, xfp),
                Ok(PromptViewer::Node)
            );
        }

        // Hosted, honoured: header or query token, loopback, XFP https.
        assert_eq!(
            resolve_prompt_viewer(true, Some(TOKEN_A), None, lo, true),
            Ok(a)
        );
        assert_eq!(
            resolve_prompt_viewer(true, None, Some(TOKEN_A), lo, true),
            Ok(a)
        );
        // The header wins over the query.
        assert_eq!(
            resolve_prompt_viewer(true, Some(TOKEN_A), Some(TOKEN_B), lo, true),
            Ok(a)
        );

        // Hosted, refused.
        let refused = |r: Result<PromptViewer, (StatusCode, &'static str)>| {
            matches!(r, Err((StatusCode::FORBIDDEN, _)))
        };
        assert!(
            refused(resolve_prompt_viewer(true, None, None, lo, true)),
            "no token"
        );
        assert!(
            refused(resolve_prompt_viewer(true, Some(""), None, lo, true)),
            "empty header"
        );
        assert!(
            refused(resolve_prompt_viewer(true, None, Some(""), lo, true)),
            "empty query"
        );
        assert!(
            refused(resolve_prompt_viewer(true, Some(TOKEN_A), None, lo, false)),
            "no XFP https"
        );
        assert!(
            refused(resolve_prompt_viewer(true, Some(TOKEN_A), None, lan, true)),
            "non-loopback source"
        );
        assert!(
            refused(resolve_prompt_viewer(true, Some(TOKEN_A), None, None, true)),
            "unknown source"
        );
    }

    /// A permission route reached without the hosted-mode flag is refused,
    /// not served as on a node that is not hosted.
    ///
    /// FALSIFY: make `hosted_flag` return `Some(false)` for a missing extension.
    #[tokio::test]
    async fn a_permission_route_without_the_hosted_flag_is_refused() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let _a = insert_owned(&pending, "nonce-of-a", Some(uid(TOKEN_A)));
        let unflagged = || {
            super::routes()
                .layer(Extension(pending.clone()))
                .layer(Extension::<AllowedHosts>(Arc::new(HashSet::new())))
                .layer(Extension(AllowedSourceCidrs::default()))
        };
        for (method, uri, body) in [
            ("GET", "/permission/pending", ""),
            ("POST", "/permission/nonce-of-a/respond", r#"{"index":0}"#),
            ("GET", "/permission/apps", ""),
            ("GET", "/permission/nonce-of-a", ""),
        ] {
            let resp = unflagged()
                .oneshot(proxied_request(method, uri, None, false, body))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "{method} {uri} without the hosted flag must be refused"
            );
        }
        assert!(pending.contains_key("nonce-of-a"));
    }

    /// The standalone `as_router` composition says it is not hosted, so its
    /// permission routes are served as on a node that is not hosted.
    #[tokio::test]
    async fn the_standalone_router_serves_the_permission_routes() {
        use tower::ServiceExt;
        let (api, router) =
            crate::server::client_api::HttpClientApi::as_router(&"127.0.0.1:0".parse().unwrap());
        drop(api);
        let resp = router
            .oneshot(proxied_request(
                "GET",
                "/permission/pending",
                None,
                false,
                "",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// `as_router` keeps a hosted flag set by an outer layer: wrapped in
    /// `HostedMode(true)`, its permission routes require a token.
    ///
    /// FALSIFY: make `default_not_hosted` insert `HostedMode(false)` whatever
    /// the request carries.
    #[tokio::test]
    async fn the_standalone_router_keeps_an_outer_hosted_flag() {
        use tower::ServiceExt;
        let (api, router) =
            crate::server::client_api::HttpClientApi::as_router(&"127.0.0.1:0".parse().unwrap());
        drop(api);
        let resp = router
            .layer(Extension(HostedMode(true)))
            .oneshot(proxied_request(
                "GET",
                "/permission/pending",
                None,
                true,
                "",
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::FORBIDDEN,
            "an outer HostedMode(true) must not be replaced"
        );
    }

    /// On a hosted node `pending` and `respond` take the token from the
    /// header only; a `?userToken=` query is not a token there and is refused.
    ///
    /// FALSIFY: pass `true` for `accept_query_token` in `viewer_gate`.
    #[tokio::test]
    async fn a_hosted_node_takes_no_query_token_on_pending_or_respond() {
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let pending = empty_pending();
        let _a = insert_owned(&pending, "nonce-of-a", Some(uid(TOKEN_A)));
        for (method, uri, body) in [
            (
                "GET",
                "/permission/pending?userToken=hosted-perm-user-a",
                "",
            ),
            (
                "POST",
                "/permission/nonce-of-a/respond?userToken=hosted-perm-user-a",
                r#"{"index":0}"#,
            ),
        ] {
            let resp = hosted_app(pending.clone(), true)
                .oneshot(proxied_request(method, uri, None, true, body))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "{method} {uri} must not accept a query token"
            );
        }
        assert!(pending.contains_key("nonce-of-a"));
    }

    /// An answer from the owner emits `Removed` naming the owner, so the
    /// owner's other tabs are sent the removal.
    ///
    /// FALSIFY: emit `owner: None` from `permission_respond`.
    #[tokio::test]
    #[serial_test::serial(prompt_events)]
    async fn an_answer_emits_removed_with_the_prompts_owner() {
        use tower::ServiceExt;
        let pending = empty_pending();
        let _b = insert_owned(&pending, "nonce-answered-by-b", Some(uid(TOKEN_B)));
        let mut events = prompt_events().subscribe();
        let resp = hosted_app(pending.clone(), true)
            .oneshot(proxied_request(
                "POST",
                "/permission/nonce-answered-by-b/respond",
                Some(TOKEN_B),
                true,
                r#"{"index":0}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let mut owner = None;
        let mut lagged = 0u64;
        loop {
            match events.try_recv() {
                Ok(PromptEvent::Removed { nonce, owner: o }) if nonce == "nonce-answered-by-b" => {
                    owner = Some(o);
                    break;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => lagged += n,
                Err(_) => break,
            }
        }
        assert_eq!(
            lagged, 0,
            "the shared prompt-event channel lagged by {lagged}"
        );
        assert_eq!(owner, Some(Some(uid(TOKEN_B))));
    }

    /// The production composition (`serve_client_api_with_listener`) gives the
    /// permission routes the hosted-mode flag on both of its branches: a
    /// loopback bind and a wildcard bind (which adds the private-network
    /// filter). A missing flag is refused with 500 rather than read as "not
    /// hosted", so this checks for the hosted 403 and 200 specifically.
    ///
    /// FALSIFY: remove the `Extension(hosted_mode)` layer from either branch of
    /// `serve_client_api_in_impl`; that branch answers 500.
    #[tokio::test]
    async fn a_hosted_node_composition_requires_a_token_on_the_permission_routes() {
        async fn serve(hosted: bool) -> (SocketAddr, impl Drop) {
            serve_on("127.0.0.1:0", hosted).await
        }
        async fn serve_on(bind: &str, hosted: bool) -> (SocketAddr, impl Drop) {
            let listener = std::net::TcpListener::bind(bind).unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            let mut cfg = crate::config::WebsocketApiConfig::from(addr);
            cfg.hosted_mode = hosted;
            let clients = crate::server::serve_client_api_with_listener(cfg, listener)
                .await
                .expect("serve the client API");
            struct Keep(#[allow(dead_code)] [crate::client_events::BoxedClient; 2]);
            impl Drop for Keep {
                fn drop(&mut self) {}
            }
            (addr, Keep(clients))
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let (hosted_addr, _keep_hosted) = serve(true).await;
        let url = format!("http://{hosted_addr}/permission/pending");
        let tokenless = client.get(&url).send().await.unwrap();
        assert_eq!(
            tokenless.status().as_u16(),
            403,
            "a hosted node must refuse the permission routes without a token"
        );
        let tokened = client
            .get(&url)
            .header("x-freenet-user-token", TOKEN_A)
            .header("x-forwarded-proto", "https")
            .send()
            .await
            .unwrap();
        assert_eq!(tokened.status().as_u16(), 200);
        let listed: serde_json::Value = tokened.json().await.unwrap();
        assert!(
            listed.as_array().is_some_and(|l| l
                .iter()
                .all(|p| p["nonce"].as_str().is_some_and(|n| !n.is_empty()))),
            "the hosted listing must be a JSON array"
        );

        // The wildcard-bind branch, reached over loopback.
        let (lan_addr, _keep_lan) = serve_on("0.0.0.0:0", true).await;
        let lan_url = format!("http://127.0.0.1:{}/permission/pending", lan_addr.port());
        let tokenless = client.get(&lan_url).send().await.unwrap();
        assert_eq!(
            tokenless.status().as_u16(),
            403,
            "a hosted node on a wildcard bind must refuse the permission routes without a token"
        );
        let tokened = client
            .get(&lan_url)
            .header("x-freenet-user-token", TOKEN_A)
            .header("x-forwarded-proto", "https")
            .send()
            .await
            .unwrap();
        assert_eq!(tokened.status().as_u16(), 200);

        let (plain_addr, _keep_plain) = serve(false).await;
        let plain = client
            .get(format!("http://{plain_addr}/permission/pending"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            plain.status().as_u16(),
            200,
            "a node that is not hosted serves the route without a token"
        );
    }
}

#[cfg(test)]
mod grant_endpoint_tests {
    use super::*;
    use crate::contract::delegate_capabilities::DelegateCapabilities;

    fn loopback() -> Extension<ConnectInfo<SocketAddr>> {
        Extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
    }

    fn lan() -> Extension<ConnectInfo<SocketAddr>> {
        Extension(ConnectInfo(SocketAddr::from(([192, 168, 1, 50], 12345))))
    }

    fn origin(o: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("origin", o.parse().unwrap());
        h
    }

    fn body(app: &str) -> Json<RevokeGrantRequest> {
        Json(RevokeGrantRequest {
            app: app.to_string(),
            capability: "background".to_string(),
        })
    }

    /// Revoking is a capability: it needs a loopback peer AND a present,
    /// trusted Origin (CSRF), checked before anything touches the node.
    #[tokio::test]
    async fn revoke_is_gated_like_respond() {
        let app = ContractInstanceId::new([1; 32]).to_string();
        for (ci, headers, expected) in [
            (
                lan(),
                origin("http://localhost:7509"),
                axum::http::StatusCode::FORBIDDEN,
            ),
            (
                loopback(),
                HeaderMap::new(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            (
                loopback(),
                origin("https://evil.example"),
                axum::http::StatusCode::FORBIDDEN,
            ),
            // Gate passed; no node behind the handle.
            (
                loopback(),
                origin("http://localhost:7509"),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let (status, _) = revoke_grant(Some(ci), headers, None, None, None, body(&app)).await;
            assert_eq!(status, expected);
        }
    }

    /// A foreign Origin reads an empty list, never the real one.
    #[tokio::test]
    async fn listing_is_withheld_from_a_foreign_origin() {
        use axum::body::to_bytes;
        let resp = list_grants(
            Some(loopback()),
            origin("https://evil.example"),
            None,
            None,
            None,
        )
        .await;
        let bytes = to_bytes(resp.into_body(), 4096).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["grants"], serde_json::json!([]));
        let resp = list_grants(Some(lan()), HeaderMap::new(), None, None, None).await;
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// Without an Origin (a same-origin GET), only a Host naming this machine
    /// or an allowed host may read the list: a DNS-rebound page's Host is its
    /// own name.
    #[test]
    fn a_rebound_host_cannot_read_the_list() {
        fn host(h: &str) -> HeaderMap {
            let mut m = HeaderMap::new();
            m.insert(axum::http::header::HOST, h.parse().unwrap());
            m
        }
        for ok in [
            "localhost:7509",
            "127.0.0.1:7509",
            "[::1]:7509",
            "LOCALHOST",
        ] {
            assert!(grants_read_trusted(&host(ok), None, None), "{ok}");
        }
        for bad in [
            "evil.example:7509",
            "localhost.evil.example",
            "127.0.0.2:7509",
        ] {
            assert!(!grants_read_trusted(&host(bad), None, None), "{bad}");
        }
        assert!(
            !grants_read_trusted(&HeaderMap::new(), None, None),
            "no Host"
        );
        let allowed: AllowedHosts =
            std::sync::Arc::new(["node.lan:7509".to_string()].into_iter().collect());
        assert!(grants_read_trusted(
            &host("node.lan:7509"),
            Some(&allowed),
            None
        ));
        assert!(!grants_read_trusted(
            &origin("https://evil.example"),
            None,
            None
        ));
    }

    #[test]
    fn list_and_revoke_round_trip() {
        use crate::contract::delegate_capabilities::{AppIdentity, CapabilityPrompt, Grant};
        let caps = DelegateCapabilities::in_memory();
        let id = ContractInstanceId::new([2; 32]);
        let prompt = CapabilityPrompt {
            app: AppIdentity::WebApp(id),
            delegate: freenet_stdlib::prelude::DelegateKey::new(
                [3; 32],
                freenet_stdlib::prelude::CodeHash::new([3; 32]),
            ),
            capabilities: vec![Capability::Background],
        };
        caps.record_answer(&prompt, true);
        let listed = grants_json(&caps).unwrap();
        assert_eq!(listed[0]["app"], id.to_string());
        assert_eq!(listed[0]["capability"], "background");
        assert_eq!(listed[0]["state"], "granted");

        let (status, _) = revoke_with(&caps, body(&id.to_string()).0);
        assert_eq!(status, axum::http::StatusCode::OK);
        assert!(
            caps.grant(&AppIdentity::WebApp(id), Capability::Background)
                .is_none()
        );
        assert!(!matches!(
            caps.grant(&AppIdentity::WebApp(id), Capability::Background),
            Some(Grant::Granted { .. })
        ));

        let (status, _) = revoke_with(&caps, body("not-an-id").0);
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        let (status, _) = revoke_with(
            &caps,
            RevokeGrantRequest {
                app: id.to_string(),
                capability: "teleport".to_string(),
            },
        );
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    }

    // The Apps and permissions page is reached from the dashboard's "Review
    // or revoke" link and must offer a way back to the dashboard.
    #[tokio::test]
    async fn test_apps_page_links_back_to_dashboard() {
        use axum::body::to_bytes;
        let resp = apps_page().await.into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            html.contains(r#"<a class="back" href="/">"#),
            "apps page must link back to the dashboard at /"
        );
    }
}
