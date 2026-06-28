use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::{ClientResponse, UserInputRequest};
use tokio::sync::{broadcast, oneshot};

/// Timeout for user input prompts. After this duration, the request is auto-denied.
pub(crate) const USER_INPUT_TIMEOUT: Duration = Duration::from_secs(60);

/// Minimum interval between browser-opens for no-tab prompts (#3820).
///
/// A burst of prompts arriving with no dashboard tab connected (e.g. a
/// background script firing many delegate ops) would otherwise open one browser
/// tab per prompt — up to `MAX_PENDING_PROMPTS` tabs at once. This caps it to
/// one tab per window. Prompts suppressed by the cooldown fall back to the
/// pre-existing behaviour (auto-deny after [`USER_INPUT_TIMEOUT`]), so the
/// cooldown never leaves a prompt worse off than before this feature existed.
const NO_TAB_OPEN_COOLDOWN: Duration = Duration::from_secs(5);

/// Maximum message length for display. Prevents abuse from untrusted delegates.
const MAX_MESSAGE_LEN: usize = 2048;
/// Maximum button label length. 64 chars fits comfortably in a button.
const MAX_LABEL_LEN: usize = 64;
/// Maximum number of response buttons. Keeps the UI usable.
const MAX_LABELS: usize = 10;
/// Maximum stored length of a runtime-attested identity hash.
///
/// Defense-in-depth: today both `delegate_key` and `CallerIdentity::WebApp`
/// hashes come from runtime context that is bounded at the source (BLAKE3
/// hex / base58 contract id, both well under this limit). Capping at the
/// insertion point keeps the prompt store from holding arbitrarily large
/// strings if a future caller passes something larger.
///
/// `permission_prompts.rs` re-uses this same cap at the JSON / HTML
/// rendering boundary, so the two layers cannot disagree about the
/// maximum.
pub(crate) const MAX_IDENTITY_HASH_CHARS: usize = 256;

/// Runtime-attested identity of the entity that triggered a permission prompt.
///
/// This is a display-layer representation: it lives only between the executor
/// (which knows the typed `ContractInstanceId` / future `DelegateKey`) and the
/// permission-prompt UI. Stringification happens at the boundary so the UI
/// doesn't depend on stdlib types and the stored prompt can be serialised to
/// the dashboard JSON without any further conversion.
///
/// Variants reflect what `MessageOrigin` in freenet-stdlib can carry today.
/// A `Delegate(String)` variant is reserved for #3860 (extending
/// `MessageOrigin` to attest delegate-to-delegate callers); the prompt UI is
/// already shaped to render it without further changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallerIdentity {
    /// No structured caller was recorded for this request. Today this covers
    /// both "no web app involved" and "delegate-to-delegate call" — those are
    /// not currently distinguishable at the API level (see #3860).
    None,
    /// The request came from a web app whose contract id was attested by
    /// `MessageOrigin::WebApp(..)`. The string is the `ContractInstanceId`
    /// in its canonical display form.
    WebApp(String),
}

/// Abstracts user prompting for delegate `RequestUserInput` messages.
///
/// Implementations receive the runtime-attested identity of the calling
/// delegate and (when known) of the originating web app. These identities are
/// rendered in the permission prompt UI so the user can see *who* is asking
/// and *which* app triggered the request. The identities MUST come from the
/// runtime context, never from the delegate's own message payload — a rogue
/// delegate must not be able to spoof another delegate's or app's identity.
pub trait UserInputPrompter: Send + Sync {
    fn prompt(
        &self,
        request: &UserInputRequest<'static>,
        delegate_key: &str,
        caller: CallerIdentity,
    ) -> impl std::future::Future<Output = Option<(usize, ClientResponse<'static>)>> + Send;
}

/// A pending permission request awaiting user response via the web dashboard.
pub(crate) struct PendingPrompt {
    pub message: String,
    pub labels: Vec<String>,
    pub delegate_key: String,
    pub caller: CallerIdentity,
    pub response_tx: oneshot::Sender<usize>,
}

/// Shared registry of pending permission prompts, keyed by 128-bit hex nonce.
pub(crate) type PendingPrompts = Arc<DashMap<String, PendingPrompt>>;

/// Global pending prompts registry, shared between the HTTP server (consumer)
/// and the DashboardPrompter (producer).
static PENDING_PROMPTS: std::sync::OnceLock<PendingPrompts> = std::sync::OnceLock::new();

/// Get or initialize the global pending prompts registry.
pub(crate) fn pending_prompts() -> PendingPrompts {
    PENDING_PROMPTS
        .get_or_init(|| Arc::new(DashMap::new()))
        .clone()
}

/// Maximum concurrent pending prompts to prevent memory exhaustion.
const MAX_PENDING_PROMPTS: usize = 32;

/// Snapshot of a prompt's display fields, sufficient for the SSE handler to
/// render an `Added` event without holding the DashMap entry. Cloned out of
/// the registry so the broadcast path doesn't pin the entry's lock.
#[derive(Clone, Debug)]
pub(crate) struct PromptSnapshot {
    pub nonce: String,
    pub message: String,
    pub labels: Vec<String>,
    pub delegate_key: String,
    pub caller: CallerIdentity,
}

/// Lifecycle event for a permission prompt. Consumed by the SSE endpoint to
/// push state changes to every open Freenet tab in real time. The polling
/// endpoint at `/permission/pending` is retained as a fallback and is not
/// driven by this stream.
#[derive(Clone, Debug)]
pub(crate) enum PromptEvent {
    Added(PromptSnapshot),
    Removed { nonce: String },
}

/// Broadcast capacity. Each lifecycle is two events (Added + Removed), and
/// MAX_PENDING_PROMPTS caps concurrent in-flight prompts at 32, so 128 leaves
/// healthy headroom even if a transient SSE subscriber lags briefly. On
/// `RecvError::Lagged`, the SSE handler resyncs from the DashMap snapshot.
const PROMPT_EVENT_CAPACITY: usize = 128;

/// Global lifecycle broadcast for permission prompts.
static PROMPT_EVENTS: std::sync::OnceLock<broadcast::Sender<PromptEvent>> =
    std::sync::OnceLock::new();

/// Get or initialize the global prompt-event broadcaster.
pub(crate) fn prompt_events() -> broadcast::Sender<PromptEvent> {
    PROMPT_EVENTS
        .get_or_init(|| broadcast::channel(PROMPT_EVENT_CAPACITY).0)
        .clone()
}

/// Fire a prompt lifecycle event. `Sender::send` returns `Err` when there
/// are no live subscribers; that's the common case when the shell page
/// isn't open yet, and the polling fallback covers it. Slow subscribers
/// see `Lagged` and resync from the DashMap.
///
/// Caps the nonce length on `Removed` so that even if an unusual code path
/// ever passes a client-supplied nonce here (today the only producers are
/// `DashboardPrompter` itself and the HTTP `respond` handler, which both
/// route through validated DashMap keys), the broadcast can't carry an
/// arbitrarily large string to every connected tab.
pub(crate) fn emit_prompt_event(event: PromptEvent) {
    let event = match event {
        PromptEvent::Removed { nonce } => PromptEvent::Removed {
            nonce: cap_identity_chars(&nonce),
        },
        other => other,
    };
    drop(prompt_events().send(event));
}

/// Action invoked to surface a permission prompt to the user when no dashboard
/// tab is connected to display it. Receives the standalone permission-page URL.
///
/// In production this opens the user's default browser (see
/// [`open_permission_page_in_browser`]); tests substitute a recording stub so
/// the decision logic can be exercised without spawning a real browser.
pub(crate) type NoTabNotifier = Arc<dyn Fn(&str) + Send + Sync>;

/// Opens the user's browser to the permission page on the local dashboard,
/// then waits for the user to click a button. The HTTP POST handler sends
/// the response back via a oneshot channel.
///
/// Works from systemd user services because the node serves HTTP (already
/// running) and the shell page's JS handles browser notification + opening.
///
/// When a prompt arrives and no dashboard tab is connected to display it, the
/// prompt would otherwise silently auto-deny after [`USER_INPUT_TIMEOUT`]; in
/// that case the prompter opens the standalone permission page directly in the
/// user's browser so background / API-triggered prompts stay actionable (#3820).
pub struct DashboardPrompter {
    pending: PendingPrompts,
    /// HTTP authority (`host:port`) of the local dashboard server, derived from
    /// the ws-api bind address (see [`dashboard_authority`]). Used to build the
    /// permission-page URL opened when no tab is connected (#3820).
    dashboard_authority: String,
    /// Invoked with the permission-page URL when a prompt has no connected tab.
    notify_no_tab: NoTabNotifier,
    /// Timestamp of the last no-tab browser-open, for the
    /// [`NO_TAB_OPEN_COOLDOWN`] rate limit. `None` until the first open.
    last_no_tab_open: std::sync::Mutex<Option<tokio::time::Instant>>,
}

impl DashboardPrompter {
    /// Build a prompter that opens the user's browser when no dashboard tab is
    /// connected. `ws_api_addr` is the address the local HTTP/WS API dashboard
    /// is served on (`config.ws_api.address` + `config.ws_api.port`).
    pub fn new(pending: PendingPrompts, ws_api_addr: SocketAddr) -> Self {
        Self {
            pending,
            dashboard_authority: dashboard_authority(ws_api_addr),
            notify_no_tab: Arc::new(open_permission_page_in_browser),
            last_no_tab_open: std::sync::Mutex::new(None),
        }
    }

    /// Alert the user out-of-band when a freshly-created prompt has no dashboard
    /// tab to display it. `subscriber_count` is the number of live SSE
    /// subscribers to the prompt-event broadcast
    /// ([`prompt_events()`]`.receiver_count()`); each connected gateway tab
    /// holds exactly one. Zero means every tab is closed, so the prompt would
    /// silently auto-deny after [`USER_INPUT_TIMEOUT`] unless we surface it.
    ///
    /// Rate-limited by [`NO_TAB_OPEN_COOLDOWN`] so a burst of no-tab prompts
    /// opens one tab per window rather than one per prompt.
    fn maybe_alert_no_tab(&self, nonce: &str, subscriber_count: usize) {
        if subscriber_count != 0 || !self.no_tab_cooldown_elapsed() {
            return;
        }
        let url = format!("http://{}/permission/{nonce}", self.dashboard_authority);
        (self.notify_no_tab)(&url);
    }

    /// Returns `true` and records "now" when at least [`NO_TAB_OPEN_COOLDOWN`]
    /// has passed since the last no-tab browser-open (or there was none yet);
    /// returns `false` without updating when still inside the cooldown window.
    fn no_tab_cooldown_elapsed(&self) -> bool {
        let now = tokio::time::Instant::now();
        let mut last = self
            .last_no_tab_open
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match *last {
            Some(prev) if now.duration_since(prev) < NO_TAB_OPEN_COOLDOWN => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }
}

/// Build the HTTP authority (`host:port`) for the local dashboard URL from the
/// ws-api bind address (#3820).
///
/// The dashboard server binds an explicit loopback companion socket for
/// wildcard / loopback binds (see `server::companion_bind_addr`), so `127.0.0.1`
/// is reachable and is the stable choice there. A bind to a *specific* interface
/// address has no loopback companion — only that address is served — so the URL
/// must target it directly, or the browser-open would hit a refused endpoint.
/// IPv6 literals are bracketed per the URL authority grammar.
fn dashboard_authority(bind: SocketAddr) -> String {
    let ip = bind.ip();
    let host = if ip.is_unspecified() || ip.is_loopback() {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        ip
    };
    match host {
        IpAddr::V4(v4) => format!("{v4}:{}", bind.port()),
        IpAddr::V6(v6) => format!("[{v6}]:{}", bind.port()),
    }
}

/// Open the standalone permission page in the user's default browser.
///
/// Used by [`DashboardPrompter`] when a permission prompt arrives with no
/// dashboard tab connected to display it (#3820). Mirrors the binary-side
/// `commands::open_url_in_browser`:
///
/// * On Windows it goes through `ShellExecuteW` (not `cmd /c start`) because the
///   service wrapper detaches the console via `FreeConsole()` at startup, after
///   which a spawned `cmd.exe` has no console and fails silently.
/// * On macOS/Linux it spawns `open` / `xdg-open` with all three standard
///   handles nulled. The node may run as a systemd / Windows service with no
///   valid inherited stdio; an un-nulled `Command::spawn()` then fails with
///   "The handle is invalid" (os error 6). See the `open_log_file` fix (#3933).
///
/// Best effort: when no browser can be opened (headless service, no
/// `DISPLAY`/`WAYLAND_DISPLAY`) the spawn fails harmlessly and the prompt's
/// existing [`USER_INPUT_TIMEOUT`] still governs.
fn open_permission_page_in_browser(url: &str) {
    #[cfg(target_os = "windows")]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;

        let operation: Vec<u16> = OsStr::new("open").encode_wide().chain(Some(0)).collect();
        let url_wide: Vec<u16> = OsStr::new(url).encode_wide().chain(Some(0)).collect();

        unsafe {
            winapi::um::shellapi::ShellExecuteW(
                std::ptr::null_mut(),
                operation.as_ptr(),
                url_wide.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                winapi::um::winuser::SW_SHOWNORMAL,
            );
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        #[cfg(target_os = "macos")]
        let mut cmd = std::process::Command::new("open");
        #[cfg(not(target_os = "macos"))]
        let mut cmd = std::process::Command::new("xdg-open");
        cmd.arg(url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        drop(cmd.spawn());
    }
}

#[cfg(test)]
impl DashboardPrompter {
    /// Construct a prompter with a custom no-tab notifier so tests can assert
    /// the decision logic without spawning a real browser.
    fn with_notifier(
        pending: PendingPrompts,
        ws_api_addr: SocketAddr,
        notify_no_tab: NoTabNotifier,
    ) -> Self {
        Self {
            pending,
            dashboard_authority: dashboard_authority(ws_api_addr),
            notify_no_tab,
            last_no_tab_open: std::sync::Mutex::new(None),
        }
    }
}

impl UserInputPrompter for DashboardPrompter {
    async fn prompt(
        &self,
        request: &UserInputRequest<'static>,
        delegate_key: &str,
        caller: CallerIdentity,
    ) -> Option<(usize, ClientResponse<'static>)> {
        if request.responses.is_empty() {
            tracing::warn!("RequestUserInput has no response options");
            return None;
        }

        if self.pending.len() >= MAX_PENDING_PROMPTS {
            tracing::warn!(
                max = MAX_PENDING_PROMPTS,
                "Too many pending permission prompts, auto-denying"
            );
            return None;
        }

        let message = parse_message(request);
        let labels = parse_button_labels(request);

        // Generate a 128-bit cryptographic nonce for the permission URL
        let nonce = generate_nonce();

        let (tx, rx) = oneshot::channel();

        // Cap stored identity hashes at MAX_IDENTITY_HASH_CHARS at the
        // insertion boundary (not just at the JSON/HTML layer). Defense-in-
        // depth: real-world delegate keys are BLAKE3 hex (~64 chars) and
        // contract ids are base58 (~44 chars), both well under the cap.
        let stored_delegate_key = cap_identity_chars(delegate_key);
        let stored_caller = match caller {
            CallerIdentity::None => CallerIdentity::None,
            CallerIdentity::WebApp(hash) => CallerIdentity::WebApp(cap_identity_chars(&hash)),
        };

        self.pending.insert(
            nonce.clone(),
            PendingPrompt {
                message: message.clone(),
                labels: labels.clone(),
                delegate_key: stored_delegate_key.clone(),
                caller: stored_caller.clone(),
                response_tx: tx,
            },
        );

        // Fire the broadcast Added event AFTER the DashMap insert so any SSE
        // subscriber that wakes up on the event can immediately find the entry
        // if it falls back to a registry lookup.
        emit_prompt_event(PromptEvent::Added(PromptSnapshot {
            nonce: nonce.clone(),
            message,
            labels,
            delegate_key: stored_delegate_key,
            caller: stored_caller,
        }));

        // #3820: if no dashboard tab is connected to display this prompt, the
        // user would never see it and it would silently auto-deny after
        // USER_INPUT_TIMEOUT. A connected gateway tab subscribes to the
        // prompt-event broadcast (one receiver per SSE connection), so zero
        // receivers means every tab is closed -- open the standalone permission
        // page in the user's browser so the prompt stays actionable. On a
        // headless service (no DISPLAY/WAYLAND_DISPLAY) the browser-open is a
        // harmless no-op and the existing timeout still governs.
        self.maybe_alert_no_tab(&nonce, prompt_events().receiver_count());

        // Log at debug, not info -- nonce is the sole auth token for this prompt
        tracing::debug!(
            request_id = request.request_id,
            "Permission prompt created, waiting for user response via dashboard"
        );

        // Wait for user response with timeout
        let result = tokio::time::timeout(USER_INPUT_TIMEOUT, rx).await;

        // Clean up if still pending. The HTTP `respond` handler removes the
        // entry on a real user click; this remove is a no-op in that case
        // and otherwise covers the timeout / dropped-channel cleanup paths.
        // Always emit Removed so subscribers can hide their overlay
        // regardless of which path retired the prompt; a duplicate Removed
        // (when both the HTTP handler and this cleanup fire) is harmless,
        // because the SSE client's hide is idempotent on nonce.
        let was_present = self.pending.remove(&nonce).is_some();
        if was_present {
            emit_prompt_event(PromptEvent::Removed {
                nonce: nonce.clone(),
            });
        }

        match result {
            Ok(Ok(idx)) if idx < request.responses.len() => {
                let response = request.responses[idx].clone().into_owned();
                Some((idx, response))
            }
            Ok(Ok(_)) => {
                tracing::warn!(nonce = %nonce, "Invalid response index from dashboard");
                None
            }
            Ok(Err(_)) => {
                tracing::debug!(nonce = %nonce, "Permission prompt channel closed");
                None
            }
            Err(_) => {
                tracing::warn!(nonce = %nonce, "Permission prompt timed out after 60s");
                None
            }
        }
    }
}

/// Truncate a runtime-attested identity hash to `MAX_IDENTITY_HASH_CHARS`
/// codepoints. Char-based so multi-byte content doesn't get split mid-grapheme.
fn cap_identity_chars(s: &str) -> String {
    s.chars().take(MAX_IDENTITY_HASH_CHARS).collect()
}

/// Generate a 128-bit cryptographic hex nonce.
fn generate_nonce() -> String {
    use crate::config::GlobalRng;
    let a = GlobalRng::random_u64();
    let b = GlobalRng::random_u64();
    format!("{a:016x}{b:016x}")
}

/// Extract a displayable message from `NotificationMessage` bytes.
pub(crate) fn parse_message(request: &UserInputRequest<'_>) -> String {
    let bytes = request.message.bytes();
    let raw = if let Ok(json_str) = serde_json::from_slice::<String>(bytes) {
        json_str
    } else {
        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|_| "A delegate is requesting permission.".to_string())
    };
    raw.chars()
        .take(MAX_MESSAGE_LEN)
        .filter(|c| !c.is_control() || *c == '\n')
        .collect()
}

/// Extract button labels from `ClientResponse` bytes as sanitized UTF-8 strings.
pub(crate) fn parse_button_labels(request: &UserInputRequest<'_>) -> Vec<String> {
    request
        .responses
        .iter()
        .take(MAX_LABELS)
        .enumerate()
        .map(|(i, r)| {
            let label =
                String::from_utf8((**r).to_vec()).unwrap_or_else(|_| format!("Option {}", i + 1));
            label
                .chars()
                .take(MAX_LABEL_LEN)
                .filter(|c| !c.is_control())
                .collect()
        })
        .collect()
}

/// Auto-approves by returning the first response. For testing only.
pub struct AutoApprovePrompter;

impl UserInputPrompter for AutoApprovePrompter {
    async fn prompt(
        &self,
        request: &UserInputRequest<'static>,
        _delegate_key: &str,
        _caller: CallerIdentity,
    ) -> Option<(usize, ClientResponse<'static>)> {
        request
            .responses
            .first()
            .map(|r| (0, r.clone().into_owned()))
    }
}

/// Always denies (returns None). For headless environments where no display
/// is available (e.g., gateway servers, CI).
#[allow(dead_code)]
pub struct AutoDenyPrompter;

impl UserInputPrompter for AutoDenyPrompter {
    async fn prompt(
        &self,
        _request: &UserInputRequest<'static>,
        _delegate_key: &str,
        _caller: CallerIdentity,
    ) -> Option<(usize, ClientResponse<'static>)> {
        None
    }
}

#[cfg(test)]
pub(crate) fn make_test_request(message: &str, responses: Vec<&str>) -> UserInputRequest<'static> {
    use freenet_stdlib::prelude::NotificationMessage;

    let msg = NotificationMessage::try_from(&serde_json::Value::String(message.to_string()))
        .expect("valid JSON");
    UserInputRequest {
        request_id: 1,
        message: msg,
        responses: responses
            .into_iter()
            .map(|r| ClientResponse::new(r.as_bytes().to_vec()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn webapp(s: &str) -> CallerIdentity {
        CallerIdentity::WebApp(s.to_string())
    }

    /// A prompter whose no-tab notifier is a no-op, so calling `prompt()` in a
    /// test never spawns a real browser (the global prompt-event broadcast is
    /// shared across tests, so `receiver_count()` can read zero here). Tests
    /// that exercise the no-tab decision itself use
    /// [`DashboardPrompter::with_notifier`] with a recording stub instead.
    fn noop_prompter(pending: PendingPrompts) -> DashboardPrompter {
        DashboardPrompter::with_notifier(pending, test_ws_addr(), Arc::new(|_| {}))
    }

    /// Default ws-api bind address used by the prompter tests (loopback:7509),
    /// which `dashboard_authority` maps to the `127.0.0.1:7509` URL authority.
    fn test_ws_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7509)
    }

    #[tokio::test]
    async fn test_auto_approve_returns_first_response() {
        let req = make_test_request("Allow this?", vec!["Allow", "Deny"]);
        let result = AutoApprovePrompter
            .prompt(&req, "dkey", webapp("cid"))
            .await;
        let (idx, response) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(&*response, b"Allow");
    }

    #[tokio::test]
    async fn test_auto_approve_empty_responses() {
        let req = make_test_request("Allow this?", vec![]);
        let result = AutoApprovePrompter
            .prompt(&req, "dkey", webapp("cid"))
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_auto_deny_always_returns_none() {
        let req = make_test_request("Allow this?", vec!["Allow", "Deny"]);
        let result = AutoDenyPrompter.prompt(&req, "dkey", webapp("cid")).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_button_labels() {
        let req = make_test_request("msg", vec!["Allow Once", "Always Allow", "Deny"]);
        let labels = parse_button_labels(&req);
        assert_eq!(labels, vec!["Allow Once", "Always Allow", "Deny"]);
    }

    #[test]
    fn test_parse_message_json_encoded() {
        let req = make_test_request("Hello world", vec![]);
        let msg = parse_message(&req);
        assert_eq!(msg, "Hello world");
    }

    #[test]
    fn test_parse_message_json_with_quotes() {
        use freenet_stdlib::prelude::NotificationMessage;
        let json_val = serde_json::Value::String("Test with \"quotes\"".to_string());
        let msg = NotificationMessage::try_from(&json_val).unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: msg,
            responses: vec![],
        };
        let parsed = parse_message(&req);
        assert_eq!(parsed, "Test with \"quotes\"");
    }

    #[tokio::test]
    async fn test_dashboard_prompter_max_pending() {
        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = noop_prompter(pending.clone());

        for i in 0..MAX_PENDING_PROMPTS {
            let (tx, _rx) = oneshot::channel();
            pending.insert(
                format!("nonce_{i}"),
                PendingPrompt {
                    message: "test".to_string(),
                    labels: vec!["OK".to_string()],
                    delegate_key: String::new(),
                    caller: CallerIdentity::None,
                    response_tx: tx,
                },
            );
        }

        let req = make_test_request("Over limit", vec!["Allow"]);
        let result = prompter.prompt(&req, "dkey", webapp("cid")).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_nonce_is_32_hex_chars() {
        let nonce = generate_nonce();
        assert_eq!(nonce.len(), 32);
        assert!(nonce.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_parse_message_strips_control_chars() {
        use freenet_stdlib::prelude::NotificationMessage;
        let json_val = serde_json::Value::String("Hello\x00\x07world".to_string());
        let msg = NotificationMessage::try_from(&json_val).unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: msg,
            responses: vec![],
        };
        let parsed = parse_message(&req);
        assert_eq!(parsed, "Helloworld");
    }

    #[test]
    fn test_parse_button_labels_invalid_utf8() {
        use freenet_stdlib::prelude::NotificationMessage;
        let req = UserInputRequest {
            request_id: 1,
            message: NotificationMessage::try_from(&serde_json::Value::String("msg".to_string()))
                .unwrap(),
            responses: vec![
                ClientResponse::new(b"Valid".to_vec()),
                ClientResponse::new(vec![0xFF, 0xFE]),
            ],
        };
        let labels = parse_button_labels(&req);
        assert_eq!(labels, vec!["Valid", "Option 2"]);
    }

    // ----- Sanitization-cap boundary tests (issue #3821) -----
    //
    // `parse_message` / `parse_button_labels` enforce the producer-side
    // display caps MAX_MESSAGE_LEN (2048), MAX_LABEL_LEN (64) and
    // MAX_LABELS (10). The existing tests above cover control-char
    // stripping and invalid-UTF-8 fallback but never exercise the length
    // caps at their boundaries. These tests pin the exact at-limit /
    // over-limit behaviour so a future cap change (or a refactor that
    // moves the `.take()` before/after the `.filter()`) is caught.

    /// A message exactly at MAX_MESSAGE_LEN chars passes through untouched.
    #[test]
    fn test_parse_message_at_max_len_unchanged() {
        let msg = "a".repeat(MAX_MESSAGE_LEN);
        let req = make_test_request(&msg, vec![]);
        let parsed = parse_message(&req);
        assert_eq!(parsed.chars().count(), MAX_MESSAGE_LEN);
        assert_eq!(parsed, msg);
    }

    /// A message one char over MAX_MESSAGE_LEN is truncated to the cap.
    #[test]
    fn test_parse_message_over_max_len_truncated() {
        let msg = "a".repeat(MAX_MESSAGE_LEN + 1);
        let req = make_test_request(&msg, vec![]);
        let parsed = parse_message(&req);
        assert_eq!(parsed.chars().count(), MAX_MESSAGE_LEN);
    }

    /// Truncation counts by `char`, not byte: a multi-byte message over the
    /// cap must truncate at MAX_MESSAGE_LEN codepoints without splitting a
    /// grapheme (which would otherwise risk a panic on a byte boundary).
    #[test]
    fn test_parse_message_over_max_len_char_based() {
        let msg = "\u{1F525}".repeat(MAX_MESSAGE_LEN + 5);
        let req = make_test_request(&msg, vec![]);
        let parsed = parse_message(&req);
        assert_eq!(parsed.chars().count(), MAX_MESSAGE_LEN);
        assert!(parsed.chars().all(|c| c == '\u{1F525}'));
    }

    /// A button label exactly at MAX_LABEL_LEN chars passes through untouched.
    #[test]
    fn test_parse_button_labels_at_max_label_len_unchanged() {
        let label = "x".repeat(MAX_LABEL_LEN);
        let req = make_test_request("msg", vec![label.as_str()]);
        let labels = parse_button_labels(&req);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].chars().count(), MAX_LABEL_LEN);
        assert_eq!(labels[0], label);
    }

    /// A button label one char over MAX_LABEL_LEN is truncated to the cap.
    #[test]
    fn test_parse_button_labels_over_max_label_len_truncated() {
        let label = "x".repeat(MAX_LABEL_LEN + 1);
        let req = make_test_request("msg", vec![label.as_str()]);
        let labels = parse_button_labels(&req);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].chars().count(), MAX_LABEL_LEN);
    }

    /// Exactly MAX_LABELS responses are all kept.
    #[test]
    fn test_parse_button_labels_at_max_labels_kept() {
        let resp: Vec<String> = (0..MAX_LABELS).map(|i| format!("Option {i}")).collect();
        let resp_refs: Vec<&str> = resp.iter().map(String::as_str).collect();
        let req = make_test_request("msg", resp_refs);
        let labels = parse_button_labels(&req);
        assert_eq!(labels.len(), MAX_LABELS);
    }

    /// One response over MAX_LABELS drops the extra label(s): the rendered
    /// button grid is capped so a delegate can't force an arbitrary count.
    #[test]
    fn test_parse_button_labels_over_max_labels_capped() {
        let resp: Vec<String> = (0..MAX_LABELS + 1).map(|i| format!("Option {i}")).collect();
        let resp_refs: Vec<&str> = resp.iter().map(String::as_str).collect();
        let req = make_test_request("msg", resp_refs);
        let labels = parse_button_labels(&req);
        assert_eq!(labels.len(), MAX_LABELS);
    }

    #[test]
    fn test_parse_message_raw_utf8() {
        use freenet_stdlib::prelude::NotificationMessage;
        let raw_msg =
            NotificationMessage::try_from(&serde_json::Value::String("Raw message".to_string()))
                .unwrap();
        let req = UserInputRequest {
            request_id: 1,
            message: raw_msg,
            responses: vec![],
        };
        let msg = parse_message(&req);
        assert_eq!(msg, "Raw message");
    }

    #[tokio::test]
    async fn test_auto_approve_with_three_responses() {
        let req = make_test_request("Allow?", vec!["Allow Once", "Always Allow", "Deny"]);
        let result = AutoApprovePrompter
            .prompt(&req, "dkey", webapp("cid"))
            .await;
        let (idx, response) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(&*response, b"Allow Once");
    }

    #[tokio::test]
    async fn test_auto_deny_with_multiple_responses() {
        let req = make_test_request("Allow?", vec!["Allow Once", "Always Allow", "Deny"]);
        let result = AutoDenyPrompter.prompt(&req, "dkey", webapp("cid")).await;
        assert!(result.is_none());
    }

    // Regression test for issue #3857: the dashboard prompter MUST populate
    // PendingPrompt's identity fields from the runtime-attested values passed
    // into prompt(), not hardcode them to "Unknown". Without this, the
    // permission overlay renders "Unknown" in the structured identity slots,
    // forcing the user to trust the delegate's self-authored message for
    // attribution, which a malicious delegate could spoof.
    #[tokio::test]
    async fn test_dashboard_prompter_populates_webapp_caller() {
        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = noop_prompter(pending.clone());

        let req = make_test_request("Approve?", vec!["Allow", "Deny"]);
        let pending_clone = pending.clone();
        let handle = tokio::spawn(async move {
            prompter
                .prompt(&req, "DLGKEY123", webapp("CONTRACT456"))
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let entry = pending_clone
            .iter()
            .next()
            .expect("prompt should be registered");
        assert_eq!(entry.value().delegate_key, "DLGKEY123");
        assert_eq!(
            entry.value().caller,
            CallerIdentity::WebApp("CONTRACT456".to_string())
        );

        let nonce = entry.key().clone();
        drop(entry);
        let (_, prompt) = pending_clone.remove(&nonce).unwrap();
        prompt.response_tx.send(1).unwrap();
        let _ = handle.await.unwrap();
    }

    // When no web-app caller is attested (delegate-to-delegate, local client,
    // or any other non-WebApp invocation path), the prompt should record
    // CallerIdentity::None — distinct from WebApp(""), so the UI can render a
    // dedicated "No app caller" line rather than a blank field.
    #[tokio::test]
    async fn test_dashboard_prompter_records_none_caller() {
        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = noop_prompter(pending.clone());

        let req = make_test_request("Approve?", vec!["Allow"]);
        let pending_clone = pending.clone();
        let handle =
            tokio::spawn(
                async move { prompter.prompt(&req, "DLGKEY", CallerIdentity::None).await },
            );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let entry = pending_clone
            .iter()
            .next()
            .expect("prompt should be registered");
        assert_eq!(entry.value().delegate_key, "DLGKEY");
        assert_eq!(entry.value().caller, CallerIdentity::None);

        let nonce = entry.key().clone();
        drop(entry);
        let (_, prompt) = pending_clone.remove(&nonce).unwrap();
        prompt.response_tx.send(0).unwrap();
        let _ = handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_dashboard_prompter_happy_path() {
        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = noop_prompter(pending.clone());

        let req = make_test_request("Allow signing?", vec!["Allow", "Deny"]);

        // Spawn the prompt in a task
        let pending_clone = pending.clone();
        let handle =
            tokio::spawn(async move { prompter.prompt(&req, "dkey", webapp("cid")).await });

        // Wait briefly for the prompt to be inserted
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Find the nonce and respond
        let nonce = pending_clone
            .iter()
            .next()
            .expect("should have a pending prompt")
            .key()
            .clone();

        let (_, prompt) = pending_clone.remove(&nonce).unwrap();
        prompt.response_tx.send(0).unwrap();

        let result = handle.await.unwrap();
        let (idx, response) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(&*response, b"Allow");
    }

    /// `DashboardPrompter::prompt` fires `PromptEvent::Added` after inserting
    /// the entry. The cleanup `Removed` is only fired by the prompter when
    /// the entry was still present at cleanup time. When an external party
    /// (the HTTP `/respond` handler in production) already removed the
    /// entry, the prompter's cleanup is a no-op and the external remover is
    /// responsible for emitting `Removed`. This test exercises that
    /// double-emit-avoidance path; the timeout-driven `Removed` path is
    /// covered by `test_prompt_timeout_emits_removed`.
    #[tokio::test]
    async fn test_prompt_added_emitted_and_external_remove_skips_cleanup_removed() {
        // Subscribe BEFORE spawning the prompter so we don't miss the
        // synchronous Added event.
        let mut rx = prompt_events().subscribe();

        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = noop_prompter(pending.clone());
        let req = make_test_request("lifecycle?", vec!["Allow", "Deny"]);

        let pending_clone = pending.clone();
        let handle =
            tokio::spawn(async move { prompter.prompt(&req, "dkey-lc", webapp("cid-lc")).await });

        // Drain events until we see our specific Added (other tests in the
        // same process may fire concurrently against the global broadcast).
        let mut added_nonce: Option<String> = None;
        for _ in 0..50 {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Ok(PromptEvent::Added(snap))) if snap.delegate_key == "dkey-lc" => {
                    added_nonce = Some(snap.nonce);
                    break;
                }
                Ok(_) => continue, // unrelated event from another test, keep draining
                Err(_) => break,   // timeout: no event in window
            }
        }
        let nonce = added_nonce.expect("PromptEvent::Added with our delegate_key");

        // Respond so the prompter exits its timeout wait and runs cleanup.
        let (_, prompt) = pending_clone.remove(&nonce).unwrap();
        prompt.response_tx.send(0).unwrap();
        let _ = handle.await.unwrap();

        // Expect a matching Removed for that nonce. May be interleaved with
        // unrelated events.
        let mut saw_removed = false;
        for _ in 0..50 {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Ok(PromptEvent::Removed { nonce: n })) if n == nonce => {
                    saw_removed = true;
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        // Note: in this test the response handler ran inside the
        // pending_clone.remove path (we removed the entry ourselves via
        // pending_clone.remove). DashboardPrompter's cleanup remove
        // returned None (was_present=false) so it did NOT emit Removed.
        // That path is only fired when the prompter's own cleanup
        // actually removes the entry (timeout / channel-dropped paths).
        // The HTTP `/respond` handler fires Removed in the success
        // path; that's covered by the SSE endpoint integration tests.
        assert!(
            !saw_removed,
            "this test exercises the manual-remove path; \
             Removed should fire only from the prompter's own cleanup \
             (timeout) or the HTTP respond handler (covered elsewhere)"
        );
    }

    /// When the prompter's own timeout cleanup runs, it must emit Removed
    /// so SSE subscribers dismiss the overlay.
    #[tokio::test(start_paused = true)]
    async fn test_prompt_timeout_emits_removed() {
        let mut rx = prompt_events().subscribe();

        let pending: PendingPrompts = Arc::new(DashMap::new());
        let prompter = noop_prompter(pending.clone());
        let req = make_test_request("timeout?", vec!["Allow"]);

        let handle = tokio::spawn(async move {
            prompter
                .prompt(&req, "dkey-timeout", webapp("cid-timeout"))
                .await
        });

        // Capture the nonce of our specific Added event.
        let mut our_nonce: Option<String> = None;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            match rx.try_recv() {
                Ok(PromptEvent::Added(snap)) if snap.delegate_key == "dkey-timeout" => {
                    our_nonce = Some(snap.nonce);
                    break;
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => break,
            }
        }
        let nonce = our_nonce.expect("Added event for the timeout test");

        // Advance virtual time past the prompt timeout.
        tokio::time::advance(USER_INPUT_TIMEOUT + Duration::from_secs(1)).await;
        let _ = handle.await.unwrap();

        let mut saw_removed = false;
        for _ in 0..50 {
            match rx.try_recv() {
                Ok(PromptEvent::Removed { nonce: n }) if n == nonce => {
                    saw_removed = true;
                    break;
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => break,
            }
        }
        assert!(
            saw_removed,
            "prompter timeout must emit PromptEvent::Removed"
        );
    }

    // #3820: when a permission prompt is created and no dashboard tab is
    // connected to display it (zero SSE subscribers), the prompter must open
    // the standalone permission page in the user's browser so the prompt stays
    // actionable instead of silently auto-denying after the timeout. When a tab
    // IS connected it receives the prompt via SSE and the browser must NOT be
    // opened. We drive `maybe_alert_no_tab` directly with a recording notifier
    // because the prompt-event broadcast is a process-global shared across
    // tests, so its live `receiver_count()` isn't deterministic here.
    #[test]
    fn maybe_alert_no_tab_opens_browser_only_when_no_subscribers() {
        use std::sync::Mutex;

        let opened: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder: NoTabNotifier = {
            let opened = opened.clone();
            Arc::new(move |url: &str| opened.lock().unwrap().push(url.to_string()))
        };
        let prompter =
            DashboardPrompter::with_notifier(Arc::new(DashMap::new()), test_ws_addr(), recorder);

        // A tab is connected (>= 1 subscriber): do not open the browser.
        prompter.maybe_alert_no_tab("nonce-with-tab", 1);
        assert!(
            opened.lock().unwrap().is_empty(),
            "browser must not open when a dashboard tab is connected"
        );

        // No tab connected (0 subscribers): open the standalone permission page.
        prompter.maybe_alert_no_tab("nonce-no-tab", 0);
        assert_eq!(
            opened.lock().unwrap().as_slice(),
            ["http://127.0.0.1:7509/permission/nonce-no-tab"],
            "browser must open exactly the standalone permission page for the prompt"
        );
    }

    // The permission-page URL must use the configured ws-api port, not a
    // hardcoded 7509 — a node on a custom port still opens the right dashboard.
    #[test]
    fn maybe_alert_no_tab_url_uses_configured_port() {
        use std::sync::Mutex;

        let opened: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder: NoTabNotifier = {
            let opened = opened.clone();
            Arc::new(move |url: &str| opened.lock().unwrap().push(url.to_string()))
        };
        let prompter = DashboardPrompter::with_notifier(
            Arc::new(DashMap::new()),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 41234),
            recorder,
        );

        prompter.maybe_alert_no_tab("abc", 0);
        assert_eq!(
            opened.lock().unwrap().as_slice(),
            ["http://127.0.0.1:41234/permission/abc"]
        );
    }

    // A burst of no-tab prompts within NO_TAB_OPEN_COOLDOWN must open only one
    // browser tab, not one per prompt (which could reach MAX_PENDING_PROMPTS
    // tabs). Suppressed prompts fall back to the pre-existing auto-deny, so the
    // cooldown never makes a prompt worse off. After the window elapses, the
    // next no-tab prompt opens again.
    #[tokio::test(start_paused = true)]
    async fn maybe_alert_no_tab_cooldown_suppresses_burst() {
        use std::sync::Mutex;

        let opened: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder: NoTabNotifier = {
            let opened = opened.clone();
            Arc::new(move |url: &str| opened.lock().unwrap().push(url.to_string()))
        };
        let prompter =
            DashboardPrompter::with_notifier(Arc::new(DashMap::new()), test_ws_addr(), recorder);

        // First no-tab prompt opens a tab; the burst that follows (no virtual
        // time elapsed) is suppressed.
        prompter.maybe_alert_no_tab("a", 0);
        prompter.maybe_alert_no_tab("b", 0);
        prompter.maybe_alert_no_tab("c", 0);
        assert_eq!(
            opened.lock().unwrap().len(),
            1,
            "a burst within the cooldown window must open only one tab"
        );

        // Once the cooldown elapses, the next no-tab prompt opens again.
        tokio::time::advance(NO_TAB_OPEN_COOLDOWN + Duration::from_millis(1)).await;
        prompter.maybe_alert_no_tab("d", 0);
        assert_eq!(
            opened.lock().unwrap().len(),
            2,
            "a no-tab prompt after the cooldown window must open a tab"
        );
    }

    // #3820 (codex review): the dashboard URL must target the address the
    // ws-api server actually serves. Wildcard / loopback binds get a loopback
    // companion socket, so 127.0.0.1 is reachable and preferred; a specific
    // interface bind has no companion, so the URL must target that address
    // (with IPv6 literals bracketed).
    #[test]
    fn dashboard_authority_maps_bind_address_to_reachable_url_host() {
        use std::net::Ipv6Addr;

        // IPv6 wildcard is the default ws-api-address.
        assert_eq!(
            dashboard_authority(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 7509)),
            "127.0.0.1:7509"
        );
        assert_eq!(
            dashboard_authority(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7509)),
            "127.0.0.1:7509"
        );
        assert_eq!(
            dashboard_authority(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7509)),
            "127.0.0.1:7509"
        );
        assert_eq!(
            dashboard_authority(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 7509)),
            "127.0.0.1:7509"
        );
        // A specific interface bind has no loopback companion: target it.
        assert_eq!(
            dashboard_authority(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)),
                8000
            )),
            "192.168.1.5:8000"
        );
        // IPv6 literals must be bracketed in the URL authority.
        assert_eq!(
            dashboard_authority(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
                8000
            )),
            "[2001:db8::1]:8000"
        );
    }

    /// Source-level regression pin for the #3933 null-stdio rule applied to the
    /// #3820 browser-open helper.
    ///
    /// `open_permission_page_in_browser` is reachable from the contract
    /// executor task, which on Windows can run with no valid console (service /
    /// autostart) after `FreeConsole()`. The macOS/Linux `Command::spawn` for
    /// `open` / `xdg-open` MUST null all three standard handles or `spawn()`
    /// fails with "The handle is invalid" (os error 6) and the browser silently
    /// never opens. The failure is Windows-only and needs a detached console,
    /// so it can't be exercised portably; pin the source-level invariant
    /// instead. See #3933 / #3820.
    #[test]
    fn open_permission_page_spawn_must_null_all_three_standard_handles() {
        // The real definition appears earlier in the file than this test's
        // string literal of the same signature, so `split_once` anchors on it.
        let src = include_str!("user_input.rs");
        let (_, after_fn_start) = src
            .split_once("fn open_permission_page_in_browser(url: &str) {")
            .expect("open_permission_page_in_browser definition not found");
        // Restrict the search window to the function body: it ends at the next
        // top-level `#[cfg(test)]` item (the `impl DashboardPrompter` test
        // constructor that immediately follows the function).
        let body = after_fn_start
            .split_once("\n#[cfg(test)]")
            .map(|(b, _)| b)
            .unwrap_or(after_fn_start);
        for handle in ["stdin", "stdout", "stderr"] {
            let pattern = format!(".{handle}(std::process::Stdio::null())");
            assert!(
                body.contains(&pattern),
                "open_permission_page_in_browser must call `{pattern}` — without \
                 it, a Windows service/autostart node fails `Command::spawn()` \
                 with os error 6 (handle invalid after FreeConsole) and the \
                 permission page silently never opens. See #3933 / #3820."
            );
        }
    }
}
