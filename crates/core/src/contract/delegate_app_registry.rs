//! Registry mapping a delegate key to the client connections ("apps") that
//! talk to it, so notification-driven delegate invocations can route their
//! outbound [`ApplicationMessage`]s back to those apps.
//!
//! This is the mirror of [`crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS`]:
//! `DELEGATE_SUBSCRIPTIONS` maps `contract -> delegates` (which delegates want
//! to hear about a contract's state changes); this registry maps
//! `delegate -> apps` (which client connections should receive the delegate's
//! resulting `ApplicationMessage`s).
//!
//! # Why it exists (issue #3275)
//!
//! When a delegate receives a `ContractNotification` (via the subscription
//! pipeline from #2830 / PR #3251) it may produce
//! [`OutboundDelegateMsg::ApplicationMessage`] responses intended for connected
//! apps. But a notification-driven invocation has no originating client request
//! to respond to — the delegate ran because a *contract* changed, not because a
//! client asked. Before this registry those messages were logged and dropped.
//!
//! An app establishes a routing path simply by talking to the delegate over its
//! WebSocket connection: any `DelegateRequest` that carries a notification
//! channel (`subscription_listener`) registers `(client, sender)` under the
//! delegate key. From then on, notification-driven `ApplicationMessage`s for
//! that delegate are pushed to the app's channel.
//!
//! # Bounded-collection invariants (`.claude/rules/code-style.md`)
//!
//! Both the number of apps per delegate ([`MAX_APPS_PER_DELEGATE`]) and the
//! number of delegates a single client may register with
//! ([`MAX_DELEGATES_PER_CLIENT`]) are capped, rejecting at insertion. Without
//! caps a client could open unbounded channels or a delegate could accrue
//! unbounded fan-out targets — an amplification vector.
//!
//! # TTL / GC-exemption bound (AGENTS.md)
//!
//! Each registration records the [`tokio::time::Instant`] it was last
//! (re)confirmed. [`REGISTRATION_TTL`] bounds how long a registration survives
//! without the app talking to the delegate again; [`sweep_expired`] prunes
//! entries past the TTL. This guarantees the map cannot pin channels for a
//! disconnected-but-not-cleanly-closed client forever, satisfying the AGENTS.md
//! rule that any cleanup exemption be time-bounded. Clean disconnects
//! ([`remove_client`]) and delegate unregistration ([`remove_delegate`]) purge
//! eagerly; the TTL is the backstop.

use std::sync::LazyLock;

use dashmap::DashMap;
use freenet_stdlib::prelude::DelegateKey;
use tokio::sync::mpsc;

use crate::client_events::{ClientId, HostResult};

/// Maximum number of distinct client connections that may register with a
/// single delegate. Caps notification fan-out cost per delegate.
pub(crate) const MAX_APPS_PER_DELEGATE: usize = 128;

/// Maximum number of distinct delegates a single client may register with.
/// Prevents a resource-spreading attack where one client registers with many
/// delegates to hold many channels.
pub(crate) const MAX_DELEGATES_PER_CLIENT: usize = 256;

/// How long a registration survives without the app re-confirming it (by
/// talking to the delegate again). The registry is refreshed on every
/// `DelegateRequest` the app sends, so an actively-used app never expires; this
/// only reaps apps whose connection died without a clean disconnect event.
///
/// 30 minutes is comfortably longer than any reasonable gap between an app's
/// delegate interactions while keeping stale channels from lingering for the
/// process lifetime.
pub(crate) const REGISTRATION_TTL: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// One app's registration with a delegate: where to push messages, plus when it
/// was last confirmed (for TTL eviction).
struct AppRegistration {
    client_id: ClientId,
    sender: mpsc::Sender<HostResult>,
    /// `tokio::time::Instant` (not `std::time::Instant`) so tests using
    /// `tokio::time::pause` / `advance` can drive TTL eviction deterministically,
    /// matching the convention used by `DelegateContextEntry` and `RealTime`.
    last_seen: tokio::time::Instant,
}

/// `delegate -> [app registrations]`.
///
/// A `Vec` (not a map keyed by `ClientId`) because the expected cardinality is
/// small (one app, occasionally a few) and we iterate the whole list on every
/// notification fan-out anyway. Bounded by [`MAX_APPS_PER_DELEGATE`].
static DELEGATE_APPS: LazyLock<DashMap<DelegateKey, Vec<AppRegistration>>> =
    LazyLock::new(DashMap::default);

/// `client -> number of delegates it is registered with`, for O(1)
/// per-client cap enforcement without scanning [`DELEGATE_APPS`].
static CLIENT_REGISTRATION_COUNTS: LazyLock<DashMap<ClientId, usize>> =
    LazyLock::new(DashMap::default);

/// Register `client_id`'s notification channel with `delegate_key`, or refresh
/// its TTL if already registered.
///
/// Returns `false` (and does not register) when a per-key or per-client cap
/// would be exceeded, so the caller can log the rejection. Re-registering an
/// existing `(delegate, client)` pair always succeeds and refreshes both the
/// sender (the client may have reconnected with a fresh channel) and the TTL.
pub(crate) fn register_app(
    delegate_key: &DelegateKey,
    client_id: ClientId,
    sender: mpsc::Sender<HostResult>,
) -> bool {
    let now = tokio::time::Instant::now();
    let mut apps = DELEGATE_APPS.entry(delegate_key.clone()).or_default();

    // Refresh path: already registered → update sender + TTL, no cap change.
    if let Some(existing) = apps.iter_mut().find(|a| a.client_id == client_id) {
        existing.sender = sender;
        existing.last_seen = now;
        return true;
    }

    // New registration for this delegate: enforce per-key cap.
    if apps.len() >= MAX_APPS_PER_DELEGATE {
        tracing::warn!(
            delegate = %delegate_key,
            %client_id,
            cap = MAX_APPS_PER_DELEGATE,
            "Rejecting app registration: delegate at max apps"
        );
        return false;
    }

    // Enforce per-client cap.
    let mut count = CLIENT_REGISTRATION_COUNTS.entry(client_id).or_insert(0);
    if *count >= MAX_DELEGATES_PER_CLIENT {
        tracing::warn!(
            delegate = %delegate_key,
            %client_id,
            cap = MAX_DELEGATES_PER_CLIENT,
            "Rejecting app registration: client at max delegate registrations"
        );
        return false;
    }

    apps.push(AppRegistration {
        client_id,
        sender,
        last_seen: now,
    });
    *count += 1;
    true
}

/// Route a notification-driven `ApplicationMessage` (already wrapped in a
/// [`HostResult`]) to every app registered with `delegate_key`.
///
/// Uses `try_send` (never `.await`) so it is safe to call from the
/// single-threaded contract-handling loop — a full or closed client channel is
/// dropped/logged, never blocks the loop (see `.claude/rules/channel-safety.md`).
/// Returns the number of apps the message was delivered to. Closed channels are
/// pruned as a side effect (the client is gone).
pub(crate) fn route_to_apps(delegate_key: &DelegateKey, message: HostResult) -> usize {
    let Some(mut apps) = DELEGATE_APPS.get_mut(delegate_key) else {
        return 0;
    };

    let mut delivered = 0usize;
    let mut closed_clients: Vec<ClientId> = Vec::new();

    apps.retain(|app| match app.sender.try_send(message.clone()) {
        Ok(()) => {
            delivered += 1;
            true
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(
                delegate = %delegate_key,
                client_id = %app.client_id,
                "App notification channel full — delegate ApplicationMessage dropped"
            );
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            // Client disconnected without a clean event; drop the registration.
            closed_clients.push(app.client_id);
            false
        }
    });

    let now_empty = apps.is_empty();
    drop(apps);

    for client_id in closed_clients {
        decrement_client_count(client_id);
    }
    if now_empty {
        DELEGATE_APPS.remove_if(delegate_key, |_, v| v.is_empty());
    }

    delivered
}

/// Remove every registration for `client_id` (clean disconnect).
pub(crate) fn remove_client(client_id: ClientId) {
    let mut removed_any = false;
    DELEGATE_APPS.retain(|_, apps| {
        let before = apps.len();
        apps.retain(|a| a.client_id != client_id);
        removed_any |= apps.len() != before;
        !apps.is_empty()
    });
    if removed_any {
        CLIENT_REGISTRATION_COUNTS.remove(&client_id);
    }
}

/// Remove all app registrations for `delegate_key` (delegate unregistered).
pub(crate) fn remove_delegate(delegate_key: &DelegateKey) {
    if let Some((_, apps)) = DELEGATE_APPS.remove(delegate_key) {
        for app in apps {
            decrement_client_count(app.client_id);
        }
    }
}

/// Prune registrations older than [`REGISTRATION_TTL`]. The TTL backstop for
/// clients that vanished without a clean disconnect or channel-close signal.
pub(crate) fn sweep_expired() {
    let now = tokio::time::Instant::now();
    let mut expired: Vec<ClientId> = Vec::new();
    DELEGATE_APPS.retain(|_, apps| {
        apps.retain(|a| {
            let keep = now.saturating_duration_since(a.last_seen) < REGISTRATION_TTL;
            if !keep {
                expired.push(a.client_id);
            }
            keep
        });
        !apps.is_empty()
    });
    for client_id in expired {
        decrement_client_count(client_id);
    }
}

fn decrement_client_count(client_id: ClientId) {
    if let Some(mut count) = CLIENT_REGISTRATION_COUNTS.get_mut(&client_id) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            drop(count);
            CLIENT_REGISTRATION_COUNTS.remove_if(&client_id, |_, v| *v == 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // The registry is a process-global (mirroring DELEGATE_SUBSCRIPTIONS), so
    // these unit tests run against SHARED state under plain `cargo test`'s
    // in-process parallelism. Each test therefore carves out its OWN key/id
    // namespace from a global counter instead of relying on clear_for_test()
    // for isolation (which would race with concurrently-running tests). Keys
    // and ClientIds from different tests never collide, so parallel execution
    // is safe without serialization.
    static NS: AtomicUsize = AtomicUsize::new(1);

    /// A block of 2^16 distinct delegate keys + client ids private to one test.
    struct Namespace(usize);
    impl Namespace {
        fn new() -> Self {
            Namespace(NS.fetch_add(1, Ordering::Relaxed))
        }
        fn key(&self, n: usize) -> DelegateKey {
            let mut bytes = [0u8; 32];
            bytes[0..8].copy_from_slice(&(self.0 as u64).to_le_bytes());
            bytes[8..16].copy_from_slice(&(n as u64).to_le_bytes());
            DelegateKey::new(bytes, CodeHash::new(bytes))
        }
        fn client(&self, n: usize) -> ClientId {
            // 16 low bits for the per-test index, rest for the namespace.
            ClientId((self.0 << 24) | (n & 0xFF_FFFF))
        }
    }

    fn host_msg() -> HostResult {
        use freenet_stdlib::client_api::HostResponse;
        use freenet_stdlib::prelude::{ApplicationMessage, OutboundDelegateMsg};
        Ok(HostResponse::DelegateResponse {
            key: DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32])),
            values: vec![OutboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(vec![1]),
            )],
        })
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn register_and_route_delivers_to_app() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (tx, mut rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(&dk, ns.client(0), tx));

        let delivered = route_to_apps(&dk, host_msg());
        assert_eq!(delivered, 1);
        assert!(rx.try_recv().is_ok(), "app must receive the message");
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn route_to_unknown_delegate_delivers_nothing() {
        let ns = Namespace::new();
        let delivered = route_to_apps(&ns.key(0), host_msg());
        assert_eq!(delivered, 0);
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn per_delegate_cap_rejects_excess_apps() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        for i in 0..MAX_APPS_PER_DELEGATE {
            let (tx, _rx) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&dk, ns.client(i), tx));
        }
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(&dk, ns.client(MAX_APPS_PER_DELEGATE), tx),
            "registration past per-delegate cap must be rejected"
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn per_client_cap_rejects_excess_delegates() {
        let ns = Namespace::new();
        let client = ns.client(0);
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (tx, _rx) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i), client, tx));
        }
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(&ns.key(MAX_DELEGATES_PER_CLIENT), client, tx),
            "registration past per-client cap must be rejected"
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn remove_client_frees_registrations() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, client, tx));
        remove_client(client);
        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        // Count freed, so client can register up to the cap again.
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (tx, _rx) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i + 1), client, tx));
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn closed_channel_is_pruned_on_route() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (tx, rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, client, tx));
        drop(rx); // client gone
        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        // Registration pruned and client count freed: can fill cap again.
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (t, _r) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i + 1), client, t));
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn ttl_sweep_evicts_stale_registration() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, ns.client(0), tx));

        tokio::time::advance(REGISTRATION_TTL + std::time::Duration::from_secs(1)).await;
        sweep_expired();
        assert_eq!(
            route_to_apps(&dk, host_msg()),
            0,
            "stale registration must be swept after TTL"
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn reregister_refreshes_ttl() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (tx, mut rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(&dk, client, tx.clone()));

        // Advance nearly to TTL, then refresh.
        tokio::time::advance(REGISTRATION_TTL - std::time::Duration::from_secs(10)).await;
        assert!(register_app(&dk, client, tx));
        // Advance past the ORIGINAL expiry but within the refreshed window.
        tokio::time::advance(std::time::Duration::from_secs(20)).await;
        sweep_expired();
        assert_eq!(
            route_to_apps(&dk, host_msg()),
            1,
            "refreshed registration must survive"
        );
        assert!(rx.try_recv().is_ok());
    }
}
