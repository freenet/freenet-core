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
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey};
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
/// Who made a registration, from the perspective that decides whether it may
/// receive a delegate's notification output.
///
/// Registrations are a push channel for a delegate's output, and that output is
/// the delegate's app's data. Before GHSA-824h-7x5x-wfmf there was no such
/// distinction and [`route_to_apps`] fanned every notification to EVERY
/// registration, so any client that sent one `ApplicationMessages` request to a
/// delegate received its subsequent output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppIdentity {
    /// Registered over a connection the node proved is local. Eligible to
    /// receive the delegate's output.
    ///
    /// `attested` is the connection's app identity where it had one (`None` for
    /// the tokenless local CLI shape — riverctl, atlasctl, fdev). It is carried
    /// for diagnostics ONLY and is deliberately NOT part of the delivery
    /// decision: see [`route_to_apps`] for why matching on it is unsafe.
    Local {
        attested: Option<ContractInstanceId>,
    },
    /// Not provably local. Recorded so the caps and TTL bookkeeping stay
    /// identical, but never a delivery target.
    Remote,
}

impl AppIdentity {
    fn may_receive(self) -> bool {
        matches!(self, Self::Local { .. })
    }
}

/// One app's registration with a delegate: where to push messages, who
/// registered it, plus when it was last confirmed (for TTL eviction).
struct AppRegistration {
    client_id: ClientId,
    identity: AppIdentity,
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
    identity: AppIdentity,
    sender: mpsc::Sender<HostResult>,
) -> bool {
    let now = tokio::time::Instant::now();
    let mut apps = DELEGATE_APPS.entry(delegate_key.clone()).or_default();

    // Refresh path: already registered → update sender + TTL, no cap change.
    // The origin is refreshed too: a client id is per-connection, so this is the
    // same connection and therefore the same (immutable) attested identity — but
    // re-storing it keeps the field from going stale if that ever changes.
    if let Some(existing) = apps.iter_mut().find(|a| a.client_id == client_id) {
        existing.sender = sender;
        existing.identity = identity;
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
        identity,
        sender,
        last_seen: now,
    });
    *count += 1;
    true
}

/// Route a notification-driven `ApplicationMessage` (already wrapped in a
/// [`HostResult`]) to every LOCAL registration for `delegate_key`.
///
/// A registration from a connection the node could not prove is local is
/// skipped: before GHSA-824h-7x5x-wfmf, sending a delegate one
/// `ApplicationMessages` request subscribed you to its output, so any off-host
/// caller could harvest a local app's delegate traffic.
///
/// # Why this does NOT match on the app's attested identity
///
/// The obvious-looking rule — deliver only to the app whose identity equals the
/// delegate's own — was implemented and REVERTED, because the only durable
/// notion of "the delegate's own identity" is its first-registration origin
/// record, which is first-writer-wins and IMMUTABLE (`redb.rs`). Matching on it
/// would have created two failures strictly worse than the bug being fixed:
///
///  * **Permanent, remotely-triggerable denial of service.** Anyone who can
///    register a delegate first fixes that record forever. Delegate WASM and
///    params are public, so the key is derivable; a poisoned record would make
///    every later notification to the legitimate app be dropped, with no way to
///    correct it short of wiping the node's database.
///  * **Breakage on re-key.** An app that re-keys (River does so routinely) gets
///    a new contract id, which can never match the frozen record.
///
/// Locality is the boundary this fix can actually enforce, so it is the only one
/// used here. The consequence is stated plainly rather than papered over:
/// **unattested local clients are not separated from each other.** A local
/// process that talks to a delegate still receives that delegate's output, as it
/// always has. "Local" means this HOST, not this USER — closing that needs a
/// real per-client authorization model, not a routing filter.
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

    apps.retain(|app| {
        // Identity mismatch: not a delivery target. Keep the registration (it is
        // still a live client, just not one entitled to THIS delegate's output);
        // only closed channels are pruned.
        // Not provably local: not a delivery target. Keep the registration (it
        // is still a live client) — only closed channels are pruned.
        //
        // `info!`, not `debug!`: the crate sets `release_max_level_info`, so a
        // `debug!` here would compile out of every shipped binary and an
        // operator whose app silently stopped receiving notifications would have
        // nothing to look at.
        if !app.identity.may_receive() {
            tracing::info!(
                delegate = %delegate_key,
                client_id = %app.client_id,
                "Withholding delegate notification from a non-local registration \
                 (GHSA-824h-7x5x-wfmf); the client is connected but off-host"
            );
            return true;
        }
        match app.sender.try_send(message.clone()) {
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
        assert!(register_app(&dk, ns.client(0), local(None), tx));

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
            assert!(register_app(&dk, ns.client(i), local(None), tx));
        }
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(&dk, ns.client(MAX_APPS_PER_DELEGATE), local(None), tx),
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
            assert!(register_app(&ns.key(i), client, local(None), tx));
        }
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(&ns.key(MAX_DELEGATES_PER_CLIENT), client, local(None), tx),
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
        assert!(register_app(&dk, client, local(None), tx));
        remove_client(client);
        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        // Count freed, so client can register up to the cap again.
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (tx, _rx) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i + 1), client, local(None), tx));
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn closed_channel_is_pruned_on_route() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (tx, rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, client, local(None), tx));
        drop(rx); // client gone
        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        // Registration pruned and client count freed: can fill cap again.
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (t, _r) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i + 1), client, local(None), t));
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn ttl_sweep_evicts_stale_registration() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, ns.client(0), local(None), tx));

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
        assert!(register_app(&dk, client, local(None), tx.clone()));

        // Advance nearly to TTL, then refresh.
        tokio::time::advance(REGISTRATION_TTL - std::time::Duration::from_secs(10)).await;
        assert!(register_app(&dk, client, local(None), tx));
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

    fn local(attested: Option<ContractInstanceId>) -> AppIdentity {
        AppIdentity::Local { attested }
    }

    fn contract(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    // -----------------------------------------------------------------------
    // GHSA-824h-7x5x-wfmf, gap 2 (delegate app registry).
    //
    // Registration is open to anyone — it is how a client asks to be pushed to
    // — and it happens BEFORE the request is dispatched, so it cannot be gated
    // on the delegate accepting the caller. The gate has to be on DELIVERY.
    //
    // Delivery keys on LOCALITY, not on the app's attested identity. An earlier
    // revision matched the registration's origin against the delegate's durable
    // first-registration record and was reverted: that record is
    // first-writer-wins and immutable, so matching on it created a permanent,
    // remotely-triggerable denial of service and broke apps that re-key. See
    // `route_to_apps`'s rustdoc.
    // -----------------------------------------------------------------------

    /// The core regression: an off-host client that talked to the delegate
    /// receives nothing, while local clients still do. Before the fix both were
    /// delivered to.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn route_skips_non_local_registrations() {
        let ns = Namespace::new();
        let dk = ns.key(0);

        let (app_tx, mut app_rx) = mpsc::channel::<HostResult>(4);
        let (cli_tx, mut cli_rx) = mpsc::channel::<HostResult>(4);
        let (remote_tx, mut remote_rx) = mpsc::channel::<HostResult>(4);

        assert!(register_app(
            &dk,
            ns.client(0),
            local(Some(contract(0x11))),
            app_tx
        ));
        assert!(register_app(&dk, ns.client(1), local(None), cli_tx));
        assert!(register_app(
            &dk,
            ns.client(2),
            AppIdentity::Remote,
            remote_tx
        ));

        let delivered = route_to_apps(&dk, host_msg());

        assert_eq!(delivered, 2, "both local registrations must receive it");
        assert!(
            app_rx.try_recv().is_ok(),
            "the local web app must receive it"
        );
        assert!(
            cli_rx.try_recv().is_ok(),
            "the local tokenless CLI must keep receiving it — riverctl, atlasctl \
             and fdev all register in this shape"
        );
        assert!(
            remote_rx.try_recv().is_err(),
            "an off-host registration must never receive a delegate's output"
        );
    }

    /// A skipped registration is retained, not evicted: the client is still live
    /// and may hold registrations with other delegates.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn skipped_remote_registration_is_retained_not_evicted() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (remote_tx, _remote_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(
            &dk,
            ns.client(0),
            AppIdentity::Remote,
            remote_tx
        ));

        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        assert!(
            DELEGATE_APPS.get(&dk).is_some_and(|apps| apps.len() == 1),
            "the registration must survive a skipped delivery"
        );
    }

    /// Delivery must NOT depend on the attested contract id. Two local apps with
    /// different attested identities both receive the output — the deliberate
    /// scope limit (locality, not per-app separation), and the reason the
    /// immutable-record matching was reverted.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn delivery_does_not_key_on_the_attested_contract_id() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (a_tx, mut a_rx) = mpsc::channel::<HostResult>(4);
        let (b_tx, mut b_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(
            &dk,
            ns.client(0),
            local(Some(contract(0x22))),
            a_tx
        ));
        assert!(register_app(
            &dk,
            ns.client(1),
            local(Some(contract(0x33))),
            b_tx
        ));

        assert_eq!(route_to_apps(&dk, host_msg()), 2);
        assert!(a_rx.try_recv().is_ok());
        assert!(b_rx.try_recv().is_ok());
    }
}
