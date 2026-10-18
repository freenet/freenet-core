//! Registry mapping a delegate key to the client connections ("apps") that
//! talk to it, so notification-driven delegate invocations can route their
//! outbound [`ApplicationMessage`]s back to those apps.
//!
//! This is the mirror of [`crate::wasm_runtime::delegate_subscriptions`]:
//! that registry maps `contract -> delegates` (which delegates want
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
//! # Lifetime: a registration never outlives its connection
//!
//! A registration is removed when its connection goes away (or the delegate is
//! unregistered), never because it has been idle. Clean disconnects
//! ([`remove_client`], which every exit of the WebSocket connection task's main
//! loop triggers) and delegate unregistration ([`remove_delegate`]) purge
//! eagerly. The backstop for a connection that ended
//! without that event (a cancelled or panicked task) is the channel itself: the
//! receiving end lives in the connection task's listener map, so once the task
//! is gone the sender reports closed, and [`sweep_closed`] and [`route_to_apps`]
//! prune it. Two tests in `client_events::websocket` pin that the task drops
//! those receivers when the client goes away, with a close frame
//! (`client_disconnect_drops_listeners_and_notifies_the_node`) and without one
//! (`client_vanishing_without_close_frame_drops_listeners`).
//!
//! This is the lifecycle-event bound the AGENTS.md cleanup rule allows: the
//! bound is the connection's lifetime, not a TTL. That lifetime is NOT bounded
//! in time by anything in the node:
//!
//!  * A peer that vanished without closing is cut off by the kernel. The
//!    connection task pings every 30 s, the ping goes unacknowledged, and the
//!    TCP retransmission timeout (about 15 minutes on Linux defaults) errors the
//!    socket. Behind a reverse proxy the node's TCP peer is the proxy, so this
//!    depends on the proxy tearing down its upstream.
//!  * A peer whose TCP stack stays up keeps its connection for as long as it
//!    likes, even if it never reads: there is no WebSocket pong or idle
//!    deadline, and a ping send into a full socket buffer waits indefinitely
//!    (#5922 tracks bounding this at the connection).
//!
//! That is acceptable because the registration is not what such a peer holds
//! open. Its connection, task, socket buffers and contract subscriptions live
//! exactly as long, with or without this registry. A registration adds that
//! request's notification channel and its listener entry, within the caps
//! below. What the registry must never do is keep a registration usable past
//! its connection, and it does not: once the connection is gone the channel is
//! closed, so the registration can deliver nothing, and it is pruned by the
//! next new registration for its delegate, sweep, or delivery attempt.
//!
//! An earlier revision expired a registration 30 minutes after the app's last
//! `ApplicationMessages` request. That evicted exactly the legitimate case, an
//! app that registers once and then only listens, which silently stopped
//! receiving pushes while still connected. It bounded nothing an abuser cares
//! about either: re-sending one request restamped the TTL, so anyone could
//! already hold slots indefinitely. Registry state is bounded per delegate and
//! per connection by the caps below, not by age.
//!
//! One consequence is a real trade-off rather than a free win: a slot in
//! [`MAX_APPS_PER_DELEGATE`] or [`MAX_DELEGATES_PER_CLIENT`] is now held for
//! the life of the connection rather than for 30 minutes after its last
//! request, and both caps refuse newcomers. Behind a colocated reverse proxy
//! every client is `Local` (see `ConnectionScope::from_source_ip`), so
//! [`LOCAL_RESERVED_SLOTS`] does not help there, and the 129th concurrent
//! connection to one delegate is refused until one of the 128 disconnects. The
//! refusal is silent to the app: its direct replies still arrive, but it gets
//! no pushes. Registration also happens before the executor checks that the
//! delegate exists, so a request to a key that is not installed holds a
//! per-client slot for the connection's life too. Under the TTL a newcomer got
//! a slot only once an idle incumbent had been silently cut off, which is the
//! bug this replaced.
//!
//! This is a deliberate exception to the rule in `.claude/rules/code-style.md`
//! and `bug-prevention-patterns.md` that a cap over entries that do not age out
//! must evict rather than refuse. Evicting here means silently cutting off a
//! connected app, which is this bug. The cap is kept honest instead by never
//! counting a dead registration: [`register_app`] prunes closed channels for
//! the delegate before applying it.

use std::sync::LazyLock;

use dashmap::DashMap;
use freenet_stdlib::prelude::DelegateKey;
use tokio::sync::mpsc;

use crate::client_events::{ClientId, HostResult};

/// Maximum number of distinct client connections that may register with a
/// single delegate. Caps notification fan-out cost per delegate.
pub(crate) const MAX_APPS_PER_DELEGATE: usize = 128;

/// Slots within [`MAX_APPS_PER_DELEGATE`] that only LOCAL registrations may
/// occupy.
///
/// Without this, a pre-existing denial of service is reachable purely off-host
/// and needs no unclean disconnect: an off-host caller simply HOLDS
/// `MAX_APPS_PER_DELEGATE` connections open, sends one `ApplicationMessages` on
/// each, and every slot is filled by live `Remote` registrations that
/// [`route_to_apps`] will never deliver to. The legitimate LOCAL app is then
/// refused at the cap indefinitely. The dead-channel prune does not help,
/// because nothing here is dead.
///
/// Capping `Remote` occupancy below the total leaves this many slots that only a
/// local client can take, so the local app can always register over off-host
/// clients (behind a colocated reverse proxy every client is local; see the
/// module docs). 32 is far above
/// the real local count (usually one app, occasionally a few) while still
/// leaving 96 for off-host clients, which register successfully and simply
/// receive no pushed output.
pub(crate) const LOCAL_RESERVED_SLOTS: usize = 32;

/// Maximum number of distinct delegates a single client may register with.
/// Prevents a resource-spreading attack where one client registers with many
/// delegates to hold many channels.
pub(crate) const MAX_DELEGATES_PER_CLIENT: usize = 256;

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
    /// Deliberately carries NO contract id. An earlier revision kept the
    /// connection's attested app identity here "for diagnostics", but nothing
    /// read it — a field justified as diagnostic that nothing diagnoses is dead
    /// weight that also invites the next reader to gate on it, which is exactly
    /// what [`route_to_apps`] explains must not happen.
    Local,
    /// Not provably local. Recorded so the caps and pruning stay identical,
    /// but never a delivery target.
    Remote,
}

impl AppIdentity {
    fn may_receive(self) -> bool {
        matches!(self, Self::Local)
    }
}

/// One app's registration with a delegate: where to push messages and who
/// registered it.
struct AppRegistration {
    client_id: ClientId,
    identity: AppIdentity,
    /// Whether this registration has already reported being withheld from
    /// delivery. Read and set ONLY through
    /// [`AppRegistration::latch_withhold_report`], so the report stays
    /// once-per-registration rather than once-per-notification. See
    /// [`route_to_apps`].
    withhold_logged: bool,
    /// Whether this registration has already reported a full channel since its
    /// last successful delivery. Set ONLY through
    /// [`AppRegistration::latch_full_report`] and cleared on delivery. See
    /// [`route_to_apps`].
    full_logged: bool,
    sender: mpsc::Sender<HostResult>,
}

impl AppRegistration {
    /// Claim the right to emit this registration's withheld-from-delivery
    /// report: `true` exactly once, `false` on every later call.
    ///
    /// A method rather than an inline `if` on purpose. The guard and the flag
    /// write belong together — inlined, deleting the `if` while keeping the
    /// assignment still sets the flag, so a test that inspects the flag passes
    /// while the line has silently become once-per-notification. Bundled here,
    /// that edit instead leaves this method uncalled, which is a `dead_code`
    /// error under the crate's `-D warnings` build, and the returned sequence is
    /// directly testable.
    fn latch_withhold_report(&mut self) -> bool {
        if self.withhold_logged {
            return false;
        }
        self.withhold_logged = true;
        true
    }

    /// Claim the right to report this registration's channel as full: `true`
    /// once, then `false` until a delivery succeeds and clears the flag. A
    /// method for the same reason as [`Self::latch_withhold_report`].
    fn latch_full_report(&mut self) -> bool {
        if self.full_logged {
            return false;
        }
        self.full_logged = true;
        true
    }
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

/// Register `client_id`'s notification channel with `delegate_key`, or replace
/// its channel if already registered.
///
/// Returns `false` (and does not register) when the per-key, off-host or
/// per-client cap would be exceeded, so the caller can log the rejection. Re-registering an
/// existing `(delegate, client)` pair always succeeds and replaces the sender:
/// each request carries a fresh channel, and the one it replaces is dropped.
pub(crate) fn register_app(
    delegate_key: &DelegateKey,
    client_id: ClientId,
    identity: AppIdentity,
    sender: mpsc::Sender<HostResult>,
) -> bool {
    let mut apps = DELEGATE_APPS.entry(delegate_key.clone()).or_default();

    // Refresh path: already registered → update sender, no cap change.
    if let Some(existing) = apps.iter_mut().find(|a| a.client_id == client_id) {
        existing.sender = sender;
        existing.identity = identity;
        // `full_logged` is left alone: the fresh channel has room (it holds at
        // most the request's own reply), so the next delivery succeeds and
        // clears it, and the next stall on the new channel is reported.
        // A refresh is the same connection, so do NOT re-arm the withhold log:
        // an app that talks to a delegate in a loop would otherwise defeat the
        // once-per-registration bound.
        return true;
    }

    // A slot held by a dead registration must not count against the caps
    // below. Without this, a connection that ended without a clean disconnect
    // keeps refusing newcomers until the next notification-path sweep, which
    // runs only when some delegate on the node is notified and so may never
    // come on a quiet node. Only this delegate's list is pruned. O(n) over a
    // list bounded by `MAX_APPS_PER_DELEGATE`, on a path that already scans it.
    let mut closed: Vec<ClientId> = Vec::new();
    apps.retain(|a| {
        let keep = !a.sender.is_closed();
        if !keep {
            closed.push(a.client_id);
        }
        keep
    });
    for dead in closed {
        decrement_client_count(dead);
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

    // LOCAL SLOT RESERVATION (see `LOCAL_RESERVED_SLOTS`). Off-host
    // registrations are capped BELOW the total, so holding connections open can
    // never crowd the local app out of the registry. Counted over `Remote`
    // entries only, so a delegate legitimately used by many local clients is
    // unaffected. O(n) over a list bounded by `MAX_APPS_PER_DELEGATE`, on a path
    // that already scans it for the refresh check above.
    if !identity.may_receive() {
        let remote_occupancy = apps.iter().filter(|a| !a.identity.may_receive()).count();
        if remote_occupancy >= MAX_APPS_PER_DELEGATE - LOCAL_RESERVED_SLOTS {
            tracing::warn!(
                delegate = %delegate_key,
                %client_id,
                cap = MAX_APPS_PER_DELEGATE - LOCAL_RESERVED_SLOTS,
                "Rejecting non-local app registration: at the off-host cap, which \
                 exists so a local app can always register (GHSA-824h-7x5x-wfmf)"
            );
            return false;
        }
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
        // Do not leave behind an empty entry for a delegate this client was
        // refused on (one `or_default` just created, or one the prune above
        // emptied): nothing else removes it until the next notification-path
        // sweep. `remove_if` re-checks emptiness under the shard lock, so a
        // concurrent registration that landed in between is kept.
        drop(count);
        let now_empty = apps.is_empty();
        drop(apps);
        if now_empty {
            DELEGATE_APPS.remove_if(delegate_key, |_, v| v.is_empty());
        }
        return false;
    }

    apps.push(AppRegistration {
        client_id,
        identity,
        withhold_logged: false,
        full_logged: false,
        sender,
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
/// single-threaded contract-handling loop: a message to a full channel is
/// dropped (the registration is kept, and the stall is logged once per run of
/// full sends), never blocking the loop (see `.claude/rules/channel-safety.md`).
/// Returns the number of apps the message was delivered to. Closed channels are
/// pruned as a side effect (the client is gone).
pub(crate) fn route_to_apps(delegate_key: &DelegateKey, message: HostResult) -> usize {
    let Some(mut apps) = DELEGATE_APPS.get_mut(delegate_key) else {
        return 0;
    };

    let mut delivered = 0usize;
    let mut closed_clients: Vec<ClientId> = Vec::new();

    apps.retain_mut(|app| {
        if !app.identity.may_receive() {
            // PRUNE BEFORE SKIP (GHSA-824h-7x5x-wfmf).
            //
            // Before this fix the `try_send` below was what reaped
            // registrations whose client had vanished without a clean
            // disconnect. Skipping ahead of it would mean a non-local
            // registration is only reaped by the next `sweep_closed` or
            // registration for this delegate.
            // `is_closed()` gives the same liveness signal `try_send` gave,
            // without delivering anything.
            //
            // SCOPE OF THIS PRUNE, precisely: it reaps CLOSED senders only. It
            // does NOT close the slot-exhaustion DoS, because the cheaper attack
            // holds its connections OPEN and nothing here is dead.
            // `LOCAL_RESERVED_SLOTS` is what makes that attack a non-problem for
            // off-host connections (see its docs for the proxy caveat); the
            // two are complementary and neither substitutes for the other.
            if app.sender.is_closed() {
                closed_clients.push(app.client_id);
                return false;
            }
            // Report ONCE per registration, not once per message. This line
            // ships (`release_max_level_info`) and the fan-out runs per
            // contract-state change, so a per-message log would let an attacker
            // turn every room update into one line per parked connection. The
            // flag lives on the registration, so `MAX_APPS_PER_DELEGATE` bounds
            // the total.
            if app.latch_withhold_report() {
                tracing::info!(
                    delegate = %delegate_key,
                    client_id = %app.client_id,
                    "Withholding delegate notifications from a non-local \
                     registration (GHSA-824h-7x5x-wfmf); the client is connected \
                     but off-host. Logged once per registration."
                );
            }
            return true;
        }
        match app.sender.try_send(message.clone()) {
            Ok(()) => {
                app.full_logged = false;
                delivered += 1;
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Once per run of full sends, not once per message. A
                // connection that is alive but not reading keeps its
                // registration for as long as it stays connected (module docs),
                // and this fan-out runs per contract-state change, so a
                // per-message line would repeat for that whole time.
                if app.latch_full_report() {
                    tracing::warn!(
                        delegate = %delegate_key,
                        client_id = %app.client_id,
                        "App notification channel full — delegate ApplicationMessages \
                         dropped until the app drains it. Logged once per run of \
                         full sends."
                    );
                }
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

/// Prune every registration whose channel is closed, across all delegates.
///
/// The backstop for connections that ended without a clean disconnect event.
/// [`route_to_apps`] prunes the same way, but only for the delegate it is
/// delivering for; this covers delegates that are not being routed to. An open
/// channel is never pruned, however long it has been idle: see the module docs.
pub(crate) fn sweep_closed() {
    let mut closed: Vec<ClientId> = Vec::new();
    DELEGATE_APPS.retain(|_, apps| {
        apps.retain(|a| {
            let keep = !a.sender.is_closed();
            if !keep {
                closed.push(a.client_id);
            }
            keep
        });
        !apps.is_empty()
    });
    for client_id in closed {
        decrement_client_count(client_id);
    }
}

/// Number of registrations for `delegate_key`, without pruning anything (unlike
/// [`route_to_apps`], which would reap a closed registration itself and so
/// cannot observe whether something else did).
#[cfg(test)]
pub(crate) fn registration_count(delegate_key: &DelegateKey) -> usize {
    DELEGATE_APPS.get(delegate_key).map_or(0, |apps| apps.len())
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

    // The registry is a process-global (mirroring delegate_subscriptions), so
    // these unit tests run against SHARED state under plain `cargo test`'s
    // in-process parallelism. Each test therefore carves out its OWN key/id
    // namespace from a global counter instead of relying on clear_for_test()
    // for isolation (which would race with concurrently-running tests). Keys
    // and ClientIds from different tests never collide, so parallel execution
    // is safe without serialization.
    static NS: AtomicUsize = AtomicUsize::new(1);

    /// A block of 2^24 distinct delegate keys + client ids private to one test.
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
            // 24 low bits for the per-test index, rest for the namespace.
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
        assert!(register_app(&dk, ns.client(0), AppIdentity::Local, tx));

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
        // Receivers held for the whole test: a closed registration is fair game
        // for any concurrent test's `sweep_closed`, which would free a slot.
        let mut keepalive = Vec::new();
        for i in 0..MAX_APPS_PER_DELEGATE {
            let (tx, rx) = mpsc::channel::<HostResult>(1);
            keepalive.push(rx);
            assert!(register_app(&dk, ns.client(i), AppIdentity::Local, tx));
        }
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(
                &dk,
                ns.client(MAX_APPS_PER_DELEGATE),
                AppIdentity::Local,
                tx
            ),
            "registration past per-delegate cap must be rejected"
        );
    }

    /// A dead registration does not hold a slot against a newcomer, even with
    /// no notification-path sweep in between. Slots are held for a
    /// connection's life, so a dead one left in place would refuse newcomers
    /// until some unrelated notification happened to sweep it.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn per_delegate_cap_does_not_count_dead_registrations() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let mut keepalive = Vec::new();
        for i in 0..MAX_APPS_PER_DELEGATE {
            let (tx, rx) = mpsc::channel::<HostResult>(1);
            keepalive.push(rx);
            assert!(register_app(&dk, ns.client(i), AppIdentity::Local, tx));
        }
        // One incumbent's connection ends without a clean disconnect.
        drop(keepalive.pop());

        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            register_app(
                &dk,
                ns.client(MAX_APPS_PER_DELEGATE),
                AppIdentity::Local,
                tx
            ),
            "a dead registration must not hold a slot against a newcomer"
        );
        assert_eq!(registration_count(&dk), MAX_APPS_PER_DELEGATE);
        assert!(
            CLIENT_REGISTRATION_COUNTS
                .get(&ns.client(MAX_APPS_PER_DELEGATE - 1))
                .is_none_or(|c| *c == 0),
            "the pruned client's registration count must be released"
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn per_client_cap_rejects_excess_delegates() {
        let ns = Namespace::new();
        let client = ns.client(0);
        // Held for the whole test, as in `per_delegate_cap_rejects_excess_apps`.
        let mut keepalive = Vec::new();
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (tx, rx) = mpsc::channel::<HostResult>(1);
            keepalive.push(rx);
            assert!(register_app(&ns.key(i), client, AppIdentity::Local, tx));
        }
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(
                &ns.key(MAX_DELEGATES_PER_CLIENT),
                client,
                AppIdentity::Local,
                tx
            ),
            "registration past per-client cap must be rejected"
        );
        assert!(
            DELEGATE_APPS
                .get(&ns.key(MAX_DELEGATES_PER_CLIENT))
                .is_none(),
            "a refused registration must not leave an empty delegate entry behind"
        );

        // ...but the cleanup is for EMPTY entries only: refusing this client on
        // a delegate that holds another client's live registration must leave
        // that registration alone.
        let shared = ns.key(MAX_DELEGATES_PER_CLIENT + 1);
        let (other_tx, mut other_rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(
            &shared,
            ns.client(1),
            AppIdentity::Local,
            other_tx
        ));
        let (tx, _rx2) = mpsc::channel::<HostResult>(1);
        assert!(!register_app(&shared, client, AppIdentity::Local, tx));
        assert_eq!(registration_count(&shared), 1);
        assert_eq!(route_to_apps(&shared, host_msg()), 1);
        assert!(other_rx.try_recv().is_ok());
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn remove_client_frees_registrations() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, client, AppIdentity::Local, tx));
        remove_client(client);
        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        // Count freed, so client can register up to the cap again.
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (tx, _rx) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i + 1), client, AppIdentity::Local, tx));
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn closed_channel_is_pruned_on_route() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (tx, rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, client, AppIdentity::Local, tx));
        drop(rx); // client gone
        assert_eq!(route_to_apps(&dk, host_msg()), 0);
        // Registration pruned and client count freed: can fill cap again.
        for i in 0..MAX_DELEGATES_PER_CLIENT {
            let (t, _r) = mpsc::channel::<HostResult>(1);
            assert!(register_app(&ns.key(i + 1), client, AppIdentity::Local, t));
        }
    }

    /// A registration whose connection ended without a clean disconnect is
    /// reaped by the sweep alone, and a live one beside it on the same delegate
    /// is untouched. Observed through `registration_count`, not `route_to_apps`,
    /// because routing prunes closed channels itself and would pass with the
    /// sweep deleted.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn sweep_prunes_closed_registration() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (dead_tx, dead_rx) = mpsc::channel::<HostResult>(1);
        let (live_tx, mut live_rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, ns.client(0), AppIdentity::Local, dead_tx));
        assert!(register_app(&dk, ns.client(1), AppIdentity::Local, live_tx));
        drop(dead_rx);

        sweep_closed();
        assert_eq!(
            registration_count(&dk),
            1,
            "exactly the closed registration must be swept"
        );
        assert!(
            CLIENT_REGISTRATION_COUNTS
                .get(&ns.client(0))
                .is_none_or(|c| *c == 0),
            "sweeping must release the dead client's registration count"
        );
        assert_eq!(
            CLIENT_REGISTRATION_COUNTS.get(&ns.client(1)).map(|c| *c),
            Some(1),
            "the live client's count must be untouched"
        );
        assert_eq!(route_to_apps(&dk, host_msg()), 1);
        assert!(
            live_rx.try_recv().is_ok(),
            "the survivor must still deliver"
        );

        // Once the last registration goes, the delegate's entry goes with it,
        // or the map keeps one empty `Vec` per delegate ever registered.
        drop(live_rx);
        sweep_closed();
        assert!(
            DELEGATE_APPS.get(&dk).is_none(),
            "an emptied delegate entry must be removed"
        );
    }

    /// Regression (#5920): an app that registers once and then only listens
    /// must keep receiving pushes. A 30-minute TTL on the last
    /// `ApplicationMessages` request used to drop such an app while it was
    /// still connected. The day-long `advance` catches a TTL reintroduced on
    /// `tokio::time`; the assertion itself is that an open channel is never
    /// swept.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn idle_live_registration_still_receives_pushes() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (tx, mut rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(&dk, ns.client(0), AppIdentity::Local, tx));
        // An off-host registration lives by the same rule: it is never a
        // delivery target, but it is not reaped while connected either.
        let (remote_tx, _remote_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(
            &dk,
            ns.client(1),
            AppIdentity::Remote,
            remote_tx
        ));

        // A day with no further `ApplicationMessages`: the apps only listen.
        tokio::time::advance(std::time::Duration::from_secs(24 * 60 * 60)).await;
        sweep_closed();
        assert_eq!(registration_count(&dk), 2);
        assert_eq!(
            route_to_apps(&dk, host_msg()),
            1,
            "a connected app must keep receiving pushes however long it has \
             been since it last talked to the delegate"
        );
        assert!(rx.try_recv().is_ok());
    }

    /// Re-registering replaces the channel. Liveness is judged on the CURRENT
    /// sender, so a refresh that kept the old one would get a live app reaped as
    /// soon as the old channel's receiver went away.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn reregister_replaces_channel() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let client = ns.client(0);
        let (old_tx, old_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(&dk, client, AppIdentity::Local, old_tx));
        let (new_tx, mut new_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(&dk, client, AppIdentity::Local, new_tx));
        drop(old_rx);

        sweep_closed();
        assert_eq!(
            route_to_apps(&dk, host_msg()),
            1,
            "the refreshed registration must survive and deliver"
        );
        assert!(
            new_rx.try_recv().is_ok(),
            "delivery must use the new channel"
        );
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

        assert!(register_app(&dk, ns.client(0), AppIdentity::Local, app_tx));
        assert!(register_app(&dk, ns.client(1), AppIdentity::Local, cli_tx));
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

    /// J1 regression: a non-local registration whose client vanished must still
    /// be PRUNED, not parked.
    ///
    /// Skipping ahead of `try_send` removed the only reaper on this path. An
    /// off-host caller could then fill `MAX_APPS_PER_DELEGATE` with dead
    /// registrations and starve the legitimate local app until something else
    /// reaped them, renewably — the same remotely-triggerable denial of service
    /// that made the record-matching design unacceptable.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn dead_non_local_registration_is_pruned_not_parked() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (remote_tx, remote_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(
            &dk,
            ns.client(0),
            AppIdentity::Remote,
            remote_tx
        ));
        // The off-host client goes away WITHOUT a clean disconnect.
        drop(remote_rx);

        assert_eq!(
            route_to_apps(&dk, host_msg()),
            0,
            "a non-local registration is never a delivery target"
        );
        assert!(
            DELEGATE_APPS.get(&dk).is_none_or(|apps| apps.is_empty()),
            "a dead non-local registration must be reaped, or an off-host caller \
             can hold every slot with dead registrations and starve the local app"
        );
        // The per-client slot must be released too, or the cap leaks.
        assert!(
            CLIENT_REGISTRATION_COUNTS
                .get(&ns.client(0))
                .is_none_or(|c| *c == 0),
            "pruning must release the client's registration count"
        );
    }

    /// Dead off-host registrations do not count against the off-host cap
    /// either: an off-host caller that opens and drops connections must not
    /// fill `MAX_APPS_PER_DELEGATE - LOCAL_RESERVED_SLOTS` with corpses that
    /// refuse the next off-host client until a notification-path sweep.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn dead_remote_registrations_free_off_host_slots() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let off_host_cap = MAX_APPS_PER_DELEGATE - LOCAL_RESERVED_SLOTS;
        let mut keepalive = Vec::new();
        for i in 0..off_host_cap {
            let (tx, rx) = mpsc::channel::<HostResult>(1);
            keepalive.push(rx);
            assert!(register_app(&dk, ns.client(i), AppIdentity::Remote, tx));
        }
        // Counterfactual: at the off-host cap, a live set refuses the next one.
        let (tx, _rx) = mpsc::channel::<HostResult>(1);
        assert!(
            !register_app(&dk, ns.client(off_host_cap), AppIdentity::Remote, tx),
            "the off-host cap must hold while every incumbent is live"
        );

        drop(keepalive.pop());
        let (tx, _rx2) = mpsc::channel::<HostResult>(1);
        assert!(
            register_app(&dk, ns.client(off_host_cap), AppIdentity::Remote, tx),
            "a dead off-host registration must not hold an off-host slot"
        );
        assert_eq!(
            registration_count(&dk),
            off_host_cap,
            "only the dead registration may be pruned"
        );
        assert!(
            CLIENT_REGISTRATION_COUNTS
                .get(&ns.client(off_host_cap - 1))
                .is_none_or(|c| *c == 0),
            "the pruned client's registration count must be released"
        );
    }

    /// K1 regression: off-host registrations must never crowd a local app out of
    /// the registry.
    ///
    /// The dead-channel prune reaps only CLOSED senders, so it does nothing
    /// against the cheaper attack: hold the connections OPEN. Without a
    /// reservation an off-host caller fills all `MAX_APPS_PER_DELEGATE` slots
    /// with live registrations that are never delivery targets, and the
    /// legitimate local app is refused at the cap for as long as the attacker
    /// keeps its sockets open.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn open_remote_connections_cannot_crowd_out_a_local_app() {
        let ns = Namespace::new();
        let dk = ns.key(0);

        // Hold the channels alive: nothing here is closed, so the prune cannot
        // help and only the reservation can.
        let mut keepalive = Vec::new();
        let mut accepted = 0usize;
        for i in 0..MAX_APPS_PER_DELEGATE {
            let (tx, rx) = mpsc::channel::<HostResult>(1);
            keepalive.push(rx);
            if register_app(&dk, ns.client(i), AppIdentity::Remote, tx) {
                accepted += 1;
            }
        }
        assert_eq!(
            accepted,
            MAX_APPS_PER_DELEGATE - LOCAL_RESERVED_SLOTS,
            "off-host registrations must stop at the reserved boundary"
        );

        // The local app must still get in, and must actually receive output.
        let (local_tx, mut local_rx) = mpsc::channel::<HostResult>(4);
        assert!(
            register_app(
                &dk,
                ns.client(MAX_APPS_PER_DELEGATE + 1),
                AppIdentity::Local,
                local_tx
            ),
            "a local app must always be able to register, however many off-host \
             clients are parked on this delegate"
        );
        assert_eq!(route_to_apps(&dk, host_msg()), 1);
        assert!(local_rx.try_recv().is_ok());
        drop(keepalive);
    }

    /// The withheld report is once per REGISTRATION, not once per notification:
    /// this line ships at `release_max_level_info`, and the fan-out runs per
    /// contract-state change.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn withhold_is_reported_once_per_registration() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (remote_tx, _remote_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(
            &dk,
            ns.client(0),
            AppIdentity::Remote,
            remote_tx
        ));

        for _ in 0..5 {
            assert_eq!(route_to_apps(&dk, host_msg()), 0);
        }

        // Assert the LATCH, not the flag. After five withheld notifications the
        // right to report must already be spent, so a further claim returns
        // false — i.e. exactly one line was emitted, not five. Inspecting
        // `withhold_logged` instead would pass even if the guard were deleted
        // and the assignment kept.
        let spent = DELEGATE_APPS
            .get_mut(&dk)
            .map(|mut apps| apps.iter_mut().all(|a| !a.latch_withhold_report()))
            .unwrap_or(false);
        assert!(
            spent,
            "the report must be claimable only once per registration, or every \
             contract-state change emits a shipped log line per parked connection"
        );
    }

    /// A full channel is reported once per run of full sends, not once per
    /// notification, and the report re-arms after a successful delivery. A
    /// connection that is alive but not reading keeps its registration
    /// indefinitely, so a per-message line would repeat for as long as it stays
    /// connected.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn full_channel_is_reported_once_per_stall() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (tx, mut rx) = mpsc::channel::<HostResult>(1);
        assert!(register_app(&dk, ns.client(0), AppIdentity::Local, tx));

        assert_eq!(route_to_apps(&dk, host_msg()), 1, "fills the channel");
        for _ in 0..4 {
            assert_eq!(route_to_apps(&dk, host_msg()), 0, "channel full");
        }
        let claim = || {
            DELEGATE_APPS
                .get_mut(&dk)
                .map(|mut apps| apps.iter_mut().all(|a| a.latch_full_report()))
                .unwrap_or(false)
        };
        // Assert the LATCH, as `withhold_is_reported_once_per_registration`
        // does: after four full sends the right to report is already spent.
        assert!(
            !claim(),
            "a stalled channel must be reported once, not once per notification"
        );
        // Full is not dead: a stalled but connected app keeps its registration.
        sweep_closed();
        assert_eq!(registration_count(&dk), 1);

        assert!(rx.try_recv().is_ok());
        assert_eq!(route_to_apps(&dk, host_msg()), 1, "the app drained it");
        assert!(
            claim(),
            "a successful delivery must re-arm the report for the next stall"
        );
    }

    /// Delivery keys ONLY on locality: two distinct local clients both receive
    /// the output. This is the deliberate scope limit (locality, not per-app
    /// separation) and the reason the immutable-record matching was reverted —
    /// see `route_to_apps`. It is also the cross-app harvesting the PR's
    /// limitations call out, pinned so the retreat is explicit rather than
    /// accidental.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn delivery_keys_only_on_locality_not_on_app_identity() {
        let ns = Namespace::new();
        let dk = ns.key(0);
        let (a_tx, mut a_rx) = mpsc::channel::<HostResult>(4);
        let (b_tx, mut b_rx) = mpsc::channel::<HostResult>(4);
        assert!(register_app(&dk, ns.client(0), AppIdentity::Local, a_tx));
        assert!(register_app(&dk, ns.client(1), AppIdentity::Local, b_tx));

        assert_eq!(route_to_apps(&dk, host_msg()), 2);
        assert!(a_rx.try_recv().is_ok());
        assert!(b_rx.try_recv().is_ok());
    }
}
