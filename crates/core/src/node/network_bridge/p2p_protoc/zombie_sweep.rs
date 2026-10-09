//! Zombie-transport sweep for [`P2pConnManager`]: which transports that were
//! never promoted to the ring the event loop collects, and when (#5654).
//!
//! A transport the ring never promoted is normally collected once it is old
//! enough. A peer that cannot join the ring still routes through its gateway
//! transport, so that transport is judged by how recently its remote sent a
//! request over it instead ([`TransportActivity::idle`]). Transports kept only
//! for that reason are capped per remote IP (IPv6: per /64) and globally, and
//! the least recently used are evicted first when a cap is exceeded.
//!
//! What this does not cover, deliberately:
//!
//! - Only transports whose remote sent a request within `3 × transient_ttl`
//!   (90s by default) are exempt. An unjoined peer that stays quiet longer than
//!   that is collected by age as before, and its next request can still go
//!   into a dead link until its own 120s idle timeout fires. #5654 is fixed for
//!   unjoined peers that keep sending requests.
//! - A third or later exempt transport from one IP (or one IPv6 /64), or any
//!   beyond the global cap, gets the age rule as before.
//!
//! Nothing relies on this sweep to close a transport that was removed from the
//! ring but left open: every `Ring::prune_connection` caller also removes or
//! replaces the transport's `connections` entry (`drop_connection_by_addr`,
//! `drop_zombie_connection`, `TransportClosed`, the replaced-connection path in
//! `handle_successful_connection`, and event-loop channel-closure teardown), and
//! topology and health evictions go through `NodeEvent::DropConnection`. So a
//! transport never loses ring membership while staying open, and never becomes
//! exempt that way.

use super::*;
use crate::operations::connect::ConnectMsg;
use crate::operations::get::GetMsg;
use crate::operations::put::PutMsg;
use crate::operations::subscribe::SubscribeMsg;
use crate::operations::update::UpdateMsg;

/// How long a transport has existed, and how long since its remote last sent a
/// request over it.
///
/// Two `Duration`s side by side are easy to swap at a call site, so they travel
/// named.
#[derive(Clone, Copy, Debug)]
pub(super) struct TransportActivity {
    /// Time since the transport was established.
    pub(super) age: Duration,
    /// Time since the remote last sent a request over this transport (see
    /// [`is_link_use_request`]), or since establishment if it never has.
    pub(super) idle: Duration,
}

impl TransportActivity {
    /// A transport whose remote has sent no request since it was established:
    /// idle for its whole life.
    pub(super) fn never_used(age: Duration) -> Self {
        Self { age, idle: age }
    }
}

/// Multiple of `transient_ttl` after which a transport that was never promoted
/// to the ring is collected however recently its remote sent a request. 120 ×
/// the 30s default is one hour.
///
/// A bound must exist because an exemption from garbage collection that
/// ordinary use refreshes must be time-bounded (AGENTS.md). It is long because
/// collecting a link the remote is still using is exactly the #5654 failure:
/// the transport has no close message, so the remote keeps sending into the
/// dead link until its 120s idle timeout fires. At one hour an unjoined peer
/// pays that ~2-minute outage at most once an hour, instead of every ~3.5
/// minutes as it did when the sweep judged by age alone.
const ACTIVE_UNPROMOTED_MAX_AGE_TTL_MULTIPLE: u32 = 120;

/// Most transports from one remote IP (IPv6 remotes on the same /64 share one
/// per-IP slot, see [`link_use_exemption_key`]) that may be kept alive by recent
/// requests alone. Two rather than one so that two peers behind one household
/// NAT, or a peer and its own restarted process, are both served. A third or
/// later transport from the same IP gets the age rule, as before #5654.
pub(super) const LINK_USE_EXEMPT_PER_IP_CAP: usize = 2;

/// Divisor of `max_connections` giving the most transports, across all remotes,
/// that may be kept alive by recent requests alone. See
/// [`link_use_exempt_global_cap`].
const LINK_USE_EXEMPT_MAX_CONNECTIONS_DIVISOR: usize = 4;

/// Most zombie transports dropped in one sweep slice. Each drop involves
/// topology pruning and orphaned-transaction handling, and its final send to
/// the per-connection channel waits up to 100ms. Assuming drops stay within
/// that send timeout, a slice takes at most about 6.4s (64 × 100ms). The prune,
/// orphaned-transaction and ready-state work before that send is not under a
/// timeout, so this is not a hard bound; the 30s stats-tick sweep before #5654
/// had the same property.
pub(super) const MAX_ZOMBIE_CLEANUP_PER_CYCLE: usize = 64;

/// Minimum spacing between the end of one sweep slice and the start of the
/// next. See [`slice_spacing`].
pub(super) const ZOMBIE_BACKLOG_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// The next slice waits at least this many times as long as the previous slice
/// took. See [`slice_spacing`].
const ZOMBIE_BACKLOG_IDLE_FACTOR: u32 = 4;

/// How long the event loop waits after a slice that took `last_slice_took`
/// before starting a spaced slice (see [`ZombieSweepState::slice_due`]): the
/// longer of [`ZOMBIE_BACKLOG_SWEEP_INTERVAL`] and [`ZOMBIE_BACKLOG_IDLE_FACTOR`]
/// × `last_slice_took`.
///
/// The cost, stated plainly:
///
/// - Under a sustained backlog, backlog slices can use up to 20% of the event
///   loop's time for any slice of 250ms or longer (a slice of `d` is followed by
///   at least `4d` of other work); shorter slices use less. Before #5654 the
///   sweep reached 20% only when a 30s-tick slice took the full 6.4s. The total
///   cost per zombie dropped is unchanged; it is paid sooner, and only on a node
///   that already has more zombies due than one slice holds.
/// - Stats-tick slices keep the 30s cadence they had before #5654 when the
///   previous slice was also a tick slice, however long it took.
/// - Combined worst case, assuming drops stay within their 100ms send timeout
///   (see [`MAX_ZOMBIE_CLEANUP_PER_CYCLE`]): no two slices run back to back, no
///   single slice is longer than before (about 6.4s), and loop share stays
///   within the pre-#5654 worst case of 6.4s every 30s (about 21%). A drop that
///   stalls outside that timeout lengthens its slice here exactly as it did on
///   the stats tick before #5654.
///
/// When drops are quick the spacing is 1s, so a backlog drains at up to 64
/// transports per second instead of 64 per 30s.
pub(super) fn slice_spacing(last_slice_took: Duration) -> Duration {
    ZOMBIE_BACKLOG_SWEEP_INTERVAL.max(last_slice_took.saturating_mul(ZOMBIE_BACKLOG_IDLE_FACTOR))
}

/// The most transports, across all remotes, that may be kept alive by recent
/// requests alone: a quarter of `max_connections`, and never fewer than
/// [`LINK_USE_EXEMPT_PER_IP_CAP`].
///
/// Such a transport is a route for a peer that is not in this node's ring, so
/// its cost is ordinary per-connection work. Deriving the cap from
/// `max_connections` ties it to the load the operator configured this node to
/// carry, and a quarter keeps that extra load well below the ring's own budget
/// (50 at the production default of 200). The floor keeps the exemption usable
/// on the small `max_connections` values used in simulation and tests.
pub(super) fn link_use_exempt_global_cap(max_connections: usize) -> usize {
    (max_connections / LINK_USE_EXEMPT_MAX_CONNECTIONS_DIVISOR).max(LINK_USE_EXEMPT_PER_IP_CAP)
}

/// The key the per-IP cap counts under.
///
/// - IPv4 remotes (including IPv4-mapped IPv6) are keyed by address.
/// - IPv6 remotes on the same /64 share one per-IP slot.
/// - Loopback remotes are keyed by full socket address, so several local nodes
///   on one host (every simulation and local test network) are not collapsed
///   into one. This mirrors the loopback rule in `Location::from_address`.
///
/// `Location::from_address` masks differently (/24 and /48) for a different
/// purpose, ring placement.
pub(super) fn link_use_exemption_key(addr: SocketAddr) -> (IpAddr, u16) {
    let ip = addr.ip().to_canonical();
    if ip.is_loopback() {
        return (ip, addr.port());
    }
    match ip {
        IpAddr::V4(_) => (ip, 0),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            let prefix = std::net::Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0);
            (IpAddr::V6(prefix), 0)
        }
    }
}

/// Whether an inbound message is a new request from the remote, which is what
/// keeps a transport that was never promoted to the ring alive (#5654).
///
/// Only request-initiating operation messages count: CONNECT `Request`, GET
/// `Request`, SUBSCRIBE `Request`, PUT `Request`/`RequestStreaming`/
/// `ProbeRequest`/`ProbeReconcile`, and UPDATE `RequestUpdate`/
/// `RequestUpdateStreaming`. Everything else is excluded:
///
/// - Responses, acks, errors and rejections answer something this node sent,
///   so they are not the remote starting new work over this link.
/// - `ConnectFailed` and `Unsubscribe` continue or end an existing transaction.
/// - UPDATE `BroadcastTo*` (all four), `NeighborHosting`, `InterestSync` and
///   `ReadyState` are fan-out: a peer sends them to every transport it holds
///   whether or not it routes through it (`handle_hosting_broadcast`,
///   `handle_broadcast_change_interests` and `handle_broadcast_ready_state` all
///   iterate `connections.keys()`), so counting them would keep stale
///   transports alive — the accumulation the sweep exists to prevent (#3267,
///   #3543).
/// - `SubscribeHint` is an unsolicited nudge.
/// - `Aborted` is dropped on arrival without doing any work
///   (`handle_inbound_message` ignores it; the driver owns cancellation).
///
/// Keepalives never reach this layer (they are transport-level `Ping`/`Pong`).
///
/// This runs in `peer_connection_listener` as each message is received, before
/// the event loop dispatches it through `node::handle_pure_network_message_v1`,
/// which decides whether to start a driver.
/// That dispatch runs on a spawned task and can still drop a request (a
/// banned contract, a duplicate CONNECT transaction, the UPDATE rate limiter);
/// reporting its decision back to the event-loop-owned connection map would
/// need either a per-request event on the bounded notification channel or a
/// second address-keyed map with its own cleanup obligations. The variant
/// filter here is the narrowest practical hook, and the caps bound what any
/// remote can keep alive regardless of how the stamp is earned.
///
/// Every match below is exhaustive, so a new variant must be classified here.
pub(super) fn is_link_use_request(msg: &NetMessage) -> bool {
    match msg {
        NetMessage::V1(v1) => match v1 {
            NetMessageV1::Connect(m) => match m {
                ConnectMsg::Request { .. } => true,
                ConnectMsg::Response { .. }
                | ConnectMsg::ObservedAddress { .. }
                | ConnectMsg::Rejected { .. }
                | ConnectMsg::ConnectFailed { .. } => false,
            },
            NetMessageV1::Put(m) => match m {
                PutMsg::Request { .. }
                | PutMsg::RequestStreaming { .. }
                | PutMsg::ProbeRequest { .. }
                | PutMsg::ProbeReconcile { .. } => true,
                PutMsg::Response { .. }
                | PutMsg::ResponseStreaming { .. }
                | PutMsg::ForwardingAck { .. }
                | PutMsg::Error { .. }
                | PutMsg::ProbeResponse { .. } => false,
            },
            NetMessageV1::Get(m) => match m {
                GetMsg::Request { .. } => true,
                GetMsg::Response { .. }
                | GetMsg::ResponseStreaming { .. }
                | GetMsg::ResponseStreamingAck { .. }
                | GetMsg::ForwardingAck { .. } => false,
            },
            NetMessageV1::Subscribe(m) => match m {
                SubscribeMsg::Request { .. } => true,
                SubscribeMsg::Response { .. }
                | SubscribeMsg::Unsubscribe { .. }
                | SubscribeMsg::ForwardingAck { .. } => false,
            },
            NetMessageV1::Update(m) => match m {
                UpdateMsg::RequestUpdate { .. } | UpdateMsg::RequestUpdateStreaming { .. } => true,
                UpdateMsg::BroadcastTo { .. }
                | UpdateMsg::BroadcastToStreaming { .. }
                | UpdateMsg::BroadcastToV2 { .. }
                | UpdateMsg::BroadcastToStreamingV2 { .. } => false,
            },
            NetMessageV1::Aborted(_)
            | NetMessageV1::NeighborHosting { .. }
            | NetMessageV1::InterestSync { .. }
            | NetMessageV1::ReadyState { .. }
            | NetMessageV1::SubscribeHint(_) => false,
        },
    }
}

/// When the remote last sent a request over one transport, shared between the
/// transport's listener task (which writes it) and the event loop's zombie sweep
/// (which reads it).
///
/// The listener stamps a request as it receives and decodes it, BEFORE queueing
/// it for the event loop. Stamping when the event loop dequeues the message was
/// too late: on a busy node the loop can keep serving higher-priority work while
/// the request waits in the queue, and a sweep planned in that window saw a
/// stale stamp and reaped a transport with a request already waiting (#5654).
///
/// Stored as microseconds after `base` (the transport's `created_at`, so both
/// are readings of the same `tokio::time::Instant` clock; virtual time under
/// the simulation's paused runtime). Zero means no request yet. Writes use
/// `fetch_max`, so the stamp never moves backwards. A request costs one
/// classification match, one clock read and one atomic write.
#[derive(Clone, Debug)]
pub(super) struct LinkUseStamp {
    base: Instant,
    micros_since_base: Arc<std::sync::atomic::AtomicU64>,
}

impl LinkUseStamp {
    pub(super) fn new(base: Instant) -> Self {
        Self {
            base,
            micros_since_base: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn record(&self, now: Instant) {
        let micros =
            u64::try_from(now.saturating_duration_since(self.base).as_micros()).unwrap_or(u64::MAX);
        self.micros_since_base
            .fetch_max(micros, std::sync::atomic::Ordering::Relaxed);
    }

    /// The instant of the most recent request, or `base` if there has been none.
    /// An unrepresentable instant reads as `base`, the most idle value, so it
    /// can only make a transport look less in use, never more.
    pub(super) fn last_use(&self) -> Instant {
        let micros = self
            .micros_since_base
            .load(std::sync::atomic::Ordering::Relaxed);
        self.base
            .checked_add(Duration::from_micros(micros))
            .unwrap_or(self.base)
    }
}

/// Stamp `stamp` with `now` when `msg` is a request from the remote
/// ([`is_link_use_request`]). Returns whether it stamped. Called by
/// `peer_connection_listener` for each decoded inbound message, before the
/// message is queued for the event loop. `now` is passed in so tests control
/// the clock.
pub(super) fn record_link_use_request(
    stamp: &LinkUseStamp,
    msg: &NetMessage,
    now: Instant,
) -> bool {
    if !is_link_use_request(msg) {
        return false;
    }
    stamp.record(now);
    true
}

/// Check whether a transport connection is a zombie, before the link-use caps.
///
/// In-ring connections are never zombies.
///
/// Gateway connections are exempt below a 1-hour absolute cap because they
/// are intentionally transient (never promoted to ring) but actively needed
/// for routing (#3595). Past that exemption they are judged by AGE alone,
/// exactly as before #5654; recent requests never extend a gateway link.
///
/// Every other connection is judged by [`TransportActivity::idle`], the time
/// since its remote last sent a request over it, not by its age. A peer that
/// cannot join the ring keeps its gateway transport as its only route and the
/// gateway never promotes it; judging that transport by age dropped it
/// silently while the peer was still sending requests (#5654). A transport
/// whose remote never sends a request has `idle == age`, so for it these are
/// exactly the original age thresholds.
///
/// Thresholds, derived from `transient_ttl` (configurable, default 30s):
///
/// - `zombie_threshold` = `transient_ttl * 3`: catches connections with no pending
///   reservation. Must be greater than `PENDING_RESERVATION_TTL` (60s) so that a
///   connection isn't immediately killed after its reservation expires. Previous
///   hardcoded value of 300s caused gateways to accumulate ~250 zombie transports,
///   overwhelming the packet processing channel and dropping keepalive packets.
/// - `absolute_zombie_threshold` = `transient_ttl * 6`: overrides `has_pending` to
///   break the refresh cycle where `connection_maintenance()` perpetually renews
///   pending reservations on gateway transports. Previous hardcoded value of 600s
///   allowed zombie transports to linger far too long.
/// - `active_max_age` = `transient_ttl * ACTIVE_UNPROMOTED_MAX_AGE_TTL_MULTIPLE`,
///   measured on AGE: the time bound on the request-driven exemption.
///
/// The per-IP and global caps on that exemption are applied afterwards by
/// [`plan_zombie_sweep`].
pub(super) fn is_zombie(
    activity: TransportActivity,
    in_ring: bool,
    has_pending: bool,
    is_gateway: bool,
    transient_ttl: Duration,
) -> bool {
    let zombie_threshold = transient_ttl * 3;
    let absolute_zombie_threshold = transient_ttl * 6;
    let active_max_age = transient_ttl * ACTIVE_UNPROMOTED_MAX_AGE_TTL_MULTIPLE;
    let TransportActivity { age, idle } = activity;

    if in_ring {
        return false;
    }
    if is_gateway {
        // Gateway transient connections are intentionally not promoted to ring,
        // but the node needs them for routing. Without this exemption, gateways
        // enter a zombie→prune→reconnect→zombie death spiral that breaks all
        // streaming transfers (#3595).
        //
        // The exemption is time-bounded: truly dead gateway connections (no
        // traffic for 1 hour) are still cleaned up. The transport-level idle
        // timeout is the primary backstop, but this ensures no permanent leaks.
        /// Gateway connections get a generous exemption window (1 hour) because they
        /// are intentionally transient but needed for routing. The transport-level
        /// idle timeout (120s keepalive) is the primary cleanup mechanism for dead
        /// gateways; this threshold is the safety net.
        const GATEWAY_ZOMBIE_EXEMPTION: Duration = Duration::from_secs(3600);
        if age < GATEWAY_ZOMBIE_EXEMPTION {
            return false;
        }
        return (age > zombie_threshold && !has_pending) || age > absolute_zombie_threshold;
    }
    // A transport cannot have been idle for longer than it has existed.
    let idle = idle.min(age);
    if age > active_max_age {
        return true;
    }
    if idle > zombie_threshold && !has_pending {
        return true;
    }
    if idle > absolute_zombie_threshold {
        return true;
    }
    false
}

/// What the sweep's uncapped rules say about one transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ZombieVerdict {
    /// Not a zombie even by age.
    Keep,
    /// A zombie by age, kept only because its remote sent a request recently.
    /// Subject to the caps in [`plan_zombie_sweep`].
    KeepForLinkUse,
    /// A zombie.
    Reap,
}

/// Classify one transport: [`is_zombie`] on its real activity, and again as if
/// it had never been used, which is the rule before #5654.
pub(super) fn zombie_verdict(
    activity: TransportActivity,
    in_ring: bool,
    has_pending: bool,
    is_gateway: bool,
    transient_ttl: Duration,
) -> ZombieVerdict {
    if is_zombie(activity, in_ring, has_pending, is_gateway, transient_ttl) {
        ZombieVerdict::Reap
    } else if is_zombie(
        TransportActivity::never_used(activity.age),
        in_ring,
        has_pending,
        is_gateway,
        transient_ttl,
    ) {
        ZombieVerdict::KeepForLinkUse
    } else {
        ZombieVerdict::Keep
    }
}

/// One transport as seen by [`plan_zombie_sweep`].
#[derive(Clone, Copy, Debug)]
pub(super) struct SweepCandidate {
    pub(super) addr: SocketAddr,
    /// Time since the transport was established.
    pub(super) age: Duration,
    /// Time since its remote last sent a request over it (see
    /// [`TransportActivity::idle`]); orders cap eviction.
    pub(super) idle: Duration,
    pub(super) verdict: ZombieVerdict,
}

/// Which transports one sweep slice drops, and why.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ZombieSweepPlan {
    /// Transports that would have been kept for recent requests but exceeded
    /// a cap, least recently used first. Dropped before `zombies`.
    pub(super) over_cap: Vec<SocketAddr>,
    /// How many of `over_cap` exceeded the per-IP cap.
    pub(super) over_per_ip_cap: usize,
    /// How many of `over_cap` exceeded the global cap.
    pub(super) over_global_cap: usize,
    /// Transports the uncapped rules reap, in map order.
    pub(super) zombies: Vec<SocketAddr>,
    /// Transports kept alive by recent requests after the caps, most recently
    /// used first.
    pub(super) kept_for_link_use: Vec<SocketAddr>,
}

impl ZombieSweepPlan {
    /// Every transport due to be dropped, over-cap evictions first.
    pub(super) fn reap_order(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.over_cap.iter().chain(self.zombies.iter()).copied()
    }

    pub(super) fn due(&self) -> usize {
        self.over_cap.len() + self.zombies.len()
    }

    /// The transports one slice drops: at most `max`, over-cap first.
    pub(super) fn slice(&self, max: usize) -> ZombieSlice {
        let reap: Vec<SocketAddr> = self.reap_order().take(max).collect();
        let over_cap_dropped = reap
            .iter()
            .filter(|addr| self.over_cap.contains(addr))
            .count();
        let backlog = self.due() > reap.len();
        ZombieSlice {
            reap,
            over_cap_dropped,
            backlog,
        }
    }
}

/// The transports one sweep slice drops.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ZombieSlice {
    /// Transports to drop, over-cap evictions first.
    pub(super) reap: Vec<SocketAddr>,
    /// How many of `reap` are over-cap evictions. Counted from what this slice
    /// drops, not from the plan, so an eviction deferred to a later slice is
    /// counted once, when it is dropped.
    pub(super) over_cap_dropped: usize,
    /// Whether transports remain due after this slice.
    pub(super) backlog: bool,
}

/// Apply the link-use caps to one sweep's verdicts.
///
/// Transports kept for recent requests are admitted most recently used first:
/// at most `per_ip_cap` per [`link_use_exemption_key`], then at most
/// `global_cap` in total. Anything beyond a cap falls back to the age rule and
/// is reaped, the least recently used first (longest idle; ties broken by age,
/// oldest first). This is the least-recently-used eviction
/// `.claude/rules/code-style.md` asks of collections refreshed on every use.
///
/// Every candidate here is already past the age threshold, so a brand-new
/// transport never competes for a slot (it is `Keep` by age). Among the
/// candidates, recency is what says a link is still carrying requests: a link
/// used a second ago is kept in preference to one idle for most of the
/// threshold. The absolute age bound, not this order, is what stops a busy link
/// holding its exemption indefinitely.
pub(super) fn plan_zombie_sweep(
    candidates: impl IntoIterator<Item = SweepCandidate>,
    per_ip_cap: usize,
    global_cap: usize,
) -> ZombieSweepPlan {
    let mut plan = ZombieSweepPlan::default();
    let mut exempt: Vec<SweepCandidate> = Vec::new();
    for candidate in candidates {
        match candidate.verdict {
            ZombieVerdict::Keep => {}
            ZombieVerdict::Reap => plan.zombies.push(candidate.addr),
            ZombieVerdict::KeepForLinkUse => exempt.push(candidate),
        }
    }

    // Most recently used first, then youngest; the address breaks remaining
    // ties so the plan is deterministic.
    exempt.sort_by(|a, b| {
        a.idle
            .cmp(&b.idle)
            .then_with(|| a.age.cmp(&b.age))
            .then_with(|| a.addr.cmp(&b.addr))
    });

    let mut per_key: HashMap<(IpAddr, u16), usize> = HashMap::new();
    let mut over_cap: Vec<SweepCandidate> = Vec::new();
    for candidate in exempt {
        let held = per_key
            .entry(link_use_exemption_key(candidate.addr))
            .or_insert(0);
        if *held >= per_ip_cap {
            plan.over_per_ip_cap += 1;
            over_cap.push(candidate);
        } else if plan.kept_for_link_use.len() >= global_cap {
            plan.over_global_cap += 1;
            over_cap.push(candidate);
        } else {
            *held += 1;
            plan.kept_for_link_use.push(candidate.addr);
        }
    }

    // Least recently used first, then oldest.
    over_cap.sort_by(|a, b| {
        b.idle
            .cmp(&a.idle)
            .then_with(|| b.age.cmp(&a.age))
            .then_with(|| a.addr.cmp(&b.addr))
    });
    plan.over_cap = over_cap.into_iter().map(|c| c.addr).collect();
    plan
}

/// Classify every transport in `connections` and apply the link-use caps
/// derived from `connection_manager`, as of `now`. Everything the sweep decides
/// happens here, without awaiting, so it is tested directly; the event loop
/// only drops what the returned plan says.
pub(super) fn plan_sweep(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    gateways: &[PeerKeyLocation],
    connection_manager: &crate::ring::ConnectionManager,
    now: Instant,
) -> (Vec<SweepCandidate>, ZombieSweepPlan) {
    let transient_ttl = connection_manager.transient_ttl();
    let candidates: Vec<SweepCandidate> = connections
        .iter()
        .map(|(addr, entry)| {
            let is_gateway = gateways.iter().any(|gw| gw.socket_addr() == Some(*addr));
            let age = now.saturating_duration_since(entry.created_at);
            let idle = now.saturating_duration_since(entry.link_use.last_use());
            let verdict = zombie_verdict(
                TransportActivity { age, idle },
                connection_manager.is_in_ring(*addr),
                connection_manager.has_connection_or_pending(*addr),
                is_gateway,
                transient_ttl,
            );
            SweepCandidate {
                addr: *addr,
                age,
                idle,
                verdict,
            }
        })
        .collect();
    let plan = plan_zombie_sweep(
        candidates.iter().copied(),
        LINK_USE_EXEMPT_PER_IP_CAP,
        link_use_exempt_global_cap(connection_manager.max_connections),
    );
    (candidates, plan)
}

/// Which call site ran a sweep slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SliceKind {
    /// No slice has run yet.
    None,
    /// The 30s stats tick.
    Tick,
    /// A backlog slice, between ticks.
    Backlog,
}

/// Zombie sweep state for one event loop: when the next slice may run, and the
/// figures reported at info level on the stats tick so they are visible in
/// release builds.
#[derive(Debug)]
pub(super) struct ZombieSweepState {
    /// Over-cap transports actually dropped, since the event loop started.
    pub(super) cap_evictions_total: u64,
    backlog: bool,
    last_slice_kind: SliceKind,
    last_slice_end: Instant,
    last_slice_took: Duration,
    // Figures from the plan of the most recent slice, and drops since the last
    // report.
    last_slice_link_use_exempt: usize,
    last_slice_over_per_ip_cap: usize,
    last_slice_over_global_cap: usize,
    last_slice_due: usize,
    last_slice_global_cap: usize,
    dropped_since_report: u64,
    cap_evictions_at_last_report: u64,
}

impl ZombieSweepState {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            cap_evictions_total: 0,
            backlog: false,
            last_slice_kind: SliceKind::None,
            last_slice_end: now,
            last_slice_took: Duration::ZERO,
            last_slice_link_use_exempt: 0,
            last_slice_over_per_ip_cap: 0,
            last_slice_over_global_cap: 0,
            last_slice_due: 0,
            last_slice_global_cap: 0,
            dropped_since_report: 0,
            cap_evictions_at_last_report: 0,
        }
    }

    /// Record the plan a slice was cut from, for the next report.
    pub(super) fn record_plan(&mut self, plan: &ZombieSweepPlan, global_cap: usize) {
        self.last_slice_link_use_exempt = plan.kept_for_link_use.len();
        self.last_slice_over_per_ip_cap = plan.over_per_ip_cap;
        self.last_slice_over_global_cap = plan.over_global_cap;
        self.last_slice_due = plan.due();
        self.last_slice_global_cap = global_cap;
    }

    /// Record a slice that ran from `started` to `ended`, run by the stats tick
    /// (`stats_tick`) or as a backlog slice.
    pub(super) fn record_slice(
        &mut self,
        started: Instant,
        ended: Instant,
        slice: &ZombieSlice,
        stats_tick: bool,
    ) {
        self.cap_evictions_total = self
            .cap_evictions_total
            .saturating_add(slice.over_cap_dropped as u64);
        self.dropped_since_report = self
            .dropped_since_report
            .saturating_add(slice.reap.len() as u64);
        self.backlog = slice.backlog;
        self.last_slice_kind = if stats_tick {
            SliceKind::Tick
        } else {
            SliceKind::Backlog
        };
        self.last_slice_took = ended.saturating_duration_since(started);
        self.last_slice_end = ended;
    }

    /// The earliest instant the next spaced slice may start: [`slice_spacing`]
    /// after the previous slice ended. `None` if that instant is not
    /// representable. Backlog slices then fail closed (none runs, no timer);
    /// the stats tick treats it as due, so the 30s sweep always comes back.
    fn next_slice_at(&self) -> Option<Instant> {
        self.last_slice_end
            .checked_add(slice_spacing(self.last_slice_took))
    }

    /// Whether the event loop may start a sweep slice now. It is the one rule
    /// for both call sites: `stats_tick` is `true` on the regular 30s tick and
    /// `false` for the check after every event.
    ///
    /// - A stats-tick slice right after another stats-tick slice (or as the
    ///   first slice) always runs. The tick's own 30s interval spaces those
    ///   slices, exactly as before #5654.
    /// - Every other slice needs a reason (a backlog, or the tick) and must
    ///   start at least [`slice_spacing`] after the previous slice ended. So a
    ///   backlog slice never follows a slice straight away, and neither does a
    ///   tick slice that follows a backlog slice.
    /// - If the spaced instant is not representable, a backlog slice is not due
    ///   but a tick slice is: sweeping can pause, never stop for good.
    pub(super) fn slice_due(&self, now: Instant, stats_tick: bool) -> bool {
        if stats_tick {
            return self.last_slice_kind != SliceKind::Backlog
                || self.next_slice_at().is_none_or(|at| now >= at);
        }
        self.backlog && self.next_slice_at().is_some_and(|at| now >= at)
    }

    /// When the event loop must wake for a backlog slice even if no event
    /// arrives. `None` without a backlog, so an idle node sets no timer.
    pub(super) fn backlog_deadline(&self) -> Option<Instant> {
        if self.backlog {
            self.next_slice_at()
        } else {
            None
        }
    }

    /// Report the sweep at info level, once per stats tick, when there is
    /// anything to report; then reset the since-last-report figures. The plan
    /// figures are from the most recent slice, which may predate this tick when
    /// the tick's own slice was not due; `last_slice_ago_ms` says how old they
    /// are.
    pub(super) fn report(&mut self, now: Instant) {
        let cap_evictions = self
            .cap_evictions_total
            .saturating_sub(self.cap_evictions_at_last_report);
        if self.last_slice_link_use_exempt > 0
            || self.last_slice_over_per_ip_cap > 0
            || self.last_slice_over_global_cap > 0
            || self.dropped_since_report > 0
            || self.backlog
        {
            tracing::info!(
                last_slice_ago_ms = now
                    .saturating_duration_since(self.last_slice_end)
                    .as_millis(),
                last_slice_link_use_exempt = self.last_slice_link_use_exempt,
                last_slice_over_per_ip_cap = self.last_slice_over_per_ip_cap,
                last_slice_over_global_cap = self.last_slice_over_global_cap,
                last_slice_zombies_due = self.last_slice_due,
                link_use_exempt_global_cap = self.last_slice_global_cap,
                link_use_exempt_per_ip_cap = LINK_USE_EXEMPT_PER_IP_CAP,
                zombies_dropped_since_last_report = self.dropped_since_report,
                cap_evictions_since_last_report = cap_evictions,
                cap_evictions_total = self.cap_evictions_total,
                backlog = self.backlog,
                "Zombie transport sweep (not promoted to ring)"
            );
        }
        self.dropped_since_report = 0;
        self.cap_evictions_at_last_report = self.cap_evictions_total;
    }
}

/// What woke the event loop.
pub(super) enum LoopWake<T> {
    /// The event stream yielded (or ended, with `None`).
    Event(Option<T>),
    /// A backlog sweep slice is due and no event arrived first.
    ZombieSweepDue,
}

/// Wait for the next event from `stream`, or, while a backlog exists, for its
/// deadline. This drains an EXISTING backlog on a quiet node, which would
/// otherwise wait for some unrelated event. It does not start sweeping on a
/// quiet node that has no backlog: that node still runs its first sweep of an
/// interval only on the next event after the stats tick is due, as before
/// #5654.
///
/// `biased` toward the stream: events are the loop's work and are handled
/// first. Under a steady event flow the timer arm may never win, which is fine,
/// because the loop checks [`ZombieSweepState::slice_due`] after every event.
/// With no backlog there is no timer at all, so an idle node does not spin.
///
/// Cancellation-safe: dropping this future drops a `StreamExt::next` future,
/// which is cancellation-safe, and a `Sleep`, which holds no state.
pub(super) async fn next_wake<S>(
    stream: &mut S,
    backlog_deadline: Option<Instant>,
) -> LoopWake<S::Item>
where
    S: futures::Stream + Unpin,
{
    match backlog_deadline {
        None => LoopWake::Event(StreamExt::next(stream).await),
        Some(deadline) => tokio::select! {
            biased;
            item = StreamExt::next(stream) => LoopWake::Event(item),
            () = tokio::time::sleep_until(deadline) => LoopWake::ZombieSweepDue,
        },
    }
}

impl P2pConnManager {
    /// Run one zombie sweep slice: plan it with [`plan_sweep`], drop at most
    /// [`MAX_ZOMBIE_CLEANUP_PER_CYCLE`] transports (over-cap evictions first),
    /// and record the slice in `state`, which decides when the next may run
    /// ([`ZombieSweepState::slice_due`]). `stats_tick` says which call site ran
    /// it.
    ///
    /// Per-slice logging is at debug; the info-level summary is emitted once per
    /// stats tick by [`ZombieSweepState::report`].
    ///
    /// Uses `drop_zombie_connection` (non-blocking `try_send`) rather than
    /// `drop_connection_by_addr` to avoid a circular deadlock with the handshake
    /// driver (#3519).
    pub(super) async fn sweep_zombie_transports(
        &mut self,
        handshake_cmd_sender: &HandshakeCommandSender,
        state: &mut ZombieSweepState,
        stats_tick: bool,
    ) {
        let started = Instant::now();
        let op_manager = self.bridge.op_manager.clone();
        let connection_manager = &op_manager.ring.connection_manager;
        let (candidates, plan) = plan_sweep(
            &self.connections,
            &self.gateways,
            connection_manager,
            started,
        );

        if let Some(own_addr) = connection_manager.get_own_addr() {
            // Lazy: on a production node the registry never iterates this.
            crate::ring::topology_registry::record_zombie_sweep_verdicts(
                own_addr,
                candidates
                    .iter()
                    .filter(|c| c.verdict != ZombieVerdict::Keep)
                    .map(|c| (c.addr, plan.kept_for_link_use.contains(&c.addr))),
            );
        }

        let slice = plan.slice(MAX_ZOMBIE_CLEANUP_PER_CYCLE);
        for addr in &slice.reap {
            self.drop_zombie_connection(*addr, handshake_cmd_sender)
                .await;
        }
        state.record_plan(
            &plan,
            link_use_exempt_global_cap(connection_manager.max_connections),
        );
        state.record_slice(started, Instant::now(), &slice, stats_tick);

        if !slice.reap.is_empty() {
            tracing::debug!(
                zombie_count = slice.reap.len(),
                zombies_due = plan.due(),
                over_cap_dropped = slice.over_cap_dropped,
                backlog = slice.backlog,
                stats_tick,
                "Zombie sweep slice"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{InterestMessage, NeighborHostingMessage, SubscribeHintMsg, Transaction};
    use crate::operations::VisitedPeers;
    use crate::operations::connect::{ConnectRequest, ConnectResponse};
    use crate::operations::get::GetMsgResult;
    use crate::operations::subscribe::SubscribeMsgResult;
    use crate::operations::test_utils::{make_contract_key, make_test_contract};
    use crate::transport::peer_connection::StreamId;
    use freenet_stdlib::prelude::{RelatedContracts, StateDelta, StateSummary, WrappedState};

    const TTL: Duration = Duration::from_secs(30);
    // With TTL=30s: zombie_threshold=90s, absolute_zombie_threshold=180s,
    // active_max_age=3600s.

    fn used(age_secs: u64, idle_secs: u64) -> TransportActivity {
        TransportActivity {
            age: Duration::from_secs(age_secs),
            idle: Duration::from_secs(idle_secs),
        }
    }

    // ---- is_zombie / zombie_verdict ----

    /// #5654: a peer that cannot join the ring keeps sending requests over its
    /// gateway transport, which the gateway never promotes. Past the old 90s
    /// age threshold that transport survives as long as the peer uses it.
    #[test]
    fn keeps_unpromoted_transport_in_use() {
        for (age, idle) in [(91, 0), (400, 10), (400, 90), (3000, 5)] {
            assert!(
                !is_zombie(used(age, idle), false, false, false, TTL),
                "request {idle}s ago (age {age}s) must not be a zombie"
            );
            assert_eq!(
                zombie_verdict(used(age, idle), false, false, false, TTL),
                ZombieVerdict::KeepForLinkUse,
                "age {age}s idle {idle}s is kept only for link use"
            );
        }
    }

    /// The sweep's original purpose survives: an unpromoted transport whose
    /// remote stopped sending requests is still collected.
    #[test]
    fn reaps_unpromoted_transport_left_idle() {
        assert!(is_zombie(used(400, 91), false, false, false, TTL));
        assert!(
            !is_zombie(used(400, 90), false, false, false, TTL),
            "exactly 90s idle is not past the threshold (uses > not >=)"
        );
        assert_eq!(
            zombie_verdict(used(400, 91), false, false, false, TTL),
            ZombieVerdict::Reap
        );
        assert_eq!(
            zombie_verdict(used(60, 60), false, false, false, TTL),
            ZombieVerdict::Keep,
            "a young transport is not a zombie by age, so it needs no exemption"
        );
    }

    /// With a pending reservation the 6×TTL override is also measured on idle
    /// time.
    #[test]
    fn pending_override_uses_idle_time() {
        assert!(!is_zombie(used(1000, 120), false, true, false, TTL));
        assert!(!is_zombie(used(1000, 180), false, true, false, TTL));
        assert!(is_zombie(used(1000, 181), false, true, false, TTL));
    }

    /// The request-driven exemption is time-bounded (AGENTS.md): a remote that
    /// keeps sending requests is still collected past `active_max_age`, pending
    /// or not.
    #[test]
    fn in_use_exemption_is_time_bounded() {
        assert!(!is_zombie(used(3600, 0), false, false, false, TTL));
        assert!(is_zombie(used(3601, 0), false, false, false, TTL));
        assert!(
            is_zombie(used(3601, 0), false, true, false, TTL),
            "a pending reservation does not extend the age bound"
        );
    }

    #[test]
    fn in_use_age_bound_scales_with_ttl() {
        // TTL=120s: active_max_age = 120 * 120s = 4h.
        let ttl = Duration::from_secs(120);
        assert!(!is_zombie(used(3601, 0), false, false, false, ttl));
        assert!(!is_zombie(used(14_400, 0), false, false, false, ttl));
        assert!(is_zombie(used(14_401, 0), false, false, false, ttl));
    }

    #[test]
    fn in_ring_unaffected_by_activity() {
        for (age, idle) in [(100_000, 100_000), (100_000, 0)] {
            assert!(!is_zombie(used(age, idle), true, false, false, TTL));
            assert_eq!(
                zombie_verdict(used(age, idle), true, false, false, TTL),
                ZombieVerdict::Keep
            );
        }
    }

    /// A gateway link keeps its pre-#5654 semantics exactly: exempt below one
    /// hour, then judged by age alone. Recent requests never extend it, at any
    /// TTL. With TTL=900s the old age thresholds (2700s/5400s) are already
    /// passed at 3601s; with TTL=1500s (4500s/9000s) they are not.
    #[test]
    fn gateway_semantics_unchanged_at_non_default_ttl() {
        let ttl_900 = Duration::from_secs(900);
        assert!(
            is_zombie(used(3601, 0), false, false, true, ttl_900),
            "a gateway link past its exemption is reaped by age even if used"
        );
        assert_eq!(
            zombie_verdict(used(3601, 0), false, false, true, ttl_900),
            ZombieVerdict::Reap
        );
        assert!(!is_zombie(used(3599, 3599), false, false, true, ttl_900));

        let ttl_1500 = Duration::from_secs(1500);
        for idle in [0, 4000] {
            assert!(
                !is_zombie(used(4000, idle), false, false, true, ttl_1500),
                "age 4000s < 3×1500s: not a zombie, as before (idle {idle}s)"
            );
        }
        for idle in [0, 4600] {
            assert!(
                is_zombie(used(4600, idle), false, false, true, ttl_1500),
                "age 4600s > 3×1500s: a zombie, as before (idle {idle}s)"
            );
            assert_eq!(
                zombie_verdict(used(4600, idle), false, false, true, ttl_1500),
                ZombieVerdict::Reap
            );
        }
        assert!(!is_zombie(used(4600, 0), false, true, true, ttl_1500));
        assert!(is_zombie(used(9001, 0), false, true, true, ttl_1500));
    }

    /// A gateway transport inside its exemption window is `Keep`, not
    /// `KeepForLinkUse`, however recently it was used. `zombie_verdict`'s
    /// never-used comparison must keep the real `is_gateway`, or gateway links
    /// would take link-use exemption slots from other remotes.
    #[test]
    fn gateway_in_its_exemption_window_is_keep_not_link_use() {
        for ttl in [TTL, Duration::from_secs(120), Duration::from_secs(900)] {
            for (age, idle) in [(400, 0), (1800, 30), (3599, 5)] {
                assert_eq!(
                    zombie_verdict(used(age, idle), false, false, true, ttl),
                    ZombieVerdict::Keep,
                    "gateway age {age}s idle {idle}s ttl {ttl:?}"
                );
            }
        }

        // Through plan_sweep: a gateway link does not take an exemption slot.
        let now = Instant::now() + Duration::from_secs(10_000);
        let cm = crate::ring::ConnectionManager::test_default();
        let gw_addr = addr("198.51.100.7:31337");
        let gateway = PeerKeyLocation::new(
            crate::transport::TransportKeypair::new().public().clone(),
            gw_addr,
        );
        let mut connections = BTreeMap::new();
        connections.insert(gw_addr, entry_at(now, 400, 0));
        let (candidates, plan) = plan_sweep(&connections, &[gateway], &cm, now);
        assert_eq!(candidates[0].verdict, ZombieVerdict::Keep);
        assert!(plan.kept_for_link_use.is_empty());
        assert_eq!(plan.due(), 0);
    }

    #[test]
    fn idle_time_is_capped_at_age() {
        assert!(!is_zombie(used(60, 500), false, false, false, TTL));
    }

    // ---- caps ----

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// An exempt candidate idle for `idle_secs`, all of the same age, so the
    /// cap order is decided by recency alone.
    fn exempt(a: &str, idle_secs: u64) -> SweepCandidate {
        exempt_aged(a, 1000, idle_secs)
    }

    fn exempt_aged(a: &str, age_secs: u64, idle_secs: u64) -> SweepCandidate {
        SweepCandidate {
            addr: addr(a),
            age: Duration::from_secs(age_secs),
            idle: Duration::from_secs(idle_secs),
            verdict: ZombieVerdict::KeepForLinkUse,
        }
    }

    #[test]
    fn global_cap_derives_from_max_connections() {
        assert_eq!(link_use_exempt_global_cap(200), 50);
        assert_eq!(link_use_exempt_global_cap(25), 6);
        assert_eq!(link_use_exempt_global_cap(5), LINK_USE_EXEMPT_PER_IP_CAP);
        assert_eq!(link_use_exempt_global_cap(0), LINK_USE_EXEMPT_PER_IP_CAP);
    }

    #[test]
    fn per_ip_cap_evicts_least_recently_used_from_that_ip() {
        let plan = plan_zombie_sweep(
            [
                exempt("203.0.113.7:1000", 500),
                exempt("203.0.113.7:1001", 200),
                exempt("203.0.113.7:1002", 300),
                exempt("198.51.100.1:1000", 900),
            ],
            2,
            100,
        );
        assert_eq!(plan.over_cap, vec![addr("203.0.113.7:1000")]);
        assert_eq!(plan.over_per_ip_cap, 1);
        assert_eq!(plan.over_global_cap, 0);
        assert_eq!(
            plan.kept_for_link_use,
            vec![
                addr("203.0.113.7:1001"),
                addr("203.0.113.7:1002"),
                addr("198.51.100.1:1000"),
            ]
        );
    }

    #[test]
    fn global_cap_evicts_least_recently_used_overall() {
        let plan = plan_zombie_sweep(
            [
                exempt("198.51.100.1:1", 400),
                exempt("198.51.100.2:1", 100),
                exempt("198.51.100.3:1", 300),
                exempt("198.51.100.4:1", 200),
            ],
            2,
            2,
        );
        assert_eq!(
            plan.over_cap,
            vec![addr("198.51.100.1:1"), addr("198.51.100.3:1")],
            "over-cap evictions are ordered least recently used first"
        );
        assert_eq!(plan.over_global_cap, 2);
        assert_eq!(
            plan.kept_for_link_use,
            vec![addr("198.51.100.2:1"), addr("198.51.100.4:1")]
        );
    }

    /// Recency, not age, decides who keeps a slot: an old link used a second
    /// ago is kept over a young link idle for most of the threshold. Equal
    /// recency falls back to age, oldest evicted first.
    #[test]
    fn cap_eviction_is_least_recently_used_not_oldest() {
        let plan = plan_zombie_sweep(
            [
                exempt_aged("198.51.100.1:1", 3000, 1),
                exempt_aged("198.51.100.2:1", 200, 80),
            ],
            2,
            1,
        );
        assert_eq!(plan.kept_for_link_use, vec![addr("198.51.100.1:1")]);
        assert_eq!(plan.over_cap, vec![addr("198.51.100.2:1")]);

        // Several over cap, where recency and age disagree: the drop order (the
        // order a bounded slice takes them in) follows recency.
        let order = plan_zombie_sweep(
            [
                exempt_aged("198.51.100.1:1", 3000, 1),
                exempt_aged("198.51.100.2:1", 200, 80),
                exempt_aged("198.51.100.3:1", 2500, 40),
                exempt_aged("198.51.100.4:1", 400, 60),
            ],
            2,
            1,
        );
        assert_eq!(
            order.over_cap,
            vec![
                addr("198.51.100.2:1"),
                addr("198.51.100.4:1"),
                addr("198.51.100.3:1"),
            ],
            "over-cap transports are dropped longest idle first, not oldest first"
        );

        let tie = plan_zombie_sweep(
            [
                exempt_aged("198.51.100.1:1", 300, 20),
                exempt_aged("198.51.100.2:1", 900, 20),
                exempt_aged("198.51.100.3:1", 600, 20),
            ],
            2,
            1,
        );
        assert_eq!(
            tie.kept_for_link_use,
            vec![addr("198.51.100.1:1")],
            "equal recency: the youngest is kept"
        );
        assert_eq!(
            tie.over_cap,
            vec![addr("198.51.100.2:1"), addr("198.51.100.3:1")],
            "equal recency: evicted oldest first"
        );
    }

    /// A per-IP eviction does not consume a global slot.
    #[test]
    fn per_ip_evictions_do_not_consume_global_slots() {
        let plan = plan_zombie_sweep(
            [
                exempt("203.0.113.7:1", 10),
                exempt("203.0.113.7:2", 20),
                exempt("203.0.113.7:3", 30),
                exempt("198.51.100.1:1", 40),
            ],
            1,
            2,
        );
        assert_eq!(
            plan.kept_for_link_use,
            vec![addr("203.0.113.7:1"), addr("198.51.100.1:1")]
        );
        assert_eq!(
            plan.over_cap,
            vec![addr("203.0.113.7:3"), addr("203.0.113.7:2")]
        );
        assert_eq!(plan.over_per_ip_cap, 2);
        assert_eq!(plan.over_global_cap, 0);
    }

    /// IPv6 remotes in one /64 share a key; different /64s do not. The port
    /// never matters outside loopback.
    #[test]
    fn exemption_key_groups_ipv6_by_64() {
        let same_64 = [
            "[2001:db8:1:2::1]:1000",
            "[2001:db8:1:2:ffff:ffff:ffff:ffff]:2000",
            "[2001:db8:1:2:abcd::9]:3000",
        ];
        let key = link_use_exemption_key(addr(same_64[0]));
        for a in same_64 {
            assert_eq!(
                link_use_exemption_key(addr(a)),
                key,
                "{a} is in the same /64"
            );
        }
        for other in ["[2001:db8:1:3::1]:1000", "[2001:db8:2:2::1]:1000"] {
            assert_ne!(
                link_use_exemption_key(addr(other)),
                key,
                "{other} is in a different /64"
            );
        }
        assert_ne!(
            link_use_exemption_key(addr("203.0.113.7:1")),
            link_use_exemption_key(addr("203.0.113.8:1")),
            "IPv4 remotes are grouped by full address"
        );
    }

    /// The per-IP cap counts an IPv6 /64 as one group.
    #[test]
    fn per_ip_cap_applies_across_one_ipv6_64() {
        let plan = plan_zombie_sweep(
            [
                exempt("[2001:db8:1:2::1]:1", 100),
                exempt("[2001:db8:1:2::2]:1", 200),
                exempt("[2001:db8:1:2::3]:1", 300),
                exempt("[2001:db8:1:3::1]:1", 400),
            ],
            2,
            100,
        );
        assert_eq!(plan.over_cap, vec![addr("[2001:db8:1:2::3]:1")]);
        assert_eq!(plan.over_per_ip_cap, 1);
        assert_eq!(plan.kept_for_link_use.len(), 3);
    }

    /// IPv4-mapped IPv6 counts as the same remote IP; loopback is keyed by full
    /// address so local multi-node setups are not collapsed.
    #[test]
    fn exemption_key_canonicalises_and_splits_loopback() {
        assert_eq!(
            link_use_exemption_key(addr("[::ffff:203.0.113.7]:1")),
            link_use_exemption_key(addr("203.0.113.7:2"))
        );
        assert_ne!(
            link_use_exemption_key(addr("[::1]:1")),
            link_use_exemption_key(addr("[::1]:2"))
        );
        assert_ne!(
            link_use_exemption_key(addr("127.0.0.1:1")),
            link_use_exemption_key(addr("127.0.0.1:2"))
        );
    }

    /// Over-cap evictions are dropped before ordinary zombies, so a backlog of
    /// idle zombies cannot hold over-cap transports alive.
    #[test]
    fn reap_order_puts_over_cap_first() {
        let mut candidates = vec![SweepCandidate {
            addr: addr("192.0.2.1:1"),
            age: Duration::from_secs(1000),
            idle: Duration::from_secs(1000),
            verdict: ZombieVerdict::Reap,
        }];
        candidates.push(exempt("198.51.100.1:1", 500));
        candidates.push(exempt("198.51.100.2:1", 100));
        candidates.push(SweepCandidate {
            addr: addr("192.0.2.2:1"),
            age: Duration::from_secs(60),
            idle: Duration::from_secs(60),
            verdict: ZombieVerdict::Keep,
        });
        let plan = plan_zombie_sweep(candidates, 2, 1);
        assert_eq!(
            plan.reap_order().collect::<Vec<_>>(),
            vec![addr("198.51.100.1:1"), addr("192.0.2.1:1")]
        );
        assert_eq!(plan.due(), 2);
        assert_eq!(plan.kept_for_link_use, vec![addr("198.51.100.2:1")]);
    }

    /// A slice drops at most `max` transports, over-cap evictions first, and
    /// reports a backlog exactly when some remain due.
    #[test]
    fn slice_bounds_drops_and_reports_backlog() {
        let mut candidates: Vec<SweepCandidate> = (0..3u16)
            .map(|i| SweepCandidate {
                addr: SocketAddr::from(([192, 0, 2, 1], i + 1)),
                age: Duration::from_secs(1000),
                idle: Duration::from_secs(1000),
                verdict: ZombieVerdict::Reap,
            })
            .collect();
        candidates.push(exempt("198.51.100.1:1", 500));
        candidates.push(exempt("198.51.100.2:1", 100));
        let plan = plan_zombie_sweep(candidates, 2, 1);
        assert_eq!(plan.due(), 4);

        let slice = plan.slice(2);
        assert_eq!(slice.reap.len(), 2);
        assert_eq!(slice.reap[0], addr("198.51.100.1:1"), "over-cap goes first");
        assert!(slice.backlog, "two remain due");

        let slice = plan.slice(4);
        assert_eq!(slice.reap.len(), 4);
        assert!(!slice.backlog, "nothing remains due");

        let slice = plan_zombie_sweep(Vec::new(), 2, 1).slice(64);
        assert!(slice.reap.is_empty());
        assert!(!slice.backlog);
    }

    /// `over_cap_dropped` counts only the over-cap transports a slice actually
    /// drops, so one deferred by the slice bound is counted once, when dropped.
    #[test]
    fn over_cap_dropped_counts_what_the_slice_drops() {
        let candidates: Vec<SweepCandidate> = (0..5u8)
            .map(|i| exempt(&format!("198.51.100.{}:1", i + 1), 100 + u64::from(i)))
            .collect();
        // Global cap 1: four transports are over cap.
        let plan = plan_zombie_sweep(candidates, 2, 1);
        assert_eq!(plan.over_cap.len(), 4);

        let first = plan.slice(3);
        assert_eq!(first.over_cap_dropped, 3, "only three fit in this slice");
        let mut state = ZombieSweepState::new(Instant::now());
        let t = Instant::now();
        state.record_slice(t, t, &first, false);
        assert_eq!(state.cap_evictions_total, 3);

        // Next sweep: the three dropped are gone; the fourth is still over cap.
        let remaining: Vec<SweepCandidate> = (0..5u8)
            .map(|i| exempt(&format!("198.51.100.{}:1", i + 1), 100 + u64::from(i)))
            .filter(|c| !first.reap.contains(&c.addr))
            .collect();
        let second = plan_zombie_sweep(remaining, 2, 1).slice(3);
        assert_eq!(second.over_cap_dropped, 1);
        state.record_slice(t, t, &second, false);
        assert_eq!(
            state.cap_evictions_total, 4,
            "four transports were evicted over cap, each counted once"
        );

        // Ordinary zombies in the slice are not counted.
        let mixed = plan_zombie_sweep(
            [
                SweepCandidate {
                    addr: addr("192.0.2.1:1"),
                    age: Duration::from_secs(900),
                    idle: Duration::from_secs(900),
                    verdict: ZombieVerdict::Reap,
                },
                exempt("198.51.100.1:1", 100),
            ],
            2,
            2,
        )
        .slice(64);
        assert_eq!(mixed.reap.len(), 1);
        assert_eq!(mixed.over_cap_dropped, 0);
    }

    // ---- backlog scheduling ----

    #[test]
    fn slice_spacing_bounds_the_sweep_duty_cycle() {
        assert_eq!(slice_spacing(Duration::ZERO), ZOMBIE_BACKLOG_SWEEP_INTERVAL);
        assert_eq!(
            slice_spacing(Duration::from_millis(250)),
            ZOMBIE_BACKLOG_SWEEP_INTERVAL,
            "a 250ms slice waits the 1s minimum, 20% of loop time"
        );
        assert_eq!(
            slice_spacing(Duration::from_secs(3)),
            Duration::from_secs(12),
            "a slow slice waits four times as long as it took"
        );
        assert_eq!(
            slice_spacing(Duration::from_millis(6400)),
            Duration::from_millis(25_600),
            "a worst-case 64 × 100ms slice waits 25.6s"
        );
    }

    fn backlog_slice(backlog: bool) -> ZombieSlice {
        ZombieSlice {
            reap: vec![addr("192.0.2.1:1")],
            over_cap_dropped: 0,
            backlog,
        }
    }

    #[test]
    fn slice_due_needs_a_reason_and_the_spacing() {
        let t0 = Instant::now();
        let mut state = ZombieSweepState::new(t0);
        assert!(
            !state.slice_due(t0 + Duration::from_secs(60), false),
            "no backlog and no tick: nothing to do"
        );
        assert!(state.backlog_deadline().is_none(), "no backlog, no timer");
        assert!(state.slice_due(t0, true), "the first tick runs a slice");

        // A quick backlog slice: the next backlog slice is due after the 1s
        // minimum, and a tick in between waits for the same spacing.
        let end = t0 + Duration::from_millis(50);
        state.record_slice(t0, end, &backlog_slice(true), false);
        assert_eq!(
            state.backlog_deadline(),
            Some(end + ZOMBIE_BACKLOG_SWEEP_INTERVAL)
        );
        for stats_tick in [false, true] {
            assert!(!state.slice_due(end + Duration::from_millis(999), stats_tick));
            assert!(state.slice_due(end + Duration::from_millis(1000), stats_tick));
        }

        // A slow backlog slice: not due, from EITHER caller, until four times
        // its duration has passed.
        let start = end + Duration::from_secs(2);
        let end = start + Duration::from_secs(3);
        state.record_slice(start, end, &backlog_slice(true), false);
        for stats_tick in [false, true] {
            assert!(
                !state.slice_due(end + Duration::from_millis(11_999), stats_tick),
                "stats_tick={stats_tick}: a slice after a backlog slice waits"
            );
            assert!(state.slice_due(end + Duration::from_secs(12), stats_tick));
        }

        // A slow TICK slice that leaves a backlog: the next tick keeps the 30s
        // cadence even though 4× the slice is longer than the rest of the
        // interval, while a backlog slice still waits for the spacing.
        let start = end + Duration::from_secs(30);
        let end = start + Duration::from_millis(6_400);
        state.record_slice(start, end, &backlog_slice(true), true);
        let next_tick = start + Duration::from_secs(30);
        assert!(
            state.slice_due(next_tick, true),
            "tick after tick: main's cadence"
        );
        assert!(
            !state.slice_due(next_tick, false),
            "a backlog slice after a tick slice waits the 25.6s spacing"
        );
        assert!(state.slice_due(end + Duration::from_millis(25_600), false));

        // A slice that clears the backlog: only the tick runs the next one.
        state.record_slice(end, end, &backlog_slice(false), true);
        assert!(state.backlog_deadline().is_none());
        assert!(!state.slice_due(end + Duration::from_secs(3600), false));
        assert!(state.slice_due(end + Duration::from_secs(30), true));
    }

    /// If the next slice instant is not representable, backlog slices fail
    /// closed (none runs, no timer) rather than to zero spacing, but the stats
    /// tick still runs, and once it has, the normal schedule resumes.
    #[test]
    fn unrepresentable_spacing_suppresses_backlog_slices_not_the_tick() {
        let t0 = Instant::now();
        let mut state = ZombieSweepState::new(t0);
        state.record_slice(t0, t0, &backlog_slice(true), false);
        state.last_slice_took = Duration::MAX;
        assert!(state.next_slice_at().is_none());
        assert!(state.backlog_deadline().is_none(), "no timer");
        let later = t0 + Duration::from_secs(86_400);
        assert!(
            !state.slice_due(later, false),
            "backlog slices stay suppressed"
        );
        assert!(state.slice_due(later, true), "the stats tick still sweeps");

        // The tick's slice restores the normal schedule.
        let end = later + Duration::from_millis(50);
        state.record_slice(later, end, &backlog_slice(true), true);
        assert!(state.next_slice_at().is_some());
        assert!(!state.slice_due(end + Duration::from_millis(999), false));
        assert!(state.slice_due(end + ZOMBIE_BACKLOG_SWEEP_INTERVAL, false));
    }

    /// One slice as run by `simulate_event_loop`.
    #[derive(Clone, Copy, Debug)]
    struct SimSlice {
        start: Instant,
        end: Instant,
        tick: bool,
    }

    // The event loop's `STATS_LOG_INTERVAL`.
    const STATS_TICK: Duration = Duration::from_secs(30);

    /// Interleave the event loop's two call sites exactly as it does: the 30s
    /// stats tick (`slice_due(now, true)`, with the tick timer reset before the
    /// sweep) and the check after every event (`slice_due(now, false)`), with
    /// an event every 100ms and every slice taking `slice_took`.
    fn simulate_event_loop(
        slice_took: Duration,
        backlog: bool,
        horizon: Duration,
    ) -> Vec<SimSlice> {
        const EVENT_EVERY: Duration = Duration::from_millis(100);
        let t0 = Instant::now();
        let mut state = ZombieSweepState::new(t0);
        let mut last_stats_log = t0;
        let mut now = t0;
        let mut slices = Vec::new();
        let mut run = |state: &mut ZombieSweepState, now: &mut Instant, tick: bool| {
            let end = *now + slice_took;
            state.record_slice(*now, end, &backlog_slice(backlog), tick);
            slices.push(SimSlice {
                start: *now,
                end,
                tick,
            });
            *now = end;
        };
        while now < t0 + horizon {
            now += EVENT_EVERY;
            if now.saturating_duration_since(last_stats_log) > STATS_TICK {
                last_stats_log = now;
                if state.slice_due(now, true) {
                    run(&mut state, &mut now, true);
                }
            } else if state.slice_due(now, false) {
                run(&mut state, &mut now, false);
            }
        }
        slices
    }

    /// Whichever call site runs them, consecutive slices keep their spacing:
    /// two tick slices start at least 30s apart, and any other pair is at least
    /// `slice_spacing` apart. So no slice runs straight after another, the tick
    /// keeps main's 30s cadence even for slow slices, and loop share stays within
    /// main's worst case (6.4s every 30s).
    #[test]
    fn stats_tick_and_backlog_slices_keep_the_spacing() {
        let horizon = Duration::from_secs(3600);
        let main_worst_share = 6.4 / 30.0 + 0.001;
        for backlog in [true, false] {
            for took_ms in [50, 250, 1_000, 5_000, 6_000, 6_200, 6_400] {
                let took = Duration::from_millis(took_ms);
                let slices = simulate_event_loop(took, backlog, horizon);
                let ctx = format!("backlog={backlog} took={took_ms}ms");
                assert!(slices.len() > 1, "{ctx}: slices must run");
                for pair in slices.windows(2) {
                    let (prev, next) = (pair[0], pair[1]);
                    if prev.tick && next.tick {
                        assert!(
                            next.start.saturating_duration_since(prev.start) >= STATS_TICK,
                            "{ctx}: two tick slices started less than 30s apart"
                        );
                    } else {
                        let gap = next.start.saturating_duration_since(prev.end);
                        assert!(
                            gap >= slice_spacing(took),
                            "{ctx}: a slice started {gap:?} after the previous ended, \
                             less than the required {:?}",
                            slice_spacing(took)
                        );
                    }
                }
                let busy: Duration = slices.iter().map(|s| s.end - s.start).sum();
                let share = busy.as_secs_f64() / horizon.as_secs_f64();
                assert!(
                    share <= main_worst_share,
                    "{ctx}: sweep slices used {:.1}% of the loop",
                    share * 100.0
                );
                if !backlog {
                    assert!(
                        slices.iter().all(|s| s.tick),
                        "{ctx}: without a backlog only the tick sweeps"
                    );
                    assert!(
                        (110..=121).contains(&slices.len()),
                        "{ctx}: one slice per 30s tick, as on main, got {}",
                        slices.len()
                    );
                }
            }
        }

        // Quick drops drain far faster than the stats tick alone.
        let quick = simulate_event_loop(Duration::from_millis(50), true, horizon);
        assert!(
            quick.len() > 3000,
            "a backlog of quick drops runs a slice about every second, got {}",
            quick.len()
        );
    }

    /// A backlog drains with no events at all: the timer arm in `next_wake`
    /// wakes the loop at each slice deadline. On a paused runtime the test
    /// would time out, virtually, if nothing woke it.
    #[tokio::test(start_paused = true)]
    async fn backlog_drains_with_no_events() {
        let cm = crate::ring::ConnectionManager::test_default();
        let created = Instant::now();
        let mut connections = BTreeMap::new();
        for port in 1..=200u16 {
            let (sender, _rx) = mpsc::channel(1);
            connections.insert(
                SocketAddr::from(([192, 0, 2, 1], port)),
                ConnectionEntry {
                    sender,
                    pub_key: None,
                    connection_id: 1,
                    created_at: created,
                    link_use: LinkUseStamp::new(created),
                    remote_version: None,
                },
            );
        }
        // test_default's transient_ttl is 60s: these are zombies after 180s.
        tokio::time::advance(Duration::from_secs(400)).await;

        fn run_slice(
            connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
            state: &mut ZombieSweepState,
            cm: &crate::ring::ConnectionManager,
            stats_tick: bool,
        ) -> usize {
            let started = Instant::now();
            let (_, plan) = plan_sweep(connections, &[], cm, started);
            let slice = plan.slice(MAX_ZOMBIE_CLEANUP_PER_CYCLE);
            for addr in &slice.reap {
                connections.remove(addr);
            }
            state.record_slice(started, Instant::now(), &slice, stats_tick);
            slice.reap.len()
        }

        // The stats tick runs the first slice.
        let mut state = ZombieSweepState::new(Instant::now());
        assert!(state.slice_due(Instant::now(), true));
        let mut slices = vec![run_slice(&mut connections, &mut state, &cm, true)];
        assert!(state.backlog_deadline().is_some());

        let mut events = futures::stream::pending::<()>();
        let drained = tokio::time::timeout(Duration::from_secs(600), async {
            while !connections.is_empty() {
                match next_wake(&mut events, state.backlog_deadline()).await {
                    LoopWake::ZombieSweepDue => {
                        if state.slice_due(Instant::now(), false) {
                            slices.push(run_slice(&mut connections, &mut state, &cm, false));
                        }
                    }
                    LoopWake::Event(_) => unreachable!("the event stream never yields"),
                }
            }
        })
        .await;
        assert!(
            drained.is_ok(),
            "the backlog must drain without any event waking the loop"
        );
        assert_eq!(slices, vec![64, 64, 64, 8]);
        assert!(state.backlog_deadline().is_none());
    }

    // ---- plan_sweep over real connection entries ----

    /// A transport that is `age_secs` old at `now`, last used `idle_secs` ago.
    fn entry_at(now: Instant, age_secs: u64, idle_secs: u64) -> ConnectionEntry {
        let (sender, _rx) = mpsc::channel(1);
        let created_at = now - Duration::from_secs(age_secs);
        let link_use = LinkUseStamp::new(created_at);
        if idle_secs < age_secs {
            link_use.record(now - Duration::from_secs(idle_secs));
        }
        ConnectionEntry {
            sender,
            pub_key: None,
            connection_id: 1,
            created_at,
            link_use,
            remote_version: None,
        }
    }

    /// The caps `plan_sweep` applies come from the connection manager: with
    /// `max_connections` 8 the global cap is 2, and the per-IP cap is 2. This
    /// fails if `plan_sweep` stops passing either cap.
    #[test]
    fn plan_sweep_applies_both_caps_from_the_connection_manager() {
        let now = Instant::now() + Duration::from_secs(10_000);
        let mut cm = crate::ring::ConnectionManager::test_default();
        // test_default: transient_ttl 60s, so a zombie by age after 180s.
        cm.max_connections = 8;

        let mut connections = BTreeMap::new();
        // Three transports from one IP, all past the 180s age threshold and in
        // use; the youngest is the least recently used.
        for (port, age, idle) in [(1, 220, 5), (2, 210, 6), (3, 200, 7)] {
            connections.insert(
                SocketAddr::from(([203, 0, 113, 7], port)),
                entry_at(now, age, idle),
            );
        }
        // Two from other IPs, used less recently.
        connections.insert(addr("198.51.100.1:1"), entry_at(now, 300, 20));
        connections.insert(addr("198.51.100.2:1"), entry_at(now, 350, 30));
        // One idle zombie and one young transport.
        connections.insert(addr("192.0.2.1:1"), entry_at(now, 400, 400));
        connections.insert(addr("192.0.2.2:1"), entry_at(now, 30, 30));

        let (candidates, plan) = plan_sweep(&connections, &[], &cm, now);
        assert_eq!(candidates.len(), connections.len());
        assert_eq!(plan.zombies, vec![addr("192.0.2.1:1")]);
        assert_eq!(
            plan.kept_for_link_use,
            vec![
                SocketAddr::from(([203, 0, 113, 7], 1)),
                SocketAddr::from(([203, 0, 113, 7], 2)),
            ],
            "the two most recently used fill both the per-IP cap and the global cap of 2"
        );
        assert_eq!(
            plan.over_per_ip_cap, 1,
            "the third transport from one IP, the youngest but least recently used"
        );
        assert_eq!(plan.over_global_cap, 2, "the two from other IPs");
        assert_eq!(
            plan.over_cap,
            vec![
                addr("198.51.100.2:1"),
                addr("198.51.100.1:1"),
                SocketAddr::from(([203, 0, 113, 7], 3)),
            ],
            "over-cap transports are ordered least recently used first"
        );
    }

    /// More zombies than one slice holds are drained by successive slices, each
    /// bounded, until the backlog clears and backlog slices stop.
    #[test]
    fn backlog_drains_across_slices() {
        let now = Instant::now() + Duration::from_secs(10_000);
        let cm = crate::ring::ConnectionManager::test_default();
        let total = MAX_ZOMBIE_CLEANUP_PER_CYCLE + 6;
        let mut connections = BTreeMap::new();
        for i in 0..total {
            let port = u16::try_from(i + 1).unwrap();
            connections.insert(
                SocketAddr::from(([192, 0, 2, 1], port)),
                entry_at(now, 400, 400),
            );
        }

        let mut state = ZombieSweepState::new(now);
        let mut t = now;
        let mut slices = Vec::new();
        loop {
            let (_, plan) = plan_sweep(&connections, &[], &cm, t);
            let slice = plan.slice(MAX_ZOMBIE_CLEANUP_PER_CYCLE);
            for addr in &slice.reap {
                connections.remove(addr);
            }
            let end = t + Duration::from_millis(10);
            state.record_slice(t, end, &slice, false);
            slices.push(slice.reap.len());
            if !slice.backlog {
                break;
            }
            assert!(
                !state.slice_due(
                    end + ZOMBIE_BACKLOG_SWEEP_INTERVAL - Duration::from_millis(1),
                    false
                ),
                "a backlog slice waits the minimum interval"
            );
            t = end + ZOMBIE_BACKLOG_SWEEP_INTERVAL;
            assert!(state.slice_due(t, false), "then it is due");
            assert!(slices.len() < 10, "the backlog must clear");
        }
        assert_eq!(slices, vec![MAX_ZOMBIE_CLEANUP_PER_CYCLE, 6]);
        assert!(connections.is_empty());
        assert!(!state.slice_due(t + Duration::from_secs(3600), false));
    }

    /// The listener must stamp a request before it queues the message for the
    /// event loop, and the connection entry the sweep reads must share that
    /// stamp. Behaviourally this is covered by
    /// `request_waiting_in_the_event_queue_protects_its_transport` and the
    /// simulation test `test_gateway_zombie_sweep_keeps_unjoined_peers_live_link`;
    /// this pin checks the wiring in `handle_successful_connection`, which only
    /// the full node build runs. Cross-file scrapes, with the stamp call required
    /// at statement position so a commented-out call fails.
    #[test]
    fn listener_stamps_requests_before_queueing_them() {
        const PROTOC: &str = include_str!("../p2p_protoc.rs");
        const LIFECYCLE: &str = include_str!("connection_lifecycle.rs");
        const CALL: &str = "zombie_sweep::record_link_use_request(";

        let listener_at = find_unique(PROTOC, "async fn peer_connection_listener(");
        let (open, close) = block_span(PROTOC, listener_at);
        let listener = &PROTOC[open..close];
        let stamp_at = listener
            .match_indices(CALL)
            .find(|(i, _)| {
                let line_start = listener[..*i].rfind('\n').map_or(0, |n| n + 1);
                listener[line_start..*i].trim().is_empty()
            })
            .map(|(i, _)| i)
            .unwrap_or_else(|| panic!("peer_connection_listener must call {CALL}..) (#5654)"));
        let queue_at = listener
            .find("ConnEvent::InboundMessage(IncomingMessage::with_remote(")
            .expect("the listener must queue inbound messages");
        assert!(
            stamp_at < queue_at,
            "the request must be stamped before it is queued for the event loop"
        );
        assert!(
            squash(&listener[stamp_at..queue_at]).starts_with(
                "zombie_sweep::record_link_use_request(&link_use,&net_message,Instant::now())"
            ),
            "the stamp must record the decoded message on the listener's own stamp"
        );

        let squashed = squash(LIFECYCLE);
        assert!(
            squashed.contains("letlink_use=zombie_sweep::LinkUseStamp::new(now);"),
            "the connection's stamp must be based on its created_at"
        );
        assert!(
            squashed.contains("created_at:now,link_use:link_use.clone(),"),
            "the connection entry must hold the stamp"
        );
        assert!(
            squashed.contains("conn_id,outbound_mix,link_use)"),
            "the listener must be given the same stamp"
        );
        assert!(
            !LIFECYCLE.contains(CALL),
            "stamping at dequeue in handle_transport_event is replaced by the listener"
        );
    }

    /// The `(open, close)` byte offsets of the brace block that starts at the
    /// first `{` at or after `from`, scanned with brace depth like
    /// `operations::connect`'s `fn_body` pin helper, so a nested block does not
    /// end the region early.
    fn block_span(source: &str, from: usize) -> (usize, usize) {
        let open = from
            + source[from..]
                .find('{')
                .unwrap_or_else(|| panic!("no block after offset {from}"));
        let mut depth = 0i32;
        for (i, b) in source.as_bytes().iter().enumerate().skip(open) {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return (open, i);
                    }
                }
                _ => {}
            }
        }
        panic!("unbalanced braces after offset {from}");
    }

    fn find_unique(source: &str, anchor: &str) -> usize {
        assert_eq!(
            source.matches(anchor).count(),
            1,
            "anchor must occur exactly once: {anchor}"
        );
        source.find(anchor).unwrap()
    }

    /// The body of the brace block that starts at the first `{` at or after
    /// `anchor`.
    fn braced_block_after<'a>(source: &'a str, anchor: &str) -> &'a str {
        let at = source
            .find(anchor)
            .unwrap_or_else(|| panic!("anchor not found: {anchor}"));
        let (open, close) = block_span(source, at);
        &source[open + 1..close]
    }

    /// `source` with all whitespace and trailing commas before `)` removed, so a
    /// pin is not sensitive to how rustfmt wraps a call.
    fn squash(source: &str) -> String {
        let compact: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        compact.replace(",)", ")")
    }

    fn calls_at_statement_position(body: &str, call: &str) -> usize {
        body.lines()
            .filter(|l| l.trim_start().starts_with(call))
            .count()
    }

    /// The event loop uses the one scheduling rule at both call sites, wakes for
    /// a backlog deadline, reports on the stats tick, and runs the sweep check
    /// on EVERY iteration: after the `let event = if let Some(result) = result
    /// { .. } else { .. };` binding, not inside its event branch, or a timer
    /// wake would run nothing. The rule itself is tested above; this pins the
    /// wiring. It is a cross-file scrape of `p2p_protoc.rs`.
    #[test]
    fn event_loop_uses_one_sweep_schedule() {
        const SRC: &str = include_str!("../p2p_protoc.rs");
        const SWEEP: &str = "ctx.sweep_zombie_transports(";
        const TICK: &str = "if last_stats_log.elapsed() > STATS_LOG_INTERVAL {";
        const BACKLOG: &str = "} else if zombie_sweep_state.slice_due(Instant::now(), false) {";

        // The wake, then the event binding, then the sweep check, then dispatch.
        let wake = find_unique(
            SRC,
            "zombie_sweep::next_wake(&mut select_stream, zombie_sweep_state.backlog_deadline())",
        );
        let bind = find_unique(SRC, "let event = if let Some(result) = result {");
        let (_, then_close) = block_span(SRC, bind);
        let after_then = &SRC[then_close + 1..];
        assert!(
            after_then.trim_start().starts_with("else {"),
            "the event binding must be `if let Some(result) = result {{ .. }} else {{ .. }}`"
        );
        let (_, else_close) = block_span(SRC, then_close + 1);
        assert!(
            SRC[else_close + 1..].trim_start().starts_with(';'),
            "the else block must end the `let event = ..;` binding"
        );
        let tick = find_unique(SRC, TICK);
        assert!(
            wake < bind && else_close < tick,
            "the sweep check must come after the event binding, not inside its \
             event branch, so a timer wake reaches it"
        );
        let (_, tick_close) = block_span(SRC, tick);
        assert!(
            SRC[tick_close..].starts_with(BACKLOG),
            "the backlog check must be the `else if` of the stats tick"
        );
        let backlog = tick_close;
        let (backlog_open, backlog_close) = block_span(SRC, backlog);
        let dispatch = find_unique(SRC, "            match event {");
        assert!(
            backlog_close < dispatch,
            "the sweep check must run before the event is dispatched"
        );

        let tick_body = &SRC[tick..tick_close];
        let tick_guard = braced_block_after(
            tick_body,
            "if zombie_sweep_state.slice_due(Instant::now(), true)",
        );
        assert_eq!(
            calls_at_statement_position(tick_guard, SWEEP),
            1,
            "the stats tick must sweep only when slice_due(now, true) allows it"
        );
        assert!(
            squash(tick_guard).contains("&mutzombie_sweep_state,true)"),
            "the tick slice must be recorded as a tick slice"
        );
        assert_eq!(
            calls_at_statement_position(tick_body, "zombie_sweep_state.report(Instant::now());"),
            1,
            "the stats tick must report the sweep"
        );

        let backlog_body = &SRC[backlog_open + 1..backlog_close];
        assert_eq!(
            calls_at_statement_position(backlog_body, SWEEP),
            1,
            "a due backlog slice must run the sweep"
        );
        assert!(
            squash(backlog_body).contains("&mutzombie_sweep_state,false)"),
            "the backlog slice must be recorded as a backlog slice"
        );
        assert_eq!(
            SRC.matches(SWEEP).count(),
            2,
            "the sweep runs from exactly the two guarded call sites"
        );
    }

    // ---- request classification ----

    /// Every wire variant, with whether it restamps a transport.
    fn every_variant() -> Vec<(&'static str, NetMessage, bool)> {
        let tx = Transaction::new::<GetMsg>();
        let key = make_contract_key(3);
        let id = *key.id();
        let stream_id = StreamId::next();
        let skip = std::collections::HashSet::new();
        let v1 = NetMessage::V1;
        vec![
            (
                "Connect::Request",
                v1(NetMessageV1::Connect(ConnectMsg::Request {
                    id: tx,
                    payload: ConnectRequest {
                        desired_location: crate::ring::Location::new(0.5),
                        joiner: PeerKeyLocation::random(),
                        ttl: 4,
                        visited: VisitedPeers::new(&tx),
                        uphill_budget: 8,
                    },
                })),
                true,
            ),
            (
                "Connect::Response",
                v1(NetMessageV1::Connect(ConnectMsg::Response {
                    id: tx,
                    payload: ConnectResponse {
                        acceptor: PeerKeyLocation::random(),
                    },
                })),
                false,
            ),
            (
                "Connect::ObservedAddress",
                v1(NetMessageV1::Connect(ConnectMsg::ObservedAddress {
                    id: tx,
                    address: addr("192.0.2.1:1"),
                })),
                false,
            ),
            (
                "Connect::Rejected",
                v1(NetMessageV1::Connect(ConnectMsg::Rejected {
                    id: tx,
                    desired_location: crate::ring::Location::new(0.5),
                })),
                false,
            ),
            (
                "Connect::ConnectFailed",
                v1(NetMessageV1::Connect(ConnectMsg::ConnectFailed {
                    id: tx,
                    failed_acceptor_addr: addr("192.0.2.1:1"),
                })),
                false,
            ),
            (
                "Put::Request",
                v1(NetMessageV1::Put(PutMsg::Request {
                    id: tx,
                    contract: make_test_contract(&[1]),
                    related_contracts: RelatedContracts::default(),
                    value: WrappedState::new(vec![1]),
                    htl: 3,
                    skip_list: skip.clone(),
                })),
                true,
            ),
            (
                "Put::Response",
                v1(NetMessageV1::Put(PutMsg::Response {
                    id: tx,
                    key,
                    hop_count: 0,
                })),
                false,
            ),
            (
                "Put::RequestStreaming",
                v1(NetMessageV1::Put(PutMsg::RequestStreaming {
                    id: tx,
                    stream_id,
                    contract_key: key,
                    total_size: 1,
                    htl: 3,
                    skip_list: skip.clone(),
                    subscribe: false,
                })),
                true,
            ),
            (
                "Put::ResponseStreaming",
                v1(NetMessageV1::Put(PutMsg::ResponseStreaming {
                    id: tx,
                    key,
                    continue_forwarding: false,
                    hop_count: 0,
                })),
                false,
            ),
            (
                "Put::ForwardingAck",
                v1(NetMessageV1::Put(PutMsg::ForwardingAck {
                    id: tx,
                    contract_key: key,
                })),
                false,
            ),
            (
                "Put::Error",
                v1(NetMessageV1::Put(PutMsg::Error {
                    id: tx,
                    cause: String::new(),
                })),
                false,
            ),
            (
                "Put::ProbeRequest",
                v1(NetMessageV1::Put(PutMsg::ProbeRequest {
                    id: tx,
                    contract_key: key,
                    summary: StateSummary::from(vec![1]),
                    htl: 3,
                    skip_list: skip.clone(),
                })),
                true,
            ),
            (
                "Put::ProbeResponse",
                v1(NetMessageV1::Put(PutMsg::ProbeResponse {
                    id: tx,
                    key,
                    holder_found: false,
                    hop_count: 0,
                    holder_summary: None,
                    reverse_delta: None,
                })),
                false,
            ),
            (
                "Put::ProbeReconcile",
                v1(NetMessageV1::Put(PutMsg::ProbeReconcile {
                    id: tx,
                    key,
                    delta: StateDelta::from(vec![1]),
                    htl: 3,
                    skip_list: skip,
                })),
                true,
            ),
            (
                "Get::Request",
                v1(NetMessageV1::Get(GetMsg::Request {
                    id: tx,
                    instance_id: id,
                    fetch_contract: false,
                    htl: 3,
                    visited: VisitedPeers::new(&tx),
                    subscribe: false,
                })),
                true,
            ),
            (
                "Get::Response",
                v1(NetMessageV1::Get(GetMsg::Response {
                    id: tx,
                    instance_id: id,
                    result: GetMsgResult::NotFound,
                    hop_count: 0,
                })),
                false,
            ),
            (
                "Get::ResponseStreaming",
                v1(NetMessageV1::Get(GetMsg::ResponseStreaming {
                    id: tx,
                    instance_id: id,
                    stream_id,
                    key,
                    total_size: 1,
                    includes_contract: false,
                })),
                false,
            ),
            (
                "Get::ResponseStreamingAck",
                v1(NetMessageV1::Get(GetMsg::ResponseStreamingAck {
                    id: tx,
                    stream_id,
                })),
                false,
            ),
            (
                "Get::ForwardingAck",
                v1(NetMessageV1::Get(GetMsg::ForwardingAck {
                    id: tx,
                    instance_id: id,
                })),
                false,
            ),
            (
                "Subscribe::Request",
                v1(NetMessageV1::Subscribe(SubscribeMsg::Request {
                    id: tx,
                    instance_id: id,
                    htl: 3,
                    visited: VisitedPeers::new(&tx),
                    is_renewal: false,
                })),
                true,
            ),
            (
                "Subscribe::Response",
                v1(NetMessageV1::Subscribe(SubscribeMsg::Response {
                    id: tx,
                    instance_id: id,
                    result: SubscribeMsgResult::NotFound,
                    hop_count: 0,
                })),
                false,
            ),
            (
                "Subscribe::Unsubscribe",
                v1(NetMessageV1::Subscribe(SubscribeMsg::Unsubscribe {
                    id: tx,
                    instance_id: id,
                })),
                false,
            ),
            (
                "Subscribe::ForwardingAck",
                v1(NetMessageV1::Subscribe(SubscribeMsg::ForwardingAck {
                    id: tx,
                    instance_id: id,
                })),
                false,
            ),
            (
                "Update::RequestUpdate",
                v1(NetMessageV1::Update(UpdateMsg::RequestUpdate {
                    id: tx,
                    key,
                    related_contracts: RelatedContracts::default(),
                    value: WrappedState::new(vec![1]),
                })),
                true,
            ),
            (
                "Update::BroadcastTo",
                v1(NetMessageV1::Update(UpdateMsg::BroadcastTo {
                    id: tx,
                    key,
                    payload: crate::message::DeltaOrFullState::Delta(vec![1]),
                    sender_summary_bytes: vec![],
                })),
                false,
            ),
            (
                "Update::RequestUpdateStreaming",
                v1(NetMessageV1::Update(UpdateMsg::RequestUpdateStreaming {
                    id: tx,
                    stream_id,
                    key,
                    total_size: 1,
                })),
                true,
            ),
            (
                "Update::BroadcastToStreaming",
                v1(NetMessageV1::Update(UpdateMsg::BroadcastToStreaming {
                    id: tx,
                    stream_id,
                    key,
                    total_size: 1,
                })),
                false,
            ),
            (
                "Update::BroadcastToV2",
                v1(NetMessageV1::Update(UpdateMsg::BroadcastToV2 {
                    id: tx,
                    key,
                    payload: crate::message::DeltaOrFullState::Delta(vec![1]),
                    sender_summary_bytes: vec![],
                    covered: crate::ring::broadcast_coverage::CoveredPeers::empty(),
                })),
                false,
            ),
            (
                "Update::BroadcastToStreamingV2",
                v1(NetMessageV1::Update(UpdateMsg::BroadcastToStreamingV2 {
                    id: tx,
                    stream_id,
                    key,
                    total_size: 1,
                    covered: crate::ring::broadcast_coverage::CoveredPeers::empty(),
                })),
                false,
            ),
            ("Aborted", v1(NetMessageV1::Aborted(tx)), false),
            (
                "NeighborHosting",
                v1(NetMessageV1::NeighborHosting {
                    message: NeighborHostingMessage::HostingAnnounce {
                        added: vec![id],
                        removed: vec![],
                        is_response: false,
                    },
                }),
                false,
            ),
            (
                "InterestSync",
                v1(NetMessageV1::InterestSync {
                    message: InterestMessage::ChangeInterests {
                        added: vec![1],
                        removed: vec![],
                    },
                }),
                false,
            ),
            (
                "ReadyState",
                v1(NetMessageV1::ReadyState { ready: true }),
                false,
            ),
            (
                "SubscribeHint",
                v1(NetMessageV1::SubscribeHint(SubscribeHintMsg {
                    key,
                    holder: PeerKeyLocation::random(),
                })),
                false,
            ),
        ]
    }

    #[test]
    fn classifies_every_wire_variant() {
        let table = every_variant();
        // 5 Connect + 9 Put + 5 Get + 4 Subscribe + 6 Update + 5 non-op.
        assert_eq!(table.len(), 34, "table must list every wire variant");
        for (name, msg, expected) in &table {
            assert_eq!(
                is_link_use_request(msg),
                *expected,
                "{name}: expected link-use request = {expected}"
            );
        }
    }

    // ---- restamp wiring ----

    #[test]
    fn record_link_use_request_stamps_only_requests() {
        let t0 = Instant::now();
        let stamp = LinkUseStamp::new(t0);
        assert_eq!(stamp.last_use(), t0, "a new stamp reads as its base");

        let table = every_variant();
        let find = |name: &str| {
            table
                .iter()
                .find(|(n, _, _)| *n == name)
                .map(|(_, m, _)| m.clone())
                .unwrap()
        };

        // Responses, acks and broadcasts leave the stamp alone.
        let t1 = t0 + Duration::from_secs(10);
        for name in [
            "Get::Response",
            "Get::ForwardingAck",
            "Connect::Response",
            "ReadyState",
            "InterestSync",
        ] {
            assert!(
                !record_link_use_request(&stamp, &find(name), t1),
                "{name} must not stamp"
            );
            assert_eq!(stamp.last_use(), t0, "{name}");
        }

        // A request stamps, through every clone of the stamp.
        let reader = stamp.clone();
        assert!(record_link_use_request(&stamp, &find("Get::Request"), t1));
        assert_eq!(reader.last_use(), t1);

        // The stamp never moves backwards.
        assert!(record_link_use_request(&stamp, &find("Get::Request"), t0));
        assert_eq!(reader.last_use(), t1);
        let t2 = t1 + Duration::from_millis(1500);
        assert!(record_link_use_request(&stamp, &find("Put::Request"), t2));
        assert_eq!(reader.last_use(), t2);
    }

    /// A connection whose `recv` yields queued bytes, then waits forever.
    struct QueuedConnection {
        addr: SocketAddr,
        inbound: mpsc::Receiver<Vec<u8>>,
    }

    impl PeerConnectionApi for QueuedConnection {
        fn remote_addr(&self) -> SocketAddr {
            self.addr
        }

        fn remote_version(&self) -> Option<(u8, u8, u16)> {
            None
        }

        fn send_message(
            &mut self,
            _msg: NetMessage,
        ) -> Pin<Box<dyn Future<Output = Result<usize, TransportError>> + Send + '_>> {
            Box::pin(async { Ok(0) })
        }

        fn recv(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>> {
            Box::pin(async move {
                match self.inbound.recv().await {
                    Some(bytes) => Ok(bytes),
                    None => std::future::pending().await,
                }
            })
        }

        fn set_orphan_stream_registry(
            &mut self,
            _registry: Arc<crate::operations::orphan_streams::OrphanStreamRegistry>,
        ) {
        }

        fn send_stream_data(
            &mut self,
            _stream_id: StreamId,
            _data: bytes::Bytes,
            _metadata: Option<bytes::Bytes>,
            _completion_tx: Option<
                tokio::sync::oneshot::Sender<crate::transport::BroadcastDeliveryOutcome>,
            >,
            _progress: Option<crate::operations::stream_progress::StreamProgressHandle>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn pipe_stream_data(
            &mut self,
            _outbound_stream_id: StreamId,
            _inbound_handle: crate::transport::peer_connection::streaming::StreamHandle,
            _metadata: Option<bytes::Bytes>,
            _progress: Option<crate::operations::stream_progress::StreamProgressHandle>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A request that the transport's listener has received and queued for the
    /// event loop, but that the loop has not yet dequeued (it is busy with
    /// higher-priority work), must already protect its transport from a sweep
    /// planned in between. Runs the real `peer_connection_listener`, holds the
    /// event queue undrained, and plans a sweep over the connection map.
    #[tokio::test(start_paused = true)]
    async fn request_waiting_in_the_event_queue_protects_its_transport() {
        let cm = crate::ring::ConnectionManager::test_default();
        let remote = addr("198.51.100.9:4000");
        let created = Instant::now();
        let link_use = LinkUseStamp::new(created);
        let (sender, commands_rx) = mpsc::channel(10);
        let mut connections = BTreeMap::new();
        connections.insert(
            remote,
            ConnectionEntry {
                sender,
                pub_key: None,
                connection_id: 7,
                created_at: created,
                link_use: link_use.clone(),
                remote_version: None,
            },
        );

        // test_default's transient_ttl is 60s: a zombie by age after 180s.
        tokio::time::advance(Duration::from_secs(400)).await;
        let (_, plan) = plan_sweep(&connections, &[], &cm, Instant::now());
        assert_eq!(plan.zombies, vec![remote], "no request yet: a zombie");

        let (bytes_tx, bytes_rx) = mpsc::channel(4);
        // Capacity 1 and never read: the request stays queued.
        let (events_tx, events_rx) = mpsc::channel(1);
        let listener = tokio::spawn(super::super::peer_connection_listener(
            commands_rx,
            Box::new(QueuedConnection {
                addr: remote,
                inbound: bytes_rx,
            }),
            remote,
            events_tx,
            7,
            Arc::new(crate::node::network_bridge::outbound_message_mix::OutboundMix::new()),
            link_use,
        ));

        let request = every_variant()
            .into_iter()
            .find(|(name, _, _)| *name == "Get::Request")
            .map(|(_, msg, _)| msg)
            .unwrap();
        bytes_tx
            .send(bincode::serialize(&request).unwrap())
            .await
            .unwrap();
        for _ in 0..10_000 {
            if events_rx.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            events_rx.len(),
            1,
            "the listener must have queued the request for the event loop"
        );

        let (candidates, plan) = plan_sweep(&connections, &[], &cm, Instant::now());
        assert_eq!(
            candidates[0].verdict,
            ZombieVerdict::KeepForLinkUse,
            "a queued, not yet dequeued request must count as link use"
        );
        assert!(plan.zombies.is_empty());
        assert_eq!(plan.kept_for_link_use, vec![remote]);

        listener.abort();
        drop(events_rx);
    }
}
