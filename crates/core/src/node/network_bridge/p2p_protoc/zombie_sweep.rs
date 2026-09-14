//! Zombie-transport sweep for [`P2pConnManager`]: which transports that were
//! never promoted to the ring the event loop collects, and when (#5654).
//!
//! A transport the ring never promoted is normally collected once it is old
//! enough. A peer that cannot join the ring still routes through its gateway
//! transport, so that transport is judged by how recently its remote sent a
//! request over it instead ([`TransportActivity::idle`]). Transports kept only
//! for that reason are capped per remote IP and globally, and the oldest are
//! evicted first when a cap is exceeded.

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

/// Most transports from one remote address group (an IPv4 address or an IPv6
/// /64, see [`link_use_exemption_key`]) that may be kept alive by recent
/// requests alone. Two rather than one so that two peers behind one household
/// NAT, or a peer and its own restarted process, are both served. A third or
/// later transport from the same group gets the age rule, as before #5654.
pub(super) const LINK_USE_EXEMPT_PER_IP_CAP: usize = 2;

/// Divisor of `max_connections` giving the most transports, across all remotes,
/// that may be kept alive by recent requests alone. See
/// [`link_use_exempt_global_cap`].
const LINK_USE_EXEMPT_MAX_CONNECTIONS_DIVISOR: usize = 4;

/// Most zombie transports dropped in one sweep slice. Each drop involves
/// topology pruning and orphaned-transaction handling, and can wait up to 100ms
/// on a full per-connection channel, so this bounds event-loop latency per
/// slice (64 × 100ms = ~6.4s worst case), exactly as on the 30s stats tick
/// before #5654.
pub(super) const MAX_ZOMBIE_CLEANUP_PER_CYCLE: usize = 64;

/// Minimum delay between the end of one sweep slice and the start of a backlog
/// slice. See [`backlog_sweep_delay`].
pub(super) const ZOMBIE_BACKLOG_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// A backlog slice waits at least this many times as long as the previous
/// slice took. See [`backlog_sweep_delay`].
const ZOMBIE_BACKLOG_IDLE_FACTOR: u32 = 4;

/// How long to wait after a slice that took `last_slice_took` before running a
/// backlog slice: the longer of [`ZOMBIE_BACKLOG_SWEEP_INTERVAL`] and
/// [`ZOMBIE_BACKLOG_IDLE_FACTOR`] × `last_slice_took`.
///
/// The budget: while a backlog persists, backlog slices occupy at most one
/// fifth of the event loop's time. That is the same fraction as the worst case
/// of the 30s stats-tick sweep (a 6.4s slice every 30s is about 21%). When drops
/// are quick the delay is 1s, so a backlog drains at up to 64 transports per
/// second instead of 64 per 30s; when every drop takes the full 100ms the delay
/// stretches to 25.6s and the drain rate falls back to roughly the stats tick's.
pub(super) fn backlog_sweep_delay(last_slice_took: Duration) -> Duration {
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
/// - IPv4 remotes (including IPv4-mapped IPv6) are grouped by address.
/// - IPv6 remotes are grouped by /64, the prefix conventionally assigned to a
///   single subscriber network.
/// - Loopback remotes are keyed by full socket address, so several local nodes
///   on one host (every simulation and local test network) are not collapsed
///   into one. This mirrors the loopback rule in `Location::from_address`.
///
/// `Location::from_address` masks differently (/24 and /48) because it groups
/// peers for ring placement; this key only has to group one subscriber's
/// transports, so it uses the narrower per-subscriber grouping.
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
/// This runs in `handle_transport_event`, before the dispatch in
/// `node::handle_pure_network_message_v1` decides whether to start a driver.
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

/// Restamp `last_link_use_at` on the transport `remote` arrived over when `msg`
/// is a request from that remote ([`is_link_use_request`]). Returns whether it
/// restamped. Never inserts: a message for an address with no transport entry
/// changes nothing.
///
/// `now` is passed in so tests control the clock. The stamp is a
/// `tokio::time::Instant`, carried forward from `created_at` so the sweep
/// compares two readings of one clock; under the simulation's paused tokio
/// runtime it is virtual time.
pub(super) fn record_link_use_request(
    connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
    remote: Option<SocketAddr>,
    msg: &NetMessage,
    now: Instant,
) -> bool {
    let Some(remote) = remote else {
        return false;
    };
    if !is_link_use_request(msg) {
        return false;
    }
    match connections.get_mut(&remote) {
        Some(entry) => {
            entry.last_link_use_at = now;
            true
        }
        None => false,
    }
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
    pub(super) age: Duration,
    pub(super) verdict: ZombieVerdict,
}

/// Which transports one sweep slice drops, and why.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ZombieSweepPlan {
    /// Transports that would have been kept for recent requests but exceeded
    /// a cap, oldest first. Dropped before `zombies`.
    pub(super) over_cap: Vec<SocketAddr>,
    /// How many of `over_cap` exceeded the per-IP cap.
    pub(super) over_per_ip_cap: usize,
    /// How many of `over_cap` exceeded the global cap.
    pub(super) over_global_cap: usize,
    /// Transports the uncapped rules reap, in map order.
    pub(super) zombies: Vec<SocketAddr>,
    /// Transports kept alive by recent requests after the caps, youngest first.
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
/// Transports kept for recent requests are admitted youngest first: at most
/// `per_ip_cap` per [`link_use_exemption_key`], then at most `global_cap` in
/// total. Anything beyond a cap falls back to the age rule and is reaped, the
/// oldest first. A newcomer is therefore never refused the exemption; it is the
/// longest-held exemptions that give way, so no remote holds one indefinitely by
/// arriving early and staying active (`.claude/rules/code-style.md`, entries
/// refreshed on every use).
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

    // Youngest first; the address breaks ties so the plan is deterministic.
    exempt.sort_by(|a, b| a.age.cmp(&b.age).then_with(|| a.addr.cmp(&b.addr)));

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

    // Oldest first.
    over_cap.sort_by(|a, b| b.age.cmp(&a.age).then_with(|| a.addr.cmp(&b.addr)));
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
            let verdict = zombie_verdict(
                TransportActivity {
                    age,
                    idle: now.saturating_duration_since(entry.last_link_use_at),
                },
                connection_manager.is_in_ring(*addr),
                connection_manager.has_connection_or_pending(*addr),
                is_gateway,
                transient_ttl,
            );
            SweepCandidate {
                addr: *addr,
                age,
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

/// Zombie sweep state for one event loop: when the next backlog slice may run,
/// and a cumulative counter reported at info level so it is visible in release
/// builds.
#[derive(Debug)]
pub(super) struct ZombieSweepState {
    /// Over-cap transports actually dropped, since the event loop started.
    pub(super) cap_evictions_total: u64,
    backlog: bool,
    last_slice_end: Instant,
    last_slice_took: Duration,
}

impl ZombieSweepState {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            cap_evictions_total: 0,
            backlog: false,
            last_slice_end: now,
            last_slice_took: Duration::ZERO,
        }
    }

    /// Record a slice that ran from `started` to `ended`.
    pub(super) fn record_slice(&mut self, started: Instant, ended: Instant, slice: &ZombieSlice) {
        self.cap_evictions_total = self
            .cap_evictions_total
            .saturating_add(slice.over_cap_dropped as u64);
        self.backlog = slice.backlog;
        self.last_slice_took = ended.saturating_duration_since(started);
        self.last_slice_end = ended;
    }

    /// Whether the event loop should run a backlog slice now: transports remain
    /// due, and [`backlog_sweep_delay`] has passed since the last slice ended.
    pub(super) fn backlog_slice_due(&self, now: Instant) -> bool {
        self.backlog
            && now.saturating_duration_since(self.last_slice_end)
                > backlog_sweep_delay(self.last_slice_took)
    }
}

impl P2pConnManager {
    /// Run one zombie sweep slice: plan it with [`plan_sweep`], drop at most
    /// [`MAX_ZOMBIE_CLEANUP_PER_CYCLE`] transports (over-cap evictions first),
    /// and record the slice in `state` so the event loop knows whether and when
    /// to run a backlog slice ([`ZombieSweepState::backlog_slice_due`]).
    ///
    /// Uses `drop_zombie_connection` (non-blocking `try_send`) rather than
    /// `drop_connection_by_addr` to avoid a circular deadlock with the handshake
    /// driver (#3519).
    pub(super) async fn sweep_zombie_transports(
        &mut self,
        handshake_cmd_sender: &HandshakeCommandSender,
        state: &mut ZombieSweepState,
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
        state.record_slice(started, Instant::now(), &slice);

        if !plan.kept_for_link_use.is_empty() || !plan.over_cap.is_empty() {
            tracing::info!(
                link_use_exempt = plan.kept_for_link_use.len(),
                link_use_exempt_global_cap =
                    link_use_exempt_global_cap(connection_manager.max_connections),
                link_use_exempt_per_ip_cap = LINK_USE_EXEMPT_PER_IP_CAP,
                over_per_ip_cap = plan.over_per_ip_cap,
                over_global_cap = plan.over_global_cap,
                over_cap_dropped = slice.over_cap_dropped,
                cap_evictions_total = state.cap_evictions_total,
                "Zombie sweep: transports kept alive by recent requests"
            );
        }
        if !slice.reap.is_empty() {
            tracing::info!(
                zombie_count = slice.reap.len(),
                zombies_due = plan.due(),
                over_cap = plan.over_cap.len(),
                backlog = slice.backlog,
                "Cleaning up zombie transports (not promoted to ring)"
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

    #[test]
    fn idle_time_is_capped_at_age() {
        assert!(!is_zombie(used(60, 500), false, false, false, TTL));
    }

    // ---- caps ----

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn exempt(a: &str, age_secs: u64) -> SweepCandidate {
        SweepCandidate {
            addr: addr(a),
            age: Duration::from_secs(age_secs),
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
    fn per_ip_cap_evicts_oldest_from_that_ip() {
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
    fn global_cap_evicts_oldest_overall() {
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
            "over-cap evictions are ordered oldest first"
        );
        assert_eq!(plan.over_global_cap, 2);
        assert_eq!(
            plan.kept_for_link_use,
            vec![addr("198.51.100.2:1"), addr("198.51.100.4:1")]
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
            verdict: ZombieVerdict::Reap,
        }];
        candidates.push(exempt("198.51.100.1:1", 500));
        candidates.push(exempt("198.51.100.2:1", 100));
        candidates.push(SweepCandidate {
            addr: addr("192.0.2.2:1"),
            age: Duration::from_secs(60),
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
        state.record_slice(t, t, &first);
        assert_eq!(state.cap_evictions_total, 3);

        // Next sweep: the three dropped are gone; the fourth is still over cap.
        let remaining: Vec<SweepCandidate> = (0..5u8)
            .map(|i| exempt(&format!("198.51.100.{}:1", i + 1), 100 + u64::from(i)))
            .filter(|c| !first.reap.contains(&c.addr))
            .collect();
        let second = plan_zombie_sweep(remaining, 2, 1).slice(3);
        assert_eq!(second.over_cap_dropped, 1);
        state.record_slice(t, t, &second);
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
    fn backlog_delay_bounds_the_sweep_duty_cycle() {
        assert_eq!(
            backlog_sweep_delay(Duration::ZERO),
            ZOMBIE_BACKLOG_SWEEP_INTERVAL
        );
        assert_eq!(
            backlog_sweep_delay(Duration::from_millis(250)),
            ZOMBIE_BACKLOG_SWEEP_INTERVAL,
            "a quick slice waits the minimum interval"
        );
        assert_eq!(
            backlog_sweep_delay(Duration::from_secs(3)),
            Duration::from_secs(12),
            "a slow slice waits four times as long as it took"
        );
        assert_eq!(
            backlog_sweep_delay(Duration::from_millis(6400)),
            Duration::from_millis(25_600),
            "a worst-case 64 × 100ms slice waits 25.6s"
        );
    }

    #[test]
    fn backlog_slice_due_only_with_backlog_and_after_the_delay() {
        let t0 = Instant::now();
        let mut state = ZombieSweepState::new(t0);
        assert!(
            !state.backlog_slice_due(t0 + Duration::from_secs(60)),
            "no backlog, no backlog slice"
        );

        let backlog = ZombieSlice {
            reap: vec![addr("192.0.2.1:1")],
            over_cap_dropped: 0,
            backlog: true,
        };
        // A quick slice: due just after the 1s minimum.
        let end = t0 + Duration::from_millis(50);
        state.record_slice(t0, end, &backlog);
        assert!(!state.backlog_slice_due(end + Duration::from_millis(1000)));
        assert!(state.backlog_slice_due(end + Duration::from_millis(1001)));

        // A slow slice: due only after four times its duration.
        let start = end + Duration::from_secs(2);
        let end = start + Duration::from_secs(3);
        state.record_slice(start, end, &backlog);
        assert!(!state.backlog_slice_due(end + Duration::from_secs(12)));
        assert!(state.backlog_slice_due(end + Duration::from_millis(12_001)));

        // A slice that clears the backlog stops further backlog slices.
        let cleared = ZombieSlice {
            reap: vec![],
            over_cap_dropped: 0,
            backlog: false,
        };
        state.record_slice(end, end, &cleared);
        assert!(!state.backlog_slice_due(end + Duration::from_secs(3600)));
    }

    // ---- plan_sweep over real connection entries ----

    /// A transport that is `age_secs` old at `now`, last used `idle_secs` ago.
    fn entry_at(now: Instant, age_secs: u64, idle_secs: u64) -> ConnectionEntry {
        let (sender, _rx) = mpsc::channel(1);
        ConnectionEntry {
            sender,
            pub_key: None,
            connection_id: 1,
            created_at: now - Duration::from_secs(age_secs),
            last_link_use_at: now - Duration::from_secs(idle_secs),
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
        // Three young transports from one IP, all in use and past the 180s age
        // threshold.
        for (port, age) in [(1, 200), (2, 210), (3, 220)] {
            connections.insert(
                SocketAddr::from(([203, 0, 113, 7], port)),
                entry_at(now, age, 10),
            );
        }
        // Two older ones from other IPs, in use.
        connections.insert(addr("198.51.100.1:1"), entry_at(now, 300, 10));
        connections.insert(addr("198.51.100.2:1"), entry_at(now, 350, 10));
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
            "the two youngest fill both the per-IP cap and the global cap of 2"
        );
        assert_eq!(plan.over_per_ip_cap, 1, "the third transport from one IP");
        assert_eq!(plan.over_global_cap, 2, "the two from other IPs");
        assert_eq!(
            plan.over_cap,
            vec![
                addr("198.51.100.2:1"),
                addr("198.51.100.1:1"),
                SocketAddr::from(([203, 0, 113, 7], 3)),
            ],
            "over-cap transports are ordered oldest first"
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
            state.record_slice(t, end, &slice);
            slices.push(slice.reap.len());
            if !slice.backlog {
                break;
            }
            assert!(
                !state.backlog_slice_due(end + ZOMBIE_BACKLOG_SWEEP_INTERVAL),
                "a backlog slice waits the minimum interval"
            );
            t = end + ZOMBIE_BACKLOG_SWEEP_INTERVAL + Duration::from_millis(1);
            assert!(state.backlog_slice_due(t), "then it is due");
            assert!(slices.len() < 10, "the backlog must clear");
        }
        assert_eq!(slices, vec![MAX_ZOMBIE_CLEANUP_PER_CYCLE, 6]);
        assert!(connections.is_empty());
        assert!(!state.backlog_slice_due(t + Duration::from_secs(3600)));
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

    fn entry(now: Instant) -> ConnectionEntry {
        let (sender, _rx) = mpsc::channel(1);
        ConnectionEntry {
            sender,
            pub_key: None,
            connection_id: 1,
            created_at: now,
            last_link_use_at: now,
            remote_version: None,
        }
    }

    #[test]
    fn record_link_use_request_restamps_only_requests() {
        let remote = addr("198.51.100.9:4000");
        let t0 = Instant::now();
        let mut connections = BTreeMap::new();
        connections.insert(remote, entry(t0));

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
                !record_link_use_request(&mut connections, Some(remote), &find(name), t1),
                "{name} must not restamp"
            );
            assert_eq!(connections[&remote].last_link_use_at, t0, "{name}");
        }

        // A request restamps.
        assert!(record_link_use_request(
            &mut connections,
            Some(remote),
            &find("Get::Request"),
            t1
        ));
        assert_eq!(connections[&remote].last_link_use_at, t1);
        assert_eq!(
            connections[&remote].created_at, t0,
            "restamping must not touch created_at"
        );

        // No remote, or an address without a transport: nothing changes and
        // nothing is inserted.
        let t2 = t1 + Duration::from_secs(10);
        assert!(!record_link_use_request(
            &mut connections,
            None,
            &find("Get::Request"),
            t2
        ));
        assert!(!record_link_use_request(
            &mut connections,
            Some(addr("198.51.100.10:4000")),
            &find("Get::Request"),
            t2
        ));
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[&remote].last_link_use_at, t1);
    }
}
