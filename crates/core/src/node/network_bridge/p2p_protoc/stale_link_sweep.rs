//! Stale-link sweep for [`P2pConnManager`]: drop a link that has gone silent
//! while requests are waiting on it.
//!
//! Neither transport tells a node that its peer closed the link, and the
//! legacy UDP idle timeout is 120 s. A request sent into such a link waits for
//! its own 60 s deadline before the operation retries elsewhere. This sweep
//! closes the link as soon as it has been silent for a window `W` while at
//! least one request is waiting on it, which fails those requests over
//! (`handle_orphaned_transactions`) after about `W` instead of 60 s.
//!
//! Silent means both at once:
//!
//! - at least one transaction is awaiting a reply from the peer, and
//! - the connection has received no authenticated packet of any kind (pings,
//!   pongs, acks and stream fragments included) for `W`, measured from the
//!   later of the last receipt and the first sweep that saw a request waiting,
//!   and
//! - the connection's listener is not holding a received message that the
//!   event loop has yet to take (see below).
//!
//! # Why a held message counts as life
//!
//! A UDP connection's receipt stamp is written where the connection is read
//! (`PeerConnection::recv`), and that is polled only by the connection's
//! listener task, which forwards each message to the event loop over a bounded
//! channel. While the loop is behind, the listener waits in that send and stops
//! reading, so the stamp of a peer that is sending us traffic stops moving
//! although the peer is alive; the peers awaited are exactly the ones that
//! just sent something. The listener therefore marks itself
//! (`LinkUseStamp::begin_forward`) before the send and records when it ended,
//! and a peer whose listener is forwarding now, or finished forwarding within
//! `W`, is not silent. QUIC stamps come from the ack-watch task and do not
//! have this problem; the extra signal is harmless there.
//!
//! # Windows against the transport keepalives
//!
//! A UDP peer pings every 5 s, but backs off when ITS pings go unanswered
//! (counted since ITS last inbound packet): after more than 5 in a row the
//! interval doubles to 10 s and then to `MAX_KEEPALIVE_INTERVAL` (15 s). That
//! happens to a live peer whose own listener is blocked (the mirror of the
//! frozen-stamp case above: its inbound stamp freezes, so it counts our
//! answers as missing) and on a link that loses our pongs one way. So a live
//! UDP peer can be quiet for up to 15 s, and the UDP window is sized from that
//! constant: `MAX_KEEPALIVE_INTERVAL + UDP_WINDOW_MARGIN` (19 s), not from the
//! 5 s base interval. A dead peer is dropped at that window, after its pings
//! have already begun to back off. A UDP connection that has carried a QUIC
//! path gets the QUIC window (34 s), because its stamp includes QUIC receipts
//! and those arrive at the QUIC keepalive (15 s).
//!
//! A peer with nothing waiting on it is never examined, so a peer that simply
//! had nothing to say is not silent.
//!
//! The decision is a pure function ([`decide`]) over injected time, so every
//! guard is unit-tested. Guards, all in [`decide`]:
//!
//! - **Tick gap.** A tick that runs later than `W / 2` after the previous one
//!   (event-loop stall, suspended machine) decides nothing; the next tick
//!   re-measures after the loop has processed the packets that queued up.
//! - **Majority.** If more than half of the peers that have requests waiting
//!   (and at least [`MAJORITY_GUARD_MIN_PEERS`]) look silent at once, the
//!   fault is more likely ours than theirs, and nothing is dropped.
//! - **Rate.** At most [`MAX_DROPS_PER_TICK`] links are dropped per tick.
//!
//! Wire-compatible: purely local policy, no new message.

use super::*;
use std::collections::HashMap;

/// How often the sweep runs. Much shorter than the 30 s stats tick, which
/// would make a window of about 19 s coarse. Cheap: one pass over
/// `connections` and the live-transaction index.
pub(super) const STALE_LINK_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

/// The longest a live UDP peer stays quiet: its keepalive interval after
/// backing off (`peer_connection::MAX_KEEPALIVE_INTERVAL`, 15 s).
pub(super) const UDP_MAX_QUIET: Duration =
    crate::transport::peer_connection::MAX_KEEPALIVE_INTERVAL;

/// Added to [`UDP_MAX_QUIET`] for the UDP window: one round trip and a lost
/// packet's worth of slack.
pub(super) const UDP_WINDOW_MARGIN: Duration = Duration::from_secs(4);

/// Keepalive interval of a QUIC-native connection.
pub(super) const QUIC_KEEPALIVE_INTERVAL: Duration =
    crate::transport::quic::config::KEEPALIVE_INTERVAL;

/// Round-trip time assumed for every link. Neither transport exposes its
/// measured srtt to the node, so v1 uses one fixed value; it only sets the
/// margin on top of two missed keepalives.
pub(super) const ASSUMED_SRTT: Duration = Duration::from_secs(1);

/// Fixed margin added on top of two keepalives and two round trips.
pub(super) const WINDOW_MARGIN: Duration = Duration::from_secs(2);

/// The window is never shorter than this, whatever the keepalive.
pub(super) const MIN_WINDOW: Duration = Duration::from_secs(12);

/// Most links one tick may drop, so a wrong decision cannot empty the ring.
pub(super) const MAX_DROPS_PER_TICK: usize = 2;

/// The majority guard needs at least this many peers with requests waiting
/// before it can judge a fraction of them.
pub(super) const MAJORITY_GUARD_MIN_PEERS: usize = 3;

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static OVERRIDE_FOR_THREAD: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Test-only, per thread: force the sweep on or off for simulations run on
/// this thread, until the returned guard is dropped. Without an override the
/// sweep runs when the transport has a keepalive ([`transport_has_keepalive`]).
#[cfg(any(test, feature = "testing"))]
#[must_use = "the override ends when the guard is dropped"]
pub struct StaleLinkSweepGuard(Option<bool>);

#[cfg(any(test, feature = "testing"))]
impl StaleLinkSweepGuard {
    /// Force the sweep on (`true`) or off (`false`) for this thread.
    pub fn force(on: bool) -> Self {
        Self(OVERRIDE_FOR_THREAD.with(|o| o.replace(Some(on))))
    }
}

#[cfg(any(test, feature = "testing"))]
impl Drop for StaleLinkSweepGuard {
    fn drop(&mut self) {
        // Restore what was there, so nested guards unwind in order.
        OVERRIDE_FOR_THREAD.with(|o| o.set(self.0));
    }
}

fn override_for_thread() -> Option<bool> {
    #[cfg(any(test, feature = "testing"))]
    {
        OVERRIDE_FOR_THREAD.with(|o| o.get())
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        None
    }
}

/// Whether the transport of the node listening on `listen` sends keepalives.
///
/// This is the real reason the sweep needs a switch: without a keepalive an
/// idle link's receipt stamp never advances, so "silent" only means "idle" and
/// the sweep would drop live links that have a slow request waiting. Simulated
/// connections run on `VirtualTime`, whose `supports_keepalive()` is `false`
/// (and the direct runner's relaxed timers, `SimulationTransportOpt`, turn it
/// off too), so a node is simulated when its address is registered with an
/// in-memory network or that option is set. Decided from the transport, not
/// from a cargo feature: feature unification has put `testing` into shipped
/// binaries before.
pub(super) fn transport_has_keepalive(listen: SocketAddr) -> bool {
    !(crate::config::SimulationTransportOpt::is_enabled()
        || crate::transport::in_memory_socket::get_socket_virtual_time(&listen).is_some())
}

/// Whether the sweep is active for the node listening on `listen`, and why.
pub(super) fn activation(listen: SocketAddr) -> (bool, &'static str) {
    match override_for_thread() {
        Some(true) => (true, "forced on by a test"),
        Some(false) => (false, "forced off by a test"),
        None if transport_has_keepalive(listen) => (true, "transport has a keepalive"),
        None => (false, "transport has no keepalive (simulation)"),
    }
}

static UNRESPONSIVE_DROPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many links this process has dropped as unresponsive since it started.
/// Counted where the decision is acted on, and logged with each drop as
/// `total`, so the field rate of misfires and real dead links alike is
/// readable. Each drop also reaches telemetry as a `Disconnected` event with
/// reason `Unresponsive` (the reason is produced nowhere else). The getter is
/// for tests; production reads the log field and the telemetry event.
#[cfg(test)]
pub(crate) fn unresponsive_drops_total() -> u64 {
    UNRESPONSIVE_DROPS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Act on a decision to drop `addr`: count it and start the re-dial cooldown.
/// Returns the running total.
///
/// The cooldown is the existing recently-failed-address backoff
/// (`ConnectionManager::record_failed_addr`): CONNECT requests this node starts
/// treat the address as already visited for 5 minutes, doubling on each repeat
/// up to an hour, and a connection that is established to the address (it
/// dialled us, or a dial succeeds) clears it. It fits because a wrongly dropped
/// peer then cannot be picked again by this node's acquisition for minutes,
/// while one that comes back by itself is not held out. It does not stop a
/// peer dialling us, nor a direct reconnect that bypasses CONNECT.
pub(super) fn note_unresponsive_drop(
    connection_manager: &crate::ring::ConnectionManager,
    addr: SocketAddr,
) -> u64 {
    connection_manager.record_failed_addr(addr);
    UNRESPONSIVE_DROPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
}

/// Which transport carries a link; sets its keepalive and so its window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LinkKind {
    Udp,
    Quic,
}

/// The window class of a connection. A UDP connection that has carried a QUIC
/// path counts as QUIC: its receipt stamp includes that path's receipts, which
/// arrive at the QUIC keepalive (15 s), so a shorter UDP window would drop a
/// healthy link between two of them.
pub(super) fn link_kind(native: bool, quic_path_seen: bool) -> LinkKind {
    if native || quic_path_seen {
        LinkKind::Quic
    } else {
        LinkKind::Udp
    }
}

/// UDP: the longest quiet a live peer can show plus a margin, `15 s + 4 s =
/// 19 s` (see the module doc). QUIC: `max(2 x keepalive + 2 x srtt + 2 s,
/// 12 s) = 34 s`, longer than two missed fixed-interval keepalives.
pub(super) fn silence_window(kind: LinkKind) -> Duration {
    match kind {
        LinkKind::Udp => UDP_MAX_QUIET + UDP_WINDOW_MARGIN,
        LinkKind::Quic => {
            (QUIC_KEEPALIVE_INTERVAL * 2 + ASSUMED_SRTT * 2 + WINDOW_MARGIN).max(MIN_WINDOW)
        }
    }
}

/// What the sweep knows about one connected peer at a tick.
#[derive(Clone, Copy, Debug)]
pub(super) struct PeerSample {
    pub(super) addr: SocketAddr,
    pub(super) kind: LinkKind,
    /// Time since the connection last received an authenticated packet.
    pub(super) since_last_receipt: Duration,
    /// How long since the connection's listener last held a received message
    /// for the event loop: zero while it holds one now, and `Duration::MAX`
    /// if it never has. While the loop is behind the listener stops reading
    /// the connection, so `since_last_receipt` freezes for a live peer.
    pub(super) since_listener_progress: Duration,
    /// Transactions awaiting a reply from this peer.
    pub(super) outstanding: usize,
    /// How long requests have been continuously waiting on this peer, as far
    /// as the sweep has observed (see [`StaleLinkSweep`]).
    pub(super) outstanding_for: Duration,
}

impl PeerSample {
    /// How long the peer has been silent while we were waiting on it: the
    /// shorter of the time since its last packet and the time since we began
    /// waiting, so an old stamp from before the request does not count.
    fn silence(&self) -> Duration {
        self.since_last_receipt
            .min(self.outstanding_for)
            .min(self.since_listener_progress)
    }

    fn is_stale(&self) -> bool {
        self.outstanding > 0 && self.silence() >= silence_window(self.kind)
    }
}

/// Why a tick dropped nothing, when it had stale links to consider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Suppressed {
    /// The tick ran too long after the previous one.
    TickGap,
    /// Too many peers looked silent at once.
    Majority,
}

/// The outcome of one tick.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Decision {
    /// Links to drop, longest silent first, at most [`MAX_DROPS_PER_TICK`].
    pub(super) drops: Vec<SocketAddr>,
    /// Set when a guard stopped the tick.
    pub(super) suppressed: Option<Suppressed>,
}

/// Decide which links to drop at `now`. `last_tick` is the previous tick, if
/// any.
pub(super) fn decide(now: Instant, last_tick: Option<Instant>, peers: &[PeerSample]) -> Decision {
    let waiting: Vec<&PeerSample> = peers.iter().filter(|p| p.outstanding > 0).collect();
    let mut stale: Vec<&PeerSample> = waiting.iter().copied().filter(|p| p.is_stale()).collect();
    if stale.is_empty() {
        return Decision::default();
    }

    // Tick gap: judged against the shortest window among the peers waited on,
    // so a stall that could fool any one of them suppresses the tick.
    if let Some(last) = last_tick {
        let gap = now.saturating_duration_since(last);
        let min_window = waiting
            .iter()
            .map(|p| silence_window(p.kind))
            .min()
            .unwrap_or(MIN_WINDOW);
        if gap > min_window / 2 {
            return Decision {
                drops: Vec::new(),
                suppressed: Some(Suppressed::TickGap),
            };
        }
    }

    // Majority: strictly more than half of the peers with requests waiting.
    if waiting.len() >= MAJORITY_GUARD_MIN_PEERS && stale.len() * 2 > waiting.len() {
        return Decision {
            drops: Vec::new(),
            suppressed: Some(Suppressed::Majority),
        };
    }

    // Longest silent first; address as a deterministic tie-break.
    stale.sort_by(|a, b| b.silence().cmp(&a.silence()).then(a.addr.cmp(&b.addr)));
    Decision {
        drops: stale
            .into_iter()
            .take(MAX_DROPS_PER_TICK)
            .map(|p| p.addr)
            .collect(),
        suppressed: None,
    }
}

impl P2pConnManager {
    /// Run one stale-link tick when due: drop at most [`MAX_DROPS_PER_TICK`]
    /// links that [`decide`] judges silent while requests wait on them.
    ///
    /// Runs on the event-loop task and calls `drop_connection_by_addr`
    /// directly, the same path the `NodeEvent::DropConnection` handler takes
    /// (prune, `handle_orphaned_transactions`, remove). No channel is sent on.
    pub(super) async fn run_stale_link_sweep(
        &mut self,
        sweep: &mut StaleLinkSweep,
        handshake_cmd_sender: &HandshakeCommandSender,
        is_awaiting_reply: impl FnMut(&crate::message::Transaction) -> bool,
    ) {
        let now = Instant::now();
        if !sweep.due(now) {
            return;
        }
        let awaiting = self
            .bridge
            .op_manager
            .ring
            .live_tx_tracker
            .awaiting_by_peer(is_awaiting_reply);
        let observed: Vec<PeerObservation> = self
            .connections
            .iter()
            .filter_map(|(addr, entry)| {
                // A transport that reports no receipt time cannot be judged.
                let receipt = entry.receipt.as_ref()?;
                // The receipt stamp of a UDP connection that has carried a QUIC
                // path includes that path's receipts, which arrive at the QUIC
                // keepalive, so it gets the QUIC window.
                let kind = link_kind(entry.native, receipt.quic_path_seen());
                Some(PeerObservation {
                    addr: *addr,
                    kind,
                    since_last_receipt: receipt.since_last_receipt(),
                    since_listener_progress: entry.link_use.since_forwarded(now),
                    awaiting: awaiting.get(addr).copied().unwrap_or(0),
                })
            })
            .collect();
        let decision = sweep.tick(now, &observed);
        if let Some(why) = decision.suppressed {
            tracing::warn!(
                ?why,
                connections = observed.len(),
                "STALE_LINK_SWEEP skipped a tick: not dropping silent links"
            );
        }
        for addr in decision.drops {
            let (kind, since, waiting) = observed
                .iter()
                .find(|o| o.addr == addr)
                .map(|o| (o.kind, o.since_last_receipt, o.awaiting))
                .unwrap_or((LinkKind::Udp, Duration::ZERO, 0));
            tracing::warn!(
                peer_addr = %addr,
                ?kind,
                silent_for_s = since.as_secs(),
                window_s = silence_window(kind).as_secs(),
                requests_waiting = waiting,
                "STALE_LINK_SWEEP dropping a link that went silent with requests waiting"
            );
            let total =
                note_unresponsive_drop(&self.bridge.op_manager.ring.connection_manager, addr);
            tracing::info!(peer_addr = %addr, total, "STALE_LINK_SWEEP drop counted, re-dial cooldown started");
            // Integration point for fix B (recently-silent peer record,
            // branch fix/get-skip-recently-silent-peer, not merged): mark
            // `addr` silent here once it lands. Not marked in this change.
            self.drop_connection_by_addr(
                addr,
                handshake_cmd_sender,
                crate::tracing::DisconnectReason::Unresponsive,
            )
            .await;
        }
    }
}

/// What the node reads from one connection at a tick.
#[derive(Clone, Copy, Debug)]
pub(super) struct PeerObservation {
    pub(super) addr: SocketAddr,
    pub(super) kind: LinkKind,
    pub(super) since_last_receipt: Duration,
    pub(super) since_listener_progress: Duration,
    /// Live transactions awaiting a reply from this peer.
    pub(super) awaiting: usize,
}

/// Per-node sweep state: the tick clock and, per peer, since when requests
/// have been waiting on it.
pub(super) struct StaleLinkSweep {
    active: bool,
    last_tick: Option<Instant>,
    next_due: Instant,
    outstanding_since: HashMap<SocketAddr, Instant>,
}

impl StaleLinkSweep {
    /// `active` is [`activation`]'s verdict, taken once when the loop starts.
    pub(super) fn new(now: Instant, active: bool) -> Self {
        Self {
            active,
            last_tick: None,
            next_due: now + STALE_LINK_SWEEP_INTERVAL,
            outstanding_since: HashMap::new(),
        }
    }

    /// The timer deadline for the next tick, for the event loop's wake-up.
    /// `None` while the sweep is inactive, so a node without it runs the same
    /// event loop as before the sweep, and while no transaction is in flight:
    /// with nothing awaiting a reply there is nothing to judge, and an armed
    /// timer on an idle node keeps a simulation from skipping ahead (a
    /// 63-node run was 5 to 8 times slower with it armed). The first event
    /// after an idle spell finds the tick due and runs it at once.
    pub(super) fn deadline(&self, any_in_flight: bool) -> Option<Instant> {
        (self.active && any_in_flight).then_some(self.next_due)
    }

    /// Whether a tick is due at `now`.
    pub(super) fn due(&self, now: Instant) -> bool {
        self.active && now >= self.next_due
    }

    /// Run one tick over `observed` (address, kind, time since last receipt,
    /// awaiting count) at `now`, returning the links to drop.
    pub(super) fn tick(&mut self, now: Instant, observed: &[PeerObservation]) -> Decision {
        let samples: Vec<PeerSample> = observed
            .iter()
            .map(|o| {
                let (addr, outstanding) = (o.addr, o.awaiting);
                let outstanding_for = if outstanding == 0 {
                    Duration::ZERO
                } else {
                    let since = *self.outstanding_since.entry(addr).or_insert(now);
                    now.saturating_duration_since(since)
                };
                PeerSample {
                    addr,
                    kind: o.kind,
                    since_last_receipt: o.since_last_receipt,
                    since_listener_progress: o.since_listener_progress,
                    outstanding,
                    outstanding_for,
                }
            })
            .collect();
        // Forget peers with nothing waiting and peers that are gone, so a
        // later request starts a fresh wait.
        self.outstanding_since
            .retain(|addr, _| observed.iter().any(|o| o.addr == *addr && o.awaiting > 0));
        let decision = decide(now, self.last_tick, &samples);
        self.last_tick = Some(now);
        self.next_due = now + STALE_LINK_SWEEP_INTERVAL;
        for addr in &decision.drops {
            self.outstanding_since.remove(addr);
        }
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::{SharedMockTimeSource, TimeSource};

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 4000))
    }

    fn udp_w() -> Duration {
        silence_window(LinkKind::Udp)
    }

    /// A UDP peer with `outstanding` requests waiting for `outstanding_for`,
    /// last heard from `silent_for` ago.
    fn peer(
        n: u8,
        silent_for: Duration,
        outstanding: usize,
        outstanding_for: Duration,
    ) -> PeerSample {
        PeerSample {
            addr: addr(n),
            kind: LinkKind::Udp,
            since_last_receipt: silent_for,
            since_listener_progress: Duration::MAX,
            outstanding,
            outstanding_for,
        }
    }

    /// A peer that has been silent and waited on for `d`.
    fn stale_for(n: u8, d: Duration) -> PeerSample {
        peer(n, d, 1, d)
    }

    fn clock() -> SharedMockTimeSource {
        SharedMockTimeSource::new()
    }

    /// Previous tick one interval ago.
    fn normal_tick(c: &SharedMockTimeSource) -> (Instant, Option<Instant>) {
        let last = c.now();
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        (c.now(), Some(last))
    }

    #[test]
    fn windows_match_the_design() {
        assert_eq!(udp_w(), Duration::from_secs(19));
        assert_eq!(silence_window(LinkKind::Quic), Duration::from_secs(34));
        assert!(silence_window(LinkKind::Udp) >= MIN_WINDOW);
    }

    /// The UDP window is derived from `MAX_KEEPALIVE_INTERVAL`, which the
    /// ping backoff in `peer_connection.rs` caps at 15 s, and 15 s is what
    /// this module's doc and the window's margin reason from. Reads only the
    /// constant's own definition, comments stripped, so a mention elsewhere in
    /// that large file cannot satisfy it; if the cap moves, re-derive the window.
    #[test]
    fn the_keepalive_backoff_cap_is_what_the_udp_window_assumes() {
        let src = crate::contract::source_pin_util::strip_comments(include_str!(
            "../../../transport/peer_connection.rs"
        ));
        for (name, expected) in [
            (
                "const MAX_KEEPALIVE_INTERVAL",
                "constMAX_KEEPALIVE_INTERVAL:Duration=Duration::from_secs(15)",
            ),
            (
                "const KEEP_ALIVE_INTERVAL",
                "constKEEP_ALIVE_INTERVAL:Duration=Duration::from_secs(5)",
            ),
        ] {
            let start = src
                .find(name)
                .unwrap_or_else(|| panic!("{name} is defined"));
            let definition: String = src[start..]
                .split(';')
                .next()
                .expect("the definition ends")
                .split_whitespace()
                .collect();
            assert!(
                definition.ends_with(expected),
                "{name} changed ({definition}): re-derive the UDP window"
            );
        }
        assert_eq!(UDP_MAX_QUIET, Duration::from_secs(15));
    }

    /// A live UDP peer whose pings are backing off is quiet for up to
    /// `MAX_KEEPALIVE_INTERVAL` between packets (its own listener blocked, or
    /// our pongs lost one way). With a request waiting for minutes, such a
    /// peer must never read as silent; a dead one still does.
    #[test]
    fn the_udp_window_covers_the_longest_keepalive_backoff() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let worst_live_gap = UDP_MAX_QUIET + Duration::from_secs(1);
        let live = peer(1, worst_live_gap, 1, Duration::from_secs(600));
        assert_eq!(decide(now, last, &[live]), Decision::default());
        assert!(udp_w() > UDP_MAX_QUIET);
    }

    #[test]
    fn no_outstanding_request_never_drops() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let d = decide(
            now,
            last,
            // Stale in every other respect: only "nothing is waiting" saves it.
            &[peer(
                1,
                Duration::from_secs(600),
                0,
                Duration::from_secs(600),
            )],
        );
        assert_eq!(d, Decision::default());
    }

    #[test]
    fn fresh_stamp_never_drops() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let d = decide(
            now,
            last,
            &[peer(1, Duration::from_secs(1), 3, Duration::from_secs(300))],
        );
        assert_eq!(d, Decision::default());
    }

    #[test]
    fn drops_after_exactly_the_window() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let just_under = udp_w() - Duration::from_nanos(1);
        assert_eq!(
            decide(now, last, &[stale_for(1, just_under)]),
            Decision::default()
        );
        let d = decide(now, last, &[stale_for(1, udp_w())]);
        assert_eq!(d.drops, vec![addr(1)]);
        assert_eq!(d.suppressed, None);
    }

    /// An old stamp from before the request must not count: a peer idle for an
    /// hour that was asked something a second ago is not silent.
    #[test]
    fn a_stamp_older_than_the_request_does_not_count() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let p = peer(1, Duration::from_secs(3600), 1, Duration::from_secs(1));
        assert_eq!(decide(now, last, &[p]), Decision::default());
        // Once the request itself has waited the window, it counts.
        let p = peer(1, Duration::from_secs(3600), 1, udp_w());
        assert_eq!(decide(now, last, &[p]).drops, vec![addr(1)]);
    }

    #[test]
    fn quic_link_uses_its_longer_window() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let mut p = stale_for(1, udp_w() + Duration::from_secs(1));
        p.kind = LinkKind::Quic;
        assert_eq!(decide(now, last, &[p]), Decision::default());
        let mut p = stale_for(1, silence_window(LinkKind::Quic));
        p.kind = LinkKind::Quic;
        assert_eq!(decide(now, last, &[p]).drops, vec![addr(1)]);
    }

    #[test]
    fn tick_gap_longer_than_half_the_window_decides_nothing() {
        let c = clock();
        let last = c.now();
        let stale = [stale_for(1, Duration::from_secs(600))];
        // Exactly W/2 is still a normal tick.
        c.advance_time(udp_w() / 2);
        assert_eq!(decide(c.now(), Some(last), &stale).drops, vec![addr(1)]);
        // Anything longer is a stall or a suspend.
        c.advance_time(Duration::from_nanos(1));
        let d = decide(c.now(), Some(last), &stale);
        assert!(d.drops.is_empty());
        assert_eq!(d.suppressed, Some(Suppressed::TickGap));
    }

    /// The first tick has no predecessor to compare with and is allowed.
    #[test]
    fn first_tick_has_no_gap() {
        let c = clock();
        let d = decide(c.now(), None, &[stale_for(1, Duration::from_secs(600))]);
        assert_eq!(d.drops, vec![addr(1)]);
    }

    /// Laptop wake: every link is stale at once. Nothing is dropped.
    #[test]
    fn majority_stale_is_a_local_stall() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let big = Duration::from_secs(600);
        let peers = [
            stale_for(1, big),
            stale_for(2, big),
            stale_for(3, big),
            peer(4, Duration::from_secs(1), 1, big),
        ];
        let d = decide(now, last, &peers);
        assert!(d.drops.is_empty());
        assert_eq!(d.suppressed, Some(Suppressed::Majority));
    }

    #[test]
    fn exactly_half_stale_is_not_a_majority() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let big = Duration::from_secs(600);
        let peers = [
            stale_for(1, big),
            stale_for(2, big),
            peer(3, Duration::from_secs(1), 1, big),
            peer(4, Duration::from_secs(1), 1, big),
        ];
        assert_eq!(decide(now, last, &peers).drops.len(), 2);
    }

    /// Fewer than three peers with requests: the fraction means nothing, so
    /// the guard does not apply (one dead peer out of one is just a dead peer).
    #[test]
    fn majority_guard_needs_three_peers() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let big = Duration::from_secs(600);
        assert_eq!(decide(now, last, &[stale_for(1, big)]).drops, vec![addr(1)]);
        assert_eq!(
            decide(now, last, &[stale_for(1, big), stale_for(2, big)])
                .drops
                .len(),
            2
        );
    }

    /// Peers with no outstanding request do not dilute the majority: they are
    /// never examined, so they are not in the denominator.
    #[test]
    fn peers_without_requests_are_not_counted_in_the_majority() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let big = Duration::from_secs(600);
        let mut peers = vec![stale_for(1, big), stale_for(2, big), stale_for(3, big)];
        for n in 10..40 {
            peers.push(peer(n, Duration::from_secs(1), 0, Duration::ZERO));
        }
        assert_eq!(
            decide(now, last, &peers).suppressed,
            Some(Suppressed::Majority)
        );
    }

    #[test]
    fn at_most_two_drops_per_tick_longest_silent_first() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let w = udp_w();
        // Four stale of ten with requests: below a majority.
        let mut peers = vec![
            stale_for(1, w),
            stale_for(2, w + Duration::from_secs(30)),
            stale_for(3, w + Duration::from_secs(10)),
            stale_for(4, w + Duration::from_secs(20)),
        ];
        for n in 5..11 {
            peers.push(peer(n, Duration::from_secs(1), 1, Duration::from_secs(60)));
        }
        let d = decide(now, last, &peers);
        assert_eq!(d.drops, vec![addr(2), addr(4)]);
        assert_eq!(d.suppressed, None);
    }

    // ---- StaleLinkSweep state ----

    fn obs(n: u8, silent_for: Duration, outstanding: usize) -> PeerObservation {
        PeerObservation {
            addr: addr(n),
            kind: LinkKind::Udp,
            since_last_receipt: silent_for,
            since_listener_progress: Duration::MAX,
            awaiting: outstanding,
        }
    }

    /// A request that begins waiting on a long-idle peer is judged from when
    /// it began waiting, over successive ticks.
    #[test]
    fn sweep_measures_from_when_requests_began_waiting() {
        let c = clock();
        let mut sweep = StaleLinkSweep::new(c.now(), true);
        let idle = Duration::from_secs(3600);
        let mut dropped_at = None;
        for i in 1..=10u32 {
            c.advance_time(STALE_LINK_SWEEP_INTERVAL);
            let d = sweep.tick(c.now(), &[obs(1, idle, 1)]);
            if !d.drops.is_empty() {
                dropped_at = Some(STALE_LINK_SWEEP_INTERVAL * i);
                break;
            }
        }
        let at = dropped_at.expect("the dead link is dropped");
        // First observed at tick 1 (5 s); waited W by tick with 5 s steps.
        assert!(
            at >= udp_w(),
            "dropped at {at:?}, before the window {:?}",
            udp_w()
        );
        assert!(
            at <= udp_w() + STALE_LINK_SWEEP_INTERVAL * 2,
            "dropped late: {at:?}"
        );
    }

    /// When the requests finish, the wait clock resets: a later request does
    /// not inherit the earlier wait.
    #[test]
    fn sweep_forgets_a_peer_with_nothing_waiting() {
        let c = clock();
        let mut sweep = StaleLinkSweep::new(c.now(), true);
        let idle = Duration::from_secs(3600);
        for _ in 0..2 {
            c.advance_time(STALE_LINK_SWEEP_INTERVAL);
            assert!(sweep.tick(c.now(), &[obs(1, idle, 1)]).drops.is_empty());
        }
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        assert!(sweep.tick(c.now(), &[obs(1, idle, 0)]).drops.is_empty());
        assert!(sweep.outstanding_since.is_empty());
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        let d = sweep.tick(c.now(), &[obs(1, idle, 1)]);
        assert!(d.drops.is_empty(), "wait clock must restart: {d:?}");
    }

    /// A peer that disappears (connection closed) is forgotten.
    #[test]
    fn sweep_forgets_a_closed_connection() {
        let c = clock();
        let mut sweep = StaleLinkSweep::new(c.now(), true);
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        sweep.tick(c.now(), &[obs(1, Duration::from_secs(1), 1)]);
        assert_eq!(sweep.outstanding_since.len(), 1);
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        sweep.tick(c.now(), &[]);
        assert!(sweep.outstanding_since.is_empty());
    }

    /// After a stall the tick is skipped, and the next normal tick measures
    /// again rather than inheriting the skipped one.
    #[test]
    fn sweep_skips_a_stalled_tick_then_remeasures() {
        let c = clock();
        let mut sweep = StaleLinkSweep::new(c.now(), true);
        let idle = Duration::from_secs(3600);
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        sweep.tick(c.now(), &[obs(1, idle, 1)]);
        // 10 minute stall: every stamp is stale, the wait looks long.
        c.advance_time(Duration::from_secs(600));
        let d = sweep.tick(c.now(), &[obs(1, idle, 1)]);
        assert_eq!(d.suppressed, Some(Suppressed::TickGap));
        assert!(d.drops.is_empty());
        // The peer spoke while the loop caught up: fresh stamp, no drop.
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        let d = sweep.tick(c.now(), &[obs(1, Duration::from_secs(1), 1)]);
        assert_eq!(d, Decision::default());
    }

    #[test]
    fn due_follows_the_interval() {
        let c = clock();
        let mut sweep = StaleLinkSweep::new(c.now(), true);
        assert!(!sweep.due(c.now()));
        c.advance_time(STALE_LINK_SWEEP_INTERVAL);
        assert!(sweep.due(c.now()));
        sweep.tick(c.now(), &[]);
        assert!(!sweep.due(c.now()));
        assert_eq!(
            sweep.deadline(true),
            Some(c.now() + STALE_LINK_SWEEP_INTERVAL)
        );
        assert_eq!(sweep.deadline(false), None, "no timer while idle");
        let off = StaleLinkSweep::new(c.now(), false);
        assert_eq!(off.deadline(true), None);
        c.advance_time(STALE_LINK_SWEEP_INTERVAL * 10);
        assert!(!off.due(c.now()));
    }

    // ---- a blocked listener is not a silent peer ----

    /// The listener holds a message the event loop has not taken, so the
    /// connection is not being read and its stamp is frozen at 100 s, with a
    /// request waiting for 100 s: a live peer, not dropped.
    #[test]
    fn a_blocked_listener_with_an_old_stamp_is_not_dropped() {
        let c = clock();
        let (now, last) = normal_tick(&c);
        let mut p = stale_for(1, Duration::from_secs(100));
        p.since_listener_progress = Duration::ZERO;
        assert_eq!(decide(now, last, &[p]), Decision::default());
        // Finished forwarding 3 s ago: the listener has only just resumed
        // reading, the stamp has not caught up yet.
        p.since_listener_progress = Duration::from_secs(3);
        assert_eq!(decide(now, last, &[p]), Decision::default());
        // Nothing forwarded for the whole window: silent after all.
        p.since_listener_progress = udp_w();
        assert_eq!(decide(now, last, &[p]).drops, vec![addr(1)]);
        p.since_listener_progress = Duration::MAX;
        assert_eq!(decide(now, last, &[p]).drops, vec![addr(1)]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_listener_stamp_reads_forwarding_now_then_the_time_since() {
        let base = Instant::now();
        let stamp = super::super::zombie_sweep::LinkUseStamp::new(base);
        assert_eq!(stamp.since_forwarded(base), Duration::MAX);
        let guard = stamp.begin_forward();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(stamp.since_forwarded(Instant::now()), Duration::ZERO);
        drop(guard);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(
            stamp.since_forwarded(Instant::now()),
            Duration::from_secs(10)
        );
    }

    /// Index of the `}` closing the `{` at `open` in `src`.
    fn closing_brace(src: &str, open: usize) -> usize {
        let mut depth = 0usize;
        for (i, c) in src[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return open + i;
                    }
                }
                _ => {}
            }
        }
        panic!("unbalanced braces");
    }

    /// The listener's `msg = conn.recv()` arm takes the forward guard under a
    /// name, before either send, and in a scope that is still open at the
    /// second send. Bounded to that arm, so a mention elsewhere cannot satisfy
    /// it, and by block depth, so a guard moved into an inner block that closes
    /// before the sends (dropped, and the stamp not held) fails it. `let _ =`
    /// would drop at once, so the binding name is part of the pin.
    #[test]
    fn the_listener_holds_the_forward_guard_across_its_sends() {
        let src = crate::contract::source_pin_util::strip_comments(
            include_str!("../p2p_protoc.rs")
                .split("\n#[cfg(test)]\nmod ")
                .next()
                .expect("production source"),
        );
        let arm_start = src
            .find("msg = conn.recv() => {")
            .expect("the listener's recv arm");
        let open = arm_start + "msg = conn.recv() ".len() + "=> ".len();
        let arm = &src[open..=closing_brace(&src, open)];
        let guard = arm
            .find("let _forwarding = link_use.begin_forward();")
            .expect("the arm takes the guard under a name");
        let first = arm
            .find("ConnEvent::NativeFirstFrame {")
            .expect("first-frame send");
        let second = arm
            .find("ConnEvent::InboundMessage(IncomingMessage::with_remote(")
            .expect("inbound send");
        assert!(
            guard < first && first < second,
            "guard, then both sends, in order"
        );
        // The scope the guard lives in must still be open at the last send.
        let mut depth = 0i64;
        let mut min_depth = i64::MAX;
        for c in arm[guard..second].chars() {
            match c {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
            min_depth = min_depth.min(depth);
        }
        assert!(
            min_depth >= 0,
            "the guard's block closes before the second send: the stamp would not be held"
        );
    }

    // ---- production default ----

    /// A live link: keepalive stamps are fresh (a ping answered every 5 s)
    /// while a request waits for minutes. The sweep never drops it.
    #[test]
    fn a_live_link_with_fresh_keepalive_stamps_is_never_dropped() {
        let c = clock();
        let mut sweep = StaleLinkSweep::new(c.now(), true);
        for _ in 0..120 {
            c.advance_time(STALE_LINK_SWEEP_INTERVAL);
            let d = sweep.tick(c.now(), &[obs(1, Duration::from_secs(4), 1)]);
            assert_eq!(d, Decision::default());
        }
    }

    // ---- window class ----

    #[test]
    fn an_upgraded_udp_connection_gets_the_quic_window() {
        assert_eq!(link_kind(false, false), LinkKind::Udp);
        assert_eq!(link_kind(true, false), LinkKind::Quic);
        assert_eq!(link_kind(false, true), LinkKind::Quic);
        let stamp = crate::transport::ReceiptStamp::new(
            Default::default(),
            Default::default(),
            std::sync::Arc::new(|| 0),
        );
        assert!(!stamp.quic_path_seen());
        let with_path = crate::transport::ReceiptStamp::new(
            Default::default(),
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(7)),
            std::sync::Arc::new(|| 0),
        );
        assert!(with_path.quic_path_seen());
    }

    // ---- activation ----

    #[test]
    fn the_override_decides_and_ends_with_its_guard() {
        let listen: SocketAddr = "10.9.9.9:1".parse().unwrap();
        let (auto, _) = activation(listen);
        assert!(auto, "an address no simulated network owns has a keepalive");
        {
            let _off = StaleLinkSweepGuard::force(false);
            assert!(!activation(listen).0);
        }
        {
            let _on = StaleLinkSweepGuard::force(true);
            assert!(activation(listen).0);
        }
        assert_eq!(override_for_thread(), None);
    }

    // ---- re-dial cooldown ----

    /// A peer dropped as unresponsive is not re-dialled within the cooldown:
    /// it is in the recently-failed set that the CONNECT acquisition excludes,
    /// and the counter moves.
    #[test]
    fn a_peer_dropped_as_unresponsive_enters_the_redial_cooldown() {
        let cm = crate::ring::ConnectionManager::test_default();
        let peer: SocketAddr = "10.8.8.8:4001".parse().unwrap();
        assert!(!cm.recently_failed_addrs().contains(&peer));
        let before = unresponsive_drops_total();
        let total = note_unresponsive_drop(&cm, peer);
        assert!(cm.recently_failed_addrs().contains(&peer));
        assert!(total > before && unresponsive_drops_total() >= total);
        // A repeat doubles the cooldown rather than resetting it.
        note_unresponsive_drop(&cm, peer);
        assert!(cm.recently_failed_addrs().contains(&peer));
    }

    /// The cooldown is applied where the drop is carried out, not only in the
    /// helper: the sweep's drop loop calls it before closing the link.
    #[test]
    fn the_drop_site_starts_the_cooldown_before_closing_the_link() {
        // Whitespace removed, so a reformat of the call cannot defeat the pin.
        let src: String = crate::contract::source_pin_util::strip_comments(
            include_str!("stale_link_sweep.rs")
                .split("\n#[cfg(test)]\nmod ")
                .next()
                .expect("production source"),
        )
        .split_whitespace()
        .collect();
        let note = src
            .find("lettotal=note_unresponsive_drop(")
            .expect("the drop site counts the drop and starts the cooldown");
        let close = src
            .find("self.drop_connection_by_addr(")
            .expect("closes the link");
        assert!(note < close);
    }

    #[test]
    fn nested_guards_restore_the_previous_override() {
        let outer = StaleLinkSweepGuard::force(true);
        {
            let _inner = StaleLinkSweepGuard::force(false);
            assert_eq!(override_for_thread(), Some(false));
        }
        assert_eq!(override_for_thread(), Some(true));
        drop(outer);
        assert_eq!(override_for_thread(), None);
    }
}
