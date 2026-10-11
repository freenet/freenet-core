//! Minimum spacing between two broadcasts of one contract to one peer (#5842).
//!
//! Every receiver rate-limits UPDATE traffic per `(sender, contract)` pair
//! (`crate::ring::update_rate_limit`). Since 0.2.133 the broadcast class allows
//! one message per [`MIN_BROADCAST_INTERVAL`] (20 ms) and repairs a drop with a
//! throttled `ResyncRequest`; that repair is itself capped per contract on both
//! the requesting and the answering side, so under a burst it does not reach
//! every peer. Older receivers allow one UPDATE of ANY kind per
//! [`MIN_UPDATE_INTERVAL`] (100 ms) and have no repair at all.
//!
//! A host that relays a received update and then commits its own a few
//! milliseconds later sends two broadcasts of the same contract to the same
//! co-hosts back to back. The receiver keeps the first and drops the second,
//! and every other host relaying the second update hits the same limit. The
//! second update is then lost on those peers until anti-entropy, 5 to 75
//! minutes later. The production broadcast queue made this worse by letting
//! two sends for one `(contract, peer)` run concurrently.
//!
//! The fix is on the SENDER, so it protects receivers of every version: a send
//! for a `(contract, peer)` pair waits until [`BROADCAST_PEER_SPACING`] has
//! passed since the previous one finished. Nothing is dropped. The broadcast
//! queue already replaces a queued entry with the newer state, so a held send
//! carries the newest state when it goes, and a timer wakes the queue when the
//! interval expires.
//!
//! [`MIN_BROADCAST_INTERVAL`]: crate::ring::update_rate_limit::MIN_BROADCAST_INTERVAL
//! [`MIN_UPDATE_INTERVAL`]: crate::ring::update_rate_limit::MIN_UPDATE_INTERVAL

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

#[cfg(any(test, feature = "simulation_tests"))]
use std::sync::Arc;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::ring::PeerKey;
use crate::util::time_source::DynTimeSource;

/// Minimum time between the end of one broadcast of a contract to a peer and
/// the start of the next.
///
/// Chosen from the receivers' limits, the larger of which is
/// [`MIN_UPDATE_INTERVAL`] (100 ms, the single per-pair budget every UPDATE
/// variant shares on peers older than 0.2.133). The extra 50 ms covers arrival
/// jitter: the receiver measures the gap between ARRIVALS, and two sends spaced
/// exactly 100 ms apart can arrive closer than that. Spacing far above the
/// 20 ms broadcast budget of newer peers costs nothing there, and it bounds a
/// single pair at about 6.7 broadcasts per second, which is within what both
/// receiver versions accept. Pinned against both limits by
/// `spacing_exceeds_every_receiver_budget`.
///
/// [`MIN_UPDATE_INTERVAL`]: crate::ring::update_rate_limit::MIN_UPDATE_INTERVAL
pub(crate) const BROADCAST_PEER_SPACING: Duration = Duration::from_millis(150);

/// Above this many tracked pairs a record first drops the entries older than
/// [`BROADCAST_PEER_SPACING`], which can no longer delay anything.
const PRUNE_AT: usize = 1024;

/// Hard ceiling on tracked pairs. Reaching it needs more than 16k distinct
/// `(contract, peer)` broadcasts inside one spacing interval; a record past it
/// is skipped, so that one pair goes unspaced (the behaviour before #5842)
/// rather than the map growing. Counted in `untracked_total`.
const MAX_TRACKED: usize = 16_384;

type PairKey = (ContractInstanceId, PeerKey);

/// When each `(contract, peer)` pair last finished a broadcast.
///
/// An entry only matters for [`BROADCAST_PEER_SPACING`] after it is written,
/// so the map holds roughly the pairs broadcast to in the last 150 ms. Entries
/// are pruned by age rather than on disconnect: a disconnected peer's entry
/// expires on its own within one interval.
pub(crate) struct BroadcastSpacing {
    last_sent: DashMap<PairKey, Instant>,
    /// Pairs with a send in progress on a path that has no queue of its own
    /// (the simulation fan-out). The production queue tracks this in its own
    /// `active` map. An entry lives only as long as its [`SendClaim`].
    #[cfg(any(test, feature = "simulation_tests"))]
    in_flight: DashMap<PairKey, ()>,
    time_source: DynTimeSource,
    spacing: Duration,
    prune_at: AtomicUsize,
    untracked_total: AtomicU64,
}

impl BroadcastSpacing {
    pub(crate) fn new(time_source: DynTimeSource) -> Self {
        Self::with_spacing(time_source, BROADCAST_PEER_SPACING)
    }

    pub(crate) fn with_spacing(time_source: DynTimeSource, spacing: Duration) -> Self {
        Self {
            last_sent: DashMap::new(),
            #[cfg(any(test, feature = "simulation_tests"))]
            in_flight: DashMap::new(),
            time_source,
            spacing,
            prune_at: AtomicUsize::new(PRUNE_AT),
            untracked_total: AtomicU64::new(0),
        }
    }

    pub(crate) fn now(&self) -> Instant {
        self.time_source.now()
    }

    /// How long a broadcast of `contract` to `peer` must still wait, or `None`
    /// when it may go now.
    pub(crate) fn remaining(
        &self,
        contract: &ContractInstanceId,
        peer: &PeerKey,
    ) -> Option<Duration> {
        let last = *self.last_sent.get(&(*contract, peer.clone()))?;
        let ready_at = last + self.spacing;
        let now = self.now();
        (ready_at > now).then(|| ready_at - now)
    }

    /// Record that a broadcast of `contract` to `peer` finished now.
    pub(crate) fn record_sent(&self, contract: &ContractInstanceId, peer: &PeerKey) {
        let now = self.now();
        let key = (*contract, peer.clone());
        if self.last_sent.len() >= self.prune_at.load(Ordering::Relaxed) {
            self.prune(now);
            // Re-arm at twice the surviving size, so a node that genuinely
            // broadcasts to many pairs per interval does not prune on every
            // record. Capped at the ceiling: a trigger above it could never be
            // reached again, and the map would stop pruning for good.
            let survivors = self.last_sent.len();
            self.prune_at.store(
                (survivors * 2).clamp(PRUNE_AT, MAX_TRACKED),
                Ordering::Relaxed,
            );
        }
        if self.last_sent.len() >= MAX_TRACKED && !self.last_sent.contains_key(&key) {
            // Every tracked pair is live (the prune above just ran at the
            // ceiling), so this pair goes unspaced rather than grow the map.
            let total = self.untracked_total.fetch_add(1, Ordering::Relaxed) + 1;
            if total.is_power_of_two() {
                tracing::info!(
                    tracked = MAX_TRACKED,
                    untracked_total = total,
                    "Broadcast spacing table full: a broadcast went out unspaced \
                     and may be dropped by the receiver's rate limiter (#5842)"
                );
            }
            return;
        }
        self.last_sent.insert(key, now);
    }

    /// Claim `(contract, peer)` for a send, or say how long to wait. The
    /// claim blocks other sends to the pair until it is dropped, and dropping
    /// it starts the spacing interval: the interval runs from the END of the
    /// send, however long the send took.
    #[cfg(any(test, feature = "simulation_tests"))]
    pub(crate) fn try_begin(
        self: &Arc<Self>,
        contract: &ContractInstanceId,
        peer: &PeerKey,
    ) -> Result<SendClaim, Duration> {
        let key = (*contract, peer.clone());
        // Claim first, then check the interval. A releasing claim stamps
        // before it releases, so whoever wins the claim afterwards is sure to
        // see the stamp; checking first could read the pair as free just
        // before an in-flight send stamps and releases it.
        match self.in_flight.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => return Err(self.spacing),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(());
            }
        }
        if let Some(wait) = self.remaining(contract, peer) {
            // Not a send: release without stamping.
            self.in_flight.remove(&key);
            return Err(wait);
        }
        Ok(SendClaim {
            spacing: self.clone(),
            key,
        })
    }

    fn prune(&self, now: Instant) {
        let spacing = self.spacing;
        self.last_sent
            .retain(|_, last| now.saturating_duration_since(*last) < spacing);
    }

    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.last_sent.len()
    }

    #[cfg(test)]
    pub(crate) fn untracked_total(&self) -> u64 {
        self.untracked_total.load(Ordering::Relaxed)
    }
}

/// A send in progress for one `(contract, peer)`, from
/// [`BroadcastSpacing::try_begin`]. Dropping it stamps the pair and then
/// releases it, in that order, so no other send can see the pair as free
/// before its interval has started.
#[cfg(any(test, feature = "simulation_tests"))]
pub(crate) struct SendClaim {
    spacing: Arc<BroadcastSpacing>,
    key: PairKey,
}

#[cfg(any(test, feature = "simulation_tests"))]
impl Drop for SendClaim {
    fn drop(&mut self) {
        self.spacing.record_sent(&self.key.0, &self.key.1);
        self.spacing.in_flight.remove(&self.key);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::transport::TransportKeypair;
    use crate::util::time_source::SharedMockTimeSource;

    fn peer() -> PeerKey {
        PeerKey::from(TransportKeypair::new().public().clone())
    }

    fn contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    /// The code between `start` and the next `end` after it, with whole-line
    /// `//` comments removed, so a commented-out call cannot satisfy a pin.
    /// Panics if either anchor is missing, so a moved anchor fails loudly
    /// rather than widening the region.
    fn region(src: &str, start: &str, end: &str) -> String {
        let at = src
            .find(start)
            .unwrap_or_else(|| panic!("anchor `{start}` not found"));
        let len = src[at + start.len()..]
            .find(end)
            .unwrap_or_else(|| panic!("end anchor `{end}` not found after `{start}`"));
        src[at..at + start.len() + len]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    const QUEUE_SRC: &str = include_str!("broadcast_queue.rs");
    const SIM_SRC: &str = include_str!("p2p_protoc/broadcast.rs");
    const HANDLER_SRC: &str = include_str!("../../contract.rs");

    /// #5842: BOTH fan-out paths apply the per-pair spacing. The simulation
    /// runs only its inline copy and the production binary only the queue, so
    /// a behavioural test of one says nothing about the other.
    #[test]
    fn both_fan_out_paths_apply_the_spacing() {
        let drain = region(QUEUE_SRC, "    async fn drain_lane<D, F>(", "\n    }\n");
        assert!(
            drain.contains("q.pop_ready(lane, Instant::now(), &group.spacing)"),
            "the production drain must pop through the spacing-aware pop_ready"
        );
        assert!(
            !drain.contains("q.pop_lane("),
            "a plain pop_lane in the drain bypasses the spacing"
        );
        assert!(
            drain.contains("tokio::time::sleep(wait)"),
            "a held entry must be sent by a timer, not by the next enqueue"
        );

        let sim = region(
            SIM_SRC,
            "    async fn broadcast_state_to_peers(",
            "\n    }\n}\n",
        );
        for needle in [
            // Bound to a named variable, so the claim lives to the end of the
            // iteration; `let _ =` would drop it at once and disarm it.
            "let _claim = match op_manager.broadcast_spacing.try_begin(key.id(), &peer_key)",
            "Self::broadcast_held_legs(",
        ] {
            assert!(
                sim.contains(needle),
                "the simulation fan-out must apply the spacing: missing `{needle}`"
            );
        }

        // The targeted heal shares the queue in production; the simulation
        // calls the send directly, so it must take the same claim.
        let heal = region(
            SIM_SRC,
            "    pub(super) async fn handle_sync_state_to_peer(",
            "\n    }\n",
        );
        assert!(
            heal.contains("op_manager.broadcast_spacing.try_begin(key.id(), &peer_key)")
                && heal.contains("Ok(_claim) =>"),
            "the simulation heal path must apply the spacing and hold the claim \
             across its send"
        );
    }

    /// #5842: a full-state leg must send the state its summary describes, on
    /// both paths, and the handler must read the pair in one event.
    #[test]
    fn full_state_legs_send_a_snapshot_read_in_one_handler_turn() {
        let leg = region(
            QUEUE_SRC,
            "pub(super) async fn broadcast_to_single_peer(",
            "\n}\n",
        );
        let sim = region(
            SIM_SRC,
            "    async fn broadcast_state_to_peers(",
            "\n    }\n}\n",
        );
        for (path, body) in [("production", &leg), ("simulation", &sim)] {
            assert!(
                body.contains("get_contract_snapshot(op_manager, &key)"),
                "the {path} full-state arm must send a state+summary snapshot"
            );
        }
        // `let snapshot = match contract_handler` is unique to the handling
        // arm; the event's own name also appears in the queue-full responder,
        // and anchoring there let this region span the GetSummaryQuery arm,
        // whose `summarize_contract_state` call then satisfied the check.
        let arm = region(
            HANDLER_SRC,
            "let snapshot = match contract_handler",
            "ContractHandlerEvent::GetDeltaQuery { key, their_summary } => {",
        );
        assert!(
            !arm.contains("ContractHandlerEvent::GetSummaryQuery"),
            "the region escaped the GetStateAndSummaryQuery arm"
        );
        let fetch = arm
            .find(".fetch_contract(key, false)")
            .expect("the arm must read the stored state");
        let summarize = arm
            .find(".summarize_contract_state(key)")
            .expect("the arm must summarize in the same handler turn");
        assert!(
            fetch < summarize,
            "the summary must be taken after the state is read, in the same arm"
        );
    }

    #[test]
    fn a_claimed_pair_waits_for_the_send_to_end_and_then_the_interval() {
        let clock = SharedMockTimeSource::new();
        let spacing = Arc::new(BroadcastSpacing::new(Arc::new(clock.clone())));
        let p = peer();
        let claim = spacing.try_begin(&contract(1), &p).expect("free pair");
        // A long send: the claim outlives the interval and still blocks.
        clock.advance_time(BROADCAST_PEER_SPACING * 3);
        assert!(spacing.try_begin(&contract(1), &p).is_err(), "in flight");
        assert!(
            spacing.try_begin(&contract(1), &peer()).is_ok(),
            "other pair"
        );
        drop(claim);
        assert_eq!(
            spacing.try_begin(&contract(1), &p).err(),
            Some(BROADCAST_PEER_SPACING),
            "the interval starts when the send ends"
        );
        clock.advance_time(BROADCAST_PEER_SPACING);
        assert!(spacing.try_begin(&contract(1), &p).is_ok());
    }

    #[test]
    fn spacing_exceeds_every_receiver_budget() {
        use crate::ring::update_rate_limit::{MIN_BROADCAST_INTERVAL, MIN_UPDATE_INTERVAL};
        assert!(BROADCAST_PEER_SPACING > MIN_BROADCAST_INTERVAL);
        // Pre-0.2.133 receivers charge every UPDATE variant to one budget of
        // MIN_UPDATE_INTERVAL and never repair a drop; the spacing must clear
        // it with room for arrival jitter.
        assert!(BROADCAST_PEER_SPACING >= MIN_UPDATE_INTERVAL + Duration::from_millis(50));
    }

    #[test]
    fn a_pair_waits_out_the_interval_and_other_pairs_do_not() {
        let clock = SharedMockTimeSource::new();
        let spacing = BroadcastSpacing::new(Arc::new(clock.clone()));
        let (p, q) = (peer(), peer());

        assert_eq!(spacing.remaining(&contract(1), &p), None, "never sent");
        spacing.record_sent(&contract(1), &p);
        assert_eq!(
            spacing.remaining(&contract(1), &p),
            Some(BROADCAST_PEER_SPACING)
        );
        assert_eq!(spacing.remaining(&contract(1), &q), None, "other peer");
        assert_eq!(spacing.remaining(&contract(2), &p), None, "other contract");

        clock.advance_time(BROADCAST_PEER_SPACING - Duration::from_millis(1));
        assert_eq!(
            spacing.remaining(&contract(1), &p),
            Some(Duration::from_millis(1))
        );
        clock.advance_time(Duration::from_millis(1));
        assert_eq!(spacing.remaining(&contract(1), &p), None, "interval over");
    }

    #[test]
    fn expired_pairs_are_pruned_so_the_map_stays_bounded() {
        let clock = SharedMockTimeSource::new();
        let spacing = BroadcastSpacing::new(Arc::new(clock.clone()));
        for i in 0..PRUNE_AT {
            spacing.record_sent(&contract((i % 251) as u8), &peer());
        }
        assert_eq!(spacing.tracked(), PRUNE_AT);
        clock.advance_time(BROADCAST_PEER_SPACING);
        spacing.record_sent(&contract(1), &peer());
        assert_eq!(
            spacing.tracked(),
            1,
            "every stale pair dropped on the next record"
        );
    }

    #[test]
    fn a_prune_keeps_live_pairs_and_drops_only_expired_ones() {
        let clock = SharedMockTimeSource::new();
        let spacing = BroadcastSpacing::new(Arc::new(clock.clone()));
        let (old, young) = (peer(), peer());
        spacing.record_sent(&contract(1), &old);
        clock.advance_time(Duration::from_millis(1));
        spacing.record_sent(&contract(1), &young);
        // `old` is exactly one interval old, `young` 1 ms short of it.
        clock.advance_time(BROADCAST_PEER_SPACING - Duration::from_millis(1));
        spacing.prune(spacing.now());
        assert_eq!(spacing.tracked(), 1, "only the expired pair is dropped");
        assert_eq!(
            spacing.remaining(&contract(1), &young),
            Some(Duration::from_millis(1)),
            "the live pair keeps its stamp"
        );
    }

    #[test]
    fn the_prune_trigger_never_rises_above_the_ceiling() {
        let clock = SharedMockTimeSource::new();
        let spacing = BroadcastSpacing::new(Arc::new(clock.clone()));
        // Fill to the ceiling with pairs that all stay live, forcing prunes
        // that keep almost everything.
        for i in 0..MAX_TRACKED {
            spacing.record_sent(&contract((i % 251) as u8), &peer());
        }
        // One more record at the ceiling triggers a prune that keeps all
        // 16384; doubling that unclamped would set the trigger out of reach.
        spacing.record_sent(&contract(1), &peer());
        assert!(spacing.prune_at.load(Ordering::Relaxed) <= MAX_TRACKED);
        // Once they expire, the next record must still prune them.
        clock.advance_time(BROADCAST_PEER_SPACING);
        spacing.record_sent(&contract(1), &peer());
        assert_eq!(
            spacing.tracked(),
            1,
            "the map must keep pruning at the ceiling"
        );
    }

    #[test]
    fn past_the_hard_ceiling_a_new_pair_is_counted_not_tracked() {
        let clock = SharedMockTimeSource::new();
        let spacing = BroadcastSpacing::new(Arc::new(clock.clone()));
        for i in 0..MAX_TRACKED {
            spacing.record_sent(&contract((i % 251) as u8), &peer());
        }
        assert_eq!(spacing.tracked(), MAX_TRACKED);
        let extra = peer();
        spacing.record_sent(&contract(1), &extra);
        assert_eq!(spacing.tracked(), MAX_TRACKED);
        assert_eq!(spacing.untracked_total(), 1);
        assert_eq!(spacing.remaining(&contract(1), &extra), None);
    }
}
