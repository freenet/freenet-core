//! Size-aware, stateful WAN link model for transport tests.
//!
//! Tier 1 of the WAN transport test bench: it sits behind
//! [`MockSocket`](super::mock_transport::MockSocket) and decides, for every
//! datagram, whether it is delivered and at what virtual time. Unlike the
//! stateless `PacketDropPolicy` / `PacketDelayPolicy` it sees packet sizes and
//! keeps per-host and per-path state, so it can model:
//!
//! - a bottleneck rate with a finite drop-tail queue (bytes) on each host's
//!   uplink, **shared by every flow that host sends**;
//! - per-direction propagation delay (each host has its own outbound delay,
//!   so asymmetric paths are a per-host override);
//! - loss applied after the queue (a lost packet still used the bottleneck):
//!   Bernoulli, or Gilbert-Elliott bursts with state per directed path;
//! - a schedule of delay spikes (optionally on one host's outbound paths
//!   only) and outages.
//!
//! Delivery is FIFO per directed path: a packet never overtakes one sent
//! before it on the same path, so packets sent just after a spike ends wait
//! behind the spike's backlog, as they would behind a real stalled queue.
//! `MockSocket` keeps that ordering per path at the receiver too: a packet
//! from one peer never waits behind a later-arriving packet from another.
//!
//! Everything runs on virtual time and is a pure function of the seed and the
//! sequence of `on_send` calls. The model also keeps ground truth the
//! transport cannot see (queueing delay, the true base RTT, which delivered
//! packets were first copies, when each packet left the bottleneck) so tests
//! can assert against it.
//!
//! `on_send` returns a list of deliveries so reordering and duplication can be
//! added without changing callers. Address translation (NAT, rebinding) is not
//! this model's job: it belongs in the socket layer, which owns addresses,
//! and composes with whatever this model decides about timing and loss.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

#[cfg(test)]
mod r1_tests;

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// How packets leaving an uplink are lost.
#[derive(Clone, Debug)]
pub enum Loss {
    None,
    /// Each packet is lost independently with this probability.
    Bernoulli(f64),
    /// Two-state burst loss, with the state kept per directed path. Before each
    /// packet the path moves good -> bad with `to_bad` and bad -> good with
    /// `to_good`; the packet is then lost with the current state's probability.
    GilbertElliott {
        to_bad: f64,
        to_good: f64,
        loss_good: f64,
        loss_bad: f64,
    },
}

/// One host's access link. Everything the host sends goes through it.
#[derive(Clone, Debug)]
pub struct HostLink {
    /// Uplink bottleneck rate in bits per second. `0` means unlimited.
    pub uplink_bps: u64,
    /// Capacity of the drop-tail queue in front of the uplink, in bytes.
    pub queue_bytes: usize,
    /// Propagation delay from this host to any peer.
    pub one_way_delay: Duration,
    pub loss: Loss,
}

/// A scheduled change. Times are virtual-time offsets from the
/// [`VirtualTime`](crate::simulation::VirtualTime) epoch.
#[derive(Clone, Debug)]
pub enum LinkEvent {
    /// Packets leaving the bottleneck during `[start, start + duration)` get
    /// `extra_delay` added to their propagation delay. With `from` set, only
    /// packets sent by that host are affected, which is how a spike of a
    /// given RTT is applied to one leg of a round trip. Overlapping spikes add.
    DelaySpike {
        start: Duration,
        duration: Duration,
        extra_delay: Duration,
        from: Option<SocketAddr>,
    },
    /// Every packet offered during `[start, start + duration)` is dropped.
    Outage { start: Duration, duration: Duration },
}

/// When a copy of a datagram reaches its receiver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Arrival {
    pub at_nanos: u64,
    /// Model-wide order in which the link scheduled this delivery. Receivers
    /// break ties between equal arrival times with it, so two concurrent
    /// sends cannot reorder after the model has decided their order.
    pub seq: u64,
}

/// One delivered datagram, as seen by the link.
#[derive(Clone, Copy, Debug)]
pub struct Delivery {
    /// When the packet finished serializing at the sender's bottleneck.
    pub departed_at_nanos: u64,
    pub arrives_at_nanos: u64,
    pub bytes: usize,
    /// False when an identical datagram was already delivered on this path.
    /// The transport retransmits the stored ciphertext, so a retransmission
    /// of a packet that did arrive is byte-identical to it.
    pub first_copy: bool,
    /// Hash of the datagram's bytes; a retransmission has the same one.
    pub content_hash: u64,
}

/// One datagram handed to the link, whatever then became of it.
#[derive(Clone, Copy, Debug)]
pub struct Offer {
    pub at_nanos: u64,
    pub content_hash: u64,
}

/// Ground-truth counters for one host's uplink.
#[derive(Clone, Debug, Default)]
pub struct HostStats {
    pub offered: u64,
    pub delivered: u64,
    pub dropped_queue_full: u64,
    pub dropped_random: u64,
    pub dropped_outage: u64,
    /// Largest time any packet waited in the queue before serialization.
    pub max_queueing_delay: Duration,
}

#[derive(Default)]
struct PathState {
    last_arrival_nanos: u64,
    /// Gilbert-Elliott state: true while the path is in the bad state.
    bad: bool,
    seen: HashSet<u64>,
    deliveries: Vec<Delivery>,
    offers: Vec<Offer>,
}

#[derive(Default)]
struct HostState {
    busy_until_nanos: u64,
    stats: HostStats,
}

struct State {
    // A per-model RNG rather than `GlobalRng`: `GlobalRng` is thread-local and
    // also drives the transport's own choices (e.g. `deterministic_select!`),
    // so sharing it would make the link's loss pattern depend on how many
    // draws the transport happened to make. Owning the stream keeps the link
    // a function of its seed and the send sequence alone.
    rng: SmallRng,
    next_seq: u64,
    overrides: HashMap<SocketAddr, HostLink>,
    events: Vec<LinkEvent>,
    hosts: HashMap<SocketAddr, HostState>,
    paths: HashMap<(SocketAddr, SocketAddr), PathState>,
}

/// The shared link fabric. One instance serves every `MockSocket` in a test.
pub struct LinkModel {
    default_link: HostLink,
    state: Mutex<State>,
}

impl LinkModel {
    /// Every host gets `default_link` unless overridden with
    /// [`set_host_link`](Self::set_host_link). All randomness comes from `seed`.
    pub fn new(default_link: HostLink, seed: u64) -> Self {
        Self {
            default_link,
            state: Mutex::new(State {
                rng: SmallRng::seed_from_u64(seed),
                next_seq: 0,
                overrides: HashMap::new(),
                events: Vec::new(),
                hosts: HashMap::new(),
                paths: HashMap::new(),
            }),
        }
    }

    pub fn set_host_link(&self, host: SocketAddr, link: HostLink) {
        self.lock().overrides.insert(host, link);
    }

    pub fn schedule(&self, event: LinkEvent) {
        self.lock().events.push(event);
    }

    /// The host's link (its override, or the default).
    pub fn host_link(&self, host: SocketAddr) -> HostLink {
        self.lock()
            .overrides
            .get(&host)
            .cloned()
            .unwrap_or_else(|| self.default_link.clone())
    }

    /// The smallest RTT physically possible between `a` and `b`: the two
    /// propagation delays, with no queueing, serialization or spike. No RTT
    /// sample the transport takes can be below this.
    pub fn base_rtt(&self, a: SocketAddr, b: SocketAddr) -> Duration {
        self.host_link(a).one_way_delay + self.host_link(b).one_way_delay
    }

    /// Decide the fate of one datagram sent at `now_nanos`. Returns when
    /// copies of it arrive at `to`: empty if it is dropped, one entry
    /// otherwise.
    pub fn on_send(
        &self,
        now_nanos: u64,
        from: SocketAddr,
        to: SocketAddr,
        packet: &[u8],
    ) -> Vec<Arrival> {
        let mut guard = self.lock();
        let State {
            rng,
            next_seq,
            overrides,
            events,
            hosts,
            paths,
        } = &mut *guard;
        let link = overrides.get(&from).unwrap_or(&self.default_link);
        let host = hosts.entry(from).or_default();
        host.stats.offered += 1;
        let mut hasher = DefaultHasher::new();
        packet.hash(&mut hasher);
        let content_hash = hasher.finish();
        let path = paths.entry((from, to)).or_default();
        path.offers.push(Offer {
            at_nanos: now_nanos,
            content_hash,
        });

        if events.iter().any(|e| match e {
            LinkEvent::Outage { start, duration } => in_window(now_nanos, *start, *duration),
            LinkEvent::DelaySpike { .. } => false,
        }) {
            host.stats.dropped_outage += 1;
            return Vec::new();
        }

        // Bottleneck: the uplink serializes one packet at a time. The bytes
        // still waiting are what the busy period represents at this rate.
        let len = packet.len();
        let (departs, waited) = if link.uplink_bps == 0 {
            (now_nanos, 0)
        } else {
            let start = host.busy_until_nanos.max(now_nanos);
            let waited = start - now_nanos;
            let backlog_bytes = waited as u128 * link.uplink_bps as u128 / 8 / NANOS_PER_SEC;
            if backlog_bytes + len as u128 > link.queue_bytes as u128 {
                host.stats.dropped_queue_full += 1;
                return Vec::new();
            }
            let serialization = (len as u128 * 8 * NANOS_PER_SEC / link.uplink_bps as u128) as u64;
            host.busy_until_nanos = start + serialization;
            (start + serialization, waited)
        };
        let waited = Duration::from_nanos(waited);
        if waited > host.stats.max_queueing_delay {
            host.stats.max_queueing_delay = waited;
        }

        let lost = match link.loss {
            Loss::None => false,
            Loss::Bernoulli(p) => rng.random::<f64>() < p,
            Loss::GilbertElliott {
                to_bad,
                to_good,
                loss_good,
                loss_bad,
            } => {
                let flip = rng.random::<f64>();
                path.bad = if path.bad {
                    flip >= to_good
                } else {
                    flip < to_bad
                };
                rng.random::<f64>() < if path.bad { loss_bad } else { loss_good }
            }
        };
        if lost {
            host.stats.dropped_random += 1;
            return Vec::new();
        }

        let spike: Duration = events
            .iter()
            .filter_map(|e| match e {
                LinkEvent::DelaySpike {
                    start,
                    duration,
                    extra_delay,
                    from: only_from,
                } if in_window(departs, *start, *duration)
                    && only_from.is_none_or(|f| f == from) =>
                {
                    Some(*extra_delay)
                }
                LinkEvent::DelaySpike { .. } | LinkEvent::Outage { .. } => None,
            })
            .sum();
        let arrives =
            (departs + (link.one_way_delay + spike).as_nanos() as u64).max(path.last_arrival_nanos);
        path.last_arrival_nanos = arrives;
        let first_copy = path.seen.insert(content_hash);
        path.deliveries.push(Delivery {
            departed_at_nanos: departs,
            arrives_at_nanos: arrives,
            bytes: len,
            first_copy,
            content_hash,
        });
        host.stats.delivered += 1;
        let seq = *next_seq;
        *next_seq += 1;
        vec![Arrival {
            at_nanos: arrives,
            seq,
        }]
    }

    pub fn host_stats(&self, host: SocketAddr) -> HostStats {
        self.lock()
            .hosts
            .get(&host)
            .map(|h| h.stats.clone())
            .unwrap_or_default()
    }

    /// Every datagram offered on `from -> to`, in send order, including the
    /// ones the link then dropped.
    pub fn offers(&self, from: SocketAddr, to: SocketAddr) -> Vec<Offer> {
        self.lock()
            .paths
            .get(&(from, to))
            .map(|p| p.offers.clone())
            .unwrap_or_default()
    }

    /// Every delivery scheduled on `from -> to`, in arrival order.
    pub fn deliveries(&self, from: SocketAddr, to: SocketAddr) -> Vec<Delivery> {
        self.lock()
            .paths
            .get(&(from, to))
            .map(|p| p.deliveries.clone())
            .unwrap_or_default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // A panic while holding the lock already failed the test; keep the
        // state readable for the assertion messages that follow.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn in_window(t_nanos: u64, start: Duration, duration: Duration) -> bool {
    let start = start.as_nanos() as u64;
    t_nanos >= start && t_nanos < start + duration.as_nanos() as u64
}

/// Bytes of first-copy deliveries arriving in `(from_nanos, to_nanos]`.
pub fn unique_bytes_between(deliveries: &[Delivery], from_nanos: u64, to_nanos: u64) -> u64 {
    deliveries
        .iter()
        .filter(|d| {
            d.first_copy && d.arrives_at_nanos > from_nanos && d.arrives_at_nanos <= to_nanos
        })
        .map(|d| d.bytes as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        ([127, 0, 0, 1], port).into()
    }

    fn link(uplink_bps: u64, queue_bytes: usize, loss: Loss) -> HostLink {
        HostLink {
            uplink_bps,
            queue_bytes,
            one_way_delay: Duration::from_millis(100),
            loss,
        }
    }

    fn arrival(m: &LinkModel, now: u64, from: u16, to: u16, packet: &[u8]) -> Option<u64> {
        m.on_send(now, addr(from), addr(to), packet)
            .first()
            .map(|a| a.at_nanos)
    }

    const MS: u64 = 1_000_000;

    #[test]
    fn serialization_and_propagation_set_arrival() {
        // 8 Mbit/s: a 1000-byte packet takes exactly 1 ms to serialize.
        let m = LinkModel::new(link(8_000_000, 10_000, Loss::None), 1);
        assert_eq!(arrival(&m, 0, 1, 2, &[1; 1000]), Some(101 * MS));
        assert_eq!(
            arrival(&m, 0, 1, 2, &[2; 1000]),
            Some(102 * MS),
            "second packet waits behind the first"
        );
        assert_eq!(
            m.host_stats(addr(1)).max_queueing_delay,
            Duration::from_millis(1)
        );
        assert_eq!(m.base_rtt(addr(1), addr(2)), Duration::from_millis(200));
    }

    #[test]
    fn drop_tail_queue_is_shared_by_all_flows_from_a_host() {
        // Room for exactly three 1000-byte packets of backlog.
        let m = LinkModel::new(link(8_000_000, 3_000, Loss::None), 1);
        let delivered = (0..5u8)
            .filter(|i| arrival(&m, 0, 1, 10 + (*i as u16 % 2), &[*i; 1000]).is_some())
            .count();
        assert_eq!(delivered, 3);
        assert_eq!(m.host_stats(addr(1)).dropped_queue_full, 2);
        // Another host's uplink is independent.
        assert!(arrival(&m, 0, 2, 1, &[9; 1000]).is_some());
        // Once the queue drains, the host can send again.
        assert!(arrival(&m, 3 * MS, 1, 10, &[7; 1000]).is_some());
    }

    #[test]
    fn queue_admits_an_exact_fit_and_refuses_one_byte_more() {
        // 2000 bytes of capacity; the first 1000-byte packet leaves 1000 of
        // backlog (it is still serializing at t=0).
        let m = LinkModel::new(link(8_000_000, 2_000, Loss::None), 1);
        assert!(arrival(&m, 0, 1, 2, &[0; 1000]).is_some());
        assert!(
            arrival(&m, 0, 1, 2, &[1; 1001]).is_none(),
            "1000 + 1001 > 2000"
        );
        assert!(
            arrival(&m, 0, 1, 2, &[2; 1000]).is_some(),
            "1000 + 1000 = 2000 fits"
        );
    }

    #[test]
    fn a_lost_packet_still_uses_the_bottleneck() {
        let m = LinkModel::new(link(8_000_000, 10_000, Loss::Bernoulli(1.0)), 1);
        assert_eq!(arrival(&m, 0, 1, 2, &[1; 1000]), None);
        m.set_host_link(addr(1), link(8_000_000, 10_000, Loss::None));
        // Serializes after the lost packet's 1 ms, not at t=0.
        assert_eq!(arrival(&m, 0, 1, 2, &[2; 1000]), Some(102 * MS));
        assert_eq!(m.host_stats(addr(1)).dropped_random, 1);
    }

    #[test]
    fn loss_is_a_pure_function_of_the_seed() {
        let run = |seed| {
            let m = LinkModel::new(link(0, 0, Loss::Bernoulli(0.3)), seed);
            (0..200u32)
                .map(|i| arrival(&m, 0, 1, 2, &i.to_le_bytes()).is_some())
                .collect::<Vec<_>>()
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7), run(8));
        let delivered = run(7).iter().filter(|d| **d).count();
        assert!(
            (110..170).contains(&delivered),
            "~70% of 200, got {delivered}"
        );
    }

    #[test]
    fn gilbert_elliott_losses_come_in_bursts() {
        // Stationary bad-state share 0.01 / (0.01 + 0.2) ~ 4.8%, all lost there.
        let ge = Loss::GilbertElliott {
            to_bad: 0.01,
            to_good: 0.2,
            loss_good: 0.0,
            loss_bad: 1.0,
        };
        let m = LinkModel::new(link(0, 0, ge), 3);
        let lost: Vec<bool> = (0..20_000u32)
            .map(|i| arrival(&m, 0, 1, 2, &i.to_le_bytes()).is_none())
            .collect();
        let losses = lost.iter().filter(|l| **l).count();
        let bursts = lost.windows(2).filter(|w| !w[0] && w[1]).count().max(1);
        assert!(
            (600..1400).contains(&losses),
            "~4.8% of 20000, got {losses}"
        );
        let mean_burst = losses as f64 / bursts as f64;
        assert!(
            mean_burst > 3.0,
            "expected bursts of ~1/0.2 = 5 packets, got {mean_burst:.2}"
        );
    }

    #[test]
    fn spike_window_is_decided_at_departure_and_later_packets_queue_behind() {
        // 8 Mbit/s, so a 1000-byte packet serializes in 1 ms.
        let m = LinkModel::new(link(8_000_000, 10_000, Loss::None), 1);
        m.schedule(LinkEvent::DelaySpike {
            start: Duration::from_millis(1000),
            duration: Duration::from_millis(1000),
            extra_delay: Duration::from_secs(3),
            from: None,
        });
        // Offered before the window but departing at its first instant: spiked.
        let edge = arrival(&m, 999 * MS, 1, 2, &[1; 1000]).unwrap();
        assert_eq!(edge, 1000 * MS + 100 * MS + 3000 * MS);
        // Departing at the window's end: not spiked, but FIFO holds it back.
        let after = arrival(&m, 1999 * MS, 1, 2, &[2; 1000]).unwrap();
        assert_eq!(after, edge, "may not overtake the spiked packet");
        // Departing just before the window: not spiked.
        let m = LinkModel::new(link(8_000_000, 10_000, Loss::None), 1);
        m.schedule(LinkEvent::DelaySpike {
            start: Duration::from_millis(1000),
            duration: Duration::from_millis(1000),
            extra_delay: Duration::from_secs(3),
            from: None,
        });
        assert_eq!(arrival(&m, 998 * MS, 1, 2, &[3; 1000]), Some(1099 * MS));
    }

    #[test]
    fn spike_on_one_host_leaves_the_reverse_path_alone_and_spikes_add() {
        let m = LinkModel::new(link(0, 0, Loss::None), 1);
        for extra in [1, 2] {
            m.schedule(LinkEvent::DelaySpike {
                start: Duration::ZERO,
                duration: Duration::from_secs(10),
                extra_delay: Duration::from_secs(extra),
                from: Some(addr(1)),
            });
        }
        assert_eq!(arrival(&m, 0, 1, 2, b"a"), Some(3100 * MS));
        assert_eq!(arrival(&m, 0, 2, 1, b"b"), Some(100 * MS));
    }

    #[test]
    fn host_overrides_make_paths_asymmetric() {
        let m = LinkModel::new(link(0, 0, Loss::None), 1);
        m.set_host_link(
            addr(1),
            HostLink {
                one_way_delay: Duration::from_millis(30),
                ..link(0, 0, Loss::None)
            },
        );
        assert_eq!(arrival(&m, 0, 1, 2, b"a"), Some(30 * MS));
        assert_eq!(arrival(&m, 0, 2, 1, b"b"), Some(100 * MS));
        assert_eq!(m.base_rtt(addr(1), addr(2)), Duration::from_millis(130));
    }

    #[test]
    fn outage_drops_and_duplicates_are_not_first_copies() {
        let m = LinkModel::new(link(0, 0, Loss::None), 1);
        m.schedule(LinkEvent::Outage {
            start: Duration::from_secs(1),
            duration: Duration::from_secs(1),
        });
        assert!(arrival(&m, 1000 * MS, 1, 2, b"x").is_none());
        assert_eq!(m.host_stats(addr(1)).dropped_outage, 1);
        arrival(&m, 2000 * MS, 1, 2, b"x").unwrap();
        arrival(&m, 2000 * MS, 1, 2, b"x").unwrap();
        let d = m.deliveries(addr(1), addr(2));
        assert_eq!(
            d.iter().map(|d| d.first_copy).collect::<Vec<_>>(),
            [true, false]
        );
        assert_eq!(unique_bytes_between(&d, 0, u64::MAX), 1);
        // Offers include the copy the outage dropped.
        assert_eq!(m.offers(addr(1), addr(2)).len(), 3);
    }
}
