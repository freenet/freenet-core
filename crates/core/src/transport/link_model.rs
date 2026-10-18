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
//! - Bernoulli loss, applied after the queue (a lost packet still used the
//!   bottleneck);
//! - a schedule of RTT spikes and outages.
//!
//! Delivery is FIFO per directed path: a packet never overtakes one sent
//! before it on the same path, so packets sent just after a spike ends wait
//! behind the spike's backlog, as they would behind a real stalled queue.
//!
//! Everything runs on virtual time and is a pure function of the seed and the
//! sequence of `on_send` calls. The model also keeps ground truth the
//! transport cannot see (queueing delay, the true base RTT, which delivered
//! packets were first copies) so tests can assert against it.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// One host's access link. Everything the host sends goes through it.
#[derive(Clone, Debug)]
pub struct HostLink {
    /// Uplink bottleneck rate in bits per second. `0` means unlimited.
    pub uplink_bps: u64,
    /// Capacity of the drop-tail queue in front of the uplink, in bytes.
    pub queue_bytes: usize,
    /// Propagation delay from this host to any peer.
    pub one_way_delay: Duration,
    /// Probability that a packet leaving the uplink is lost.
    pub loss: f64,
}

/// A scheduled change to every path. Times are virtual-time offsets from the
/// [`VirtualTime`](crate::simulation::VirtualTime) epoch.
#[derive(Clone, Debug)]
pub enum LinkEvent {
    /// Packets leaving the bottleneck during `[start, start + duration)` get
    /// `extra_one_way` added to their propagation delay.
    DelaySpike {
        start: Duration,
        duration: Duration,
        extra_one_way: Duration,
    },
    /// Every packet offered during `[start, start + duration)` is dropped.
    Outage { start: Duration, duration: Duration },
}

/// Why the model dropped a packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    QueueFull,
    RandomLoss,
    Outage,
}

/// One delivered datagram, as seen by the link.
#[derive(Clone, Copy, Debug)]
pub struct Delivery {
    pub arrives_at_nanos: u64,
    pub bytes: usize,
    /// False when an identical datagram was already delivered on this path.
    /// The transport retransmits the stored ciphertext, so a retransmission
    /// of a packet that did arrive is byte-identical to it.
    pub first_copy: bool,
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
    seen: HashSet<u64>,
    deliveries: Vec<Delivery>,
}

#[derive(Default)]
struct HostState {
    busy_until_nanos: u64,
    stats: HostStats,
}

struct State {
    rng: SmallRng,
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

    /// Decide the fate of one datagram sent at `now_nanos`. Returns the virtual
    /// time it arrives at `to`, or `None` if it is dropped.
    pub fn on_send(
        &self,
        now_nanos: u64,
        from: SocketAddr,
        to: SocketAddr,
        packet: &[u8],
    ) -> Option<u64> {
        self.on_send_inner(now_nanos, from, to, packet).ok()
    }

    fn on_send_inner(
        &self,
        now: u64,
        from: SocketAddr,
        to: SocketAddr,
        packet: &[u8],
    ) -> Result<u64, DropReason> {
        let mut guard = self.lock();
        let State {
            rng,
            overrides,
            events,
            hosts,
            paths,
        } = &mut *guard;
        let link = overrides.get(&from).unwrap_or(&self.default_link);
        let host = hosts.entry(from).or_default();
        host.stats.offered += 1;

        if events.iter().any(|e| match e {
            LinkEvent::Outage { start, duration } => in_window(now, *start, *duration),
            LinkEvent::DelaySpike { .. } => false,
        }) {
            host.stats.dropped_outage += 1;
            return Err(DropReason::Outage);
        }

        // Bottleneck: the uplink serializes one packet at a time. The bytes
        // still waiting are what the busy period represents at this rate.
        let len = packet.len();
        let (departs, waited) = if link.uplink_bps == 0 {
            (now, 0)
        } else {
            let start = host.busy_until_nanos.max(now);
            let waited = start - now;
            let backlog_bytes = waited as u128 * link.uplink_bps as u128 / 8 / NANOS_PER_SEC;
            if backlog_bytes + len as u128 > link.queue_bytes as u128 {
                host.stats.dropped_queue_full += 1;
                return Err(DropReason::QueueFull);
            }
            let serialization = (len as u128 * 8 * NANOS_PER_SEC / link.uplink_bps as u128) as u64;
            host.busy_until_nanos = start + serialization;
            (start + serialization, waited)
        };
        let waited = Duration::from_nanos(waited);
        if waited > host.stats.max_queueing_delay {
            host.stats.max_queueing_delay = waited;
        }

        if rng.random::<f64>() < link.loss {
            host.stats.dropped_random += 1;
            return Err(DropReason::RandomLoss);
        }

        let spike: Duration = events
            .iter()
            .filter_map(|e| match e {
                LinkEvent::DelaySpike {
                    start,
                    duration,
                    extra_one_way,
                } if in_window(departs, *start, *duration) => Some(*extra_one_way),
                LinkEvent::DelaySpike { .. } | LinkEvent::Outage { .. } => None,
            })
            .sum();
        let path = paths.entry((from, to)).or_default();
        let arrives =
            (departs + (link.one_way_delay + spike).as_nanos() as u64).max(path.last_arrival_nanos);
        path.last_arrival_nanos = arrives;
        let mut hasher = DefaultHasher::new();
        packet.hash(&mut hasher);
        let first_copy = path.seen.insert(hasher.finish());
        path.deliveries.push(Delivery {
            arrives_at_nanos: arrives,
            bytes: len,
            first_copy,
        });
        host.stats.delivered += 1;
        Ok(arrives)
    }

    pub fn host_stats(&self, host: SocketAddr) -> HostStats {
        self.lock()
            .hosts
            .get(&host)
            .map(|h| h.stats.clone())
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

    fn link(uplink_bps: u64, queue_bytes: usize, loss: f64) -> HostLink {
        HostLink {
            uplink_bps,
            queue_bytes,
            one_way_delay: Duration::from_millis(100),
            loss,
        }
    }

    #[test]
    fn serialization_and_propagation_set_arrival() {
        // 8 Mbit/s: a 1000-byte packet takes exactly 1 ms to serialize.
        let m = LinkModel::new(link(8_000_000, 10_000, 0.0), 1);
        let first = m.on_send(0, addr(1), addr(2), &[1; 1000]).unwrap();
        let second = m.on_send(0, addr(1), addr(2), &[2; 1000]).unwrap();
        assert_eq!(first, 101_000_000);
        assert_eq!(second, 102_000_000, "second packet waits behind the first");
        assert_eq!(
            m.host_stats(addr(1)).max_queueing_delay,
            Duration::from_millis(1)
        );
        assert_eq!(m.base_rtt(addr(1), addr(2)), Duration::from_millis(200));
    }

    #[test]
    fn drop_tail_queue_is_shared_by_all_flows_from_a_host() {
        // Room for exactly three 1000-byte packets of backlog.
        let m = LinkModel::new(link(8_000_000, 3_000, 0.0), 1);
        let delivered: Vec<_> = (0..5u8)
            .map(|i| m.on_send(0, addr(1), addr(10 + (i as u16 % 2)), &[i; 1000]))
            .collect();
        assert_eq!(delivered.iter().filter(|d| d.is_some()).count(), 3);
        assert_eq!(m.host_stats(addr(1)).dropped_queue_full, 2);
        // Another host's uplink is independent.
        assert!(m.on_send(0, addr(2), addr(1), &[9; 1000]).is_some());
        // Once the queue drains, the host can send again.
        assert!(
            m.on_send(3_000_000, addr(1), addr(10), &[7; 1000])
                .is_some()
        );
    }

    #[test]
    fn loss_is_a_pure_function_of_the_seed() {
        let run = |seed| {
            let m = LinkModel::new(link(0, 0, 0.3), seed);
            (0..200u32)
                .map(|i| m.on_send(0, addr(1), addr(2), &i.to_le_bytes()).is_some())
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
    fn spike_delays_packets_and_later_packets_queue_behind_it() {
        let m = LinkModel::new(link(0, 0, 0.0), 1);
        m.schedule(LinkEvent::DelaySpike {
            start: Duration::from_secs(1),
            duration: Duration::from_secs(1),
            extra_one_way: Duration::from_secs(3),
        });
        let before = m.on_send(500_000_000, addr(1), addr(2), b"a").unwrap();
        let during = m.on_send(1_500_000_000, addr(1), addr(2), b"b").unwrap();
        let after = m.on_send(2_100_000_000, addr(1), addr(2), b"c").unwrap();
        assert_eq!(before, 600_000_000);
        assert_eq!(during, 4_600_000_000);
        assert_eq!(after, during, "FIFO: may not overtake the spiked packet");
        // The reverse path has its own FIFO.
        assert_eq!(
            m.on_send(2_100_000_000, addr(2), addr(1), b"d").unwrap(),
            2_200_000_000
        );
    }

    #[test]
    fn outage_drops_and_duplicates_are_not_first_copies() {
        let m = LinkModel::new(link(0, 0, 0.0), 1);
        m.schedule(LinkEvent::Outage {
            start: Duration::from_secs(1),
            duration: Duration::from_secs(1),
        });
        assert!(m.on_send(1_000_000_000, addr(1), addr(2), b"x").is_none());
        assert_eq!(m.host_stats(addr(1)).dropped_outage, 1);
        m.on_send(2_000_000_000, addr(1), addr(2), b"x").unwrap();
        m.on_send(2_000_000_000, addr(1), addr(2), b"x").unwrap();
        let d = m.deliveries(addr(1), addr(2));
        assert_eq!(
            d.iter().map(|d| d.first_copy).collect::<Vec<_>>(),
            [true, false]
        );
        assert_eq!(unique_bytes_between(&d, 0, u64::MAX), 1);
    }
}
