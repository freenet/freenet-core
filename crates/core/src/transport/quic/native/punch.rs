//! Punching for a native connection (#5940, design 2.4).
//!
//! Both sides of a native connection send small punch datagrams to the
//! other's address before and during the handshake, so each NAT has a
//! mapping for the other side when its first QUIC packet arrives. A punch is
//! [`PUNCH_LEN`] bytes, [`PUNCH_FIRST_BYTE`] then 8 random ones; the
//! receiver's demux drops it as `Other` and counts it.
//!
//! Punches go every [`PUNCH_INTERVAL`], at most [`PUNCHES_PER_DIAL`] per dial.
//! What one node sends toward one destination IP, punches and a client's
//! Initials together, is bounded by a [`PunchBudget`] across dials:
//! [`PUNCH_BUDGET_BYTES`] per minute per IP, charged when a dial starts at
//! what that dial can send at most, so a dial that is refused sends nothing.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::time::Duration;

use parking_lot::Mutex;

use crate::config::GlobalRng;
use crate::simulation::TimeSource;
use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
use crate::transport::normalize_mapped_addr;

/// Time between two punches of one dial.
pub(crate) const PUNCH_INTERVAL: Duration = Duration::from_millis(250);

/// Punches one dial sends at most: 8 s at [`PUNCH_INTERVAL`].
pub(crate) const PUNCHES_PER_DIAL: u32 = 32;

/// Bytes one destination IP may be sent per minute by native dials, punches
/// and Initials together.
pub(crate) const PUNCH_BUDGET_BYTES: u64 = 64 * 1024;

/// The window [`PUNCH_BUDGET_BYTES`] refills over.
pub(crate) const PUNCH_BUDGET_WINDOW: Duration = Duration::from_secs(60);

/// Destination IPs the budget remembers; the least recently charged one is
/// forgotten first, and starts again with a full budget.
pub(crate) const PUNCH_BUDGET_ENTRIES: usize = 4096;

/// Initials a native client sends at most in its window, a Retry's second
/// handshake included: from a 100 ms initial RTT the probe timeout doubles
/// from about 300 ms, so 8 s holds about six, and the Retry adds one or two.
pub(crate) const INITIALS_PER_DIAL: u64 = 8;

/// The most one dial's punches can send.
pub(crate) const PUNCH_BYTES_PER_DIAL: u64 = PUNCHES_PER_DIAL as u64 * PUNCH_LEN as u64;

/// The most a native client's dial can send toward its server: its punches
/// and its Initials (each padded to the 1200-byte minimum).
pub(crate) const CLIENT_BYTES_PER_DIAL: u64 = PUNCH_BYTES_PER_DIAL
    + INITIALS_PER_DIAL * crate::transport::quic::config::MAX_UDP_PAYLOAD as u64;

/// A token bucket per destination IP, on the caller's clock.
pub(crate) struct PunchBudget {
    buckets: Mutex<lru::LruCache<IpAddr, Bucket>>,
}

#[derive(Clone, Copy)]
struct Bucket {
    /// Bytes left, as of `at`.
    left: u64,
    at: Duration,
}

impl Default for PunchBudget {
    fn default() -> Self {
        Self::with_entries(PUNCH_BUDGET_ENTRIES)
    }
}

impl PunchBudget {
    fn with_entries(entries: usize) -> Self {
        Self {
            buckets: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(entries).unwrap_or(NonZeroUsize::MIN),
            )),
        }
    }

    /// Take `bytes` from `to`'s IP at `now` if they are there. A refusal
    /// takes nothing.
    pub(crate) fn charge(&self, to: SocketAddr, bytes: u64, now: Duration) -> bool {
        let ip = normalize_mapped_addr(to).ip();
        let mut buckets = self.buckets.lock();
        let bucket = buckets.get_or_insert_mut(ip, || Bucket {
            left: PUNCH_BUDGET_BYTES,
            at: now,
        });
        let elapsed = now.saturating_sub(bucket.at).as_nanos();
        let window = PUNCH_BUDGET_WINDOW.as_nanos();
        let refill = PUNCH_BUDGET_BYTES as u128 * elapsed / window;
        if refill > 0 {
            let left = (bucket.left as u128 + refill).min(PUNCH_BUDGET_BYTES as u128);
            bucket.left = left as u64;
            // Move the bucket's time on by what the credited bytes took, so
            // the part of a byte not yet earned is kept for the next charge;
            // a full bucket earns nothing more, so its time is now.
            bucket.at = if bucket.left == PUNCH_BUDGET_BYTES {
                now
            } else {
                let earned = refill * window / PUNCH_BUDGET_BYTES as u128;
                bucket.at + Duration::from_nanos(earned as u64)
            };
        }
        if bucket.left < bytes {
            return false;
        }
        bucket.left -= bytes;
        true
    }
}

/// One punch datagram: the reserved first byte and 8 random bytes.
pub(crate) fn punch_datagram() -> [u8; PUNCH_LEN] {
    let mut datagram = [0u8; PUNCH_LEN];
    datagram[0] = PUNCH_FIRST_BYTE;
    GlobalRng::fill_bytes(&mut datagram[1..]);
    datagram
}

/// Send punches to `to` through `send` every [`PUNCH_INTERVAL`], the first at
/// once, until [`PUNCHES_PER_DIAL`] have gone or `until` (on `clock`) has
/// passed. Never returns early otherwise: the caller races it with the
/// handshake and drops it when that ends. A failed send counts as sent, as a
/// lost datagram would.
pub(crate) async fn punch<T: TimeSource>(
    send: impl Fn(SocketAddr, &[u8; PUNCH_LEN]),
    to: SocketAddr,
    clock: &T,
    until: Duration,
) {
    for sent in 0..PUNCHES_PER_DIAL {
        if clock.now() >= until {
            return;
        }
        send(to, &punch_datagram());
        if sent + 1 < PUNCHES_PER_DIAL {
            clock.sleep_cancelable(PUNCH_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::simulation::VirtualTime;
    use crate::transport::quic::tests::{bounded, drive};

    fn to(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 31337)
    }

    /// A destination IP takes PUNCH_BUDGET_BYTES a minute: six client dials
    /// at once, the seventh refused, another IP unaffected, all of it back a
    /// minute later and half of it after half a minute.
    #[test]
    fn a_destination_ip_takes_the_budget_per_minute() {
        let budget = PunchBudget::default();
        let dials = PUNCH_BUDGET_BYTES / CLIENT_BYTES_PER_DIAL;
        assert_eq!(dials, 6, "about six dials a minute");
        let t0 = Duration::from_secs(100);
        for _ in 0..dials {
            assert!(budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0));
        }
        assert!(!budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0));
        assert!(budget.charge(to("198.51.100.2"), CLIENT_BYTES_PER_DIAL, t0));
        let half = t0 + PUNCH_BUDGET_WINDOW / 2;
        // Half refilled, on top of what the six dials left.
        let left = PUNCH_BUDGET_BYTES - dials * CLIENT_BYTES_PER_DIAL;
        let available = left + PUNCH_BUDGET_BYTES / 2;
        for _ in 0..available / CLIENT_BYTES_PER_DIAL {
            assert!(budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, half));
        }
        assert!(!budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, half));
        let later = half + PUNCH_BUDGET_WINDOW;
        for _ in 0..dials {
            assert!(budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, later));
        }
    }

    /// A refused charge takes nothing, and every port and the IPv4-mapped
    /// form of one IP share its budget.
    #[test]
    fn one_ip_is_one_budget() {
        let budget = PunchBudget::default();
        let now = Duration::ZERO;
        assert!(budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES - 10, now));
        assert!(!budget.charge(to("198.51.100.1"), 11, now));
        let other_port: SocketAddr = "198.51.100.1:9".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:198.51.100.1]:31337".parse().unwrap();
        assert!(budget.charge(other_port, 5, now));
        assert!(budget.charge(mapped, 5, now));
        assert!(!budget.charge(to("198.51.100.1"), 1, now));
    }

    /// Small charges at a fine interval lose nothing to rounding: from empty,
    /// taking every byte available each millisecond takes exactly the budget
    /// in one window.
    #[test]
    fn small_charges_lose_nothing_to_rounding() {
        let budget = PunchBudget::default();
        assert!(budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, Duration::ZERO));
        let mut taken = 0;
        for ms in 1..=PUNCH_BUDGET_WINDOW.as_millis() as u64 {
            while budget.charge(to("198.51.100.1"), 1, Duration::from_millis(ms)) {
                taken += 1;
            }
        }
        assert_eq!(taken, PUNCH_BUDGET_BYTES);
    }

    /// The least recently charged IP is forgotten first when the budget is
    /// full of IPs, and starts again with a full budget.
    #[test]
    fn a_forgotten_ip_starts_full() {
        let budget = PunchBudget::with_entries(2);
        let now = Duration::ZERO;
        assert!(budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, now));
        assert!(!budget.charge(to("198.51.100.1"), 1, now), "empty");
        assert!(budget.charge(to("198.51.100.2"), 1, now));
        assert!(budget.charge(to("198.51.100.3"), 1, now));
        assert!(budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, now));
    }

    /// A punch is the reserved byte and 8 bytes from GlobalRng.
    #[test]
    fn a_punch_is_the_reserved_byte_and_eight_random_ones() {
        let _seed = GlobalRng::seed_guard(7);
        let a = punch_datagram();
        let b = punch_datagram();
        assert_eq!(a.len(), PUNCH_LEN);
        assert_eq!(a[0], PUNCH_FIRST_BYTE);
        assert_ne!(a[1..], b[1..]);
        assert_eq!(
            crate::transport::demux::classify(&a),
            crate::transport::metrics::DemuxClass::Other
        );
    }

    /// Punches go at once and then every PUNCH_INTERVAL, at most
    /// PUNCHES_PER_DIAL of them, and none at or after `until`.
    #[test]
    fn punches_follow_the_interval_and_stop() {
        bounded(|| async {
            for (until, expected) in [
                (Duration::from_secs(60), PUNCHES_PER_DIAL as usize),
                (Duration::from_secs(1), 4),
                (Duration::ZERO, 0),
            ] {
                let time = VirtualTime::new();
                let sent = Arc::new(Mutex::new(Vec::new()));
                let (clock, record) = (time.clone(), sent.clone());
                let target = to("198.51.100.1");
                drive(&time, Duration::from_secs(120), async move {
                    let at = clock.clone();
                    punch(
                        move |dest, datagram| {
                            assert_eq!(dest, target);
                            assert_eq!(datagram[0], PUNCH_FIRST_BYTE);
                            record.lock().unwrap().push(at.now());
                        },
                        target,
                        &clock,
                        until,
                    )
                    .await
                })
                .await;
                let sent = sent.lock().unwrap().clone();
                assert_eq!(sent.len(), expected, "until {until:?}");
                for (i, at) in sent.iter().enumerate() {
                    assert_eq!(*at, PUNCH_INTERVAL * i as u32, "punch {i}");
                }
            }
        });
    }
}
