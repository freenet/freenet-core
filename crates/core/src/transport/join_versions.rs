//! `join_versions`: which releases the network is actually running (#5940).
//!
//! Making QUIC the only transport needs to know which releases are still out
//! there, including nodes whose own telemetry is off. Two views, emitted once
//! per [`JOIN_VERSIONS_INTERVAL`]:
//!
//! - **Gateway joins** (gateways only): each SUCCESSFUL inbound gateway
//!   handshake, by the joiner's version bucket and the transport it joined
//!   over. Counted where the listener completes the handshake
//!   (`UdpPacketsListener::listen`, the `gw_connection_tasks` success arm in
//!   `connection_handler.rs`), never on intro receipt: an intro is spoofable,
//!   and the joiner has proved it holds the session key only once the
//!   handshake completes. These are joiners whose own telemetry may be off.
//! - **Neighbours** (every node): a snapshot at emission time of this node's
//!   live ring connections by their peer's version bucket, read from
//!   `ConnectionManager::live_neighbour_versions`. A snapshot, not a count of
//!   connection events, so reconnects do not skew it.
//!
//! **Bounded labels.** A version maps to a label relative to this node's own
//! version ([`OWN_VERSION`]): the exact version string for this node's
//! major.minor and a patch in the last [`EXACT_WINDOW`] releases up to and
//! including its own, otherwise `older`, `newer` or `unknown`. So there are at
//! most [`BUCKET_SLOTS`] labels whatever a peer sends: a joiner-chosen version
//! can never create a new one. Counts are fixed arrays indexed by slot, so the
//! bound is structural, not a cap that is checked.
//!
//! **Privacy.** No keys, no addresses. In `gateway_joins` only, a count from 1
//! to 4 is reported as the string `"<5"`, so one joiner in a quiet hour cannot
//! be singled out. Zero counts are omitted in both maps.
//!
//! **Observation only.** Nothing on the data path reads these counters.
//!
//! The gateway counters are process-wide, like `TRANSPORT_METRICS`, so several
//! gateways in one test process would share (and each drain) them. Telemetry is
//! off in tests, so the emitter does not run there.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::ring::ConnectionManager;
use crate::transport::connection_handler::OWN_VERSION;

/// How often `join_versions` is emitted, and the window `gateway_joins`
/// covers (its counters reset at each emission).
pub(crate) const JOIN_VERSIONS_INTERVAL: Duration = Duration::from_secs(3600);

/// Releases with an exact label: this node's own patch and the 31 before it,
/// in this node's major.minor. 32 covers well over the releases a peer can be
/// behind while still compatible, and keeps the label set small.
pub(crate) const EXACT_WINDOW: u16 = 32;

/// Gateway-join counts below this are reported as `"<5"`.
const SMALL_COUNT_FLOOR: u64 = 5;

/// One slot per exact version, then `older`, `newer`, `unknown`.
pub(crate) const BUCKET_SLOTS: usize = EXACT_WINDOW as usize + 3;
const OLDER_SLOT: usize = EXACT_WINDOW as usize;
const NEWER_SLOT: usize = EXACT_WINDOW as usize + 1;
const UNKNOWN_SLOT: usize = EXACT_WINDOW as usize + 2;

/// Counts per version bucket, indexed by [`VersionBucket::slot`].
pub(crate) type BucketCounts = [u64; BUCKET_SLOTS];

/// The transport a joiner reached the gateway over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinTransport {
    /// Today's UDP gateway handshake (intro packet, then ack).
    LegacyIntro,
    /// A join made over QUIC directly. Reserved: QUIC-native joins do not
    /// exist yet, so nothing records this and its count is always zero.
    #[allow(dead_code)]
    Native,
}

impl JoinTransport {
    pub(crate) const ALL: [Self; 2] = [Self::LegacyIntro, Self::Native];

    const fn index(self) -> usize {
        match self {
            Self::LegacyIntro => 0,
            Self::Native => 1,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyIntro => "legacy_intro",
            Self::Native => "native",
        }
    }
}

/// A peer version relative to this node's own version. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionBucket {
    /// This node's major.minor, `own_patch - offset` with `offset <
    /// EXACT_WINDOW`.
    Exact {
        offset: u16,
    },
    Older,
    Newer,
    Unknown,
}

impl VersionBucket {
    const fn slot(self) -> usize {
        match self {
            Self::Exact { offset } => offset as usize,
            Self::Older => OLDER_SLOT,
            Self::Newer => NEWER_SLOT,
            Self::Unknown => UNKNOWN_SLOT,
        }
    }

    /// The bucket a slot holds, or `None` for an exact slot that is not a
    /// real version (own patch below 31).
    fn from_slot(own: (u8, u8, u16), slot: usize) -> Option<Self> {
        match slot {
            OLDER_SLOT => Some(Self::Older),
            NEWER_SLOT => Some(Self::Newer),
            UNKNOWN_SLOT => Some(Self::Unknown),
            offset if offset < EXACT_WINDOW as usize && offset <= own.2 as usize => {
                Some(Self::Exact {
                    offset: offset as u16,
                })
            }
            _ => None,
        }
    }

    pub(crate) fn label(self, own: (u8, u8, u16)) -> String {
        match self {
            Self::Exact { offset } => format!("{}.{}.{}", own.0, own.1, own.2 - offset),
            Self::Older => "older".to_string(),
            Self::Newer => "newer".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

/// Bucket `peer` relative to `own`. `None` (the version was not learned) is
/// `unknown`.
pub(crate) fn version_bucket(own: (u8, u8, u16), peer: Option<(u8, u8, u16)>) -> VersionBucket {
    let Some(peer) = peer else {
        return VersionBucket::Unknown;
    };
    if peer > own {
        return VersionBucket::Newer;
    }
    if (peer.0, peer.1) == (own.0, own.1) && peer.2 >= own.2.saturating_sub(EXACT_WINDOW - 1) {
        return VersionBucket::Exact {
            offset: own.2 - peer.2,
        };
    }
    VersionBucket::Older
}

/// The label `peer` is reported under, relative to `own`.
#[cfg(test)]
pub(crate) fn bucket_label(own: (u8, u8, u16), peer: Option<(u8, u8, u16)>) -> String {
    version_bucket(own, peer).label(own)
}

/// Successful gateway joins since the last emission, by transport and bucket.
pub(crate) struct GatewayJoinCounters {
    counts: [[AtomicU64; BUCKET_SLOTS]; JoinTransport::ALL.len()],
}

impl GatewayJoinCounters {
    pub(crate) const fn new() -> Self {
        Self {
            counts: [const { [const { AtomicU64::new(0) }; BUCKET_SLOTS] };
                JoinTransport::ALL.len()],
        }
    }

    pub(crate) fn record(&self, transport: JoinTransport, peer: Option<(u8, u8, u16)>) {
        let slot = version_bucket(OWN_VERSION, peer).slot();
        self.counts[transport.index()][slot].fetch_add(1, Ordering::Relaxed);
    }

    /// The counts since the last call, resetting them to zero.
    pub(crate) fn take(&self) -> [BucketCounts; JoinTransport::ALL.len()] {
        let mut out = [[0; BUCKET_SLOTS]; JoinTransport::ALL.len()];
        for (row, counters) in out.iter_mut().zip(&self.counts) {
            for (value, counter) in row.iter_mut().zip(counters) {
                *value = counter.swap(0, Ordering::Relaxed);
            }
        }
        out
    }
}

/// The process-wide gateway-join counters the emitter drains.
pub(crate) static GATEWAY_JOINS: GatewayJoinCounters = GatewayJoinCounters::new();

/// Count one successful inbound gateway handshake at the listener bound to
/// `listener_addr`.
pub(crate) fn record_gateway_join(
    listener_addr: SocketAddr,
    transport: JoinTransport,
    peer: Option<(u8, u8, u16)>,
) {
    counters_for(listener_addr).record(transport, peer);
}

#[cfg(not(test))]
fn counters_for(_listener_addr: SocketAddr) -> &'static GatewayJoinCounters {
    &GATEWAY_JOINS
}

/// In unit tests a test can give one listener its own counters, so a count
/// asserted on cannot come from another test's gateway in the same process.
#[cfg(test)]
fn counters_for(listener_addr: SocketAddr) -> &'static GatewayJoinCounters {
    test_registry::TEST_COUNTERS
        .get(&listener_addr)
        .map(|entry| *entry.value())
        .unwrap_or(&GATEWAY_JOINS)
}

#[cfg(test)]
pub(crate) mod test_registry {
    use super::*;

    pub(super) static TEST_COUNTERS: std::sync::LazyLock<
        dashmap::DashMap<SocketAddr, &'static GatewayJoinCounters>,
    > = std::sync::LazyLock::new(dashmap::DashMap::new);

    /// Give the listener at `addr` its own counters and return them.
    pub(crate) fn isolate(addr: SocketAddr) -> &'static GatewayJoinCounters {
        let counters: &'static GatewayJoinCounters =
            Box::leak(Box::new(GatewayJoinCounters::new()));
        TEST_COUNTERS.insert(addr, counters);
        counters
    }
}

/// Count `versions` (one per live connection) into buckets.
pub(crate) fn neighbour_counts(
    own: (u8, u8, u16),
    versions: impl IntoIterator<Item = Option<(u8, u8, u16)>>,
) -> BucketCounts {
    let mut counts = [0; BUCKET_SLOTS];
    for version in versions {
        counts[version_bucket(own, version).slot()] += 1;
    }
    counts
}

fn bucket_map(
    own: (u8, u8, u16),
    counts: &BucketCounts,
    render: impl Fn(u64) -> serde_json::Value,
) -> serde_json::Map<String, serde_json::Value> {
    counts
        .iter()
        .enumerate()
        .filter(|(_, count)| **count > 0)
        .filter_map(|(slot, count)| {
            VersionBucket::from_slot(own, slot).map(|bucket| (bucket.label(own), render(*count)))
        })
        .collect()
}

/// A gateway-join count as reported: `"<5"` from 1 to 4, the number otherwise.
fn suppressed_count(count: u64) -> serde_json::Value {
    if count < SMALL_COUNT_FLOOR {
        serde_json::Value::from("<5")
    } else {
        serde_json::Value::from(count)
    }
}

/// Build the `join_versions` event. `gateway_joins` is `Some` only on a
/// gateway; a transport with no joins is omitted from it.
pub(crate) fn join_versions_json(
    own: (u8, u8, u16),
    gateway_joins: Option<&[BucketCounts; JoinTransport::ALL.len()]>,
    neighbours: &BucketCounts,
) -> serde_json::Value {
    let mut event = serde_json::Map::new();
    event.insert(
        "window_secs".to_string(),
        JOIN_VERSIONS_INTERVAL.as_secs().into(),
    );
    if let Some(joins) = gateway_joins {
        let by_transport: serde_json::Map<String, serde_json::Value> = JoinTransport::ALL
            .iter()
            .filter_map(|transport| {
                let buckets = bucket_map(own, &joins[transport.index()], suppressed_count);
                (!buckets.is_empty()).then(|| (transport.as_str().to_string(), buckets.into()))
            })
            .collect();
        event.insert("gateway_joins".to_string(), by_transport.into());
    }
    event.insert(
        "neighbours".to_string(),
        bucket_map(own, neighbours, serde_json::Value::from).into(),
    );
    event.into()
}

/// Drain `counters` (on a gateway) and build the event for this window.
fn take_join_versions(
    counters: &GatewayJoinCounters,
    is_gateway: bool,
    neighbour_versions: Vec<Option<(u8, u8, u16)>>,
) -> serde_json::Value {
    let joins = is_gateway.then(|| counters.take());
    let neighbours = neighbour_counts(OWN_VERSION, neighbour_versions);
    join_versions_json(OWN_VERSION, joins.as_ref(), &neighbours)
}

/// Spawn the hourly `join_versions` emitter and register it with the
/// `BackgroundTaskMonitor`. The caller in `p2p_impl.rs` spawns it only when
/// telemetry is enabled.
pub(crate) fn spawn_join_versions_reporter(
    local_peer_id: String,
    connection_manager: ConnectionManager,
    monitor: &BackgroundTaskMonitor,
) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(JOIN_VERSIONS_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes immediately; the first event covers a
        // full window.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let event = take_join_versions(
                &GATEWAY_JOINS,
                connection_manager.is_gateway(),
                connection_manager.live_neighbour_versions(),
            );
            crate::tracing::telemetry::send_standalone_event_with_peer_id(
                "join_versions",
                &local_peer_id,
                event,
            );
        }
    });
    monitor.register("join_versions_reporter", handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWN: (u8, u8, u16) = (0, 2, 157);

    #[test]
    fn buckets_at_the_window_edges() {
        assert_eq!(bucket_label(OWN, Some(OWN)), "0.2.157");
        assert_eq!(bucket_label(OWN, Some((0, 2, 157 - 31))), "0.2.126");
        assert_eq!(bucket_label(OWN, Some((0, 2, 157 - 32))), "older");
        assert_eq!(bucket_label(OWN, Some((0, 2, 158))), "newer");
        assert_eq!(bucket_label(OWN, Some((0, 1, 200))), "older");
        assert_eq!(bucket_label(OWN, Some((0, 3, 0))), "newer");
        assert_eq!(bucket_label(OWN, Some((1, 0, 0))), "newer");
        assert_eq!(bucket_label(OWN, None), "unknown");
    }

    #[test]
    fn a_low_own_patch_has_a_short_window() {
        let own = (0, 3, 2);
        assert_eq!(bucket_label(own, Some((0, 3, 0))), "0.3.0");
        assert_eq!(bucket_label(own, Some((0, 2, 999))), "older");
        // A slot beyond patch 0 is not a real version and is never reported.
        assert_eq!(VersionBucket::from_slot(own, 3), None);
    }

    #[test]
    fn the_label_set_is_bounded_whatever_peers_send() {
        let mut labels = std::collections::HashSet::new();
        for own in [OWN, (0, 2, 3), (1, 0, 0), (255, 255, u16::MAX)] {
            for major in [0u8, 1, 2, 254, 255] {
                for minor in [0u8, 1, 2, 3, 255] {
                    for patch in (0..=u16::MAX).step_by(7).chain(100..=200) {
                        labels.insert(bucket_label(own, Some((major, minor, patch))));
                    }
                }
            }
            labels.insert(bucket_label(own, None));
            assert!(
                labels.len() <= BUCKET_SLOTS,
                "{} labels for own {own:?}",
                labels.len()
            );
            labels.clear();
        }
        assert_eq!(BUCKET_SLOTS, 35);
    }

    #[test]
    fn every_reachable_bucket_survives_the_slot_round_trip() {
        for patch in 0..=OWN.2 + 1 {
            let bucket = version_bucket(OWN, Some((0, 2, patch)));
            assert_eq!(VersionBucket::from_slot(OWN, bucket.slot()), Some(bucket));
        }
    }

    fn counts(pairs: &[((u8, u8, u16), u64)]) -> BucketCounts {
        let mut out = [0; BUCKET_SLOTS];
        for (version, count) in pairs {
            out[version_bucket(OWN, Some(*version)).slot()] += count;
        }
        out
    }

    #[test]
    fn gateway_counts_below_five_are_suppressed_and_zeros_omitted() {
        let mut joins = [[0; BUCKET_SLOTS]; JoinTransport::ALL.len()];
        joins[JoinTransport::LegacyIntro.index()] = counts(&[
            ((0, 2, 157), 1),
            ((0, 2, 156), 4),
            ((0, 2, 155), 5),
            ((0, 2, 100), 12),
        ]);
        let neighbours = counts(&[((0, 2, 157), 1), ((0, 2, 156), 3)]);
        let json = join_versions_json(OWN, Some(&joins), &neighbours);
        println!("{json}");
        assert_eq!(
            json,
            serde_json::json!({
                "window_secs": 3600,
                "gateway_joins": {
                    "legacy_intro": {
                        "0.2.157": "<5",
                        "0.2.156": "<5",
                        "0.2.155": 5,
                        "older": 12,
                    },
                },
                "neighbours": { "0.2.157": 1, "0.2.156": 3 },
            })
        );
    }

    #[test]
    fn a_non_gateway_reports_no_gateway_joins() {
        let neighbours = counts(&[((0, 2, 157), 2)]);
        let json = join_versions_json(OWN, None, &neighbours);
        assert_eq!(
            json,
            serde_json::json!({ "window_secs": 3600, "neighbours": { "0.2.157": 2 } })
        );
    }

    #[test]
    fn take_join_versions_resets_the_gateway_counters() {
        let counters = GatewayJoinCounters::new();
        for _ in 0..6 {
            counters.record(JoinTransport::LegacyIntro, Some(OWN_VERSION));
        }
        counters.record(JoinTransport::LegacyIntro, None);
        let own_label = bucket_label(OWN_VERSION, Some(OWN_VERSION));

        let first = take_join_versions(&counters, true, vec![Some(OWN_VERSION), None]);
        assert_eq!(first["gateway_joins"]["legacy_intro"][&own_label], 6);
        assert_eq!(first["gateway_joins"]["legacy_intro"]["unknown"], "<5");
        assert_eq!(first["neighbours"][&own_label], 1);
        assert_eq!(first["neighbours"]["unknown"], 1);

        let second = take_join_versions(&counters, true, vec![]);
        assert_eq!(
            second["gateway_joins"],
            serde_json::json!({}),
            "the counters reset at each emission"
        );
        assert_eq!(second["neighbours"], serde_json::json!({}));
    }

    #[test]
    fn a_non_gateway_does_not_drain_the_counters() {
        let counters = GatewayJoinCounters::new();
        counters.record(JoinTransport::LegacyIntro, Some(OWN_VERSION));
        let json = take_join_versions(&counters, false, vec![]);
        assert!(json.get("gateway_joins").is_none());
        assert_eq!(
            counters.take()[JoinTransport::LegacyIntro.index()]
                .iter()
                .sum::<u64>(),
            1
        );
    }
}
