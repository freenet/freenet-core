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
//! version ([`OWN_VERSION`]) and compatibility floor ([`OWN_FLOOR`]): the exact
//! version string for this node's major.minor and a patch in the last
//! [`EXACT_WINDOW`] releases up to and including its own; when the floor is in
//! an earlier series (wire format 2, #6189), also the exact version for the
//! floor's major.minor and a patch in the [`EXACT_WINDOW`] releases from the
//! floor's; otherwise `older`, `newer` or `unknown`. So there are at most
//! [`BUCKET_SLOTS`] (67) labels whatever a peer sends, and 35 while the floor
//! shares this node's series: a joiner-chosen version can never create a new
//! one. Counts are fixed arrays indexed by slot, so the
//! bound is structural, not a cap that is checked.
//!
//! **Privacy.** No keys, no addresses. In `gateway_joins` only, a count from 1
//! to 4 is reported as the string `"<5"`, which hides small counts in the
//! hourly aggregate and nothing more. It does not stop a peer's version being
//! tied to the peer: `neighbours` is not suppressed, `connect_connected`
//! already uploads each connected peer's id and address, and peers with
//! telemetry on upload their own version, so a node's `neighbours` histogram
//! minus the known versions can give a telemetry-off neighbour's version. What
//! this event adds over existing telemetry is that version, which the peer
//! already advertises in the handshake to every node it connects to.
//! Reporting the versions of peers that turned their own telemetry off is
//! deliberate: it is the point of the census. Zero counts are omitted in both
//! maps.
//!
//! **Limits.** The first tick is skipped, so a node that restarts more often
//! than hourly never emits. Anyone who completes handshakes with a gateway can
//! inflate `gateway_joins` under a version of their choosing.
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
use crate::transport::connection_handler::{OWN_FLOOR, OWN_VERSION};

/// How often `join_versions` is emitted, and the window `gateway_joins`
/// covers (its counters reset at each emission).
pub(crate) const JOIN_VERSIONS_INTERVAL: Duration = Duration::from_secs(3600);

/// Releases with an exact label in each series: this node's own patch and
/// the 31 before it in this node's major.minor, and, when the compatibility
/// floor is in an earlier series, the floor's patch and the 31 after it.
/// 32 covers well over the releases a peer can be behind while still
/// compatible, and keeps the label set small.
pub(crate) const EXACT_WINDOW: u16 = 32;

/// Gateway-join counts below this are reported as `"<5"`.
const SMALL_COUNT_FLOOR: u64 = 5;

/// One slot per exact version in this node's series, one per exact version
/// in the floor's series, then `older`, `newer`, `unknown`.
pub(crate) const BUCKET_SLOTS: usize = 2 * EXACT_WINDOW as usize + 3;
const FLOOR_SLOT_BASE: usize = EXACT_WINDOW as usize;
const OLDER_SLOT: usize = 2 * EXACT_WINDOW as usize;
const NEWER_SLOT: usize = OLDER_SLOT + 1;
const UNKNOWN_SLOT: usize = OLDER_SLOT + 2;

/// Counts per version bucket, indexed by [`VersionBucket::slot`].
pub(crate) type BucketCounts = [u64; BUCKET_SLOTS];

/// The versions buckets are relative to: this build's version and its
/// compatibility floor, the two values the handshake advertises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VersionRef {
    pub(crate) own: (u8, u8, u16),
    pub(crate) floor: (u8, u8, u16),
}

impl VersionRef {
    /// Whether the floor is in another major.minor series than this build
    /// (wire format 2, #6189), so that series gets exact labels too.
    const fn cross_series(self) -> bool {
        self.floor.0 != self.own.0 || self.floor.1 != self.own.1
    }
}

/// This build: [`OWN_VERSION`] and [`OWN_FLOOR`], read from the same
/// constants `PROTOC_VERSION` encodes.
pub(crate) const THIS_BUILD: VersionRef = VersionRef {
    own: OWN_VERSION,
    floor: OWN_FLOOR,
};

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

/// A peer version relative to a [`VersionRef`]. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionBucket {
    /// This node's major.minor, `own_patch - offset` with `offset <
    /// EXACT_WINDOW`.
    Exact {
        offset: u16,
    },
    /// The floor's major.minor (only when it differs from this node's),
    /// `floor_patch + offset` with `offset < EXACT_WINDOW`.
    FloorExact {
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
            Self::FloorExact { offset } => FLOOR_SLOT_BASE + offset as usize,
            Self::Older => OLDER_SLOT,
            Self::Newer => NEWER_SLOT,
            Self::Unknown => UNKNOWN_SLOT,
        }
    }

    /// The bucket a slot holds, or `None` for an exact slot that is not a
    /// real version (own patch below 31, a floor in this node's own series,
    /// or a floor patch within 31 of `u16::MAX`).
    fn from_slot(r: VersionRef, slot: usize) -> Option<Self> {
        match slot {
            OLDER_SLOT => Some(Self::Older),
            NEWER_SLOT => Some(Self::Newer),
            UNKNOWN_SLOT => Some(Self::Unknown),
            offset if offset < FLOOR_SLOT_BASE => {
                (offset <= r.own.2 as usize).then_some(Self::Exact {
                    offset: offset as u16,
                })
            }
            slot if slot < OLDER_SLOT => {
                let offset = slot - FLOOR_SLOT_BASE;
                (r.cross_series() && r.floor.2 as usize + offset <= u16::MAX as usize).then_some(
                    Self::FloorExact {
                        offset: offset as u16,
                    },
                )
            }
            _ => None,
        }
    }

    pub(crate) fn label(self, r: VersionRef) -> String {
        match self {
            Self::Exact { offset } => format!("{}.{}.{}", r.own.0, r.own.1, r.own.2 - offset),
            Self::FloorExact { offset } => {
                format!("{}.{}.{}", r.floor.0, r.floor.1, r.floor.2 + offset)
            }
            Self::Older => "older".to_string(),
            Self::Newer => "newer".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

/// Bucket `peer` relative to `r`. `None` (the version was not learned) is
/// `unknown`.
pub(crate) fn version_bucket(r: VersionRef, peer: Option<(u8, u8, u16)>) -> VersionBucket {
    let Some(peer) = peer else {
        return VersionBucket::Unknown;
    };
    let own = r.own;
    if peer > own {
        return VersionBucket::Newer;
    }
    if (peer.0, peer.1) == (own.0, own.1) && peer.2 >= own.2.saturating_sub(EXACT_WINDOW - 1) {
        return VersionBucket::Exact {
            offset: own.2 - peer.2,
        };
    }
    let floor = r.floor;
    if r.cross_series()
        && (peer.0, peer.1) == (floor.0, floor.1)
        && peer.2 >= floor.2
        && peer.2 - floor.2 < EXACT_WINDOW
    {
        return VersionBucket::FloorExact {
            offset: peer.2 - floor.2,
        };
    }
    VersionBucket::Older
}

/// The label `peer` is reported under, relative to `r`.
#[cfg(test)]
pub(crate) fn bucket_label(r: VersionRef, peer: Option<(u8, u8, u16)>) -> String {
    version_bucket(r, peer).label(r)
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
        let slot = version_bucket(THIS_BUILD, peer).slot();
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
    r: VersionRef,
    versions: impl IntoIterator<Item = Option<(u8, u8, u16)>>,
) -> BucketCounts {
    let mut counts = [0; BUCKET_SLOTS];
    for version in versions {
        counts[version_bucket(r, version).slot()] += 1;
    }
    counts
}

fn bucket_map(
    r: VersionRef,
    counts: &BucketCounts,
    render: impl Fn(u64) -> serde_json::Value,
) -> serde_json::Map<String, serde_json::Value> {
    counts
        .iter()
        .enumerate()
        .filter(|(_, count)| **count > 0)
        .filter_map(|(slot, count)| {
            VersionBucket::from_slot(r, slot).map(|bucket| (bucket.label(r), render(*count)))
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
    r: VersionRef,
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
                let buckets = bucket_map(r, &joins[transport.index()], suppressed_count);
                (!buckets.is_empty()).then(|| (transport.as_str().to_string(), buckets.into()))
            })
            .collect();
        event.insert("gateway_joins".to_string(), by_transport.into());
    }
    event.insert(
        "neighbours".to_string(),
        bucket_map(r, neighbours, serde_json::Value::from).into(),
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
    let neighbours = neighbour_counts(THIS_BUILD, neighbour_versions);
    join_versions_json(THIS_BUILD, joins.as_ref(), &neighbours)
}

/// Spawn the hourly `join_versions` emitter and register it with the
/// `BackgroundTaskMonitor`. The caller in `p2p_impl.rs` spawns it only when
/// telemetry is enabled.
/// Spawn the reporter only when telemetry is on and this is not a test
/// environment: the event exists to be uploaded, and nothing else reads it.
pub(crate) fn spawn_join_versions_reporter_if_enabled(
    telemetry: &crate::config::TelemetryConfig,
    local_peer_id: String,
    connection_manager: ConnectionManager,
    monitor: &BackgroundTaskMonitor,
) -> bool {
    if !telemetry.enabled || telemetry.is_test_environment {
        return false;
    }
    spawn_join_versions_reporter(local_peer_id, connection_manager, monitor);
    true
}

fn spawn_join_versions_reporter(
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

    /// The reporter runs only with telemetry on outside a test environment,
    /// and is registered with the node's monitor under its name, so its exit
    /// is noticed like any other background task's.
    #[tokio::test]
    async fn the_reporter_is_gated_on_telemetry_and_registered() {
        let cases = [
            (true, false, true),
            (false, false, false),
            (true, true, false),
            (false, true, false),
        ];
        for (enabled, is_test_environment, expected) in cases {
            let telemetry = crate::config::TelemetryConfig {
                enabled,
                is_test_environment,
                ..Default::default()
            };
            let monitor = BackgroundTaskMonitor::new();
            let spawned = spawn_join_versions_reporter_if_enabled(
                &telemetry,
                "peer".to_string(),
                ConnectionManager::test_default(),
                &monitor,
            );
            assert_eq!(
                spawned, expected,
                "enabled={enabled} test_env={is_test_environment}"
            );
            let names = monitor.task_names();
            if expected {
                assert_eq!(names, vec!["join_versions_reporter"]);
            } else {
                assert!(names.is_empty(), "nothing spawned, got {names:?}");
            }
        }
    }

    /// A floor in this node's own series: the only exact labels are its own.
    const OWN: VersionRef = VersionRef {
        own: (0, 2, 157),
        floor: (0, 2, 150),
    };
    /// A floor in the previous series (#6189 format 2).
    const CROSS: VersionRef = VersionRef {
        own: (1, 0, 2),
        floor: (0, 2, 150),
    };

    #[test]
    fn buckets_at_the_window_edges() {
        assert_eq!(bucket_label(OWN, Some(OWN.own)), "0.2.157");
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
        let own = VersionRef {
            own: (0, 3, 2),
            floor: (0, 3, 0),
        };
        assert_eq!(bucket_label(own, Some((0, 3, 0))), "0.3.0");
        assert_eq!(bucket_label(own, Some((0, 2, 999))), "older");
        // A slot beyond patch 0 is not a real version and is never reported.
        assert_eq!(VersionBucket::from_slot(own, 3), None);
        // With the floor in its own series, no floor slot is a version.
        assert_eq!(VersionBucket::from_slot(own, FLOOR_SLOT_BASE), None);
    }

    fn distinct_labels(r: VersionRef) -> usize {
        let mut labels = std::collections::HashSet::new();
        for major in [0u8, 1, 2, 254, 255] {
            for minor in [0u8, 1, 2, 3, 255] {
                for patch in (0..=u16::MAX).step_by(7).chain(100..=200) {
                    labels.insert(bucket_label(r, Some((major, minor, patch))));
                }
            }
        }
        labels.insert(bucket_label(r, None));
        labels.len()
    }

    #[test]
    fn the_label_set_is_bounded_whatever_peers_send() {
        assert_eq!(BUCKET_SLOTS, 67);
        let same_series = [
            OWN,
            VersionRef {
                own: (0, 2, 3),
                floor: (0, 2, 0),
            },
            VersionRef {
                own: (255, 255, u16::MAX),
                floor: (255, 255, 0),
            },
        ];
        for r in same_series {
            let n = distinct_labels(r);
            assert!(n <= 35, "{n} labels for {r:?}");
        }
        let cross_series = [
            CROSS,
            VersionRef {
                own: (1, 0, 0),
                floor: (0, 255, u16::MAX - 3),
            },
            VersionRef {
                own: (2, 0, 40),
                floor: (1, 3, 0),
            },
        ];
        for r in cross_series {
            let n = distinct_labels(r);
            assert!(n <= BUCKET_SLOTS, "{n} labels for {r:?}");
        }
    }

    #[test]
    fn a_cross_series_floor_gets_exact_labels_for_its_series() {
        assert_eq!(bucket_label(CROSS, Some((0, 2, 150))), "0.2.150");
        assert_eq!(bucket_label(CROSS, Some((0, 2, 181))), "0.2.181");
        assert_eq!(bucket_label(CROSS, Some((0, 2, 182))), "older");
        assert_eq!(bucket_label(CROSS, Some((0, 2, 149))), "older");
        assert_eq!(bucket_label(CROSS, Some((0, 1, 160))), "older");
        assert_eq!(bucket_label(CROSS, Some((1, 0, 0))), "1.0.0");
        assert_eq!(bucket_label(CROSS, Some((1, 0, 2))), "1.0.2");
        assert_eq!(bucket_label(CROSS, Some((1, 0, 3))), "newer");
        for patch in 150..=181u16 {
            assert_eq!(
                bucket_label(CROSS, Some((0, 2, patch))),
                format!("0.2.{patch}")
            );
        }
        // A floor in this node's own series adds nothing: below the own
        // window is `older`, even at the floor's patch.
        let same = VersionRef {
            own: (0, 2, 200),
            floor: (0, 2, 150),
        };
        assert_eq!(bucket_label(same, Some((0, 2, 150))), "older");
        // Every reachable cross-series bucket round-trips through its slot.
        for (series, patches) in [((0u8, 2u8), 140..=190u16), ((1, 0), 0..=3)] {
            for patch in patches {
                let b = version_bucket(CROSS, Some((series.0, series.1, patch)));
                assert_eq!(VersionBucket::from_slot(CROSS, b.slot()), Some(b));
            }
        }
    }

    #[test]
    fn every_reachable_bucket_survives_the_slot_round_trip() {
        for patch in 0..=OWN.own.2 + 1 {
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
        let own_label = bucket_label(THIS_BUILD, Some(OWN_VERSION));

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
