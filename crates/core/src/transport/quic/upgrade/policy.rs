//! Whether this node may upgrade a UDP session to QUIC, and how many QUIC
//! paths it may hold (#5940, step e5a-2).
//!
//! [`QuicPolicy::disabled_by`] names the first rule that stops upgrades, and
//! [`QuicPolicy::allowed`] is true when none does:
//!
//! - `transport-quic` is not `off` ([`QuicMode`]);
//! - the crash-loop breaker is not keeping QUIC off ([`BreakerOutcome`]).
//!   Today a suppressing breaker also stops the endpoint being built, so the
//!   availability is `Disabled` anyway; the policy checks the breaker itself
//!   so the result does not depend on that, and so a refusal names the cause;
//! - `quic-max-paths` is not 0, which is a decision to turn upgrades off;
//! - the endpoint is [`QuicAvailability::Up`]. Every other availability is
//!   final, so once the endpoint is down the node does not upgrade again
//!   before it restarts.
//!
//! [`QuicPathPermits`] bounds the paths the node holds, from offering or
//! dialing through active, at `quic-max-paths`. A permit is released when it
//! is dropped, so a path cannot keep one past its own end.
//!
//! [`QuicPolicy::own_caps`] is this node's half of the data negotiation: the
//! data bit for a peer in the slice, in `prefer` mode only.
//!
//! [`super::driver`] reads the policy for each decision. [`log_startup`] runs
//! at node startup.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::{NetworkApiConfig, QuicMaxPathsSetting};
use crate::transport::TransportPublicKey;
use crate::transport::metrics::QuicEndpointState;
use crate::transport::quic::QuicMode;
use crate::transport::quic::breaker::{BreakerOutcome, Verdict};
use crate::transport::quic::control::caps;
use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};

/// The share of pairs, in percent, that carry data on QUIC in this release
/// when it sets no ramp ([`QUIC_DATA_RAMP_START`]), for a node in `prefer`
/// mode: a fixed step (0, then 5 for the first data release). Both sides must
/// set their bit, so a pair moves only once both run a release that puts it in
/// the slice. `quic-data-percent` replaces it on one node.
pub(in crate::transport) const QUIC_DATA_PERCENT: u8 = 0;

/// Whether this node, as configured, may carry data on QUIC: `transport-quic`
/// is `prefer` and its share of pairs can be above 0 while it runs: a
/// `quic-data-percent` above 0, or with none, this release's ramp
/// ([`QUIC_DATA_RAMP_START`], from its first minute) or a fixed
/// [`QUIC_DATA_PERCENT`] above 0. While it is not, its QUIC paths are shadow
/// paths, which take no data and cost only the fixed charge, so the
/// connection limit is not lowered for QUIC memory
/// (`NetworkApiConfig::connection_limit`). The emergency-stop release (no
/// ramp, a share of 0) makes it false, so that release also gives the node
/// its configured limit back.
pub(crate) fn may_carry_data(config: &NetworkApiConfig) -> bool {
    may_carry_data_with(config, QUIC_DATA_RAMP_START)
}

/// [`may_carry_data`] with `ramp_start` in place of the release's.
fn may_carry_data_with(config: &NetworkApiConfig, ramp_start: Option<u64>) -> bool {
    configured_mode(config) == QuicMode::Prefer
        && match DataShare::of(config.quic_data_percent(), ramp_start) {
            DataShare::Fixed(percent) => percent > 0,
            DataShare::Ramp { .. } => true,
        }
}

/// When this release's data ramp starts, in seconds since the Unix epoch, or
/// `None` for no ramp: then [`QUIC_DATA_PERCENT`] applies. Set in its own reviewed PR, to
/// the planned publish time, just before the release's soak, and left set: past
/// 24 h it reads 100%, the steady state. A node in `prefer` mode carries data
/// with [`super::slice::ramp_percent`] of its pairs, from the wall clock at each
/// promotion. Both sides must set their bit, so a pair whose clocks disagree only
/// starts later. `quic-data-percent` replaces the ramp on one node.
/// `scripts/check_quic_data_ramp_t0.sh` reads this line, so keep it on one line
/// in this form.
// PLACEHOLDER (2100-01-01): replace with the planned publish time before the
// soak. Left as is, scripts/check_quic_data_ramp_t0.sh refuses a live cut.
pub(in crate::transport) const QUIC_DATA_RAMP_START: Option<u64> = Some(4_102_444_800);

/// The release whose cut must come within 3 h of [`QUIC_DATA_RAMP_START`], as
/// `"X.Y.Z"`, or `None`. Set together with the ramp start, in the same reviewed
/// PR, to the version of the release that ships it; release.yml's validate job
/// then refuses a live cut of that version unless the clock is within 3 h of the
/// start (`scripts/check_quic_data_ramp_t0.sh`, which reads this line, so keep
/// it on one line in this form). Left set afterwards, like the start: it names
/// one version, so a later release is not held to T0, though one that would
/// ship the ramp unchecked (before this release, or skipping it) is refused. An
/// emergency stop that sets the start back to `None` leaves this as it is.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::transport) const QUIC_DATA_RAMP_RELEASE: Option<&str> = Some("0.2.157");

/// Whether this build reads data from a QUIC path. It does: `PeerConnection::recv`
/// drains the session's data channels, so a node that says it accepts data delivers
/// a peer's frames. While this is `false`, [`QuicPolicy::own_caps`] returns 0
/// outside test builds (`cfg(test)` or the `testing` feature, which the simulations
/// use). A peer's data bit proves it reads, so a sender needs no version check
/// (`Session::enable_data`).
pub(in crate::transport) const QUIC_DATA_RECEIVER_READY: bool = true;

/// Whether `own_caps` may set the data bit in this build.
const fn data_bit_allowed(testing_build: bool) -> bool {
    testing_build || QUIC_DATA_RECEIVER_READY
}

/// How many of its pairs a `prefer` node carries data with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataShare {
    /// This percentage: `quic-data-percent`, or with no ramp
    /// [`QUIC_DATA_PERCENT`].
    Fixed(u8),
    /// The ramp from this start, in seconds since the Unix epoch.
    Ramp { start_unix_secs: u64 },
}

impl DataShare {
    /// The share for a `quic-data-percent` of `fixed`, with a ramp from `start`
    /// or else the release's fixed share.
    fn of(fixed: Option<u8>, start: Option<u64>) -> Self {
        match (fixed, start) {
            (Some(percent), _) => Self::Fixed(percent),
            (None, Some(start_unix_secs)) => Self::Ramp { start_unix_secs },
            (None, None) => Self::Fixed(QUIC_DATA_PERCENT),
        }
    }

    /// The percentage at `now`.
    pub(crate) fn percent_at(self, now: std::time::SystemTime) -> u8 {
        match self {
            Self::Fixed(percent) => percent,
            Self::Ramp { start_unix_secs } => {
                let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(start_unix_secs);
                // Before the start reads as the start.
                super::slice::ramp_percent(now.duration_since(start).unwrap_or_default())
            }
        }
    }
}

/// The band a percentage is logged by: 5, then every 10.
fn log_band(percent: u8) -> u8 {
    if percent < 10 {
        percent.min(5)
    } else {
        percent / 10 * 10
    }
}

/// The node's QUIC upgrade policy. See the module docs.
#[derive(Debug)]
pub(crate) struct QuicPolicy {
    mode: QuicMode,
    /// `quic-data-percent`, or the ramp ([`QUIC_DATA_RAMP_START`]).
    share: DataShare,
    /// `quic-data-percent` is set, so a test's own ramp does not apply
    /// (`QuicUpgradeTrace::ramp_data_started_before`).
    #[cfg(any(test, feature = "testing"))]
    percent_set: bool,
    /// The band last logged at INFO ([`log_band`]); `u8::MAX` before the first.
    logged_band: std::sync::atomic::AtomicU8,
    breaker: BreakerOutcome,
    availability: QuicAvailabilityWatch,
    permits: QuicPathPermits,
}

/// What a [`QuicPolicy`] takes from the node's configuration and its crash-loop
/// breaker. The endpoint's availability comes when the transport opens.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QuicPolicyConfig {
    mode: QuicMode,
    share: DataShare,
    #[cfg(any(test, feature = "testing"))]
    percent_set: bool,
    max_paths: usize,
    breaker: BreakerOutcome,
}

impl QuicPolicyConfig {
    /// The policy inputs for a node configured by `config`, whose crash-loop
    /// breaker decided `breaker` ([`breaker::gate`]).
    ///
    /// [`breaker::gate`]: crate::transport::quic::breaker::gate
    pub(crate) fn new(config: &NetworkApiConfig, breaker: BreakerOutcome) -> Self {
        Self {
            mode: configured_mode(config),
            share: DataShare::of(config.quic_data_percent(), QUIC_DATA_RAMP_START),
            #[cfg(any(test, feature = "testing"))]
            percent_set: config.quic_data_percent().is_some(),
            max_paths: config.quic_max_paths(),
            breaker,
        }
    }
}

/// Why the policy refuses upgrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisabledBy {
    /// `transport-quic` is off.
    Mode,
    /// The crash-loop breaker keeps QUIC off for this run, with this
    /// verdict.
    Breaker(Verdict),
    /// `quic-max-paths` is 0.
    NoPaths,
    /// The endpoint is not up: never built, down, or released.
    Endpoint(QuicAvailability),
}

/// Why [`QuicPolicy::admit`] refused a new path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathRefusal {
    /// The policy refuses upgrades at all.
    Disabled(DisabledBy),
    /// Every permit is held.
    Busy,
}

/// A bounded count of the QUIC paths the node holds. Clones share the count.
#[derive(Debug, Clone)]
pub(crate) struct QuicPathPermits(Arc<PermitCount>);

#[derive(Debug)]
struct PermitCount {
    limit: usize,
    in_use: AtomicUsize,
}

/// One held path slot, released when dropped.
#[derive(Debug)]
pub(crate) struct QuicPathPermit(Arc<PermitCount>);

impl Drop for QuicPathPermit {
    fn drop(&mut self) {
        self.0.in_use.fetch_sub(1, Ordering::AcqRel);
    }
}

impl QuicPolicy {
    /// The policy for a node configured by `config`, whose crash-loop
    /// breaker decided `breaker` ([`breaker::gate`]) and whose endpoint
    /// reports `availability`. Logs nothing: [`log_startup`] reports the
    /// configuration once.
    ///
    /// [`breaker::gate`]: crate::transport::quic::breaker::gate
    #[cfg(test)]
    pub(crate) fn new(
        config: &NetworkApiConfig,
        breaker: BreakerOutcome,
        availability: QuicAvailabilityWatch,
    ) -> Self {
        Self::from_config(QuicPolicyConfig::new(config, breaker), availability)
    }

    /// The policy for `config`, whose endpoint reports `availability`.
    pub(crate) fn from_config(
        config: QuicPolicyConfig,
        availability: QuicAvailabilityWatch,
    ) -> Self {
        Self {
            mode: config.mode,
            share: config.share,
            #[cfg(any(test, feature = "testing"))]
            percent_set: config.percent_set,
            logged_band: std::sync::atomic::AtomicU8::new(u8::MAX),
            breaker: config.breaker,
            availability,
            permits: QuicPathPermits::new(config.max_paths),
        }
    }

    /// The mode the node runs.
    #[cfg(test)]
    pub(crate) fn mode(&self) -> QuicMode {
        self.mode
    }

    /// The first rule that stops upgrades now, in the order of the module
    /// docs, or `None` when upgrades are allowed. Read it again for each
    /// decision: it changes for good when the endpoint goes down.
    pub(crate) fn disabled_by(&self) -> Option<DisabledBy> {
        if self.mode == QuicMode::Off {
            return Some(DisabledBy::Mode);
        }
        if let Some(verdict) = self.breaker.suppressing_verdict() {
            return Some(DisabledBy::Breaker(verdict));
        }
        if self.permits.limit() == 0 {
            return Some(DisabledBy::NoPaths);
        }
        match self.availability.get() {
            QuicAvailability::Up => None,
            other @ (QuicAvailability::Disabled
            | QuicAvailability::Down
            | QuicAvailability::Released) => Some(DisabledBy::Endpoint(other)),
        }
    }

    /// Whether the node may upgrade a session to QUIC now.
    pub(crate) fn allowed(&self) -> bool {
        self.disabled_by().is_none()
    }

    /// A permit for one new path, or why there is none. A disabled policy
    /// refuses without touching the permits.
    ///
    /// `Ok` does not prove the endpoint is still up: it can go down just
    /// after the check, so a path must still handle a down endpoint.
    pub(crate) fn admit(&self) -> Result<QuicPathPermit, PathRefusal> {
        if let Some(cause) = self.disabled_by() {
            return Err(PathRefusal::Disabled(cause));
        }
        self.permits.try_acquire().ok_or(PathRefusal::Busy)
    }

    /// What this node says it supports on a path to `peer`
    /// ([`crate::transport::quic::control::caps`]). The peer's key comes from the
    /// node's record, never from a message, so no message can set the bucket. A
    /// peer picks its own keypair and so its own bucket: the slice controls the
    /// rollout, it is not a defence.
    ///
    /// `now` is the wall clock (the node's path clock; a simulation's virtual time)
    /// when the session is promoted, which is when this is asked: a pair that
    /// enters the slice later takes data from its next session.
    pub(in crate::transport) fn own_caps(
        &self,
        local: &TransportPublicKey,
        peer: &TransportPublicKey,
        now: std::time::SystemTime,
    ) -> u32 {
        self.own_caps_with(self.share, local, peer, now)
    }

    /// Whether `quic-data-percent` is set. Tests only.
    #[cfg(any(test, feature = "testing"))]
    pub(in crate::transport) fn percent_set(&self) -> bool {
        self.percent_set
    }

    /// The policy's data share. Tests only.
    #[cfg(any(test, feature = "testing"))]
    pub(in crate::transport) fn share(&self) -> DataShare {
        self.share
    }

    /// [`Self::own_caps`] with `share` in place of the policy's (a simulation's
    /// own ramp start).
    pub(in crate::transport) fn own_caps_with(
        &self,
        share: DataShare,
        local: &TransportPublicKey,
        peer: &TransportPublicKey,
        now: std::time::SystemTime,
    ) -> u32 {
        if !data_bit_allowed(cfg!(any(test, feature = "testing"))) {
            return 0;
        }
        match self.mode {
            QuicMode::Prefer => {
                let percent = share.percent_at(now);
                self.log_band(percent, share);
                let data = if super::slice::in_slice(local, peer, percent) {
                    caps::DATA
                } else {
                    0
                };
                // A node that carries data with some pairs, or will, takes a
                // recycle for data as no fault and is worth recycling toward.
                // One that never will (a share fixed at 0) does not say so, so
                // no peer closes a path to it that could never carry data.
                let recycles = if share == DataShare::Fixed(0) {
                    0
                } else {
                    caps::RECYCLES
                };
                data | recycles
            }
            QuicMode::Off | QuicMode::Shadow => 0,
        }
    }

    /// Whether this node's data bit on a path can still come on while the
    /// node runs: only on a ramp, in `prefer`. A fixed share gives a pair the
    /// same bit for the life of the process, so a path without the bit is never
    /// rechecked for it (`machine::UpgradeMachine::recheck_data`).
    pub(in crate::transport) fn bit_may_come_on_with(&self, share: DataShare) -> bool {
        data_bit_allowed(cfg!(any(test, feature = "testing")))
            && self.mode == QuicMode::Prefer
            && matches!(share, DataShare::Ramp { .. })
    }

    /// The share this node applies to its pairs' data bit, for telemetry
    /// (`quic_ramp_share_pct`): the policy's share in `prefer` where the build
    /// lets the bit be set and nothing that cannot change while the node runs
    /// keeps QUIC off (the breaker, `quic-max-paths = 0`), else `None`
    /// (reported as 0). The endpoint's state can change, so the gauge reads it
    /// at each snapshot ([`crate::transport::metrics::ShareSource`]).
    pub(crate) fn advertised_share(&self) -> Option<DataShare> {
        let can_carry = data_bit_allowed(cfg!(any(test, feature = "testing")))
            && self.mode == QuicMode::Prefer
            && self.breaker.suppressing_verdict().is_none()
            && self.permits.limit() > 0;
        can_carry.then_some(self.share)
    }

    /// Where `quic_ramp_share_pct` is read from: [`Self::advertised_share`],
    /// the endpoint's availability and the wall clock `wall`.
    pub(crate) fn share_source(
        &self,
        wall: Box<dyn Fn() -> std::time::SystemTime + Send + Sync>,
    ) -> Option<crate::transport::metrics::ShareSource> {
        let share = self.advertised_share()?;
        Some(crate::transport::metrics::ShareSource {
            share,
            availability: self.availability.clone(),
            wall,
        })
    }

    /// [`Self::bit_may_come_on_with`] with the policy's share.
    pub(in crate::transport) fn bit_may_come_on(&self) -> bool {
        self.bit_may_come_on_with(self.share)
    }

    /// Report the share at INFO when it enters a new band, so an operator can see
    /// the ramp move.
    ///
    /// Called from `own_caps`, which the upgrade driver calls while it holds a
    /// session's lock. The INFO line is written only when the band changes
    /// (an atomic swap decides it): 11 times over a whole ramp (5%, then every
    /// 10% to 100%), more only if the wall clock steps back and forth. So the
    /// lock is held over a log write that rarely; every other call is the swap
    /// alone.
    fn log_band(&self, percent: u8, share: DataShare) {
        let band = log_band(percent);
        let before = self
            .logged_band
            .swap(band, std::sync::atomic::Ordering::Relaxed);
        if before == band {
            return;
        }
        if percent == 0 {
            tracing::info!(
                share = ?share,
                "QUIC data: this node offers data to none of its pairs (share 0%)"
            );
        } else {
            tracing::info!(
                data_percent = percent,
                share = ?share,
                "QUIC data: this node offers data to this share of its pairs"
            );
        }
    }

    /// The path permits.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn permits(&self) -> &QuicPathPermits {
        &self.permits
    }
}

impl QuicPathPermits {
    /// Permits for at most `limit` paths at once. A limit of 0 admits none.
    pub(crate) fn new(limit: usize) -> Self {
        Self(Arc::new(PermitCount {
            limit,
            in_use: AtomicUsize::new(0),
        }))
    }

    /// A permit, or `None` when `limit` are held.
    pub(crate) fn try_acquire(&self) -> Option<QuicPathPermit> {
        let count = &self.0;
        count
            .in_use
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < count.limit).then_some(held + 1)
            })
            .ok()
            .map(|_| QuicPathPermit(Arc::clone(&self.0)))
    }

    /// The permits held now.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    pub(crate) fn in_use(&self) -> usize {
        self.0.in_use.load(Ordering::Acquire)
    }

    /// The most permits that can be held at once.
    pub(crate) fn limit(&self) -> usize {
        self.0.limit
    }
}

/// The configured `transport-quic` mode. An unknown value is off, and
/// `quic_settings` has already warned about it.
fn configured_mode(config: &NetworkApiConfig) -> QuicMode {
    QuicMode::from_config_value(config.transport_quic.as_deref())
}

/// The QUIC endpoint state of a node that will not build an endpoint, decided
/// before it binds: off by its `transport-quic` setting, off by this
/// platform's default, or kept off by the crash-loop breaker (`gated_on` is
/// false although the mode builds one). `None` when the node goes on to build
/// the endpoint; the bind then records `Up` or why it failed.
pub(crate) fn endpoint_state_before_bind(
    config: &NetworkApiConfig,
    gated_on: bool,
) -> Option<QuicEndpointState> {
    if configured_mode(config) == QuicMode::Off {
        return Some(if config.transport_quic.is_some() {
            QuicEndpointState::OffConfig
        } else {
            QuicEndpointState::OffPlatform
        });
    }
    (!gated_on).then_some(QuicEndpointState::OffBreaker)
}

/// Whether `transport-quic` is set to `off` itself, as opposed to unset or to
/// a value this binary does not know (which `QuicMode::from_config_value`
/// warns about).
fn set_to_off(config: &NetworkApiConfig) -> bool {
    config
        .transport_quic
        .as_deref()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(QuicMode::OFF))
}

/// Report the QUIC upgrade configuration once, at node startup: the
/// configured mode and whether it was set or is the platform default (INFO;
/// the breaker, a failed bind or an abort build can still keep QUIC off, and
/// each says so in its own line), the share of
/// pairs `prefer` negotiates data with (INFO), and, with QUIC on, a
/// `quic-data-percent` (WARN) or `quic-max-paths` (WARN) that cannot be parsed, or
/// a `quic-max-paths` lowered to the node's connection limit (INFO). In a build
/// that aborts on panic QUIC stays off whatever the
/// configuration (`QuicEndpoint::bind` refuses), and after the mode line only
/// that is reported (WARN).
pub(crate) fn log_startup(config: &NetworkApiConfig) {
    tracing::info!(
        mode = ?configured_mode(config),
        source = if config.transport_quic.is_some() { "configured" } else { "default" },
        "QUIC transport mode configured"
    );
    if set_to_off(config) {
        tracing::warn!(
            "transport-quic = \"off\" is deprecated: QUIC is to replace the UDP transport, \
             and a later release will not offer a way to turn it off"
        );
    }
    if configured_mode(config) != QuicMode::Off && !crate::transport::quic::UNWINDS {
        tracing::warn!(
            transport_quic = ?config.transport_quic,
            "This build aborts on panic (panic = \"abort\"), so QUIC stays off: the QUIC \
             transport needs panics to unwind to contain them"
        );
        return;
    }
    if configured_mode(config) == QuicMode::Prefer {
        let share = DataShare::of(config.quic_data_percent(), QUIC_DATA_RAMP_START);
        if share == DataShare::Fixed(0) {
            tracing::info!(
                share = ?share,
                "transport-quic = \"prefer\" with no data share (no ramp in this build \
                 and no quic-data-percent): every pair keeps its data on UDP"
            );
        } else {
            tracing::info!(
                share = ?share,
                "transport-quic = \"prefer\" negotiates data with this share of pairs: a \
                 pair sends its data on QUIC only if both sides set their data bit, and all \
                 other traffic stays on UDP"
            );
        }
    }
    // These keys matter only with QUIC on.
    if configured_mode(config) == QuicMode::Off {
        return;
    }
    if let (Some(raw), None) = (
        config.quic_data_percent.as_deref(),
        config.quic_data_percent(),
    ) {
        tracing::warn!(
            quic_data_percent = %raw,
            ramp_start = ?QUIC_DATA_RAMP_START,
            "quic-data-percent is not a whole number from 0 to 100 and is ignored; using \
             this release's share of pairs"
        );
    }
    let max_connections = config.connection_limit().max;
    match config.quic_max_paths_setting() {
        QuicMaxPathsSetting::Unset => {}
        QuicMaxPathsSetting::Invalid(raw) => tracing::warn!(
            quic_max_paths = %raw,
            default = max_connections,
            "quic-max-paths is not a whole number and is ignored; using the connection \
             limit"
        ),
        QuicMaxPathsSetting::Set(limit) if limit > max_connections => tracing::info!(
            quic_max_paths = limit,
            max_connections,
            "quic-max-paths is above the connection limit and is lowered to it"
        ),
        QuicMaxPathsSetting::Set(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ramp start names the release it ships in, so the release-time check
    /// (`scripts/check_quic_data_ramp_t0.sh`) has a version to key on; a
    /// named release is `X.Y.Z`, the form the script and release.yml use. A
    /// release with no start is allowed: an emergency stop resets the start
    /// and leaves the release, which names an earlier version.
    #[test]
    fn a_ramp_start_names_its_release() {
        if QUIC_DATA_RAMP_START.is_some() {
            assert!(
                QUIC_DATA_RAMP_RELEASE.is_some(),
                "QUIC_DATA_RAMP_START is set, so QUIC_DATA_RAMP_RELEASE must name its release"
            );
        }
        if let Some(release) = QUIC_DATA_RAMP_RELEASE {
            let parts: Vec<&str> = release.split('.').collect();
            assert!(
                parts.len() == 3
                    && parts
                        .iter()
                        .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())),
                "QUIC_DATA_RAMP_RELEASE must be X.Y.Z, got {release:?}"
            );
        }
    }

    const AVAILABILITIES: [QuicAvailability; 4] = [
        QuicAvailability::Disabled,
        QuicAvailability::Up,
        QuicAvailability::Down,
        QuicAvailability::Released,
    ];

    const BREAKERS: [BreakerOutcome; 3] = [
        BreakerOutcome::NotConsulted,
        BreakerOutcome::Decided(Verdict::Arm),
        BreakerOutcome::Decided(Verdict::Trip),
    ];

    /// Before the bind, a node reports off by config for `off` or an unknown
    /// value, off by platform for an unset key on a platform whose default is
    /// off, and off by the breaker when the breaker took the settings away;
    /// otherwise it leaves the state to the bind.
    #[test]
    fn the_endpoint_state_before_bind_names_why_quic_is_off() {
        use QuicEndpointState::*;
        for value in ["off", " OFF ", "bogus"] {
            let config = config(value, None);
            assert_eq!(
                endpoint_state_before_bind(&config, false),
                Some(OffConfig),
                "{value}"
            );
        }
        for value in ["shadow", "prefer"] {
            let config = config(value, None);
            assert_eq!(endpoint_state_before_bind(&config, true), None, "{value}");
            assert_eq!(
                endpoint_state_before_bind(&config, false),
                Some(OffBreaker),
                "{value}"
            );
        }
        let mut unset = config("shadow", None);
        unset.transport_quic = None;
        if QuicMode::default_mode() == QuicMode::Off {
            assert_eq!(endpoint_state_before_bind(&unset, false), Some(OffPlatform));
        } else {
            assert_eq!(endpoint_state_before_bind(&unset, true), None);
            assert_eq!(endpoint_state_before_bind(&unset, false), Some(OffBreaker));
        }
    }

    /// The deprecation warning is for `off` itself, not for an unset key or an
    /// unknown value (which has its own warning).
    #[test]
    fn only_transport_quic_off_is_deprecated() {
        for (value, deprecated) in [
            (Some("off"), true),
            (Some(" Off "), true),
            (Some("shadow"), false),
            (Some("prefer"), false),
            (Some("bogus"), false),
            (None, false),
        ] {
            let mut config = config("shadow", None);
            config.transport_quic = value.map(str::to_string);
            assert_eq!(set_to_off(&config), deprecated, "{value:?}");
        }
    }

    fn config(transport_quic: &str, max_paths: Option<&str>) -> NetworkApiConfig {
        // Every key has a default, so an empty document is the default config.
        let mut config: NetworkApiConfig = toml::from_str("").expect("default network config");
        config.transport_quic = Some(transport_quic.to_string());
        config.quic_max_paths = max_paths.map(str::to_string);
        config
    }

    fn policy(
        mode: &str,
        breaker: BreakerOutcome,
        availability: QuicAvailability,
        max_paths: usize,
    ) -> QuicPolicy {
        let (_sender, watch) = QuicAvailabilityWatch::for_test(availability);
        QuicPolicy::new(&config(mode, Some(&max_paths.to_string())), breaker, watch)
    }

    /// `disabled_by()` over every combination of its inputs names the first
    /// rule that applies, in the documented order, and `allowed()` and
    /// `admit()` agree with it. Each rule has a case where it alone decides,
    /// so dropping or reordering any one of them fails this test.
    #[test]
    fn disabled_by_names_the_first_rule_that_applies() {
        let mut allowed_cases = 0;
        for mode in ["off", "shadow", "prefer"] {
            for breaker in BREAKERS {
                for max_paths in [0, 4] {
                    for availability in AVAILABILITIES {
                        let expected = if mode == "off" {
                            Some(DisabledBy::Mode)
                        } else if let BreakerOutcome::Decided(Verdict::Trip) = breaker {
                            Some(DisabledBy::Breaker(Verdict::Trip))
                        } else if max_paths == 0 {
                            Some(DisabledBy::NoPaths)
                        } else if availability != QuicAvailability::Up {
                            Some(DisabledBy::Endpoint(availability))
                        } else {
                            None
                        };
                        let case = format!(
                            "mode {mode}, breaker {breaker:?}, max_paths {max_paths}, \
                             availability {availability:?}"
                        );
                        let policy = policy(mode, breaker, availability, max_paths);
                        assert_eq!(policy.disabled_by(), expected, "{case}");
                        assert_eq!(policy.allowed(), expected.is_none(), "{case}");
                        match (policy.admit(), expected) {
                            (Ok(_), None) => allowed_cases += 1,
                            (Err(refusal), Some(cause)) => {
                                assert_eq!(refusal, PathRefusal::Disabled(cause), "{case}");
                                assert_eq!(policy.permits().in_use(), 0, "{case}");
                            }
                            (outcome, _) => panic!("{case}: admit gave {outcome:?}"),
                        }
                    }
                }
            }
        }
        // shadow and prefer, with a breaker that did not run or armed.
        assert_eq!(allowed_cases, 4);
    }

    /// The policy reads the availability at each call: an endpoint that goes
    /// down after the policy was built turns it off, and paths admitted
    /// before keep their permits until dropped.
    #[test]
    fn endpoint_going_down_turns_the_policy_off() {
        let (sender, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let policy = QuicPolicy::new(
            &config("shadow", Some("2")),
            BreakerOutcome::NotConsulted,
            watch,
        );
        let held = policy.admit().expect("allowed while up");
        sender.send_replace(QuicAvailability::Down);
        assert_eq!(
            policy.admit().unwrap_err(),
            PathRefusal::Disabled(DisabledBy::Endpoint(QuicAvailability::Down))
        );
        assert_eq!(policy.permits().in_use(), 1);
        drop(held);
        assert_eq!(policy.permits().in_use(), 0);
    }

    /// The policy takes its mode and limit from the config: the limit is
    /// `quic-max-paths` lowered to the node's connection limit
    /// (`max-number-of-connections` here, as mode `off` carries no data), and
    /// an unparsable value means the default.
    #[test]
    fn the_policy_reads_mode_and_limit_from_the_config() {
        let up = || QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let build = |mode, max_paths| {
            QuicPolicy::new(&config(mode, max_paths), BreakerOutcome::NotConsulted, up())
        };
        let max_connections = config("off", None).max_connections;
        assert!(max_connections > 3, "premise");
        for (mode, effective) in [
            ("off", QuicMode::Off),
            ("shadow", QuicMode::Shadow),
            ("prefer", QuicMode::Prefer),
            ("a-later-mode", QuicMode::Off),
        ] {
            assert_eq!(build(mode, None).mode(), effective, "{mode}");
        }
        for (max_paths, limit) in [
            (None, max_connections),
            (Some("3"), 3),
            (Some(" 3 "), 3),
            (Some("0"), 0),
            (Some("lots"), max_connections),
            (Some("-1"), max_connections),
        ] {
            assert_eq!(
                build("shadow", max_paths).permits().limit(),
                limit,
                "{max_paths:?}"
            );
        }
        let above = (max_connections + 1).to_string();
        assert_eq!(
            build("shadow", Some(&above)).permits().limit(),
            max_connections,
            "lowered to the connection limit (max-number-of-connections, as shadow carries no data)"
        );
    }

    /// `may_carry_data` (what lowers the connection limit) holds exactly when
    /// `own_caps` can give some pair the data bit, so the two rules cannot
    /// drift: for each mode and share, over 400 pairs (at 5% the chance that
    /// none is in the slice is under 1e-8).
    #[test]
    fn may_carry_data_matches_the_data_bit_rule() {
        use crate::transport::TransportKeypair;
        let pairs: Vec<_> = (0..400)
            .map(|_| {
                (
                    TransportKeypair::new().public().clone(),
                    TransportKeypair::new().public().clone(),
                )
            })
            .collect();
        for mode in ["off", "shadow", "prefer"] {
            for percent in [None, Some("0"), Some("5"), Some("50"), Some("100")] {
                let mut config = config(mode, None);
                config.quic_data_percent = percent.map(str::to_string);
                let up = QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
                let policy = QuicPolicy::new(&config, BreakerOutcome::NotConsulted, up);
                let some_pair_carries = pairs.iter().any(|(local, peer)| {
                    policy.own_caps(local, peer, std::time::SystemTime::now()) & caps::DATA != 0
                });
                assert_eq!(
                    may_carry_data(&config),
                    some_pair_carries,
                    "{mode} {percent:?}"
                );
            }
        }
    }

    /// With a ramp in the release, a `prefer` node may carry data from the
    /// ramp's first minute unless `quic-data-percent` overrides it, so the
    /// connection limit is lowered for the whole ramp; without one (the
    /// emergency-stop release) it may not, at this release's share of 0.
    #[test]
    fn may_carry_data_follows_the_ramp() {
        let ramp = Some(1_700_000_000);
        for (mode, percent, start, want) in [
            ("prefer", None, ramp, true),
            ("prefer", Some("0"), ramp, false),
            ("prefer", Some("5"), ramp, true),
            ("prefer", None, None, false),
            ("prefer", Some("0"), None, false),
            ("shadow", None, ramp, false),
            ("off", None, ramp, false),
        ] {
            let mut config = config(mode, None);
            config.quic_data_percent = percent.map(str::to_string);
            assert_eq!(
                may_carry_data_with(&config, start),
                want,
                "{mode} {percent:?} {start:?}"
            );
        }
        assert_eq!(
            QUIC_DATA_PERCENT, 0,
            "the emergency-stop case above reads false"
        );
    }

    /// The share reported as `quic_ramp_share_pct`: the policy's share in
    /// `prefer`, none in `shadow` or `off`.
    #[test]
    fn the_advertised_share_is_the_share_only_in_prefer() {
        for (mode, max_paths, want) in [
            ("prefer", None, Some(DataShare::Fixed(40))),
            ("prefer", Some("0"), None),
            ("shadow", None, None),
            ("off", None, None),
        ] {
            let mut config = config(mode, max_paths);
            config.quic_data_percent = Some("40".to_string());
            let up = QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
            let policy = QuicPolicy::new(&config, BreakerOutcome::NotConsulted, up);
            assert_eq!(policy.advertised_share(), want, "{mode} {max_paths:?}");
        }
        // The crash-loop breaker keeping QUIC off for this run.
        let mut config = config("prefer", None);
        config.quic_data_percent = Some("40".to_string());
        let up = QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let suppressed = QuicPolicy::new(&config, BreakerOutcome::Decided(Verdict::Trip), up);
        assert_eq!(suppressed.advertised_share(), None, "breaker");
    }

    /// The data bit needs `prefer`, the peer in the slice, and the key or the
    /// release's share to say so. `shadow` never sets it, whatever the key says,
    /// and at this release's share `prefer` does not either.
    #[test]
    fn own_caps_needs_prefer_and_the_peer_in_the_slice() {
        use crate::transport::TransportKeypair;
        let local = TransportKeypair::new().public().clone();
        let peer = TransportKeypair::new().public().clone();
        let bucket = crate::transport::quic::upgrade::slice::bucket(&local, &peer);
        let caps_with = |mode: &str, percent: Option<String>| {
            let mut config = config(mode, None);
            config.quic_data_percent = percent;
            let up = QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
            QuicPolicy::new(&config, BreakerOutcome::NotConsulted, up).own_caps(
                &local,
                &peer,
                std::time::SystemTime::now(),
            )
        };
        if QUIC_DATA_RAMP_START.is_none() {
            assert_eq!(caps_with("prefer", None), 0, "no ramp, no data");
        }
        // With no quic-data-percent the policy takes the release's ramp, and
        // own_caps asks with it, whether or not this build sets a start.
        let mut plain = config("prefer", None);
        plain.quic_data_percent = None;
        let up = QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let policy = QuicPolicy::new(&plain, BreakerOutcome::NotConsulted, up);
        assert_eq!(policy.share(), DataShare::of(None, QUIC_DATA_RAMP_START));
        let now = std::time::SystemTime::now();
        for peer in (0..200).map(key) {
            assert_eq!(
                policy.own_caps(&local, &peer, now),
                policy.own_caps_with(policy.share(), &local, &peer, now)
            );
        }
        // An unreadable quic-data-percent reads as unset: the release's share.
        assert_eq!(
            caps_with("prefer", Some("lots".to_string())),
            caps_with("prefer", None)
        );
        assert_eq!(
            caps_with("prefer", Some("100".to_string())),
            caps::DATA | caps::RECYCLES
        );
        assert_eq!(caps_with("prefer", Some("0".to_string())), 0);
        assert_eq!(
            caps_with("prefer", Some(bucket.to_string())) & caps::DATA,
            0,
            "a bucket equal to the percentage is out"
        );
        assert_eq!(
            caps_with("prefer", Some((bucket + 1).to_string())),
            caps::DATA | caps::RECYCLES,
            "the bucket below the percentage is in"
        );
        for mode in ["shadow", "off"] {
            assert_eq!(caps_with(mode, Some("100".to_string())), 0, "{mode}");
        }
        // RECYCLES says the node carries data with some pairs or will: any
        // share but one fixed at 0, in `prefer` only.
        assert_eq!(
            caps_with("prefer", Some("1".to_string())) & caps::RECYCLES,
            caps::RECYCLES
        );
        let ramp = DataShare::Ramp {
            start_unix_secs: 1_900_000_000,
        };
        let prefer = QuicPolicy::new(
            &config("prefer", None),
            BreakerOutcome::NotConsulted,
            QuicAvailabilityWatch::for_test(QuicAvailability::Up).1,
        );
        assert_eq!(
            prefer.own_caps_with(ramp, &local, &peer, std::time::UNIX_EPOCH) & caps::RECYCLES,
            caps::RECYCLES,
            "on a ramp, before the start"
        );
    }

    /// The receive side exists, so a build without `cfg(test)` or the `testing`
    /// feature may set the data bit. It still does so only through the mode and the
    /// share (the ramp, or `quic-data-percent`).
    #[test]
    fn a_build_with_the_receive_side_may_set_the_data_bit() {
        const { assert!(QUIC_DATA_RECEIVER_READY, "the receive side is in `recv`") };
        assert!(data_bit_allowed(false));
        assert!(data_bit_allowed(true));
        // Tests build with the testing cfg, so only the source can show that
        // `own_caps` consults the gate before anything else.
        let source = include_str!("policy.rs");
        let body = source
            .split("fn own_caps_with(")
            .nth(1)
            .and_then(|rest| rest.split("\n    }\n").next())
            .expect("own_caps_with exists");
        let gate = body
            .find("if !data_bit_allowed(cfg!(any(test, feature = \"testing\")))")
            .expect("own_caps consults the gate");
        let mode = body
            .find("match self.mode")
            .expect("own_caps reads the mode");
        assert!(gate < mode, "the gate comes first");
    }

    fn key(seed: u32) -> TransportPublicKey {
        TransportPublicKey::from_bytes(*blake3::hash(&seed.to_be_bytes()).as_bytes())
    }

    fn at(secs: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)
    }

    /// Only a ramp, in `prefer`, can turn a pair's bit on while the node runs,
    /// so only then is a path without the bit rechecked.
    #[test]
    fn only_a_ramp_in_prefer_can_turn_the_bit_on() {
        let up = || QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let ramp = DataShare::Ramp {
            start_unix_secs: 1_900_000_000,
        };
        let prefer = QuicPolicy::new(&config("prefer", None), BreakerOutcome::NotConsulted, up());
        let shadow = QuicPolicy::new(&config("shadow", None), BreakerOutcome::NotConsulted, up());
        assert!(prefer.bit_may_come_on_with(ramp));
        assert!(!prefer.bit_may_come_on_with(DataShare::Fixed(0)));
        assert!(!prefer.bit_may_come_on_with(DataShare::Fixed(50)));
        assert!(!shadow.bit_may_come_on_with(ramp));
    }

    /// With a ramp, a `prefer` node's bit follows the curve at the time it is
    /// asked, from the start it is given: none of a pair's peers before the share
    /// covers it, and every peer once it does. A time before the start reads as
    /// the start. `quic-data-percent` replaces the ramp, and shadow sets no bit.
    #[test]
    fn own_caps_follow_the_ramp_from_its_start() {
        let up = || QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let policy = |mode, percent: Option<&str>| {
            let mut config = config(mode, None);
            config.quic_data_percent = percent.map(str::to_string);
            QuicPolicy::new(&config, BreakerOutcome::NotConsulted, up())
        };
        let start = 1_900_000_000;
        let ramp = DataShare::Ramp {
            start_unix_secs: start,
        };
        let prefer = policy("prefer", None);
        let local = key(0);
        let peers: Vec<_> = (1..400).map(key).collect();
        let carrying = |secs| {
            peers
                .iter()
                .filter(|peer| prefer.own_caps_with(ramp, &local, peer, at(secs)) & caps::DATA != 0)
                .count()
        };
        let share = |n: usize| n * 100 / peers.len();
        assert!(
            (2..=9).contains(&share(carrying(start - 3600))),
            "before: the first value"
        );
        assert!((2..=9).contains(&share(carrying(start))));
        assert!((20..=31).contains(&share(carrying(start + 12 * 3600))));
        assert_eq!(carrying(start + 24 * 3600), peers.len(), "all at 24 h");
        assert_eq!(
            DataShare::of(Some(40), Some(start)),
            DataShare::Fixed(40),
            "quic-data-percent replaces the ramp"
        );
        assert_eq!(
            DataShare::of(None, None),
            DataShare::Fixed(QUIC_DATA_PERCENT),
            "no ramp: the release's fixed share"
        );
        let shadow = policy("shadow", None);
        assert_eq!(
            shadow.own_caps_with(ramp, &local, &peers[0], at(start + 30 * 3600)),
            0
        );
    }

    /// Two nodes whose clocks disagree each set their own bit, and a pair carries
    /// data only with both, so the pair starts when the slower clock lets it: skew
    /// delays a pair and never moves one out.
    #[test]
    fn clock_skew_between_two_nodes_only_delays_a_pair() {
        let start = 1_900_000_000u64;
        let ramp = DataShare::Ramp {
            start_unix_secs: start,
        };
        let skew = 2 * 3600;
        let pairs: Vec<_> = (0..300).map(|i| (key(2 * i), key(2 * i + 1))).collect();
        for (a, b) in &pairs {
            let mut was_on = false;
            for minute in (0..30 * 60).step_by(5) {
                let now = start + minute * 60;
                let fast = super::super::slice::in_slice(a, b, ramp.percent_at(at(now)));
                let slow = super::super::slice::in_slice(
                    a,
                    b,
                    ramp.percent_at(at(now.saturating_sub(skew))),
                );
                let on = fast && slow;
                assert_eq!(on, slow, "the slower clock decides");
                assert!(on || !was_on, "a pair never leaves");
                was_on = on;
            }
            assert!(was_on, "every pair is on once both clocks are past 24 h");
        }
    }

    /// The share is logged at INFO once per band as `own_caps` sees it move, with
    /// its own wording at 0%, so a log at 0% does not read as data being on.
    #[test]
    fn the_share_is_logged_once_per_band() {
        const ON: &str = "offers data to this share of its pairs";
        const NONE: &str = "offers data to none of its pairs";
        let up = QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let policy = QuicPolicy::new(&config("prefer", None), BreakerOutcome::NotConsulted, up);
        let (local, peer) = (key(0), key(1));
        let (logs, guard) = crate::util::test_log_capture::install();
        for percent in [0, 0, 5, 7, 37, 39, 100] {
            policy.own_caps_with(DataShare::Fixed(percent), &local, &peer, at(0));
        }
        drop(guard);
        let logs = logs.lock().unwrap().clone();
        let ours: Vec<_> = logs
            .iter()
            .filter(|line| line.contains(ON) || line.contains(NONE))
            .collect();
        assert_eq!(ours.len(), 4, "{logs:?}");
        assert!(
            ours[0].contains(NONE) && ours[0].starts_with("INFO"),
            "{}",
            ours[0]
        );
        for (line, percent) in ours[1..].iter().zip(["5", "37", "100"]) {
            assert!(line.contains(ON), "{line}");
            assert!(line.contains(&format!("data_percent={percent}")), "{line}");
        }
    }

    /// The bands an operator sees at INFO: 5, then every 10.
    #[test]
    fn the_log_bands_are_these() {
        for (percent, band) in [
            (0, 0),
            (5, 5),
            (9, 5),
            (10, 10),
            (15, 10),
            (25, 20),
            (99, 90),
            (100, 100),
        ] {
            assert_eq!(log_band(percent), band, "{percent}");
        }
    }

    /// Permits stop at the limit, a dropped permit frees its slot, and the
    /// count returns to zero when every permit is gone. Clones share it.
    #[test]
    fn permits_respect_the_limit_and_release_on_drop() {
        let permits = QuicPathPermits::new(2);
        let first = permits.try_acquire().expect("first");
        let second = permits.clone().try_acquire().expect("second, via a clone");
        assert!(permits.try_acquire().is_none(), "at the limit");
        assert_eq!(permits.in_use(), 2);
        drop(first);
        assert_eq!(permits.in_use(), 1);
        let third = permits.try_acquire().expect("a released slot is reusable");
        assert!(permits.try_acquire().is_none());
        drop((second, third));
        assert_eq!(permits.in_use(), 0);
        assert_eq!(permits.limit(), 2);
        assert!(QuicPathPermits::new(0).try_acquire().is_none());
    }

    /// An allowed policy at its limit refuses with `Busy`, and a limit of 0
    /// is reported as disabled, not busy.
    #[test]
    fn busy_at_the_limit_and_disabled_at_zero() {
        let up = QuicAvailability::Up;
        let policy = policy("shadow", BreakerOutcome::NotConsulted, up, 1);
        let held = policy.admit().expect("one slot");
        assert_eq!(policy.admit().unwrap_err(), PathRefusal::Busy);
        drop(held);
        assert!(policy.admit().is_ok());
        let none = self::policy("shadow", BreakerOutcome::NotConsulted, up, 0);
        assert_eq!(
            none.admit().unwrap_err(),
            PathRefusal::Disabled(DisabledBy::NoPaths)
        );
    }

    /// A `quic-data-percent` that cannot be read is reported with QUIC on, and
    /// says nothing with QUIC off, where it does nothing.
    #[test]
    fn startup_warns_about_an_unreadable_data_percent() {
        const INVALID: &str = "quic-data-percent is not a whole number";
        for (mode, value, warned) in [
            ("shadow", "101", true),
            ("prefer", "lots", true),
            ("off", "lots", false),
            ("shadow", "100", false),
        ] {
            let mut config = config(mode, None);
            config.quic_data_percent = Some(value.to_string());
            let (logs, guard) = crate::util::test_log_capture::install();
            log_startup(&config);
            drop(guard);
            let logs = logs.lock().unwrap().clone();
            let hits: Vec<_> = logs.iter().filter(|l| l.contains(INVALID)).collect();
            assert_eq!(hits.len(), usize::from(warned), "{mode} {value}: {logs:?}");
        }
    }

    /// Threads released together race for permits while each holds its
    /// permit across a yield. The count never passes the limit, reaches it
    /// (so the race really happened), and returns to zero.
    #[test]
    fn concurrent_acquires_never_exceed_the_limit() {
        const LIMIT: usize = 3;
        const THREADS: usize = 8;
        let permits = QuicPathPermits::new(LIMIT);
        let peak = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(std::sync::Barrier::new(THREADS));
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let permits = permits.clone();
                let peak = Arc::clone(&peak);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..5_000 {
                        if let Some(permit) = permits.try_acquire() {
                            peak.fetch_max(permits.in_use(), Ordering::AcqRel);
                            std::thread::yield_now();
                            drop(permit);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(peak.load(Ordering::Acquire), LIMIT);
        assert_eq!(permits.in_use(), 0);
    }

    /// Startup says what `prefer` negotiates (INFO), warns about a
    /// `quic-max-paths` it cannot parse, and says when it lowers one, the
    /// last two only with QUIC on; a plain configuration logs none of it.
    #[test]
    fn startup_reports_prefer_and_max_paths() {
        const PREFER: &str = "negotiates data with this share of pairs";
        const NO_SHARE: &str = "with no data share";
        let prefer = if QUIC_DATA_RAMP_START.is_none() {
            NO_SHARE
        } else {
            PREFER
        };
        const INVALID: &str = "quic-max-paths is not a whole number";
        const LOWERED: &str = "quic-max-paths is above the connection limit";
        let above = (config("off", None).max_connections + 1).to_string();
        for (mode, max_paths, expected) in [
            ("off", None, None),
            ("off", Some("lots"), None),
            ("off", Some(above.as_str()), None),
            ("shadow", Some("3"), None),
            ("prefer", None, Some(("INFO", prefer))),
            ("shadow", Some("lots"), Some(("WARN", INVALID))),
            ("shadow", Some(above.as_str()), Some(("INFO", LOWERED))),
        ] {
            let config = config(mode, max_paths);
            let (logs, guard) = crate::util::test_log_capture::install();
            log_startup(&config);
            drop(guard);
            let logs = logs.lock().unwrap().clone();
            let ours: Vec<_> = logs
                .iter()
                .filter(|line| {
                    [PREFER, NO_SHARE, INVALID, LOWERED]
                        .iter()
                        .any(|n| line.contains(n))
                })
                .collect();
            match expected {
                None => assert!(ours.is_empty(), "{mode} {max_paths:?}: {logs:?}"),
                Some((level, needle)) => {
                    assert_eq!(ours.len(), 1, "{mode} {max_paths:?}: {logs:?}");
                    assert!(ours[0].starts_with(level), "{}", ours[0]);
                    assert!(ours[0].contains(needle), "{}", ours[0]);
                }
            }
        }
    }
}
