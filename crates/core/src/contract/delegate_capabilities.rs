//! Node-enforced delegate capabilities: manifests, consent-once grants per app,
//! lifecycle events, and the budget that bounds unprompted delegate runs.
//!
//! # What this is for
//!
//! A delegate normally runs only when an open app sends it a message. A
//! delegate that must act with no tab open (a shop answering a buyer while the
//! seller is away) declares it in a manifest embedded in its WASM
//! (`#[delegate(manifest(lifecycle = [..], capabilities = [Background]))]`,
//! read with `DelegateManifest::from_wasm`). The node then:
//!
//! 1. asks the user ONCE, at registration, whether the registering app may run
//!    in the background (a node-authored "Freenet asks" prompt, not a
//!    delegate-authored one), and remembers the answer per app;
//! 2. delivers `LifecycleEvent::Installed` once per delegate per node, and
//!    `LifecycleEvent::NodeStarted` after each start, only to delegates whose
//!    manifest lists that kind AND whose app holds the grant;
//! 3. bounds what unprompted runs (lifecycle and contract-notification runs)
//!    may cost: loop time, and contract operations (GET/PUT/UPDATE/SUBSCRIBE,
//!    whether answered locally or from the network) for both.
//!
//! # App identity
//!
//! A grant belongs to an [`AppIdentity`], today always the web app's contract
//! instance id, attested by the connection that registered the delegate (a
//! loopback client holding a token for that app; non-local registrations carry
//! no app and get no grants). The instance id, not the verifying key in the
//! container's parameters, because the key alone is public: a different
//! container WASM carrying the same key as its parameter, but skipping the
//! signature check, would otherwise inherit the app's grants. The instance id
//! is `hash(container code, key)`, so it survives UI updates (state updates)
//! and delegate re-keys (the binding is made again at registration), and it
//! changes only on a container-WASM re-key, which also changes the app's URL.
//! The trade-off: that re-key re-prompts. The identity is a tagged enum so a
//! verified-key variant can be added later without a storage migration.
//!
//! The attestation is the same one the delegate's `MessageOrigin::WebApp`
//! and the registration-origin record already rest on, so a grant is exactly
//! as strong as that: whoever can present an app's token can bind a delegate
//! to that app and, if the app is granted, have it run in the background.
//! The node mints such tokens for any contract id to loopback clients
//! (#5264), so this is not a boundary against local processes, which can
//! already drive any delegate by holding a connection open. Keying grants per
//! delegate code instead would ask again on every delegate upgrade, which the
//! consent-once rule rules out.
//!
//! # Platform delegates
//!
//! One delegate may serve several apps. It keeps a set of up to
//! [`MAX_APPS_PER_DELEGATE`] bound apps, and background delivery is enabled if
//! ANY bound app holds the grant. There is no first-writer ownership, so a
//! later app cannot be locked out, and an app the user never approved gains
//! nothing: the grant it would need is its own.
//!
//! # Prompts
//!
//! Only for capabilities not yet granted to the registering app. "Allow" is
//! stored; "Not now" is stored as a denial with a [`DENIAL_COOL_OFF`]; no
//! answer (timeout) stores nothing, so the next registration asks again.
//! Revocation removes the grant.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use freenet_stdlib::prelude::{
    Capability, ContractInstanceId, DelegateKey, DelegateManifest, LifecycleEvent, LifecycleKind,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::util::time_source::{DynTimeSource, InstantTimeSrc, TimeSource};

/// Apps one delegate can be bound to. Above this a new binding is not recorded
/// (the delegate keeps working in the foreground for that app).
pub(crate) const MAX_APPS_PER_DELEGATE: usize = 8;

/// How long a "Not now" suppresses the prompt for that app.
pub(crate) const DENIAL_COOL_OFF: Duration = Duration::from_secs(7 * 24 * 3600);

/// Largest registered-parameters blob kept for lifecycle runs. A delegate with
/// larger parameters gets no lifecycle events (logged at registration).
pub(crate) const MAX_STORED_PARAMS_BYTES: usize = 64 * 1024;

/// Delegates with a capability record, node-wide. Bounds the table and the
/// node-start scan. At the cap an unprotected record is evicted (see
/// `make_room_for_record`); with none, new manifests are not recorded.
pub(crate) const MAX_CAPABILITY_RECORDS: usize = 1024;

/// Records one app may create. An app registering many throwaway delegates
/// fills its own quota, not the node's table.
pub(crate) const MAX_RECORDS_PER_APP: usize = 64;

/// Lifecycle runs waiting for the contract loop. Producers `try_send`; a full
/// queue drops the run with a counter (an `Installed` stays undelivered and is
/// retried at the next registration or grant, a `NodeStarted` is lost for this
/// start).
pub(crate) const LIFECYCLE_QUEUE_CAPACITY: usize = 256;

/// Stable wire code of each capability in the grant table.
fn capability_code(cap: Capability) -> Option<u16> {
    #[allow(clippy::wildcard_enum_match_arm)]
    match cap {
        Capability::Background => Some(1),
        // `Unknown` and anything a newer stdlib adds: not grantable here.
        _ => None,
    }
}

fn capability_from_code(code: u16) -> Option<Capability> {
    match code {
        1 => Some(Capability::Background),
        _ => None,
    }
}

/// Human wording of a capability in the node's own prompt.
pub(crate) fn capability_description(cap: Capability) -> &'static str {
    #[allow(clippy::wildcard_enum_match_arm)]
    match cap {
        Capability::Background => {
            "run when it is installed and each time Freenet starts, even with its tab closed"
        }
        _ => "use a capability this node does not know",
    }
}

/// Who a grant belongs to. See the module docs for why this is the web app's
/// contract instance id rather than its signing key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AppIdentity {
    /// A web app, by its container contract's instance id.
    WebApp(ContractInstanceId),
}

impl AppIdentity {
    const TAG_WEBAPP: u8 = 1;
    const ENCODED_LEN: usize = 33;

    fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        match self {
            AppIdentity::WebApp(id) => {
                out[0] = Self::TAG_WEBAPP;
                out[1..].copy_from_slice(id.as_bytes());
            }
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        match bytes[0] {
            Self::TAG_WEBAPP => {
                let id: [u8; 32] = bytes[1..].try_into().ok()?;
                Some(AppIdentity::WebApp(ContractInstanceId::new(id)))
            }
            _ => None,
        }
    }

    /// Display form for prompts and the dashboard.
    pub(crate) fn display(&self) -> String {
        match self {
            AppIdentity::WebApp(id) => id.to_string(),
        }
    }
}

fn grant_key(app: &AppIdentity, code: u16) -> [u8; 35] {
    let mut k = [0u8; 35];
    k[..33].copy_from_slice(&app.encode());
    k[33..].copy_from_slice(&code.to_be_bytes());
    k
}

/// The user's answer for one (app, capability).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Grant {
    Granted { at_ms: u64 },
    Denied { at_ms: u64, until_ms: u64 },
}

impl Grant {
    fn encode(&self) -> [u8; 17] {
        let mut out = [0u8; 17];
        match *self {
            Grant::Granted { at_ms } => {
                out[0] = 1;
                out[1..9].copy_from_slice(&at_ms.to_le_bytes());
            }
            Grant::Denied { at_ms, until_ms } => {
                out[0] = 2;
                out[1..9].copy_from_slice(&at_ms.to_le_bytes());
                out[9..].copy_from_slice(&until_ms.to_le_bytes());
            }
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 17 {
            return None;
        }
        let at_ms = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
        let until_ms = u64::from_le_bytes(bytes[9..].try_into().ok()?);
        match bytes[0] {
            1 => Some(Grant::Granted { at_ms }),
            2 => Some(Grant::Denied { at_ms, until_ms }),
            _ => None,
        }
    }
}

/// What the node keeps about one manifest-declaring delegate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DelegateRecord {
    pub manifest: DelegateManifest,
    /// The parameters the delegate was registered with, so lifecycle runs get
    /// them (a notification run gets empty parameters, #5616; these must not).
    pub params: Vec<u8>,
    pub apps: Vec<AppIdentity>,
    /// `Installed` has been delivered on this node.
    pub installed_delivered: bool,
}

/// On-disk form, version-prefixed.
#[derive(Serialize, Deserialize)]
struct DelegateRecordV1 {
    manifest_json: Vec<u8>,
    params: Vec<u8>,
    apps: Vec<Vec<u8>>,
    installed_delivered: bool,
}

const RECORD_V1: u8 = 1;

impl DelegateRecord {
    fn encode(&self) -> Vec<u8> {
        let v1 = DelegateRecordV1 {
            manifest_json: self.manifest.to_bytes(),
            params: self.params.clone(),
            apps: self.apps.iter().map(|a| a.encode().to_vec()).collect(),
            installed_delivered: self.installed_delivered,
        };
        let mut out = vec![RECORD_V1];
        // Infallible: plain byte vectors and a bool, no maps or custom
        // serializers that could refuse.
        out.extend(bincode::serialize(&v1).expect("record serializes"));
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let (&version, rest) = bytes.split_first()?;
        if version != RECORD_V1 {
            return None;
        }
        let v1: DelegateRecordV1 = bincode::deserialize(rest).ok()?;
        Some(Self {
            manifest: DelegateManifest::from_bytes(&v1.manifest_json).ok()?,
            params: v1.params,
            apps: v1
                .apps
                .iter()
                .filter_map(|a| AppIdentity::decode(a))
                .collect(),
            installed_delivered: v1.installed_delivered,
        })
    }
}

/// Raw persistence for records and grants. The ReDb implementation is the
/// production one; [`MemoryCapabilityStorage`] serves tests and builds without
/// redb (where grants then last only for the process).
pub(crate) trait CapabilityStorage: Send + Sync {
    fn put_record(&self, key: &DelegateKey, value: &[u8]) -> anyhow::Result<()>;
    fn get_record(&self, key: &DelegateKey) -> anyhow::Result<Option<Vec<u8>>>;
    fn remove_record(&self, key: &DelegateKey) -> anyhow::Result<()>;
    fn all_records(&self) -> anyhow::Result<Vec<(DelegateKey, Vec<u8>)>>;
    fn put_grant(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    fn get_grant(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
    fn remove_grant(&self, key: &[u8]) -> anyhow::Result<()>;
    fn all_grants(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

#[cfg(feature = "redb")]
impl CapabilityStorage for crate::contract::storages::redb::ReDb {
    fn put_record(&self, key: &DelegateKey, value: &[u8]) -> anyhow::Result<()> {
        Ok(self.put_delegate_capability_record(key, value)?)
    }
    fn get_record(&self, key: &DelegateKey) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.get_delegate_capability_record(key)?)
    }
    fn remove_record(&self, key: &DelegateKey) -> anyhow::Result<()> {
        Ok(self.remove_delegate_capability_record(key)?)
    }
    fn all_records(&self) -> anyhow::Result<Vec<(DelegateKey, Vec<u8>)>> {
        Ok(self.load_all_delegate_capability_records()?)
    }
    fn put_grant(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.put_app_capability_grant(key, value)?)
    }
    fn get_grant(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.get_app_capability_grant(key)?)
    }
    fn remove_grant(&self, key: &[u8]) -> anyhow::Result<()> {
        Ok(self.remove_app_capability_grant(key)?)
    }
    fn all_grants(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.load_all_app_capability_grants()?)
    }
}

/// In-process storage. See [`CapabilityStorage`].
#[cfg(any(test, not(feature = "redb")))]
#[derive(Default)]
pub(crate) struct MemoryCapabilityStorage {
    records: Mutex<HashMap<DelegateKey, Vec<u8>>>,
    grants: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[cfg(any(test, not(feature = "redb")))]
impl CapabilityStorage for MemoryCapabilityStorage {
    fn put_record(&self, key: &DelegateKey, value: &[u8]) -> anyhow::Result<()> {
        self.records.lock().insert(key.clone(), value.to_vec());
        Ok(())
    }
    fn get_record(&self, key: &DelegateKey) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.records.lock().get(key).cloned())
    }
    fn remove_record(&self, key: &DelegateKey) -> anyhow::Result<()> {
        self.records.lock().remove(key);
        Ok(())
    }
    fn all_records(&self) -> anyhow::Result<Vec<(DelegateKey, Vec<u8>)>> {
        Ok(self
            .records
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    fn put_grant(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.grants.lock().insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    fn get_grant(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.grants.lock().get(key).cloned())
    }
    fn remove_grant(&self, key: &[u8]) -> anyhow::Result<()> {
        self.grants.lock().remove(key);
        Ok(())
    }
    fn all_grants(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .grants
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

/// Read a delegate's manifest from its raw WASM module. `None` for no
/// manifest, and for an unreadable one (logged): a delegate whose manifest
/// cannot be read is treated as having asked for nothing.
pub(crate) fn read_manifest(key: &DelegateKey, code: &[u8]) -> Option<DelegateManifest> {
    match DelegateManifest::from_wasm(code) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(
                delegate = %key,
                error = %e,
                "Delegate manifest section is unreadable; treating the delegate as having none"
            );
            None
        }
    }
}

/// A lifecycle event queued for delivery on the contract loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LifecycleRun {
    pub key: DelegateKey,
    pub event: LifecycleEvent,
}

/// A node-authored prompt the caller must raise (from a spawned task, never
/// inline on the contract loop) and answer with
/// [`DelegateCapabilities::record_answer`] or
/// [`DelegateCapabilities::prompt_unanswered`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapabilityPrompt {
    pub app: AppIdentity,
    pub delegate: DelegateKey,
    pub capabilities: Vec<Capability>,
}

impl CapabilityPrompt {
    /// The node's own text for the card. Never includes delegate-supplied
    /// text: the card is labelled as the node's.
    pub(crate) fn message(&self) -> String {
        let wants: Vec<&str> = self
            .capabilities
            .iter()
            .map(|c| capability_description(*c))
            .collect();
        format!(
            "The Freenet app {} wants to: {}. Freenet will remember your answer; you can \
             change this later from the Freenet dashboard.",
            self.app.display(),
            wants.join("; ")
        )
    }

    pub(crate) const ALLOW_INDEX: usize = 0;

    pub(crate) fn labels() -> Vec<String> {
        vec!["Allow".to_string(), "Not now".to_string()]
    }
}

/// Why an unprompted operation was refused. Every refusal is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BudgetRefusal {
    /// The delegate's own per-minute network-operation allowance is spent.
    DelegateNetworkOps,
    /// The node-wide per-minute network-operation allowance is spent.
    NodeNetworkOps,
    /// Too many PUT/UPDATEs to one contract from this delegate this minute.
    ContractWrites,
}

impl BudgetRefusal {
    pub(crate) fn message(&self) -> &'static str {
        match self {
            BudgetRefusal::DelegateNetworkOps => {
                "refused: this delegate's budget for unprompted contract operations is spent; retry later"
            }
            BudgetRefusal::NodeNetworkOps => {
                "refused: this node's budget for unprompted delegate contract operations is spent; retry later"
            }
            BudgetRefusal::ContractWrites => {
                "refused: too many unprompted writes to this contract from this delegate; retry later"
            }
        }
    }
}

/// Limits for unprompted runs. Generous on purpose: a grant is consent, not an
/// exemption, but existing notification-driven apps must not regress.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BudgetLimits {
    /// Loop time a delegate may spend in lifecycle runs: refill per second.
    pub duty_refill_per_sec: Duration,
    /// ...and bucket size.
    pub duty_burst: Duration,
    /// Node-wide loop time for lifecycle runs: refill per second.
    pub node_duty_refill_per_sec: Duration,
    /// ...and bucket size.
    pub node_duty_burst: Duration,
    /// Network operations (GET/PUT/UPDATE/SUBSCRIBE) per delegate per minute.
    pub ops_per_delegate_per_min: u32,
    /// Same, node-wide.
    pub ops_per_node_per_min: u32,
    /// PUT/UPDATEs per (delegate, contract) per minute: the #5558
    /// self-notification loop bound.
    pub writes_per_contract_per_min: u32,
}

impl Default for BudgetLimits {
    fn default() -> Self {
        Self {
            // 1% of wall time per delegate, 10% per node, as #5614/the RFC propose.
            duty_refill_per_sec: Duration::from_millis(10),
            duty_burst: Duration::from_secs(10),
            node_duty_refill_per_sec: Duration::from_millis(100),
            node_duty_burst: Duration::from_secs(30),
            ops_per_delegate_per_min: 300,
            ops_per_node_per_min: 3000,
            writes_per_contract_per_min: 60,
        }
    }
}

/// A token bucket in microseconds. Refill keeps the fractional remainder
/// (#5614 review F3: a truncating refill loses it and under-fills).
#[derive(Debug, Clone)]
struct DutyBucket {
    /// Signed: a run that overspends leaves the bucket in debt, and the debt
    /// is paid back before the next run is admitted. Clipping at zero would
    /// forgive overdraft and let a delegate take far more than its share.
    tokens_us: i64,
    last: tokio::time::Instant,
    /// Carried sub-microsecond refill, in nanoseconds of elapsed time.
    carry_ns: u128,
}

impl DutyBucket {
    fn full(burst: Duration, now: tokio::time::Instant) -> Self {
        Self {
            tokens_us: burst.as_micros().min(i64::MAX as u128) as i64,
            last: now,
            carry_ns: 0,
        }
    }

    fn refill(&mut self, now: tokio::time::Instant, per_sec: Duration, burst: Duration) {
        let elapsed_ns = now.saturating_duration_since(self.last).as_nanos() + self.carry_ns;
        self.last = now;
        // tokens (us) = elapsed (s) * per_sec (us) = elapsed_ns * per_sec_us / 1e9
        let per_sec_us = per_sec.as_micros();
        let product = elapsed_ns * per_sec_us;
        let add = product / 1_000_000_000;
        self.carry_ns = if per_sec_us == 0 {
            0
        } else {
            (product % 1_000_000_000) / per_sec_us
        };
        let cap = burst.as_micros().min(i64::MAX as u128) as i64;
        let add = add.min(i64::MAX as u128) as i64;
        self.tokens_us = self.tokens_us.saturating_add(add).min(cap);
    }
}

/// A fixed one-minute window counter.
#[derive(Debug, Clone)]
struct MinuteWindow {
    start: tokio::time::Instant,
    count: u32,
}

impl MinuteWindow {
    fn new(now: tokio::time::Instant) -> Self {
        Self {
            start: now,
            count: 0,
        }
    }

    fn roll(&mut self, now: tokio::time::Instant) {
        if now.saturating_duration_since(self.start) >= Duration::from_secs(60) {
            self.start = now;
            self.count = 0;
        }
    }
}

/// Entries idle for this long are dropped from the per-delegate maps.
const BUDGET_ENTRY_IDLE_TTL: Duration = Duration::from_secs(600);

#[derive(Debug)]
struct Budget {
    limits: BudgetLimits,
    duty: HashMap<DelegateKey, DutyBucket>,
    node_duty: DutyBucket,
    ops: HashMap<DelegateKey, MinuteWindow>,
    node_ops: MinuteWindow,
    writes: HashMap<(DelegateKey, ContractInstanceId), MinuteWindow>,
    last_gc: tokio::time::Instant,
}

impl Budget {
    fn new(limits: BudgetLimits, now: tokio::time::Instant) -> Self {
        Self {
            limits,
            duty: HashMap::new(),
            node_duty: DutyBucket::full(limits.node_duty_burst, now),
            ops: HashMap::new(),
            node_ops: MinuteWindow::new(now),
            writes: HashMap::new(),
            last_gc: now,
        }
    }

    fn gc(&mut self, now: tokio::time::Instant) {
        if now.saturating_duration_since(self.last_gc) < BUDGET_ENTRY_IDLE_TTL {
            return;
        }
        self.last_gc = now;
        let limits = self.limits;
        // A full bucket is indistinguishable from a fresh one: drop it.
        self.duty.retain(|_, b| {
            b.refill(now, limits.duty_refill_per_sec, limits.duty_burst);
            (b.tokens_us as i128) < limits.duty_burst.as_micros() as i128
        });
        self.ops
            .retain(|_, w| now.saturating_duration_since(w.start) < BUDGET_ENTRY_IDLE_TTL);
        self.writes
            .retain(|_, w| now.saturating_duration_since(w.start) < BUDGET_ENTRY_IDLE_TTL);
    }

    fn duty_available(&mut self, key: &DelegateKey, now: tokio::time::Instant) -> bool {
        self.gc(now);
        let limits = self.limits;
        self.node_duty
            .refill(now, limits.node_duty_refill_per_sec, limits.node_duty_burst);
        let bucket = self
            .duty
            .entry(key.clone())
            .or_insert_with(|| DutyBucket::full(limits.duty_burst, now));
        bucket.refill(now, limits.duty_refill_per_sec, limits.duty_burst);
        bucket.tokens_us > 0 && self.node_duty.tokens_us > 0
    }

    /// `node_wide`: also charge the node bucket. Only lifecycle runs do; a
    /// notification run charges its delegate alone, so busy notification
    /// traffic cannot starve every delegate's lifecycle runs.
    fn charge_duty(
        &mut self,
        key: &DelegateKey,
        spent: Duration,
        node_wide: bool,
        now: tokio::time::Instant,
    ) {
        let limits = self.limits;
        let us = spent.as_micros().min(i64::MAX as u128) as i64;
        let bucket = self
            .duty
            .entry(key.clone())
            .or_insert_with(|| DutyBucket::full(limits.duty_burst, now));
        // Debt is floored at half a burst: enough that a long run is paid
        // back before the next lifecycle run, but repayable (at the default
        // limits, 500 s) inside the lifecycle retry window (60 x 15 s), so a
        // burst of notification traffic (charged but never refused) cannot
        // on its own guarantee the next lifecycle run is dropped. Pinned by
        // `debt_is_repaid_inside_the_retry_window`.
        let floor = -(limits.duty_burst.as_micros().min(i64::MAX as u128) as i64) / 2;
        bucket.tokens_us = bucket.tokens_us.saturating_sub(us).max(floor);
        if node_wide {
            let floor = -(limits.node_duty_burst.as_micros().min(i64::MAX as u128) as i64) / 2;
            self.node_duty.tokens_us = self.node_duty.tokens_us.saturating_sub(us).max(floor);
        }
    }

    fn admit_op(
        &mut self,
        key: &DelegateKey,
        write_to: Option<&ContractInstanceId>,
        now: tokio::time::Instant,
    ) -> Result<(), BudgetRefusal> {
        self.gc(now);
        let limits = self.limits;
        self.node_ops.roll(now);
        if self.node_ops.count >= limits.ops_per_node_per_min {
            return Err(BudgetRefusal::NodeNetworkOps);
        }
        let window = self
            .ops
            .entry(key.clone())
            .or_insert_with(|| MinuteWindow::new(now));
        window.roll(now);
        if window.count >= limits.ops_per_delegate_per_min {
            return Err(BudgetRefusal::DelegateNetworkOps);
        }
        if let Some(contract) = write_to {
            let w = self
                .writes
                .entry((key.clone(), *contract))
                .or_insert_with(|| MinuteWindow::new(now));
            w.roll(now);
            if w.count >= limits.writes_per_contract_per_min {
                return Err(BudgetRefusal::ContractWrites);
            }
            w.count += 1;
        }
        // Re-borrow: the entry above may have been created in this call.
        if let Some(window) = self.ops.get_mut(key) {
            window.count += 1;
        }
        self.node_ops.count += 1;
        Ok(())
    }
}

/// Counters exported for observability. Every refusal is counted.
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct CapabilityStats {
    pub lifecycle_delivered: AtomicU64,
    pub lifecycle_failed: AtomicU64,
    pub lifecycle_queue_full: AtomicU64,
    pub lifecycle_deferred_duty: AtomicU64,
    pub lifecycle_deferred_parked: AtomicU64,
    pub lifecycle_dropped_attempts: AtomicU64,
    pub lifecycle_deduplicated: AtomicU64,
    pub lifecycle_skipped_missing: AtomicU64,
    pub lifecycle_dropped_not_granted: AtomicU64,
    pub refused_delegate_ops: AtomicU64,
    pub refused_node_ops: AtomicU64,
    pub refused_contract_writes: AtomicU64,
    pub forged_lifecycle_refused: AtomicU64,
}

/// See `DelegateCapabilities::recorded`.
#[derive(Default)]
struct Recorded {
    apps: HashMap<DelegateKey, Vec<AppIdentity>>,
    /// `false` if loading the table at start failed: then storage, not this
    /// mirror, is asked (fail closed rather than treat every delegate as
    /// unrecorded).
    complete: bool,
}

impl Recorded {
    fn records_of(&self, app: &AppIdentity) -> usize {
        self.apps.values().filter(|apps| apps.contains(app)).count()
    }
}

/// Per-node capability state. Held by the executor (and reachable from the
/// `OpManager` for the HTTP server); never a process global, because
/// simulation tests run many nodes in one process.
pub(crate) struct DelegateCapabilities {
    storage: Arc<dyn CapabilityStorage>,
    time: DynTimeSource,
    lifecycle_tx: mpsc::Sender<LifecycleRun>,
    lifecycle_rx: Mutex<Option<mpsc::Receiver<LifecycleRun>>>,
    prompts_in_flight: Mutex<HashSet<AppIdentity>>,
    budget: Mutex<Budget>,
    /// Keys with a record and their bound apps, mirrored in memory: it decides
    /// which delegates the unprompted-run budget applies to (checked on every
    /// notification run) and counts records against the caps without a table
    /// scan. Updated only after the storage write it mirrors succeeded.
    recorded: Mutex<Recorded>,
    pub(crate) stats: CapabilityStats,
}

impl std::fmt::Debug for DelegateCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegateCapabilities")
            .finish_non_exhaustive()
    }
}

impl DelegateCapabilities {
    pub(crate) fn new(storage: Arc<dyn CapabilityStorage>) -> Arc<Self> {
        Self::with_time_source(
            storage,
            Arc::new(InstantTimeSrc::new()),
            BudgetLimits::default(),
        )
    }

    pub(crate) fn with_time_source(
        storage: Arc<dyn CapabilityStorage>,
        time: DynTimeSource,
        limits: BudgetLimits,
    ) -> Arc<Self> {
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel(LIFECYCLE_QUEUE_CAPACITY);
        let now = time.now();
        let recorded = match storage.all_records() {
            Ok(records) => Recorded {
                apps: records
                    .into_iter()
                    .map(|(k, bytes)| {
                        let apps = DelegateRecord::decode(&bytes)
                            .map(|r| r.apps)
                            .unwrap_or_default();
                        (k, apps)
                    })
                    .collect(),
                complete: true,
            },
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load delegate capability records");
                Recorded::default()
            }
        };
        Arc::new(Self {
            recorded: Mutex::new(recorded),
            storage,
            time,
            lifecycle_tx,
            lifecycle_rx: Mutex::new(Some(lifecycle_rx)),
            prompts_in_flight: Mutex::new(HashSet::new()),
            budget: Mutex::new(Budget::new(limits, now)),
            stats: CapabilityStats::default(),
        })
    }

    /// In-memory, for tests.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Arc<Self> {
        Self::new(Arc::new(MemoryCapabilityStorage::default()))
    }

    /// The receiving end, taken once by the contract loop.
    pub(crate) fn take_lifecycle_rx(&self) -> Option<mpsc::Receiver<LifecycleRun>> {
        self.lifecycle_rx.lock().take()
    }

    pub(crate) fn now(&self) -> tokio::time::Instant {
        self.time.now()
    }

    fn now_ms(&self) -> u64 {
        self.time
            .system_time_now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn load_record(&self, key: &DelegateKey) -> Option<DelegateRecord> {
        match self.storage.get_record(key) {
            Ok(Some(bytes)) => {
                let rec = DelegateRecord::decode(&bytes);
                if rec.is_none() {
                    tracing::warn!(delegate = %key, "Unreadable delegate capability record; ignoring it");
                }
                rec
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(delegate = %key, error = %e, "Failed to read delegate capability record");
                None
            }
        }
    }

    fn store_record(&self, key: &DelegateKey, rec: &DelegateRecord) -> bool {
        match self.storage.put_record(key, &rec.encode()) {
            Ok(()) => {
                self.recorded
                    .lock()
                    .apps
                    .insert(key.clone(), rec.apps.clone());
                true
            }
            Err(e) => {
                tracing::warn!(delegate = %key, error = %e, "Failed to store delegate capability record");
                false
            }
        }
    }

    /// The stored answer for (app, cap), if any.
    pub(crate) fn grant(&self, app: &AppIdentity, cap: Capability) -> Option<Grant> {
        let code = capability_code(cap)?;
        match self.storage.get_grant(&grant_key(app, code)) {
            Ok(Some(bytes)) => Grant::decode(&bytes),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to read capability grant; treating as not granted");
                None
            }
        }
    }

    fn is_granted(&self, app: &AppIdentity, cap: Capability) -> bool {
        matches!(self.grant(app, cap), Some(Grant::Granted { .. }))
    }

    /// Whether `app` declined every capability in `wanted` and is still in the
    /// cool-off.
    fn declined(&self, app: &AppIdentity, wanted: &[Capability]) -> bool {
        let now_ms = self.now_ms();
        !wanted.is_empty()
            && wanted.iter().all(|cap| {
                matches!(self.grant(app, *cap), Some(Grant::Denied { until_ms, .. }) if now_ms < until_ms)
            })
    }

    /// Whether any app bound to this delegate holds `cap`.
    fn any_bound_app_granted(&self, rec: &DelegateRecord, cap: Capability) -> bool {
        rec.apps.iter().any(|app| self.is_granted(app, cap))
    }

    /// Called after a delegate registration SUCCEEDED. `code` is the raw WASM
    /// module, `app` the attested registering app (`None` for a non-local or
    /// tokenless registration, which records the manifest but binds no app
    /// and prompts nobody).
    ///
    /// Returns the prompt to raise, if the app lacks a capability the manifest
    /// asks for and has not declined it recently. Queues `Installed` when it
    /// is due.
    #[cfg(test)]
    pub(crate) fn on_registered(
        &self,
        key: &DelegateKey,
        code: &[u8],
        params: &[u8],
        app: Option<AppIdentity>,
    ) -> Option<CapabilityPrompt> {
        let manifest = read_manifest(key, code)?;
        self.on_registered_manifest(key, manifest, params, app)
    }

    /// [`Self::on_registered`] with the manifest already read.
    pub(crate) fn on_registered_manifest(
        &self,
        key: &DelegateKey,
        manifest: DelegateManifest,
        params: &[u8],
        app: Option<AppIdentity>,
    ) -> Option<CapabilityPrompt> {
        let wants_lifecycle = [LifecycleKind::Installed, LifecycleKind::NodeStarted]
            .iter()
            .any(|k| manifest.wants_lifecycle(*k));
        // Background is the only capability today and only lifecycle events
        // use it, so a manifest without lifecycle kinds has nothing to ask
        // for; one with lifecycle kinds needs Background whatever it lists.
        if !wants_lifecycle {
            return None;
        }
        let wanted = Self::relevant_capabilities(&manifest);
        if params.len() > MAX_STORED_PARAMS_BYTES {
            tracing::warn!(
                delegate = %key,
                params_bytes = params.len(),
                max = MAX_STORED_PARAMS_BYTES,
                "Delegate parameters too large to keep for lifecycle runs; it will get none"
            );
            return None;
        }

        let existing = self.load_record(key);
        let is_new = existing.is_none();
        let mut rec = match existing {
            Some(rec) => rec,
            // An unattested registration can never be delivered to (it binds
            // no app), so it gets no record: a record is created only by an
            // app's registration.
            None if app.is_none() => return None,
            None if app
                .is_some_and(|a| self.recorded.lock().records_of(&a) >= MAX_RECORDS_PER_APP) =>
            {
                tracing::warn!(
                    delegate = %key,
                    max = MAX_RECORDS_PER_APP,
                    "This app already has the maximum number of background-capable delegates; not recording another"
                );
                return None;
            }
            // Declined recently: nothing would be delivered and nobody would
            // be asked, so there is nothing to record.
            None if app.is_some_and(|a| self.declined(&a, &wanted)) => return None,
            None => {
                if !self.make_room_for_record() {
                    tracing::warn!(
                        delegate = %key,
                        max = MAX_CAPABILITY_RECORDS,
                        "Too many manifest-declaring delegates on this node; not recording this one"
                    );
                    return None;
                }
                DelegateRecord {
                    manifest: manifest.clone(),
                    params: params.to_vec(),
                    apps: Vec::new(),
                    installed_delivered: false,
                }
            }
        };
        let mut changed = rec.manifest != manifest || rec.params != params;
        rec.manifest = manifest;
        rec.params = params.to_vec();
        if let Some(app) = app
            && !rec.apps.contains(&app)
        {
            if rec.apps.len() >= MAX_APPS_PER_DELEGATE {
                // Full: drop an unprotected bound app (no relevant grant, no
                // prompt open). Refusing instead would let anyone who can bind
                // eight apps lock the real one out for good.
                if let Some(pos) = rec
                    .apps
                    .iter()
                    .position(|a| !self.app_is_protected(&rec.manifest, a))
                {
                    rec.apps.remove(pos);
                }
            }
            if rec.apps.len() < MAX_APPS_PER_DELEGATE {
                rec.apps.push(app);
                changed = true;
            } else {
                tracing::warn!(
                    delegate = %key,
                    app = %app.display(),
                    max = MAX_APPS_PER_DELEGATE,
                    "Delegate is bound to the maximum number of apps, all protected; not binding another"
                );
            }
        }
        if (changed || is_new) && !self.store_record(key, &rec) {
            return None;
        }

        let prompt = app.filter(|a| rec.apps.contains(a)).and_then(|app| {
            let now_ms = self.now_ms();
            let missing: Vec<Capability> = wanted
                .iter()
                .copied()
                .filter(|cap| match self.grant(&app, *cap) {
                    Some(Grant::Granted { .. }) => false,
                    Some(Grant::Denied { until_ms, .. }) => now_ms >= until_ms,
                    None => true,
                })
                .collect();
            if missing.is_empty() || !self.prompts_in_flight.lock().insert(app) {
                return None;
            }
            Some(CapabilityPrompt {
                app,
                delegate: key.clone(),
                capabilities: missing,
            })
        });

        self.maybe_queue_installed(key, &rec);
        prompt
    }

    /// Capabilities that make an app's binding worth keeping: what the
    /// manifest asks for, plus Background if it lists any lifecycle kind
    /// (delivery requires Background whatever the manifest says).
    fn relevant_capabilities(manifest: &DelegateManifest) -> Vec<Capability> {
        let mut caps = manifest.known_capabilities();
        let wants_lifecycle = [LifecycleKind::Installed, LifecycleKind::NodeStarted]
            .iter()
            .any(|k| manifest.wants_lifecycle(*k));
        if wants_lifecycle && !caps.contains(&Capability::Background) {
            caps.push(Capability::Background);
        }
        caps
    }

    /// Whether `app`'s binding to a delegate with `manifest` must not be
    /// evicted: it holds a relevant grant, or the user is being asked right
    /// now (evicting it then would throw the answer away).
    fn app_is_protected(&self, manifest: &DelegateManifest, app: &AppIdentity) -> bool {
        self.prompts_in_flight.lock().contains(app)
            || Self::relevant_capabilities(manifest)
                .iter()
                .any(|cap| self.is_granted(app, *cap))
    }

    /// Keep the record table under [`MAX_CAPABILITY_RECORDS`]. At the cap, drop
    /// one record none of whose apps is protected (holds a relevant grant or
    /// has a prompt open), so registrations with junk manifests cannot crowd
    /// out a delegate the user approved or is approving. `false` when every
    /// record is protected.
    fn make_room_for_record(&self) -> bool {
        {
            let recorded = self.recorded.lock();
            if recorded.complete && recorded.apps.len() < MAX_CAPABILITY_RECORDS {
                return true;
            }
        }
        let records = match self.storage.all_records() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to count delegate capability records");
                return false;
            }
        };
        if records.len() < MAX_CAPABILITY_RECORDS {
            return true;
        }
        let victim = records.into_iter().find_map(|(key, bytes)| {
            let protected = DelegateRecord::decode(&bytes).is_some_and(|rec| {
                rec.apps
                    .iter()
                    .any(|app| self.app_is_protected(&rec.manifest, app))
            });
            (!protected).then_some(key)
        });
        match victim {
            Some(key) => self.remove_record(&key),
            None => false,
        }
    }

    /// Delete a record and, only once that succeeded, its mirror entry.
    fn remove_record(&self, key: &DelegateKey) -> bool {
        match self.storage.remove_record(key) {
            Ok(()) => {
                self.recorded.lock().apps.remove(key);
                true
            }
            Err(e) => {
                tracing::warn!(delegate = %key, error = %e, "Failed to remove delegate capability record");
                false
            }
        }
    }

    /// A local app unregistered the delegate. Only that app's binding goes;
    /// the record goes once no app is bound, which also clears its Installed
    /// flag (a later registration is a reinstall). An unregister from a
    /// connection that is not a bound app (another app, the CLI) changes
    /// nothing here: it must not be able to reset another app's delegate.
    /// Grants belong to apps and stay.
    pub(crate) fn on_unregistered(&self, key: &DelegateKey, app: Option<AppIdentity>) {
        let Some(app) = app else {
            return;
        };
        let Some(mut rec) = self.load_record(key) else {
            return;
        };
        if !rec.apps.contains(&app) {
            return;
        }
        rec.apps.retain(|a| *a != app);
        if rec.apps.is_empty() {
            self.remove_record(key);
        } else {
            self.store_record(key, &rec);
        }
    }

    fn maybe_queue_installed(&self, key: &DelegateKey, rec: &DelegateRecord) {
        if rec.installed_delivered
            || !rec.manifest.wants_lifecycle(LifecycleKind::Installed)
            || !self.any_bound_app_granted(rec, Capability::Background)
        {
            return;
        }
        self.queue(LifecycleRun {
            key: key.clone(),
            event: LifecycleEvent::Installed,
        });
    }

    /// Non-blocking enqueue; a full queue is counted and logged.
    pub(crate) fn queue(&self, run: LifecycleRun) -> bool {
        match self.lifecycle_tx.try_send(run) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(run)) => {
                self.stats
                    .lifecycle_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    delegate = %run.key,
                    event = ?run.event,
                    "Lifecycle queue full; dropping this lifecycle event"
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Store the user's answer to a [`CapabilityPrompt`] and, on Allow, queue
    /// `Installed` for every delegate of that app that is still waiting for it.
    pub(crate) fn record_answer(&self, prompt: &CapabilityPrompt, allowed: bool) {
        let now_ms = self.now_ms();
        for cap in &prompt.capabilities {
            let Some(code) = capability_code(*cap) else {
                continue;
            };
            let grant = if allowed {
                Grant::Granted { at_ms: now_ms }
            } else {
                Grant::Denied {
                    at_ms: now_ms,
                    until_ms: now_ms.saturating_add(DENIAL_COOL_OFF.as_millis() as u64),
                }
            };
            if let Err(e) = self
                .storage
                .put_grant(&grant_key(&prompt.app, code), &grant.encode())
            {
                tracing::warn!(error = %e, "Failed to store capability grant");
            }
        }
        self.prompts_in_flight.lock().remove(&prompt.app);
        if allowed {
            self.queue_installed_for_app(&prompt.app);
        }
    }

    /// The prompt went unanswered (timeout, no tab): store nothing, so the
    /// next registration asks again.
    pub(crate) fn prompt_unanswered(&self, prompt: &CapabilityPrompt) {
        self.prompts_in_flight.lock().remove(&prompt.app);
    }

    fn queue_installed_for_app(&self, app: &AppIdentity) {
        let records = match self.storage.all_records() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list delegate capability records");
                return;
            }
        };
        for (key, bytes) in records {
            if let Some(rec) = DelegateRecord::decode(&bytes)
                && rec.apps.contains(app)
            {
                self.maybe_queue_installed(&key, &rec);
            }
        }
    }

    /// Revoke a grant (dashboard). Lifecycle events stop at once (delivery
    /// re-checks the grant); contract-notification runs are not gated by the
    /// grant and continue. The next registration by that app prompts again.
    pub(crate) fn revoke(&self, app: &AppIdentity, cap: Capability) -> bool {
        let Some(code) = capability_code(cap) else {
            return false;
        };
        self.storage.remove_grant(&grant_key(app, code)).is_ok()
    }

    /// Every stored answer, for the dashboard.
    pub(crate) fn grants(&self) -> anyhow::Result<Vec<(AppIdentity, Capability, Grant)>> {
        Ok(self
            .storage
            .all_grants()?
            .into_iter()
            .filter_map(|(k, v)| {
                if k.len() != 35 {
                    return None;
                }
                let app = AppIdentity::decode(&k[..33])?;
                let cap = capability_from_code(u16::from_be_bytes([k[33], k[34]]))?;
                Some((app, cap, Grant::decode(&v)?))
            })
            .collect())
    }

    /// The parameters to run `key` with for an event of `kind`, or `None` if
    /// the event must not be delivered: no record, the manifest does not list
    /// the kind, or no bound app holds the Background grant. Checked again at
    /// delivery, so a revocation between queueing and running takes effect.
    pub(crate) fn delivery_params(
        &self,
        key: &DelegateKey,
        kind: LifecycleKind,
    ) -> Option<Vec<u8>> {
        let rec = self.load_record(key)?;
        if !rec.manifest.wants_lifecycle(kind)
            || !self.any_bound_app_granted(&rec, Capability::Background)
        {
            return None;
        }
        if kind == LifecycleKind::Installed && rec.installed_delivered {
            return None;
        }
        Some(rec.params)
    }

    /// Record that `Installed` was handed to the delegate. Set BEFORE the run,
    /// so a delegate that crashes the node in `Installed` does not get it
    /// again on every start.
    pub(crate) fn mark_installed_delivered(&self, key: &DelegateKey) {
        if let Some(mut rec) = self.load_record(key) {
            rec.installed_delivered = true;
            self.store_record(key, &rec);
        }
    }

    /// Delegates that get `NodeStarted` on this start.
    pub(crate) fn node_started_targets(&self) -> Vec<DelegateKey> {
        self.start_targets(LifecycleKind::NodeStarted)
    }

    /// Granted delegates still owed `Installed` (its earlier delivery was
    /// dropped: queue full, or deferred past the retry limit). Re-queued at
    /// start so a delegate does not get NodeStarted without ever having got
    /// Installed.
    pub(crate) fn installed_pending_targets(&self) -> Vec<DelegateKey> {
        self.start_targets(LifecycleKind::Installed)
    }

    fn start_targets(&self, kind: LifecycleKind) -> Vec<DelegateKey> {
        let records = match self.storage.all_records() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list delegate capability records at start");
                return Vec::new();
            }
        };
        records
            .into_iter()
            .filter_map(|(key, bytes)| {
                let rec = DelegateRecord::decode(&bytes)?;
                let owed = kind != LifecycleKind::Installed || !rec.installed_delivered;
                (owed
                    && rec.manifest.wants_lifecycle(kind)
                    && self.any_bound_app_granted(&rec, Capability::Background))
                .then_some(key)
            })
            .collect()
    }

    /// Whether `key`'s unprompted runs are budgeted: only delegates that took
    /// up the capability system, meaning a manifest registered by an app that
    /// holds the Background grant. Every other delegate's notification runs,
    /// including an ungranted manifest delegate's, behave exactly as before
    /// this module existed, so delegates nobody approved cannot spend the
    /// node-wide allowance that approved ones rely on.
    pub(crate) fn is_budgeted(&self, key: &DelegateKey) -> bool {
        let apps = {
            let recorded = self.recorded.lock();
            match recorded.apps.get(key) {
                Some(apps) => apps.clone(),
                None if recorded.complete => return false,
                None => match self.load_record(key) {
                    Some(rec) => rec.apps,
                    None => return false,
                },
            }
        };
        apps.iter()
            .any(|app| self.is_granted(app, Capability::Background))
    }

    /// Whether `key` has a capability record.
    #[cfg(test)]
    pub(crate) fn is_recorded(&self, key: &DelegateKey) -> bool {
        self.recorded.lock().apps.contains_key(key)
    }

    /// The delegate this record is for is no longer registered on this node:
    /// drop the record. A later registration is a new install.
    pub(crate) fn on_delegate_missing(&self, key: &DelegateKey) {
        self.remove_record(key);
    }

    /// Whether a lifecycle run for `key` may start now (duty budget).
    pub(crate) fn duty_available(&self, key: &DelegateKey) -> bool {
        let now = self.time.now();
        self.budget.lock().duty_available(key, now)
    }

    /// Charge time spent on the loop in an unprompted run (wall time between
    /// entering and leaving the run on the loop, including work it awaited
    /// there). `node_wide` for lifecycle runs only; see `Budget::charge_duty`.
    pub(crate) fn charge_duty(&self, key: &DelegateKey, spent: Duration, node_wide: bool) {
        let now = self.time.now();
        self.budget.lock().charge_duty(key, spent, node_wide, now);
    }

    /// Admission for one contract operation from an unprompted run. `write_to`
    /// is the target contract for a PUT/UPDATE.
    pub(crate) fn admit_op(
        &self,
        key: &DelegateKey,
        write_to: Option<&ContractInstanceId>,
    ) -> Result<(), BudgetRefusal> {
        let now = self.time.now();
        let result = self.budget.lock().admit_op(key, write_to, now);
        if let Err(refusal) = result {
            let counter = match refusal {
                BudgetRefusal::DelegateNetworkOps => &self.stats.refused_delegate_ops,
                BudgetRefusal::NodeNetworkOps => &self.stats.refused_node_ops,
                BudgetRefusal::ContractWrites => &self.stats.refused_contract_writes,
            };
            let n = counter.fetch_add(1, Ordering::Relaxed);
            // First refusal and every 100th after, so a runaway is visible
            // without flooding the log.
            if n % 100 == 0 {
                tracing::warn!(
                    delegate = %key,
                    ?refusal,
                    total = n + 1,
                    "Refused a contract operation from an unprompted delegate run"
                );
            }
        }
        result
    }
}

/// How long NodeStarted runs are spread over after a start, so a node with
/// many background delegates does not run them all at once while it is also
/// answering its first client requests.
pub(crate) const NODE_STARTED_SMEAR: Duration = Duration::from_secs(60);

/// Minimum delay before the first NodeStarted run: lets the node finish its
/// own start-up work (connections, restored subscriptions) first.
pub(crate) const NODE_STARTED_MIN_DELAY: Duration = Duration::from_secs(5);

/// Retry delay for a lifecycle run that could not start (delegate parked, or
/// its duty budget spent).
pub(crate) const LIFECYCLE_RETRY_DELAY: Duration = Duration::from_secs(15);

/// Attempts before a deferred lifecycle run is dropped (about 15 minutes).
pub(crate) const LIFECYCLE_MAX_ATTEMPTS: u32 = 60;

/// Lifecycle runs started per loop iteration, so a burst cannot starve client
/// work (each is a full delegate run).
pub(crate) const MAX_LIFECYCLE_RUNS_PER_ITERATION: usize = 1;

/// Lifecycle runs waiting on the loop, ordered by due time. Owned by the
/// contract loop.
/// A run for a `(delegate, kind)` already waiting is not added again, so the
/// schedule holds at most one run per delegate per kind: bounded by twice the
/// record cap.
#[derive(Default)]
pub(crate) struct LifecycleSchedule {
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(tokio::time::Instant, u64)>>,
    runs: HashMap<u64, (LifecycleRun, u32)>,
    pending: HashMap<(DelegateKey, LifecycleKind), u64>,
    seq: u64,
}

impl LifecycleSchedule {
    /// `false` if a run of the same kind for the same delegate is already
    /// waiting (the new one is dropped).
    pub(crate) fn push(
        &mut self,
        due: tokio::time::Instant,
        run: LifecycleRun,
        attempts: u32,
    ) -> bool {
        if let Some(seq) = self.pending.get(&(run.key.clone(), run.event.kind())) {
            // Keep the waiting run, but a fresh request restarts its attempt
            // count, so a run about to give up is not dropped just after a
            // new reason to deliver it arrived.
            if let Some((_, waiting)) = self.runs.get_mut(seq) {
                *waiting = (*waiting).min(attempts);
            }
            return false;
        }
        self.seq += 1;
        self.pending
            .insert((run.key.clone(), run.event.kind()), self.seq);
        self.heap.push(std::cmp::Reverse((due, self.seq)));
        self.runs.insert(self.seq, (run, attempts));
        true
    }

    /// The next run due at or before `now`, with its attempt count.
    pub(crate) fn pop_due(&mut self, now: tokio::time::Instant) -> Option<(LifecycleRun, u32)> {
        let std::cmp::Reverse((due, _)) = self.heap.peek()?;
        if *due > now {
            return None;
        }
        let std::cmp::Reverse((_, seq)) = self.heap.pop()?;
        let (run, attempts) = self.runs.remove(&seq)?;
        self.pending.remove(&(run.key.clone(), run.event.kind()));
        Some((run, attempts))
    }

    pub(crate) fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.heap.peek().map(|std::cmp::Reverse((due, _))| *due)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.runs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::CodeHash;

    fn key(n: u8) -> DelegateKey {
        DelegateKey::new([n; 32], CodeHash::new([n; 32]))
    }

    fn app(n: u8) -> AppIdentity {
        AppIdentity::WebApp(ContractInstanceId::new([n; 32]))
    }

    /// A WASM module holding only a manifest section.
    pub(crate) fn wasm_with_manifest(manifest: &DelegateManifest) -> Vec<u8> {
        let payload = manifest.to_bytes();
        let name = freenet_stdlib::prelude::MANIFEST_SECTION_NAME.as_bytes();
        let mut body = vec![name.len() as u8];
        body.extend_from_slice(name);
        body.extend_from_slice(&payload);
        let mut m = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x00];
        let mut len = body.len() as u32;
        loop {
            let mut b = (len & 0x7f) as u8;
            len >>= 7;
            if len != 0 {
                b |= 0x80;
            }
            m.push(b);
            if len == 0 {
                break;
            }
        }
        m.extend(body);
        m
    }

    fn background_manifest() -> DelegateManifest {
        DelegateManifest::new(
            vec![LifecycleKind::Installed, LifecycleKind::NodeStarted],
            vec![Capability::Background],
        )
    }

    fn caps() -> (Arc<DelegateCapabilities>, SharedMockTimeSource) {
        let time = SharedMockTimeSource::new();
        (
            DelegateCapabilities::with_time_source(
                Arc::new(MemoryCapabilityStorage::default()),
                Arc::new(time.clone()),
                BudgetLimits::default(),
            ),
            time,
        )
    }

    fn drain(rx: &mut mpsc::Receiver<LifecycleRun>) -> Vec<LifecycleRun> {
        let mut out = Vec::new();
        while let Ok(r) = rx.try_recv() {
            out.push(r);
        }
        out
    }

    #[test]
    fn encodings_round_trip() {
        let a = app(3);
        assert_eq!(AppIdentity::decode(&a.encode()), Some(a));
        assert_eq!(AppIdentity::decode(&[9u8; 33]), None);
        for g in [
            Grant::Granted { at_ms: 5 },
            Grant::Denied {
                at_ms: 5,
                until_ms: 99,
            },
        ] {
            assert_eq!(Grant::decode(&g.encode()), Some(g));
        }
        let rec = DelegateRecord {
            manifest: background_manifest(),
            params: vec![1, 2, 3],
            apps: vec![app(1), app(2)],
            installed_delivered: true,
        };
        assert_eq!(DelegateRecord::decode(&rec.encode()), Some(rec));
    }

    #[test]
    fn no_manifest_records_nothing_and_prompts_nobody() {
        let (c, _) = caps();
        let mut rx = c.take_lifecycle_rx().unwrap();
        let bare = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        assert_eq!(c.on_registered(&key(1), &bare, &[], Some(app(1))), None);
        assert!(c.storage.all_records().unwrap().is_empty());
        assert!(drain(&mut rx).is_empty());
    }

    /// Consent once: the first registration prompts, Allow is remembered, a
    /// re-registration (same app, or a re-keyed delegate of the same app)
    /// never prompts again, and Installed is queued exactly once.
    #[test]
    fn consent_is_asked_once_and_remembered() {
        let (c, _) = caps();
        let mut rx = c.take_lifecycle_rx().unwrap();
        let wasm = wasm_with_manifest(&background_manifest());

        let prompt = c
            .on_registered(&key(1), &wasm, b"p", Some(app(1)))
            .expect("first registration asks");
        assert_eq!(prompt.capabilities, vec![Capability::Background]);
        assert!(drain(&mut rx).is_empty(), "no Installed before the grant");

        // A second registration while the prompt is open does not stack a card.
        assert_eq!(c.on_registered(&key(1), &wasm, b"p", Some(app(1))), None);

        c.record_answer(&prompt, true);
        let runs = drain(&mut rx);
        assert_eq!(
            runs,
            vec![LifecycleRun {
                key: key(1),
                event: LifecycleEvent::Installed
            }]
        );
        assert_eq!(
            c.delivery_params(&key(1), LifecycleKind::Installed),
            Some(b"p".to_vec())
        );
        c.mark_installed_delivered(&key(1));

        // Re-registration: no prompt, no second Installed.
        assert_eq!(c.on_registered(&key(1), &wasm, b"p", Some(app(1))), None);
        assert!(drain(&mut rx).is_empty());
        assert_eq!(c.delivery_params(&key(1), LifecycleKind::Installed), None);

        // A re-keyed delegate of the same app inherits the grant: no prompt,
        // and it gets its own Installed straight away.
        assert_eq!(c.on_registered(&key(2), &wasm, b"q", Some(app(1))), None);
        assert_eq!(drain(&mut rx).len(), 1);
    }

    #[test]
    fn not_now_suppresses_the_prompt_for_the_cool_off_only() {
        let (c, time) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let prompt = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        c.record_answer(&prompt, false);
        assert_eq!(c.on_registered(&key(1), &wasm, &[], Some(app(1))), None);
        assert_eq!(c.delivery_params(&key(1), LifecycleKind::NodeStarted), None);
        time.advance_time(DENIAL_COOL_OFF + Duration::from_secs(1));
        assert!(c.on_registered(&key(1), &wasm, &[], Some(app(1))).is_some());
    }

    #[test]
    fn an_unanswered_prompt_stores_nothing_and_asks_again() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let prompt = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        c.prompt_unanswered(&prompt);
        assert_eq!(c.grant(&app(1), Capability::Background), None);
        assert!(c.on_registered(&key(1), &wasm, &[], Some(app(1))).is_some());
    }

    /// No attested app, no binding, no prompt and no delivery.
    #[test]
    fn an_unattested_registration_gets_nothing() {
        let (c, _) = caps();
        let mut rx = c.take_lifecycle_rx().unwrap();
        let wasm = wasm_with_manifest(&background_manifest());
        assert_eq!(c.on_registered(&key(1), &wasm, &[], None), None);
        assert!(
            c.storage.all_records().unwrap().is_empty(),
            "no record for no app"
        );
        assert!(drain(&mut rx).is_empty());
        assert!(c.node_started_targets().is_empty());
        assert_eq!(c.delivery_params(&key(1), LifecycleKind::NodeStarted), None);
    }

    /// A platform delegate is delivered to if ANY bound app holds the grant,
    /// and a second app's registration neither steals nor needs the first's.
    #[test]
    fn a_platform_delegate_runs_if_any_bound_app_is_granted() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let p1 = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        let p2 = c.on_registered(&key(1), &wasm, &[], Some(app(2))).unwrap();
        c.record_answer(&p1, false);
        assert!(c.node_started_targets().is_empty());
        c.record_answer(&p2, true);
        assert_eq!(c.node_started_targets(), vec![key(1)]);
        // Revoking the only grant stops delivery.
        assert!(c.revoke(&app(2), Capability::Background));
        assert!(c.node_started_targets().is_empty());
        assert_eq!(c.delivery_params(&key(1), LifecycleKind::NodeStarted), None);
    }

    /// At the record cap, a never-granted record makes room; a granted one is
    /// never evicted to make room.
    #[test]
    fn the_record_cap_evicts_only_ungranted_records() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let k = |n: u32| {
            DelegateKey::new(
                *blake3::hash(&n.to_le_bytes()).as_bytes(),
                CodeHash::new([1; 32]),
            )
        };
        // Spread over granted apps, within each app's quota.
        let granted_app = |n: u32| app(10 + (n / (MAX_RECORDS_PER_APP as u32 - 1)) as u8);
        let grant = |a: AppIdentity| {
            c.record_answer(
                &CapabilityPrompt {
                    app: a,
                    delegate: key(0),
                    capabilities: vec![Capability::Background],
                },
                true,
            )
        };
        for n in 0..(MAX_CAPABILITY_RECORDS as u32 - 1) {
            grant(granted_app(n));
            assert!(
                c.on_registered(&k(n), &wasm, &[], Some(granted_app(n)))
                    .is_none()
            );
        }
        // One ungranted record fills the table.
        let ungranted = key(250);
        let _ = c.on_registered(&ungranted, &wasm, &[], Some(app(9)));
        c.prompt_unanswered(&CapabilityPrompt {
            app: app(9),
            delegate: ungranted.clone(),
            capabilities: vec![Capability::Background],
        });
        assert_eq!(
            c.storage.all_records().unwrap().len(),
            MAX_CAPABILITY_RECORDS
        );

        // A newcomer takes the ungranted record's place.
        let newcomer = key(251);
        let _ = c.on_registered(&newcomer, &wasm, &[], Some(app(8)));
        let keys: HashSet<DelegateKey> = c
            .storage
            .all_records()
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys.len(), MAX_CAPABILITY_RECORDS);
        assert!(keys.contains(&newcomer));
        assert!(!keys.contains(&ungranted));

        // The newcomer's app has a prompt open, so every record is now
        // protected, and a further newcomer is refused rather than evicting.
        let late = key(252);
        assert!(c.on_registered(&late, &wasm, &[], Some(app(7))).is_none());
        let after: HashSet<DelegateKey> = c
            .storage
            .all_records()
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(after, keys, "no protected record is evicted");
        assert!(!c.is_budgeted(&late));
    }

    /// A full app list drops an ungranted binding for a new app, so eight
    /// bindings cannot lock the real app out.
    #[test]
    fn a_full_app_list_makes_room_by_dropping_an_ungranted_app() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        for n in 0..MAX_APPS_PER_DELEGATE as u8 {
            let _ = c.on_registered(&key(1), &wasm, &[], Some(app(n)));
            c.prompt_unanswered(&CapabilityPrompt {
                app: app(n),
                delegate: key(1),
                capabilities: vec![Capability::Background],
            });
        }
        let granted_app = app(0);
        c.record_answer(
            &CapabilityPrompt {
                app: granted_app,
                delegate: key(1),
                capabilities: vec![Capability::Background],
            },
            true,
        );
        let real = app(100);
        let _ = c.on_registered(&key(1), &wasm, &[], Some(real));
        let rec = c.load_record(&key(1)).unwrap();
        assert_eq!(rec.apps.len(), MAX_APPS_PER_DELEGATE);
        assert!(rec.apps.contains(&real), "the new app got a slot");
        assert!(
            rec.apps.contains(&granted_app),
            "the granted app kept its slot"
        );
    }

    /// Delivery requires the manifest to list the kind, whatever the grant.
    #[test]
    fn delivery_requires_the_listed_kind() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&DelegateManifest::new(
            vec![LifecycleKind::NodeStarted],
            vec![Capability::Background],
        ));
        let p = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        c.record_answer(&p, true);
        assert_eq!(c.delivery_params(&key(1), LifecycleKind::Installed), None);
        assert!(
            c.delivery_params(&key(1), LifecycleKind::NodeStarted)
                .is_some()
        );
        // A manifest listing lifecycle kinds without Background (only possible
        // by hand, the macro refuses it) is asked for Background anyway, since
        // delivery needs it, and gets nothing until it is granted.
        let wasm = wasm_with_manifest(&DelegateManifest::new(
            vec![LifecycleKind::NodeStarted],
            vec![],
        ));
        let p = c
            .on_registered(&key(2), &wasm, &[], Some(app(9)))
            .expect("lifecycle implies Background");
        assert_eq!(p.capabilities, vec![Capability::Background]);
        assert_eq!(c.delivery_params(&key(2), LifecycleKind::NodeStarted), None);
        c.record_answer(&p, true);
        assert!(
            c.delivery_params(&key(2), LifecycleKind::NodeStarted)
                .is_some()
        );
    }

    #[test]
    fn duty_budget_refuses_when_spent_and_refills_with_remainder() {
        let time = SharedMockTimeSource::new();
        let limits = BudgetLimits {
            duty_refill_per_sec: Duration::from_millis(10),
            duty_burst: Duration::from_millis(50),
            ..BudgetLimits::default()
        };
        let c = DelegateCapabilities::with_time_source(
            Arc::new(MemoryCapabilityStorage::default()),
            Arc::new(time.clone()),
            limits,
        );
        assert!(c.duty_available(&key(1)));
        c.charge_duty(&key(1), Duration::from_millis(60), true);
        assert!(!c.duty_available(&key(1)), "spent past the burst");
        // Overdraft is debt, not forgiven: 50 ms burst - 60 ms = -10 ms, and
        // 10 ms of refill at 10 ms/s takes a full second.
        time.advance_time(Duration::from_millis(900));
        assert!(
            !c.duty_available(&key(1)),
            "still paying back the overdraft"
        );
        time.advance_time(Duration::from_millis(200));
        assert!(c.duty_available(&key(1)));
        // The node bucket (10% default refill) was charged by the lifecycle
        // run; a notification run (node_wide = false) charges only its own
        // delegate.
        let node_before = c.budget.lock().node_duty.tokens_us;
        c.charge_duty(&key(2), Duration::from_millis(40), false);
        assert_eq!(c.budget.lock().node_duty.tokens_us, node_before);
        assert!(c.duty_available(&key(3)), "another delegate is unaffected");
        // 0.3 us of refill per step ten times is 3 us: a truncating refill
        // would add 0.
        let start = c.budget.lock().duty.get(&key(1)).unwrap().tokens_us;
        for _ in 0..10 {
            time.advance_time(Duration::from_micros(30));
            let _ = c.duty_available(&key(1));
        }
        let tokens = c.budget.lock().duty.get(&key(1)).unwrap().tokens_us;
        assert_eq!(tokens - start, 3);
    }

    /// Notification runs are charged but never refused, so debt is floored:
    /// a long burst of them cannot starve lifecycle runs for days.
    #[test]
    fn duty_debt_is_floored_at_one_burst() {
        let time = SharedMockTimeSource::new();
        let limits = BudgetLimits {
            duty_refill_per_sec: Duration::from_millis(10),
            duty_burst: Duration::from_secs(10),
            ..BudgetLimits::default()
        };
        let c = DelegateCapabilities::with_time_source(
            Arc::new(MemoryCapabilityStorage::default()),
            Arc::new(time.clone()),
            limits,
        );
        for _ in 0..1000 {
            c.charge_duty(&key(1), Duration::from_secs(10), false);
        }
        assert_eq!(
            c.budget.lock().duty.get(&key(1)).unwrap().tokens_us,
            -5_000_000
        );
        // 5 s of debt at 10 ms/s: back above zero after 500 s, not days.
        time.advance_time(Duration::from_secs(501));
        assert!(c.duty_available(&key(1)));
    }

    /// Only a delegate whose app holds the grant is budgeted: ungranted
    /// manifest delegates cannot spend the allowance granted ones rely on.
    #[test]
    fn only_granted_delegates_are_budgeted() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let p = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        assert!(c.is_recorded(&key(1)));
        assert!(!c.is_budgeted(&key(1)), "recorded but not granted");
        c.record_answer(&p, true);
        assert!(c.is_budgeted(&key(1)));
        assert!(c.revoke(&app(1), Capability::Background));
        assert!(!c.is_budgeted(&key(1)));
    }

    /// A granted delegate still owed Installed is re-queued at start.
    #[test]
    fn installed_still_owed_is_a_start_target() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let p = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        assert!(c.installed_pending_targets().is_empty(), "not granted yet");
        c.record_answer(&p, true);
        assert_eq!(c.installed_pending_targets(), vec![key(1)]);
        c.mark_installed_delivered(&key(1));
        assert!(c.installed_pending_targets().is_empty());
    }

    /// A manifest with Background but no lifecycle kind has nothing to use it
    /// for yet: no prompt, no record.
    #[test]
    fn background_without_lifecycle_asks_nothing() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&DelegateManifest::new(vec![], vec![Capability::Background]));
        assert!(c.on_registered(&key(1), &wasm, &[], Some(app(1))).is_none());
        assert!(!c.is_recorded(&key(1)));
    }

    #[test]
    fn an_app_can_fill_only_its_own_record_quota() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let k = |n: u32| {
            DelegateKey::new(
                *blake3::hash(&n.to_le_bytes()).as_bytes(),
                CodeHash::new([1; 32]),
            )
        };
        for n in 0..(MAX_RECORDS_PER_APP as u32 + 5) {
            let _ = c.on_registered(&k(n), &wasm, &[], Some(app(1)));
            c.prompt_unanswered(&CapabilityPrompt {
                app: app(1),
                delegate: k(n),
                capabilities: vec![Capability::Background],
            });
        }
        assert_eq!(c.storage.all_records().unwrap().len(), MAX_RECORDS_PER_APP);
        // Another app is unaffected.
        assert!(
            c.on_registered(&key(200), &wasm, &[], Some(app(2)))
                .is_some()
        );
        assert!(c.is_recorded(&key(200)));
    }

    /// An app in its "Not now" cool-off creates no new records.
    #[test]
    fn a_declined_app_creates_no_new_records() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let p = c.on_registered(&key(1), &wasm, &[], Some(app(1))).unwrap();
        c.record_answer(&p, false);
        assert!(c.on_registered(&key(2), &wasm, &[], Some(app(1))).is_none());
        assert!(!c.is_recorded(&key(2)));
    }

    /// Protected bindings (a relevant grant, or a prompt open right now) are
    /// never the ones evicted; a lifecycle-only manifest counts Background as
    /// relevant.
    #[test]
    fn binding_eviction_spares_granted_and_prompting_apps() {
        let (c, _) = caps();
        let lifecycle_only = wasm_with_manifest(&DelegateManifest::new(
            vec![LifecycleKind::NodeStarted],
            vec![],
        ));
        // app(0) is granted Background via some other delegate.
        c.record_answer(
            &CapabilityPrompt {
                app: app(0),
                delegate: key(9),
                capabilities: vec![Capability::Background],
            },
            true,
        );
        for n in 0..MAX_APPS_PER_DELEGATE as u8 {
            if let Some(p) = c.on_registered(&key(1), &lifecycle_only, &[], Some(app(n))) {
                // Nobody answers: the prompt closes without a decision.
                c.prompt_unanswered(&p);
            }
        }
        let rec = c.load_record(&key(1)).unwrap();
        assert_eq!(rec.apps.len(), MAX_APPS_PER_DELEGATE);
        let _ = c.on_registered(&key(1), &lifecycle_only, &[], Some(app(100)));
        let rec = c.load_record(&key(1)).unwrap();
        assert!(
            rec.apps.contains(&app(0)),
            "the granted app keeps its binding"
        );
        assert!(rec.apps.contains(&app(100)));

        // With a prompt open for every ungranted app, nothing is evicted.
        let wasm = wasm_with_manifest(&background_manifest());
        for n in 0..MAX_APPS_PER_DELEGATE as u8 {
            let _ = c.on_registered(&key(2), &wasm, &[], Some(app(50 + n)));
        }
        let _ = c.on_registered(&key(2), &wasm, &[], Some(app(120)));
        let rec = c.load_record(&key(2)).unwrap();
        assert!(
            !rec.apps.contains(&app(120)),
            "no binding with an open prompt is evicted"
        );
    }

    #[test]
    fn unregister_drops_only_the_unregistering_apps_binding() {
        let (c, _) = caps();
        let wasm = wasm_with_manifest(&background_manifest());
        let _ = c.on_registered(&key(1), &wasm, &[], Some(app(1)));
        let _ = c.on_registered(&key(1), &wasm, &[], Some(app(2)));
        c.on_unregistered(&key(1), None);
        c.on_unregistered(&key(1), Some(app(3)));
        assert_eq!(c.load_record(&key(1)).unwrap().apps.len(), 2);
        c.on_unregistered(&key(1), Some(app(1)));
        assert_eq!(c.load_record(&key(1)).unwrap().apps, vec![app(2)]);
        c.on_unregistered(&key(1), Some(app(2)));
        assert!(c.load_record(&key(1)).is_none());
        assert!(!c.is_recorded(&key(1)));
    }

    /// The debt floor must be repayable within the lifecycle retry window at
    /// the default limits, or a delegate at the floor always loses its next
    /// lifecycle run.
    #[test]
    fn debt_is_repaid_inside_the_retry_window() {
        let limits = BudgetLimits::default();
        let floor_us = limits.duty_burst.as_micros() / 2;
        let repay_s = floor_us / limits.duty_refill_per_sec.as_micros();
        let window_s = (LIFECYCLE_RETRY_DELAY * (LIFECYCLE_MAX_ATTEMPTS - 1)).as_secs() as u128;
        assert!(repay_s < window_s, "repay {repay_s}s vs window {window_s}s");
    }

    #[test]
    fn network_ops_are_bounded_per_delegate_per_node_and_per_contract() {
        let time = SharedMockTimeSource::new();
        let limits = BudgetLimits {
            ops_per_delegate_per_min: 3,
            ops_per_node_per_min: 5,
            writes_per_contract_per_min: 2,
            ..BudgetLimits::default()
        };
        let c = DelegateCapabilities::with_time_source(
            Arc::new(MemoryCapabilityStorage::default()),
            Arc::new(time.clone()),
            limits,
        );
        let target = ContractInstanceId::new([7; 32]);
        assert_eq!(c.admit_op(&key(1), Some(&target)), Ok(()));
        assert_eq!(c.admit_op(&key(1), Some(&target)), Ok(()));
        assert_eq!(
            c.admit_op(&key(1), Some(&target)),
            Err(BudgetRefusal::ContractWrites)
        );
        assert_eq!(c.admit_op(&key(1), None), Ok(()));
        assert_eq!(
            c.admit_op(&key(1), None),
            Err(BudgetRefusal::DelegateNetworkOps)
        );
        assert_eq!(c.admit_op(&key(2), None), Ok(()));
        assert_eq!(c.admit_op(&key(3), None), Ok(()));
        assert_eq!(
            c.admit_op(&key(4), None),
            Err(BudgetRefusal::NodeNetworkOps)
        );
        assert_eq!(c.stats.refused_node_ops.load(Ordering::Relaxed), 1);
        assert_eq!(c.stats.refused_delegate_ops.load(Ordering::Relaxed), 1);
        assert_eq!(c.stats.refused_contract_writes.load(Ordering::Relaxed), 1);
        time.advance_time(Duration::from_secs(61));
        assert_eq!(c.admit_op(&key(1), Some(&target)), Ok(()));
    }

    #[test]
    fn a_full_lifecycle_queue_counts_the_drop() {
        let (c, _) = caps();
        let _rx = c.take_lifecycle_rx().unwrap();
        for n in 0..LIFECYCLE_QUEUE_CAPACITY {
            assert!(c.queue(LifecycleRun {
                key: key((n % 250) as u8),
                event: LifecycleEvent::Installed,
            }));
        }
        assert!(!c.queue(LifecycleRun {
            key: key(1),
            event: LifecycleEvent::Installed,
        }));
        assert_eq!(c.stats.lifecycle_queue_full.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn schedule_pops_in_due_order_and_not_early() {
        let mut sched = LifecycleSchedule::default();
        let t0 = tokio::time::Instant::now();
        let run = |n| LifecycleRun {
            key: key(n),
            event: LifecycleEvent::Installed,
        };
        sched.push(t0 + Duration::from_secs(10), run(1), 0);
        sched.push(t0 + Duration::from_secs(5), run(2), 3);
        assert_eq!(sched.next_deadline(), Some(t0 + Duration::from_secs(5)));
        assert_eq!(sched.pop_due(t0), None);
        assert_eq!(
            sched.pop_due(t0 + Duration::from_secs(6)),
            Some((run(2), 3))
        );
        assert_eq!(sched.pop_due(t0 + Duration::from_secs(6)), None);
        assert_eq!(
            sched.pop_due(t0 + Duration::from_secs(10)),
            Some((run(1), 0))
        );
        assert_eq!(sched.len(), 0);
        assert_eq!(sched.next_deadline(), None);

        // One waiting run per (delegate, kind).
        assert!(sched.push(t0, run(3), 0));
        assert!(!sched.push(t0, run(3), 0));
        assert!(sched.push(
            t0,
            LifecycleRun {
                key: key(3),
                event: LifecycleEvent::NodeStarted {
                    down_since_ms: None
                },
            },
            0
        ));
        assert_eq!(sched.len(), 2);
        let _ = sched.pop_due(t0);
        let _ = sched.pop_due(t0);
        assert!(sched.push(t0, run(3), 0), "free again once popped");

        // A duplicate restarts the waiting run's attempt count.
        let mut sched = LifecycleSchedule::default();
        assert!(sched.push(t0, run(4), 59));
        assert!(!sched.push(t0, run(4), 0));
        assert_eq!(sched.pop_due(t0), Some((run(4), 0)));
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn grants_and_records_survive_a_reopen() {
        let dir = crate::util::tests::get_temp_dir();
        let wasm = wasm_with_manifest(&background_manifest());
        {
            let db = crate::contract::storages::Storage::new(dir.path())
                .await
                .unwrap();
            let c = DelegateCapabilities::new(Arc::new(db));
            let p = c.on_registered(&key(1), &wasm, b"x", Some(app(1))).unwrap();
            c.record_answer(&p, true);
        }
        let db = crate::contract::storages::Storage::new(dir.path())
            .await
            .unwrap();
        let c = DelegateCapabilities::new(Arc::new(db));
        assert!(matches!(
            c.grant(&app(1), Capability::Background),
            Some(Grant::Granted { .. })
        ));
        assert_eq!(c.node_started_targets(), vec![key(1)]);
        assert_eq!(c.on_registered(&key(1), &wasm, b"x", Some(app(1))), None);
    }
}
