//! Mixed-version network test: the current build (N) and the previous
//! published releases (N-1, N-2) run in one Docker NAT network and must
//! interoperate. The 1.0 transition is not a flag day, so a rolling upgrade has
//! to work in both orders (old gateway with new peers, new gateway with old
//! peers), and the handshake floor has to keep a below-floor node out.
//!
//! Tests (each scenario is its own network):
//! - `mixed_version_old_gateway` (A): gateway N-1, peers [N, N, N-1].
//! - `mixed_version_new_gateway` (B): gateway N, peers [N-1, N-1, N].
//! - `mixed_version_floor` (C): proves the FLOOR causes a refusal. First a
//!   control, gateway N with peers [N-1, N-2], which must all connect (so N-2 is
//!   wire-compatible with this source). Then the same peers against N', a second
//!   build of the same source whose floor is set to min(N-1's version, HEAD's
//!   version) (`FREENET_MIN_COMPATIBLE_VERSION`): N-1 must be accepted, N-2
//!   refused. The only difference between the two gateways is the floor
//!   constant, so the refusal is caused by the floor N' advertises in the
//!   handshake. Either side may enforce it: an old release also refuses a peer
//!   whose floor exceeds its own version. The test therefore shows that the
//!   advertised floor decides the outcome, not that N's own check is the one
//!   that fires.
//!   A floor raise past N-2 turns the control red on purpose (see the test).
//! - `mixed_version_very_old_node`: gateway N, peers [N-1, below-floor], where
//!   the below-floor node is the newest release under N's floor. It stays out and
//!   does not disturb the rest of the network. This is NOT a floor test: a node
//!   that old could also fail for wire drift unrelated to the floor.
//!
//! What is asserted (never log lines):
//! - connectivity from each node's own `ConnectedPeers`, polled to a bound;
//! - each container runs the binary the manifest names (sha256 of PID 1's exe);
//! - cross-version PUT then GET, SUBSCRIBE + one UPDATE delivered as an
//!   `UpdateNotification` on the subscribing client's WebSocket, both directions;
//! - a 600 KiB streaming PUT/GET round trip with a non-uniform payload, both
//!   directions;
//! - a refused node reports no connections for a window and its container keeps
//!   running. There is no positive signal that the refusal was orderly (the node
//!   gives no refusal reason to clients), so "refused" is all this claims.
//!
//! Transport: the node exposes no per-connection transport and logs nothing about
//! it, so the transport a pair actually used cannot be read back. The report
//! records only the CONFIGURED mode of each node. `FREENET_MIXED_QUIC=force`
//! configures builds that support it to prefer native QUIC data at 100%.
//!
//! Versions and floors are never hard-coded, with one exception: `NATIVE_QUIC_SINCE`
//! (below). N and N' floors come from `crates/core/Cargo.toml` and the N' env;
//! N-1, N-2 and the below-floor build from the manifest written by
//! `scripts/fetch-release-binary.sh`.
//!
//! Requires: FREENET_TEST_DOCKER_NAT=1, FREENET_BINARY_PATH (N),
//! FREENET_NPRIME_BINARY_PATH and FREENET_NPRIME_FLOOR (N'), FREENET_MIXED_VERSIONS
//! (the manifest), a Docker daemon. A missing N-2, below-floor or N' input FAILS
//! the test; `FREENET_MIXED_LENIENT=1` (local use only, never CI) turns that into a
//! skip. Run one test at a time (`--test-threads 1`): each owns a Docker network.
//! Do not run it while anything else on the host uses `freenet-nat-*` containers:
//! the harness removes stale ones at startup. `FREENET_MIXED_ARTIFACT_DIR`
//! receives per-scenario reports and container logs.
//!
//! Local recipe (from the repo root): build N with `cargo build`, build N' with
//! `FREENET_MIN_COMPATIBLE_VERSION=<the lower of N-1's version and HEAD's> cargo
//! build` into another location (the build refuses a floor above HEAD's version), run `scripts/fetch-release-binary.sh resolve DIR --core-toml
//! crates/core/Cargo.toml`, then run the tests with the variables above.

mod common;

use anyhow::{Context, Result, anyhow, bail};
use common::{
    APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT, connect_ws_with_retry, get_contract_state,
    load_contract, subscribe_to_contract, update_contract_state,
};
use freenet_ping_app::ping_client::wait_for_put_response;
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractRequest, ContractResponse, HostResponse, NodeQuery, QueryResponse,
        WebApi,
    },
    prelude::*,
};
use freenet_test_network::{Backend, DockerNatConfig, FreenetBinary, NetworkBuilder};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

/// Wait for the whole network to form. Doubled from the single-version NAT test.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
/// After the harness reports readiness, how long every expected node may take to
/// show a connection in its own `ConnectedPeers`.
const CONNECT_SETTLE: Duration = Duration::from_secs(60);
/// How long the subscriber's client may wait for the UPDATE notification. The
/// update is sent once, so a repair path cannot hide a broken live delivery.
const UPDATE_TIMEOUT: Duration = Duration::from_secs(40);
/// Bound on one PUT or GET round trip.
const OP_TIMEOUT: Duration = Duration::from_secs(90);
/// Window in which a refused node must stay unconnected, after the rest of the
/// network is demonstrably up.
const REFUSAL_WINDOW: Duration = Duration::from_secs(60);
/// Extra time past the refusal window for a refused node's final probe to answer.
const REFUSAL_GRACE: Duration = Duration::from_secs(90);
/// Bytes of padding in the large state. Above the 64 KiB streaming threshold and
/// the 512 KiB single-message size.
const LARGE_PADDING: usize = 600 * 1024;

/// The one hard-coded version: the first release that can carry native QUIC, so
/// the first with the `--transport-quic` and `--quic-data-percent` flags (checked
/// with `freenet network --help` on the 0.2.163 binary; 0.2.162 has neither flag
/// set). Native QUIC pairs form only between builds at or above it.
const NATIVE_QUIC_SINCE: Version = Version(0, 2, 163);

// ---------------------------------------------------------------- versions

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Version(u32, u32, u32);

impl Version {
    fn parse(s: &str) -> Result<Self> {
        let mut it = s.trim().trim_start_matches('v').split('.');
        let mut next = || -> Result<u32> {
            it.next()
                .ok_or_else(|| anyhow!("version '{s}' is not X.Y.Z"))?
                .parse()
                .with_context(|| format!("version '{s}' is not numeric"))
        };
        let v = Version(next()?, next()?, next()?);
        if it.next().is_some() {
            bail!("version '{s}' is not X.Y.Z");
        }
        Ok(v)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

#[derive(Clone, Debug)]
struct Build {
    /// "N", "N'", "N-1", "N-2" or "below-floor".
    role: &'static str,
    /// Built from this checkout's source (N and N'), as opposed to a release.
    from_head: bool,
    version: Version,
    /// Lowest version this build accepts in the handshake.
    floor: Version,
    binary: PathBuf,
    sha256: String,
}

impl Build {
    fn label(&self) -> String {
        format!("{}@{}", self.role, self.version)
    }
}

/// Two builds may connect iff each is at or above the other's floor.
fn can_connect(a: &Build, b: &Build) -> bool {
    a.version >= b.floor && b.version >= a.floor
}

/// `package.version` and `min-compatible-version` from a Cargo.toml text. The
/// floor falls back to the package version, the order `crates/core/build.rs` uses
/// when the release env var is not set.
fn version_and_floor(cargo_toml: &str) -> Result<(Version, Version)> {
    let field = |key: &str| {
        cargo_toml.lines().find_map(|l| {
            let rest = l.strip_prefix(key)?.trim_start().strip_prefix('=')?;
            Some(rest.trim().trim_matches('"').to_string())
        })
    };
    let version = Version::parse(&field("version").ok_or_else(|| anyhow!("no package version"))?)?;
    let floor = match field("min-compatible-version") {
        Some(f) => Version::parse(&f)?,
        None => version,
    };
    Ok((version, floor))
}

fn sha256_file(path: &Path) -> Result<String> {
    let out = Command::new("sha256sum")
        .arg(path)
        .output()
        .context("running sha256sum")?;
    if !out.status.success() {
        bail!("sha256sum {} failed", path.display());
    }
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("empty sha256sum output for {}", path.display()))
}

/// `FREENET_MIXED_LENIENT=1` lets a missing optional input skip instead of fail.
/// Local use only: CI never sets it, so a missing binary cannot pass silently.
fn lenient() -> bool {
    std::env::var("FREENET_MIXED_LENIENT").is_ok_and(|v| v == "1")
}

/// A required input is missing: an error, or a loud skip in lenient mode.
fn missing(what: &str) -> Result<()> {
    if lenient() {
        eprintln!("SKIPPED (FREENET_MIXED_LENIENT=1): {what}");
        Ok(())
    } else {
        bail!("{what} (set FREENET_MIXED_LENIENT=1 to skip locally; CI must not)")
    }
}

/// `FREENET_MIXED_QUIC`: unset or `default`, or `force`. Anything else is an
/// error, so a typo cannot silently run the wrong mode.
fn quic_mode() -> Result<bool> {
    match std::env::var("FREENET_MIXED_QUIC").as_deref() {
        Err(_) | Ok("default") => Ok(false),
        Ok("force") => Ok(true),
        Ok(other) => bail!("FREENET_MIXED_QUIC must be 'default' or 'force', got '{other}'"),
    }
}

struct Builds {
    head: Build,
    /// Same source as `head` with its floor set to min(N-1, HEAD) versions.
    nprime: Option<Build>,
    n1: Build,
    n2: Option<Build>,
    /// Newest release strictly below N's floor, if one can be fetched.
    below: Option<Build>,
}

fn load_builds() -> Result<Builds> {
    quic_mode()?;
    let core_toml = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../crates/core/Cargo.toml");
    let (version, floor) = version_and_floor(
        &std::fs::read_to_string(&core_toml)
            .with_context(|| format!("reading {}", core_toml.display()))?,
    )?;
    let head_binary: PathBuf = std::env::var("FREENET_BINARY_PATH")
        .context("FREENET_BINARY_PATH must point to the N binary")?
        .into();
    let head = Build {
        role: "N",
        from_head: true,
        version,
        floor,
        sha256: sha256_file(&head_binary)?,
        binary: head_binary,
    };

    let manifest_path = std::env::var("FREENET_MIXED_VERSIONS").context(
        "FREENET_MIXED_VERSIONS must point to versions.json from fetch-release-binary.sh",
    )?;
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
    let from_manifest = |key: &str, role: &'static str| -> Result<Option<Build>> {
        let Some(e) = manifest.get(key) else {
            return Ok(None);
        };
        let get = |f: &str| {
            e.get(f)
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("{key}.{f} missing in {manifest_path}"))
        };
        let binary: PathBuf = get("path")?.into();
        let sha256 = sha256_file(&binary)?;
        if sha256 != get("binary_sha256")? {
            bail!(
                "{role}: {} does not hash to the manifest's binary_sha256",
                binary.display()
            );
        }
        Ok(Some(Build {
            role,
            from_head: false,
            version: Version::parse(get("version")?)?,
            floor: Version::parse(get("floor")?)?,
            binary,
            sha256,
        }))
    };
    let n1 = from_manifest("n1", "N-1")?.ok_or_else(|| anyhow!("manifest has no n1"))?;
    let n2 = from_manifest("n2", "N-2")?;
    let below = from_manifest("below", "below-floor")?;
    if let Some(b) = &below
        && b.version >= head.floor
    {
        bail!(
            "below-floor build {} is not below N's floor {}: stale manifest",
            b.version,
            head.floor
        );
    }

    let nprime = match (
        std::env::var("FREENET_NPRIME_BINARY_PATH"),
        std::env::var("FREENET_NPRIME_FLOOR"),
    ) {
        (Ok(path), Ok(floor)) => {
            let binary: PathBuf = path.into();
            Some(Build {
                role: "N'",
                from_head: true,
                version: head.version,
                floor: Version::parse(&floor)?,
                sha256: sha256_file(&binary)?,
                binary,
            })
        }
        _ => None,
    };
    if let Some(np) = &nprime {
        // The floor test's claim is "N-1 is accepted, N-2 is refused", so N-2 <
        // floor <= N-1. The floor also cannot exceed HEAD's own version: between
        // a release being published and its version bump reaching main, N-1 can
        // be newer than HEAD, and the build refuses such a floor.
        if np.floor > n1.version {
            bail!(
                "N' floor {} is above N-1's version {}: N-1 would be refused",
                np.floor,
                n1.version
            );
        }
        if np.floor > head.version {
            bail!(
                "N' floor {} is above HEAD's version {}: the build cannot use it",
                np.floor,
                head.version
            );
        }
        eprintln!(
            "N' floor {}: {}",
            np.floor,
            if np.floor == n1.version {
                "equals N-1's version"
            } else {
                "capped at HEAD's version, which is below N-1's (a release not yet merged back)"
            }
        );
        if let Some(n2) = &n2
            && n2.version >= np.floor
        {
            bail!(
                "N-2 {} is not below N' floor {}: the floor test cannot refuse it",
                n2.version,
                np.floor
            );
        }
    }

    // A floor above its own version would refuse the node itself: a wrong read.
    for b in [Some(&head), Some(&n1), n2.as_ref(), below.as_ref()]
        .into_iter()
        .flatten()
    {
        if b.floor > b.version {
            bail!("{}: floor {} is above its own version", b.label(), b.floor);
        }
    }
    // Different roles must be different files, or a mis-wired path would test nothing.
    let all: Vec<&Build> = [
        Some(&head),
        nprime.as_ref(),
        Some(&n1),
        n2.as_ref(),
        below.as_ref(),
    ]
    .into_iter()
    .flatten()
    .collect();
    for (i, a) in all.iter().enumerate() {
        for b in &all[i + 1..] {
            if a.sha256 == b.sha256 {
                bail!("{} and {} are the same binary", a.label(), b.label());
            }
        }
    }
    Ok(Builds {
        head,
        nprime,
        n1,
        n2,
        below,
    })
}

// ---------------------------------------------------------------- scenario

struct Spec<'a> {
    name: &'static str,
    gateway: &'a Build,
    peers: Vec<&'a Build>,
}

struct Node<'a> {
    name: String,
    build: &'a Build,
    container: String,
    /// Empty until the network is built.
    ws_url: String,
}

/// A step label in the error chain, so a failure says which criterion broke.
fn step<T>(label: &str, r: Result<T>) -> Result<T> {
    r.with_context(|| format!("step={label}"))
}

fn docker(args: &[&str]) -> String {
    Command::new("docker")
        .args(args)
        .output()
        .map(|o| String::from_utf8_lossy(&[o.stdout, o.stderr].concat()).into_owned())
        .unwrap_or_else(|e| format!("docker {args:?} failed to start: {e}"))
}

fn container_running(name: &str) -> bool {
    docker(&["inspect", "-f", "{{.State.Running}}", name]).trim() == "true"
}

/// Inner bounds of one probe. The outer bound in `probe` exceeds their sum, so a
/// slow node is never misread as a timed-out probe.
const PROBE_CONNECT_SECS: u64 = 20;
const PROBE_REPLY_TIMEOUT: Duration = Duration::from_secs(15);
const PROBE_OUTER_TIMEOUT: Duration = Duration::from_secs(45);

/// Ask a node how many peers it is connected to, for use inside polling windows.
///
/// `Ok(None)` is "no data yet": the WebSocket could not be opened, the request
/// could not be sent, or no reply arrived in time. Those are what a node that is
/// starting, busy or restarting looks like. Anything else is a protocol break and
/// is an `Err` that must fail the caller at once: an unexpected reply variant, or
/// a reply that does not decode. Finding that kind of break is what this test is
/// for, so it must not be absorbed as "no data".
async fn probe(ws_url: &str) -> Result<Option<usize>> {
    probe_within(ws_url, PROBE_OUTER_TIMEOUT).await
}

/// `probe` with the outer bound lowered to `limit` when that is shorter, so a
/// caller with a hard deadline is never held past it by a stalled node.
async fn probe_within(ws_url: &str, limit: Duration) -> Result<Option<usize>> {
    let outer = limit.min(PROBE_OUTER_TIMEOUT);
    let attempt = async {
        let mut client = match connect_ws_with_retry(
            &format!("{ws_url}?encodingProtocol=native"),
            "probe",
            PROBE_CONNECT_SECS,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  probe of {ws_url}: cannot open a WebSocket: {e:#}");
                return Ok(None);
            }
        };
        if let Err(e) = client
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await
        {
            eprintln!("  probe of {ws_url}: cannot send: {e}");
            return Ok(None);
        }
        match tokio::time::timeout(PROBE_REPLY_TIMEOUT, client.recv()).await {
            Err(_) => {
                eprintln!("  probe of {ws_url}: no reply within {PROBE_REPLY_TIMEOUT:?}");
                Ok(None)
            }
            Ok(Err(e)) => bail!("client-api: reply to ConnectedPeers did not decode: {e}"),
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }))) => {
                Ok(Some(peers.len()))
            }
            Ok(Ok(other)) => bail!("client-api: unexpected reply to ConnectedPeers: {other:?}"),
        }
    };
    match tokio::time::timeout(outer, attempt).await {
        Ok(r) => r,
        Err(_) => {
            eprintln!("  probe of {ws_url} timed out after {outer:?}");
            Ok(None)
        }
    }
}

/// Contract options with a tag that makes the key unique to one check, so a
/// stale delivery from an earlier check cannot satisfy a later one.
fn contract_for(tag: &str) -> Result<ContractContainer> {
    let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
    let tagged = |code_key: String| PingContractOptions {
        frequency: Duration::from_secs(5),
        ttl: Duration::from_secs(300),
        tag: format!("{APP_TAG}-{tag}"),
        code_key,
    };
    let temp = Parameters::from(serde_json::to_vec(&tagged(String::new()))?);
    let container = load_contract(&path_to_code, temp)?;
    let code_hash = CodeHash::from_code(container.data());
    let params = Parameters::from(serde_json::to_vec(&tagged(code_hash.to_string()))?);
    load_contract(&path_to_code, params)
}

async fn put_state(
    client: &mut WebApi,
    container: ContractContainer,
    state: &Ping,
) -> Result<ContractKey> {
    let key = container.key();
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: container,
            state: WrappedState::new(serde_json::to_vec(state)?),
            related_contracts: RelatedContracts::new(),
            subscribe: false,
            blocking_subscribe: false,
        }))
        .await?;
    tokio::time::timeout(OP_TIMEOUT, wait_for_put_response(client, &key))
        .await
        .context("PUT timed out")?
        .map_err(|e| anyhow!("PUT failed: {e}"))
}

async fn get_state(client: &mut WebApi, key: ContractKey) -> Result<Ping> {
    tokio::time::timeout(OP_TIMEOUT, get_contract_state(client, key, true))
        .await
        .context("GET timed out")?
}

/// Wait on the subscribing client's own WebSocket for the `UpdateNotification`
/// that carries `entry`. This is live delivery to the subscriber: unlike a GET,
/// it cannot be satisfied by a request forwarded to the writer's node.
async fn wait_for_update_notification(
    client: &mut WebApi,
    key: ContractKey,
    entry: &str,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    let deadline = start + timeout;
    let mut other_messages = 0u32;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!(
                "no UpdateNotification carrying '{entry}' within {timeout:?} \
                 ({other_messages} unrelated message(s) seen)"
            );
        }
        match tokio::time::timeout(remaining, client.recv()).await {
            Err(_) => continue,
            Ok(Err(e)) => bail!("client closed while waiting for the update: {e}"),
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key: k,
                update,
            }))) if k == key => {
                let carries =
                    |bytes: &[u8]| bytes.windows(entry.len()).any(|w| w == entry.as_bytes());
                let hit = match &update {
                    UpdateData::Delta(d) => carries(d.as_ref()),
                    UpdateData::State(s) => carries(s.as_ref()),
                    UpdateData::StateAndDelta { state, delta } => {
                        carries(state.as_ref()) || carries(delta.as_ref())
                    }
                    _ => false,
                };
                if hit {
                    eprintln!(
                        "update notification carrying '{entry}' after {:?}",
                        start.elapsed()
                    );
                    return Ok(());
                }
                other_messages += 1;
            }
            Ok(Ok(_)) => other_messages += 1,
        }
    }
}

struct Report {
    scenario: &'static str,
    steps: Vec<(String, Duration)>,
    notes: Vec<String>,
}

impl Report {
    fn note(&mut self, s: impl Into<String>) {
        let s = s.into();
        eprintln!("[{}] {s}", self.scenario);
        self.notes.push(s);
    }
    async fn timed<T>(&mut self, label: &str, fut: impl Future<Output = Result<T>>) -> Result<T> {
        let start = Instant::now();
        let r = step(label, fut.await);
        let took = start.elapsed();
        eprintln!(
            "[{}] step {label}: {} in {took:?}",
            self.scenario,
            if r.is_ok() { "ok" } else { "FAILED" }
        );
        self.steps.push((label.to_string(), took));
        r
    }
}

/// One direction of the interop checks: `src` writes, `dst` reads.
/// Cross-version PUT then GET, then SUBSCRIBE at `dst` and an UPDATE from `src`
/// that must arrive as a notification on `dst`'s client.
async fn check_direction(
    report: &mut Report,
    src: (&Node<'_>, &mut WebApi),
    dst: (&Node<'_>, &mut WebApi),
    dir_tag: &str,
) -> Result<()> {
    let (src_node, src_client) = src;
    let (dst_node, dst_client) = dst;
    let dir = format!("{} -> {}", src_node.build.label(), dst_node.build.label());

    let container = contract_for(dir_tag)?;
    let key = container.key();
    let mut initial = Ping::default();
    initial.insert(format!("init-{dir_tag}"));

    report
        .timed(&format!("put-get {dir}"), async {
            put_state(src_client, container, &initial).await?;
            let state = get_state(dst_client, key).await?;
            if !state.contains_key(&format!("init-{dir_tag}")) {
                bail!(
                    "GET at {dir_tag} dst returned a state without the PUT entry; keys: {:?}",
                    state.keys().collect::<Vec<_>>()
                );
            }
            Ok(())
        })
        .await?;

    report
        .timed(&format!("subscribe-update {dir}"), async {
            tokio::time::timeout(OP_TIMEOUT, subscribe_to_contract(dst_client, key))
                .await
                .context("SUBSCRIBE timed out")??;
            // The subscribe response is the only readiness signal the node gives,
            // and it confirms registration upstream, not that the writer's node
            // already lists this subscriber. A fixed settle covers the rest; the
            // UPDATE is then sent once and must arrive live.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut delta = Ping::default();
            delta.insert(format!("upd-{dir_tag}"));
            update_contract_state(src_client, key, delta).await?;
            wait_for_update_notification(dst_client, key, &format!("upd-{dir_tag}"), UPDATE_TIMEOUT)
                .await
        })
        .await
}

/// Payload with no repeating byte, so truncation, reordering or fragment
/// duplication changes the content, not just the length.
fn large_pattern() -> Vec<u8> {
    (0..LARGE_PADDING)
        .map(|i| ((i.wrapping_mul(131) ^ (i >> 7)) % 251) as u8)
        .collect()
}

/// Large-state streaming round trip, `src` PUT and `dst` GET.
async fn check_large(
    report: &mut Report,
    src_node: &Node<'_>,
    dst_node: &Node<'_>,
    dir_tag: &str,
) -> Result<()> {
    let dir = format!("{} -> {}", src_node.build.label(), dst_node.build.label());
    report
        .timed(&format!("large-state {dir}"), async {
            let mut src = connect_ws_with_retry(
                &format!("{}?encodingProtocol=native&streaming=true", src_node.ws_url),
                &src_node.name,
                30,
            )
            .await
            .context("client-api: streaming WebSocket")?;
            let mut dst = connect_ws_with_retry(
                &format!("{}?encodingProtocol=native&streaming=true", dst_node.ws_url),
                &dst_node.name,
                30,
            )
            .await
            .context("client-api: streaming WebSocket")?;
            let container = contract_for(dir_tag)?;
            let key = container.key();
            let pattern = large_pattern();
            let mut state = Ping::default();
            state.padding = Some(pattern.clone());
            state.insert(format!("large-{dir_tag}"));
            put_state(&mut src, container, &state).await?;
            let got = get_state(&mut dst, key).await?;
            let body = got.padding.as_deref().unwrap_or_default();
            if body != pattern.as_slice() || !got.contains_key(&format!("large-{dir_tag}")) {
                bail!(
                    "large state came back different: {} of {LARGE_PADDING} bytes, content equal: {}",
                    body.len(),
                    body == pattern.as_slice()
                );
            }
            Ok(())
        })
        .await
}

/// Extra node arguments for a build in this run's transport mode.
fn transport_args(b: &Build) -> Vec<&'static str> {
    if quic_mode().unwrap_or(false) && b.version >= NATIVE_QUIC_SINCE {
        vec!["--transport-quic", "prefer", "--quic-data-percent", "100"]
    } else {
        Vec::new()
    }
}

/// The configured transport mode of a node. This is configuration, not
/// observation: nothing in the node reports which transport a pair used.
fn transport_mode(b: &Build) -> &'static str {
    if !transport_args(b).is_empty() {
        "configured: native QUIC preferred, data share 100%"
    } else if b.version >= NATIVE_QUIC_SINCE {
        "configured: node default (hashed per pair)"
    } else {
        "build predates native QUIC (legacy UDP only)"
    }
}

/// The node's own log: files under its data and config directories, plus what it
/// wrote to the container's output.
fn node_log(container: &str) -> String {
    let files = docker(&[
        "exec",
        container,
        "sh",
        "-c",
        "find /data /config -type f -name '*.log*' 2>/dev/null | xargs -r cat",
    ]);
    format!("{files}\n{}", docker(&["logs", container]))
}

fn write_artifacts(report: &Report, nodes: &[Node<'_>], outcome: &Result<()>) {
    let Ok(dir) = std::env::var("FREENET_MIXED_ARTIFACT_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir).join(report.scenario);
    let _ = std::fs::create_dir_all(&dir);
    let summary = json!({
        "scenario": report.scenario,
        "result": match outcome { Ok(()) => "ok".to_string(), Err(e) => format!("{e:#}") },
        "nodes": nodes.iter().map(|n| json!({
            "name": n.name,
            "role": n.build.role,
            "version": n.build.version.to_string(),
            "floor": n.build.floor.to_string(),
            "binary_sha256": n.build.sha256,
            "transport": transport_mode(n.build),
            "container": n.container,
        })).collect::<Vec<_>>(),
        "steps": report.steps.iter().map(|(l, d)| json!({"step": l, "seconds": d.as_secs_f64()})).collect::<Vec<_>>(),
        "notes": report.notes,
    });
    let _ = std::fs::write(
        dir.join("report.json"),
        serde_json::to_string_pretty(&summary).unwrap_or_default(),
    );
    // Best effort: when the network failed to build the containers may be gone,
    // and the harness's preserved node directories carry the logs instead.
    for n in nodes {
        let tag = format!("{}-{}", n.name, n.build.label().replace(['@', '\''], "-"));
        let _ = std::fs::write(
            dir.join(format!("{tag}.docker.log")),
            node_log(&n.container),
        );
        let _ = std::fs::write(
            dir.join(format!("{tag}.inspect.json")),
            docker(&["inspect", &n.container]),
        );
    }
}

async fn run_scenario(spec: Spec<'_>) -> Result<()> {
    let mut report = Report {
        scenario: spec.name,
        steps: Vec::new(),
        notes: Vec::new(),
    };

    // Names and expectations. Container names count the gateway first.
    let config = DockerNatConfig::default();
    let prefix = config.name_prefix.clone();
    let all: Vec<&Build> = std::iter::once(spec.gateway)
        .chain(spec.peers.iter().copied())
        .collect();
    let expect_connected: Vec<bool> = all
        .iter()
        .enumerate()
        .map(|(i, b)| i == 0 || can_connect(spec.gateway, b))
        .collect();
    for (i, (b, ok)) in all.iter().zip(&expect_connected).enumerate() {
        report.note(format!(
            "node {i} {} floor {} {} binary {} sha256 {}: expected to {} the gateway",
            b.label(),
            b.floor,
            transport_mode(b),
            b.binary.display(),
            b.sha256,
            if *ok { "connect to" } else { "be refused by" }
        ));
    }

    let mut nodes: Vec<Node<'_>> = vec![Node {
        name: "gw".into(),
        build: spec.gateway,
        container: format!("{prefix}-gw-0"),
        ws_url: String::new(),
    }];
    for (i, b) in spec.peers.iter().enumerate() {
        nodes.push(Node {
            name: format!("peer{i}"),
            build: b,
            container: format!("{prefix}-peer-{}", i + 1),
            ws_url: String::new(),
        });
    }

    let ratio = expect_connected.iter().filter(|c| **c).count() as f64 / all.len() as f64;
    // The default binary is unused when every node is overridden, and the
    // gateway is overridden below, so the first peer's binary is a harmless default.
    let mut builder = NetworkBuilder::new()
        .backend(Backend::DockerNat(config))
        .gateways(1)
        .peers(spec.peers.len())
        .binary(FreenetBinary::Path(spec.gateway.binary.clone()))
        .gateway_binary(0, FreenetBinary::Path(spec.gateway.binary.clone()))
        // A node that sees a newer release, or a remote floor above its own
        // version, would stage the real release and replace the binary under
        // test. Blackholing GitHub works on binaries that predate any flag.
        .extra_hosts_all(["github.com:127.0.0.1", "api.github.com:127.0.0.1"])
        .require_connectivity(ratio - 0.01)
        .connectivity_timeout(CONNECT_TIMEOUT)
        .preserve_temp_dirs_on_failure(true)
        .gateway_extra_args(0, transport_args(spec.gateway));
    for (i, b) in spec.peers.iter().enumerate() {
        builder = builder
            .peer_binary(i, FreenetBinary::Path(b.binary.clone()))
            .peer_extra_args(i, transport_args(b));
    }

    let network = match builder.build().await {
        Ok(n) => n,
        Err(e) => {
            let err = anyhow!("step=connect: the network did not form: {e}");
            let outcome = Err(anyhow!("{err:#}"));
            write_artifacts(&report, &nodes, &outcome);
            return Err(err);
        }
    };
    nodes[0].ws_url = network.gateway(0).ws_url();
    for i in 0..spec.peers.len() {
        nodes[i + 1].ws_url = network.peer(i).ws_url();
    }

    let outcome = run_checks(&mut report, &nodes, &expect_connected).await;

    write_artifacts(&report, &nodes, &outcome);
    outcome
}

/// Each container must run the binary the test believes it does: PID 1's exe
/// hashes to the build's file, and `--version` names the build's version.
fn verify_running_binaries(report: &mut Report, nodes: &[Node<'_>]) -> Result<()> {
    for n in nodes {
        let got = docker(&["exec", &n.container, "sha256sum", "/proc/1/exe"]);
        let got = got.split_whitespace().next().unwrap_or_default();
        if got != n.build.sha256 {
            bail!(
                "{} {} runs sha256 '{got}', expected {} ({})",
                n.name,
                n.build.label(),
                n.build.sha256,
                n.build.binary.display()
            );
        }
        let version = docker(&["exec", &n.container, "/proc/1/exe", "--version"]);
        let version = version
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        let want = n.build.version.to_string();
        // Exact token, so 0.2.16 cannot match 0.2.164 as a substring.
        if !version.split_whitespace().any(|t| t == want) {
            bail!(
                "{} {} reports version '{version}', expected {}",
                n.name,
                n.build.label(),
                n.build.version
            );
        }
        report.note(format!(
            "{} runs {} ({version}), sha256 verified",
            n.name,
            n.build.label()
        ));
    }
    Ok(())
}

async fn run_checks(
    report: &mut Report,
    nodes: &[Node<'_>],
    expect_connected: &[bool],
) -> Result<()> {
    step("binary-identity", verify_running_binaries(report, nodes))?;

    // Connectivity, from each node's own connected-peer data, polled to a bound.
    report
        .timed("connect", async {
            let deadline = Instant::now() + CONNECT_SETTLE;
            loop {
                // A probe that fails or times out is "no data yet", not a failure:
                // only the deadline fails this step.
                let mut counts = Vec::new();
                for (n, expected) in nodes.iter().zip(expect_connected) {
                    counts.push((n, *expected, probe(&n.ws_url).await?));
                }
                let ready = counts
                    .iter()
                    .all(|(_, expected, c)| !*expected || c.is_some_and(|c| c > 0));
                if ready || Instant::now() >= deadline {
                    for (n, expected, count) in &counts {
                        eprintln!(
                            "  {} {} connections={count:?} expected_connected={expected}",
                            n.name,
                            n.build.label()
                        );
                    }
                    if !ready {
                        bail!("not every node allowed to connect has a connection after {CONNECT_SETTLE:?}");
                    }
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
        .await?;

    // Refusal: the rest of the network is up, so a silent refused node is not a
    // dead network. The refused node must stay unconnected and keep running.
    let refused: Vec<&Node<'_>> = nodes
        .iter()
        .zip(expect_connected)
        .filter(|(_, ok)| !**ok)
        .map(|(n, _)| n)
        .collect();
    if !refused.is_empty() {
        let gateway = nodes[0].build;
        report
            .timed("refusal", async {
                let deadline = Instant::now() + REFUSAL_WINDOW;
                // Hard stop. A refused node whose WebSocket handshake stalls makes
                // every probe run to its outer bound, so counting rounds could
                // outlast the test runner's limit and lose the report; past this
                // instant the step fails normally instead.
                let grace_deadline = deadline + REFUSAL_GRACE;
                // Most recent probe per refused node. The LAST probe of the
                // window must have answered and shown 0 connections: a node that
                // answered 0 early, connected later and then went quiet must not
                // pass.
                let mut last: Vec<Option<usize>> = vec![None; refused.len()];
                loop {
                    for (i, n) in refused.iter().enumerate() {
                        // `probe` errs on a protocol break; that fails the step.
                        let left = grace_deadline.saturating_duration_since(Instant::now());
                        last[i] = probe_within(&n.ws_url, left).await?;
                        if let Some(c) = last[i]
                            && c != 0
                        {
                            bail!(
                                "{} {} should be refused by the gateway {} (floor {}) but has {c} connection(s)",
                                n.name,
                                n.build.label(),
                                gateway.label(),
                                gateway.floor,
                            );
                        }
                        if !container_running(&n.container) {
                            bail!("{} {} exited instead of staying out", n.name, n.build.label());
                        }
                    }
                    // Past the window, keep probing until every refused node's latest
                    // probe has answered, or the grace deadline passes.
                    let answered = last.iter().all(Option::is_some);
                    if (Instant::now() >= deadline && answered) || Instant::now() >= grace_deadline
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                for (n, l) in refused.iter().zip(&last) {
                    if l.is_none() {
                        bail!(
                            "{} {} did not answer its final ConnectedPeers probe",
                            n.name,
                            n.build.label()
                        );
                    }
                }
                // The gateway lists peers by NAT address, which cannot be mapped
                // back to nodes, so check the count from below: every admitted
                // node must be there. The refused node's own view above (no
                // connections) is the absence check. Retried to a bound.
                let allowed = expect_connected.iter().filter(|c| **c).count() - 1;
                let gw_deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    match probe(&nodes[0].ws_url).await? {
                        Some(c) if c >= allowed => return Ok(()),
                        other if Instant::now() >= gw_deadline => bail!(
                            "gateway reports {other:?} connections, fewer than the {allowed} admitted nodes"
                        ),
                        _ => tokio::time::sleep(Duration::from_secs(2)).await,
                    }
                }
            })
            .await?;
    }

    // Operations between connected nodes of different builds, both directions.
    let live: Vec<&Node<'_>> = nodes
        .iter()
        .zip(expect_connected)
        .filter(|(_, ok)| **ok)
        .map(|(n, _)| n)
        .collect();
    let pairs = cross_pairs(&live);
    if pairs.is_empty() {
        bail!("step=setup: no connected pair of a HEAD build and a different build");
    }
    for (i, (a, b)) in pairs.into_iter().enumerate() {
        report.note(format!(
            "operations run between {} {} and {} {}",
            a.name,
            a.build.label(),
            b.name,
            b.build.label()
        ));
        run_pair_ops(report, a, b, i).await?;
    }

    // Nothing may have exited, or a replaced binary would pass unseen.
    for n in nodes {
        if !container_running(&n.container) {
            bail!(
                "step=liveness: {} {} is no longer running",
                n.name,
                n.build.label()
            );
        }
    }
    Ok(())
}

/// Interop operations between `a` (a different build) and `b` (a HEAD build),
/// both directions.
async fn run_pair_ops(report: &mut Report, a: &Node<'_>, b: &Node<'_>, idx: usize) -> Result<()> {
    let mut ca = connect_ws_with_retry(
        &format!("{}?encodingProtocol=native", a.ws_url),
        &a.name,
        30,
    )
    .await
    .context("step=client-api")?;
    let mut cb = connect_ws_with_retry(
        &format!("{}?encodingProtocol=native", b.ws_url),
        &b.name,
        30,
    )
    .await
    .context("step=client-api")?;

    let scenario = format!("{}-p{idx}", report.scenario);
    check_direction(
        report,
        (a, &mut ca),
        (b, &mut cb),
        &format!("{scenario}-ab"),
    )
    .await?;
    check_direction(
        report,
        (b, &mut cb),
        (a, &mut ca),
        &format!("{scenario}-ba"),
    )
    .await?;
    check_large(report, a, b, &format!("{scenario}-large-ab")).await?;
    check_large(report, b, a, &format!("{scenario}-large-ba")).await?;
    Ok(())
}

/// For each distinct non-HEAD build among the connected nodes, a pair (that
/// node, a HEAD node). HEAD nodes that are peers are preferred over the gateway
/// so peer-to-peer links between versions are exercised where the topology has
/// them (whether such a direct link exists is not asserted).
fn cross_pairs<'a>(live: &[&'a Node<'a>]) -> Vec<(&'a Node<'a>, &'a Node<'a>)> {
    let head_node = live.iter().rev().find(|n| n.build.from_head).copied();
    let Some(head_node) = head_node else {
        return Vec::new();
    };
    let mut seen: Vec<&PathBuf> = Vec::new();
    let mut out = Vec::new();
    for n in live.iter().rev().filter(|n| !n.build.from_head) {
        if !seen.contains(&&n.build.binary) {
            seen.push(&n.build.binary);
            out.push((*n, head_node));
        }
    }
    out
}

// ---------------------------------------------------------------- tests

fn require_env() -> Result<()> {
    // Fail loudly if someone removes #[ignore] and runs this without Docker.
    if std::env::var("FREENET_TEST_DOCKER_NAT").is_err() {
        bail!("FREENET_TEST_DOCKER_NAT must be set: this test requires Docker NAT simulation");
    }
    Ok(())
}

/// A: old gateway, new peers join and operate through it.
#[ignore] // Only runs when FREENET_TEST_DOCKER_NAT=1
#[tokio::test(flavor = "multi_thread")] // Docker backend uses block_in_place()
async fn mixed_version_old_gateway() -> Result<()> {
    require_env()?;
    let b = load_builds()?;
    run_scenario(Spec {
        name: "A-old-gateway",
        gateway: &b.n1,
        peers: vec![&b.head, &b.head, &b.n1],
    })
    .await
}

/// B: new gateway, old peers join it and link to new peers.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn mixed_version_new_gateway() -> Result<()> {
    require_env()?;
    let b = load_builds()?;
    run_scenario(Spec {
        name: "B-new-gateway",
        gateway: &b.head,
        peers: vec![&b.n1, &b.n1, &b.head],
    })
    .await
}

/// C: the floor causes the refusal. Control first: with N's own floor, N-1 and
/// N-2 both connect, so N-2 is wire-compatible with this source. Then N', the
/// same source with its floor raised to min(N-1, HEAD), accepts N-1 and refuses
/// N-2. Only the floor constant differs between the two gateways.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn mixed_version_floor() -> Result<()> {
    require_env()?;
    let b = load_builds()?;
    if !can_connect(&b.head, &b.n1) {
        bail!(
            "floor raised above N-1: N {} has floor {}, N-1 is {} with floor {}. \
             This is not a rolling upgrade.",
            b.head.version,
            b.head.floor,
            b.n1.version,
            b.n1.floor
        );
    }
    let Some(n2) = b.n2.as_ref() else {
        return missing("no N-2 release is available for the floor test");
    };
    let Some(nprime) = b.nprime.as_ref() else {
        return missing(
            "FREENET_NPRIME_BINARY_PATH and FREENET_NPRIME_FLOOR are not set, \
             so the floor cannot be shown to cause the refusal",
        );
    };

    // The control is what shows N-2 is wire-compatible with this source, so the
    // N' refusal below is caused by the floor and not by drift. If N's own floor
    // already excludes N-2 the control cannot run, and without it the test proves
    // nothing, so it fails (a lenient local run skips). This turns the nightly red
    // on purpose when a floor raise passes N-2: that raise is exactly the
    // deliberate event this test watches. Read the raise as intended, then either
    // wait for the next release to move N-2 above the floor, or, if the raise is
    // permanent policy, change the test to take the control from an older source.
    if !can_connect(&b.head, n2) {
        return missing(&format!(
            "N's own floor {} already excludes N-2 {}, so the control (N accepts N-2) cannot \
             run and the floor test would prove nothing",
            b.head.floor, n2.version
        ));
    }
    run_scenario(Spec {
        name: "C-control-n-accepts-n2",
        gateway: &b.head,
        peers: vec![&b.n1, n2],
    })
    .await
    .context("control scenario (N with its own floor)")?;

    if can_connect(nprime, n2) {
        bail!(
            "N' (floor {}) would accept N-2 {}: bad setup",
            nprime.floor,
            n2.version
        );
    }
    run_scenario(Spec {
        name: "C-floor-nprime-refuses-n2",
        gateway: nprime,
        peers: vec![&b.n1, n2],
    })
    .await
    .context("floor scenario (N' with floor raised to N-1)")
}

/// A node far below the floor stays out and does not disturb the rest of the
/// network. Not a floor test: that old a node could also fail for wire drift.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn mixed_version_very_old_node() -> Result<()> {
    require_env()?;
    let b = load_builds()?;
    let Some(below) = b.below.as_ref() else {
        return missing(&format!(
            "no published release below N's floor {} has a musl asset with a checksum entry",
            b.head.floor
        ));
    };
    run_scenario(Spec {
        name: "D-very-old-node",
        gateway: &b.head,
        peers: vec![&b.n1, below],
    })
    .await
}

// ---------------------------------------------------------------- unit tests

#[cfg(test)]
mod tests {
    use super::*;

    fn build(v: &str, f: &str) -> Build {
        Build {
            role: "N",
            from_head: true,
            version: Version::parse(v).unwrap(),
            floor: Version::parse(f).unwrap(),
            binary: PathBuf::new(),
            sha256: String::new(),
        }
    }

    #[test]
    fn versions_compare_numerically() {
        assert!(Version::parse("0.2.100").unwrap() > Version::parse("0.2.99").unwrap());
        assert!(Version::parse("1.0.0").unwrap() > Version::parse("0.2.999").unwrap());
        assert!(Version::parse("0.2").is_err());
        assert!(Version::parse("0.2.3.4").is_err());
    }

    #[test]
    fn connect_rule_uses_both_floors() {
        let new = build("0.2.166", "0.2.165");
        let n1 = build("0.2.165", "0.2.122");
        let n2 = build("0.2.164", "0.2.122");
        assert!(can_connect(&new, &n1));
        assert!(!can_connect(&new, &n2), "below N's floor is refused");
        assert!(!can_connect(&n2, &new), "the rule is symmetric");
        // An old node whose own floor is above N refuses N.
        let future = build("0.2.170", "0.2.169");
        assert!(!can_connect(&new, &future));
    }

    #[test]
    fn floor_is_read_from_metadata_or_falls_back() {
        let with = "[package]\nversion = \"0.2.163\"\n[package.metadata.freenet]\nmin-compatible-version = \"0.2.122\"\n";
        assert_eq!(
            version_and_floor(with).unwrap(),
            (Version(0, 2, 163), Version(0, 2, 122))
        );
        let without = "[package]\nversion = \"0.2.150\"\n";
        assert_eq!(
            version_and_floor(without).unwrap(),
            (Version(0, 2, 150), Version(0, 2, 150))
        );
    }

    #[test]
    fn the_checked_in_manifest_parses() {
        let core = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../crates/core/Cargo.toml");
        let (v, f) = version_and_floor(&std::fs::read_to_string(core).unwrap()).unwrap();
        assert!(f <= v, "floor {f} above version {v}");
    }

    #[test]
    fn large_pattern_has_no_short_period() {
        let p = large_pattern();
        assert_eq!(p.len(), LARGE_PADDING);
        // Not uniform, and not a short repeating block either.
        assert!(p.iter().any(|b| *b != p[0]));
        assert_ne!(p[..4096], p[4096..8192]);
    }
}
