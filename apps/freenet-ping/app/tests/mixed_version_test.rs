//! Mixed-version network test: the current build (N) and the previous
//! published releases (N-1, N-2) run in one Docker NAT network and must
//! interoperate. The 1.0 transition is not a flag day, so a rolling upgrade has
//! to work in both orders (old gateway with new peers, new gateway with old
//! peers) and a deliberate floor raise has to refuse old nodes cleanly.
//!
//! Scenarios (each is its own network):
//! - A: gateway N-1, peers [N, N, N-1].
//! - B: gateway N, peers [N-1, N-1, N].
//! - C: gateway N, peers [N-1, N-2]. N-2 must be refused if and only if N's
//!   handshake floor is above N-2's version (or N-2's floor above N).
//!
//! Assertions are on what the network delivers (connections, PUT/GET,
//! SUBSCRIBE+UPDATE in both directions, large-state streaming), never on log
//! lines. Which transport each pair used is recorded in the output, not
//! asserted, so a run that never exercised native QUIC is visible.
//!
//! Versions and floors are never hard-coded: N comes from `crates/core/Cargo.toml`,
//! N-1 and N-2 from the manifest written by `scripts/fetch-release-binary.sh`.
//!
//! Requires: FREENET_TEST_DOCKER_NAT=1, FREENET_BINARY_PATH (the N binary),
//! FREENET_MIXED_VERSIONS (the manifest), a Docker daemon. Run the three tests
//! one at a time (`--test-threads 1`): each owns a Docker network. Optional:
//! FREENET_MIXED_ARTIFACT_DIR receives per-scenario reports and container logs.

mod common;

use anyhow::{Context, Result, anyhow, bail};
use common::{
    APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT, connect_ws_with_retry, get_contract_state,
    load_contract, subscribe_to_contract, update_contract_state, wait_for_ping_entry,
};
use freenet_ping_app::ping_client::wait_for_put_response;
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, HostResponse, NodeQuery, QueryResponse, WebApi},
    prelude::*,
};
use freenet_test_network::{Backend, DockerNatConfig, FreenetBinary, NetworkBuilder, TestNetwork};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

/// Wait for the whole network to form. Doubled from the single-version NAT test.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a subscriber may take to observe an UPDATE. The update is sent once,
/// so a repair path cannot hide a broken live delivery behind a long window.
const UPDATE_TIMEOUT: Duration = Duration::from_secs(40);
/// Bound on one PUT or GET round trip.
const OP_TIMEOUT: Duration = Duration::from_secs(90);
/// Window in which a refused node must stay unconnected, after the rest of the
/// network is demonstrably up.
const REFUSAL_WINDOW: Duration = Duration::from_secs(60);
/// Entries added to the ping state, in bytes of padding. Above the 64 KiB
/// streaming threshold and above the 512 KiB single-message size.
const LARGE_PADDING: usize = 600 * 1024;

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
    /// "N", "N-1" or "N-2".
    role: &'static str,
    version: Version,
    /// Lowest version this build accepts in the handshake.
    floor: Version,
    binary: PathBuf,
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

struct Builds {
    head: Build,
    n1: Build,
    n2: Option<Build>,
    /// Newest published release strictly below N's floor, if one can be fetched.
    below: Option<Build>,
}

fn load_builds() -> Result<Builds> {
    let core_toml = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../crates/core/Cargo.toml");
    let (version, floor) = version_and_floor(
        &std::fs::read_to_string(&core_toml)
            .with_context(|| format!("reading {}", core_toml.display()))?,
    )?;
    let head = Build {
        role: "N",
        version,
        floor,
        binary: std::env::var("FREENET_BINARY_PATH")
            .context("FREENET_BINARY_PATH must point to the N binary")?
            .into(),
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
        Ok(Some(Build {
            role,
            version: Version::parse(get("version")?)?,
            floor: Version::parse(get("floor")?)?,
            binary: get("path")?.into(),
        }))
    };
    let n1 = from_manifest("n1", "N-1")?.ok_or_else(|| anyhow!("manifest has no n1"))?;
    let n2 = from_manifest("n2", "N-2")?;
    let below = from_manifest("below", "below-floor")?;
    if let Some(b) = &below {
        // The manifest picks it by N's floor; a mismatch means a stale manifest.
        if b.version >= head.floor {
            bail!(
                "below-floor build {} is not below N's floor {}: stale manifest",
                b.version,
                head.floor
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
        if !b.binary.is_file() {
            bail!(
                "{}: binary {} does not exist",
                b.label(),
                b.binary.display()
            );
        }
    }
    Ok(Builds {
        head,
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

async fn connected_peer_count(ws_url: &str) -> Result<usize> {
    let mut client =
        connect_ws_with_retry(&format!("{ws_url}?encodingProtocol=native"), "probe", 30)
            .await
            .context("client-api: cannot open a WebSocket")?;
    client
        .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
        .await?;
    let resp = tokio::time::timeout(Duration::from_secs(15), client.recv())
        .await
        .context("client-api: no reply to ConnectedPeers")??;
    match resp {
        HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }) => Ok(peers.len()),
        other => bail!("client-api: unexpected reply to ConnectedPeers: {other:?}"),
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
/// Cross-version PUT then GET, then SUBSCRIBE at `dst` and an UPDATE from `src`.
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
            // The subscription reply confirms registration at the subscriber's
            // upstream; give it time to reach the writer's node before the single
            // UPDATE. Bounded, and measured: see the run timings.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut delta = Ping::default();
            delta.insert(format!("upd-{dir_tag}"));
            update_contract_state(src_client, key, delta).await?;
            wait_for_ping_entry(dst_client, key, &format!("upd-{dir_tag}"), UPDATE_TIMEOUT)
                .await
                .map(|_| ())
        })
        .await
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
            let mut state = Ping::with_padding(LARGE_PADDING);
            state.insert(format!("large-{dir_tag}"));
            put_state(&mut src, container, &state).await?;
            let got = get_state(&mut dst, key).await?;
            let len = got.padding.as_ref().map_or(0, Vec::len);
            if len != LARGE_PADDING || !got.contains_key(&format!("large-{dir_tag}")) {
                bail!("large state came back damaged: padding {len} of {LARGE_PADDING} bytes");
            }
            Ok(())
        })
        .await
}

/// Per-node facts for the report: which binary, what it connected to. Which
/// transport each node used is read from the node's own log, as evidence for a
/// human; nothing asserts on it.
fn transport_evidence(net: &TestNetwork, nodes: &[Node<'_>]) -> Vec<String> {
    let mut out = Vec::new();
    let peers: Vec<_> = (0..nodes.len() - 1).map(|i| net.peer(i)).collect();
    let logs = std::iter::once(net.gateway(0)).chain(peers);
    for (node, peer) in nodes.iter().zip(logs) {
        let text = std::fs::read_to_string(peer.log_path()).unwrap_or_default();
        let count = |needle: &str| {
            text.lines()
                .filter(|l| l.to_ascii_lowercase().contains(needle))
                .count()
        };
        out.push(format!(
            "{} {}: log lines mentioning quic={}, native={}, udp={}",
            node.name,
            node.build.label(),
            count("quic"),
            count("native"),
            count("udp"),
        ));
    }
    out
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
            "container": n.container,
        })).collect::<Vec<_>>(),
        "steps": report.steps.iter().map(|(l, d)| json!({"step": l, "seconds": d.as_secs_f64()})).collect::<Vec<_>>(),
        "notes": report.notes,
    });
    let _ = std::fs::write(
        dir.join("report.json"),
        serde_json::to_string_pretty(&summary).unwrap_or_default(),
    );
    for n in nodes {
        let tag = format!("{}-{}", n.name, n.build.label().replace('@', "-"));
        let _ = std::fs::write(
            dir.join(format!("{tag}.docker.log")),
            docker(&["logs", &n.container]),
        );
        let _ = std::fs::write(
            dir.join(format!("{tag}.inspect.json")),
            docker(&["inspect", &n.container]),
        );
    }
}

async fn run_scenario(spec: Spec<'_>, head: &Build) -> Result<()> {
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
            "node {i} {} floor {} binary {}: expected to {} the gateway",
            b.label(),
            b.floor,
            b.binary.display(),
            if *ok { "connect to" } else { "be refused by" }
        ));
    }

    let ratio = expect_connected.iter().filter(|c| **c).count() as f64 / all.len() as f64;
    let mut builder = NetworkBuilder::new()
        .backend(Backend::DockerNat(config))
        .gateways(1)
        .peers(spec.peers.len())
        .binary(FreenetBinary::Path(head.binary.clone()))
        .gateway_binary(0, FreenetBinary::Path(spec.gateway.binary.clone()))
        // A node that sees a newer release, or a remote floor above its own
        // version, would stage the real release and replace the binary under
        // test. Blackholing GitHub works on binaries that predate any flag.
        .extra_hosts_all(["github.com:127.0.0.1", "api.github.com:127.0.0.1"])
        .require_connectivity(ratio - 0.01)
        .connectivity_timeout(CONNECT_TIMEOUT)
        .preserve_temp_dirs_on_failure(true);
    for (i, b) in spec.peers.iter().enumerate() {
        builder = builder.peer_binary(i, FreenetBinary::Path(b.binary.clone()));
    }

    let network = builder
        .build()
        .await
        .map_err(|e| anyhow!("step=connect: the network did not form: {e}"))?;

    let mut nodes: Vec<Node<'_>> = vec![Node {
        name: "gw".into(),
        build: spec.gateway,
        container: format!("{prefix}-gw-0"),
        ws_url: network.gateway(0).ws_url(),
    }];
    for (i, b) in spec.peers.iter().enumerate() {
        nodes.push(Node {
            name: format!("peer{i}"),
            build: b,
            container: format!("{prefix}-peer-{}", i + 1),
            ws_url: network.peer(i).ws_url(),
        });
    }

    let outcome = run_checks(&mut report, &nodes, &expect_connected, head).await;

    for line in transport_evidence(&network, &nodes) {
        report.note(line);
    }
    write_artifacts(&report, &nodes, &outcome);
    outcome
}

async fn run_checks(
    report: &mut Report,
    nodes: &[Node<'_>],
    expect_connected: &[bool],
    head: &Build,
) -> Result<()> {
    // Connectivity, from each node's own connected-peer data.
    report
        .timed("connect", async {
            let mut counts = Vec::new();
            for (n, expected) in nodes.iter().zip(expect_connected) {
                let count = connected_peer_count(&n.ws_url).await?;
                counts.push((n, *expected, count));
            }
            for (n, expected, count) in &counts {
                eprintln!(
                    "  {} {} connections={count} expected_connected={expected}",
                    n.name,
                    n.build.label()
                );
            }
            for (n, expected, count) in &counts {
                if *expected && *count == 0 {
                    bail!(
                        "{} {} has no connections but is allowed to connect",
                        n.name,
                        n.build.label()
                    );
                }
            }
            Ok(())
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
        report
            .timed("floor-refusal", async {
                let deadline = Instant::now() + REFUSAL_WINDOW;
                while Instant::now() < deadline {
                    for n in &refused {
                        let c = connected_peer_count(&n.ws_url).await?;
                        if c != 0 {
                            bail!(
                                "{} {} should be refused (floor {} / version {}) but has {c} connection(s)",
                                n.name,
                                n.build.label(),
                                head.floor,
                                n.build.version
                            );
                        }
                        if !container_running(&n.container) {
                            bail!(
                                "{} {} exited instead of being refused cleanly",
                                n.name,
                                n.build.label()
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                // The gateway must have admitted the control peers only.
                let allowed = expect_connected.iter().filter(|c| **c).count() - 1;
                let gw_count = connected_peer_count(&nodes[0].ws_url).await?;
                if gw_count != allowed {
                    bail!("gateway reports {gw_count} connections, expected {allowed}");
                }
                Ok(())
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
        bail!("step=setup: no connected pair of N and an older build in this scenario");
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

/// Interop operations between `a` (an older build) and `b` (N), both directions.
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

/// For each distinct older build among the connected nodes, a pair (older node, N
/// node). N nodes that are peers are preferred over the gateway so peer-to-peer
/// links between versions are exercised where the topology has them.
fn cross_pairs<'a>(live: &[&'a Node<'a>]) -> Vec<(&'a Node<'a>, &'a Node<'a>)> {
    let is_head = |n: &Node<'_>| n.build.role == "N";
    let head_node = live.iter().rev().find(|n| is_head(n)).copied();
    let Some(head_node) = head_node else {
        return Vec::new();
    };
    let mut seen: Vec<&PathBuf> = Vec::new();
    let mut out = Vec::new();
    for n in live.iter().rev().filter(|n| !is_head(n)) {
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
    run_scenario(
        Spec {
            name: "A-old-gateway",
            gateway: &b.n1,
            peers: vec![&b.head, &b.head, &b.n1],
        },
        &b.head,
    )
    .await
}

/// B: new gateway, old peers join it and link to new peers.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn mixed_version_new_gateway() -> Result<()> {
    require_env()?;
    let b = load_builds()?;
    run_scenario(
        Spec {
            name: "B-new-gateway",
            gateway: &b.head,
            peers: vec![&b.n1, &b.n1, &b.head],
        },
        &b.head,
    )
    .await
}

/// C: the floor. N-1 must always be accepted by N. N-2 is refused if and only
/// if the floors say so, and the refusal must be clean.
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
    // The control peer N-1 first, then N-2 (accepted when at or above the floor),
    // then the newest release below the floor (refused). The below-floor build
    // is what exercises the refusal path while the floor is still low.
    let mut peers = vec![&b.n1];
    peers.extend(b.n2.as_ref());
    match b.below.as_ref() {
        Some(below) => peers.push(below),
        None => eprintln!(
            "NOTE scenario C: no published release below N's floor {} has a verified \
             musl asset, so the refusal path is exercised only if N-2 is itself below the floor",
            b.head.floor
        ),
    }
    if peers.len() == 1 {
        eprintln!("SKIPPED scenario C: there is no N-2 or below-floor release to test");
        return Ok(());
    }
    run_scenario(
        Spec {
            name: "C-floor",
            gateway: &b.head,
            peers,
        },
        &b.head,
    )
    .await
}

// ---------------------------------------------------------------- unit tests

#[cfg(test)]
mod tests {
    use super::*;

    fn build(v: &str, f: &str) -> Build {
        Build {
            role: "N",
            version: Version::parse(v).unwrap(),
            floor: Version::parse(f).unwrap(),
            binary: PathBuf::new(),
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
}
