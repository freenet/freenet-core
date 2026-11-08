use std::net::{Ipv4Addr, SocketAddr};

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use super::*;
use crate::contract::delegate_capabilities::{
    CapabilityStorage, FlakyCapabilityStorage, MemoryCapabilityStorage,
};
use crate::server::HostedMode;

const HOST: &str = "127.0.0.1:7509";
const SELF_ORIGIN: &str = "http://127.0.0.1:7509";

fn app_id() -> ContractInstanceId {
    ContractInstanceId::new([5u8; 32])
}

fn new_state() -> (AppGrantState, Arc<DelegateCapabilities>) {
    let caps = DelegateCapabilities::new(Arc::new(MemoryCapabilityStorage::default()));
    (AppGrantState::with_capabilities(caps.clone()), caps)
}

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 50000))
}

fn router_with(state: &AppGrantState, hosted: bool, peer: SocketAddr) -> Router {
    routes()
        .layer(Extension(state.clone()))
        .layer(Extension(HostedMode(hosted)))
        .layer(Extension(ConnectInfo(peer)))
}

fn router(state: &AppGrantState) -> Router {
    router_with(state, false, loopback())
}

async fn send(router: &Router, req: Request<Body>) -> (StatusCode, String) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
}

fn create_req_named(name: &str) -> Request<Body> {
    Request::post(ROUTE_REQUEST)
        .header("host", HOST)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "app": app_id().to_string(), "client_name": name }).to_string(),
        ))
        .unwrap()
}

/// A request as a native client sends it: loopback Host, no Origin, no
/// `Sec-Fetch-*`.
fn create_req() -> Request<Body> {
    create_req_named("River <b>desktop</b>")
}

fn poll_req(request_id: &str) -> Request<Body> {
    Request::get(format!("/v1/app-grants/request/{request_id}"))
        .header("host", HOST)
        .body(Body::empty())
        .unwrap()
}

fn page_req(approve_id: &str) -> Request<Body> {
    Request::get(format!("/v1/app-grants/approve/{approve_id}"))
        .header("host", HOST)
        .body(Body::empty())
        .unwrap()
}

fn approve_req(approve_id: &str, nonce: &str, allow: bool, origin: Option<&str>) -> Request<Body> {
    let mut b = Request::post(format!("/v1/app-grants/approve/{approve_id}"))
        .header("host", HOST)
        .header("content-type", "application/json");
    if let Some(o) = origin {
        b = b.header("origin", o);
    }
    b.body(Body::from(
        serde_json::json!({ "nonce": nonce, "allow": allow }).to_string(),
    ))
    .unwrap()
}

fn list_req() -> Request<Body> {
    Request::get(ROUTE_LIST)
        .header("host", HOST)
        .body(Body::empty())
        .unwrap()
}

fn revoke_req(id: &str, origin: Option<&str>) -> Request<Body> {
    let mut b = Request::post(ROUTE_REVOKE)
        .header("host", HOST)
        .header("content-type", "application/json");
    if let Some(o) = origin {
        b = b.header("origin", o);
    }
    b.body(Body::from(serde_json::json!({ "id": id }).to_string()))
        .unwrap()
}

/// Make a request; returns `(request_id, approve_id)`.
async fn create(router: &Router) -> (String, String) {
    let (status, body) = send(router, create_req()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v = json(&body);
    let request_id = v["request_id"].as_str().unwrap().to_string();
    let url = v["approve_url"].as_str().unwrap();
    let approve_id = url
        .strip_prefix("http://127.0.0.1:7509/v1/app-grants/approve/")
        .expect("approve_url names the node and the page")
        .to_string();
    assert_ne!(
        request_id, approve_id,
        "the page URL must not carry the poll id"
    );
    (request_id, approve_id)
}

fn nonce_of(html: &str) -> String {
    let start = html.find("data-nonce=\"").expect("page carries a nonce") + "data-nonce=\"".len();
    html[start..start + 64].to_string()
}

/// Render the approval page; returns its nonce.
async fn open_page(router: &Router, approve_id: &str) -> String {
    let (status, html) = send(router, page_req(approve_id)).await;
    assert_eq!(status, StatusCode::OK, "{html}");
    nonce_of(&html)
}

async fn approve(router: &Router, approve_id: &str) {
    let nonce = open_page(router, approve_id).await;
    let (status, body) = send(
        router,
        approve_req(approve_id, &nonce, true, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// Full flow up to the first collection; returns `(request_id, token)`.
async fn granted(router: &Router) -> (String, String) {
    let (request_id, approve_id) = create(router).await;
    approve(router, &approve_id).await;
    let (status, body) = send(router, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let token = json(&body)["token"].as_str().unwrap().to_string();
    (request_id, token)
}

async fn resolve(state: &AppGrantState, token: &str) -> GrantLookup {
    state
        .resolve_connection(&AuthToken::from(token.to_string()))
        .await
}

fn fill_grants(caps: &DelegateCapabilities) {
    while caps.native_grant_count().unwrap() < MAX_NATIVE_GRANTS {
        assert!(
            caps.create_native_grant(
                GrantToken::generate().hash(),
                AppIdentity::WebApp(app_id()),
                "x"
            )
            .unwrap()
        );
    }
}

#[tokio::test]
async fn request_approve_poll_issues_the_token_and_stores_only_its_hash() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;

    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v = json(&body);
    assert_eq!(v["status"], "approved");
    assert_eq!(v["app"], app_id().to_string());
    let token = v["token"].as_str().unwrap().to_string();
    assert_eq!(bs58::decode(&token).into_vec().unwrap().len(), 32);

    // Stored by hash, for the right app, with the name as given (display only).
    let grants = caps.native_grants().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].0, GrantTokenHash::of(&token));
    assert_eq!(grants[0].1.app, AppIdentity::WebApp(app_id()));
    assert_eq!(grants[0].1.client_name, "River <b>desktop</b>");
}

/// A lost poll response is recoverable: the request holder gets the SAME
/// token again (stored once), until the token is first used to connect.
#[tokio::test]
async fn token_can_be_re_collected_until_first_use() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, token) = granted(&r).await;

    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(json(&body)["token"], token.as_str(), "the same token");
    assert_eq!(caps.native_grant_count().unwrap(), 1, "stored once");

    // First use ends re-collection.
    let GrantLookup::Granted(live) = resolve(&state, &token).await else {
        panic!("the grant resolves");
    };
    drop(live);
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!body.contains(&token));
}

/// Wait until `n` polls are waiting on the request's change channel. `false`
/// if that does not happen within `within`.
async fn wait_for_waiters(
    state: &AppGrantState,
    request_id: &str,
    n: usize,
    within: Duration,
) -> bool {
    let deadline = Instant::now() + within;
    loop {
        let waiting = state
            .0
            .pending
            .lock()
            .get(request_id)
            .map_or(0, |p| p.changed.receiver_count());
        if waiting >= n {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// A storage hook that reports when it is reached and then blocks until
/// released. Returns `(hook, reached, release)`.
#[allow(clippy::type_complexity)]
fn blocking_hook() -> (
    Box<dyn FnOnce() + Send>,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let hook = Box::new(move || {
        reached_tx.send(()).ok();
        release_rx.recv_timeout(Duration::from_secs(10)).ok();
    });
    (hook, reached_rx, release_tx)
}

async fn wait_reached(reached: std::sync::mpsc::Receiver<()>) {
    tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .expect("the storage hook was reached");
}

fn flaky_state() -> (
    AppGrantState,
    Arc<DelegateCapabilities>,
    Arc<FlakyCapabilityStorage>,
) {
    let storage = Arc::new(FlakyCapabilityStorage::default());
    let caps = DelegateCapabilities::new(storage.clone());
    (
        AppGrantState::with_capabilities(caps.clone()),
        caps,
        storage,
    )
}

fn approved_hash(state: &AppGrantState, request_id: &str) -> GrantTokenHash {
    match &state.0.pending.lock()[request_id].outcome {
        Outcome::Approved { token, .. } => token.hash(),
        Outcome::Pending | Outcome::Denied => panic!("approved"),
    }
}

/// Four polls race for one approval while the first writer is held inside
/// the store: the others wait on it, and the grant is written exactly once.
/// Multi-thread, so the write really runs on the blocking pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_polls_get_the_same_token_and_store_it_once() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    let (hook, reached, release) = blocking_hook();
    *storage.after_native_get.lock() = Some(Box::new(move |_| hook()));

    let first = {
        let r = r.clone();
        let id = request_id.clone();
        tokio::spawn(async move { send(&r, poll_req(&id)).await })
    };
    wait_reached(reached).await;
    let others: Vec<_> = (0..3)
        .map(|_| {
            let r = r.clone();
            let id = request_id.clone();
            tokio::spawn(async move { send(&r, poll_req(&id)).await })
        })
        .collect();
    // With the `storing` mark the other three wait on the first writer; give
    // up after a while so a regression fails on the insert count below rather
    // than hanging.
    let waited = wait_for_waiters(&state, &request_id, 3, Duration::from_secs(10)).await;
    release.send(()).ok();
    let mut tokens = std::collections::HashSet::new();
    for p in std::iter::once(first).chain(others) {
        let (status, body) = tokio::time::timeout(Duration::from_secs(10), p)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        tokens.insert(json(&body)["token"].as_str().unwrap().to_string());
    }
    assert!(waited, "the other polls waited on the first writer");
    assert_eq!(tokens.len(), 1, "every poll sees the one token");
    assert_eq!(
        storage
            .native_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "written exactly once"
    );
    assert_eq!(caps.native_grant_count().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn poll_waits_then_reports_pending() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, _) = create(&r).await;
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(json(&body)["status"], "pending");
}

#[tokio::test]
async fn a_waiting_poll_is_woken_by_the_approval() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    let poll = {
        let r = r.clone();
        tokio::spawn(async move { send(&r, poll_req(&request_id)).await })
    };
    // Let the poll subscribe before answering.
    tokio::task::yield_now().await;
    approve(&r, &approve_id).await;
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("the approval wakes the poll well before POLL_WAIT")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(json(&body)["status"], "approved");
}

#[tokio::test(start_paused = true)]
async fn concurrent_polls_are_capped_and_slots_are_released() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, _) = create(&r).await;
    state.0.polls.store(MAX_CONCURRENT_POLLS, Ordering::SeqCst);
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(state.0.polls.load(Ordering::SeqCst), MAX_CONCURRENT_POLLS);
    state.0.polls.store(0, Ordering::SeqCst);
    // A poll that waits out POLL_WAIT gives its slot back.
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::ACCEPTED
    );
    assert_eq!(state.0.polls.load(Ordering::SeqCst), 0);
    // So does a refused one.
    assert_eq!(send(&r, poll_req("unknown")).await.0, StatusCode::NOT_FOUND);
    assert_eq!(state.0.polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn deny_is_reported_and_stores_nothing() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    let nonce = open_page(&r, &approve_id).await;
    let (status, _) = send(
        &r,
        approve_req(&approve_id, &nonce, false, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["status"], "denied");
    assert!(json(&body).get("token").is_none());
    assert!(caps.native_grants().unwrap().is_empty());
    // A lost denial can be collected again, like an approval.
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["status"], "denied");
}

#[tokio::test]
async fn request_and_poll_refuse_browser_requests() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, _) = create(&r).await;

    for header in [("origin", SELF_ORIGIN), ("sec-fetch-mode", "cors")] {
        let mut req = create_req();
        req.headers_mut()
            .insert(header.0, header.1.parse().unwrap());
        assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN, "{header:?}");
        let mut req = poll_req(&request_id);
        req.headers_mut()
            .insert(header.0, header.1.parse().unwrap());
        assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN, "{header:?}");
    }
    // Even `Origin: null`.
    let mut req = create_req();
    req.headers_mut().insert("origin", "null".parse().unwrap());
    assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN);
}

/// One request of every route, each valid in every respect the test does not
/// vary (real ids, a real page nonce, the node's Origin, a real grant id), so
/// a refusal can only come from the property under test.
async fn every_route(r: &Router) -> Vec<(&'static str, Request<Body>)> {
    let (request_id, _) = create(r).await;
    // Separate requests for the page and the POST: rendering the page rotates
    // its nonce, which would make the POST fail for the wrong reason.
    let (_, page_id) = create(r).await;
    let (_, approve_id) = create(r).await;
    let nonce = open_page(r, &approve_id).await;
    let (_, token) = granted(r).await;
    let grant_id = GrantTokenHash::of(&token).to_hex();
    vec![
        ("request", create_req()),
        ("poll", poll_req(&request_id)),
        ("page", page_req(&page_id)),
        (
            "approve",
            approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
        ),
        ("list", list_req()),
        ("revoke", revoke_req(&grant_id, Some(SELF_ORIGIN))),
    ]
}

#[tokio::test]
async fn every_route_refuses_a_non_loopback_peer() {
    let (state, caps) = new_state();
    let lan = router_with(&state, false, SocketAddr::from(([192, 168, 1, 7], 50000)));
    for (name, req) in every_route(&router(&state)).await {
        assert_eq!(send(&lan, req).await.0, StatusCode::FORBIDDEN, "{name}");
    }
    assert_eq!(caps.native_grant_count().unwrap(), 1, "nothing revoked");
}

#[tokio::test]
async fn every_route_is_refused_on_a_hosted_node() {
    let (state, caps) = new_state();
    let hosted = router_with(&state, true, loopback());
    for (name, req) in every_route(&router(&state)).await {
        assert_eq!(send(&hosted, req).await.0, StatusCode::FORBIDDEN, "{name}");
    }
    assert_eq!(caps.native_grant_count().unwrap(), 1, "nothing revoked");
}

#[tokio::test]
async fn every_route_refuses_forwarded_requests() {
    for header in [
        "x-forwarded-for",
        "x-forwarded-proto",
        "x-forwarded-host",
        "forwarded",
        "x-real-ip",
    ] {
        let (state, caps) = new_state();
        let r = router(&state);
        for (name, mut req) in every_route(&r).await {
            req.headers_mut().insert(header, "1.2.3.4".parse().unwrap());
            assert_eq!(
                send(&r, req).await.0,
                StatusCode::FORBIDDEN,
                "{name} {header}"
            );
        }
        assert_eq!(caps.native_grant_count().unwrap(), 1, "nothing revoked");
    }
}

/// A DNS-rebound page reaches the node with its own name as `Host`, and with a
/// matching `Origin`, so origin equality alone would pass.
#[tokio::test]
async fn every_route_refuses_a_non_loopback_host() {
    let (state, caps) = new_state();
    let r = router(&state);
    for (name, mut req) in every_route(&r).await {
        req.headers_mut()
            .insert("host", "evil.example:7509".parse().unwrap());
        if req.headers().contains_key("origin") {
            req.headers_mut()
                .insert("origin", "http://evil.example:7509".parse().unwrap());
        }
        assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN, "{name}");
    }
    assert_eq!(caps.native_grant_count().unwrap(), 1, "nothing revoked");
}

/// The controls of each property, positively: the requests `every_route`
/// builds succeed when nothing is changed, so the refusals above are caused
/// by the property each test varies.
#[tokio::test]
async fn every_route_succeeds_unmodified() {
    let (state, caps) = new_state();
    let r = router(&state);
    for (name, req) in every_route(&r).await {
        let status = if name == "poll" {
            // Pending: the long poll would wait; check it is not refused.
            let fut = send(&r, req);
            match tokio::time::timeout(Duration::from_millis(50), fut).await {
                Ok((status, _)) => status,
                Err(_) => StatusCode::ACCEPTED,
            }
        } else {
            send(&r, req).await.0
        };
        assert!(
            status.is_success(),
            "{name} should succeed unmodified, got {status}"
        );
    }
    assert_eq!(caps.native_grant_count().unwrap(), 0, "revoke worked");
}

#[tokio::test]
async fn approve_requires_the_nodes_exact_origin() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    for origin in [
        None,
        Some("null"),
        // Another local web server: localhost, wrong port.
        Some("http://127.0.0.1:8080"),
        Some("http://localhost:7509"),
        // Right authority, wrong scheme: the node serves plain HTTP here.
        Some("https://127.0.0.1:7509"),
    ] {
        let nonce = open_page(&r, &approve_id).await;
        let (status, _) = send(&r, approve_req(&approve_id, &nonce, true, origin)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{origin:?}");
    }
    // Still unanswered.
    let poll =
        tokio::time::timeout(Duration::from_millis(50), send(&r, poll_req(&request_id))).await;
    assert!(poll.is_err(), "the request must still be pending");
}

#[tokio::test]
async fn approval_page_refuses_framed_and_cross_site_loads() {
    let (state, _) = new_state();
    let r = router(&state);
    let (_, approve_id) = create(&r).await;
    for (name, value) in [
        ("sec-fetch-dest", "iframe"),
        ("sec-fetch-dest", "empty"),
        ("sec-fetch-site", "cross-site"),
        ("sec-fetch-site", "same-site"),
    ] {
        let mut req = page_req(&approve_id);
        req.headers_mut().insert(name, value.parse().unwrap());
        assert_eq!(
            send(&r, req).await.0,
            StatusCode::FORBIDDEN,
            "{name}: {value}"
        );
    }
    // Opened directly from another program, as intended.
    let mut req = page_req(&approve_id);
    req.headers_mut()
        .insert("sec-fetch-dest", "document".parse().unwrap());
    req.headers_mut()
        .insert("sec-fetch-site", "none".parse().unwrap());
    assert_eq!(send(&r, req).await.0, StatusCode::OK);
}

#[tokio::test]
async fn approval_nonce_is_single_use_and_tied_to_the_newest_page() {
    let (state, _) = new_state();
    let r = router(&state);
    let (_, approve_id) = create(&r).await;

    // Approving without ever rendering the page fails.
    let (status, _) = send(
        &r,
        approve_req(&approve_id, &"0".repeat(64), true, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A wrong nonce burns the page's nonce too.
    let nonce = open_page(&r, &approve_id).await;
    let (status, _) = send(
        &r,
        approve_req(&approve_id, "wrong", true, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &r,
        approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Re-rendering replaces the nonce.
    let old = open_page(&r, &approve_id).await;
    let new = open_page(&r, &approve_id).await;
    let (status, _) = send(&r, approve_req(&approve_id, &old, true, Some(SELF_ORIGIN))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let new2 = open_page(&r, &approve_id).await;
    assert_ne!(new, new2);
    let (status, _) = send(&r, approve_req(&approve_id, &new2, true, Some(SELF_ORIGIN))).await;
    assert_eq!(status, StatusCode::OK);

    // Answered: the page and a second answer are refused.
    let (status, _) = send(&r, page_req(&approve_id)).await;
    assert_eq!(status, StatusCode::GONE);
    let (status, _) = send(
        &r,
        approve_req(&approve_id, &new2, false, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test(start_paused = true)]
async fn an_approved_but_uncollected_request_expires() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    tokio::time::advance(REQUEST_TTL + Duration::from_secs(1)).await;
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(caps.native_grants().unwrap().is_empty());
}

/// The POST uses the nonce of a page rendered BEFORE expiry, with no request
/// in between that could sweep the request first.
#[tokio::test(start_paused = true)]
async fn approving_an_expired_request_is_refused() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    let nonce = open_page(&r, &approve_id).await;
    tokio::time::advance(REQUEST_TTL + Duration::from_secs(1)).await;
    let (status, _) = send(
        &r,
        approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
    let (status, _) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(caps.native_grants().unwrap().is_empty());
    let (status, _) = send(&r, page_req(&approve_id)).await;
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test]
async fn unknown_ids_are_refused() {
    let (state, _) = new_state();
    let r = router(&state);
    assert_eq!(
        send(&r, poll_req(&"a".repeat(64))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&r, page_req(&"a".repeat(64))).await.0,
        StatusCode::GONE
    );
}

#[tokio::test(start_paused = true)]
async fn pending_requests_are_capped_and_the_cap_recovers_on_expiry() {
    let (state, _) = new_state();
    let r = router(&state);
    for _ in 0..MAX_PENDING_REQUESTS {
        create(&r).await;
    }
    assert_eq!(
        send(&r, create_req()).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );
    tokio::time::advance(REQUEST_TTL + Duration::from_secs(1)).await;
    create(&r).await;
}

#[tokio::test]
async fn grant_limit_is_reported_before_and_at_approval() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    let nonce = open_page(&r, &approve_id).await;
    fill_grants(&caps);

    // A new request is refused with the distinct status.
    let (status, body) = send(&r, create_req()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(json(&body)["status"], GRANT_LIMIT);

    // The page says so instead of offering Allow.
    let (status, html) = send(&r, page_req(&approve_id)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(html.contains("Revoke one"), "{html}");

    // An Allow on a page opened earlier is refused before "Allowed" is shown,
    // and the request is not answered.
    let (status, body) = send(
        &r,
        approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let poll =
        tokio::time::timeout(Duration::from_millis(50), send(&r, poll_req(&request_id))).await;
    assert!(poll.is_err(), "the request must still be pending");
}

/// The limit filled up between the click and the collection: the client gets
/// the distinct status, its next poll waits for a change instead of retrying
/// at once, and a revocation wakes it and lets the same request be collected.
#[tokio::test]
async fn grant_limit_at_collection_waits_and_recovers_on_revoke() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    fill_grants(&caps);

    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(json(&body)["status"], GRANT_LIMIT);
    assert!(json(&body).get("token").is_none());

    // The next poll does not answer at once: it waits for a change.
    let waiting = {
        let r = r.clone();
        let id = request_id.clone();
        tokio::spawn(async move { send(&r, poll_req(&id)).await })
    };
    assert!(
        wait_for_waiters(&state, &request_id, 1, Duration::from_secs(5)).await,
        "a poll at the limit must wait for a change"
    );
    assert!(!waiting.is_finished());

    let (victim, _) = caps.native_grants().unwrap()[0];
    assert_eq!(
        send(&r, revoke_req(&victim.to_hex(), Some(SELF_ORIGIN)))
            .await
            .0,
        StatusCode::OK
    );
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the revocation wakes the waiting poll")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(caps.native_grant_count().unwrap(), MAX_NATIVE_GRANTS);
}

/// With nothing changing, a poll at the limit answers again only after
/// `POLL_WAIT`, so a re-polling client is paced to one try per wait.
#[tokio::test(start_paused = true)]
async fn grant_limit_re_polls_are_paced() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    fill_grants(&caps);
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::CONFLICT
    );
    let started = Instant::now();
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::CONFLICT
    );
    assert!(started.elapsed() >= POLL_WAIT, "{:?}", started.elapsed());
}

/// Revoking drops the pending request too: a re-poll is 404 and can never
/// store the grant again.
#[tokio::test]
async fn revoke_drops_the_pending_request() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, token) = granted(&r).await;
    let hash = GrantTokenHash::of(&token);
    assert_eq!(
        send(&r, revoke_req(&hash.to_hex(), Some(SELF_ORIGIN)))
            .await
            .0,
        StatusCode::OK
    );
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(caps.native_grant_count().unwrap(), 0);
    assert!(matches!(
        resolve(&state, &token).await,
        GrantLookup::NotFound
    ));
}

/// Once stored, a re-poll returns the token even when the node has since
/// reached the limit: the stored grant is not counted again.
#[tokio::test]
async fn re_poll_after_the_limit_fills_returns_the_same_token() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, token) = granted(&r).await;
    fill_grants(&caps);
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(json(&body)["token"], token.as_str());
}

/// Once stored, a re-poll does not touch storage at all (here: a store whose
/// reads fail still answers it).
#[tokio::test]
async fn re_poll_after_storing_does_no_storage_io() {
    let storage = Arc::new(FlakyCapabilityStorage::default());
    let caps = DelegateCapabilities::new(storage.clone());
    let state = AppGrantState::with_capabilities(caps);
    let r = router(&state);
    let (request_id, token) = granted(&r).await;
    storage
        .fail_grants
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(json(&body)["token"], token.as_str());
}

/// The request goes away (revoked or expired) while a poll is writing its
/// grant: the row written is removed, since no client received the token.
#[tokio::test]
async fn a_request_dropped_while_storing_leaves_no_grant() {
    let storage = Arc::new(FlakyCapabilityStorage::default());
    let caps = DelegateCapabilities::new(storage.clone());
    let state = AppGrantState::with_capabilities(caps.clone());
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    let hash = {
        let pending = state.0.pending.lock();
        match &pending[&request_id].outcome {
            Outcome::Approved { token, .. } => token.hash(),
            Outcome::Pending | Outcome::Denied => panic!("approved"),
        }
    };
    // Runs inside the write's first read, i.e. after the poll marked the
    // request `storing` and released the lock.
    let dropper = state.clone();
    *storage.after_native_get.lock() = Some(Box::new(move |_| {
        dropper.forget_requests_for(hash);
    }));
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(storage.after_native_get.lock().is_none(), "the hook ran");
    assert_eq!(caps.native_grant_count().unwrap(), 0);
}

/// An answer given late in the request's life is still collectable for the
/// full window after the answer.
#[tokio::test(start_paused = true)]
async fn a_late_answer_gets_the_full_collection_window() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    tokio::time::advance(REQUEST_TTL - Duration::from_secs(5)).await;
    approve(&r, &approve_id).await;
    tokio::time::advance(REQUEST_TTL - Duration::from_secs(5)).await;
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::NOT_FOUND
    );
}

#[test]
fn trace_uri_redacts_the_grant_capabilities() {
    let u = |s: &str| s.parse::<axum::http::Uri>().unwrap();
    let id = "a".repeat(64);
    let redacted = redact_uri_for_log(&u(&format!("/v1/app-grants/request/{id}")));
    assert_eq!(redacted, "/v1/app-grants/request/<redacted>");
    assert!(!redact_uri_for_log(&u(&format!("/v1/app-grants/approve/{id}?x=1"))).contains(&id));
    assert_eq!(
        redact_uri_for_log(&u("/v1/app-grants/request")),
        "/v1/app-grants/request"
    );
    assert_eq!(
        redact_uri_for_log(&u(
            "/v1/contract/command?authToken=secret-a&userToken=secret-b"
        )),
        "/v1/contract/command",
        "the query (credentials) is never logged"
    );
    assert_eq!(
        redact_uri_for_log(&u("/permission/apps")),
        "/permission/apps"
    );
}

#[tokio::test]
async fn approval_page_shows_the_app_and_marks_the_name_unverified() {
    let (state, _) = new_state();
    let r = router(&state);
    let (_, approve_id) = create(&r).await;
    let resp = r.clone().oneshot(page_req(&approve_id)).await.unwrap();
    let headers = resp.headers().clone();
    assert_eq!(headers["x-frame-options"], "DENY");
    let csp = headers["content-security-policy"].to_str().unwrap();
    assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
    assert_eq!(headers["cache-control"], "no-store");
    let html = String::from_utf8(
        axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains(&app_id().to_string()));
    assert!(
        html.contains("<bdi>River &lt;b&gt;desktop&lt;/b&gt;</bdi>"),
        "name is escaped and isolated"
    );
    assert!(!html.contains("<b>desktop"));
    assert!(html.contains("has not verified this name"));
    assert!(!html.contains("{{"), "every placeholder is filled");
}

/// A name containing the page's placeholders must render as that literal
/// text, not as the live nonce or page id.
#[tokio::test]
async fn placeholders_in_the_client_name_are_not_expanded() {
    let (state, _) = new_state();
    let r = router(&state);
    let (status, body) = send(
        &r,
        create_req_named("{{NONCE}} {{APPROVE_ID}} {{SCRIPT_NONCE}}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let url = json(&body)["approve_url"].as_str().unwrap().to_string();
    let approve_id = url.rsplit('/').next().unwrap().to_string();
    let (_, html) = send(&r, page_req(&approve_id)).await;
    let nonce = nonce_of(&html);
    assert_eq!(
        html.matches(nonce.as_str()).count(),
        1,
        "nonce only in data-nonce"
    );
    assert!(html.contains("<bdi>{{NONCE}} {{APPROVE_ID}} {{SCRIPT_NONCE}}</bdi>"));
}

#[tokio::test]
async fn format_characters_are_stripped_from_the_name() {
    let (state, _) = new_state();
    let r = router(&state);
    let (_, body) = send(&r, create_req_named("abc\u{202E}def\u{2066}")).await;
    let url = json(&body)["approve_url"].as_str().unwrap().to_string();
    let approve_id = url.rsplit('/').next().unwrap().to_string();
    let (_, html) = send(&r, page_req(&approve_id)).await;
    assert!(html.contains("<bdi>abcdef</bdi>"));
    assert!(!html.contains('\u{202E}'));
}

#[test]
fn approve_url_authority_is_rebuilt_from_parsed_parts() {
    let h = |v: &str| {
        let mut m = HeaderMap::new();
        m.insert("host", v.parse().unwrap());
        m
    };
    assert_eq!(
        loopback_authority(&h("127.0.0.1:7509")).as_deref(),
        Some("127.0.0.1:7509")
    );
    assert_eq!(
        loopback_authority(&h("LOCALHOST:7509")).as_deref(),
        Some("localhost:7509")
    );
    assert_eq!(
        loopback_authority(&h("[::1]:7509")).as_deref(),
        Some("[::1]:7509")
    );
    assert_eq!(
        loopback_authority(&h("localhost")).as_deref(),
        Some("localhost")
    );
    assert_eq!(loopback_authority(&h("127.0.0.1:7509@evil")), None);
    assert_eq!(loopback_authority(&h("127.0.0.1:7509/x")), None);
    assert_eq!(loopback_authority(&h("evil.example:7509")), None);
    assert_eq!(loopback_authority(&h("[::1]x")), None);
}

#[tokio::test]
async fn resolve_connection_attests_the_granted_app_and_revocation_closes_it() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (_, token) = granted(&r).await;
    let hash = GrantTokenHash::of(&token);

    let GrantLookup::Granted(live) = resolve(&state, &token).await else {
        panic!("the grant resolves");
    };
    assert_eq!(live.contract_id, app_id());
    assert_eq!(state.live_connections(hash), 1);
    let revoked = live.guard.revoked();
    assert!(!revoked.is_cancelled());

    // An unknown token resolves to nothing and leaves no registration.
    assert!(matches!(
        resolve(&state, "nope").await,
        GrantLookup::NotFound
    ));
    assert_eq!(state.0.live.lock().len(), 1);

    // Revoke through the dashboard route.
    assert_eq!(
        send(&r, revoke_req(&hash.to_hex(), Some(SELF_ORIGIN)))
            .await
            .0,
        StatusCode::OK
    );
    assert!(
        revoked.is_cancelled(),
        "the live connection is told to close"
    );
    assert!(caps.native_grants().unwrap().is_empty());
    assert!(
        matches!(resolve(&state, &token).await, GrantLookup::NotFound),
        "a revoked token is refused as not found"
    );

    drop(live);
    assert_eq!(state.live_connections(hash), 0);
}

/// A store that cannot be read refuses the token (fails closed), reports it
/// as transient rather than revoked, and leaves no live registration behind.
#[tokio::test]
async fn unreadable_store_fails_closed_as_unavailable() {
    let storage = Arc::new(FlakyCapabilityStorage::default());
    let caps = DelegateCapabilities::new(storage.clone());
    let state = AppGrantState::with_capabilities(caps.clone());
    let token = GrantToken::generate();
    assert!(
        caps.create_native_grant(token.hash(), AppIdentity::WebApp(app_id()), "x")
            .unwrap()
    );
    storage
        .fail_grants
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(matches!(
        resolve(&state, token.expose()).await,
        GrantLookup::Unavailable
    ));
    assert!(state.0.live.lock().is_empty(), "no leaked registration");
    storage
        .fail_grants
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(matches!(
        resolve(&state, token.expose()).await,
        GrantLookup::Granted(_)
    ));
    // A node with no grant store yet (still starting) is also transient.
    let starting = AppGrantState::new(ExportOpManagerHandle::default());
    assert!(matches!(
        resolve(&starting, token.expose()).await,
        GrantLookup::Unavailable
    ));
}

#[tokio::test]
async fn dashboard_revoke_needs_the_exact_origin_and_list_hides_the_token() {
    let (state, _) = new_state();
    let r = router(&state);
    let (_, token) = granted(&r).await;
    let hash = GrantTokenHash::of(&token);

    let (status, body) = send(&r, list_req()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains(&token), "the list never shows the token");
    let v = json(&body);
    assert_eq!(v["grants"][0]["id"], hash.to_hex());
    assert_eq!(v["grants"][0]["app"], app_id().to_string());

    // A foreign Origin reads an empty list.
    let mut req = list_req();
    req.headers_mut()
        .insert("origin", "http://127.0.0.1:8080".parse().unwrap());
    let (status, body) = send(&r, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["grants"], serde_json::json!([]));

    for origin in [None, Some("http://127.0.0.1:8080"), Some("null")] {
        assert_eq!(
            send(&r, revoke_req(&hash.to_hex(), origin)).await.0,
            StatusCode::FORBIDDEN,
            "{origin:?}"
        );
    }
    assert!(matches!(
        resolve(&state, &token).await,
        GrantLookup::Granted(_)
    ));
}

#[test]
fn auth_token_debug_does_not_print_the_token() {
    let t = AuthToken::from("super-secret-token-value".to_string());
    let s = format!("{t:?} {:?}", Some((t.clone(), app_id())));
    assert!(!s.contains("super-secret"), "{s}");
}

/// Source pins: the browser pages' JavaScript and the Rust handlers must agree
/// on routes and body keys. These assets are served by `include_str!` and no
/// Rust test executes their JavaScript, so a rename on one side would ship
/// silently.
#[test]
fn page_scripts_match_the_routes_and_body_keys() {
    let approve = include_str!("assets/approve.html");
    assert!(approve.contains("fetch('/v1/app-grants/approve/'"));
    assert!(ROUTE_APPROVE.starts_with("/v1/app-grants/approve/"));
    assert!(approve.contains("data-nonce=\"{{NONCE}}\""));
    assert!(approve.contains("data-approve-id=\"{{APPROVE_ID}}\""));
    assert!(approve.contains("nonce: req.dataset.nonce"));
    assert!(approve.contains("allow: yes"));
    assert!(approve.contains("<script nonce=\"{{SCRIPT_NONCE}}\">"));
    assert!(approve.contains("r.status === 409"));
    // `ApproveSubmit { nonce, allow }`.
    let parsed: ApproveSubmit =
        serde_json::from_value(serde_json::json!({ "nonce": "n", "allow": true })).unwrap();
    assert!(parsed.allow && parsed.nonce == "n");

    let apps = include_str!("../permission_prompts/assets/apps_permissions.html");
    assert!(apps.contains(&format!("fetch('{ROUTE_LIST}')")));
    assert!(apps.contains(&format!("fetch('{ROUTE_REVOKE}'")));
    assert!(apps.contains("JSON.stringify({ id: g.id })"));
    // `RevokeNativeGrant { id }`.
    let parsed: RevokeNativeGrant =
        serde_json::from_value(serde_json::json!({ "id": "x" })).unwrap();
    assert_eq!(parsed.id, "x");
    for key in [
        "g.unreadable",
        "g.client_name",
        "g.app",
        "g.created_at_ms",
        "g.last_used_at_ms",
        "g.id",
    ] {
        assert!(apps.contains(key), "{key}");
    }
}

/// A source file reduced for scanning: `//` comments dropped and all
/// whitespace removed, so reflows and comments cannot hide or fake a match.
fn scan_form(src: &str) -> String {
    src.lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// Every use of `req.uri()` in `src`, with the text just before it.
fn uri_uses(src: &str) -> Vec<String> {
    let s = scan_form(src);
    s.match_indices("req.uri()")
        .map(|(i, _)| s[i.saturating_sub(30)..i].to_string())
        .collect()
}

/// Source pins: every request URI that reaches a log goes through the
/// redacting helper (path only, no query, grant ids redacted), and every
/// `TraceLayer` uses the redacting span. The only other use allowed is
/// parsing the query (`try_from_uri`), which logs nothing.
#[test]
fn request_logging_goes_through_the_redacting_helper() {
    let sources = [
        (
            "websocket.rs",
            include_str!("../../../client_events/websocket.rs"),
        ),
        (
            "permission_prompts.rs",
            include_str!("../permission_prompts.rs"),
        ),
        ("server.rs", include_str!("../../../server.rs")),
        ("app_grants.rs", include_str!("../app_grants.rs")),
    ];
    for (name, src) in sources {
        for before in uri_uses(src) {
            assert!(
                before.ends_with("redact_uri_for_log(") || before.ends_with("try_from_uri("),
                "{name}: a request URI is used outside the redacting helper: ...{before}req.uri()"
            );
        }
    }
    // The request span itself.
    let ag = scan_form(include_str!("../app_grants.rs"));
    let start = ag
        .find("pub(crate)fnmake_request_span<B>")
        .expect("make_request_span exists");
    let body = &ag[start..start + ag[start..].find('}').expect("function body ends")];
    assert!(
        body.contains("uri=%redact_uri_for_log(req.uri())"),
        "{body}"
    );
    // Both TraceLayers use it.
    let server = scan_form(include_str!("../../../server.rs"));
    let layers = server.matches("TraceLayer::new_for_http()").count();
    assert_eq!(layers, 2);
    assert_eq!(
        server
            .matches("TraceLayer::new_for_http().make_span_with(client_api::app_grants::make_request_span)")
            .count(),
        layers,
        "every TraceLayer uses the redacting span"
    );
}

#[test]
fn upgrade_grant_header_gate() {
    let h = |pairs: &[(&str, &str)]| {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        m
    };
    assert!(upgrade_may_use_grant(&h(&[("host", "127.0.0.1:7509")])));
    assert!(upgrade_may_use_grant(&h(&[("host", "localhost:7509")])));
    assert!(upgrade_may_use_grant(&h(&[("host", "[::1]:7509")])));
    assert!(!upgrade_may_use_grant(&h(&[])), "missing Host");
    assert!(!upgrade_may_use_grant(&h(&[("host", "evil.example:7509")])));
    for fwd in [
        "x-forwarded-for",
        "forwarded",
        "x-real-ip",
        "via",
        "true-client-ip",
        "cf-connecting-ip",
        "x-client-ip",
    ] {
        assert!(
            !upgrade_may_use_grant(&h(&[("host", "127.0.0.1:7509"), (fwd, "1.2.3.4")])),
            "{fwd}"
        );
    }
}

/// A write that commits but reports an error leaves a row; revoking it then
/// removes the row and the request, and a re-poll can neither collect nor
/// re-create the grant.
#[tokio::test]
async fn revoke_after_a_commit_that_reported_an_error() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    let hash = approved_hash(&state, &request_id);
    let token = match &state.0.pending.lock()[&request_id].outcome {
        Outcome::Approved { token, .. } => token.expose().to_string(),
        Outcome::Pending | Outcome::Denied => panic!("approved"),
    };
    storage
        .native_insert_commits_then_errs
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (status, _) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(caps.native_grant_count().unwrap(), 1, "the row did commit");
    assert_eq!(
        send(&r, revoke_req(&hash.to_hex(), Some(SELF_ORIGIN)))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(caps.native_grant_count().unwrap(), 0);
    assert!(
        !state.0.pending.lock().contains_key(&request_id),
        "the request is gone"
    );
    assert!(matches!(
        resolve(&state, &token).await,
        GrantLookup::NotFound
    ));
}

/// The retry after a write that committed but reported an error finds its
/// own row before counting, so it stores even when the table is now full.
#[tokio::test(start_paused = true)]
async fn retry_after_a_reported_error_finds_its_row_before_the_limit() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    storage
        .native_insert_commits_then_errs
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    fill_grants(&caps);
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(caps.native_grant_count().unwrap(), MAX_NATIVE_GRANTS);
}

/// A revocation whose HTTP request goes away mid-way still completes: the live
/// connection is closed and the pending request is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_revoke_still_completes() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, token) = granted(&r).await;
    let hash = GrantTokenHash::of(&token);
    let live = state.register_live(hash);
    let closed = live.revoked();
    let (hook, reached, release) = blocking_hook();
    *storage.before_native_remove.lock() = Some(hook);

    // Boxed, so dropping it really drops the future.
    let mut revoke = Box::pin(state.revoke(caps.clone(), hash));
    tokio::select! {
        biased;
        _ = &mut revoke => panic!("the delete is held by the hook"),
        _ = std::future::ready(()) => {}
    }
    drop(revoke);
    wait_reached(reached).await;
    release.send(()).ok();
    tokio::time::timeout(Duration::from_secs(5), closed.cancelled())
        .await
        .expect("the live connection is closed");
    assert!(!state.0.pending.lock().contains_key(&request_id));
    assert_eq!(caps.native_grant_count().unwrap(), 0);
}

/// The delete failing or panicking does not stop the live connection being
/// closed or the request being dropped. Multi-thread: the panic is in the
/// blocking pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoke_closes_connections_when_the_delete_fails_or_panics() {
    for panics in [false, true] {
        let (state, caps, storage) = flaky_state();
        let r = router(&state);
        let (request_id, token) = granted(&r).await;
        let hash = GrantTokenHash::of(&token);
        let live = state.register_live(hash);
        if panics {
            *storage.before_native_remove.lock() =
                Some(Box::new(|| panic!("injected delete panic")));
        } else {
            storage
                .fail_native_remove
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        assert!(state.revoke(caps.clone(), hash).await.is_err());
        assert!(live.revoked().is_cancelled(), "panics: {panics}");
        assert!(!state.0.pending.lock().contains_key(&request_id));
    }
}

/// The same with the delete panicking inline (current-thread runtime): the
/// revocation task unwinds, and its drop guard still closes the connection.
#[tokio::test]
async fn revoke_closes_connections_when_the_delete_panics_inline() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, token) = granted(&r).await;
    let hash = GrantTokenHash::of(&token);
    let live = state.register_live(hash);
    *storage.before_native_remove.lock() = Some(Box::new(|| panic!("injected delete panic")));
    assert!(state.revoke(caps, hash).await.is_err());
    assert!(live.revoked().is_cancelled());
    assert!(!state.0.pending.lock().contains_key(&request_id));
}

/// A collecting poll whose client goes away mid-write does not wedge the
/// request: the write finishes, and the next poll collects the token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_collecting_poll_does_not_wedge_the_request() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    let (hook, reached, release) = blocking_hook();
    *storage.after_native_get.lock() = Some(Box::new(move |_| hook()));
    let first = {
        let r = r.clone();
        let id = request_id.clone();
        tokio::spawn(async move { send(&r, poll_req(&id)).await })
    };
    wait_reached(reached).await;
    first.abort();
    drop(first.await);
    release.send(()).ok();
    let (status, body) =
        tokio::time::timeout(Duration::from_secs(10), send(&r, poll_req(&request_id)))
            .await
            .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(caps.native_grant_count().unwrap(), 1);
}

/// The same, revoked while the abandoned write is still running: the write's
/// own cleanup removes the row it made, also when the write reported an error
/// after committing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_collecting_poll_revoked_mid_write_leaves_no_row() {
    for commit_then_err in [false, true] {
        let (state, caps, storage) = flaky_state();
        let r = router(&state);
        let (request_id, approve_id) = create(&r).await;
        approve(&r, &approve_id).await;
        let hash = approved_hash(&state, &request_id);
        storage
            .native_insert_commits_then_errs
            .store(commit_then_err, std::sync::atomic::Ordering::SeqCst);
        let (hook, reached, release) = blocking_hook();
        *storage.after_native_get.lock() = Some(Box::new(move |_| hook()));
        let first = {
            let r = r.clone();
            let id = request_id.clone();
            tokio::spawn(async move { send(&r, poll_req(&id)).await })
        };
        wait_reached(reached).await;
        first.abort();
        drop(first.await);
        // No row yet, so the revoke itself finds nothing to delete.
        assert!(!state.revoke(caps.clone(), hash).await.unwrap());
        release.send(()).ok();
        // Two deletes: the revocation's (no row yet), then the write's own
        // cleanup. Wait for that second one, then check what it left.
        let deadline = Instant::now() + Duration::from_secs(10);
        while storage
            .native_removes
            .load(std::sync::atomic::Ordering::SeqCst)
            < 2
        {
            assert!(
                Instant::now() < deadline,
                "commit_then_err {commit_then_err}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            storage
                .native_inserts
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            caps.native_grant_count().unwrap(),
            0,
            "commit_then_err {commit_then_err}"
        );
        assert_eq!(
            send(&r, poll_req(&request_id)).await.0,
            StatusCode::NOT_FOUND
        );
    }
}

/// A poll arriving while a revocation's delete is still running gets 404: the
/// request is dropped before the delete, so the token cannot be collected (or
/// stored again) in that window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poll_during_a_revocation_gets_404() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, token) = granted(&r).await;
    let hash = GrantTokenHash::of(&token);
    let (hook, reached, release) = blocking_hook();
    *storage.before_native_remove.lock() = Some(hook);
    let revoking = {
        let state = state.clone();
        let caps = caps.clone();
        tokio::spawn(async move { state.revoke(caps, hash).await })
    };
    wait_reached(reached).await;
    let (status, body) = send(&r, poll_req(&request_id)).await;
    release.send(()).ok();
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(revoking.await.unwrap().unwrap());
    assert_eq!(caps.native_grant_count().unwrap(), 0);
}

/// The store task panicking inline (current-thread runtime) does not leave
/// the request marked `storing`: the poll gets 503, and a later poll stores.
#[tokio::test(start_paused = true)]
async fn a_panicking_store_does_not_wedge_the_request() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    *storage.after_native_get.lock() = Some(Box::new(|_| panic!("injected store panic")));
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(caps.native_grant_count().unwrap(), 1);
}

/// `off_thread` maps a panicking closure to `None`, and the connect-time
/// lookup maps that to `Unavailable` (retry), not "not found".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_thread_panic_maps_to_none_and_to_unavailable() {
    assert!(off_thread(|| -> u8 { panic!("injected") }).await.is_none());
    assert_eq!(off_thread(|| 7u8).await, Some(7));
    let (state, caps, storage) = flaky_state();
    let token = GrantToken::generate();
    assert!(
        caps.create_native_grant(token.hash(), AppIdentity::WebApp(app_id()), "x")
            .unwrap()
    );
    *storage.after_native_get.lock() = Some(Box::new(|_| panic!("injected read panic")));
    assert!(matches!(
        resolve(&state, token.expose()).await,
        GrantLookup::Unavailable
    ));
    assert!(state.0.live.lock().is_empty(), "no leaked registration");
}

/// A denial given late is collectable for the full window after the answer.
#[tokio::test(start_paused = true)]
async fn a_late_denial_gets_the_full_collection_window() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    tokio::time::advance(REQUEST_TTL - Duration::from_secs(5)).await;
    let nonce = open_page(&r, &approve_id).await;
    assert_eq!(
        send(
            &r,
            approve_req(&approve_id, &nonce, false, Some(SELF_ORIGIN))
        )
        .await
        .0,
        StatusCode::OK
    );
    tokio::time::advance(REQUEST_TTL - Duration::from_secs(5)).await;
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["status"], "denied");
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        send(&r, poll_req(&request_id)).await.0,
        StatusCode::NOT_FOUND
    );
}

/// Bodies are parsed only after the gate, on every route that takes one: a
/// refused caller's malformed body gets the gate's refusal, an admitted one
/// gets 400, a non-JSON content type 415, and an oversized body 413.
#[tokio::test]
async fn bodies_are_parsed_after_the_gate() {
    let (state, _) = new_state();
    let r = router(&state);
    let lan = router_with(&state, false, SocketAddr::from(([192, 168, 1, 7], 50000)));
    let (_, approve_id) = create(&r).await;
    let approve_path = format!("/v1/app-grants/approve/{approve_id}");
    for (path, origin) in [
        (ROUTE_REQUEST, None),
        (approve_path.as_str(), Some(SELF_ORIGIN)),
        (ROUTE_REVOKE, Some(SELF_ORIGIN)),
    ] {
        let req = |content_type: &str, body: Body| {
            let mut b = Request::post(path)
                .header("host", HOST)
                .header("content-type", content_type);
            if let Some(o) = origin {
                b = b.header("origin", o);
            }
            b.body(body).unwrap()
        };
        assert_eq!(
            send(&lan, req("application/json", Body::from("not json")))
                .await
                .0,
            StatusCode::FORBIDDEN,
            "{path}: gate first"
        );
        assert_eq!(
            send(&r, req("application/json", Body::from("not json")))
                .await
                .0,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
        assert_eq!(
            send(&r, req("text/plain", Body::from("{}"))).await.0,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{path}"
        );
        assert_eq!(
            send(&r, req("application/json", Body::from(vec![b' '; 8192])))
                .await
                .0,
            StatusCode::PAYLOAD_TOO_LARGE,
            "{path}"
        );
        // The boundary: exactly MAX_BODY_BYTES is read (and fails to parse);
        // one byte more is refused unread.
        assert_eq!(
            send(
                &r,
                req("application/json", Body::from(vec![b' '; MAX_BODY_BYTES]))
            )
            .await
            .0,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
        assert_eq!(
            send(
                &r,
                req(
                    "application/json",
                    Body::from(vec![b' '; MAX_BODY_BYTES + 1])
                )
            )
            .await
            .0,
            StatusCode::PAYLOAD_TOO_LARGE,
            "{path}"
        );
    }
}

/// A `HEAD` of the approval page does not rotate its nonce, and answers with
/// the status a GET would.
#[tokio::test]
async fn head_does_not_rotate_the_approval_nonce() {
    let (state, _) = new_state();
    let r = router(&state);
    let unknown = Request::head(format!("/v1/app-grants/approve/{}", "a".repeat(64)))
        .header("host", HOST)
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&r, unknown).await.0, StatusCode::GONE);
    let (_, approve_id) = create(&r).await;
    let nonce = open_page(&r, &approve_id).await;
    let head = Request::head(format!("/v1/app-grants/approve/{approve_id}"))
        .header("host", HOST)
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&r, head).await.0, StatusCode::OK);
    assert_eq!(
        send(
            &r,
            approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN))
        )
        .await
        .0,
        StatusCode::OK
    );
}

/// An unreadable stored record makes the connect-time lookup report
/// "unavailable" (keep the token), not "not found".
#[tokio::test]
async fn unreadable_record_resolves_to_unavailable() {
    let (state, _, storage) = flaky_state();
    let token = GrantToken::generate();
    assert!(
        storage
            .insert_native_grant_capped(&token.hash().0, b"\x09garbage", 64)
            .unwrap()
    );
    assert!(matches!(
        resolve(&state, token.expose()).await,
        GrantLookup::Unavailable
    ));
}

/// A row this build cannot decode is listed (so it is visible and counted
/// where the user can see it), can be revoked by its id, and once revoked its
/// token reads as "not found" (4401) rather than "unavailable" (4503).
#[tokio::test]
async fn an_unreadable_row_is_listed_and_revocable() {
    let (state, caps, storage) = flaky_state();
    let r = router(&state);
    let token = GrantToken::generate();
    assert!(
        storage
            .insert_native_grant_capped(&token.hash().0, b"\x09garbage", 64)
            .unwrap()
    );
    let (status, body) = send(&r, list_req()).await;
    assert_eq!(status, StatusCode::OK);
    let v = json(&body);
    assert_eq!(v["grants"][0]["id"], token.hash().to_hex(), "{body}");
    assert_eq!(v["grants"][0]["unreadable"], true);
    assert!(matches!(
        resolve(&state, token.expose()).await,
        GrantLookup::Unavailable
    ));
    assert_eq!(
        send(&r, revoke_req(&token.hash().to_hex(), Some(SELF_ORIGIN)))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(caps.native_grant_count().unwrap(), 0);
    assert!(matches!(
        resolve(&state, token.expose()).await,
        GrantLookup::NotFound
    ));
}
