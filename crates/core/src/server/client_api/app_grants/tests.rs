use std::net::{Ipv4Addr, SocketAddr};

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use super::*;
use crate::contract::delegate_capabilities::{FlakyCapabilityStorage, MemoryCapabilityStorage};
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

fn resolve(state: &AppGrantState, token: &str) -> GrantLookup {
    state.resolve_connection(&AuthToken::from(token.to_string()))
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
    let GrantLookup::Granted(live) = resolve(&state, &token) else {
        panic!("the grant resolves");
    };
    drop(live);
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!body.contains(&token));
}

#[tokio::test]
async fn concurrent_polls_get_the_same_token_and_store_it_once() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    let polls: Vec<_> = (0..4)
        .map(|_| {
            let r = r.clone();
            let id = request_id.clone();
            tokio::spawn(async move { send(&r, poll_req(&id)).await })
        })
        .collect();
    tokio::task::yield_now().await;
    approve(&r, &approve_id).await;
    let mut tokens = std::collections::HashSet::new();
    for p in polls {
        let (status, body) = tokio::time::timeout(Duration::from_secs(5), p)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        tokens.insert(json(&body)["token"].as_str().unwrap().to_string());
    }
    assert_eq!(tokens.len(), 1, "every poll sees the one token");
    assert_eq!(caps.native_grant_count().unwrap(), 1, "stored once");
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
/// the distinct status, and once the user revokes one, the same request can
/// still be collected.
#[tokio::test]
async fn grant_limit_at_collection_is_recoverable() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    fill_grants(&caps);

    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(json(&body)["status"], GRANT_LIMIT);
    assert!(json(&body).get("token").is_none());

    let (victim, _) = caps.native_grants().unwrap()[0];
    assert!(caps.revoke_native_grant(victim).unwrap());
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(caps.native_grant_count().unwrap(), MAX_NATIVE_GRANTS);
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

    let GrantLookup::Granted(live) = resolve(&state, &token) else {
        panic!("the grant resolves");
    };
    assert_eq!(live.contract_id, app_id());
    assert_eq!(state.live_connections(hash), 1);
    let revoked = live.guard.revoked();
    assert!(!revoked.is_cancelled());

    // An unknown token resolves to nothing and leaves no registration.
    assert!(matches!(resolve(&state, "nope"), GrantLookup::NotFound));
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
        matches!(resolve(&state, &token), GrantLookup::NotFound),
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
        resolve(&state, token.expose()),
        GrantLookup::Unavailable
    ));
    assert!(state.0.live.lock().is_empty(), "no leaked registration");
    storage
        .fail_grants
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(matches!(
        resolve(&state, token.expose()),
        GrantLookup::Granted(_)
    ));
    // A node with no grant store yet (still starting) is also transient.
    let starting = AppGrantState::new(ExportOpManagerHandle::default());
    assert!(matches!(
        resolve(&starting, token.expose()),
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
    assert!(matches!(resolve(&state, &token), GrantLookup::Granted(_)));
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
        "g.client_name",
        "g.app",
        "g.created_at_ms",
        "g.last_used_at_ms",
        "g.id",
    ] {
        assert!(apps.contains(key), "{key}");
    }
}
