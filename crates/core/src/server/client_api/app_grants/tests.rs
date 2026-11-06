use std::net::{Ipv4Addr, SocketAddr};

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use super::*;
use crate::contract::delegate_capabilities::MemoryCapabilityStorage;
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

/// A request as a native client sends it: loopback Host, no Origin, no
/// `Sec-Fetch-*`.
fn create_req() -> Request<Body> {
    Request::post(ROUTE_REQUEST)
        .header("host", HOST)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "app": app_id().to_string(), "client_name": "River <b>desktop</b>" })
                .to_string(),
        ))
        .unwrap()
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

/// Render the approval page; returns its nonce.
async fn open_page(router: &Router, approve_id: &str) -> String {
    let (status, html) = send(router, page_req(approve_id)).await;
    assert_eq!(status, StatusCode::OK, "{html}");
    let start = html.find("data-nonce=\"").expect("page carries a nonce") + "data-nonce=\"".len();
    html[start..start + 64].to_string()
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

#[tokio::test]
async fn request_approve_poll_returns_the_token_exactly_once() {
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

    // Once only.
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!body.contains(&token));
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

#[tokio::test]
async fn every_route_refuses_a_non_loopback_peer() {
    let (state, _) = new_state();
    let lan = router_with(&state, false, SocketAddr::from(([192, 168, 1, 7], 50000)));
    let (request_id, approve_id) = create(&router(&state)).await;
    let nonce = open_page(&router(&state), &approve_id).await;
    for req in [
        create_req(),
        poll_req(&request_id),
        page_req(&approve_id),
        approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
    ] {
        assert_eq!(send(&lan, req).await.0, StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn every_route_is_refused_on_a_hosted_node() {
    let (state, _) = new_state();
    let hosted = router_with(&state, true, loopback());
    let (request_id, approve_id) = create(&router(&state)).await;
    for req in [
        create_req(),
        poll_req(&request_id),
        page_req(&approve_id),
        approve_req(&approve_id, "x", true, Some(SELF_ORIGIN)),
        Request::get(ROUTE_LIST)
            .header("host", HOST)
            .body(Body::empty())
            .unwrap(),
    ] {
        assert_eq!(send(&hosted, req).await.0, StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn every_route_refuses_forwarded_requests() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    let nonce = open_page(&r, &approve_id).await;
    for header in [
        "x-forwarded-for",
        "x-forwarded-proto",
        "x-forwarded-host",
        "forwarded",
    ] {
        for mut req in [
            create_req(),
            poll_req(&request_id),
            page_req(&approve_id),
            approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
        ] {
            req.headers_mut().insert(header, "1.2.3.4".parse().unwrap());
            assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN, "{header}");
        }
    }
    // None of those consumed the nonce or answered the request.
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

/// A DNS-rebound page reaches the node with its own name as `Host`, and with a
/// matching `Origin`, so origin equality alone would pass.
#[tokio::test]
async fn non_loopback_host_is_refused() {
    let (state, _) = new_state();
    let r = router(&state);
    let (_, approve_id) = create(&r).await;
    let mut req = page_req(&approve_id);
    req.headers_mut()
        .insert("host", "evil.example:7509".parse().unwrap());
    assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN);
    let nonce = open_page(&r, &approve_id).await;
    let mut req = approve_req(&approve_id, &nonce, true, Some("http://evil.example:7509"));
    req.headers_mut()
        .insert("host", "evil.example:7509".parse().unwrap());
    assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN);
    let mut req = create_req();
    req.headers_mut()
        .insert("host", "evil.example:7509".parse().unwrap());
    assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN);
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
async fn expired_requests_are_refused() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    tokio::time::advance(REQUEST_TTL + Duration::from_secs(1)).await;
    let (status, body) = send(&r, poll_req(&request_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(caps.native_grants().unwrap().is_empty());

    let (_, approve_id) = create(&r).await;
    let nonce = open_page(&r, &approve_id).await;
    tokio::time::advance(REQUEST_TTL + Duration::from_secs(1)).await;
    let (status, _) = send(&r, page_req(&approve_id)).await;
    assert_eq!(status, StatusCode::GONE);
    let (status, _) = send(
        &r,
        approve_req(&approve_id, &nonce, true, Some(SELF_ORIGIN)),
    )
    .await;
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

#[tokio::test]
async fn pending_requests_are_capped() {
    let (state, _) = new_state();
    let r = router(&state);
    for _ in 0..MAX_PENDING_REQUESTS {
        create(&r).await;
    }
    assert_eq!(
        send(&r, create_req()).await.0,
        StatusCode::TOO_MANY_REQUESTS
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
        html.contains("River &lt;b&gt;desktop&lt;/b&gt;"),
        "name is escaped"
    );
    assert!(!html.contains("<b>desktop"));
    assert!(html.contains("has not verified this name"));
    assert!(!html.contains("{{"), "every placeholder is filled");
}

#[tokio::test]
async fn resolve_connection_attests_the_granted_app_and_revocation_closes_it() {
    let (state, caps) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    let token = json(&send(&r, poll_req(&request_id)).await.1)["token"]
        .as_str()
        .unwrap()
        .to_string();
    let token = AuthToken::from(token);
    let hash = GrantTokenHash::of(token.as_str());

    let live = state
        .resolve_connection(&token)
        .expect("the grant resolves");
    assert_eq!(live.contract_id, app_id());
    assert_eq!(state.live_connections(hash), 1);
    let revoked = live.guard.revoked();
    assert!(!revoked.is_cancelled());

    // An unknown token resolves to nothing and leaves no registration.
    assert!(
        state
            .resolve_connection(&AuthToken::from("nope".to_string()))
            .is_none()
    );
    assert_eq!(state.0.live.lock().len(), 1);

    // Revoke through the dashboard route.
    let req = Request::post(ROUTE_REVOKE)
        .header("host", HOST)
        .header("origin", SELF_ORIGIN)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "id": hash.to_hex() }).to_string(),
        ))
        .unwrap();
    assert_eq!(send(&r, req).await.0, StatusCode::OK);
    assert!(
        revoked.is_cancelled(),
        "the live connection is told to close"
    );
    assert!(caps.native_grants().unwrap().is_empty());
    assert!(
        state.resolve_connection(&token).is_none(),
        "a revoked token is refused"
    );

    drop(live);
    assert_eq!(state.live_connections(hash), 0);
}

#[tokio::test]
async fn dashboard_revoke_needs_the_exact_origin_and_list_hides_the_token() {
    let (state, _) = new_state();
    let r = router(&state);
    let (request_id, approve_id) = create(&r).await;
    approve(&r, &approve_id).await;
    let token = json(&send(&r, poll_req(&request_id)).await.1)["token"]
        .as_str()
        .unwrap()
        .to_string();
    let hash = GrantTokenHash::of(&token);

    let (status, body) = send(
        &r,
        Request::get(ROUTE_LIST)
            .header("host", HOST)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains(&token), "the list never shows the token");
    let v = json(&body);
    assert_eq!(v["grants"][0]["id"], hash.to_hex());
    assert_eq!(v["grants"][0]["app"], app_id().to_string());

    for origin in [None, Some("http://127.0.0.1:8080")] {
        let mut b = Request::post(ROUTE_REVOKE)
            .header("host", HOST)
            .header("content-type", "application/json");
        if let Some(o) = origin {
            b = b.header("origin", o);
        }
        let req = b
            .body(Body::from(
                serde_json::json!({ "id": hash.to_hex() }).to_string(),
            ))
            .unwrap();
        assert_eq!(send(&r, req).await.0, StatusCode::FORBIDDEN, "{origin:?}");
    }
    assert!(state.resolve_connection(&AuthToken::from(token)).is_some());
}

#[test]
fn auth_token_debug_does_not_print_the_token() {
    let t = AuthToken::from("super-secret-token-value".to_string());
    let s = format!("{t:?} {:?}", Some((t.clone(), app_id())));
    assert!(!s.contains("super-secret"), "{s}");
}
