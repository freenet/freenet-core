//! End-to-end regression test for `NodeInfo::uptime_seconds` (#5223).
//!
//! The node-diagnostics reply hardcoded `uptime_seconds: 0` behind a
//! `// TODO: implement actual uptime tracking`, so every `freenet service
//! report` upload claimed the node had zero uptime. Diagnostic reports R7JSRK
//! (a node that had been up ~7 hours), WEWYWY (169 connected peers) and R7W5NC
//! (83 connected peers) all read `uptime_seconds = 0` while every sibling field
//! in the same response carried real data — the field a human reaches for when
//! diagnosing a restart loop was the one that lied.
//!
//! This exercises the real client path (WebSocket → `NodeQuery::NodeDiagnostics`
//! → the `QueryNodeDiagnostics` handler in `p2p_protoc.rs`) rather than the
//! helper in isolation, so it fails if the handler stops calling the helper.

use std::time::Duration;

use freenet::test_utils::TestContext;
use freenet_macros::freenet_test;
use freenet_stdlib::client_api::{
    ClientRequest, HostResponse, NodeDiagnosticsConfig, NodeQuery, QueryResponse, WebApi,
};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

/// Ceiling on a freshly-booted test node's uptime.
///
/// Deliberately tight enough to have a real red state: the harness boots this
/// node within the test, so uptime must be on the order of the startup wait.
/// A reading above this means `started_at` is being measured from something
/// other than node construction — machine boot, or a shared process-wide
/// instant carried across tests in the same binary. A looser ceiling (an hour,
/// say) would be an assertion no input could fail, which is worth nothing.
const MAX_PLAUSIBLE_TEST_UPTIME_SECS: u64 = 300;

/// Seconds to wait between the two diagnostics queries. Must be >= 2 so the
/// second reading is at least one WHOLE second larger than the first even when
/// the first query lands just after a second boundary (`uptime_seconds`
/// truncates).
const GAP_SECS: u64 = 2;

async fn query_uptime_seconds(client: &mut WebApi) -> anyhow::Result<u64> {
    let config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: false,
        include_subscriptions: false,
        contract_keys: vec![],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: false,
    };

    client
        .send(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
            config,
        }))
        .await?;

    match timeout(Duration::from_secs(30), client.recv()).await {
        Ok(Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(response)))) => {
            let node_info = response
                .node_info
                .ok_or_else(|| anyhow::anyhow!("diagnostics response missing node_info"))?;
            Ok(node_info.uptime_seconds)
        }
        Ok(Ok(other)) => Err(anyhow::anyhow!(
            "unexpected response to NodeDiagnostics query: {other:?}"
        )),
        Ok(Err(e)) => Err(anyhow::anyhow!("diagnostics query failed: {e}")),
        Err(_) => Err(anyhow::anyhow!("diagnostics query timed out after 30s")),
    }
}

/// A node that has been running reports a non-zero, monotonically increasing
/// `uptime_seconds`.
///
/// Two assertions, because each catches a different way the field can be dead:
/// - the first reading must be non-zero (catches the hardcoded `0` of #5223);
/// - the second reading, taken `GAP_SECS` later, must be strictly larger
///   (catches any hardcoded or frozen constant, which a `> 0` check alone
///   would happily accept).
#[freenet_test(nodes = ["gateway"], health_check_readiness = true, timeout_secs = 180)]
async fn test_node_diagnostics_reports_nonzero_increasing_uptime(
    ctx: &mut TestContext,
) -> TestResult {
    let gateway = ctx.gateway()?;
    let (stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(stream);

    // The node has already been up through startup + readiness, so uptime is
    // well past one second by now. Sleep anyway so the assertion does not
    // depend on how long startup happened to take.
    tokio::time::sleep(Duration::from_secs(GAP_SECS)).await;

    let first = query_uptime_seconds(&mut client).await?;
    assert!(
        first > 0,
        "uptime_seconds must be non-zero for a node that has been running for \
         at least {GAP_SECS}s, got {first}. This is the #5223 regression: the \
         field was hardcoded to 0, so reports R7JSRK / WEWYWY / R7W5NC all \
         claimed zero uptime on nodes that had been up for hours."
    );
    assert!(
        first < MAX_PLAUSIBLE_TEST_UPTIME_SECS,
        "uptime_seconds of {first} is implausible for a node this test booted \
         seconds ago — `started_at` is measuring from something other than \
         node construction"
    );

    tokio::time::sleep(Duration::from_secs(GAP_SECS)).await;

    let second = query_uptime_seconds(&mut client).await?;
    assert!(
        second > first,
        "uptime_seconds must increase as the node keeps running: read {first} \
         then {second} after a {GAP_SECS}s gap. An unchanged value means the \
         field is a constant rather than a real elapsed-time measurement."
    );

    Ok(())
}
