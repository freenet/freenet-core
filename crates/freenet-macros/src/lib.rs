//! Procedural macros for Freenet integration tests
//!
//! This crate provides the `#[freenet_test]` attribute macro that automates
//! test infrastructure setup including:
//! - Node configuration and startup
//! - TestLogger initialization
//! - WebSocket client management
//! - Automatic failure reporting with event aggregation
//!
//! # Example
//!
//! ```ignore
//! use freenet::test_utils::TestContext;
//!
//! #[freenet_test(nodes = ["gateway", "peer-1"])]
//! async fn test_put_operation(ctx: &mut TestContext) -> TestResult {
//!     let contract = load_contract("test-contract", vec![].into())?;
//!     let state = WrappedState::from(vec![1, 2, 3]);
//!
//!     ctx.put("peer-1", contract, state).await?;
//!
//!     Ok(())
//! }
//! ```

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn};

mod codegen;
mod parser;

use codegen::generate_test_code;
use parser::FreenetTestArgs;

/// Attribute macro for Freenet integration tests
///
/// Automatically sets up:
/// - Multiple nodes (gateway + peers)
/// - TestLogger with JSON output
/// - Event log collection
/// - WebSocket clients (lazy-initialized)
/// - Comprehensive failure reports
///
/// # Attributes
///
/// - `nodes` (required): Array of node labels
/// - `gateways` (optional): Array of nodes that should be gateways. If not specified, the first node is a gateway.
/// - `auto_connect_peers` (optional): If true, peer nodes are configured to connect to all gateway nodes (default: false)
/// - `timeout_secs` (optional): Test timeout in seconds (default: 180)
/// - `startup_wait_secs` (optional): Node startup wait in seconds (default: 15). When `wait_for_connections` is true,
///   this becomes the maximum timeout for waiting for connections.
/// - `wait_for_connections` (optional): If true, polls for connection events instead of sleeping for a fixed duration.
///   This makes tests more reliable by waiting until connections are actually established. (default: false)
/// - `expected_connections` (optional): Minimum number of connections expected per peer node when using
///   `wait_for_connections`. If not specified, defaults to 1 (each peer connects to at least one gateway).
/// - `aggregate_events` (optional): When to aggregate events:
///   - `"on_failure"` (default): Only on test failure
///   - `"always"`: Always show event analysis
///   - `"never"`: Never aggregate
/// - `log_level` (optional): Log level filter (default: "freenet=debug,info")
/// - `tokio_flavor` (optional): Tokio runtime flavor:
///   - `"current_thread"` (default): Single-threaded runtime
///   - `"multi_thread"`: Multi-threaded runtime
/// - `tokio_worker_threads` (optional): Number of worker threads for multi_thread flavor (no default)
///
/// # Examples
///
/// ## Single Gateway (default)
/// ```ignore
/// #[freenet_test(nodes = ["gateway", "peer-1"])]
/// async fn test_simple(ctx: &mut TestContext) -> TestResult {
///     let gateway = ctx.gateway()?;  // First node
///     Ok(())
/// }
/// ```
///
/// ## Multiple Gateways
/// ```ignore
/// #[freenet_test(
///     nodes = ["gw-1", "gw-2", "peer-1", "peer-2"],
///     gateways = ["gw-1", "gw-2"],
///     timeout_secs = 180,
///     aggregate_events = "on_failure",
///     tokio_flavor = "multi_thread",
///     tokio_worker_threads = 8
/// )]
/// async fn test_multi_gateway(ctx: &mut TestContext) -> TestResult {
///     let gateways = ctx.gateways();  // All gateway nodes
///     let peers = ctx.peers();        // All peer nodes
///     Ok(())
/// }
/// ```
///
/// ## Auto-Connect Peers to Gateways
/// ```ignore
/// #[freenet_test(
///     nodes = ["gateway", "peer-1", "peer-2"],
///     auto_connect_peers = true,  // Peers will connect to gateway
///     timeout_secs = 120
/// )]
/// async fn test_with_connections(ctx: &mut TestContext) -> TestResult {
///     // Peers are configured to discover and connect to the gateway
///     let gateway = ctx.gateway()?;
///     let peers = ctx.peers();
///     // Test peer-gateway interactions...
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn freenet_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute arguments
    let args = parse_macro_input!(attr as FreenetTestArgs);

    // Parse the test function
    let input_fn = parse_macro_input!(item as ItemFn);

    // Generate the expanded test code
    match generate_test_code(args, input_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
