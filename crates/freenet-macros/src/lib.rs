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

mod parser;
mod codegen;

use parser::FreenetTestArgs;
use codegen::generate_test_code;

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
/// - `nodes` (required): Array of node labels, first is gateway
/// - `timeout_secs` (optional): Test timeout in seconds (default: 180)
/// - `startup_wait_secs` (optional): Node startup wait in seconds (default: 15)
/// - `aggregate_events` (optional): When to aggregate events:
///   - `"on_failure"` (default): Only on test failure
///   - `"always"`: Always show event analysis
///   - `"never"`: Never aggregate
/// - `log_level` (optional): Log level filter (default: "freenet=debug,info")
///
/// # Example
///
/// ```ignore
/// #[freenet_test(
///     nodes = ["gateway", "peer-1", "peer-2"],
///     timeout_secs = 180,
///     aggregate_events = "on_failure"
/// )]
/// async fn test_multi_peer_operation(ctx: &mut TestContext) -> TestResult {
///     // Test code with access to ctx
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
