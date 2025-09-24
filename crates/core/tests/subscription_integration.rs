// Integration tests for subscription functionality
// Tests the fixes in PR #1854:
// 1. Transaction ID correlation for subscription responses
// 2. Multiple peer candidates (k=3) for resilience
// 3. Optimal location nodes can subscribe

mod subscription_integration;

use subscription_integration::*;

// Re-export the test scenarios so they can be run
pub use subscription_integration::scenarios::*;