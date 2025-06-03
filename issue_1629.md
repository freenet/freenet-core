## Description
The `test_multiple_clients_subscription` test in `crates/core/tests/operations.rs` is flaky and consistently times out on CI, though it passes locally. This test has had its timeout increased previously (from 60s to 90s in commit 364c2793) but continues to fail intermittently.

## Current Behavior
- Test times out on CI with "Did not receive update response within timeout period"
- Test passes locally in ~52 seconds
- Has been marked as `#[ignore]` in PR #1622 to unblock development

## Expected Behavior
Test should pass reliably on both local machines and CI environments.

## Possible Causes
1. CI machines are slower than local development machines
2. Network setup differences in CI environment
3. Race conditions or timing-sensitive code in the test
4. The test may be doing too much work for a single test

## Suggested Solutions
1. Further increase the timeout (though this is a band-aid)
2. Split the test into smaller, more focused tests
3. Investigate and fix the underlying timing sensitivity
4. Add better logging to understand why it times out on CI

## References
- Originally reported timeout issues were addressed by increasing timeout from 60s to 90s
- Test disabled in PR #1622 with `#[ignore = "Flaky test - times out on CI. See issue #1629"]`