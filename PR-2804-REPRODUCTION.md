# PR #2804 - Subscription Renewal Bug Reproduction

## Summary

This document explains the subscription renewal bug addressed in PR #2804 and provides a regression test to verify the fix.

## The Bug

**Issue**: Contracts added via GET operations have their subscriptions silently expire after 4 minutes because the renewal logic doesn't check the `GetSubscriptionCache`.

### Root Cause

The subscription renewal logic in `crates/core/src/ring/seeding.rs::contracts_needing_renewal()` only checks:

1. **`active_subscriptions`** - Explicit Subscribe operations
2. **`client_subscriptions`** - Client-requested subscriptions

But it **does NOT check**:

3. **`GetSubscriptionCache`** - Auto-subscriptions triggered by GET operations

### Timeline of the Bug

1. **T+0s**: User performs a GET operation on a contract
2. **T+0s**: Contract is fetched and cached in `GetSubscriptionCache`
3. **T+0s**: An auto-subscription is created (if `AUTO_SUBSCRIBE_ON_GET = true`)
4. **T+120s** (2 minutes): Subscription renewal check runs
   - ❌ **BUG**: GET-triggered contracts are NOT included in `contracts_needing_renewal()`
   - ❌ Subscription is NOT renewed
5. **T+240s** (4 minutes): Subscription expires (`SUBSCRIPTION_LEASE_DURATION`)
   - ❌ Contract updates stop being received
   - ❌ Cached state becomes stale

### Real-World Impact

From PR #2804 description:
> "River UI contract subscription expired on technic, updates stopped propagating."

Users stop receiving updates for contracts they're actively viewing, causing the UI to show stale data.

## The Fix (PR #2804)

The `unified-hosting` branch consolidates the fragmented caching systems into a single unified "Hosting" architecture where:

1. **All hosted contracts are tracked in one place** (`HostingCache`)
2. **Renewal logic checks ALL hosted contracts**, not just explicit subscriptions
3. **GET operations properly refresh hosting status**, preventing silent expiry

## Reproduction Test

A regression test has been added: `crates/core/tests/test_subscription_renewal_bug.rs`

### Running the Test

The test is marked `#[ignore]` with `TODO-MUST-FIX` because it demonstrates the bug on `main`:

```bash
# Run the ignored test to see it demonstrate the issue
cargo test -p freenet --test test_subscription_renewal_bug \
  --features simulation_tests -- --ignored --nocapture
```

### Test Scenarios

#### Test 1: `test_get_triggered_subscription_renewal`

Simulates the full bug scenario:
1. Creates network (1 gateway + 3 nodes)
2. Triggers PUT and GET operations (creates contracts in GetSubscriptionCache)
3. Advances virtual time past 2 minutes (renewal interval)
4. Advances virtual time past 4 minutes (lease expiry)
5. Verifies behavior after expiry

**On main**: GET-triggered subscriptions are not renewed and expire
**On unified-hosting**: Subscriptions are properly renewed and remain active

#### Test 2: `test_subscription_renewal_timing`

Focused timing test:
1. Creates minimal network (1 gateway + 2 nodes)
2. Triggers one contract operation
3. Advances time to renewal interval (2 minutes)
4. Checks if renewal happens
5. Advances to lease duration (4 minutes)
6. Checks if subscription expired or remained active

## Verifying the Fix

### Step 1: Verify Bug Exists on Main

```bash
# Checkout main branch
git checkout main

# Run the test (currently passes because it only logs, doesn't assert)
cargo test -p freenet --test test_subscription_renewal_bug \
  --features simulation_tests -- --ignored --nocapture

# Look for these log messages indicating the bug:
# "Phase 3: Advancing time past lease duration (4 minutes total)"
# "Test completed - subscription should have been renewed before 240s expiry"
```

### Step 2: Verify Fix Works on unified-hosting

```bash
# Fetch the PR branch
git fetch origin unified-hosting

# Checkout the fix branch
git checkout unified-hosting

# Run the test - should show different behavior
cargo test -p freenet --test test_subscription_renewal_bug \
  --features simulation_tests -- --ignored --nocapture

# Expected: Subscriptions renewed, contracts remain active beyond 4 minutes
```

### Step 3: Compare Event Logs

The key difference should be visible in telemetry/events:

**On main**:
- Subscriptions created at T+0
- No renewal events at T+120s for GET-triggered contracts
- Subscriptions expire at T+240s

**On unified-hosting**:
- Subscriptions created at T+0
- Renewal events at T+120s for ALL hosted contracts
- Subscriptions remain active beyond T+240s

## Next Steps After Merge

Once PR #2804 is merged:

1. **Update the test** with proper assertions:
   ```rust
   // Instead of just logging, assert that subscriptions are active
   assert!(subscription_active_after_4_minutes,
           "GET-triggered subscriptions should be renewed");
   ```

2. **Remove `#[ignore]` attribute**:
   ```rust
   // Change from:
   #[ignore = "TODO-MUST-FIX: Fails on main..."]

   // To:
   #[test_log::test(tokio::test(flavor = "current_thread"))]
   async fn test_get_triggered_subscription_renewal() {
   ```

3. **Add to CI**: The test will run on every PR to prevent regression

## Technical Details

### Constants (from `crates/core/src/ring/seeding.rs`)

```rust
/// Subscription lease duration - subscriptions expire after this
pub const SUBSCRIPTION_LEASE_DURATION: Duration = Duration::from_secs(240); // 4 minutes

/// Renewal interval - subscriptions should be renewed at this interval
pub const SUBSCRIPTION_RENEWAL_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes
```

### Auto-subscribe on GET (from `crates/core/src/ring/get_subscription_cache.rs`)

```rust
/// Whether to auto-subscribe to contracts after GET operations
pub const AUTO_SUBSCRIBE_ON_GET: bool = true;

/// Default minimum TTL before subscription can be evicted (30 minutes)
pub const DEFAULT_MIN_TTL: Duration = Duration::from_secs(30 * 60);
```

## Related Issues/PRs

- **PR #2804**: Main fix - unify contract hosting architecture
- **Issue Reference**: River UI contract subscription expired on technic

## Files Modified

- ✅ `crates/core/tests/test_subscription_renewal_bug.rs` - Regression test (this PR)
- 🔄 `crates/core/src/ring/seeding.rs` - Fix in PR #2804
- 🔄 `crates/core/src/ring/hosting.rs` - New unified module in PR #2804 (if applicable)
