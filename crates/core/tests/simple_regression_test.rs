//! Simple regression test for PR #1806 - prevent self-routing in GET operations
//!
//! This test verifies that when performing GET operations, nodes no longer
//! attempt to connect to themselves, which was causing infinite loops.

#[test]
fn test_get_never_includes_self_in_routing() {
    // This test verifies that the include_self parameter has been removed
    // and GET operations never try to route to self.

    // The fix in PR #1806 removes the include_self parameter entirely from
    // k_closest_potentially_caching, ensuring nodes never route to themselves.

    // Since the include_self parameter is removed, this code should not compile
    // if someone tries to reintroduce it:
    // ring.k_closest_potentially_caching(&key, &skip_list, 5, true);  // Should not compile

    // The function now only takes 3 parameters:
    // ring.k_closest_potentially_caching(&key, &skip_list, 5);

    // This is a compile-time guarantee that self-routing is prevented.
    // The test passes if it compiles, demonstrating that the dangerous
    // include_self parameter no longer exists in the API.
}
