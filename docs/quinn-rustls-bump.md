# Rehearsed bump of quinn / rustls (and the quinn fork sync)

Use this when the RustSec watch (`.github/workflows/rustsec-watch.yml`) fails,
or when a release fixes a transport/TLS advisory. Goal: a reviewed bump PR in
hours, not days.

0. **Known advisory being fixed?** Add it to `scripts/rustsec-baseline.tsv`
   (id, crate, version, tracking link, expiry about 14 days out) so the daily
   run is not red while the fix is in review; delete the entry in the fix PR.
1. **Read the failing advisory.** The job log lists id, crate, version and
   patched range. Entries labelled `fork-upstream` are advisories against the
   upstream crates behind `freenet-quinn*`; they come from
   `scripts/rustsec-fork-upstream.map`, which holds a lower bound (see its
   header), so first check whether the fork head already contains the fix.
2. **rustls, quinn-udp, ring, webpki** (plain crates.io dependencies):
   `cargo update -p <crate> --precise <patched>` and commit `Cargo.lock`
   (or merge the Dependabot PR). The `quinn` pin in `crates/core/Cargo.toml` is
   exact, so these never move by themselves.
3. **quinn / quinn-proto (the fork):**
   1. Sync `freenet/quinn` with upstream's `0.11.x` branch (merge, keep only the
      package renames and README). If upstream has released the fix the fork
      exists for (quinn-rs/quinn#2987), retire the fork instead and depend on
      upstream (see the comment above the `quinn` entry in
      `crates/core/Cargo.toml`).
   2. Run the fork's tests, publish `freenet-quinn` and `freenet-quinn-proto`
      with a new version, and bump the exact pins in `crates/core/Cargo.toml`.
   3. Re-check the quinn-internals items in `.claude/rules/transport.md`
      (PANICS) against the new code, as the `Cargo.toml` comment requires.
   4. Update `scripts/rustsec-fork-upstream.map` to the newest released
      upstream versions that are ancestors of the new fork commit
      (`gh api repos/freenet/quinn/compare/quinn-proto-<ver>...<sha>`).
4. **Verify locally before pushing** (PR CI is not the test runner):
   `scripts/rustsec-watch.sh scheduled`, then `cargo clippy` with CI's flags and
   `cargo nextest run -p freenet quic` (plus the transport tests the change
   touches).
5. **Review and release.** Transport/TLS changes are Full-tier review. Whether to
   cut a release is a separate decision (see `docs/RELEASING.md`).
