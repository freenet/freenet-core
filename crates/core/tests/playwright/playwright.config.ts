import { defineConfig, devices } from "@playwright/test";

// Playwright configuration for the Freenet shell smoke tests
// (freenet/freenet-core#3856).
//
// The freenet node is NOT started by Playwright. The Rust harness
// (crates/core/tests/playwright_shell.rs) boots an in-process node, publishes
// the fixture website contract, and passes the ready-to-load shell URL via the
// FREENET_SHELL_URL environment variable before invoking `npx playwright
// test`. This keeps node lifecycle on the well-tested `#[freenet_test]` path
// and leaves Playwright as a thin browser-driver layer.
//
// To run these tests standalone (without the Rust harness) point
// FREENET_SHELL_URL at a running node's shell URL, e.g.:
//   FREENET_SHELL_URL=http://127.0.0.1:50001/v1/contract/web/<key>/ \
//     npx playwright test

const shellUrl = process.env.FREENET_SHELL_URL;

export default defineConfig({
  testDir: "./tests",
  // Generous, CI-friendly timeouts: the first iframe load fetches and unpacks
  // the contract bundle, which can be slow on a cold/contended CI runner.
  timeout: 60_000,
  expect: { timeout: 20_000 },
  // The shell is a single node; running specs in parallel against one node
  // only adds contention without coverage. Keep it serial and deterministic.
  fullyParallel: false,
  workers: 1,
  // Never silently pass on an accidentally-skipped suite in CI.
  forbidOnly: !!process.env.CI,
  // No automatic retries — a flaky shell test is a real bug to investigate,
  // not something to paper over (project "flaky tests are broken tests" rule).
  retries: 0,
  reporter: process.env.CI ? [["github"], ["list"]] : "list",
  use: {
    baseURL: shellUrl,
    headless: true,
    trace: "retain-on-failure",
    // Capture console + page errors for the CSP-violation assertions.
    ignoreHTTPSErrors: true,
  },
  // All three engines. This suite exists because the shell's behaviour differs
  // BETWEEN engines: #5087 (blank new tab) reproduces only in WebKit, #5106
  // (dead click) only in Firefox, and the #3818 sandbox-escape guard in
  // shell.spec.ts passes vacuously in Chromium — there the escape is refused a
  // step earlier, so the assertion runs against an empty set and the header it
  // guards could be deleted with the run still green. Only Firefox actually
  // exercises it. A chromium-only matrix is how #5087 shipped green.
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
    {
      name: "firefox",
      use: { ...devices["Desktop Firefox"] },
    },
    {
      name: "webkit",
      use: { ...devices["Desktop Safari"] },
    },
  ],
});
