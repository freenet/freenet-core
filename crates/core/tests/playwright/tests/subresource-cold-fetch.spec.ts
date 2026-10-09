import { test, expect } from "@playwright/test";

// The web route's subresource path, in a real browser (freenet/freenet-core#5406).
//
// #3942 made a cold-cache subresource request fetch the contract, so an `<img>`
// pointing into a container the reader had never opened would resolve. #4417
// then narrowed that fetch to contracts the node already stored or subscribed
// to, which closed the case for every contract the node had not seen — the
// regression #5406 reports. The fix bounds the fetch instead of refusing it.
//
// Rust-level coverage of the decision lives in
// `crates/core/src/server/path_handlers.rs` (`variable_content_triggers_fetch_on_cache_miss`
// and siblings), but those call the handler directly. What they cannot see is
// what a BROWSER receives, which is the half that matters here: a subresource
// is loaded by the engine, not by a person, and an error the engine cannot
// interpret — a missing CORS header on a response into a null-origin sandboxed
// iframe — surfaces as an opaque "CORS error" with no status at all. That
// exact confusion is on record (user report SUB0PT1MAL / cirro, 2026-07-29).
// See `.claude/rules/browser-assets.md`.

const shellUrl = process.env.FREENET_SHELL_URL;

test.beforeAll(() => {
  if (!shellUrl) {
    throw new Error(
      "FREENET_SHELL_URL is not set. These tests are normally driven by the " +
        "Rust harness (crates/core/tests/playwright_shell.rs), which boots a " +
        "node, publishes the fixture, and exports the shell URL.",
    );
  }
});

// A syntactically VALID contract id that cannot exist: base58 of 32 zero bytes.
// It has to parse, or the route answers 400 from the key-parsing guard and
// never reaches the fetch path these tests are about.
const ABSENT_KEY = "1".repeat(32);

function origin(): string {
  return new URL(shellUrl!).origin;
}

/** The contract key the Rust harness published the fixture webapp under. */
function fixtureKey(): string {
  const match = new URL(shellUrl!).pathname.match(
    /\/v\d+\/contract\/web\/([^/]+)\//,
  );
  if (!match) {
    throw new Error(`could not read a contract key out of ${shellUrl}`);
  }
  return match[1];
}

test("a subresource of a contract the node cannot find answers 503, not an opaque failure", async ({
  request,
}) => {
  // Deliberately a subresource URL, not the shell root: the root has always
  // fetched unconditionally, and it is the subresource path that #4417 gated
  // and this change re-opened.
  const response = await request.get(
    `${origin()}/v1/contract/web/${ABSENT_KEY}/image.png`,
    { failOnStatusCode: false },
  );

  // 503 rather than the pre-#3942 instant 404. This IS a behaviour change and
  // it is the intended one: the node genuinely asked the network and the
  // answer was "not found yet", which for a contract that is merely slow to
  // propagate must not be reported as terminal.
  expect(response.status()).toBe(503);
  expect(response.headers()["retry-after"]).toBe("60");

  // The header that decides whether a sandboxed iframe sees a status at all.
  // Without it the engine reports a CORS failure and the real status is
  // invisible to the page and to anyone reading a bug report from it.
  expect(response.headers()["access-control-allow-origin"]).toBe("*");
});

test("a subresource loads without the contract root being visited first", async ({
  page,
}) => {
  // The positive direction, in the engine: fetch a page out of the fixture
  // contract by subresource URL in a browser that has never loaded its shell.
  //
  // Honest about what this does and does not prove. The harness node PUBLISHED
  // this fixture, so it stores it, so #4417's presence gate would also have let
  // it through — this test does not discriminate the fix. What it does cover is
  // everything downstream of the decision that only a real engine exercises:
  // the route wiring, the sandbox headers, and that the response is a document
  // the browser will render rather than an error it cannot classify. The
  // discriminating case needs a contract the node has never stored, which a
  // single-node harness cannot produce.
  const response = await page.goto(
    `${origin()}/v1/contract/web/${fixtureKey()}/page2.html`,
    { waitUntil: "domcontentloaded" },
  );

  expect(response).not.toBeNull();
  expect(response!.status()).toBe(200);
  await expect(page.locator("body")).not.toBeEmpty();
});

test("a traversal path is refused without waiting on a network fetch", async ({
  request,
}) => {
  // Rejected before the fetch, so this must come back promptly rather than
  // after a GET's retry loop. The timing assertion is deliberately loose: it is
  // there to catch the ordering regression (a full network round trip on a path
  // that can never resolve), not to measure the server.
  const started = Date.now();
  const response = await request.get(
    `${origin()}/v1/contract/web/${ABSENT_KEY}/..%2f..%2fetc%2fhostname`,
    { failOnStatusCode: false },
  );
  const elapsed = Date.now() - started;

  expect(response.status()).toBe(400);
  expect(elapsed).toBeLessThan(5_000);
  expect(await response.text()).not.toContain("root:");
});
