import { test, expect } from "@playwright/test";

// The web route's answer for a contract the node could not locate.
//
// Rust-level coverage of this lives in
// `crates/core/src/server/path_handlers.rs::handle_get_response_maps_network_not_found_to_transient_retry`,
// but that test calls the handler and `IntoResponse` directly. It cannot see
// route wiring, and — the reason this file exists — it cannot see whether the
// page a browser actually receives reloads itself.
//
// That property is the whole point of the fix. The node returns the same
// `ContractResponse::NotFound` for a contract that has not propagated yet and
// for a key that will never resolve, so an auto-refreshing page would re-issue a
// network GET every minute for as long as any tab sat on a mistyped URL. See
// `.claude/rules/browser-assets.md`, "assume every open tab pays the cost".

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
// It has to parse, or the route answers 400 from the key-parsing guard and never
// reaches the fetch path this test is about.
const ABSENT_KEY = "1".repeat(32);

function absentContractUrl(): string {
  const origin = new URL(shellUrl!).origin;
  return `${origin}/v1/contract/web/${ABSENT_KEY}/`;
}

test("a contract the node cannot find answers 503 and does not reload itself", async ({
  page,
}) => {
  const response = await page.goto(absentContractUrl(), {
    waitUntil: "domcontentloaded",
  });
  expect(response).not.toBeNull();

  // 503, not 500: a client must be told to come back, because "not found" here
  // routinely means "not found YET". And not 404, which a crawler treats as
  // terminal — that would permanently drop a contract that was merely slow to
  // propagate.
  expect(response!.status()).toBe(503);
  expect(response!.headers()["retry-after"]).toBe("60");
  expect(response!.headers()["cache-control"]).toBe("no-store");

  // No auto-refresh, in the DOM the browser actually parsed.
  await expect(page.locator('meta[http-equiv="refresh"]')).toHaveCount(0);

  // And the page says which situation this is, rather than reading as a crash.
  await expect(page.locator("h1")).toContainText(/could ?n[o']t find/i);
});

test("the not-found page issues no further requests on its own", async ({
  page,
}) => {
  const url = absentContractUrl();
  const requested: string[] = [];
  page.on("request", (r) => requested.push(r.url()));

  await page.goto(url, { waitUntil: "networkidle" });
  const afterLoad = requested.length;

  // A meta-refresh fires on a timer, so settling once proves nothing on its own.
  // Wait past a refresh interval and assert the count did not move. Kept well
  // under RETRY_REFRESH_SECS (60s) would prove nothing either, so this waits
  // long enough to catch a short interval while staying inside the suite budget:
  // any reload-driven request lands as a repeat of the same URL.
  await page.waitForTimeout(5_000);

  const repeats = requested.filter((r) => r === url).length;
  expect(
    requested.length,
    `page issued ${requested.length - afterLoad} request(s) after load; ` +
      `a reloading not-found page costs one network GET per tab per interval`,
  ).toBe(afterLoad);
  expect(repeats).toBe(1);
});
