import { test, expect, type Response } from "@playwright/test";

// Contract web responses must be `Cache-Control: no-store` (freenet-core#5323).
//
// A contract's bundle is mutable at a stable URL: `fdev website update` changes
// what `/v1/contract/web/<key>/` returns while the URL stays the same. With no
// cache directive a browser that has the app open keeps running the superseded
// build until a hard refresh, and the app cannot fix that itself, because the
// stale document is the one that would have carried the fix.
//
// The header is applied as a layer over the contract-content routes in
// `client_api/v1.rs` and `v2.rs`. The Rust router test
// (`contract_content_routes_are_served_no_store` in `client_api.rs`) drives
// every pipeline that emits contract bytes, including the JS rewrite branch and
// `ServeFile`, which this fixture has no assets to reach. What it cannot see is
// the node's full production composition and what an engine actually receives
// for the documents it loads, including the sandboxed iframe, which is what
// this spec checks.

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

/** `/v1/contract/web/<key>` for the fixture, without the trailing slash. */
function contractBase(): string {
  const url = new URL(shellUrl!);
  const match = url.pathname.match(/^(\/v\d+\/contract\/web\/[^/]+)\//);
  if (!match) {
    throw new Error(`could not read a contract key out of ${shellUrl}`);
  }
  return `${url.origin}${match[1]}`;
}

test("the shell and the sandboxed app document are served no-store", async ({
  page,
}) => {
  const base = contractBase();
  const contractResponses: Response[] = [];
  page.on("response", (response) => {
    if (response.url().startsWith(`${base}/`)) {
      contractResponses.push(response);
    }
  });

  await page.goto(`${base}/`);
  // The iframe's document is the contract's own HTML, served by a different
  // pipeline from the shell. Waiting for its content means its response has
  // been recorded too.
  const app = page.frameLocator("iframe");
  await expect(app.locator("body")).not.toBeEmpty();

  const isSandbox = (r: Response) =>
    new URL(r.url()).searchParams.get("__sandbox") === "1";
  // Both documents must have been seen, or the assertions below could pass
  // over a set that is missing the pipeline they are meant to cover.
  expect(contractResponses.some((r) => !isSandbox(r))).toBe(true);
  expect(contractResponses.some(isSandbox)).toBe(true);

  for (const response of contractResponses) {
    // `allHeaders` rather than `headers`: the latter omits some headers in
    // some engines.
    const headers = await response.allHeaders();
    expect(
      headers["cache-control"],
      `${response.url()} must be no-store, or an open tab keeps running the ` +
        `old bundle after a republish (#5323)`,
    ).toBe("no-store");
  }
});

test("the no-trailing-slash redirect stays cacheable", async ({ request }) => {
  // It carries no contract content, and it is a 308 precisely so that browsers
  // can cache it (#4019). The no-store layer must not reach it.
  const response = await request.get(contractBase(), { maxRedirects: 0 });
  expect(response.status()).toBe(308);
  expect(response.headers()["cache-control"]).toBeUndefined();
});
