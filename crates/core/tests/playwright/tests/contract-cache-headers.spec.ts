import { test, expect, type Response } from "@playwright/test";

// Contract HTML documents must be `Cache-Control: no-store` (freenet-core#5323).
//
// A contract's bundle is mutable at a stable URL: `fdev website update` changes
// what `/v1/contract/web/<key>/` returns while the URL stays the same. A cached
// document pins an open tab to the superseded build, and the app cannot fix
// that itself, because the stale document is the one that would have carried
// the fix. Assets are deliberately left alone (see `no_store_html` in
// `client_api.rs`).
//
// The header is applied by a layer over the contract-content routes in
// `client_api/v1.rs` and `v2.rs`, keyed on the HTML `Content-Type`. The Rust
// router test (`contract_html_is_served_no_store_and_assets_are_not` in
// `client_api.rs`) drives every pipeline, including the JS rewrite branch and
// `ServeFile` assets, which this fixture never reaches because it contains
// only HTML. What it cannot see is the router as the harness node composes it,
// and what an engine actually receives for the documents it loads, including
// the sandboxed iframe. That is what this spec checks.

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
  // Only documents are covered; assets keep their caching (#5888). The fixture
  // is all HTML, so this skips nothing today.
  const documents: { response: Response; cacheControl?: string }[] = [];
  for (const response of contractResponses) {
    // `allHeaders` rather than `headers`: the latter omits some headers in
    // some engines.
    const headers = await response.allHeaders();
    if (/^(text\/html|application\/xhtml\+xml)\b/i.test(headers["content-type"] ?? "")) {
      documents.push({ response, cacheControl: headers["cache-control"] });
    }
  }
  // Both documents must be among them, or the assertions below could pass
  // over a set that is missing the pipeline they are meant to cover.
  expect(documents.some(({ response }) => !isSandbox(response))).toBe(true);
  expect(documents.some(({ response }) => isSandbox(response))).toBe(true);

  for (const { response, cacheControl } of documents) {
    expect(
      cacheControl,
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
