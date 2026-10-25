import { test, expect, type Page, type Request } from "@playwright/test";

// The shell's permission routes and the hosted user's token.
//
// A hosted node requires the user's token on `/permission/pending`,
// `/permission/{nonce}/respond` and the permission WebSocket, and shows each
// user only their own prompts. The shell sends the token as the
// `X-Freenet-User-Token` header on the two fetches and as `?userToken=` on the
// socket (a browser cannot set a header on a WebSocket handshake), and only
// from a page served over https. On a node that is not hosted it sends no
// token at all.
//
// Tests tagged @hosted run only in the hosted pass of the Rust harness
// (crates/core/tests/playwright_shell.rs), which restarts the published node
// in hosted mode behind a small TLS proxy and runs
// `npx playwright test --grep @hosted`. The untagged test runs in the ordinary
// pass, which uses `--grep-invert @hosted`.
//
// Why a browser test: the Rust tests prove what the node does with a request,
// and the Node unit tests prove what the extracted helpers build. Neither
// shows that the shell, running in a real engine behind TLS, actually sends
// the token on each of the three channels, or that the sandboxed app never
// receives it.

const shellUrl = process.env.FREENET_SHELL_URL;

const TOKEN_HEADER = "x-freenet-user-token";

test.beforeAll(() => {
  if (!shellUrl) {
    throw new Error(
      "FREENET_SHELL_URL is not set. These tests are normally driven by the " +
        "Rust harness (crates/core/tests/playwright_shell.rs).",
    );
  }
});

// Records the shell's own permission traffic: every /permission/pending
// request with its headers and status, and every permission socket URL.
function recordPermissionTraffic(page: Page) {
  const pending: { headers: Record<string, string>; status: number | null }[] = [];
  const sockets: string[] = [];
  page.on("requestfinished", async (req: Request) => {
    if (!new URL(req.url()).pathname.endsWith("/permission/pending")) return;
    if (req.frame() !== page.mainFrame()) return;
    const resp = await req.response();
    pending.push({ headers: await req.allHeaders(), status: resp ? resp.status() : null });
  });
  page.on("websocket", (ws) => {
    if (ws.url().includes("/permission/events/ws")) sockets.push(ws.url());
  });
  return { pending, sockets };
}

test("@hosted the shell sends its user token on the permission fetch and socket", async ({
  page,
}) => {
  const traffic = recordPermissionTraffic(page);
  await page.goto(shellUrl!);

  await expect.poll(() => traffic.pending.length).toBeGreaterThan(0);
  await expect.poll(() => traffic.sockets.length).toBeGreaterThan(0);

  const first = traffic.pending[0];
  const token = first.headers[TOKEN_HEADER];
  expect(token, "the pending fetch must carry the user token").toBeTruthy();
  expect(first.status, "the node must accept the tokened pending fetch").toBe(200);

  const socket = new URL(traffic.sockets[0]);
  expect(socket.protocol).toBe("wss:");
  expect(
    socket.searchParams.get("userToken"),
    "the permission socket must carry the same token",
  ).toBe(token);

  // Non-vacuity: the same fetch WITHOUT the header is refused, so the 200
  // above is the token at work and not a node that ignores it.
  const bare = await page.evaluate(async () => (await fetch("/permission/pending")).status);
  expect(bare, "a hosted node must refuse the pending fetch without a token").toBe(403);
});

test("@hosted an answer carries the user token (an unknown prompt is 404, not 403)", async ({
  page,
}) => {
  // Stand in for the node's permission socket so a prompt card can be shown
  // without running a delegate. Only the socket is replaced; the answer POST
  // goes to the real node, which answers 404 for a nonce it does not hold
  // when the token is accepted, and 403 when it is missing.
  let push: ((frame: string) => void) | null = null;
  await page.routeWebSocket(/\/permission\/events\/ws/, (ws) => {
    push = (frame) => ws.send(frame);
  });
  const traffic = recordPermissionTraffic(page);
  await page.goto(shellUrl!);
  await expect.poll(() => push !== null).toBe(true);
  // Let the reconcile that runs when the socket opens finish first, so it
  // cannot hide the card shown below.
  await expect.poll(() => traffic.pending.length).toBeGreaterThan(0);

  const nonce = "0".repeat(32);
  push!(
    JSON.stringify({
      event: "prompt_added",
      data: {
        nonce,
        author: "delegate",
        message: "Playwright test prompt",
        labels: ["Allow", "Deny"],
        delegate_key: "test-delegate-key",
        caller: { kind: "none", hash: null },
      },
    }),
  );

  const button = page.locator("#__freenet_perm_overlay button", { hasText: "Allow" }).first();
  await expect(button).toBeVisible();
  const [answer] = await Promise.all([
    page.waitForRequest((r) => r.url().endsWith(`/permission/${nonce}/respond`)),
    button.click(),
  ]);
  const headers = await answer.allHeaders();
  const token = traffic.pending[0].headers[TOKEN_HEADER];
  expect(token, "the shell must have a user token on this hosted node").toBeTruthy();
  expect(headers[TOKEN_HEADER], "the answer must carry the user token").toBe(token);
  const resp = await answer.response();
  expect(resp?.status(), "the node accepted the token and does not hold the nonce").toBe(404);

  // Non-vacuity: the same answer without the token is refused, so the 404
  // above is the token at work.
  const bare = await page.evaluate(
    async (n) =>
      (
        await fetch(`/permission/${n}/respond`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ index: 0 }),
        })
      ).status,
    nonce,
  );
  expect(bare, "a hosted node must refuse an answer without a token").toBe(403);
});

test("@hosted the app iframe never receives the user token", async ({ page }) => {
  const traffic = recordPermissionTraffic(page);
  const frameRequests: { url: string; headers: Record<string, string> }[] = [];
  page.on("requestfinished", async (req) => {
    if (req.frame() === page.mainFrame()) return;
    frameRequests.push({ url: req.url(), headers: await req.allHeaders() });
  });
  await page.goto(shellUrl!);
  await expect(page.frameLocator("#app").locator("body")).toBeVisible();
  await expect.poll(() => traffic.pending.length).toBeGreaterThan(0);
  const token = traffic.pending[0].headers[TOKEN_HEADER];
  expect(token).toBeTruthy();
  await expect.poll(() => frameRequests.length).toBeGreaterThan(0);

  for (const r of frameRequests) {
    expect(r.headers[TOKEN_HEADER], `iframe request ${r.url} must carry no token`).toBeUndefined();
    expect(r.url.includes(token!), `iframe request ${r.url} must not contain the token`).toBe(
      false,
    );
  }
  for (const frame of page.frames()) {
    if (frame === page.mainFrame()) continue;
    expect(frame.url().includes(token!), "an iframe URL must not contain the token").toBe(false);
  }
});

test("the shell sends no user token on the permission routes of a node that is not hosted", async ({
  page,
}) => {
  const traffic = recordPermissionTraffic(page);
  await page.goto(shellUrl!);
  await expect.poll(() => traffic.pending.length).toBeGreaterThan(0);
  await expect.poll(() => traffic.sockets.length).toBeGreaterThan(0);

  expect(traffic.pending[0].headers[TOKEN_HEADER]).toBeUndefined();
  expect(traffic.pending[0].status).toBe(200);
  expect(new URL(traffic.sockets[0]).searchParams.has("userToken")).toBe(false);
});
