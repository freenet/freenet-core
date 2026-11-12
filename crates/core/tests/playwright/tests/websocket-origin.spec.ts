import { test, expect, type Page } from "@playwright/test";

// Which browser pages may open the node's client-API WebSocket, checked in a
// real engine. Rust tests cover the handshake with hand-built headers; this
// covers what a browser actually sends.
//
// - The node's own shell page, and a webapp inside its sandboxed iframe (via
//   the injected shim and the shell's bridge), must connect.
// - A page on an unrelated origin, and a sandboxed `Origin: null` frame inside
//   it, must not.
// - The shell's app-identity cookie must not be readable from script.
//
// Engine note: Chromium refuses the two cross-origin cases by itself (Local
// Network Access), so only Firefox and WebKit exercise the node's Origin check
// there. Do not narrow the engine matrix.

const shellUrl = process.env.FREENET_SHELL_URL;

function commandSocketUrl(): string {
  const u = new URL(shellUrl as string);
  return `ws://${u.host}/v1/contract/command?encodingProtocol=native`;
}

// Resolves to "open" or "failed" for a WebSocket opened by script on `page`.
async function tryOpen(page: Page, url: string): Promise<string> {
  return page.evaluate(
    (wsUrl: string) =>
      new Promise<string>((resolve) => {
        const ws = new WebSocket(wsUrl);
        const timer = setTimeout(() => resolve("timeout"), 15_000);
        ws.onopen = () => {
          clearTimeout(timer);
          ws.close();
          resolve("open");
        };
        ws.onerror = () => {
          clearTimeout(timer);
          resolve("failed");
        };
      }),
    url,
  );
}

test.beforeEach(() => {
  test.skip(!shellUrl, "FREENET_SHELL_URL not set (run via the Rust harness)");
});

test("the shell page can open the command WebSocket", async ({ page }) => {
  await page.goto(shellUrl as string);
  expect(await tryOpen(page, commandSocketUrl())).toBe("open");
});

test("a webapp in the sandboxed iframe connects through the shell bridge", async ({
  page,
}) => {
  await page.goto(shellUrl as string);
  const frame = page.frameLocator("iframe");
  await expect(frame.locator("#title")).toBeVisible();
  const appFrame = page.frames().find((f) => f !== page.mainFrame());
  expect(appFrame, "the sandboxed app frame must exist").toBeTruthy();
  const result = await appFrame!.evaluate(
    (wsUrl: string) =>
      new Promise<string>((resolve) => {
        // `WebSocket` here is the injected shim, which asks the shell to open
        // the real socket; the node sees the shell's Origin, not `null`.
        const ws = new WebSocket(wsUrl);
        const timer = setTimeout(() => resolve("timeout"), 15_000);
        ws.onopen = () => {
          clearTimeout(timer);
          ws.close();
          resolve("open");
        };
        ws.onerror = () => {
          clearTimeout(timer);
          resolve("failed");
        };
      }),
    commandSocketUrl(),
  );
  expect(result).toBe("open");
});

test("a page on another origin cannot open the command WebSocket", async ({ page }) => {
  // Serve a page at an unrelated origin without any network access.
  await page.route("http://unrelated-site.test/**", (route) =>
    route.fulfill({ status: 200, contentType: "text/html", body: "<html><body>x</body></html>" }),
  );
  await page.goto("http://unrelated-site.test/");
  expect(await tryOpen(page, commandSocketUrl())).toBe("failed");
});

test("a sandboxed (Origin: null) frame on another site cannot open the command WebSocket", async ({
  page,
}) => {
  const wsUrl = commandSocketUrl();
  const frameHtml = `<script>
    var ws = new WebSocket(${JSON.stringify(wsUrl)});
    ws.onopen = function () { parent.postMessage("open", "*"); ws.close(); };
    ws.onerror = function () { parent.postMessage("failed", "*"); };
  </script>`;
  // The listener is installed by the page itself, before the frame exists, so
  // the frame's verdict cannot arrive before anyone is listening.
  const body = `<html><body>
    <script>
      window.__verdict = null;
      addEventListener("message", function (e) { window.__verdict = String(e.data); });
    </script>
    <iframe sandbox="allow-scripts" srcdoc="${frameHtml
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")}"></iframe>
  </body></html>`;
  await page.route("http://unrelated-site.test/**", (route) =>
    route.fulfill({ status: 200, contentType: "text/html", body }),
  );
  await page.goto("http://unrelated-site.test/");
  await expect
    .poll(() => page.evaluate(() => (window as unknown as { __verdict: string | null }).__verdict), {
      timeout: 15_000,
    })
    .toBe("failed");
});

test("the shell's app-identity cookie is not readable from script", async ({ page }) => {
  const response = await page.goto(shellUrl as string);
  const setCookie = (await response!.allHeaders())["set-cookie"] ?? "";
  expect(setCookie, "the shell must still set its token cookie").toMatch(/authorization=/i);
  expect(await page.evaluate(() => document.cookie)).not.toMatch(/authorization=/i);
});
