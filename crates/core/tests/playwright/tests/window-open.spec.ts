import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

// Behavioral regression tests for the programmatic `window.open` override in
// the injected navigation interceptor
// (crates/core/src/server/path_handlers/assets/navigation_interceptor.js).
//
// Bug (freenet-core#4645): on a hosted node the contract app runs in a
// sandboxed iframe with an opaque origin and no `allow-popups-to-escape-sandbox`.
// A new tab the app opens with `window.open()` inherits that sandbox, gets a
// null origin, can't read the per-user access key in localStorage, and dead-ends
// on the "Open this app in a normal tab" page. The fix overrides `window.open`
// inside the iframe so http(s) NEW-window opens are forwarded to the shell (via
// the same `open_url` postMessage the anchor interceptor uses), which opens them
// with the shell's real origin.
//
// These tests load the REAL interceptor asset inside a genuinely sandboxed
// (opaque-origin) iframe — the override guards on `window.parent !== window`, so
// a top-level eval (like websocket-shim.spec.ts uses) would just fall back to
// native and prove nothing. The iframe posts back what it observed; the parent
// collects the forwarded `open_url` messages plus the per-call return values and
// the iframe's origin. No running node is required, so this runs inside
// playwright-shell.yml like websocket-shim.spec.ts.

const INTERCEPTOR = readFileSync(
  join(
    __dirname,
    "..",
    "..",
    "..",
    "src",
    "server",
    "path_handlers",
    "assets",
    "navigation_interceptor.js",
  ),
  "utf8",
);

type CallResult = { label: string; arg: string; ret: string };
type Forwarded = { url: string; shiftKey: unknown };
type Collected = {
  forwarded: Forwarded[];
  report: {
    origin: string;
    results: CallResult[];
    nativeCalls: string[];
    error?: string;
  } | null;
};

// Build the sandboxed-iframe document: a spy standing in for the native
// window.open (so fallbacks are observable without spawning real popups), then
// the real interceptor (which binds the spy as its native fallback), then an
// exercise script that calls the overridden window.open and reports back.
function buildSrcdoc(baseHref: string, calls: Array<{ label: string; expr: string }>): string {
  const exercise = calls
    .map(
      (c) =>
        `tryOpen(${JSON.stringify(c.label)}, function () { return window.open(${c.expr}); });`,
    )
    .join("\n    ");
  // NB: the interceptor asset contains no literal "</script>", so embedding it
  // in an inner <script> is safe. srcdoc is passed to page.evaluate as a JS
  // argument (not HTML-parsed by the parent), so the parent script is unaffected.
  return `<!doctype html><html><head><base href="${baseHref}"></head><body>
<script>
  window.__nativeCalls = [];
  window.open = function (u) { window.__nativeCalls.push(String(u)); return { __spy: true }; };
</script>
<script>${INTERCEPTOR}</script>
<script>
  try {
    var results = [];
    function tryOpen(label, fn) {
      var r, err = null;
      try { r = fn(); } catch (e) { err = String(e); }
      results.push({
        label: label,
        arg: label,
        ret: err ? ('THREW:' + err) : (r && r.__spy ? 'NATIVE_SPY' : (r === null ? 'null' : typeof r)),
      });
    }
    ${exercise}
    parent.postMessage({ __harness_report__: true, results: results, nativeCalls: window.__nativeCalls, origin: String(window.origin) }, '*');
  } catch (e) {
    parent.postMessage({ __harness_report__: true, error: String(e) }, '*');
  }
</script>
</body></html>`;
}

async function runHarness(
  page: import("@playwright/test").Page,
  srcdoc: string,
): Promise<Collected> {
  await page.goto("about:blank");
  await page.evaluate((srcdocArg: string) => {
    (window as unknown as { __collected: Collected }).__collected = {
      forwarded: [],
      report: null,
    };
    window.addEventListener("message", (e: MessageEvent) => {
      const m = e.data as Record<string, unknown>;
      const c = (window as unknown as { __collected: Collected }).__collected;
      if (m && m.__freenet_shell__ && m.type === "open_url") {
        c.forwarded.push({ url: m.url as string, shiftKey: m.shiftKey });
      }
      if (m && m.__harness_report__) {
        c.report = m as unknown as Collected["report"];
      }
    });
    const f = document.createElement("iframe");
    f.setAttribute("sandbox", "allow-scripts allow-popups");
    f.srcdoc = srcdocArg;
    document.body.appendChild(f);
  }, srcdoc);

  await page
    .waitForFunction(
      () => (window as unknown as { __collected: Collected }).__collected.report !== null,
      { timeout: 5000 },
    )
    .catch(() => {});
  return page.evaluate(
    () => (window as unknown as { __collected: Collected }).__collected,
  );
}

const BASE = "https://node.example/v1/contract/web/KEY/?__sandbox=1";

test("forwards http(s) new-window opens to the shell as an absolute URL, returning null", async ({
  page,
}) => {
  const collected = await runHarness(
    page,
    buildSrcdoc(BASE, [
      { label: "relative", expr: "'page2/sub#frag'" },
      { label: "absolute-https", expr: "'https://example.com/x?q=1'" },
      { label: "absolute-http", expr: "'http://example.org/y'" },
      { label: "url-object", expr: "new URL('https://obj.example/u')" },
    ]),
  );
  const report = collected.report!;
  expect(report.error).toBeUndefined();
  // The iframe is genuinely sandboxed (opaque origin) — the exact failing
  // condition #4645 is about.
  expect(report.origin).toBe("null");

  const fwd = collected.forwarded.map((f) => f.url);
  // Relative resolves against the iframe base to an absolute URL.
  expect(fwd).toContain("https://node.example/v1/contract/web/KEY/page2/sub#frag");
  expect(fwd).toContain("https://example.com/x?q=1");
  expect(fwd).toContain("http://example.org/y");
  // URL objects are coerced and forwarded, not dead-ended.
  expect(fwd).toContain("https://obj.example/u");
  expect(collected.forwarded.length).toBe(4);
  // All forwards ask for a plain tab.
  expect(collected.forwarded.every((f) => f.shiftKey === false)).toBe(true);
  // The forwarded case drops the WindowProxy (returns null); none went native.
  for (const r of report.results) expect(r.ret).toBe("null");
  expect(report.nativeCalls.length).toBe(0);
});

test("falls back to native for non-http(s), empty, and in-place (_self) targets", async ({
  page,
}) => {
  const collected = await runHarness(
    page,
    buildSrcdoc(BASE, [
      { label: "about-blank", expr: "'about:blank'" },
      { label: "empty", expr: "''" },
      { label: "no-arg", expr: "" },
      { label: "javascript", expr: "'javascript:alert(1)'" },
      { label: "blob", expr: "'blob:https://x/abc'" },
      { label: "data", expr: "'data:text/html,x'" },
      { label: "self-target", expr: "'https://example.com/z', '_self'" },
      // Target keywords are ASCII case-insensitive: _SELF must stay native too.
      { label: "self-upper", expr: "'https://example.com/zz', '_SELF'" },
    ]),
  );
  const report = collected.report!;
  expect(report.error).toBeUndefined();
  // Nothing was forwarded — every case must reach the native open.
  expect(collected.forwarded.length).toBe(0);
  for (const r of report.results) expect(r.ret).toBe("NATIVE_SPY");
  // _self / _SELF forward a real URL to native (in-place navigation).
  expect(report.nativeCalls).toContain("https://example.com/z");
  expect(report.nativeCalls).toContain("https://example.com/zz");
  expect(report.nativeCalls).toContain("about:blank");
  expect(report.nativeCalls).toContain("javascript:alert(1)");
});

test("forwards absolute external URLs without reserializing the query string", async ({
  page,
}) => {
  // The __sandbox strip must NOT touch an absolute external target: reserializing
  // its query (%20 -> +, ~ -> %7E) could invalidate signed/opaque links.
  const signed = "https://example.com/o?X-Amz-Signature=abc&a=%2F%2f%20~";
  const collected = await runHarness(
    page,
    buildSrcdoc(BASE, [{ label: "signed-external", expr: JSON.stringify(signed) }]),
  );
  const report = collected.report!;
  expect(report.error).toBeUndefined();
  expect(collected.forwarded.length).toBe(1);
  const url = collected.forwarded[0].url;
  // No form-encoding corruption and no dropped/added params.
  expect(url).not.toContain("+");
  expect(url).toContain("%20");
  expect(url).toContain("~");
  expect(url).toContain("X-Amz-Signature=abc");
});

test("does not forward loopback targets (open_url refuses them); stays native", async ({
  page,
}) => {
  const collected = await runHarness(
    page,
    buildSrcdoc(BASE, [
      { label: "loopback-ip", expr: "'http://127.0.0.1:8080/x'" },
      { label: "loopback-name", expr: "'http://localhost:8080/x'" },
    ]),
  );
  const report = collected.report!;
  expect(report.error).toBeUndefined();
  expect(collected.forwarded.length).toBe(0);
  expect(report.nativeCalls).toContain("http://127.0.0.1:8080/x");
  expect(report.nativeCalls).toContain("http://localhost:8080/x");
});

test("strips the __sandbox routing param so a hash-only open serves the shell wrapper", async ({
  page,
}) => {
  // A hash-only target inherits ?__sandbox=1 from the iframe base; forwarding
  // that unchanged would open raw sandbox content top-level (no shell wrapper).
  const collected = await runHarness(page, buildSrcdoc(BASE, [{ label: "hash-only", expr: "'#deep/link'" }]));
  const report = collected.report!;
  expect(report.error).toBeUndefined();
  expect(collected.forwarded.length).toBe(1);
  const url = collected.forwarded[0].url;
  expect(url).not.toContain("__sandbox");
  expect(url).toBe("https://node.example/v1/contract/web/KEY/#deep/link");
});

test("does not override window.open at top level (parent guard)", async ({ page }) => {
  // Loaded top-level, window.parent === window, so the override must NOT engage
  // (otherwise it would break the shell page's own window.open). Evaluate the
  // interceptor at top level with a postMessage spy and confirm no forward.
  await page.goto("about:blank");
  const forwarded = await page.evaluate((src: string) => {
    const posts: unknown[] = [];
    (window as unknown as { postMessage: unknown }).postMessage = (m: unknown) => posts.push(m);
    let nativeCalled = false;
    (window as unknown as { open: unknown }).open = () => {
      nativeCalled = true;
      return null;
    };
    // Indirect eval runs the IIFE in global scope (window.parent === window).
    (0, eval)(src);
    (window as unknown as { open: (u: string) => unknown }).open("https://example.com/x");
    return { posts, nativeCalled };
  }, INTERCEPTOR);
  expect(forwarded.nativeCalled).toBe(true);
  expect(forwarded.posts.length).toBe(0);
});
