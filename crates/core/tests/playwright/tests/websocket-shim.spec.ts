import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

// Regression tests for the outbound send() path of the injected WebSocket shim
// (crates/core/src/server/path_handlers/assets/websocket_shim.js).
//
// Bug (fixed here): the sandboxed webapp (River) runs in a separate OS process
// (an OOPIF). wasm-bindgen hands `send()` a `Uint8Array`, which is NOT
// `instanceof ArrayBuffer`, so the pre-fix code left the postMessage transfer
// list EMPTY and every outbound WS frame was structured-clone COPIED across the
// process boundary instead of transferred zero-copy. On tab-focus the webapp
// flushes ~15 queued frames (~740 KB) at once and each copy reallocated under
// ArrayBuffer GC pressure -> a ~2.7 s main-thread CPU burst (spun the laptop
// fan). A live CDP A/B patch proved the shell handler self-time dropped
// 2.73 s -> 0.00 s with identical bytes still reaching the node.
//
// The fix transfers the backing buffer for ArrayBuffer VIEWS too. Because the
// Uint8Array may be a view into WASM linear memory (transferring
// `memory.buffer` would detach ALL of WASM memory and crash the app), it copies
// the view out once in-process via `.slice()` and transfers that copy's buffer.
//
// These tests load the real shim JS in a headless Chromium, stub
// `window.parent.postMessage` (top-level page, so `window.parent === window`)
// to capture the exact transfer list the shim passes, and assert the transfer
// semantics per payload type. The stub deliberately does NOT forward to the
// real postMessage, so the buffers are never actually detached and the
// delivered bytes remain readable for assertion.
//
// This spec needs a browser but NOT a running node, so unlike shell.spec.ts it
// does not require FREENET_SHELL_URL. It still runs inside the dedicated
// playwright-shell.yml CI job (which invokes `npx playwright test`).

const shimSource = readFileSync(
  join(
    __dirname,
    "..",
    "..",
    "..",
    "src",
    "server",
    "path_handlers",
    "assets",
    "websocket_shim.js",
  ),
  "utf8",
);

type SendProbe = {
  transferLen: number;
  transfer0IsArrayBuffer: boolean;
  transfer0ByteLength: number | null;
  dataBytes: number[];
  dataBufferIsTransferred: boolean;
};

type Probes = {
  uint8: SendProbe;
  subarrayView: SendProbe & { sharedBufferByteLength: number };
  arraybuffer: SendProbe & { dataIsTransfer0: boolean };
  string: SendProbe & { dataEquals: boolean };
};

test("send() transfers ArrayBuffer views zero-copy without detaching WASM memory", async ({
  page,
}) => {
  await page.goto("about:blank");

  const probes = (await page.evaluate((src: string) => {
    // Capture every outbound `send` frame plus the transfer list the shim
    // passed. window.parent === window for a top-level page, so overriding
    // window.postMessage intercepts window.parent.postMessage. We do NOT call
    // through to the native postMessage: that keeps the buffers un-detached so
    // the delivered bytes stay readable below.
    const sends: Array<{ message: { data: unknown }; transfer: Transferable[] }> = [];
    (window as unknown as { postMessage: unknown }).postMessage = function (
      message: { __freenet_ws__?: boolean; type?: string; data: unknown },
      _targetOrigin: string,
      transfer?: Transferable[],
    ) {
      if (message && message.__freenet_ws__ && message.type === "send") {
        sends.push({ message, transfer: transfer || [] });
      }
    };

    // Load the real shim (an IIFE that installs window.WebSocket). Indirect
    // eval runs it in global scope.
    (0, eval)(src);

    const WS = (window as unknown as { WebSocket: new (url: string) => any }).WebSocket;

    function capture(payload: unknown): {
      message: { data: unknown };
      transfer: Transferable[];
    } {
      const ws = new WS("ws://test.invalid");
      ws.readyState = 1; // shim's send() throws unless OPEN; no real handshake here.
      sends.length = 0;
      ws.send(payload);
      return sends[sends.length - 1];
    }

    function toBytes(data: unknown): number[] {
      if (data instanceof ArrayBuffer) return Array.from(new Uint8Array(data));
      if (ArrayBuffer.isView(data)) {
        const v = data as ArrayBufferView;
        return Array.from(
          new Uint8Array(v.buffer, v.byteOffset, v.byteLength),
        );
      }
      return [];
    }

    function probe(s: { message: { data: unknown }; transfer: Transferable[] }): {
      transferLen: number;
      transfer0IsArrayBuffer: boolean;
      transfer0ByteLength: number | null;
      dataBytes: number[];
      dataBufferIsTransferred: boolean;
    } {
      const data = s.message.data;
      const t0 = s.transfer[0];
      const dataBuffer = ArrayBuffer.isView(data)
        ? (data as ArrayBufferView).buffer
        : data instanceof ArrayBuffer
          ? data
          : null;
      return {
        transferLen: s.transfer.length,
        transfer0IsArrayBuffer: t0 instanceof ArrayBuffer,
        transfer0ByteLength: t0 instanceof ArrayBuffer ? t0.byteLength : null,
        dataBytes: toBytes(data),
        dataBufferIsTransferred: dataBuffer !== null && dataBuffer === t0,
      };
    }

    // 1) Plain Uint8Array (the wasm-bindgen case that regressed).
    const uint8 = probe(capture(new Uint8Array([1, 2, 3])));

    // 2) Uint8Array that is a SUBARRAY view over a larger buffer (models a view
    // into WASM linear memory). The fix must transfer a fresh 3-byte copy, NOT
    // the shared 6-byte backing buffer.
    const shared = new Uint8Array([0, 1, 2, 3, 4, 5]);
    const view = shared.subarray(1, 4); // [1,2,3], byteOffset 1 over a 6-byte buffer
    const sub = capture(view);
    const subProbe = probe(sub);
    const subarrayView = {
      ...subProbe,
      sharedBufferByteLength: shared.buffer.byteLength,
      // The transferred buffer must be distinct from the shared backing buffer.
      transfer0IsSharedBuffer: sub.transfer[0] === shared.buffer,
    };

    // 3) A real ArrayBuffer still transfers itself (unchanged behaviour).
    const ab = new Uint8Array([9, 9]).buffer;
    const abSend = capture(ab);
    const arraybuffer = {
      ...probe(abSend),
      dataIsTransfer0: abSend.message.data === abSend.transfer[0],
    };

    // 4) A string transfers nothing (strings cannot be transferred).
    const strSend = capture("hello");
    const string = {
      ...probe(strSend),
      dataEquals: strSend.message.data === "hello",
    };

    return { uint8, subarrayView, arraybuffer, string };
  }, shimSource)) as unknown as Probes & {
    subarrayView: { transfer0IsSharedBuffer: boolean };
  };

  // 1) Uint8Array: backing buffer MUST be in the transfer list (the fix). The
  // pre-fix code left transfer empty -> these assertions fail without the fix.
  expect(
    probes.uint8.transferLen,
    "Uint8Array send must transfer its backing buffer (regression: empty transfer list -> cross-process COPY)",
  ).toBe(1);
  expect(probes.uint8.transfer0IsArrayBuffer).toBe(true);
  expect(probes.uint8.transfer0ByteLength).toBe(3);
  // Bytes must be preserved through the copy-out.
  expect(probes.uint8.dataBytes).toEqual([1, 2, 3]);
  // The delivered payload's backing buffer is exactly the transferred buffer.
  expect(probes.uint8.dataBufferIsTransferred).toBe(true);

  // 2) Subarray view: transfer a fresh 3-byte copy, never the shared 6-byte
  // buffer (transferring WASM linear memory would detach ALL of it).
  expect(probes.subarrayView.transferLen).toBe(1);
  expect(probes.subarrayView.transfer0ByteLength).toBe(3);
  expect(probes.subarrayView.dataBytes).toEqual([1, 2, 3]);
  expect(
    probes.subarrayView.transfer0IsSharedBuffer,
    "must NOT transfer the shared/WASM-memory backing buffer",
  ).toBe(false);
  expect(
    probes.subarrayView.sharedBufferByteLength,
    "shared backing buffer must remain intact (not detached)",
  ).toBe(6);

  // 3) ArrayBuffer: unchanged — transfers itself.
  expect(probes.arraybuffer.transferLen).toBe(1);
  expect(probes.arraybuffer.transfer0IsArrayBuffer).toBe(true);
  expect(probes.arraybuffer.dataIsTransfer0).toBe(true);
  expect(probes.arraybuffer.dataBytes).toEqual([9, 9]);

  // 4) String: transfers nothing.
  expect(probes.string.transferLen).toBe(0);
  expect(probes.string.dataEquals).toBe(true);
});
