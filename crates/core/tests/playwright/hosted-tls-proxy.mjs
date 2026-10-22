// A minimal same-host TLS-terminating reverse proxy for the hosted-mode
// Playwright run (crates/core/tests/playwright_shell.rs).
//
// A hosted node honours a user token only from a loopback source carrying
// `X-Forwarded-Proto: https`, and the shell mints and sends the token only
// from a page served over https. This proxy provides both: it serves https
// with the test certificate in crates/core/tests/data/tls_native_roots/ and
// forwards every request, including WebSocket upgrades, to the node over
// loopback with `X-Forwarded-Proto: https`. The browser's Host and Origin are
// passed through unchanged, as a real proxy such as Caddy does.
//
// Usage: node hosted-tls-proxy.mjs <node-port>
// Prints `LISTENING <port>` on stdout once it accepts connections.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';

const target = Number(process.argv[2]);
if (!Number.isInteger(target) || target <= 0) {
  console.error('usage: node hosted-tls-proxy.mjs <node-port>');
  process.exit(2);
}

const here = dirname(fileURLToPath(import.meta.url));
const certDir = join(here, '..', 'data', 'tls_native_roots');

function derToPem(der, label) {
  const b64 = der.toString('base64').match(/.{1,64}/g).join('\n');
  return `-----BEGIN ${label}-----\n${b64}\n-----END ${label}-----\n`;
}

const cert = derToPem(readFileSync(join(certDir, 'leaf.der')), 'CERTIFICATE');
const key = derToPem(readFileSync(join(certDir, 'leaf.key.der')), 'PRIVATE KEY');

// Hop-by-hop headers are not forwarded on an ordinary request.
const HOP = new Set([
  'connection',
  'keep-alive',
  'proxy-connection',
  'transfer-encoding',
  'upgrade',
  'te',
  'trailer',
]);

function forwardedHeaders(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers)) {
    if (!HOP.has(k.toLowerCase())) out[k] = v;
  }
  out['x-forwarded-proto'] = 'https';
  return out;
}

const server = https.createServer({ cert, key }, (req, res) => {
  const upstream = http.request(
    {
      host: '127.0.0.1',
      port: target,
      method: req.method,
      path: req.url,
      headers: forwardedHeaders(req.headers),
    },
    (up) => {
      res.writeHead(up.statusCode || 502, up.headers);
      up.pipe(res);
    },
  );
  upstream.on('error', () => {
    if (!res.headersSent) res.writeHead(502);
    res.end();
  });
  req.pipe(upstream);
});

// WebSocket upgrades: replay the request line and headers to the node with
// `X-Forwarded-Proto: https` added, then splice the two sockets.
server.on('upgrade', (req, socket, head) => {
  const upstream = net.connect(target, '127.0.0.1', () => {
    const lines = [`${req.method} ${req.url} HTTP/1.1`];
    for (let i = 0; i < req.rawHeaders.length; i += 2) {
      const name = req.rawHeaders[i];
      if (name.toLowerCase() === 'x-forwarded-proto') continue;
      lines.push(`${name}: ${req.rawHeaders[i + 1]}`);
    }
    lines.push('X-Forwarded-Proto: https');
    upstream.write(lines.join('\r\n') + '\r\n\r\n');
    if (head && head.length) upstream.write(head);
    upstream.pipe(socket);
    socket.pipe(upstream);
  });
  const close = () => {
    upstream.destroy();
    socket.destroy();
  };
  upstream.on('error', close);
  socket.on('error', close);
});

server.listen(0, '127.0.0.1', () => {
  console.log(`LISTENING ${server.address().port}`);
});
