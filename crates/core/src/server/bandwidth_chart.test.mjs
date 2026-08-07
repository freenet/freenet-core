// Behavioral tests for the browser-local bandwidth chart in dashboard.js.
//
// The production factory is extracted between exact, unique sentinels. Browser
// dependencies are injected, so sampling, rendering, cleanup, and edge cases
// run deterministically under Node without external packages.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import assert from 'node:assert/strict';

const here = dirname(fileURLToPath(import.meta.url));
const assetPath = join(here, 'home_page/assets/dashboard.js');
const src = readFileSync(assetPath, 'utf8');

function exactRegion(begin, end) {
  const start = src.indexOf(begin);
  const finish = src.indexOf(end);
  assert.notEqual(start, -1, 'missing source sentinel: ' + begin);
  assert.notEqual(finish, -1, 'missing source sentinel: ' + end);
  assert.equal(src.lastIndexOf(begin), start, 'duplicate source sentinel: ' + begin);
  assert.equal(src.lastIndexOf(end), finish, 'duplicate source sentinel: ' + end);
  assert.ok(finish > start, 'source sentinels are out of order');
  return src.slice(start + begin.length, finish);
}

const factorySource = exactRegion(
  '/* bandwidth-chart-factory:BEGIN */',
  '/* bandwidth-chart-factory:END */',
);
const createBandwidthChart = new Function(
  factorySource + '\nreturn createBandwidthChart;',
)();

function hasClass(element, className) {
  return (element.getAttribute('class') || '').split(/\s+/).includes(className);
}

function matches(element, selector) {
  if (selector === '[data-bandwidth-chart]') {
    return Object.hasOwn(element.attrs, 'data-bandwidth-chart');
  }
  if (selector === 'svg.bw-chart') {
    return element.getAttribute('tag') === 'svg' && hasClass(element, 'bw-chart');
  }
  if (selector.startsWith('.')) return hasClass(element, selector.slice(1));
  return false;
}

function fakeEl(attrs = {}) {
  return {
    attrs: { ...attrs },
    style: {},
    innerHTML: '',
    children: [],
    parentNode: null,
    setAttribute(name, value) {
      this.attrs[name] = String(value);
    },
    getAttribute(name) {
      return this.attrs[name] ?? null;
    },
    appendChild(child) {
      if (child.parentNode) child.parentNode.removeChild(child);
      child.parentNode = this;
      this.children.push(child);
    },
    removeChild(child) {
      const index = this.children.indexOf(child);
      if (index >= 0) {
        this.children.splice(index, 1);
        child.parentNode = null;
      }
    },
    querySelector(selector) {
      for (const child of this.children) {
        if (matches(child, selector)) return child;
        const nested = child.querySelector(selector);
        if (nested) return nested;
      }
      return null;
    },
    querySelectorAll(selector) {
      const found = [];
      for (const child of this.children) {
        if (matches(child, selector)) found.push(child);
        found.push(...child.querySelectorAll(selector));
      }
      return found;
    },
    closest(selector) {
      let current = this;
      while (current) {
        if (matches(current, selector)) return current;
        current = current.parentNode;
      }
      return null;
    },
    contains(element) {
      if (element === this) return true;
      return this.children.some((child) => child.contains(element));
    },
    focus() {
      this.focused = true;
    },
  };
}

function makeChart(options = {}) {
  const appended = [];
  const createdNS = [];
  let now = options.now ?? 0;
  const deps = {
    now: () => now,
    getRect: () => ({
      left: options.left ?? 0,
      top: options.top ?? 0,
      width: options.width ?? 560,
      height: options.height ?? 150,
    }),
    createEl: (tag) => fakeEl({ tag }),
    createElNS: (namespace, tag) => {
      createdNS.push([namespace, tag]);
      return fakeEl({ tag });
    },
    appendToBody: (element) => appended.push(element),
    innerWidth: () => options.innerWidth ?? 1024,
    innerHeight: () => options.innerHeight ?? 768,
    activeElement: () => options.activeElement ?? null,
    ...options.deps,
  };
  return {
    chart: createBandwidthChart(deps),
    appended,
    createdNS,
    setNow(value) {
      now = value;
    },
  };
}

function chartHost(uploaded, downloaded, uptime) {
  const root = fakeEl({ tag: 'root' });
  const host = fakeEl({
    tag: 'section',
    class: 'chart-section bw-section',
    'data-bandwidth-chart': '',
    'data-bytes-uploaded': String(uploaded),
    'data-bytes-downloaded': String(downloaded),
    'data-node-uptime-secs': String(uptime),
  });
  const content = fakeEl({ tag: 'div', class: 'bw-chart-content' });
  host.appendChild(content);
  root.appendChild(host);
  return { root, host, content };
}

function fakeSvg() {
  const svg = fakeEl({
    tag: 'svg',
    class: 'chart-svg bw-chart',
    viewBox: '0 0 560 150',
    'data-plot-top': '12',
    'data-plot-bottom': '126',
  });
  const values = [
    ['100.0', '110.0', '50.0', '-10s', '0.1 KB/s', '2.0 KB/s'],
    ['300.0', '30.0', '90.0', '-5s', '4.9 KB/s', '0.8 KB/s'],
    ['500.0', '60.0', '10.0', 'now', '1.0 KB/s', '8.8 KB/s'],
  ];
  for (const value of values) {
    svg.appendChild(
      fakeEl({
        tag: 'circle',
        class: 'bw-hit',
        cx: value[0],
        'data-up-y': value[1],
        'data-down-y': value[2],
        'data-age-label': value[3],
        'data-up-label': value[4],
        'data-down-label': value[5],
      }),
    );
  }
  return svg;
}

function svgFromMarkup(markup) {
  function attributes(fragment) {
    const attrs = {};
    for (const match of fragment.matchAll(/([\w:-]+)="([^"]*)"/g)) {
      attrs[match[1]] = match[2];
    }
    return attrs;
  }

  const svgTag = markup.match(/<svg ([^>]+)>/);
  assert.ok(svgTag, 'rendered SVG tag missing');
  const svg = fakeEl({ tag: 'svg', ...attributes(svgTag[1]) });
  for (const circle of markup.matchAll(/<circle ([^>]+)\/>/g)) {
    const attrs = attributes(circle[1]);
    if (attrs.class === 'bw-hit') {
      svg.appendChild(fakeEl({ tag: 'circle', ...attrs }));
    }
  }
  return svg;
}

// First observation establishes a baseline; the second records a rate.
{
  const { chart } = makeChart();
  assert.equal(chart.observeTotals(1000n, 2000n, 0), false);
  assert.equal(chart.getSamples().length, 0);
  assert.equal(chart.observeTotals(6000n, 12000n, 5000), true);
  assert.deepEqual(chart.getSamples(), [
    { at: 5000, upBps: 1000, downBps: 2000 },
  ]);
}

// Sub-second refresh bursts are coalesced without losing their bytes.
{
  const { chart } = makeChart();
  chart.observeTotals(0, 0, 0);
  assert.equal(chart.observeTotals(500, 1000, 500), false);
  assert.equal(chart.observeTotals(1000, 2000, 1000), true);
  assert.deepEqual(chart.getSamples()[0], {
    at: 1000,
    upBps: 1000,
    downBps: 2000,
  });
}

// A backward clock resets the baseline so collection recovers immediately.
{
  const { chart } = makeChart();
  chart.observeTotals(100, 100, 1000);
  assert.equal(chart.observeTotals(200, 200, 900), false);
  assert.equal(chart.getSamples().length, 0);
  chart.observeTotals(300, 500, 1900);
  assert.deepEqual(chart.getSamples()[0], {
    at: 1900,
    upBps: 100,
    downBps: 300,
  });
}

// Counter resets and gaps beyond the visible minute start a fresh series.
{
  const { chart } = makeChart();
  chart.observeTotals(1000, 1000, 0);
  chart.observeTotals(2000, 3000, 5000);
  assert.equal(chart.getSamples().length, 1);

  assert.equal(chart.observeTotals(10, 20, 10000), false);
  assert.equal(chart.getSamples().length, 0, 'counter reset clears history');
  chart.observeTotals(510, 1020, 15000);
  assert.deepEqual(chart.getSamples()[0], {
    at: 15000,
    upBps: 100,
    downBps: 200,
  });

  assert.equal(chart.observeTotals(10000, 20000, 76000), false);
  assert.equal(chart.getSamples().length, 0, 'long gap clears stale history');
  chart.observeTotals(10500, 21000, 81000);
  assert.deepEqual(chart.getSamples()[0], {
    at: 81000,
    upBps: 100,
    downBps: 200,
  });

  for (const [resetUp, resetDown] of [
    [10, 4000],
    [4000, 10],
  ]) {
    const independent = makeChart().chart;
    independent.observeTotals(1000, 1000, 0);
    independent.observeTotals(2000, 2000, 5000);
    assert.equal(independent.observeTotals(resetUp, resetDown, 10000), false);
    assert.equal(
      independent.getSamples().length,
      0,
      'either individual counter reset must clear both series',
    );
  }
}

// Exactly one minute is a valid interval; history remains time/count bounded.
{
  const { chart } = makeChart();
  chart.observeTotals(0, 0, 0);
  assert.equal(chart.observeTotals(6000, 12000, 60000), true);

  const bounded = makeChart().chart;
  bounded.observeTotals(0, 0, 0);
  for (let i = 1; i <= 200; i++) {
    bounded.observeTotals(i * 100, i * 200, i * 1000);
  }
  const samples = bounded.getSamples();
  assert.equal(samples.length, 61);
  assert.equal(samples[0].at, 140000);
  assert.equal(samples[60].at, 200000);
}

// BigInt counters preserve deltas after totals exceed Number's safe range.
{
  const { chart } = makeChart();
  const start = 9007199254740993n;
  chart.observeTotals(start, start, 0);
  chart.observeTotals(start + 5000n, start + 10000n, 5000);
  assert.deepEqual(chart.getSamples()[0], {
    at: 5000,
    upBps: 1000,
    downBps: 2000,
  });
}

// Invalid, absent, and blank counters clear stale history without throwing.
{
  const { chart } = makeChart();
  chart.observeTotals(0, 0, 0);
  chart.observeTotals(1000, 1000, 1000);
  assert.equal(chart.getSamples().length, 1);
  assert.equal(chart.observeTotals('not-a-counter', 0, 2000), false);
  assert.equal(chart.getSamples().length, 0);
  assert.equal(chart.observeTotals(-1, 0, 1000), false);
  assert.equal(chart.observeTotals(null, 0, 2000), false);
  assert.equal(chart.observeTotals('  ', 0, 3000), false);
  assert.equal(chart.getSamples().length, 0);
}

// Rendering uses timestamps for geometry and includes semantic/accessibility UI.
{
  const { chart } = makeChart();
  const html = chart.renderMarkup([
    { at: 0, upBps: 1000, downBps: 3000 },
    { at: 1000, upBps: 5000, downBps: 2000 },
    { at: 60000, upBps: 2000, downBps: 9000 },
  ]);
  const xValues = [...html.matchAll(/class="bw-hit" cx="([0-9.]+)"/g)].map(
    (match) => Number(match[1]),
  );
  assert.equal(xValues.length, 3);
  assert.ok(
    xValues[1] - xValues[0] < (xValues[2] - xValues[0]) * 0.05,
    'the one-second sample must sit near the start of a sixty-second axis',
  );
  assert.ok(html.includes('role="img"'));
  assert.ok(html.includes('<title id="bandwidth-chart-title">'));
  assert.ok(html.includes('bytes per second'));
  assert.ok(html.includes('bw-series-up'));
  assert.ok(html.includes('bw-series-down'));
  assert.ok(html.includes('View sample data'));
  assert.ok(html.includes('<table>'));
  assert.equal((html.match(/<tbody>[\s\S]*<\/tbody>/)?.[0].match(/<tr>/g) || []).length, 3);
  assert.ok(html.includes('<td>1000 B/s</td><td>2.9 KB/s</td>'));
  assert.ok(html.includes('data-age-label="now"'));
  assert.ok(html.includes('data-up-y="'));
  assert.ok(html.includes('data-down-y="'));
  assert.ok(html.includes('data-up-label="'));
  assert.ok(html.includes('data-down-label="'));
  assert.ok(html.includes('8.8 KB/s'));
  assert.ok(html.includes('>-1m</text>'));
  assert.ok(html.includes('>-45s</text>'));
  assert.ok(html.includes('>-30s</text>'));
  assert.ok(html.includes('>-15s</text>'));
}

// A partially filled history stays at the right edge of a fixed minute domain.
{
  const { chart } = makeChart();
  const html = chart.renderMarkup([
    { at: 5000, upBps: 100, downBps: 200 },
    { at: 10000, upBps: 200, downBps: 400 },
  ]);
  const xValues = [...html.matchAll(/class="bw-hit" cx="([0-9.]+)"/g)].map(
    (match) => Number(match[1]),
  );
  assert.ok(xValues[0] > xValues[1] * 0.9, 'unobserved time must remain blank');
}

// All-zero traffic still has three distinct, unit-bearing Y labels.
{
  const { chart } = makeChart();
  const html = chart.renderMarkup([
    { at: 0, upBps: 0, downBps: 0 },
    { at: 5000, upBps: 0, downBps: 0 },
  ]);
  assert.ok(html.includes('>0 B/s</text>'));
  assert.ok(html.includes('>0.5 B/s</text>'));
  assert.ok(html.includes('>1 B/s</text>'));
}

// Long Y labels drive the emitted label/axis padding rather than a test literal.
{
  const { chart } = makeChart();
  const html = chart.renderMarkup([
    { at: 0, upBps: 0, downBps: 0 },
    { at: 5000, upBps: 123456789, downBps: 1 },
  ]);
  const label = html.match(
    /<text x="([0-9.]+)"[^>]*>117\.7 MB\/s<\/text>/,
  );
  assert.ok(label, 'peak rate label missing');
  const labelX = Number(label[1]);
  assert.ok(labelX - '117.7 MB/s'.length * 6 >= 0, 'label would be clipped');
}

// sampleAndRender preserves history across replaced chart hosts.
{
  const harness = makeChart();
  const first = chartHost(1000, 2000, 100);
  assert.equal(harness.chart.sampleAndRender(first.root), true);
  assert.ok(first.content.innerHTML.includes('Collecting bandwidth samples'));

  harness.setNow(5000);
  const second = chartHost(6000, 12000, 105);
  harness.chart.sampleAndRender(second.root);
  assert.ok(second.content.innerHTML.includes('Collecting bandwidth samples'));

  harness.setNow(10000);
  const third = chartHost(11000, 22000, 110);
  harness.chart.sampleAndRender(third.root);
  assert.ok(third.content.innerHTML.includes('class="chart-svg bw-chart"'));
  assert.equal(harness.chart.getSamples().length, 2);
}

// Node restarts and a disappearing transfer card cannot bridge generations.
{
  const harness = makeChart();
  harness.chart.sampleAndRender(chartHost(1000, 2000, 100).root);
  harness.setNow(5000);
  harness.chart.sampleAndRender(chartHost(6000, 12000, 105).root);
  assert.equal(harness.chart.getSamples().length, 1);

  harness.setNow(10000);
  harness.chart.sampleAndRender(chartHost(50000, 80000, 2).root);
  assert.equal(
    harness.chart.getSamples().length,
    0,
    'lower node uptime resets even when new counters are numerically higher',
  );

  harness.setNow(15000);
  harness.chart.sampleAndRender(chartHost(55000, 90000, 7).root);
  assert.deepEqual(harness.chart.getSamples()[0], {
    at: 15000,
    upBps: 1000,
    downBps: 2000,
  });

  assert.equal(harness.chart.sampleAndRender(fakeEl({ tag: 'root' })), false);
  assert.equal(harness.chart.getSamples().length, 0, 'missing host clears history');
}

// Uptime advancing too slowly also catches a quick restart with higher uptime.
{
  const harness = makeChart();
  harness.chart.sampleAndRender(chartHost(10, 20, 1).root);
  harness.setNow(5000);
  harness.chart.sampleAndRender(chartHost(10000, 20000, 4).root);
  assert.equal(harness.chart.getSamples().length, 0);
}

// Uptime jumping ahead catches browser clocks paused during system sleep.
{
  const harness = makeChart();
  harness.chart.sampleAndRender(chartHost(1000, 2000, 100).root);
  harness.setNow(5000);
  harness.chart.sampleAndRender(chartHost(6000, 12000, 105).root);
  assert.equal(harness.chart.getSamples().length, 1);

  harness.setNow(6000);
  harness.chart.sampleAndRender(chartHost(500000, 800000, 405).root);
  assert.equal(
    harness.chart.getSamples().length,
    0,
    'sleep-time bytes must not be compressed into a current spike',
  );
}

// Refresh preserves an expanded data table and returns focus to its summary.
{
  const oldSummary = fakeEl({ tag: 'summary', class: 'bw-data-summary' });
  const oldDetails = fakeEl({ tag: 'details', class: 'bw-data' });
  oldDetails.open = true;
  oldDetails.appendChild(oldSummary);

  const harness = makeChart({ activeElement: oldSummary });
  harness.chart.observeTotals(1000, 2000, 0);
  harness.chart.observeTotals(6000, 12000, 5000);
  harness.chart.observeTotals(11000, 22000, 10000);
  harness.setNow(10000);
  const oldPage = chartHost(11000, 22000, 110);
  oldPage.content.appendChild(oldDetails);
  harness.chart.sampleAndRender(oldPage.root);
  harness.chart.beforeRefresh(oldPage.root);

  harness.setNow(15000);
  const replacement = chartHost(16000, 32000, 115);
  const newDetails = fakeEl({ tag: 'details', class: 'bw-data' });
  const newSummary = fakeEl({ tag: 'summary', class: 'bw-data-summary' });
  newDetails.appendChild(newSummary);
  replacement.content.appendChild(newDetails);
  harness.chart.sampleAndRender(replacement.root);

  assert.ok(replacement.content.innerHTML.includes('class="bw-data" open'));
  assert.equal(newSummary.focused, true);
}

// Hover creation, update, and cleanup use a coherent DOM model.
{
  const { chart, appended, createdNS } = makeChart();
  const svg = svgFromMarkup(
    chart.renderMarkup([
      { at: 0, upBps: 100, downBps: 2048 },
      { at: 5000, upBps: 5000, downBps: 800 },
      { at: 10000, upBps: 1024, downBps: 9000 },
    ]),
  );
  const middleX = Number(svg.querySelectorAll('.bw-hit')[1].getAttribute('cx'));
  chart.onPointer({ clientX: middleX, clientY: 100, target: svg });
  const overlay = svg.querySelector('.bw-overlay');
  assert.ok(overlay, 'overlay appended');
  assert.equal(overlay.parentNode, svg);
  assert.deepEqual(createdNS, [['http://www.w3.org/2000/svg', 'g']]);
  assert.ok(overlay.innerHTML.includes('x1="' + middleX + '"'));
  assert.equal(appended.length, 1);
  assert.ok(appended[0].innerHTML.includes('-5s'));
  assert.ok(appended[0].innerHTML.includes('Up 4.9 KB/s'));

  chart.clear();
  assert.equal(svg.querySelector('.bw-overlay'), null, 'overlay detached');
  assert.equal(overlay.parentNode, null);
  assert.equal(appended[0].style.display, 'none');
}

// Moving between charts removes the previous chart's overlay.
{
  const { chart } = makeChart();
  const first = fakeSvg();
  const second = fakeSvg();
  chart.onPointer({ clientX: 100, clientY: 50, target: first });
  const oldOverlay = first.querySelector('.bw-overlay');
  chart.onPointer({ clientX: 500, clientY: 50, target: second });
  assert.equal(first.querySelector('.bw-overlay'), null);
  assert.equal(oldOverlay.parentNode, null);
  assert.ok(second.querySelector('.bw-overlay'));
}

// Coordinate mapping honors viewBox origin, and nearest-hit ties are stable.
{
  const { chart } = makeChart();
  assert.equal(
    chart.viewBoxX({ left: 100, width: 1120 }, '10 0 560 150', 660),
    290,
  );
  const svg = fakeSvg();
  const hits = svg.querySelectorAll('.bw-hit');
  assert.equal(chart.nearestHit(hits, 200), hits[0]);
  assert.equal(chart.nearestHit(hits, 560), hits[2]);
}

// Tooltip positioning clamps to tiny viewports as well as right/bottom edges.
{
  const normal = makeChart({ innerWidth: 800, innerHeight: 600 }).chart;
  const element = { style: {} };
  normal.positionTooltip(element, 780, 590);
  assert.deepEqual(element.style, {
    left: '582px',
    top: '514px',
    maxWidth: '190px',
  });

  const tiny = makeChart({ innerWidth: 100, innerHeight: 50 }).chart;
  const tinyElement = { style: {} };
  tiny.positionTooltip(tinyElement, 5, 5);
  assert.deepEqual(tinyElement.style, {
    left: '4px',
    top: '4px',
    maxWidth: '92px',
  });
}

// No chart or no hit targets is a safe no-op.
{
  const { chart, appended } = makeChart();
  assert.equal(chart.sampleAndRender(fakeEl({ tag: 'root' })), false);
  const svg = fakeSvg();
  for (const hit of [...svg.querySelectorAll('.bw-hit')]) svg.removeChild(hit);
  chart.onPointer({ clientX: 20, clientY: 20, target: svg });
  assert.equal(svg.querySelector('.bw-overlay'), null);
  assert.equal(appended.length, 0);
}

// Execute the exact production wiring so commenting out calls breaks the test.
{
  const wiring = exactRegion(
    '/* bandwidth-chart-wiring:BEGIN */',
    '/* bandwidth-chart-wiring:END */',
  );
  const events = {};
  const calls = [];
  let productionDeps;
  const document = {
    activeElement: { id: 'focused' },
    body: { appendChild: () => {} },
    createElement: (tag) => ({ tag }),
    createElementNS: (namespace, tag) => ({ namespace, tag }),
    addEventListener: (type, listener) => {
      events[type] = listener;
    },
  };
  const window = {
    innerWidth: 800,
    innerHeight: 600,
    performance: { now: () => 1234 },
  };
  const chart = {
    sampleAndRender: (root) => calls.push(['sample', root]),
    onPointer: (event) => calls.push(['pointer', event]),
    clear: () => calls.push(['clear']),
  };
  const create = (deps) => {
    productionDeps = deps;
    return chart;
  };

  new Function('createBandwidthChart', 'document', 'window', wiring)(
    create,
    document,
    window,
  );
  assert.deepEqual(calls, [['sample', document]]);
  assert.equal(productionDeps.now(), 1234, 'sampling clock must be monotonic');
  assert.equal(productionDeps.activeElement(), document.activeElement);

  const move = { type: 'move' };
  const down = { type: 'down' };
  events.pointermove(move);
  events.pointerdown(down);
  events.mouseleave();
  assert.deepEqual(calls.slice(1), [
    ['pointer', move],
    ['pointer', down],
    ['clear'],
  ]);

  const refresh = exactRegion(
    '/* bandwidth-chart-refresh:BEGIN */',
    '/* bandwidth-chart-refresh:END */',
  );
  const order = [];
  const oldMain = {};
  Object.defineProperty(oldMain, 'innerHTML', {
    set(value) {
      order.push(['swap', value]);
    },
  });
  const newMain = { innerHTML: '<main>fresh</main>' };
  const refreshChart = {
    beforeRefresh: (root) => order.push(['before', root]),
    sampleAndRender: (root) => order.push(['sample', root]),
  };
  new Function(
    'bandwidthChart',
    'oldMain',
    'newMain',
    'document',
    refresh,
  )(refreshChart, oldMain, newMain, document);
  assert.deepEqual(order, [
    ['before', document],
    ['swap', '<main>fresh</main>'],
    ['sample', document],
  ]);
}

console.log('bandwidth_chart.test.mjs: all assertions passed');
