/* Theme is a THREE-state PREFERENCE — explicit 'light', explicit 'dark', or no
 * choice — held in localStorage, and separately a TWO-state RESOLVED theme
 * stamped onto `data-theme` for the stylesheet to key off.
 *
 * Keeping those two things apart is the whole design, and conflating them was
 * a real bug in the first version of this change. It resolved the preference
 * into CSS by adding one `prefers-color-scheme` block that redefined the
 * `:root` colour variables, and left the attribute unstamped when there was no
 * explicit choice. But 24 separate rules in style.css key their light styling
 * off `[data-theme='light']` directly, with hardcoded colours rather than
 * those variables — every warning banner, NAT diagnostic, health banner and
 * verdict badge among them. An OS-light operator therefore got light page
 * chrome with dark-locked components on top: `.warning` rendering #fbbf24
 * amber on a #f7f5f2 background, which is harder to read than either theme
 * done properly.
 *
 * Stamping the RESOLVED value fixes all 24 at once, because they were already
 * written correctly — they were simply never reached. The preference stays in
 * localStorage, so stamping 'dark' for an OS-dark visitor does NOT record a
 * choice they did not make; `effectiveTheme()` reads the store, not the
 * attribute.
 *
 * The `prefers-color-scheme` block stays as the no-JavaScript fallback: it
 * gets the base colours right on its own, which is a partial theme rather than
 * a broken one.
 *
 * Applied inline at the top of the file, before first paint, so the page does
 * not flash the wrong theme on load. */
(function () {
  var pref = null;
  try {
    pref = localStorage.getItem('theme');
  } catch (e) {
    /* localStorage unavailable — resolve from the OS alone */
  }
  var prefersLight = false;
  try {
    prefersLight = !!(
      window.matchMedia &&
      window.matchMedia('(prefers-color-scheme: light)').matches
    );
  } catch (e) {
    /* matchMedia unavailable — treat as no OS preference */
  }
  document.documentElement.setAttribute(
    'data-theme',
    resolveTheme(pref, prefersLight),
  );
})();

/* The theme preference, with an in-memory fallback.
 *
 * `localStorage` throws in some privacy modes. Swallowing that is not harmless
 * here, because `effectiveTheme()` reads the preference back to decide what the
 * NEXT click should do. With a throwing store the read always reported "no
 * preference" and fell through to the OS, so after one click the page was light
 * while the icon still described a dark page, and every further click resolved
 * to the same direction — a toggle that goes one way and then stops, with an
 * icon that contradicts the screen.
 *
 * Mirroring the write in memory makes the control work whether or not the
 * preference can be persisted; only durability across a reload is lost, which
 * is what a browser refusing storage has already decided. Same shape as
 * `tfGet`/`tfSet` for the table filters, for the same reason. */
var themeMemory = null;

function themePrefGet() {
  if (themeMemory !== null) return themeMemory;
  try {
    return localStorage.getItem('theme');
  } catch (e) {
    return null;
  }
}

function themePrefSet(value) {
  themeMemory = value;
  try {
    localStorage.setItem('theme', value);
  } catch (e) {
    /* memory copy above is the fallback */
  }
}

/* theme-resolve:BEGIN
 *
 * Resolves the three-state theme setting. Extracted verbatim by
 * `theme_preference.test.mjs`, so it takes its browser dependencies as
 * parameters rather than reaching for globals — a substring pin cannot tell
 * you whether an explicit choice correctly beats the OS in BOTH directions,
 * which is the part that was previously unrepresentable.
 *
 * `pref` is the STORED preference — 'light', 'dark', or null/absent when the
 * operator has chosen nothing. Deliberately not the stamped `data-theme`
 * attribute: that always holds a resolved value, so feeding it back in would
 * make every visitor look like they had chosen explicitly. */
function resolveTheme(pref, prefersLight) {
  if (pref === 'light' || pref === 'dark') return pref;
  return prefersLight ? 'light' : 'dark';
}
/* theme-resolve:END */

/* What the page is CURRENTLY showing, which is not the same as what was
 * chosen: with no explicit choice the answer comes from the OS, so reading
 * the attribute alone would report "dark" for a light-mode user and make the
 * first toggle click appear to do nothing. */
function effectiveTheme() {
  var prefersLight = false;
  try {
    prefersLight = !!(
      window.matchMedia &&
      window.matchMedia('(prefers-color-scheme: light)').matches
    );
  } catch (e) {
    /* matchMedia unavailable — treat as no OS preference */
  }
  /* Read the PREFERENCE, not the stamped attribute. The attribute now always
     holds a resolved 'light' or 'dark', so reading it back would report every
     visitor as having made an explicit choice and the OS would stop being
     consulted after the first paint. */
  return resolveTheme(themePrefGet(), prefersLight);
}

function updateThemeIcon() {
  var icon = document.getElementById('theme-icon');
  if (!icon) return;
  /* The icon shows the theme you would switch TO, not the current one. */
  icon.textContent =
    effectiveTheme() === 'light'
      ? '\uD83C\uDF19' /* moon = click for dark */
      : '\u2600\uFE0F'; /* sun  = click for light */
}

/* Follow a LIVE OS theme change, while no explicit choice is stored.
 *
 * Needed because the resolved theme is stamped onto `data-theme` at load: the
 * stylesheet's `prefers-color-scheme` block is guarded on the attribute being
 * absent, so once stamped it no longer matches and the page would otherwise
 * ignore the OS until the next reload. Stamping is what makes the 24
 * attribute-keyed component rules work under OS-follow, so the cost of it is
 * this listener.
 *
 * It also keeps the toggle's icon honest. The icon advertises what the NEXT
 * click will do, so an OS flip that changes the page without changing the icon
 * leaves it promising the opposite of what it delivers.
 *
 * Gated on there being no stored preference: an explicit choice must not be
 * overridden by the OS changing underneath it. */
function watchOsTheme() {
  if (!window.matchMedia) return;
  var mq = window.matchMedia('(prefers-color-scheme: light)');
  var onChange = function () {
    var pref = themePrefGet();
    if (pref === 'light' || pref === 'dark') return;
    document.documentElement.setAttribute(
      'data-theme',
      resolveTheme(null, mq.matches),
    );
    updateThemeIcon();
  };
  if (mq.addEventListener) {
    mq.addEventListener('change', onChange);
  } else if (mq.addListener) {
    /* Safari < 14 */
    mq.addListener(onChange);
  }
}

function toggleTheme() {
  var next = effectiveTheme() === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  themePrefSet(next);
  updateThemeIcon();
}

/* ── Toast notifications ── */
function showToast(msg, opts) {
  var container = document.getElementById('toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container';
    document.body.appendChild(container);
  }
  var t = document.createElement('div');
  t.className = 'toast' + (opts && opts.error ? ' toast-error' : '');
  t.textContent = msg;
  container.appendChild(t);
  setTimeout(
    function () {
      t.style.transition = 'opacity 0.25s';
      t.style.opacity = '0';
      setTimeout(function () {
        if (t.parentNode) t.parentNode.removeChild(t);
      }, 260);
    },
    (opts && opts.duration) || 1600,
  );
}

/* ── Copy contract key to clipboard ── */
function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  /* Fallback for older browsers / non-secure contexts */
  return new Promise(function (resolve, reject) {
    try {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      var ok = document.execCommand('copy');
      document.body.removeChild(ta);
      ok ? resolve() : reject(new Error('execCommand failed'));
    } catch (e) {
      reject(e);
    }
  });
}

/* ── Sortable tables ── */
function compareCells(a, b, type) {
  if (type === 'num') {
    var na = parseFloat(a);
    var nb = parseFloat(b);
    var aBad = isNaN(na),
      bBad = isNaN(nb);
    if (aBad && bBad) return 0;
    if (aBad) return 1; /* missing values sort to bottom */
    if (bBad) return -1;
    return na - nb;
  }
  return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
}

function applySort(table, colIndex, dir) {
  var tbody = table.querySelector('tbody');
  if (!tbody) return;
  var ths = table.querySelectorAll('thead th');
  var th = ths[colIndex];
  if (!th) return;
  var type = th.getAttribute('data-sort-type') || 'text';
  var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
  rows.sort(function (r1, r2) {
    var c1 = r1.children[colIndex];
    var c2 = r2.children[colIndex];
    var v1 = c1 ? c1.getAttribute('data-sort') || c1.textContent : '';
    var v2 = c2 ? c2.getAttribute('data-sort') || c2.textContent : '';
    var cmp = compareCells(v1, v2, type);
    return dir === 'desc' ? -cmp : cmp;
  });
  rows.forEach(function (r) {
    tbody.appendChild(r);
  });
  ths.forEach(function (h) {
    h.classList.remove('sort-asc', 'sort-desc');
  });
  th.classList.add(dir === 'desc' ? 'sort-desc' : 'sort-asc');
}

function sortKey(table) {
  return 'sort:' + (table.getAttribute('data-table-id') || 'tbl');
}

function handleHeaderClick(th) {
  var table = th.closest('table.sortable');
  if (!table) return;
  var ths = Array.prototype.slice.call(table.querySelectorAll('thead th'));
  var idx = ths.indexOf(th);
  if (idx < 0) return;
  var current = th.classList.contains('sort-asc')
    ? 'asc'
    : th.classList.contains('sort-desc')
      ? 'desc'
      : null;
  var dir = current === 'asc' ? 'desc' : 'asc';
  applySort(table, idx, dir);
  try {
    sessionStorage.setItem(sortKey(table), idx + ':' + dir);
  } catch (e) {}
  /* Re-apply the collapse against the NEW order. `applySort` reorders the
     <tr> elements but never touches `style.display`, and the collapse is a
     POSITIONAL cut — "the first 25 rows in current order". Without this the
     display flags stay attached to whichever elements were visible under the
     PREVIOUS order, so sorting a collapsed table showed an arbitrary
     leftover subset re-sorted among itself, not the top 25 of the new sort.
     The count in the status line stayed correct, which is what made it hard
     to notice: the right number of the wrong rows. */
  reapplyTableViewFor(table);
}

/* Find the filter controls governing `table`, if any, and recompute which of
   its rows are visible. */
function reapplyTableViewFor(table) {
  var id = table.getAttribute('data-table-id');
  if (!id) return;
  var wrap = document.querySelector(
    '.table-filter[data-filter-for="' + id + '"]',
  );
  if (wrap) applyTableView(wrap);
}

function restoreSort() {
  document.querySelectorAll('table.sortable').forEach(function (table) {
    try {
      var saved = sessionStorage.getItem(sortKey(table));
      if (!saved) return;
      var parts = saved.split(':');
      var idx = parseInt(parts[0], 10);
      var dir = parts[1] === 'desc' ? 'desc' : 'asc';
      if (!isNaN(idx)) applySort(table, idx, dir);
    } catch (e) {}
  });
}

/* Sort restore reorders rows too, so the collapse must be recomputed after it
   for the same reason as a live header click. `restoreTableFilters` runs
   after `restoreSort` at both call sites, which covers this — but the
   ordering between them is load-bearing, so do not reverse it. */

/* ── Long-table filter + collapse ──────────────────────────────────────
   The peers and subscribed-contracts tables are unbounded: a production
   gateway rendered 210 peer rows, 70% of an 11,780px page, with no way to
   find one row among them. Rather than truncate server-side (which would
   make a filter lie about what it searched), every row is still rendered
   and the table is COLLAPSED to a readable default here, with the filter
   searching the full set and auto-expanding while a query is active.

   State lives in sessionStorage for the same reason sort order does: the
   5s auto-refresh replaces <main> wholesale, so an un-persisted filter
   would be wiped mid-keystroke — worse than not having one. */
var COLLAPSE_ROWS = 25;

function filterKey(id) {
  return 'filter:' + id;
}
function expandKey(id) {
  return 'expand:' + id;
}

/* sessionStorage throws in some privacy modes. The existing `sort:` handling
   swallows that and degrades to "no saved sort", which is harmless. Here it
   would NOT be: `applyTableView` reads the expand flag back immediately after
   writing it, so a swallowed write made "Show all" inert — the click appeared
   to do nothing at all. Mirror every write in memory and read that first, so
   the controls keep working when persistence does not. */
var tfMemory = {};

function tfGet(key) {
  if (Object.prototype.hasOwnProperty.call(tfMemory, key)) return tfMemory[key];
  try {
    return sessionStorage.getItem(key);
  } catch (e) {
    return null;
  }
}

function tfSet(key, value) {
  tfMemory[key] = value;
  try {
    sessionStorage.setItem(key, value);
  } catch (e) {
    /* memory copy above is the fallback */
  }
}

/* tf-search:BEGIN
 *
 * The text a row is matched against. Extracted by `table_filter.test.mjs`.
 *
 * `textContent` alone is NOT enough and that was a real bug: the contracts
 * table renders a 12-character abbreviated key, so pasting a real contract key
 * — the obvious thing to do, and what the copy button beside every row puts on
 * the clipboard — matched nothing at all.
 *
 * The extra text comes from `data-copy` ONLY, and the narrowness is
 * deliberate. `data-copy` holds the full form of the value the cell
 * abbreviates, so matching it can only ever surface a row for a reason the
 * user can see. The two neighbouring attributes both look tempting and are
 * both wrong: `data-sort` carries raw sort keys that differ from the display
 * (`data-sort="1048576"` renders as "1.0 MB", `data-sort="{state_rank}"` as a
 * badge), and `title` carries prose — every contract row has a copy button
 * titled "Copy contract key", so including it would make the query "key"
 * match every row in the table. Both would surface rows for reasons invisible
 * on screen, which is worse than not matching at all: the user cannot tell
 * why the row is there. */
function rowSearchText(textContent, attrValues) {
  var parts = [textContent || ''];
  for (var i = 0; i < attrValues.length; i++) {
    if (attrValues[i]) parts.push(attrValues[i]);
  }
  return parts.join(' ').toLowerCase();
}

/* Decide which row indices are visible. Pure so it can be tested without a
 * DOM: `matches` is the per-row predicate result in current table order.
 *
 * Separated from the DOM application because the collapse is a POSITIONAL cut
 * over the CURRENT order, which is what made the post-sort bug possible — the
 * decision and the elements it was baked into could drift apart. */
function visibleRowFlags(matches, showAll, cap) {
  var out = [];
  var shown = 0;
  for (var i = 0; i < matches.length; i++) {
    if (!matches[i]) {
      out.push(false);
      continue;
    }
    shown++;
    out.push(showAll || shown <= cap);
  }
  return out;
}
/* tf-search:END */

function applyTableView(wrap) {
  var id = wrap.getAttribute('data-filter-for');
  var table = document.querySelector('table[data-table-id="' + id + '"]');
  if (!table) return;
  var tbody = table.querySelector('tbody');
  if (!tbody) return;
  var input = wrap.querySelector('.tf-input');
  var status = wrap.querySelector('.tf-status');
  var toggle = wrap.querySelector('.tf-toggle');

  var q = (input && input.value ? input.value : '').trim().toLowerCase();
  var expanded = tfGet(expandKey(id)) === '1';
  /* A query implies "show me everything that matches", so filtering
     overrides the collapse rather than fighting it. */
  var showAll = expanded || q.length > 0;

  var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
  var matches = rows.map(function (r) {
    if (q.length === 0) return true;
    /* Fold in the full values the cell text abbreviates. See rowSearchText
       for why this is data-copy alone and not title or data-sort. */
    var attrs = [];
    var withCopy = r.querySelectorAll('[data-copy]');
    for (var i = 0; i < withCopy.length; i++) {
      attrs.push(withCopy[i].getAttribute('data-copy'));
    }
    return rowSearchText(r.textContent, attrs).indexOf(q) !== -1;
  });
  var flags = visibleRowFlags(matches, showAll, COLLAPSE_ROWS);
  var matched = 0;
  rows.forEach(function (r, i) {
    if (matches[i]) matched++;
    r.style.display = flags[i] ? '' : 'none';
  });

  var total = rows.length;
  var shown = showAll ? matched : Math.min(matched, COLLAPSE_ROWS);
  if (status) {
    if (q.length > 0) {
      status.textContent =
        'Showing ' +
        shown +
        ' of ' +
        matched +
        ' matching (' +
        total +
        ' total)';
    } else if (showAll) {
      status.textContent = 'Showing all ' + total;
    } else {
      status.textContent = 'Showing ' + shown + ' of ' + total;
    }
  }
  if (toggle) {
    /* While a query is active the collapse is not in force, so offering to
       toggle it would be a control that does nothing. */
    toggle.hidden = q.length > 0 || total <= COLLAPSE_ROWS;
    toggle.textContent = expanded ? 'Show fewer' : 'Show all ' + total;
    toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  }
}

function applyAllTableViews() {
  document.querySelectorAll('.table-filter').forEach(applyTableView);
}

function restoreTableFilters(focusState) {
  document.querySelectorAll('.table-filter').forEach(function (wrap) {
    var id = wrap.getAttribute('data-filter-for');
    var input = wrap.querySelector('.tf-input');
    if (input) {
      input.value = tfGet(filterKey(id)) || '';
      /* Restoring the VALUE alone is not enough: the refresh replaces <main>,
         destroying the element the caret was in, so the next keystroke after a
         refresh goes nowhere and the user has to click back into the box —
         every five seconds, while typing. Put focus and the caret back too,
         but ONLY when the box is on screen.

         Restoring it unconditionally is what made the refresh yank the page:
         focus() scrolls the element into view, the box sits at the TOP of a
         card whose table can be thousands of pixels tall, so a user who
         filtered and then scrolled down to read the matches was thrown back
         every five seconds. Measured: scrollY 3000 -> 231.

         Suppressing that scroll turned out not to be portable. `preventScroll`
         is honoured by Chromium and Firefox but not WebKit, and neither a
         synchronous scroll reassignment nor one on the next animation frame
         beat WebKit's deferred scroll-into-view — it still landed on 1341,
         deterministically, once the test stopped racing it.

         So gate on visibility instead, which needs no engine cooperation: if
         the box was already FULLY in the viewport, focusing it cannot scroll
         anywhere. Full containment is required — see isInViewport; a
         half-visible box is still scrolled into view on focus. If it is off screen the user is reading rows rather than
         typing, and silently stealing focus back to an input they cannot see
         is not the behaviour to want anyway. */
      if (focusState && focusState.id === id && focusState.visible) {
        input.focus({ preventScroll: true });
        try {
          /* Unreachable today and kept deliberately: the control is
             `type="search"` (cards.rs), where setSelectionRange is supported
             in every engine. It throws on `email` and `number`, so this catch
             is here only so that changing the input's type stays a cosmetic
             change rather than one that breaks the refresh. */
          input.setSelectionRange(focusState.start, focusState.end);
        } catch (e) {
          /* leave the caret wherever focus() put it */
        }
      }
    }
    applyTableView(wrap);
  });
}

/* Is the element FULLY within the viewport? Used to decide whether restoring
   focus is safe — see the comment at the focus() call.
 *
 * Containment, not intersection, and the difference is the whole point. The
 * browser scrolls a focused element into view unless it is ENTIRELY visible,
 * so a box with only its top half on screen still gets scrolled — and WebKit
 * ignores `preventScroll`, so nothing stops it there. An intersection test
 * (`bottom > 0 && top < h`) would call that box "visible", let the focus
 * through, and reintroduce the jump for the partial-visibility band. It would
 * also make the claim at the focus() call ("focusing it cannot scroll
 * anywhere") false, which is how the bug would survive review: the comment
 * would still read as correct.
 *
 * The cost of being strict is that a box straddling the viewport edge loses
 * focus on refresh, which is the same outcome as a box fully off screen and
 * is the safe direction to err. */
function isInViewport(el) {
  if (!el || typeof el.getBoundingClientRect !== 'function') return false;
  var r = el.getBoundingClientRect();
  var h = window.innerHeight || document.documentElement.clientHeight;
  var w = window.innerWidth || document.documentElement.clientWidth;
  /* One pixel of slack. Sub-pixel layout routinely leaves a box that is
     visually flush with an edge reporting a fractional overflow — measured
     `bottom: 700.4` against a 700px viewport for a box the browser itself had
     just scrolled fully into view. An exact comparison calls that "not
     contained", drops the focus restore, and the caret is lost for a box the
     user can see perfectly well. The browsers round too. */
  var slack = 1;
  return (
    r.top >= -slack &&
    r.left >= -slack &&
    r.bottom <= h + slack &&
    r.right <= w + slack
  );
}

/* Which filter box had focus, and where the caret was, so the refresh can put
   it back. Read BEFORE the <main> swap; the element does not survive it. */
function captureFilterFocus() {
  var el = document.activeElement;
  if (!el || !el.classList || !el.classList.contains('tf-input')) return null;
  var wrap = el.closest('.table-filter');
  if (!wrap) return null;
  return {
    id: wrap.getAttribute('data-filter-for'),
    start: el.selectionStart,
    end: el.selectionEnd,
    /* Measured on the OLD element, before the swap. Measuring the fresh one
       instead races layout: right after the innerHTML write WebKit
       intermittently reports an off-screen rect, which suppressed the focus
       restore on ~3 runs in 10. The old element is also the more faithful
       question to ask — "could the user see the box they were typing in?" —
       and the replacement occupies the same position. */
    visible: isInViewport(el),
  };
}

function handleFilterInput(input) {
  var wrap = input.closest('.table-filter');
  if (!wrap) return;
  tfSet(filterKey(wrap.getAttribute('data-filter-for')), input.value);
  applyTableView(wrap);
}

function handleFilterToggle(btn) {
  var wrap = btn.closest('.table-filter');
  if (!wrap) return;
  var id = wrap.getAttribute('data-filter-for');
  var expanded = tfGet(expandKey(id)) === '1';
  tfSet(expandKey(id), expanded ? '0' : '1');
  applyTableView(wrap);
}

/* ── Update-available check (GitHub releases, cached 12h) ── */
function compareSemver(a, b) {
  var pa = String(a)
    .replace(/^v/, '')
    .split(/[.\-+]/);
  var pb = String(b)
    .replace(/^v/, '')
    .split(/[.\-+]/);
  var n = Math.max(pa.length, pb.length);
  for (var i = 0; i < n; i++) {
    var na = parseInt(pa[i], 10);
    var nb = parseInt(pb[i], 10);
    if (isNaN(na) && isNaN(nb)) {
      var s = (pa[i] || '').localeCompare(pb[i] || '');
      if (s !== 0) return s;
      continue;
    }
    if (isNaN(na)) return -1;
    if (isNaN(nb)) return 1;
    if (na !== nb) return na - nb;
  }
  return 0;
}

function showUpdateBadge(latestTag) {
  var el = document.getElementById('update-badge');
  if (!el) return;
  el.textContent = 'Update: v' + String(latestTag).replace(/^v/, '');
  el.title =
    'A newer Freenet release is available — click to view release notes';
  el.hidden = false;
}

/* Update-check core, extracted so the caching/back-off state machine can be
   driven under Node by update_check.test.mjs. All browser dependencies are
   injected, so this stays free of direct window/document/fetch references —
   keep it that way, and keep it bracketed by the markers.

   Invariants (tested behaviorally in update_check.test.mjs):
   - a fresh SUCCESS within TTL_MS is served from cache with no request;
   - a FAILURE is remembered and suppresses further requests for FAIL_TTL_MS
     (#5102: previously only success was cached, so a rate-limited browser
     re-requested on every single page load — knocking hardest exactly while
     the IP was already being refused);
   - the failure window expires, so a limited client recovers on its own;
   - a 200 carrying no usable tag counts as a failure, so a malformed
     response cannot drive a per-reload retry loop;
   - the badge is shown only when the fetched tag is strictly newer. */
/* update-check:BEGIN */
function createUpdateChecker(deps) {
  var TTL_MS = 12 * 60 * 60 * 1000;
  var FAIL_TTL_MS = 60 * 60 * 1000;

  function readCache() {
    try {
      var raw = deps.getItem('freenet-update-check');
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      return null;
    }
  }

  function write(entry) {
    try {
      deps.setItem('freenet-update-check', JSON.stringify(entry));
    } catch (e) {
      /* storage unavailable (private mode / quota) — degrade to no caching */
    }
  }

  /* Returns a promise so callers/tests can await settling; the production
     caller ignores it. */
  function check(current) {
    if (!current || current === '?') return Promise.resolve('skipped');
    var now = deps.now();
    var cached = readCache();

    if (
      cached &&
      cached.tag &&
      cached.checkedAt &&
      now - cached.checkedAt < TTL_MS
    ) {
      if (deps.compareSemver(cached.tag, current) > 0)
        deps.showBadge(cached.tag);
      return Promise.resolve('cached');
    }
    /* Back off after a failed check. GitHub's core limit resets hourly, so one
       retry per hour per browser is both polite and enough to recover without
       the user doing anything. */
    if (cached && cached.failedAt && now - cached.failedAt < FAIL_TTL_MS) {
      return Promise.resolve('backoff');
    }

    var rememberFailure = function () {
      write({ failedAt: now });
      return 'failed';
    };

    return deps
      .fetchLatest()
      .then(function (data) {
        var tag = data && data.tag_name;
        if (!tag) return rememberFailure();
        write({ tag: tag, checkedAt: now });
        if (deps.compareSemver(tag, current) > 0) deps.showBadge(tag);
        return 'fetched';
      })
      .catch(function (e) {
        /* Network blocked / GitHub rate-limited (403/429) — go quiet for
           FAIL_TTL_MS rather than retrying on every page load. Keep the debug
           line: it is the only client-side signal for a silently missing badge,
           and its sibling checkVersionMismatch() still logs one. */
        deps.onError(e);
        return rememberFailure();
      });
  }

  return { check: check };
}
/* update-check:END */

function checkForUpdate() {
  var badge = document.getElementById('version-badge');
  if (!badge) return;
  /* This check must stay on api.github.com — unlike the node's own poll it
     runs in a browser, and the quota-free github.com redirect sends no CORS
     headers so fetch() cannot read it. It therefore spends from the same
     60/hr per-IP REST budget, shared with every other machine behind the same
     NAT/CGNAT or VPN exit — hence the failure back-off above. */
  var checker = createUpdateChecker({
    now: function () {
      return Date.now();
    },
    getItem: function (k) {
      return localStorage.getItem(k);
    },
    setItem: function (k, v) {
      localStorage.setItem(k, v);
    },
    compareSemver: compareSemver,
    showBadge: showUpdateBadge,
    onError: function (e) {
      console.debug('Update check failed:', e);
    },
    fetchLatest: function () {
      return fetch(
        'https://api.github.com/repos/freenet/freenet-core/releases/latest',
        { headers: { Accept: 'application/vnd.github+json' } },
      ).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      });
    },
  });
  checker.check(badge.getAttribute('data-version') || '');
}

/* A version string is "known" when it is non-empty and not the '?'
   placeholder the homepage uses before a node snapshot exists.
   Mirrors version_is_known() in home_page.rs — keep both in sync. */
function versionIsKnown(v) {
  return !!v && v !== '?';
}

/* Show the stale-assets banner iff both the asset version (baked into this
   served page at compile time) and the live runtime version are known and
   differ. Mirrors should_show_version_banner() in home_page.rs. The point
   of comparing against a LIVE fetch (not the rendered data-version) is to
   catch the #4289 case: the browser is holding a cached page emitted by an
   old binary while a newer binary is now answering requests. */
function checkVersionMismatch() {
  var banner = document.getElementById('version-mismatch-banner');
  if (!banner) return;
  var assetVersion = banner.getAttribute('data-asset-version') || '';
  if (!versionIsKnown(assetVersion)) return;
  fetch('/v1/version', { headers: { Accept: 'application/json' } })
    .then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    })
    .then(function (data) {
      var runtimeVersion = data && data.version;
      if (!versionIsKnown(runtimeVersion)) return;
      if (runtimeVersion !== assetVersion) {
        banner.textContent =
          'Asset version ' +
          assetVersion +
          ' ≠ node version ' +
          runtimeVersion +
          ' — this page is stale, refresh to load the current version.';
        banner.hidden = false;
      } else {
        /* Versions agree (e.g. after a refresh fixed the staleness). */
        banner.hidden = true;
      }
    })
    .catch(function (e) {
      /* Endpoint unreachable / node mid-startup — don't show a spurious banner. */
      console.debug('Version check failed:', e);
    });
}

/* Tab switching for per-operation-type charts */
function switchTab(el) {
  var tabId = el.getAttribute('data-tab');
  /* Deactivate all tabs and panels in this group */
  var group = el.closest('.tab-group');
  if (!group) return;
  group.querySelectorAll('.tab-label').forEach(function (t) {
    t.classList.remove('tab-active');
  });
  group.querySelectorAll('.tab-panel').forEach(function (p) {
    p.classList.remove('tab-panel-active');
  });
  /* Activate selected */
  el.classList.add('tab-active');
  var panel = group.querySelector('#panel-' + tabId);
  if (panel) panel.classList.add('tab-panel-active');
  /* Remember active tab for auto-refresh persistence */
  try {
    sessionStorage.setItem('activeOpTab', tabId);
  } catch (e) {}
}

/* ── Import data (.fnsx) modal ──
   The receiving end of #4592: a user who exported a `freenet-data.fnsx` bundle
   from a hosted "try Freenet" server uploads it here to import their delegate
   secrets into THIS local peer via `POST /v1/import`. The modal lives outside
   <main> (see home.html) so the 5s auto-refresh never wipes it. */
function setImportStatus(msg, isError) {
  var el = document.getElementById('import-status');
  if (!el) return;
  el.textContent = msg || '';
  el.classList.toggle('import-status-error', !!isError);
}

function updateImportKeyLabel() {
  var kind = document.getElementById('import-key-kind');
  var label = document.getElementById('import-key-label');
  var input = document.getElementById('import-key');
  var isPass = kind && kind.value === 'passphrase';
  if (label) label.textContent = isPass ? 'Passphrase' : 'Access key';
  if (input)
    input.placeholder = isPass
      ? 'Enter your passphrase'
      : 'Paste your access key';
}

function openImportModal() {
  var modal = document.getElementById('import-modal');
  if (!modal) return;
  setImportStatus('', false);
  updateImportKeyLabel();
  modal.hidden = false;
  var file = document.getElementById('import-file');
  if (file) file.focus();
}

function closeImportModal() {
  var modal = document.getElementById('import-modal');
  if (!modal) return;
  modal.hidden = true;
  /* Clear the secret key (and the rest of the form) from the DOM on close. */
  var key = document.getElementById('import-key');
  if (key) key.value = '';
  var file = document.getElementById('import-file');
  if (file) file.value = '';
  var overwrite = document.getElementById('import-overwrite');
  if (overwrite) overwrite.checked = false;
  setImportStatus('', false);
}

function runImport() {
  var fileInput = document.getElementById('import-file');
  var keyInput = document.getElementById('import-key');
  var kindSel = document.getElementById('import-key-kind');
  var overwrite = document.getElementById('import-overwrite');
  var submit = document.getElementById('import-submit');

  var file = fileInput && fileInput.files && fileInput.files[0];
  if (!file) {
    setImportStatus('Choose a .fnsx file to import.', true);
    return;
  }
  var key = keyInput ? keyInput.value.trim() : '';
  if (!key) {
    setImportStatus('Enter the key that protects the bundle.', true);
    return;
  }

  var headers = {
    'X-Freenet-Bundle-Key': key,
    'X-Freenet-Bundle-Key-Kind': kindSel ? kindSel.value : 'token',
  };
  if (overwrite && overwrite.checked) {
    headers['X-Freenet-Import-Overwrite'] = 'true';
  }

  if (submit) submit.disabled = true;
  setImportStatus('Importing…', false);

  /* POST the raw file bytes as the body (application/octet-stream, NOT
     multipart) — /v1/import reads the body verbatim. The browser attaches an
     Origin header to this same-origin POST, which the import gate requires
     (loopback + trusted dashboard origin, no per-contract token). */
  fetch('/v1/import', { method: 'POST', headers: headers, body: file })
    .then(function (r) {
      return r.text().then(function (body) {
        return { ok: r.ok, status: r.status, body: body };
      });
    })
    .then(function (res) {
      if (submit) submit.disabled = false;
      if (res.ok) {
        var imported = 0;
        var skipped = 0;
        try {
          var data = JSON.parse(res.body);
          imported = data.imported || 0;
          skipped = (data.skipped && data.skipped.length) || 0;
        } catch (e) {
          /* A 200 with an unparseable body still means success. */
        }
        var msg =
          'Imported ' + imported + (imported === 1 ? ' secret' : ' secrets');
        if (skipped) msg += ', ' + skipped + ' skipped (already present)';
        closeImportModal();
        showToast(msg);
      } else {
        /* Non-2xx bodies are plain, non-secret reason strings from the
           endpoint (wrong-key 422, forbidden 403, too-large 413, ...). */
        var reason = (res.body || '').trim();
        setImportStatus(
          reason || 'Import failed (HTTP ' + res.status + ')',
          true,
        );
      }
    })
    .catch(function (e) {
      if (submit) submit.disabled = false;
      setImportStatus(
        'Import failed: ' + (e && e.message ? e.message : 'network error'),
        true,
      );
    });
}

/* ── Auto-refresh scheduler (extracted for Node unit tests) ──
   The scheduling state machine is deliberately self-contained: every browser
   dependency (timers, the actual fetch/DOM-swap refresh, visibility state) is
   injected via `deps`, so dashboard_refresh.test.mjs can extract this factory
   verbatim (between the refresh-scheduler:BEGIN/END markers) and drive it
   under Node with fake timers. Keep it free of direct references to
   window/document, and keep it bracketed by those markers.

   Invariants (tested behaviorally in dashboard_refresh.test.mjs):
   - one fetch at a time: refreshDashboard() is a no-op while a refresh is
     in flight (refreshInFlight guard);
   - the poll chain never forks: refreshTimer is reset to null the moment its
     setTimeout callback fires, so a concurrent clearTimeout(refreshTimer)
     can never silently no-op against an already-spent timer id;
   - hidden tabs poll at HIDDEN_REFRESH_MS, visible tabs at
     VISIBLE_REFRESH_MS (#3353), and becoming visible refreshes immediately
     unless a refresh is already running (then it just reschedules). */
/* refresh-scheduler:BEGIN */
function createRefreshScheduler(deps) {
  var VISIBLE_REFRESH_MS = 5000;
  var HIDDEN_REFRESH_MS = 60000;
  var refreshTimer = null;
  /* Guards against a second concurrent refreshDashboard() chain: without
     this, a visibilitychange->visible event racing against an in-flight
     timer-triggered fetch would clearTimeout() an id that already fired
     (a no-op) and then kick off a second .finally(scheduleRefresh) chain,
     breaking the "one fetch at a time" invariant. */
  var refreshInFlight = false;

  function currentRefreshInterval() {
    return deps.isHidden() ? HIDDEN_REFRESH_MS : VISIBLE_REFRESH_MS;
  }

  function refreshDashboard() {
    if (refreshInFlight) {
      /* A fetch is already running (either the timer-driven one or one
         kicked off by a prior visibilitychange) — do nothing so we never
         run two overlapping refresh chains. */
      return Promise.resolve();
    }
    refreshInFlight = true;
    return deps.refresh().finally(function () {
      refreshInFlight = false;
    });
  }

  function scheduleRefresh() {
    if (refreshTimer !== null) deps.clearTimeout(refreshTimer);
    refreshTimer = deps.setTimeout(function () {
      /* Clear before firing: once this callback runs, the timer id is
         already spent, so leaving refreshTimer set to it would make a
         concurrent clearTimeout(refreshTimer) elsewhere a silent no-op. */
      refreshTimer = null;
      refreshDashboard().finally(scheduleRefresh);
    }, currentRefreshInterval());
  }

  /* When the tab regains visibility, refresh right away instead of waiting
     out whatever remains of the hidden-tab backoff timer, then fall back to
     the normal cadence for the next tick. If a refresh is already in flight
     (e.g. the hidden-tab timer fired just before visibility changed), just
     reschedule at the normal cadence instead of starting a second fetch. */
  function onVisibilityChange() {
    if (deps.isHidden()) return;
    if (refreshTimer !== null) deps.clearTimeout(refreshTimer);
    refreshTimer = null;
    if (refreshInFlight) {
      scheduleRefresh();
      return;
    }
    refreshDashboard().finally(scheduleRefresh);
  }

  return {
    scheduleRefresh: scheduleRefresh,
    onVisibilityChange: onVisibilityChange,
  };
}
/* refresh-scheduler:END */

document.addEventListener('DOMContentLoaded', function () {
  /* Reads the EFFECTIVE theme, not the attribute: with no explicit choice the
     page follows the OS, and checking the attribute alone showed a sun icon
     to a light-mode user who was already looking at a light page. */
  updateThemeIcon();
  watchOsTheme();

  /* Restore active tab after page load / auto-refresh */
  function restoreTab() {
    try {
      var saved = sessionStorage.getItem('activeOpTab');
      if (saved) {
        var tab = document.querySelector(
          '.tab-label[data-tab="' + saved + '"]',
        );
        if (tab) switchTab(tab);
      }
    } catch (e) {}
  }
  restoreTab();
  restoreSort();
  restoreTableFilters();
  checkForUpdate();
  checkVersionMismatch();

  /* Import modal controls. These elements live OUTSIDE <main> (see home.html),
     so they are stable across auto-refresh and can be bound directly, once. */
  var importCancel = document.getElementById('import-cancel');
  if (importCancel) importCancel.addEventListener('click', closeImportModal);
  var importSubmit = document.getElementById('import-submit');
  if (importSubmit) importSubmit.addEventListener('click', runImport);
  var importKind = document.getElementById('import-key-kind');
  if (importKind) importKind.addEventListener('change', updateImportKeyLabel);
  var importOverlay = document.getElementById('import-modal');
  if (importOverlay) {
    importOverlay.addEventListener('click', function (ev) {
      /* A click on the backdrop (not the card) dismisses the modal. */
      if (ev.target === importOverlay) closeImportModal();
    });
  }
  var importKey = document.getElementById('import-key');
  if (importKey) {
    importKey.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        runImport();
      }
    });
  }
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    var modal = document.getElementById('import-modal');
    if (modal && !modal.hidden) closeImportModal();
  });

  /* Delegated click handler \u2014 survives <main> innerHTML swaps from auto-refresh,
       so we don't need to re-bind after each refresh. */
  document.addEventListener('input', function (ev) {
    var tf = ev.target.closest && ev.target.closest('.tf-input');
    if (tf) handleFilterInput(tf);
  });

  document.addEventListener('click', function (ev) {
    /* The open button is inside <main>, which auto-refresh re-renders, so it
       must be handled via delegation to survive innerHTML swaps. */
    var importOpen = ev.target.closest && ev.target.closest('.import-open-btn');
    if (importOpen) {
      ev.preventDefault();
      openImportModal();
      return;
    }
    /* Same reason as the import button: the filter controls live inside
       <main> and are destroyed by every auto-refresh, so they are handled
       by delegation rather than re-bound. */
    var tfToggle = ev.target.closest && ev.target.closest('.tf-toggle');
    if (tfToggle) {
      ev.preventDefault();
      handleFilterToggle(tfToggle);
      return;
    }
    var copy = ev.target.closest && ev.target.closest('.copy-key');
    if (copy) {
      ev.preventDefault();
      ev.stopPropagation();
      var text = copy.getAttribute('data-copy') || copy.textContent.trim();
      copyToClipboard(text)
        .then(function () {
          showToast('Contract key copied');
          copy.classList.add('copied');
          setTimeout(function () {
            copy.classList.remove('copied');
          }, 900);
        })
        .catch(function () {
          showToast('Copy failed', { error: true });
        });
      return;
    }
    var th = ev.target.closest && ev.target.closest('table.sortable thead th');
    if (th) {
      handleHeaderClick(th);
      return;
    }
  });

  /* Auto-refresh: fetch the page and swap dynamic content without a full reload.
       Uses setTimeout chaining (not setInterval) so slow responses don't overlap.

       Refresh cadence follows tab visibility (#3353): a hidden/backgrounded tab
       backs off to a much longer interval since nobody is watching, and polling
       every 5s while backgrounded only burns CPU/battery and spams the local
       node with requests nobody reads. The moment the tab becomes visible again
       we refresh immediately (rather than waiting out the stale timer) so the
       user sees current data right away, then resume the fast cadence.

       Scheduling policy (intervals, in-flight dedup, timer bookkeeping) lives
       in createRefreshScheduler above; this block supplies the browser-real
       dependencies: the fetch + DOM-swap refresh, real timers, and
       document.hidden. */
  function fetchAndSwapDashboard() {
    return fetch(window.location.href)
      .then(function (r) {
        return r.text();
      })
      .then(function (html) {
        var parser = new DOMParser();
        var doc = parser.parseFromString(html, 'text/html');
        var newMain = doc.querySelector('main');
        var oldMain = document.querySelector('main');
        /* Read the caret position BEFORE the innerHTML write on the next line
           destroys the element holding it. */
        var focusBeforeSwap = captureFilterFocus();
        if (newMain && oldMain) oldMain.innerHTML = newMain.innerHTML;
        /* Update the tab title (connection state + count, #3509) so a
           backgrounded tab still surfaces the current status at a glance. */
        if (doc.title) document.title = doc.title;
        /* Update header elements (outside <main>) */
        var newUp = doc.querySelector('.uptime');
        var oldUp = document.querySelector('.uptime');
        if (newUp && oldUp) oldUp.textContent = newUp.textContent;
        var newBadge = doc.querySelector('#version-badge');
        var oldBadge = document.getElementById('version-badge');
        if (newBadge && oldBadge) {
          oldBadge.textContent = newBadge.textContent;
          var nv = newBadge.getAttribute('data-version');
          if (nv) oldBadge.setAttribute('data-version', nv);
        }
        var newIcon = doc.querySelector('link[rel="icon"]');
        var oldIcon = document.querySelector('link[rel="icon"]');
        if (newIcon && oldIcon)
          oldIcon.setAttribute('href', newIcon.getAttribute('href'));
        /* Restore tab selection, table sort and table filters after the
           content swap. The filter restore is NOT optional here: the swap
           destroys the input element, so without this the box empties and the
           table re-expands roughly every five seconds — which browser
           validation caught and no Rust test could, since they never execute
           this file. */
        restoreTab();
        restoreSort();
        restoreTableFilters(focusBeforeSwap);
        /* Re-check the live runtime version so the stale-assets banner
                 appears (or clears) if the serving process changes while the
                 page stays open. The banner's data-asset-version stays anchored
                 to the originally-loaded page, which is the version we're
                 comparing against. */
        checkVersionMismatch();
      })
      .catch(function (e) {
        console.warn('Dashboard refresh failed:', e);
      });
  }

  var refreshScheduler = createRefreshScheduler({
    setTimeout: function (fn, ms) {
      return setTimeout(fn, ms);
    },
    clearTimeout: function (id) {
      clearTimeout(id);
    },
    refresh: fetchAndSwapDashboard,
    isHidden: function () {
      return document.hidden;
    },
  });

  document.addEventListener(
    'visibilitychange',
    refreshScheduler.onVisibilityChange,
  );

  refreshScheduler.scheduleRefresh();
});
