// Running a query: paging, timed runs, errors, the filter chips and
// drill-down, and the top-level render of a response.
import { bff } from './bff.js';
import { clear, el, errorText, fmtMs } from './common.js';
import {
  celString, compareRuns, distribution, encodeState, splitConjuncts, toggleConjunct,
} from './query-state.js';
import { $, TIMED_RUNS, state } from './search-state.js';
import { currentHash, readForm, remember } from './search-form.js';
import { buildRequest, syncFolds } from './search-request.js';
import { droppedNode, fetchTexts, learnColumns, renderBaseline } from './search-results.js';
import {
  hitNode, pageScale, renderAggregate, renderDense, renderProfile, renderRaw, renderSynonyms, share,
} from './search-hits.js';
import { didYouMean, hideSuggest, stopStream } from './search-live.js';

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

// One run of the form. A run that is not a page turn starts from the
// first page; `push` writes the form to the URL as a new history entry.
export async function run({ paging = false, push = true } = {}) {
  stopStream();
  hideSuggest();
  syncFolds();
  if (!paging) { state.cursor = ''; state.cursors = []; }
  $('ab-panel').classList.add('hidden');
  $('results-panel').classList.remove('hidden');
  hideError();
  const hash = encodeState(readForm(), state.defaults);
  if (push && hash !== currentHash()) history.pushState(null, '', hash ? `#${hash}` : location.pathname);
  let request;
  try {
    request = await buildRequest();
  } catch (e) {
    return showError(e);
  }
  state.request = request;
  $('run').disabled = true;
  const started = performance.now();
  try {
    const response = await bff.call('Query', request);
    response._wall_ms = performance.now() - started;
    state.response = response;
    await render(response);
    if (!paging) remember(hash, response);
    learnColumns();
    didYouMean();
  } catch (e) {
    state.response = { error: e };
    showError(e);
    renderRaw();
  } finally {
    $('run').disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Timing: the same request in sequence, for its latency distribution
// ---------------------------------------------------------------------------

export async function timeQuery() {
  stopStream();
  hideSuggest();
  hideError();
  let request;
  try {
    request = await buildRequest();
  } catch (e) {
    return showError(e);
  }
  // The first page, with the server's own total and without the cost
  // of an explain tree.
  delete request.explain;
  delete request.cursor;
  request.profile = true;
  const button = $('time');
  button.disabled = true;
  const timing = { label: `${$('shape').value} ${JSON.stringify($('q').value.trim())}`, server: [], wall: [], rankings: new Set() };
  try {
    for (let i = 0; i < TIMED_RUNS; i++) {
      button.textContent = `${i + 1}/${TIMED_RUNS}`;
      const started = performance.now();
      const r = await bff.call('Query', request);
      timing.wall.push(performance.now() - started);
      timing.server.push(Number(r.profile?.totalMs ?? 0));
      const hits = r.groups?.length ? r.groups.flatMap((g) => g.hits || []) : r.hits || [];
      timing.rankings.add(hits.map((h) => h.docId).join(','));
    }
    state.timings = [timing, ...state.timings].slice(0, 2);
    renderTiming();
  } catch (e) {
    showError(e);
  } finally {
    button.disabled = false;
    button.textContent = 'Time';
  }
}

// The newest timing over the one before it, on one axis from zero so
// the two can be compared by eye.
export function renderTiming() {
  const box = clear($('timing'));
  box.classList.toggle('hidden', !state.timings.length);
  const max = Math.max(...state.timings.flatMap((t) => [...t.server, ...t.wall]));
  state.timings.forEach((t, i) => {
    box.append(el('div', { class: 'head' }, [
      el('span', { class: 'muted', text: i ? 'before' : 'timed' }), ' ',
      el('span', { class: 'mono', text: t.label }), ' ',
      el('span', { class: 'muted', text: `${t.wall.length} runs in sequence, ${t.rankings.size === 1 ? 'the same ranking every run' : `${t.rankings.size} different rankings`}` }),
    ]));
    for (const [name, values, title] of [['server', t.server, 'QueryProfile.total_ms'], ['wall', t.wall, 'request to response in this browser']]) {
      const d = distribution(values);
      const strip = el('span', { class: `strip${i ? ' before' : ''}`, role: 'img', 'aria-label': `${name}: min ${fmtMs(d.min)}, p50 ${fmtMs(d.p50)}, p95 ${fmtMs(d.p95)}, max ${fmtMs(d.max)}` });
      for (const v of values) strip.append(el('i', { style: `left:${share(v, max)}%`, title: fmtMs(v) }));
      strip.append(el('b', { style: `left:${share(d.p50, max)}%`, title: `p50 ${fmtMs(d.p50)}` }));
      box.append(el('div', { class: 'row', title }, [
        el('span', { class: 'mono muted', text: name }),
        strip,
        el('span', { class: 'mono', text: `min ${fmtMs(d.min)} · p50 ${fmtMs(d.p50)} · p95 ${fmtMs(d.p95)} · max ${fmtMs(d.max)}` }),
      ]));
    }
  });
  if (state.timings.length) box.append(el('div', { class: 'axis mono muted' }, [el('span', { text: '0' }), el('span', { text: fmtMs(max) })]));
}

export function page(direction) {
  if (direction > 0) {
    if (!state.response?.nextCursor) return;
    state.cursors.push(state.cursor);
    state.cursor = state.response.nextCursor;
  } else {
    if (!state.cursors.length) return;
    state.cursor = state.cursors.pop();
  }
  run({ paging: true, push: false });
}

// The field an engine message is about, by the word the message opens
// with or names, so the red line has a place on the form.
export const ERROR_FIELDS = [
  [/^histogram\b/, 'hist-expr'], [/^percentile/, 'pct-expr'], [/^projections?\b/, 'columns'],
  [/^sort\b/, 'sort'], [/^collapse\b/, 'collapse'], [/^filter\b|\bCEL\b/, 'filter'],
  [/^aggregat|group_by|group-by/, 'group-by'],
];

export function showError(e) {
  const node = $('error');
  node.textContent = errorText(e);
  node.classList.remove('hidden');
  const message = String(e?.error ?? '');
  const field = ERROR_FIELDS.find(([pattern]) => pattern.test(message));
  if (field) $(field[1]).classList.add('invalid');
  if ($('results').querySelector('.hit')) {
    $('results-panel').classList.add('stale');
    $('result-count').textContent = 'the query failed; these are the previous results';
  }
}

export function hideError() {
  $('error').classList.add('hidden');
  $('results-panel').classList.remove('stale');
  for (const node of document.querySelectorAll('.invalid')) node.classList.remove('invalid');
}

// ---------------------------------------------------------------------------
// The filter as chips, and drill-down into it
// ---------------------------------------------------------------------------

export function renderChips() {
  const box = clear($('filter-chips'));
  const parts = splitConjuncts($('filter').value);
  box.classList.toggle('hidden', !parts.length);
  if (!parts.length) return;
  box.append(el('span', { class: 'muted small', text: 'filter' }));
  for (const part of parts) {
    box.append(el('span', { class: 'chip mono' }, [
      part,
      el('button', { text: '×', title: 'remove this condition', onclick: () => setFilter(toggleConjunct($('filter').value, part)) }),
    ]));
  }
}

export function setFilter(value) {
  $('filter').value = value;
  renderChips();
  run();
}

// Narrow to (or, with `exclude`, away from) one value of a column.
export function drill(column, value, exclude) {
  const is = `${column} == ${celString(value)}`;
  const not = `${column} != ${celString(value)}`;
  const [add, drop] = exclude ? [not, is] : [is, not];
  const parts = splitConjuncts($('filter').value);
  if (parts.includes(drop)) setFilter(toggleConjunct(toggleConjunct($('filter').value, drop), add));
  else setFilter(toggleConjunct($('filter').value, add));
}

export function drillTitle(column, value) {
  return `click: only ${column} == ${value}; shift-click: exclude it; click again to undo`;
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

export async function render(response) {
  const hits = response.hits || [];
  const groups = response.groups || [];
  const count = groups.length ? `${groups.length} group(s)` : `${hits.length} hit(s)`;
  const unhighlighted = $('highlight').checked && !state.request.highlight && $('shape').value === 'lexical';
  $('result-count').textContent = `${count}, executed ${response.executed || ''}${unhighlighted ? ' (no snippets: the engine cuts them for an unfiltered lexical query only)' : ''}`;
  $('next').disabled = !response.nextCursor;
  $('prev').disabled = !state.cursors.length;
  renderProfile(response);
  renderAggregate(response.aggregate);
  renderSynonyms(response.synonymExpansions);
  renderDense(response);
  renderRaw();

  const all = groups.length ? groups.flatMap((g) => g.hits || []) : hits;
  if ($('fetch-text').checked) await fetchTexts(all.map((h) => h.docId));
  const scale = pageScale(all);
  for (const id of ['pin', 'export-csv', 'export-json']) $(id).classList.toggle('hidden', !all.length);
  const comparison = state.baseline && !state.cursor ? compareRuns(state.baseline.ids, all.map((h) => String(h.docId))) : null;
  if (comparison) all.forEach((h, i) => scale.moves.set(h, comparison.moves[i]));
  renderBaseline(comparison);
  const results = clear($('results'));
  if (!all.length) {
    results.append(el('div', { class: 'empty', text: 'Nothing matched. Loosen the filter, or check the did-you-mean line above.' }));
  } else if (groups.length) {
    for (const g of groups) {
      const key = g.key ? (g.key.text ?? g.key.integer ?? g.key.number) : '';
      const box = el('div', { class: 'group' }, [
        el('div', { class: 'key' }, [`${key}`, ' ', el('span', { class: 'muted small mono', text: `${g.complete ? 'complete' : 'partial'}, pool ${g.poolHits ?? 0}` })]),
      ]);
      const inner = el('div', { class: 'inner' });
      for (const h of g.hits || []) inner.append(hitNode(h, scale));
      box.append(inner);
      results.append(box);
    }
  } else {
    for (const h of hits) results.append(hitNode(h, scale));
  }
  if (comparison?.dropped.length) results.append(await droppedNode(comparison.dropped));
}
