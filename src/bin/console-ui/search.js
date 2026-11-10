// The search page: builds a QueryRequest from the form, runs it through
// the facade, and renders hits, explain trees, groups, aggregations, and
// the profile. Also typeahead (Suggest), did-you-mean (TermSuggest), the
// streaming query, the A/B comparison (VariantSearch), the filter chips
// and drill-down, the URL state, and the history.
import {
  api, el, clear, fmtMs, fmtNum, debounce, loadConfig, grpcurlFor, mountHeader,
  copyText, errorText, enums,
} from '/common.js';
import {
  splitTopLevel, splitConjuncts, toggleConjunct, replaceRange, isIdentifier, celString,
  rangeColumn, rangeClause, calendarUpper, encodeState, decodeState,
} from '/query-state.js';

const $ = (id) => document.getElementById(id);
const state = {
  config: null,
  request: null,
  response: null,
  cursors: [],      // cursor stack for paging back
  cursor: '',
  texts: new Map(), // doc id -> text
  tab: 'response',
  stream: null,
  vector: null,     // last embedded vector, with the text it came from
  defaults: null,   // the form as the page loads it, for the URL state
  suggestIndex: -1,
};

// The controls the URL carries. A pasted vector is left out: it does not
// fit in a URL.
const FORM_IDS = [
  'q', 'shape', 'fusion', 'rrf-k', 'blend-norm', 'blend-comb', 'w-dense', 'w-lex', 'score-mode',
  'msm', 'filter', 'k', 'selection-k', 'sort', 'collapse', 'inner-hits', 'columns', 'highlight',
  'hl-mode', 'explain', 'profile', 'fetch-text', 'group-by', 'hist-expr', 'hist-interval',
  'hist-calendar', 'pct-expr', 'pct-list', 'ab-a', 'ab-b',
];
const HISTORY_KEY = 'pipestream.console.history';
const HISTORY_MAX = 30;

mountHeader('search');
init();

async function init() {
  try {
    state.config = await loadConfig();
  } catch (e) {
    showError(e);
    state.config = { analysis: false, methods: [] };
  }
  if (!state.config.analysis) $('dense-note').classList.remove('hidden');
  $('shape').addEventListener('change', syncShape);
  $('fusion').addEventListener('change', syncShape);
  $('add-clause').addEventListener('click', () => addClause());
  addClause('must', 'lexical', '');
  $('add-agg').addEventListener('click', () => addAgg());
  $('run').addEventListener('click', () => run());
  $('stream').addEventListener('click', () => (state.stream ? stopStream() : streamQuery()));
  $('prev').addEventListener('click', () => page(-1));
  $('next').addEventListener('click', () => page(1));
  $('q').addEventListener('keydown', queryKey);
  $('q').addEventListener('input', debounce(typeahead, 150));
  $('q').addEventListener('blur', () => setTimeout(hideSuggest, 150));
  $('filter').addEventListener('input', renderChips);
  $('keys').addEventListener('click', toggleKeys);
  document.addEventListener('keydown', pageKey);
  for (const b of document.querySelectorAll('.tabs button')) {
    b.addEventListener('click', () => { state.tab = b.dataset.tab; renderRaw(); });
  }
  $('copy-raw').addEventListener('click', () => copyText($('raw').textContent));
  const presets = variantPresets();
  for (const sel of [$('ab-a'), $('ab-b')]) {
    for (const p of presets) sel.append(el('option', { value: p.id, text: p.label }));
  }
  $('ab-b').value = presets[1]?.id ?? presets[0].id;
  $('run-ab').addEventListener('click', () => runAb());
  $('clear-history').addEventListener('click', () => { saveHistory([]); renderHistory(); });
  state.defaults = readForm();
  syncShape();
  renderHistory();
  window.addEventListener('popstate', () => restoreFromUrl());
  restoreFromUrl();
}

// ---------------------------------------------------------------------------
// The form as a value: the URL state and the history restore from it
// ---------------------------------------------------------------------------

function readForm() {
  const form = {};
  for (const id of FORM_IDS) {
    const node = $(id);
    form[id] = node.type === 'checkbox' ? node.checked : node.value;
  }
  form.clauses = [...$('clauses').querySelectorAll('.clause')].map((row) => ({
    kind: row.querySelector('.kind').value,
    leaf: row.querySelector('.leaf').value,
    value: row.querySelector('.value').value,
  }));
  form.aggs = [...$('aggs').querySelectorAll('.agg')].map((row) => ({
    name: row.querySelector('.name').value,
    expression: row.querySelector('.expr').value,
    op: row.querySelector('.op').value,
  }));
  return form;
}

function writeForm(form) {
  for (const id of FORM_IDS) {
    const node = $(id);
    if (node.type === 'checkbox') node.checked = !!form[id];
    else node.value = form[id];
  }
  clear($('clauses'));
  for (const c of form.clauses) addClause(c.kind, c.leaf, c.value);
  clear($('aggs'));
  for (const a of form.aggs) addAgg(a.name, a.expression, a.op);
  syncShape();
  renderChips();
}

function currentHash() { return location.hash.replace(/^#/, ''); }

// The URL fragment is the form: restore it, and run when it names a query.
function restoreFromUrl() {
  const hash = currentHash();
  writeForm(decodeState(hash, state.defaults));
  if (hash) run({ push: false });
}

function loadHistory() {
  try {
    const list = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

function saveHistory(list) {
  try { localStorage.setItem(HISTORY_KEY, JSON.stringify(list)); } catch { /* storage is a convenience */ }
}

function remember(hash, response) {
  if (!hash) return;
  const form = readForm();
  const entry = {
    hash,
    text: form.q,
    shape: form.shape,
    filter: form.filter,
    hits: (response.groups?.length ? response.groups.flatMap((g) => g.hits || []) : response.hits || []).length,
    ms: response._wall_ms,
    at: Date.now(),
  };
  saveHistory([entry, ...loadHistory().filter((h) => h.hash !== hash)].slice(0, HISTORY_MAX));
  renderHistory();
}

function renderHistory() {
  const list = loadHistory();
  const box = clear($('history'));
  $('clear-history').classList.toggle('hidden', !list.length);
  if (!list.length) { box.textContent = 'Queries you run are listed here.'; return; }
  for (const h of list) {
    box.append(el('a', { class: 'past', href: `#${h.hash}`, title: new Date(h.at).toLocaleString() }, [
      el('span', { class: 'what' }, [
        el('span', { class: 'q', text: h.text || '(no text)' }),
        h.filter ? el('span', { class: 'mono muted', text: ` ${h.filter}` }) : null,
      ]),
      el('span', { class: 'mono muted', text: `${h.shape} · ${fmtNum(h.hits)} · ${fmtMs(h.ms)}` }),
    ]));
  }
}

// ---------------------------------------------------------------------------
// Form -> QueryRequest
// ---------------------------------------------------------------------------

function syncShape() {
  const shape = $('shape').value;
  $('shape-hybrid').classList.toggle('hidden', shape !== 'hybrid');
  $('shape-dense-fields').classList.toggle('hidden', !(shape === 'dense' || shape === 'hybrid'));
  $('shape-boolean').classList.toggle('hidden', shape !== 'boolean');
  const fusion = $('fusion').value;
  $('rrf-fields').classList.toggle('hidden', fusion !== 'rrf');
  $('blend-fields').classList.toggle('hidden', fusion !== 'blend');
}

function addClause(kind = 'should', leaf = 'lexical', value = '') {
  const row = el('div', { class: 'clause' }, [
    el('select', { class: 'kind' }, ['must', 'should', 'must_not'].map((k) => el('option', { value: k, text: k, selected: k === kind }))),
    el('select', { class: 'leaf' }, ['lexical', 'dense', 'filter'].map((k) => el('option', { value: k, text: k, selected: k === leaf }))),
    el('input', { class: 'mono value', value, placeholder: 'text, or a CEL predicate, or (dense) the query text' }),
    el('button', { text: '×', title: 'remove this clause', onclick: () => row.remove() }),
  ]);
  $('clauses').append(row);
}

function addAgg(name = '', expression = '', op = 'AGGREGATE_OP_COUNT') {
  const row = el('div', { class: 'agg' }, [
    el('input', { class: 'mono name', value: name, placeholder: 'name' }),
    el('input', { class: 'mono expr', value: expression, placeholder: 'expression, e.g. year' }),
    el('select', { class: 'op' }, enums.aggregateOp.map((o) => el('option', { value: o, text: o.replace('AGGREGATE_OP_', '').toLowerCase(), selected: o === op }))),
    el('button', { text: '×', title: 'remove this fold', onclick: () => row.remove() }),
  ]);
  $('aggs').append(row);
}

function lexicalLeaf(id, text) {
  return { search: { id, lexical: { text } } };
}

function denseLeaf(id, vector) {
  const dense = { vector };
  if ($('score-mode').value === 'fp32') dense.score_mode = enums.fp32;
  return { search: { id, dense } };
}

function filterLeaf(id, cel) {
  return { filter: { id, cel } };
}

async function queryVector(text) {
  const pasted = $('vector').value.trim();
  if (pasted) {
    const v = JSON.parse(pasted);
    if (!Array.isArray(v) || !v.every((x) => typeof x === 'number')) throw { error: 'the pasted vector is not a JSON array of numbers' };
    return v;
  }
  if (!state.config.analysis) throw { error: 'a dense query needs a pasted vector: no analysis sidecar is configured', code: 'FAILED_PRECONDITION' };
  if (state.vector && state.vector.text === text) return state.vector.vector;
  const r = await api.post('/api/embed', { text });
  state.vector = { text, vector: r.vector };
  return r.vector;
}

function parseSort(spec) {
  return spec.split(',').map((s) => s.trim()).filter(Boolean).map((s) => {
    const [column, dir] = s.split(/\s+/);
    return dir && dir.toLowerCase() === 'desc' ? { column, descending: true } : { column };
  });
}

function aggregateSpec() {
  const out = {};
  const groupBy = $('group-by').value.trim();
  if (groupBy) { out.group_by = groupBy; out.max_groups = 50; }
  const aggregations = [];
  for (const row of $('aggs').querySelectorAll('.agg')) {
    const name = row.querySelector('.name').value.trim();
    const expression = row.querySelector('.expr').value.trim();
    if (!name && !expression) continue;
    aggregations.push({ name: name || expression, expression: expression || '1', op: row.querySelector('.op').value });
  }
  const histExpr = $('hist-expr').value.trim();
  if (histExpr) {
    const h = { name: `hist:${histExpr}`, expression: histExpr, max_buckets: 200 };
    const calendar = $('hist-calendar').value;
    if (calendar) h.calendar = calendar;
    else h.interval = Number($('hist-interval').value || 1);
    out.histograms = [h];
  }
  const pctExpr = $('pct-expr').value.trim();
  if (pctExpr) {
    const percentiles = ($('pct-list').value || '50, 90, 99').split(',').map((s) => Number(s.trim())).filter((n) => Number.isFinite(n));
    out.percentiles = [{ name: `pct:${pctExpr}`, expression: pctExpr, percentiles }];
  }
  // A group-by alone is not a request the engine takes: it wants at
  // least one fold, so the count the rail shows anyway is asked for.
  if (groupBy && !aggregations.length && !out.histograms && !out.percentiles) {
    aggregations.push({ name: 'count', expression: '1', op: 'AGGREGATE_OP_COUNT' });
  }
  if (aggregations.length) out.aggregations = aggregations;
  return Object.keys(out).length ? out : null;
}

async function buildRequest() {
  const text = $('q').value.trim();
  const shape = $('shape').value;
  const filter = $('filter').value.trim();
  let selection;
  let vector;
  const needsVector = shape === 'dense' || shape === 'hybrid'
    || (shape === 'boolean' && [...$('clauses').querySelectorAll('.leaf')].some((l) => l.value === 'dense'));
  if (needsVector) vector = await queryVector(text || $('vector').value.trim());
  if (shape !== 'browse' && shape !== 'boolean' && !text && !vector) throw { error: 'query text is required' };

  if (shape === 'lexical') selection = lexicalLeaf('lex', text);
  else if (shape === 'dense') selection = denseLeaf('dense', vector);
  else if (shape === 'hybrid') {
    const scoring = {};
    const wd = Number($('w-dense').value), wl = Number($('w-lex').value);
    const weights = {};
    if ($('w-dense').value) weights.dense_weight = wd;
    if ($('w-lex').value) weights.lexical_weight = wl;
    const fusion = $('fusion').value;
    if (fusion === 'rrf') scoring.rrf = { rrf_k: Number($('rrf-k').value || 60), ...weights };
    else if (fusion === 'blend') scoring.score_blend = { normalization: enums.normalization[$('blend-norm').value], combination: enums.combination[$('blend-comb').value], ...weights };
    else if (fusion === 'decomposed') scoring.decomposed = weights;
    else scoring.cascade = { gate_id: 'dense' };
    selection = { composite: { operator: enums.or, clauses: [lexicalLeaf('lex', text), denseLeaf('dense', vector)], scoring } };
  } else if (shape === 'boolean') {
    const boolean = { must: [], should: [], must_not: [] };
    let i = 0;
    for (const row of $('clauses').querySelectorAll('.clause')) {
      const kind = row.querySelector('.kind').value;
      const leaf = row.querySelector('.leaf').value;
      const value = row.querySelector('.value').value.trim();
      const id = `${leaf}${i++}`;
      let node;
      if (leaf === 'lexical') node = lexicalLeaf(id, value || text);
      else if (leaf === 'dense') node = denseLeaf(id, await queryVector(value || text));
      else node = filterLeaf(id, value);
      boolean[kind].push(node);
    }
    if (filter) boolean.must.push(filterLeaf('filter', filter));
    const msm = Number($('msm').value || 0);
    if (msm) boolean.minimum_should_match = msm;
    const agg = aggregateSpec();
    if (agg) boolean.aggregate = agg;
    selection = { boolean };
  } else {
    if (!filter) throw { error: 'a browse needs a CEL filter' };
    selection = filterLeaf('filter', filter);
  }
  if (filter && shape !== 'boolean' && shape !== 'browse') {
    selection = { composite: { operator: enums.and, clauses: [selection, filterLeaf('filter', filter)] } };
  }

  const request = { k: Number($('k').value || 10), selection };
  const selectionK = Number($('selection-k').value || 0);
  if (selectionK) request.selection_k = selectionK;
  if ($('explain').checked) request.explain = true;
  if ($('profile').checked) request.profile = true;
  const sort = parseSort($('sort').value);
  if (sort.length) request.sort = sort;
  const collapse = $('collapse').value.trim();
  if (collapse) request.collapse = { column: collapse, inner_hits: Number($('inner-hits').value || 0) };
  const columns = splitTopLevel($('columns').value, ',');
  if (columns.length) request.projections = columns.map((expression) => ({ name: expression, expression }));
  // Snippets are served for the single lexical selection only, and a
  // filter makes the selection a composite; the stored text shows then.
  if ($('highlight').checked && shape === 'lexical' && !filter) {
    request.highlight = { max_snippets: 2, max_chars: 240, mode: $('hl-mode').value };
  }
  if (shape !== 'boolean') {
    const agg = aggregateSpec();
    if (agg) request.aggregate = agg;
  }
  if (state.cursor) request.cursor = state.cursor;
  return request;
}

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

// One run of the form. A run that is not a page turn starts from the
// first page; `push` writes the form to the URL as a new history entry.
async function run({ paging = false, push = true } = {}) {
  stopStream();
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
    const response = await api.rpc('SearchService', 'Query', request);
    response._wall_ms = performance.now() - started;
    state.response = response;
    await render(response);
    if (!paging) remember(hash, response);
    didYouMean();
  } catch (e) {
    state.response = { error: e };
    showError(e);
    renderRaw();
  } finally {
    $('run').disabled = false;
  }
}

function page(direction) {
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
const ERROR_FIELDS = [
  [/^histogram\b/, 'hist-expr'], [/^percentile/, 'pct-expr'], [/^projection\b/, 'columns'],
  [/^sort\b/, 'sort'], [/^collapse\b/, 'collapse'], [/^filter\b|\bCEL\b/, 'filter'],
  [/^aggregat|group_by|group-by/, 'group-by'],
];

function showError(e) {
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

function hideError() {
  $('error').classList.add('hidden');
  $('results-panel').classList.remove('stale');
  for (const node of document.querySelectorAll('.invalid')) node.classList.remove('invalid');
}

// ---------------------------------------------------------------------------
// The filter as chips, and drill-down into it
// ---------------------------------------------------------------------------

function renderChips() {
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

function setFilter(value) {
  $('filter').value = value;
  renderChips();
  run();
}

// Narrow to (or, with `exclude`, away from) one value of a column.
function drill(column, value, exclude) {
  const is = `${column} == ${celString(value)}`;
  const not = `${column} != ${celString(value)}`;
  const [add, drop] = exclude ? [not, is] : [is, not];
  const parts = splitConjuncts($('filter').value);
  if (parts.includes(drop)) setFilter(toggleConjunct(toggleConjunct($('filter').value, drop), add));
  else setFilter(toggleConjunct($('filter').value, add));
}

function drillTitle(column, value) {
  return `click: only ${column} == ${value}; shift-click: exclude it; click again to undo`;
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

async function render(response) {
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
}

async function fetchTexts(ids) {
  const missing = ids.filter((id) => id != null && !state.texts.has(String(id)));
  if (!missing.length) return;
  try {
    const r = await api.post('/api/documents', { doc_ids: missing });
    for (const d of r.documents || []) state.texts.set(String(d.doc_id), d);
  } catch (e) {
    // Text is a convenience; the hit list still renders.
    for (const id of missing) state.texts.set(String(id), { text: '', error: errorText(e) });
  }
}

// What the bars on a page of hits are drawn against: the top score, and
// per signal id the largest value on the page.
function pageScale(hits) {
  const scale = { score: 0, signals: new Map(), legs: 0 };
  for (const h of hits) {
    scale.score = Math.max(scale.score, Number(h.score ?? 0));
    scale.legs = Math.max(scale.legs, (h.signals || []).length);
    for (const s of h.signals || []) scale.signals.set(s.id, Math.max(scale.signals.get(s.id) ?? 0, Number(s.score)));
  }
  return scale;
}

function share(value, max) {
  return max > 0 ? Math.max(0, Math.min(100, (100 * Number(value)) / max)) : 0;
}

function projectedValue(p) {
  return p.stringValue ?? p.uintValue ?? p.intValue ?? p.doubleValue ?? p.boolValue;
}

function hitNode(h, scale) {
  const doc = state.texts.get(String(h.docId));
  const node = el('div', { class: 'hit' });
  node.append(el('div', { class: 'head' }, [
    el('span', { class: 'rank', text: `#${h.rank ?? ''}` }),
    el('span', { class: 'score', text: Number(h.score ?? 0).toFixed(6) }),
    scale.score > 0 ? el('span', { class: 'scorebar', title: `${share(h.score, scale.score).toFixed(0)}% of the top score on this page` }, [el('span', { style: `width:${share(h.score, scale.score)}%` })]) : null,
    el('button', { class: 'id quiet', text: `doc ${h.docId}`, title: 'copy the document id', onclick: () => copyText(String(h.docId)) }),
    doc?.lineage ? el('span', { class: 'muted', text: `parent ${doc.lineage.parent_id} group ${doc.lineage.group_id}` }) : null,
    h.sortValues?.length ? el('span', { class: 'muted', text: `sort ${h.sortValues.map((v) => v.text ?? v.integer ?? v.number).join(' | ')}` }) : null,
  ]));

  // The projected values carry no name: the request's projection at the
  // same index names each.
  const projections = state.request?.projections || [];
  if (h.projected?.length) {
    const columns = el('div', { class: 'columns' });
    h.projected.forEach((p, i) => {
      const name = projections[i]?.name ?? `#${i}`;
      const value = projectedValue(p);
      const shown = value == null ? '–' : p.intValue != null ? epochLabel(p.intValue) : String(value);
      const facet = p.stringValue != null && isIdentifier(name);
      columns.append(el(facet ? 'button' : 'span', {
        class: 'chip',
        title: facet ? drillTitle(name, value) : value == null ? 'no value' : String(value),
        onclick: facet ? (e) => drill(name, value, e.shiftKey) : null,
      }, [el('span', { class: 'muted', text: `${name} ` }), el('span', { class: 'mono', text: shown })]));
    });
    node.append(columns);
  }

  if (h.snippets?.length) {
    for (const s of h.snippets) node.append(el('div', { class: 'text' }, snippetNodes(s)));
  } else if (doc?.text) {
    node.append(textNode(doc.text));
  } else if (doc?.error) {
    node.append(el('div', { class: 'muted small', text: `text unavailable: ${doc.error}` }));
  }

  // One signal is the score again; two or more are the legs, each drawn
  // against the largest value that leg has on this page.
  if (scale.legs > 1 && h.signals?.length) {
    const legs = el('div', { class: 'legs' });
    for (const s of h.signals) {
      legs.append(el('div', { class: 'leg', title: `${s.id} ${s.score}: ${share(s.score, scale.signals.get(s.id)).toFixed(0)}% of this leg's best on the page` }, [
        el('span', { class: 'mono muted', text: s.id }),
        el('span', { class: 'track' }, [el('span', { style: `width:${share(s.score, scale.signals.get(s.id))}%` })]),
        el('span', { class: 'mono', text: Number(s.score).toFixed(4) }),
      ]));
    }
    node.append(legs);
  }
  const meta = el('div', { class: 'meta signals' });
  for (const d of h.dimensions || []) meta.append(el('span', { text: `${d.id}: ${d.skipped ? 'skipped' : `raw ${fmtNum(d.raw)} norm ${fmtNum(d.normalized)} → ${fmtNum(d.contribution)}`}` }));
  if (h.matched?.length) meta.append(el('span', { text: `matched ${h.matched.join(',')}` }));
  if (meta.firstChild) node.append(meta);
  if (h.explain) {
    const drawer = el('details', { class: 'tree' }, [el('summary', { class: 'muted small', text: 'explain' })]);
    drawer.append(el('ul', {}, [explainNode(h.explain)]));
    node.append(drawer);
  }
  return node;
}

// Stored text, clamped to a few lines with a toggle when it is long.
function textNode(text) {
  const long = text.length > 420 || text.split('\n').length > 5;
  const body = el('div', { class: long ? 'text clamp' : 'text', text });
  if (!long) return body;
  const toggle = el('button', { class: 'quiet small', text: 'more', onclick: () => { toggle.textContent = body.classList.toggle('clamp') ? 'more' : 'less'; } });
  return el('div', {}, [body, toggle]);
}

function snippetNodes(s) {
  const text = s.text || '';
  const spans = (s.highlights || []).map((x) => [Number(x.start ?? 0), Number(x.end ?? 0)]).sort((a, b) => a[0] - b[0]);
  const out = [];
  let pos = 0;
  for (const [start, end] of spans) {
    if (start < pos || end > text.length) continue;
    out.push(text.slice(pos, start), el('mark', { text: text.slice(start, end) }));
    pos = end;
  }
  out.push(text.slice(pos));
  if (s.field) out.unshift(el('span', { class: 'muted small', text: `${s.field}: ` }));
  return out;
}

function explainNode(node) {
  const details = (node.details || []);
  const label = [el('span', { class: 'v', text: fmtNum(node.value) }), node.description || ''];
  if (!details.length) return el('li', {}, label);
  const d = el('details', { open: true }, [el('summary', {}, label), el('ul', {}, details.map(explainNode))]);
  return el('li', {}, [d]);
}

// The phases of QueryProfile in the order they run, each with its slot
// in the categorical palette (app.css): the slot follows the phase.
const PHASES = [
  ['selection', 'selectionMs'], ['boost', 'boostMs'], ['values', 'valuesMs'], ['scorer', 'scorerMs'],
  ['projection', 'projectionMs'], ['rerank', 'rerankMs'], ['collapse', 'collapseMs'],
];

function renderProfile(response) {
  const line = clear($('profile-line'));
  const bar = clear($('phases'));
  const p = response.profile;
  line.append(el('span', { text: `wall ${fmtMs(response._wall_ms)}` }));
  bar.classList.toggle('hidden', !p);
  if (!p) return;
  line.append(el('span', { text: `total ${fmtMs(p.totalMs)}` }));
  if (p.shardsTotal) line.append(el('span', { text: `shards ${p.shardsSkipped ?? 0}/${p.shardsTotal} skipped` }));
  if (p.segmentsTotal) line.append(el('span', { text: `segments ${p.segmentsSkipped ?? 0}/${p.segmentsTotal} skipped` }));
  if (Number(p.rerankRows)) line.append(el('span', { text: `rerank ${fmtNum(p.rerankRows)} rows, ${fmtNum(p.rerankPages)} pages` }));
  if (response.servedTopologyGeneration) line.append(el('span', { text: `topology ${response.servedTopologyGeneration}` }));

  const parts = PHASES.map(([name, key], i) => ({ name, ms: Number(p[key] || 0), slot: i + 1 })).filter((x) => x.ms > 0);
  const total = Number(p.totalMs || 0);
  const rest = total - parts.reduce((a, x) => a + x.ms, 0);
  if (rest > 0.0005) parts.push({ name: 'fan-out and merge', ms: rest, slot: 0 });
  const sum = parts.reduce((a, x) => a + x.ms, 0);
  if (!sum) { bar.classList.add('hidden'); return; }
  const track = el('div', { class: 'stack', role: 'img', 'aria-label': `phase timings: ${parts.map((x) => `${x.name} ${fmtMs(x.ms)}`).join(', ')}` });
  const legend = el('div', { class: 'legend' });
  for (const x of parts) {
    const title = `${x.name} ${fmtMs(x.ms)} (${((100 * x.ms) / sum).toFixed(0)}%)`;
    track.append(el('span', { class: `seg s${x.slot}`, style: `flex-grow:${x.ms}`, title }));
    legend.append(el('span', { title }, [el('i', { class: `swatch s${x.slot}` }), `${x.name} `, el('span', { class: 'mono', text: fmtMs(x.ms) })]));
  }
  bar.append(track, legend);
}

function foldValue(x) {
  return x.uintValue ?? x.intValue ?? x.doubleValue ?? '–';
}

function renderAggregate(agg) {
  const out = clear($('agg-out'));
  if (!agg) { out.textContent = 'Add a group-by, a fold, a histogram, or percentiles.'; return; }
  const spec = state.request?.aggregate ?? state.request?.selection?.boolean?.aggregate ?? {};
  out.classList.remove('muted');
  out.append(el('div', { class: 'small mono muted', text: `matched ${fmtNum(agg.matched)}${Number(agg.ungrouped) ? `, ungrouped ${fmtNum(agg.ungrouped)}` : ''}` }));
  const folds = (r) => (r || []).filter((x) => x.name !== 'count').map((x) => `${x.name} = ${fmtNum(foldValue(x))} (${fmtNum(x.present)} present)`);
  for (const line of folds(agg.results)) out.append(el('div', { class: 'small', text: line }));

  if (agg.groups?.length) {
    const column = spec.group_by;
    const drillable = isIdentifier(column);
    const active = splitConjuncts($('filter').value);
    const max = Math.max(1, ...agg.groups.map((g) => Number(g.matched)));
    const list = el('div', { class: 'facets' });
    for (const g of agg.groups) {
      const on = active.includes(`${column} == ${celString(g.value)}`);
      list.append(el(drillable ? 'button' : 'div', {
        class: `facet${on ? ' on' : ''}`,
        title: drillable ? drillTitle(column, g.value) : null,
        onclick: drillable ? (e) => drill(column, g.value, e.shiftKey) : null,
      }, [
        el('span', { class: 'name' }, [g.value, folds(g.results).length ? el('span', { class: 'muted small', text: ` ${folds(g.results).join('; ')}` }) : null]),
        el('span', { class: 'count', text: fmtNum(g.matched) }),
        el('span', { class: 'bar', style: `width:${(100 * Number(g.matched)) / max}%` }),
      ]));
    }
    out.append(el('h3', { text: column }), list);
  }
  (agg.histograms || []).forEach((h, i) => out.append(histogramNode(h, spec.histograms?.[i])));
  for (const p of agg.percentiles || []) {
    const table = el('table', {}, [el('tbody', {}, (p.values || []).map((v) => el('tr', {}, [
      el('td', { text: `p${v.percentile}` }), el('td', { class: 'num', text: fmtNum(foldValue(v)) }),
    ])))]);
    out.append(el('h3', { text: p.name.replace(/^pct:/, '') }), el('div', { class: 'small muted', text: `${fmtNum(p.present)} present` }), table);
  }
}

// One histogram as a column chart. A fixed histogram's buckets are keyed
// by `lower` and the gaps between them are empty buckets; a calendar
// one's by `lowerInt`, epoch microseconds, drawn as returned.
function histogramNode(h, spec) {
  const expression = spec?.expression ?? h.name.replace(/^hist:/, '');
  const calendar = spec?.calendar;
  const interval = Number(spec?.interval || 0);
  let buckets = (h.buckets || []).map((b) => ({ lower: calendar ? Number(b.lowerInt) : Number(b.lower), count: Number(b.count) }));
  if (!calendar && interval > 0 && buckets.length > 1) {
    const counts = new Map(buckets.map((b) => [b.lower, b.count]));
    const first = buckets[0].lower, last = buckets[buckets.length - 1].lower;
    const steps = Math.round((last - first) / interval);
    if (steps < 400) buckets = Array.from({ length: steps + 1 }, (_, i) => ({ lower: first + i * interval, count: counts.get(first + i * interval) ?? 0 }));
  }
  // Bucket bounds print without digit grouping: 1990 is as likely a year as a count.
  const label = (lower) => (calendar ? epochLabel(lower) : String(Number(lower.toPrecision(12))));
  const upper = (lower) => (calendar ? calendarUpper(lower, calendar) : lower + interval);
  const target = rangeColumn(expression, calendar);
  const clause = (lower) => (target ? rangeClause(target, lower, upper(lower)) : null);
  const active = splitConjuncts($('filter').value);
  const max = Math.max(1, ...buckets.map((b) => b.count));
  const idle = `${fmtNum(h.present)} present${Number(h.unbucketable) ? `, ${fmtNum(h.unbucketable)} unbucketable` : ''}`;
  const readout = el('div', { class: 'small muted mono', text: idle });
  const plot = el('div', { class: 'columns-chart' });
  for (const b of buckets) {
    const c = clause(b.lower);
    const text = `${label(b.lower)} to ${label(upper(b.lower))}: ${fmtNum(b.count)}`;
    plot.append(el(c ? 'button' : 'span', {
      class: `col${c && active.includes(c) ? ' on' : ''}`,
      'aria-label': text,
      onmouseenter: () => { readout.textContent = text; },
      onfocus: () => { readout.textContent = text; },
      onmouseleave: () => { readout.textContent = idle; },
      title: c ? 'click: only this range; click again to undo' : null,
      onclick: c ? () => setFilter(replaceRange($('filter').value, target.column, c)) : null,
    }, [el('span', { style: `height:${b.count ? Math.max(3, (100 * b.count) / max) : 0}%` })]));
  }
  const axis = buckets.length ? el('div', { class: 'axis small muted mono' }, [el('span', { text: label(buckets[0].lower) }), el('span', { text: label(upper(buckets[buckets.length - 1].lower)) })]) : null;
  return el('div', { class: 'histogram' }, [el('h3', { text: expression }), plot, axis, readout]);
}

function epochLabel(micros) {
  const n = Number(micros);
  if (!Number.isFinite(n) || Math.abs(n) < 1e14) return String(micros);
  return new Date(n / 1000).toISOString().slice(0, 10);
}

function renderSynonyms(list) {
  const out = clear($('synonyms'));
  if (!list?.length) { out.textContent = 'none reported'; return; }
  for (const s of list) out.append(el('div', { class: 'small mono', text: JSON.stringify(s) }));
}

// The dense outcome messages as name and value rows; unset strings are left out.
function renderDense(response) {
  const out = clear($('dense-out'));
  const parts = [response.denseExecution, response.denseQuality].filter(Boolean);
  if (!parts.length) { out.textContent = 'no dense leaf'; return; }
  const list = el('dl', { class: 'kv' });
  for (const part of parts) {
    for (const [name, value] of Object.entries(part)) {
      if (value === '' || value == null) continue;
      const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
      list.append(el('dt', { text: name }), el('dd', { class: 'mono', text }));
    }
  }
  out.append(list);
}

function renderRaw() {
  const pre = $('raw');
  for (const b of document.querySelectorAll('.tabs button')) b.classList.toggle('active', b.dataset.tab === state.tab);
  if (state.tab === 'request') pre.textContent = JSON.stringify(state.request, null, 2);
  else if (state.tab === 'grpcurl') pre.textContent = state.request ? grpcurlFor(state.config, 'SearchService', 'Query', state.request) : '';
  else {
    const r = state.response ? { ...state.response } : null;
    if (r) delete r._wall_ms;
    pre.textContent = JSON.stringify(r, null, 2);
  }
}

// ---------------------------------------------------------------------------
// Keyboard
// ---------------------------------------------------------------------------

function toggleKeys() {
  const hidden = $('keys-help').classList.toggle('hidden');
  $('keys').setAttribute('aria-expanded', String(!hidden));
}

function queryKey(e) {
  const items = [...$('suggest').querySelectorAll('li')];
  const open = !$('suggest').classList.contains('hidden') && items.length;
  if (open && (e.key === 'ArrowDown' || e.key === 'ArrowUp')) {
    e.preventDefault();
    if (state.suggestIndex < 0) state.suggestIndex = e.key === 'ArrowDown' ? 0 : items.length - 1;
    else state.suggestIndex = (state.suggestIndex + (e.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
    items.forEach((li, i) => li.classList.toggle('active', i === state.suggestIndex));
  } else if (e.key === 'Enter' && !e.ctrlKey && !e.metaKey) {
    if (open && state.suggestIndex >= 0) { e.preventDefault(); return items[state.suggestIndex].choose(); }
    hideSuggest();
    run();
  } else if (e.key === 'Escape') {
    hideSuggest();
  }
}

function pageKey(e) {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); hideSuggest(); return run(); }
  if (e.key === 'Escape') { $('keys-help').classList.add('hidden'); $('keys').setAttribute('aria-expanded', 'false'); return; }
  if (e.ctrlKey || e.metaKey || e.altKey || e.target.closest('input, textarea, select')) return;
  if (e.key === '/') { e.preventDefault(); $('q').focus(); $('q').select(); }
  else if (e.key === '?') toggleKeys();
  else if (e.key === '[') page(-1);
  else if (e.key === ']') page(1);
}

// ---------------------------------------------------------------------------
// Typeahead and did-you-mean
// ---------------------------------------------------------------------------

async function typeahead() {
  const input = $('q');
  const text = input.value;
  const m = text.match(/(\S+)$/);
  if (!m || m[1].length < 2) return hideSuggest();
  const prefix = m[1].toLowerCase();
  try {
    const r = await api.rpc('SearchService', 'Suggest', { field: 'body', prefix, limit: 8, analysis: state.config.body_spec });
    const list = clear($('suggest'));
    state.suggestIndex = -1;
    const suggestions = r.suggestions || [];
    if (!suggestions.length) return hideSuggest();
    for (const s of suggestions) {
      const li = el('li', {}, [el('span', { text: s.term }), el('span', { class: 'muted mono', text: `df ${fmtNum(s.df)}` })]);
      li.choose = () => { input.value = `${text.slice(0, m.index)}${s.term} `; hideSuggest(); input.focus(); };
      li.addEventListener('mousedown', li.choose);
      list.append(li);
    }
    list.classList.remove('hidden');
  } catch {
    hideSuggest();
  }
}

function hideSuggest() {
  $('suggest').classList.add('hidden');
  state.suggestIndex = -1;
}

async function didYouMean() {
  const box = $('dym');
  box.classList.add('hidden');
  const text = $('q').value.trim();
  if (!text || $('shape').value === 'browse') return;
  try {
    const r = await api.rpc('SearchService', 'TermSuggest', { field: 'body', text, mode: 'TERM_SUGGEST_MODE_MISSING', limit: 3, analysis: state.config.body_spec });
    const terms = (r.terms || []).filter((t) => t.candidates?.length);
    if (!terms.length) return;
    clear(box).append('Did you mean: ');
    for (const t of terms) {
      for (const c of t.candidates.slice(0, 3)) {
        box.append(el('button', {
          text: `${t.term} → ${c.term}`,
          title: `df ${c.df}, ${c.distance} edit(s)`,
          onclick: () => { $('q').value = $('q').value.replace(t.term, c.term); run(); },
        }), ' ');
      }
    }
    box.classList.remove('hidden');
  } catch {
    // The suggester is optional; a rejection leaves the box hidden.
  }
}

// ---------------------------------------------------------------------------
// The streaming query
// ---------------------------------------------------------------------------

function phaseName(phase) {
  return String(phase || '').replace('QUERY_STREAM_PHASE_', '').toLowerCase();
}

// Each revision replaces the hit list as it arrives; the completion's
// response then renders as a unary one does.
async function streamQuery() {
  hideError();
  state.cursor = ''; state.cursors = [];
  $('ab-panel').classList.add('hidden');
  $('results-panel').classList.remove('hidden');
  let request;
  try {
    request = await buildRequest();
  } catch (e) {
    return showError(e);
  }
  // Explain is served on the unary route only: a revision's hits carry no tree.
  delete request.explain;
  state.request = request;
  const log = clear($('stream-log'));
  log.classList.remove('hidden');
  $('stream').textContent = 'Stop';
  const started = performance.now();
  state.stream = api.stream('SearchService', 'QueryStream', { query: request }, {
    onEvent: async (msg) => {
      if (msg.revision) {
        const r = msg.revision;
        const hits = r.hits || [];
        log.append(el('div', { text: `revision ${r.revision} ${phaseName(r.phase)}: ${hits.length} hit(s) at ${fmtMs(performance.now() - started)}` }));
        if (!hits.length) return;
        if ($('fetch-text').checked) await fetchTexts(hits.map((h) => h.docId));
        if (!state.stream) return;
        $('result-count').textContent = `revision ${r.revision} (${phaseName(r.phase)}): ${hits.length} hit(s), still settling`;
        const scale = pageScale(hits);
        const results = clear($('results'));
        for (const h of hits) results.append(hitNode(h, scale));
      } else if (msg.completion) {
        const c = msg.completion;
        log.append(el('div', { text: `completion: ${c.completed ? 'certified' : 'NOT completed'} at revision ${c.finalRevision}${c.errorMessage ? `, ${c.errorMessage}` : ''}` }));
        if (c.response) {
          c.response._wall_ms = performance.now() - started;
          state.response = c.response;
          await render(c.response);
        }
      }
    },
    onError: (err) => { showError(err); stopStream(); },
    onEnd: () => stopStream(),
  });
}

function stopStream() {
  if (state.stream) { state.stream.close(); state.stream = null; }
  $('stream').textContent = 'Stream';
}

// ---------------------------------------------------------------------------
// A/B through VariantSearch
// ---------------------------------------------------------------------------

function variantPresets() {
  const hybrid = (label, mode, extra = {}) => ({
    id: `hybrid:${mode}`, label, needsVector: true,
    build: (text, vector) => ({ hybrid: { text, vector, legs: { fusion_mode: enums.fusion[mode], ...extra } } }),
  });
  return [
    { id: 'bm25', label: 'BM25 only', needsVector: false, build: (text) => ({ bm25: { text } }) },
    hybrid('Hybrid, rank fusion', 'global_rank'),
    hybrid('Hybrid, cascade', 'cascade'),
    hybrid('Hybrid, score blend', 'score_blend'),
    hybrid('Hybrid, weighted sum', 'decomposed'),
    hybrid('Hybrid, two-level', 'two_level'),
  ];
}

async function runAb() {
  stopStream();
  hideError();
  const text = $('q').value.trim();
  if (!text) return showError({ error: 'query text is required' });
  const presets = variantPresets();
  const a = presets.find((p) => p.id === $('ab-a').value);
  const b = presets.find((p) => p.id === $('ab-b').value);
  let vector = null;
  try {
    if (a.needsVector || b.needsVector) vector = await queryVector(text);
  } catch (e) {
    return showError(e);
  }
  const k = Number($('k').value || 10);
  const request = {
    k,
    interleave: true,
    variants: [
      { label: 'A', ...a.build(text, vector) },
      { label: 'B', ...b.build(text, vector) },
    ],
  };
  const filter = $('filter').value.trim();
  if (filter) for (const v of request.variants) { (v.bm25 || v.hybrid).filter = filter; }
  state.request = request;
  $('run-ab').disabled = true;
  try {
    const response = await api.rpc('SearchService', 'VariantSearch', request);
    state.response = response;
    state.tab = 'response';
    renderRaw();
    await renderAb(response, a, b);
  } catch (e) {
    showError(e);
  } finally {
    $('run-ab').disabled = false;
  }
}

async function renderAb(response, a, b) {
  const panel = clear($('ab-panel'));
  // The comparison stands in for the hit list, which belongs to the
  // last Query and would read as part of it.
  $('results-panel').classList.add('hidden');
  panel.classList.remove('hidden');
  const results = response.results || [];
  if ($('fetch-text').checked) {
    await fetchTexts(results.flatMap((r) => (r.hits || []).map((h) => h.docId)).concat(response.interleaving?.docIds || []));
  }
  const cols = el('div', { class: 'ab' });
  const label = { A: a.label, B: b.label };
  for (const r of results) {
    const col = el('div', { class: 'col' }, [el('h3', { text: `${r.label}: ${label[r.label] || ''}`, class: r.label === 'A' ? 'team-a' : 'team-b' }), el('div', { class: 'muted small', text: fmtMs(r.elapsedMs) })]);
    const other = results.find((o) => o.label !== r.label);
    (r.hits || []).forEach((h, i) => {
      const rankThere = other?.hits?.findIndex((o) => o.docId === h.docId) ?? -1;
      const move = rankThere < 0 ? 'only here' : rankThere === i ? '=' : rankThere > i ? `▲${rankThere - i}` : `▼${i - rankThere}`;
      const doc = state.texts.get(String(h.docId));
      col.append(el('div', { class: 'hit' }, [
        el('div', { class: 'head' }, [el('span', { class: 'rank', text: `#${i + 1}` }), el('span', { class: 'score', text: Number(h.score).toFixed(4) }), el('span', { class: 'id', text: `doc ${h.docId}` }), el('span', { class: 'muted', text: move })]),
        doc?.text ? el('div', { class: 'text small', text: doc.text.slice(0, 240) }) : null,
      ]));
    });
    cols.append(col);
  }
  panel.append(el('h2', { text: 'A/B' }), cols);
  for (const d of response.diffs || []) {
    panel.append(el('div', { class: 'profile' }, [
      `${d.reference} vs ${d.variant} at depth ${d.depth}:`,
      `overlap ${d.overlap} (${(Number(d.overlapFraction) * 100).toFixed(0)}%)`,
      `kendall τ ${fmtNum(d.kendallTau)}`, `rbo ${fmtNum(d.rbo)}`,
      `score regret ${fmtNum(d.scoreRegret)}`, d.top1Flipped ? 'top-1 flipped' : 'top-1 same',
    ].map((text) => el('span', { text }))));
  }
  if (response.interleaving) {
    const il = response.interleaving;
    const list = el('div');
    (il.docIds || []).forEach((id, i) => {
      const team = (il.teams || [])[i] || '';
      const cls = team.endsWith('_A') ? 'team-a' : team.endsWith('_B') ? 'team-b' : '';
      const doc = state.texts.get(String(id));
      list.append(el('div', { class: 'hit' }, [
        el('div', { class: 'head' }, [el('span', { class: 'rank', text: `#${i + 1}` }), el('span', { class: cls, text: team.replace('INTERLEAVE_TEAM_', 'team ') }), el('span', { class: 'id', text: `doc ${id}` })]),
        doc?.text ? el('div', { class: 'text small', text: doc.text.slice(0, 200) }) : null,
      ]));
    });
    panel.append(el('h3', { text: `Interleaved (seed ${il.seed})` }), el('div', { class: 'note', text: 'Team draft interleaving: each position was drawn from A or B; a click on a document credits its team.' }), list);
  }
}
