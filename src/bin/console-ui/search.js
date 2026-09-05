// The search page: builds a QueryRequest from the form, runs it through
// the facade, and renders hits, explain trees, groups, aggregations, and
// the profile line. Also typeahead (Suggest), did-you-mean (TermSuggest),
// the streaming query, and the A/B comparison (VariantSearch).
import {
  api, el, clear, fmtMs, fmtNum, debounce, loadConfig, grpcurlFor, mountHeader,
  copyText, errorText, isUnimplemented, enums,
} from '/common.js';

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
};

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
  syncShape();
  $('add-clause').addEventListener('click', () => addClause());
  addClause('must', 'lexical', '');
  $('add-agg').addEventListener('click', () => addAgg());
  $('run').addEventListener('click', () => run());
  $('stream').addEventListener('click', () => streamQuery());
  $('prev').addEventListener('click', () => page(-1));
  $('next').addEventListener('click', () => page(1));
  $('q').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { hideSuggest(); run(); }
    if (e.key === 'Escape') hideSuggest();
  });
  $('q').addEventListener('input', debounce(typeahead, 150));
  $('q').addEventListener('blur', () => setTimeout(hideSuggest, 150));
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
    el('button', { text: '×', onclick: () => row.remove() }),
  ]);
  $('clauses').append(row);
}

function addAgg(name = '', expression = '', op = 'AGGREGATE_OP_COUNT') {
  const row = el('div', { class: 'agg' }, [
    el('input', { class: 'mono name', value: name, placeholder: 'name' }),
    el('input', { class: 'mono expr', value: expression, placeholder: 'expression, e.g. year' }),
    el('select', { class: 'op' }, enums.aggregateOp.map((o) => el('option', { value: o, text: o.replace('AGGREGATE_OP_', '').toLowerCase(), selected: o === op }))),
    el('button', { text: '×', onclick: () => row.remove() }),
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
  if (aggregations.length) out.aggregations = aggregations;
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
  if ($('highlight').checked && shape === 'lexical') {
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

async function run() {
  stopStream();
  $('ab-panel').classList.add('hidden');
  hideError();
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
  run();
}

$('q').addEventListener('change', () => { state.cursor = ''; state.cursors = []; });
for (const id of ['shape', 'filter', 'k', 'sort', 'collapse']) {
  $(id).addEventListener('change', () => { state.cursor = ''; state.cursors = []; });
}

function showError(e) {
  const node = $('error');
  node.textContent = errorText(e);
  node.classList.remove('hidden');
}

function hideError() { $('error').classList.add('hidden'); }

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

async function render(response) {
  const results = clear($('results'));
  const hits = response.hits || [];
  const groups = response.groups || [];
  $('result-count').textContent = groups.length
    ? `${groups.length} group(s), executed ${response.executed || ''}`
    : `${hits.length} hit(s), executed ${response.executed || ''}`;
  $('next').disabled = !response.nextCursor;
  $('prev').disabled = !state.cursors.length;
  renderProfile(response);
  renderAggregate(response.aggregate);
  renderSynonyms(response.synonymExpansions);
  renderDense(response);
  renderRaw();

  const all = groups.length ? groups.flatMap((g) => g.hits || []) : hits;
  if ($('fetch-text').checked) await fetchTexts(all.map((h) => h.docId));
  if (groups.length) {
    for (const g of groups) {
      const key = g.key ? (g.key.text ?? g.key.integer ?? g.key.number) : '';
      const box = el('div', { class: 'group' }, [
        el('div', { class: 'key' }, [`${key}`, ' ', el('span', { class: 'muted small mono', text: `${g.complete ? 'complete' : 'partial'}, pool ${g.poolHits ?? 0}` })]),
      ]);
      const inner = el('div', { class: 'inner' });
      for (const h of g.hits || []) inner.append(hitNode(h));
      box.append(inner);
      results.append(box);
    }
  } else {
    for (const h of hits) results.append(hitNode(h));
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

function hitNode(h) {
  const doc = state.texts.get(String(h.docId));
  const node = el('div', { class: 'hit' });
  node.append(el('div', { class: 'head' }, [
    el('span', { class: 'rank', text: `#${h.rank ?? ''}` }),
    el('span', { class: 'score', text: Number(h.score ?? 0).toFixed(6) }),
    el('span', { class: 'id', text: `doc ${h.docId}` }),
    doc?.lineage ? el('span', { class: 'muted', text: `parent ${doc.lineage.parent_id} group ${doc.lineage.group_id}` }) : null,
    h.sortValues?.length ? el('span', { class: 'muted', text: `sort ${h.sortValues.map((v) => v.text ?? v.integer ?? v.number).join(' | ')}` }) : null,
  ]));
  if (h.snippets?.length) {
    for (const s of h.snippets) node.append(el('div', { class: 'text' }, snippetNodes(s)));
  } else if (doc?.text) {
    node.append(el('div', { class: 'text', text: doc.text.length > 1200 ? `${doc.text.slice(0, 1200)} …` : doc.text }));
  } else if (doc?.error) {
    node.append(el('div', { class: 'muted small', text: `text unavailable: ${doc.error}` }));
  }
  const meta = el('div', { class: 'meta signals' });
  for (const s of h.signals || []) meta.append(el('span', { text: `${s.id}=${Number(s.score).toFixed(4)}` }));
  for (const d of h.dimensions || []) meta.append(el('span', { text: `${d.id}: ${d.skipped ? 'skipped' : `raw ${fmtNum(d.raw)} norm ${fmtNum(d.normalized)} → ${fmtNum(d.contribution)}`}` }));
  if (h.matched?.length) meta.append(el('span', { text: `matched ${h.matched.join(',')}` }));
  for (const p of h.projected || []) meta.append(el('span', { text: `${p.name ?? ''}=${p.stringValue ?? p.intValue ?? p.doubleValue ?? p.boolValue}` }));
  node.append(meta);
  if (h.explain) {
    const drawer = el('details', { class: 'tree' }, [el('summary', { class: 'muted small', text: 'explain' })]);
    drawer.append(el('ul', {}, [explainNode(h.explain)]));
    node.append(drawer);
  }
  return node;
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

function renderProfile(response) {
  const line = clear($('profile-line'));
  const p = response.profile;
  line.append(el('span', { text: `wall ${fmtMs(response._wall_ms)}` }));
  if (!p) return;
  const phases = [['selection', p.selectionMs], ['boost', p.boostMs], ['values', p.valuesMs], ['scorer', p.scorerMs], ['projection', p.projectionMs], ['rerank', p.rerankMs], ['collapse', p.collapseMs]];
  for (const [name, ms] of phases) if (ms) line.append(el('span', { text: `${name} ${fmtMs(ms)}` }));
  line.append(el('span', { text: `total ${fmtMs(p.totalMs)}` }));
  if (p.segmentsTotal) line.append(el('span', { text: `segments ${p.segmentsSkipped ?? 0}/${p.segmentsTotal} skipped` }));
  if (p.rerankRows) line.append(el('span', { text: `rerank ${fmtNum(p.rerankRows)} rows, ${fmtNum(p.rerankPages)} pages` }));
  if (response.servedTopologyGeneration) line.append(el('span', { text: `topology ${response.servedTopologyGeneration}` }));
}

function renderAggregate(agg) {
  const out = clear($('agg-out'));
  if (!agg) { out.textContent = 'none requested'; return; }
  out.append(el('div', { class: 'small mono', text: `matched ${fmtNum(agg.matched)}${agg.ungrouped ? `, ungrouped ${fmtNum(agg.ungrouped)}` : ''}` }));
  const results = (r) => (r || []).map((x) => `${x.name} = ${x.intValue ?? x.doubleValue ?? '–'} (${fmtNum(x.present)} present)`);
  for (const line of results(agg.results)) out.append(el('div', { class: 'small', text: line }));
  if (agg.groups?.length) {
    const max = Math.max(...agg.groups.map((g) => Number(g.matched)));
    const list = el('div');
    for (const g of agg.groups) {
      list.append(el('div', { class: 'facet' }, [
        el('span', {}, [g.value, results(g.results).length ? el('span', { class: 'muted small', text: ` ${results(g.results).join('; ')}` }) : null]),
        el('span', { class: 'count', text: fmtNum(g.matched) }),
      ]));
      list.append(el('div', { class: 'bar', style: `width:${(100 * Number(g.matched)) / max}%` }));
    }
    out.append(list);
  }
  for (const h of agg.histograms || []) {
    const box = el('div', {}, [el('div', { class: 'small muted', text: `${h.name}: ${fmtNum(h.present)} present, ${fmtNum(h.unbucketable)} unbucketable` })]);
    const max = Math.max(1, ...(h.buckets || []).map((b) => Number(b.count)));
    for (const b of h.buckets || []) {
      const lower = b.lowerInt != null ? epochLabel(b.lowerInt) : fmtNum(b.lower);
      box.append(el('div', { class: 'facet' }, [el('span', { class: 'mono', text: lower }), el('span', { class: 'count', text: fmtNum(b.count) })]));
      box.append(el('div', { class: 'bar', style: `width:${(100 * Number(b.count)) / max}%` }));
    }
    out.append(box);
  }
  for (const p of agg.percentiles || []) {
    out.append(el('div', { class: 'small', text: `${p.name}: ${(p.values || []).map((v) => `p${v.percentile} = ${v.intValue ?? v.doubleValue}`).join(', ')} (${fmtNum(p.present)} present)` }));
  }
}

function epochLabel(micros) {
  const n = Number(micros);
  if (!Number.isFinite(n) || Math.abs(n) < 1e12) return String(micros);
  return new Date(n / 1000).toISOString().slice(0, 10);
}

function renderSynonyms(list) {
  const out = clear($('synonyms'));
  if (!list?.length) { out.textContent = 'none reported'; return; }
  for (const s of list) out.append(el('div', { class: 'small mono', text: JSON.stringify(s) }));
}

function renderDense(response) {
  const out = clear($('dense-out'));
  const d = response.denseExecution;
  const q = response.denseQuality;
  if (!d && !q) { out.textContent = 'no dense leaf'; return; }
  if (d) out.append(el('div', { text: JSON.stringify(d) }));
  if (q) out.append(el('div', { text: JSON.stringify(q) }));
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
    const suggestions = r.suggestions || [];
    if (!suggestions.length) return hideSuggest();
    for (const s of suggestions) {
      list.append(el('li', {
        onmousedown: () => { input.value = text.slice(0, m.index) + s.term + ' '; hideSuggest(); input.focus(); },
      }, [el('span', { text: s.term }), el('span', { class: 'muted mono', text: `df ${fmtNum(s.df)}` })]));
    }
    list.classList.remove('hidden');
  } catch {
    hideSuggest();
  }
}

function hideSuggest() { $('suggest').classList.add('hidden'); }

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

async function streamQuery() {
  stopStream();
  hideError();
  let request;
  try {
    request = await buildRequest();
  } catch (e) {
    return showError(e);
  }
  state.request = request;
  const log = clear($('stream-log'));
  log.classList.remove('hidden');
  $('stream').textContent = 'Stop';
  const started = performance.now();
  state.stream = api.stream('SearchService', 'QueryStream', { query: request }, {
    onEvent: async (msg) => {
      if (msg.revision) {
        const r = msg.revision;
        log.append(el('div', { text: `revision ${r.revision} ${String(r.phase || '').replace('QUERY_STREAM_PHASE_', '').toLowerCase()}: ${(r.hits || []).length} hit(s) at ${fmtMs(performance.now() - started)}` }));
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
    ]));
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
