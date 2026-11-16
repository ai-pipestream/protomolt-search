// The search form's dynamic controls and the QueryRequest built from it.
import { bff } from './bff.js';
import { clear, el, enums } from './common.js';
import { parseSynonyms, splitTopLevel } from './query-state.js';
import { $, MISSING, NORMALIZATIONS, SCORE_OPS, state } from './search-state.js';

// ---------------------------------------------------------------------------
// Form -> QueryRequest
// ---------------------------------------------------------------------------

export function syncShape() {
  const shape = $('shape').value;
  $('shape-hybrid').classList.toggle('hidden', shape !== 'hybrid');
  $('shape-dense-fields').classList.toggle('hidden', !(shape === 'dense' || shape === 'hybrid'));
  $('shape-boolean').classList.toggle('hidden', shape !== 'boolean');
  const fusion = $('fusion').value;
  $('rrf-fields').classList.toggle('hidden', fusion !== 'rrf');
  $('blend-fields').classList.toggle('hidden', fusion !== 'blend');
  // The engine serves these on the single lexical leaf only.
  $('lex-options').classList.toggle('hidden', shape !== 'lexical');
  syncFolds();
}

// A folded panel says how many of its options are set, and opens when
// the form it was restored from set any.
export function syncFolds() {
  const lex = [$('phrase').checked, $('prefix').checked, $('synonyms').value.trim(), $('synonyms-off').checked]
    .filter(Boolean).length + $('stages').querySelectorAll('.stage').length;
  $('lex-options-count').textContent = lex ? `${lex} set` : '';
  if (lex) $('lex-options').open = true;
  const boost = $('boost-text').value.trim() ? 1 : 0;
  $('boost-count').textContent = boost ? 'set' : '';
  if (boost) $('boost-options').open = true;
  const dims = scorerDimensions().length;
  $('scorer-count').textContent = dims ? `${dims} dimension(s)` : '';
  if (dims) $('scorer-options').open = true;
  $('boost-signal-only').classList.toggle('hidden', !(dims && boost));
  clear($('signal-ids')).append(...['base', ...signalIds()].map((value) => el('option', { value })));
}

// The query ids the current form produces: what a scorer dimension can
// read by `query_relevance_id`. A filter has no relevance.
export function signalIds() {
  const shape = $('shape').value;
  const ids = [];
  if (shape === 'lexical' || shape === 'hybrid') ids.push('lex');
  if (shape === 'dense' || shape === 'hybrid') ids.push('dense');
  if (shape === 'boolean') {
    [...$('clauses').querySelectorAll('.leaf')].forEach((leaf, i) => { if (leaf.value !== 'filter') ids.push(`${leaf.value}${i}`); });
  }
  if ($('boost-text').value.trim()) ids.push('boost');
  return ids;
}

export function addDim(dim = {}) {
  const select = (cls, options, chosen) => el('select', { class: cls }, options.map(([value, label, title]) => el('option', { value, text: label, title, selected: value === (chosen ?? '') })));
  const row = el('div', { class: 'stage' }, [
    el('input', { class: 'mono dim-id', value: dim.id ?? '', placeholder: 'name', title: 'names the dimension on each hit' }),
    el('input', { class: 'mono source', value: dim.source ?? '', placeholder: 'base, or a query id', list: 'signal-ids', title: 'base is the selection\'s own score; anything else is a query id (lex, dense, boost, a clause id)' }),
    el('button', { text: '×', title: 'remove this dimension', onclick: () => row.remove() }),
    el('div', { class: 'params' }, [
      el('input', { class: 'mono weight', type: 'number', step: 'any', value: dim.weight ?? '', placeholder: 'weight 1', title: 'weight; 0 reports the dimension without counting it' }),
      select('normalization', NORMALIZATIONS, dim.normalization),
      select('missing', MISSING, dim.missing),
    ]),
  ]);
  $('dims').append(row);
}

// The scorer's dimensions as the request spells them. A row needs a
// source; its name defaults to the source.
export function scorerDimensions() {
  const dimensions = [];
  for (const row of $('dims').querySelectorAll('.stage')) {
    const source = row.querySelector('.source').value.trim();
    if (!source) continue;
    const dimension = {
      id: row.querySelector('.dim-id').value.trim() || source,
      source: source === 'base' ? { base: true } : { query_relevance_id: source },
    };
    const weight = row.querySelector('.weight').value;
    if (weight !== '') dimension.weight = Number(weight);
    const normalization = row.querySelector('.normalization').value;
    if (normalization) dimension.normalization = normalization;
    const missing = row.querySelector('.missing').value;
    if (missing) dimension.missing = missing;
    dimensions.push(dimension);
  }
  return dimensions;
}

export function addStage(stage = {}) {
  const input = (cls, value) => el('input', { class: `mono ${cls}`, type: 'number', step: 'any', value: value ?? '', placeholder: cls, title: cls });
  const op = el('select', { class: 'op' }, SCORE_OPS.map(([value, label, title]) => el('option', { value, text: label, title, selected: value === stage.op })));
  const row = el('div', { class: 'stage' }, [
    op,
    el('input', { class: 'mono column', value: stage.column ?? '', placeholder: 'column', list: 'known-columns' }),
    el('button', { text: '×', title: 'remove this stage', onclick: () => row.remove() }),
    el('div', { class: 'params' }, [input('weight', stage.weight), input('origin', stage.origin), input('scale', stage.scale)]),
  ]);
  // Only the parameters the operation reads take a value.
  const sync = () => {
    const reads = SCORE_OPS.find(([value]) => value === op.value)[3];
    for (const name of ['weight', 'origin', 'scale']) row.querySelector(`.${name}`).classList.toggle('hidden', !reads.includes(name));
    op.title = SCORE_OPS.find(([value]) => value === op.value)[2];
  };
  op.addEventListener('change', sync);
  sync();
  $('stages').append(row);
}

export function addClause(kind = 'should', leaf = 'lexical', value = '') {
  const row = el('div', { class: 'clause' }, [
    el('select', { class: 'kind' }, ['must', 'should', 'must_not'].map((k) => el('option', { value: k, text: k, selected: k === kind }))),
    el('select', { class: 'leaf' }, ['lexical', 'dense', 'filter'].map((k) => el('option', { value: k, text: k, selected: k === leaf }))),
    el('input', { class: 'mono value', value, placeholder: 'text, or a CEL predicate, or (dense) the query text' }),
    el('button', { text: '×', title: 'remove this clause', onclick: () => row.remove() }),
  ]);
  $('clauses').append(row);
}

export function addAgg(name = '', expression = '', op = 'AGGREGATE_OP_COUNT') {
  const row = el('div', { class: 'agg' }, [
    el('input', { class: 'mono name', value: name, placeholder: 'name' }),
    el('input', { class: 'mono expr', value: expression, placeholder: 'expression, e.g. year', list: 'known-columns' }),
    el('select', { class: 'op' }, enums.aggregateOp.map((o) => el('option', { value: o, text: o.replace('AGGREGATE_OP_', '').toLowerCase(), selected: o === op }))),
    el('button', { text: '×', title: 'remove this fold', onclick: () => row.remove() }),
  ]);
  $('aggs').append(row);
}

export function lexicalLeaf(id, text) {
  return { search: { id, lexical: { text } } };
}

// The single lexical leaf with the options the form sets on it.
export function lexicalLeafWithOptions(id, text) {
  const lexical = { text };
  if ($('phrase').checked) lexical.phrase = { slop: Number($('slop').value || 0) };
  if ($('prefix').checked) {
    // The last word becomes the prefix; the words before it stay terms.
    // A prefix is normalized under the field's char filters, so the
    // engine wants the analysis spec spelled out.
    const words = text.split(/\s+/).filter(Boolean);
    const prefix = words.pop() ?? '';
    lexical.text = words.join(' ');
    lexical.prefixes = [{ prefix, max_expansions: Number($('prefix-max').value || 50) }];
    lexical.analysis = state.config.body_spec;
  }
  const synonyms = parseSynonyms($('synonyms').value);
  if (synonyms.length) lexical.synonyms = synonyms;
  if ($('synonyms-off').checked) lexical.synonyms_off = true;
  const stages = [];
  for (const row of $('stages').querySelectorAll('.stage')) {
    const column = row.querySelector('.column').value.trim();
    if (!column) continue;
    const stage = { op: row.querySelector('.op').value, column };
    for (const name of SCORE_OPS.find(([value]) => value === stage.op)[3]) {
      const value = row.querySelector(`.${name}`).value;
      if (value !== '') stage[name] = Number(value);
    }
    stages.push(stage);
  }
  if (stages.length) lexical.score_stages = stages;
  return { search: { id, lexical } };
}

export function denseLeaf(id, vector) {
  const dense = { vector };
  if ($('score-mode').value === 'fp32') dense.score_mode = enums.fp32;
  if ($('exec-mode').value) dense.execution_mode = $('exec-mode').value;
  return { search: { id, dense } };
}

export function filterLeaf(id, cel) {
  return { filter: { id, cel } };
}

export async function queryVector(text) {
  const pasted = $('vector').value.trim();
  if (pasted) {
    const v = JSON.parse(pasted);
    if (!Array.isArray(v) || !v.every((x) => typeof x === 'number')) throw { error: 'the pasted vector is not a JSON array of numbers' };
    return v;
  }
  if (!state.config.analysis) throw { error: 'a dense query needs a pasted vector: no analysis sidecar is configured', code: 'FAILED_PRECONDITION' };
  if (state.vector && state.vector.text === text) return state.vector.vector;
  const r = await bff.call('PreviewEmbedding', { text });
  state.vector = { text, vector: r.vector };
  return r.vector;
}

export function parseSort(spec) {
  return spec.split(',').map((s) => s.trim()).filter(Boolean).map((s) => {
    const [column, dir] = s.split(/\s+/);
    return dir && dir.toLowerCase() === 'desc' ? { column, descending: true } : { column };
  });
}

export function aggregateSpec() {
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

export async function buildRequest() {
  const text = $('q').value.trim();
  const shape = $('shape').value;
  const filter = $('filter').value.trim();
  let selection;
  let vector;
  const needsVector = shape === 'dense' || shape === 'hybrid'
    || (shape === 'boolean' && [...$('clauses').querySelectorAll('.leaf')].some((l) => l.value === 'dense'));
  if (needsVector) vector = await queryVector(text || $('vector').value.trim());
  if (shape !== 'browse' && shape !== 'boolean' && !text && !vector) throw { error: 'query text is required' };

  if (shape === 'lexical') selection = lexicalLeafWithOptions('lex', text);
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
  const dimensions = scorerDimensions();
  if (dimensions.length) request.scorer = { operation: $('scorer-op').value, dimensions };
  const boostText = $('boost-text').value.trim();
  if (boostText) {
    const boost = { query: { id: 'boost', lexical: { text: boostText } } };
    // With a scorer the boost is a signal only, and the engine refuses
    // the fields of the reorder it no longer does.
    for (const [id, field] of dimensions.length ? [] : [['boost-window', 'window'], ['boost-base', 'base_weight'], ['boost-weight', 'boost_weight']]) {
      if ($(id).value !== '') boost[field] = Number($(id).value);
    }
    request.boosts = [boost];
  }
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
