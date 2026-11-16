// The search page: builds a QueryRequest from the form, runs it through
// the facade, and renders hits, explain trees, groups, aggregations, and
// the profile. Also typeahead (Suggest), did-you-mean (TermSuggest), the
// streaming query, the A/B comparison (VariantSearch), the filter chips
// and drill-down, the URL state, and the history. This module wires the
// page; the work lives in the search-*.js modules it imports.
import { copyText, debounce, el, loadConfig, mountHeader } from './common.js';
import { $, BASELINE_KEY, state } from './search-state.js';
import { readForm, renderHistory, restoreFromUrl, saveHistory } from './search-form.js';
import { addAgg, addClause, addDim, addStage, syncFolds, syncShape } from './search-request.js';
import { page, renderChips, run, showError, timeQuery } from './search-run.js';
import {
  download, exportCsv, pinBaseline, renderBaseline, renderKnownColumns,
} from './search-results.js';
import { renderRaw } from './search-hits.js';
import {
  hideSuggest, pageKey, queryKey, runAb, stopStream, streamQuery, toggleKeys, typeahead,
  variantPresets,
} from './search-live.js';

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
  $('add-stage').addEventListener('click', () => addStage());
  $('add-dim').addEventListener('click', () => { addDim(); syncFolds(); });
  $('scorer-options').addEventListener('toggle', syncFolds);
  $('time').addEventListener('click', timeQuery);
  $('pin').addEventListener('click', pinBaseline);
  $('export-csv').addEventListener('click', exportCsv);
  $('export-json').addEventListener('click', () => download('response.json', 'application/json', $('raw').textContent));
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
  try { state.baseline = JSON.parse(sessionStorage.getItem(BASELINE_KEY) || 'null'); } catch { state.baseline = null; }
  syncShape();
  renderHistory();
  renderKnownColumns();
  renderBaseline();
  window.addEventListener('popstate', () => restoreFromUrl());
  restoreFromUrl();
}
