// The pinned baseline, export, the column names seen, and stored text.
import { bff } from './bff.js';
import { clear, el, errorText } from './common.js';
import { encodeState, identifiersIn, toCsv } from './query-state.js';
import { $, BASELINE_KEY, COLUMNS_KEY, COLUMNS_MAX, COLUMN_INPUTS, state } from './search-state.js';
import { readForm } from './search-form.js';
import { page, render, run } from './search-run.js';
import { projectedValue } from './search-hits.js';

// ---------------------------------------------------------------------------
// The pinned baseline
// ---------------------------------------------------------------------------

export function currentHits() {
  const r = state.response || {};
  return r.groups?.length ? r.groups.flatMap((g) => g.hits || []) : r.hits || [];
}

export function pinBaseline() {
  const form = readForm();
  const label = [form.shape, form.q ? `"${form.q}"` : '', form.filter].filter(Boolean).join(' ');
  state.baseline = { label, hash: encodeState(form, state.defaults), ids: currentHits().map((h) => String(h.docId)), at: Date.now() };
  try { sessionStorage.setItem(BASELINE_KEY, JSON.stringify(state.baseline)); } catch { /* the pin lasts for the page then */ }
  if (state.response) render(state.response);
}

export function unpinBaseline() {
  state.baseline = null;
  try { sessionStorage.removeItem(BASELINE_KEY); } catch { /* nothing was stored */ }
  if (state.response?.hits) render(state.response);
  else renderBaseline();
}

export function renderBaseline(comparison) {
  const box = clear($('baseline'));
  const b = state.baseline;
  box.classList.toggle('hidden', !b);
  if (!b) return;
  box.append(
    el('span', { class: 'muted', text: 'baseline' }),
    el('a', { class: 'mono', href: `#${b.hash}`, title: 'restore the form the baseline was run from', text: b.label }),
    el('span', { class: 'muted mono', text: `${b.ids.length} hit(s)` }),
  );
  if (comparison) {
    const depth = comparison.moves.length;
    box.append(el('span', { class: 'mono' }, [
      `overlap ${comparison.overlap}/${Math.max(depth, b.ids.length)} · ${comparison.added} new · ${comparison.dropped.length} dropped · `,
      el('span', { class: comparison.topSame ? '' : 'move down', text: comparison.topSame ? 'top hit same' : 'top hit changed' }),
    ]));
  } else if (state.cursor) {
    box.append(el('span', { class: 'muted', text: 'compared on the first page only' }));
  }
  box.append(el('span', { class: 'spacer' }), el('button', { class: 'quiet', text: 'unpin', onclick: unpinBaseline }));
}

export function moveNode(move) {
  if (move === undefined) return null;
  if (move === null) return el('span', { class: 'move new', text: 'new', title: 'not in the baseline' });
  if (move === 0) return el('span', { class: 'move', text: '=', title: 'same rank as in the baseline' });
  return el('span', { class: `move ${move > 0 ? 'up' : 'down'}`, text: `${move > 0 ? '▲' : '▼'}${Math.abs(move)}`, title: `${move > 0 ? 'up' : 'down'} ${Math.abs(move)} against the baseline` });
}

// What the baseline had that this run does not, folded under the hits.
export async function droppedNode(dropped) {
  if ($('fetch-text').checked) await fetchTexts(dropped.map((d) => d.id));
  const list = el('div');
  for (const d of dropped) {
    const doc = state.texts.get(d.id);
    list.append(el('div', { class: 'hit' }, [
      el('div', { class: 'head' }, [el('span', { class: 'rank', text: `#${d.rank}` }), el('span', { class: 'id', text: `doc ${d.id}` }), el('span', { class: 'move down', text: 'dropped' })]),
      doc?.text ? el('div', { class: 'text clamp', text: doc.text }) : null,
    ]));
  }
  return el('details', { class: 'dropped' }, [el('summary', { class: 'muted', text: `${dropped.length} in the baseline, not in this run (baseline ranks)` }), list]);
}

// ---------------------------------------------------------------------------
// Export, and the column names seen
// ---------------------------------------------------------------------------

export function download(name, type, text) {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = el('a', { href: url, download: name });
  document.body.append(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function exportCsv() {
  const hits = currentHits();
  const signals = [...new Set(hits.flatMap((h) => (h.signals || []).map((s) => s.id)))];
  const projections = (state.request?.projections || []).map((p) => p.name);
  const rows = [['rank', 'doc_id', 'score', ...signals.map((id) => `signal:${id}`), ...projections, 'text']];
  for (const h of hits) {
    const bySignal = new Map((h.signals || []).map((s) => [s.id, s.score]));
    rows.push([
      h.rank, h.docId, h.score, ...signals.map((id) => bySignal.get(id)),
      ...projections.map((_, i) => (h.projected?.[i] ? projectedValue(h.projected[i]) : '')),
      state.texts.get(String(h.docId))?.text ?? (h.snippets || []).map((s) => s.text).join(' … '),
    ]);
  }
  download('hits.csv', 'text/csv', toCsv(rows));
}

export function knownColumns() {
  try {
    const list = JSON.parse(localStorage.getItem(COLUMNS_KEY) || '[]');
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

// No RPC lists a cluster's columns, so the names a query that succeeded
// used are remembered and offered on the single-column inputs.
export function learnColumns() {
  const stages = [...$('stages').querySelectorAll('.column')].map((n) => n.value);
  const aggs = [...$('aggs').querySelectorAll('.expr')].map((n) => n.value);
  const used = identifiersIn([...COLUMN_INPUTS.map((id) => $(id).value), ...stages, ...aggs].join(' ; '));
  const list = [...new Set([...used, ...knownColumns()])].slice(0, COLUMNS_MAX);
  try { localStorage.setItem(COLUMNS_KEY, JSON.stringify(list)); } catch { /* suggestions are a convenience */ }
  renderKnownColumns();
}

export function renderKnownColumns() {
  const list = clear($('known-columns'));
  for (const name of knownColumns()) list.append(el('option', { value: name }));
}

export async function fetchTexts(ids) {
  const missing = ids.filter((id) => id != null && !state.texts.has(String(id)));
  if (!missing.length) return;
  try {
    const r = await bff.call('PreviewDocuments', { doc_ids: missing, max_chars: 4000 });
    for (const d of r.documents || []) state.texts.set(String(d.docId), { doc_id: d.docId, text: d.snippet, truncated: d.truncated });
  } catch (e) {
    // Text is a convenience; the hit list still renders.
    for (const id of missing) state.texts.set(String(id), { text: '', error: errorText(e) });
  }
}
